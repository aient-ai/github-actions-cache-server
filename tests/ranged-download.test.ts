import type { ByteRange } from '~/lib/ranged-download'
import { randomBytes, randomUUID } from 'node:crypto'
import { Readable } from 'node:stream'
import { setTimeout as sleep } from 'node:timers/promises'

import { describe, expect, test } from 'vitest'
import { getDatabase } from '~/lib/db'
import {
  createParallelRangeStream,
  parseRangeHeader,
  RangeNotSatisfiableError,
  resolveRange,
} from '~/lib/ranged-download'
import { Storage } from '~/lib/storage'

function chunked(buffer: Buffer, chunkBytes: number) {
  return Readable.from(
    (function* () {
      for (let offset = 0; offset < buffer.length; offset += chunkBytes)
        yield buffer.subarray(offset, offset + chunkBytes)
    })(),
  )
}

describe('range header', () => {
  test.each([
    ['bytes=0-99', { start: 0, end: 99 }],
    ['bytes=100-', { start: 100 }],
    ['bytes=-50', { suffixLength: 50 }],
    [' bytes=5-5 ', { start: 5, end: 5 }],
  ])('parses %j', (header, expected) => {
    expect(parseRangeHeader(header)).toEqual(expected)
  })

  test.each([undefined, '', 'bytes=-', 'bytes=10-5', 'bytes=0-1,5-6', 'items=0-1', 'bytes=a-b'])(
    'serves %j as a full response',
    (header) => {
      expect(parseRangeHeader(header)).toBeUndefined()
    },
  )

  test('resolves against the object size', () => {
    expect(resolveRange({ start: 10 }, 100)).toEqual({ start: 10, end: 99 })
    expect(resolveRange({ start: 10, end: 1000 }, 100)).toEqual({ start: 10, end: 99 })
    expect(resolveRange({ suffixLength: 30 }, 100)).toEqual({ start: 70, end: 99 })
    expect(resolveRange({ suffixLength: 300 }, 100)).toEqual({ start: 0, end: 99 })
    expect(() => resolveRange({ start: 100 }, 100)).toThrow(RangeNotSatisfiableError)
    expect(() => resolveRange({ suffixLength: 0 }, 100)).toThrow(RangeNotSatisfiableError)
    expect(() => resolveRange({ suffixLength: 5 }, 0)).toThrow(RangeNotSatisfiableError)
  })
})

describe('parallel range stream', () => {
  test('reassembles windows in order while bounding in-flight reads', async () => {
    const object = randomBytes(1_000_003)
    let inFlight = 0
    let maxInFlight = 0
    const requested: ByteRange[] = []
    const stream = createParallelRangeStream(
      async (range) => {
        requested.push(range)
        inFlight++
        maxInFlight = Math.max(maxInFlight, inFlight)
        // later windows finish first, so ordering must come from reassembly
        await sleep(Math.max(1, 20 - requested.length))
        const body = chunked(object.subarray(range.start, range.end + 1), 7000)
        body.once('close', () => inFlight--)
        return body
      },
      { start: 0, end: object.length - 1 },
      { concurrency: 4, chunkBytes: 64 * 1024 },
    )

    const restored = Buffer.concat(await stream.toArray())
    expect(restored.equals(object)).toBe(true)
    expect(requested).toHaveLength(Math.ceil(object.length / (64 * 1024)))
    expect(maxInFlight).toBeGreaterThan(1)
    expect(maxInFlight).toBeLessThanOrEqual(4)
  })

  test('serves a sub-range', async () => {
    const object = randomBytes(300_000)
    const stream = createParallelRangeStream(
      async (range) => chunked(object.subarray(range.start, range.end + 1), 4096),
      { start: 12_345, end: 234_567 },
      { concurrency: 3, chunkBytes: 50_000 },
    )
    const restored = Buffer.concat(await stream.toArray())
    expect(restored.equals(object.subarray(12_345, 234_568))).toBe(true)
  })

  test('retries a failed window from its first undelivered byte', async () => {
    const object = randomBytes(200_000)
    const opened: ByteRange[] = []
    let failed = false
    const stream = createParallelRangeStream(
      async (range) => {
        opened.push(range)
        const body = object.subarray(range.start, range.end + 1)
        if (range.start === 100_000 && !failed) {
          failed = true
          return Readable.from(
            (async function* () {
              yield body.subarray(0, 10_000)
              throw new Error('connection reset')
            })(),
          )
        }
        return chunked(body, 8192)
      },
      { start: 0, end: object.length - 1 },
      { concurrency: 2, chunkBytes: 100_000 },
    )
    const restored = Buffer.concat(await stream.toArray())
    expect(restored.equals(object)).toBe(true)
    expect(opened).toContainEqual({ start: 110_000, end: 199_999 })
  })

  test('fails when a window keeps failing', async () => {
    const stream = createParallelRangeStream(
      async () => {
        throw new Error('storage unavailable')
      },
      { start: 0, end: 999 },
      { concurrency: 2, chunkBytes: 100, attempts: 2 },
    )
    await expect(stream.toArray()).rejects.toThrow('storage unavailable')
  })

  test('rejects a short window', async () => {
    const stream = createParallelRangeStream(
      async () => Readable.from([Buffer.alloc(10)]),
      { start: 0, end: 99 },
      { concurrency: 2, chunkBytes: 50, attempts: 1 },
    )
    await expect(stream.toArray()).rejects.toThrow('Storage returned 10 of 50 bytes')
  })

  test('aborts in-flight reads when the consumer goes away', async () => {
    const destroyed: ByteRange[] = []
    const stream = createParallelRangeStream(
      async (range) => {
        const body = new Readable({ read() {} })
        body.push(Buffer.alloc(10))
        body.once('close', () => {
          destroyed.push(range)
        })
        return body
      },
      { start: 0, end: 999 },
      { concurrency: 3, chunkBytes: 100 },
    )
    await new Promise((resolve) => stream.once('data', resolve))
    stream.destroy()
    await expect.poll(() => destroyed.length).toBe(3)
  })
})

describe('download endpoint', () => {
  test(
    'serves full, ranged and unsatisfiable requests for a merged entry',
    { timeout: 30_000 },
    async () => {
      const storage = await Storage.fromEnv()
      const db = await getDatabase()
      const folderName = `ranged-${randomUUID()}`
      const locationId = randomUUID()
      const entryId = randomUUID()
      const object = randomBytes(3 * 1024 * 1024 + 17)
      await storage.adapter.uploadStream(`${folderName}/merged`, Readable.from(object))
      await db
        .insertInto('storage_locations')
        .values({
          id: locationId,
          folderName,
          partCount: 0,
          mergedAt: Date.now(),
          mergeStartedAt: Date.now(),
          partsDeletedAt: Date.now(),
          lastDownloadedAt: null,
        })
        .execute()
      await db
        .insertInto('cache_entries')
        .values({
          id: entryId,
          key: randomUUID(),
          version: 'v1',
          scope: 'refs/heads/main',
          repoId: '123',
          updatedAt: Date.now(),
          locationId,
        })
        .execute()

      const url = `${process.env.API_BASE_URL}/download/${entryId}`
      try {
        const full = await fetch(url)
        expect(full.status).toBe(200)
        expect(full.headers.get('content-length')).toBe(String(object.length))
        expect(full.headers.get('accept-ranges')).toBe('bytes')
        expect(Buffer.from(await full.arrayBuffer()).equals(object)).toBe(true)

        // BuildKit resumes with an open-ended range and requires a matching Content-Range
        const resumed = await fetch(url, { headers: { range: 'bytes=1000000-' } })
        expect(resumed.status).toBe(206)
        expect(resumed.headers.get('content-range')).toBe(
          `bytes 1000000-${object.length - 1}/${object.length}`,
        )
        expect(Buffer.from(await resumed.arrayBuffer()).equals(object.subarray(1_000_000))).toBe(
          true,
        )

        const small = await fetch(url, { headers: { range: 'bytes=5-9' } })
        expect(small.status).toBe(206)
        expect(small.headers.get('content-length')).toBe('5')
        expect(Buffer.from(await small.arrayBuffer()).equals(object.subarray(5, 10))).toBe(true)

        const unsatisfiable = await fetch(url, { headers: { range: `bytes=${object.length}-` } })
        expect(unsatisfiable.status).toBe(416)
        expect(unsatisfiable.headers.get('content-range')).toBe(`bytes */${object.length}`)
      } finally {
        await db.deleteFrom('cache_entries').where('id', '=', entryId).execute()
        await db
          .deleteFrom('storage_reader_leases')
          .where('storageLocationId', '=', locationId)
          .execute()
        await db.deleteFrom('storage_locations').where('id', '=', locationId).execute()
        await storage.adapter.deleteFolder(folderName)
      }
    },
  )
})
