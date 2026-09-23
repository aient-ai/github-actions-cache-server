import type { IncomingMessage } from 'node:http'
import { createHash, randomBytes, randomUUID } from 'node:crypto'
import http from 'node:http'
import { Readable, Writable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { setTimeout as sleep } from 'node:timers/promises'

import { describe, expect, test } from 'vitest'
import { getDatabase } from '~/lib/db'
import { Storage } from '~/lib/storage'

const OBJECT_BYTES = 64 * 1024 * 1024
// tests/setup.ts runs the server with DOWNLOAD_CONCURRENCY=4 × DOWNLOAD_CHUNK_BYTES=1 MiB
const WINDOW_BUDGET_BYTES = 4 * 1024 * 1024
const GC_SLACK_BYTES = 28 * 1024 * 1024

async function createMergedEntry(object: Buffer) {
  const storage = await Storage.fromEnv()
  const db = await getDatabase()
  const folderName = `backpressure-${randomUUID()}`
  const locationId = randomUUID()
  const entryId = randomUUID()
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
  return {
    url: `${process.env.API_BASE_URL}/download/${entryId}`,
    locationId,
    async readerLeases() {
      const leases = await db
        .selectFrom('storage_reader_leases')
        .select('id')
        .where('storageLocationId', '=', locationId)
        .execute()
      return leases.length
    },
    async remove() {
      await db.deleteFrom('cache_entries').where('id', '=', entryId).execute()
      await db
        .deleteFrom('storage_reader_leases')
        .where('storageLocationId', '=', locationId)
        .execute()
      await db.deleteFrom('storage_locations').where('id', '=', locationId).execute()
      await storage.adapter.deleteFolder(folderName)
    },
  }
}

/** Bytes the server process holds in Buffers/ArrayBuffers plus the V8 heap. */
async function serverMemoryBytes() {
  const response = await fetch(`${process.env.API_BASE_URL}/metrics`)
  const text = await response.text()
  const metric = (name: string) =>
    Number(text.match(new RegExp(String.raw`^${name} (\S+)$`, 'm'))![1])
  return metric('nodejs_external_memory_bytes') + metric('nodejs_heap_size_used_bytes')
}

function get(url: string) {
  return new Promise<IncomingMessage>((resolve, reject) =>
    http.get(url, resolve).once('error', reject),
  )
}

describe('download backpressure', () => {
  test(
    'a slow client throttles storage reads instead of buffering the object in the server',
    { timeout: 120_000 },
    async () => {
      const object = randomBytes(OBJECT_BYTES)
      const entry = await createMergedEntry(object)
      try {
        const baseline = await serverMemoryBytes()
        const response = await get(entry.url)
        expect(response.statusCode).toBe(200)

        // ~64 KiB/s until `slow` is cleared, then as fast as the socket allows
        let slow = true
        const hash = createHash('sha256')
        const done = pipeline(
          response,
          new Writable({
            highWaterMark: 16 * 1024,
            write(chunk: Buffer, _encoding, callback) {
              hash.update(chunk)
              if (slow) setTimeout(callback, (chunk.length / (64 * 1024)) * 1000)
              else callback()
            },
          }),
        )

        let peakGrowth = 0
        for (let sample = 0; sample < 12; sample++) {
          await sleep(250)
          peakGrowth = Math.max(peakGrowth, (await serverMemoryBytes()) - baseline)
        }
        slow = false
        await done

        expect(hash.digest('hex')).toBe(createHash('sha256').update(object).digest('hex'))
        // an unthrottled server reads the whole 64 MiB object into its response buffer.
        // The server never forces a GC, so the sample also holds up to ~20 MiB of
        // collectable garbage from the storage client; it stays flat as the object or
        // the slow phase grows, so half the object still separates the two.
        expect(peakGrowth).toBeLessThan(WINDOW_BUDGET_BYTES + GC_SLACK_BYTES)
      } finally {
        await entry.remove()
      }
    },
  )

  test('a client disconnect cancels the download and releases its reader lease', async () => {
    const entry = await createMergedEntry(randomBytes(16 * 1024 * 1024))
    try {
      const response = await get(entry.url)
      expect(response.statusCode).toBe(200)
      // read one chunk, then stall so the server blocks on backpressure
      await new Promise((resolve) => response.once('data', resolve))
      response.pause()
      await expect.poll(entry.readerLeases).toBe(1)

      response.destroy()
      await expect.poll(entry.readerLeases, { timeout: 5000 }).toBe(0)
    } finally {
      await entry.remove()
    }
  })
})
