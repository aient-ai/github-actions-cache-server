import { Readable } from 'node:stream'

/** An inclusive byte range, as in `Range: bytes=start-end`. */
export interface ByteRange {
  start: number
  end: number
}

/** A single-range `Range` request before it is resolved against the object size. */
export type RangeRequest = { start: number; end?: number } | { suffixLength: number }

export class RangeNotSatisfiableError extends Error {
  constructor(readonly totalBytes: number) {
    super(`Requested range is not satisfiable for an object of ${totalBytes} bytes`)
    this.name = 'RangeNotSatisfiableError'
  }
}

/**
 * Parses a `Range: bytes=…` header with exactly one range. Absent, malformed
 * and multi-range headers return `undefined` and are served as a full
 * response, which RFC 9110 §14.2 permits.
 */
export function parseRangeHeader(header: string | undefined): RangeRequest | undefined {
  const match = header?.trim().match(/^bytes=(\d*)-(\d*)$/)
  if (!match) return
  const [, first = '', last = ''] = match
  const start = first === '' ? undefined : Number(first)
  const end = last === '' ? undefined : Number(last)
  if (start === undefined) {
    if (end === undefined || !Number.isSafeInteger(end)) return
    return { suffixLength: end }
  }
  if (!Number.isSafeInteger(start)) return
  if (end === undefined) return { start }
  if (!Number.isSafeInteger(end) || end < start) return
  return { start, end }
}

export function resolveRange(request: RangeRequest, totalBytes: number): ByteRange {
  if ('suffixLength' in request) {
    if (request.suffixLength === 0 || totalBytes === 0)
      throw new RangeNotSatisfiableError(totalBytes)
    return { start: Math.max(0, totalBytes - request.suffixLength), end: totalBytes - 1 }
  }
  if (request.start >= totalBytes) throw new RangeNotSatisfiableError(totalBytes)
  return { start: request.start, end: Math.min(request.end ?? totalBytes - 1, totalBytes - 1) }
}

interface Window extends ByteRange {
  chunks: Buffer[]
  received: number
  settled: boolean
  failure?: { error: unknown }
  wake?: () => void
}

async function fetchWindow(
  open: (range: ByteRange) => Promise<Readable>,
  window: Window,
  signal: AbortSignal,
  attempts: number,
) {
  const expected = window.end - window.start + 1
  for (let attempt = 1; ; attempt++) {
    try {
      // a retry resumes after the bytes an earlier attempt already delivered
      const stream = await open({ start: window.start + window.received, end: window.end })
      const abort = () => stream.destroy(signal.reason)
      if (signal.aborted) abort()
      signal.addEventListener('abort', abort, { once: true })
      try {
        for await (const chunk of stream as AsyncIterable<Buffer>) {
          window.received += chunk.length
          if (window.received > expected)
            throw new Error(
              `Storage returned more than the ${expected} bytes of range ${window.start}-${window.end}`,
            )
          window.chunks.push(chunk)
          window.wake?.()
        }
      } finally {
        signal.removeEventListener('abort', abort)
      }
      if (window.received !== expected)
        throw new Error(
          `Storage returned ${window.received} of ${expected} bytes of range ${window.start}-${window.end}`,
        )
      return
    } catch (err) {
      if (signal.aborted || attempt >= attempts) throw err
    }
  }
}

/**
 * Streams `range` of one object in order while reading it as `concurrency`
 * ranged requests of at most `chunkBytes`. Object stores often cap a single
 * GET stream well below the link rate; parallel windows recover the rest.
 *
 * The window at the head of the stream is forwarded as it arrives, so the
 * client sees bytes at single-stream speed or better from the first request.
 * The following windows are buffered, bounding memory per download to about
 * `concurrency × chunkBytes`. The next window starts only once the head
 * window has been fully handed to the consumer, so a slow client throttles the
 * reads. A failed window is retried from its first undelivered byte; when the
 * consumer destroys the stream every in-flight request is aborted.
 */
export function createParallelRangeStream(
  open: (range: ByteRange) => Promise<Readable>,
  range: ByteRange,
  {
    concurrency,
    chunkBytes,
    attempts = 3,
  }: { concurrency: number; chunkBytes: number; attempts?: number },
) {
  const controller = new AbortController()
  const pending: Window[] = []
  let nextStart = range.start

  const fill = () => {
    while (pending.length < concurrency && nextStart <= range.end) {
      const window: Window = {
        start: nextStart,
        end: Math.min(nextStart + chunkBytes - 1, range.end),
        chunks: [],
        received: 0,
        settled: false,
      }
      nextStart = window.end + 1
      pending.push(window)
      void fetchWindow(open, window, controller.signal, attempts)
        .catch((err: unknown) => (window.failure = { error: err }))
        .finally(() => {
          window.settled = true
          window.wake?.()
        })
    }
  }

  async function* drain(window: Window) {
    while (true) {
      const chunk = window.chunks.shift()
      if (chunk) {
        yield chunk
        continue
      }
      if (window.failure) throw window.failure.error
      if (window.settled) return
      await new Promise<void>((resolve) => (window.wake = resolve))
      window.wake = undefined
    }
  }

  async function* windows() {
    fill()
    while (pending.length > 0) {
      yield* drain(pending[0]!)
      pending.shift()
      fill()
    }
  }

  // Not `Readable.from`: its destroy waits for the generator to return, which
  // never happens while the generator awaits a stalled window.
  const iterator = windows()
  let reading = false
  const stream = new Readable({
    read() {
      if (reading) return
      reading = true
      iterator.next().then(
        ({ done, value }) => {
          reading = false
          stream.push(done ? null : value)
        },
        (err) => stream.destroy(err),
      )
    },
    destroy(err, callback) {
      controller.abort()
      callback(err)
    },
  })
  return stream
}
