import type { ServerResponse } from 'node:http'
import type { Readable } from 'node:stream'
import { finished } from 'node:stream'
import { z } from 'zod'
import { logger } from '~/lib/logger'
import { parseRangeHeader, RangeNotSatisfiableError } from '~/lib/ranged-download'
import { getStorage } from '~/lib/storage'

const pathParamsSchema = z.object({
  cacheEntryId: z.string(),
})

export default defineEventHandler(async (event) => {
  const parsedPathParams = pathParamsSchema.safeParse(event.context.params)
  if (!parsedPathParams.success)
    throw createError({
      statusCode: 400,
      statusMessage: `Invalid path parameters: ${parsedPathParams.error.message}`,
    })

  const { cacheEntryId } = parsedPathParams.data

  const storage = await getStorage()
  let download
  try {
    download = await storage.download(
      cacheEntryId,
      parseRangeHeader(getRequestHeader(event, 'range')),
    )
  } catch (err) {
    if (!(err instanceof RangeNotSatisfiableError)) throw err
    setResponseStatus(event, 416)
    setResponseHeader(event, 'content-range', `bytes */${err.totalBytes}`)
    return ''
  }
  if (!download)
    throw createError({
      statusCode: 404,
      message: 'Cache file not found',
    })
  const { stream, totalBytes, range } = download

  if (totalBytes !== undefined) {
    const { start, end } = range ?? { start: 0, end: totalBytes - 1 }
    setResponseHeader(event, 'accept-ranges', 'bytes')
    setResponseHeader(event, 'content-length', end - start + 1)
    if (range) {
      setResponseStatus(event, 206)
      setResponseHeader(event, 'content-range', `bytes ${start}-${end}/${totalBytes}`)
    }
  }

  const res = event.node.res
  try {
    await pipeToResponse(stream, res)
  } catch (err) {
    // Once the response has started flushing, we can't surface stream errors
    // as an HTTP error — Nitro's default error handler would call
    // `setResponseHeaders` after headers were already sent and crash with
    // ERR_HTTP_HEADERS_SENT (logged as an unhandled error). Client aborts on
    // long downloads are expected (cancelled jobs, parallel runners), so we
    // log and swallow once headers are out.
    if (res.headersSent) {
      // a truncated body must not look like a stalled one to the client
      res.destroy()
      if (event.node.req.destroyed)
        logger.debug(`Client aborted /download/${cacheEntryId}: ${(err as Error).message}`)
      else logger.error(`Download stream failed for ${cacheEntryId}`, { error: err })
      return
    }
    for (const header of ['accept-ranges', 'content-length', 'content-range'])
      res.removeHeader(header)
    throw err
  }
})

/**
 * Pipes `stream` into the response at the pace the client reads it. h3's
 * `sendStream` ignores `res.write`'s return value for web streams, so a slow
 * client made the server buffer the whole object, and for Node streams it
 * never destroys the source when the client goes away.
 */
function pipeToResponse(stream: Readable, res: ServerResponse) {
  return new Promise<void>((resolve, reject) => {
    res.once('finish', resolve)
    res.once('close', () => {
      // destroying the source aborts its storage reads and releases its lease
      if (!res.writableFinished) stream.destroy(new Error('Client closed the connection'))
    })
    finished(stream, { writable: false }, (err) => {
      if (!err) return
      stream.unpipe(res)
      reject(err)
    })
    stream.pipe(res)
  })
}
