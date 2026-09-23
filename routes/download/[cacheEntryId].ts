import { Readable } from 'node:stream'
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

  try {
    await sendStream(event, Readable.toWeb(stream) as ReadableStream)
  } catch (err) {
    // Once the response has started flushing, we can't surface stream errors
    // as an HTTP error — Nitro's default error handler would call
    // `setResponseHeaders` after headers were already sent and crash with
    // ERR_HTTP_HEADERS_SENT (logged as an unhandled error). Client aborts on
    // long downloads are expected (cancelled jobs, parallel runners), so we
    // log and swallow once headers are out.
    if (event.node.res.headersSent) {
      if (event.node.req.destroyed)
        logger.debug(`Client aborted /download/${cacheEntryId}: ${(err as Error).message}`)
      else logger.error(`Download stream failed for ${cacheEntryId}`, { error: err })
      return
    }
    throw err
  }
})
