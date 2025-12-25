import { PassThrough, type Readable } from 'node:stream'
import { mergeStreamsFromUrls, type MergeFormat } from '@bitofsky/merge-streams'
import type {
  AuthInfo,
  StatementResult,
  FetchStreamOptions,
  StatementManifest,
} from '../types.js'
import { getChunk } from '../databricks-api.js'
import { AbortError } from '../errors.js'
import { validateSucceededResult } from '../util.js'

/**
 * Create a readable stream from statement result.
 * Merges all external link chunks into a single binary stream,
 * preserving the original format (JSON_ARRAY, CSV, ARROW_STREAM).
 */
export function fetchStream(
  statementResult: StatementResult,
  auth: AuthInfo,
  options: FetchStreamOptions = {}
): Readable {
  const { signal } = options
  const manifest = validateSucceededResult(statementResult)
  const format = manifest.format as MergeFormat

  // Create PassThrough as output (readable by consumer)
  const output = new PassThrough()

  // Handle AbortSignal
  if (signal) {
    const onAbort = () => {
      output.destroy(new AbortError('Stream aborted'))
    }
    signal.addEventListener('abort', onAbort, { once: true })
    output.once('close', () => {
      signal.removeEventListener('abort', onAbort)
    })
  }

  // Start async merge process
  // Errors are forwarded to the stream consumer via destroy.
  mergeChunksToStream(statementResult, auth, manifest, format, output, signal).catch(
    (err) => {
      output.destroy(err as Error)
    }
  )

  return output
}

/**
 * Collect all external link URLs and merge them into output stream
 */
async function mergeChunksToStream(
  statementResult: StatementResult,
  auth: AuthInfo,
  manifest: StatementManifest,
  format: MergeFormat,
  output: PassThrough,
  signal?: AbortSignal
): Promise<void> {
  const result = statementResult.result

  // Collect all external link URLs
  let urls = result?.external_links?.map((link) => link.external_link) ?? []

  // If no URLs in initial result, fetch from chunks
  if (urls.length === 0 && manifest.total_chunk_count > 0) {
    for (let i = 0; i < manifest.total_chunk_count; i++) {
      if (signal?.aborted) throw new AbortError('Aborted while collecting URLs')

      // Chunk metadata contains external link URLs when results are chunked.
      const chunkData = await getChunk(auth, statementResult.statement_id, i, signal)
      const chunkUrls = chunkData.external_links?.map((link) => link.external_link) ?? []
      urls.push(...chunkUrls)
    }
  }

  // No external links - close the stream
  if (urls.length === 0)
    return void output.end()

  // Merge all URLs using merge-streams
  await mergeStreamsFromUrls(format, signal ? { urls, output, signal } : { urls, output })
}
