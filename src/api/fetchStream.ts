import type {
  AuthInfo,
  ExternalLinkInfo,
  StatementResult,
  FetchStreamOptions,
  StatementManifest,
} from '../types.js'
import { PassThrough, Readable } from 'node:stream'
import { mergeStreamsFromUrls, type MergeFormat } from '@bitofsky/merge-streams'
import { getChunk } from '../databricks-api.js'
import { AbortError, DatabricksSqlError } from '../errors.js'
import { pipeUrlToOutput, validateSucceededResult } from '../util.js'

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
  const { signal, forceMerge } = options
  const manifest = validateSucceededResult(statementResult)
  const format = manifest.format as MergeFormat

  if (statementResult.result?.data_array) {
    throw new DatabricksSqlError(
      'fetchStream only supports EXTERNAL_LINKS results',
      'UNSUPPORTED_FORMAT',
      statementResult.statement_id
    )
  }

  // Create PassThrough as output (readable by consumer)
  const output = new PassThrough()

  // Handle AbortSignal
  if (signal) {
    const onAbort = () => output.destroy(new AbortError('Stream aborted'))
    signal.addEventListener('abort', onAbort, { once: true })
    output.once('close', () => signal.removeEventListener('abort', onAbort))
  }

  // Start async merge process
  // Errors are forwarded to the stream consumer via destroy.
  mergeChunksToStream(statementResult, auth, manifest, format, output, signal, forceMerge)
    .catch((err) => output.destroy(err as Error))

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
  signal?: AbortSignal,
  forceMerge?: boolean
): Promise<void> {
  const urls = await collectExternalUrls(statementResult, auth, manifest, signal)

  // No external links - close the stream
  if (urls.length === 0)
    return void output.end()

  // Single URL - pipe directly to output unless forcing merge
  if (urls.length === 1 && !forceMerge)
    // Avoid merge-streams overhead for a single URL unless forced.
    return pipeUrlToOutput(urls[0]!, output, signal)

  // Merge all URLs using merge-streams
  return mergeStreamsFromUrls(format, signal ? { urls, output, signal } : { urls, output })
}

async function collectExternalUrls(
  statementResult: StatementResult,
  auth: AuthInfo,
  manifest: StatementManifest,
  signal?: AbortSignal
): Promise<string[]> {
  const urls = extractExternalLinks(statementResult.result?.external_links)
  if (urls.length > 0)
    return urls

  if (!manifest.total_chunk_count)
    return []

  const chunkUrls: string[] = []
  for (let i = 0; i < manifest.total_chunk_count; i++) {
    if (signal?.aborted)
      throw new AbortError('Aborted while collecting URLs')

    // Chunk metadata contains external link URLs when results are chunked.
    const chunkData = await getChunk(auth, statementResult.statement_id, i, signal)
    chunkUrls.push(...extractExternalLinks(chunkData.external_links))
  }

  return chunkUrls
}

function extractExternalLinks(externalLinks?: ExternalLinkInfo[]): string[] {
  if (!externalLinks)
    return []

  return externalLinks
    .map((link) => link.external_link)
    .filter(isNonEmptyString)
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0
}
