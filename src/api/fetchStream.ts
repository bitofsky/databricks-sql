import type { MergeFormat } from '@bitofsky/merge-streams'
import type {
  AuthInfo,
  ExternalLinkInfo,
  FetchStreamOptions,
  StatementManifest,
  StatementResult,
} from '../types.js'

import { PassThrough, Readable } from 'node:stream'

import { mergeStreamsFromUrls } from '@bitofsky/merge-streams'

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
  const { signal, forceMerge, logger } = options
  const manifest = validateSucceededResult(statementResult)
  const format = manifest.format as MergeFormat
  const statementId = statementResult.statement_id
  const baseLog = { statementId, manifest, format, forceMerge }

  if (statementResult.result?.data_array) {
    logger?.error?.(
      `fetchStream only supports EXTERNAL_LINKS results for statement ${statementId}.`,
      { ...baseLog, hasDataArray: true }
    )
    throw new DatabricksSqlError(
      'fetchStream only supports EXTERNAL_LINKS results',
      'UNSUPPORTED_FORMAT',
      statementId
    )
  }

  logger?.info?.(`fetchStream creating stream for statement ${statementId}.`, {
    ...baseLog,
    hasExternalLinks: Boolean(statementResult.result?.external_links?.length),
  })

  // Create PassThrough as output (readable by consumer)
  const output = new PassThrough()

  // Handle AbortSignal
  if (signal) {
    const onAbort = () => {
      logger?.info?.(`fetchStream abort signal received while streaming statement ${statementId}.`, baseLog)
      output.destroy(new AbortError('Stream aborted'))
    }
    signal.addEventListener('abort', onAbort, { once: true })
    output.once('close', () => signal.removeEventListener('abort', onAbort))
  }

  // Prevent AbortError from becoming an uncaught exception when no error handler is attached.
  output.on('error', (err) => {
    if (err instanceof AbortError)
      return
    if (output.listenerCount('error') === 1)
      throw err
  })

  // Start async merge process
  // Errors are forwarded to the stream consumer via destroy.
  mergeChunksToStream(statementResult, auth, manifest, format, output, signal, forceMerge, logger)
    .catch((err) => {
      logger?.error?.(`fetchStream error while streaming statement ${statementId}.`, {
        ...baseLog,
        error: err,
      })
      output.destroy(err as Error)
    })

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
  forceMerge?: boolean,
  logger?: FetchStreamOptions['logger']
): Promise<void> {
  const statementId = statementResult.statement_id
  const baseLog = { statementId, manifest, format, forceMerge }
  logger?.info?.(`fetchStream collecting external links for statement ${statementId}.`, baseLog)
  const urls = await collectExternalUrls(statementResult, auth, manifest, signal)

  // No external links - close the stream
  if (urls.length === 0) {
    logger?.info?.(`fetchStream no external links found for statement ${statementId}.`, baseLog)
    return void output.end()
  }

  // Single URL - pipe directly to output unless forcing merge
  if (urls.length === 1 && !forceMerge) {
    logger?.info?.(`fetchStream piping single external link for statement ${statementId}.`, {
      ...baseLog,
      urlCount: urls.length,
    })
    // Avoid merge-streams overhead for a single URL unless forced.
    return pipeUrlToOutput(urls[0]!, output, signal)
  }

  // Merge all URLs using merge-streams
  logger?.info?.(`fetchStream merging ${urls.length} external links for statement ${statementId}.`, {
    ...baseLog,
    urlCount: urls.length,
  })
  return mergeStreamsFromUrls(format, signal ? { urls, output, signal } : { urls, output })
}

async function collectExternalUrls(
  statementResult: StatementResult,
  auth: AuthInfo,
  manifest: StatementManifest,
  signal?: AbortSignal
): Promise<string[]> {
  const chunkUrls = new Map<number, string[]>()

  addChunkLinks(chunkUrls, statementResult.result?.external_links)

  if (!manifest.total_chunk_count)
    return flattenChunkUrls(chunkUrls)

  for (let i = 0; i < manifest.total_chunk_count; i++) {
    if (chunkUrls.has(i))
      continue
    if (signal?.aborted)
      throw new AbortError('Aborted while collecting URLs')

    // Chunk metadata contains external link URLs when results are chunked.
    const chunkData = await getChunk(auth, statementResult.statement_id, i, signal)
    addChunkLinks(chunkUrls, chunkData.external_links)
  }

  return flattenChunkUrls(chunkUrls)
}

function addChunkLinks(
  chunkUrls: Map<number, string[]>,
  externalLinks?: ExternalLinkInfo[]
): void {
  if (!externalLinks)
    return

  for (const link of externalLinks) {
    if (!isNonEmptyString(link.external_link))
      continue

    const existing = chunkUrls.get(link.chunk_index)
    if (existing) {
      existing.push(link.external_link)
    } else {
      chunkUrls.set(link.chunk_index, [link.external_link])
    }
  }
}

function flattenChunkUrls(chunkUrls: Map<number, string[]>): string[] {
  if (chunkUrls.size === 0)
    return []

  const sorted = [...chunkUrls.entries()].sort(([a], [b]) => a - b)
  const urls: string[] = []
  for (const [, links] of sorted) {
    urls.push(...links)
  }
  return urls
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0
}
