import type {
  AuthInfo,
  MergeExternalLinksOptions,
  StatementResult,
} from '../types.js'

import { validateSucceededResult } from '../util.js'
import { fetchStream } from './fetchStream.js'

/**
 * Merge external links from StatementResult into a single stream,
 * upload it via the provided callback, and return updated StatementResult.
 *
 * If the result is not external links (inline data or empty), returns the original as-is.
 */
export async function mergeExternalLinks(
  statementResult: StatementResult,
  auth: AuthInfo,
  options: MergeExternalLinksOptions
): Promise<StatementResult> {
  const { signal, mergeStreamToExternalLink, forceMerge, logger } = options
  const statementId = statementResult.statement_id
  const manifest = statementResult.manifest
  const externalLinks = statementResult.result?.external_links
  const totalChunks = manifest?.total_chunk_count ?? 0
  const logContext = { statementId, manifest, totalChunks, forceMerge }

  // If not external links, return original as-is
  if (!externalLinks) {
    logger?.info?.(`mergeExternalLinks no external links to merge for statement ${statementId}.`, logContext)
    return statementResult
  }

  if (!forceMerge) {
    const isSingleChunk = totalChunks <= 1

    // Skip merging when a single external link already exists unless forced.
    if (isSingleChunk) {
      logger?.info?.(`mergeExternalLinks skipping merge for single external link in statement ${statementId}.`, {
        ...logContext,
        totalChunks,
      })
      return statementResult
    }
  }

  // Get merged stream via fetchStream
  logger?.info?.(`mergeExternalLinks merging external links for statement ${statementId}.`, logContext)
  const stream = fetchStream(statementResult, auth, {
    ...signal ? { signal } : {},
    ...forceMerge !== undefined ? { forceMerge } : {},
    ...logger ? { logger } : {},
  })

  // Upload via callback
  logger?.info?.(`mergeExternalLinks uploading merged external link for statement ${statementId}.`, logContext)
  const uploadResult = await mergeStreamToExternalLink(stream)
  logger?.info?.(`mergeExternalLinks uploaded merged external link for statement ${statementId}.`, {
    ...logContext,
    byteCount: uploadResult.byte_count,
    expiration: uploadResult.expiration,
  })

  // Build updated StatementResult
  // Manifest must exist for external links; validate before constructing new result.
  const validatedManifest = validateSucceededResult(statementResult)
  const totalRowCount = validatedManifest.total_row_count ?? 0

  return {
    statement_id: statementResult.statement_id,
    status: statementResult.status,
    manifest: {
      ...validatedManifest,
      total_chunk_count: 1,
      total_byte_count: uploadResult.byte_count,
      chunks: [
        {
          chunk_index: 0,
          row_offset: 0,
          row_count: totalRowCount,
          byte_count: uploadResult.byte_count,
        },
      ],
    },
    result: {
      external_links: [
        {
          chunk_index: 0,
          row_offset: 0,
          row_count: totalRowCount,
          byte_count: uploadResult.byte_count,
          external_link: uploadResult.externalLink,
          expiration: uploadResult.expiration,
        },
      ],
    },
  }
}
