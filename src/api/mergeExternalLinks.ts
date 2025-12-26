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
  const { signal, mergeStreamToExternalLink, forceMerge } = options

  // If not external links, return original as-is
  if (!statementResult.result?.external_links)
    return statementResult

  if (!forceMerge) {
    const totalChunks = statementResult.manifest?.total_chunk_count
    const externalLinks = statementResult.result.external_links
    const isSingleChunk = totalChunks === undefined ? externalLinks.length <= 1 : totalChunks <= 1

    // Skip merging when a single external link already exists unless forced.
    if (isSingleChunk && externalLinks.length <= 1)
      return statementResult
  }

  // Get merged stream via fetchStream
  const stream = fetchStream(statementResult, auth, {
    ...signal ? { signal } : {},
    ...forceMerge !== undefined ? { forceMerge } : {},
  })

  // Upload via callback
  const uploadResult = await mergeStreamToExternalLink(stream)

  // Build updated StatementResult
  // Manifest must exist for external links; validate before constructing new result.
  const manifest = validateSucceededResult(statementResult)
  const totalRowCount = manifest.total_row_count ?? 0

  return {
    statement_id: statementResult.statement_id,
    status: statementResult.status,
    manifest: {
      ...manifest,
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
