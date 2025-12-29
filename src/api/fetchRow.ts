import type { Readable } from 'node:stream'
import type {
  AuthInfo,
  FetchRowsOptions,
  RowArray,
  RowObject,
  StatementResult,
} from '../types.js'

import { parser } from 'stream-json'
import { streamArray } from 'stream-json/streamers/StreamArray'

import { getChunk } from '../databricks-api.js'
import { createRowMapper } from '../createRowMapper.js'
import { AbortError, DatabricksSqlError } from '../errors.js'
import { validateSucceededResult } from '../util.js'
import { fetchStream } from './fetchStream.js'

/**
 * Process each row from statement result with a callback.
 * Supports INLINE results and JSON_ARRAY external links.
 */
export async function fetchRow(
  statementResult: StatementResult,
  auth: AuthInfo,
  options: FetchRowsOptions = {}
): Promise<void> {
  const { signal, onEachRow, format, logger } = options
  const manifest = validateSucceededResult(statementResult)
  const statementId = statementResult.statement_id
  const logContext = { statementId, manifest, requestedFormat: format }
  // Map JSON_ARRAY rows to JSON_OBJECT when requested.
  const mapRow = createRowMapper(manifest, format)

  logger?.info?.(`fetchRow fetching rows for statement ${statementId}.`, {
    ...logContext,
    resultType: statementResult.result?.external_links ? 'EXTERNAL_LINKS' : 'INLINE',
  })

  if (statementResult.result?.external_links) {
    if (manifest.format !== 'JSON_ARRAY') {
      logger?.error?.(`fetchRow only supports JSON_ARRAY for external_links; got ${manifest.format}.`, logContext)
      throw new DatabricksSqlError(
        `fetchRow only supports JSON_ARRAY for external_links. Received: ${manifest.format}`,
        'UNSUPPORTED_FORMAT',
        statementId
      )
    }

    logger?.info?.(`fetchRow streaming external links for statement ${statementId}.`, logContext)
    const stream = fetchStream(statementResult, auth, {
      ...signal ? { signal } : {},
      ...logger ? { logger } : {},
    })
    await consumeJsonArrayStream(stream, mapRow, onEachRow, signal, logger, logContext)
    return
  }

  const totalChunks = manifest.total_chunk_count

  // Process first chunk (inline data_array)
  const dataArray = statementResult.result?.data_array
  if (dataArray) {
    logger?.info?.(`fetchRow processing inline rows for statement ${statementId}.`, {
      ...logContext,
      inlineRows: dataArray.length,
    })
    for (const row of dataArray) {
      if (signal?.aborted) throw new AbortError('Aborted')
      // Convert row to requested shape before callback.
      onEachRow?.(mapRow(row as RowArray))
    }
  }

  // Process additional chunks if any
  if (totalChunks > 1) {
    logger?.info?.(`fetchRow processing ${totalChunks} chunks for statement ${statementId}.`, logContext)
    for (let chunkIndex = 1; chunkIndex < totalChunks; chunkIndex++) {
      if (signal?.aborted) throw new AbortError('Aborted')

      const chunk = await getChunk(auth, statementId, chunkIndex, signal)

      // Additional chunks should also be data_array (INLINE)
      if (chunk.external_links)
        throw new DatabricksSqlError(
          'fetchRow only supports INLINE results. Chunk contains external_links.',
          'UNSUPPORTED_FORMAT',
          statementId
        )

      if (chunk.data_array) {
        for (const row of chunk.data_array) {
          if (signal?.aborted) throw new AbortError('Aborted')
          // Apply the same mapping for each chunked row.
          onEachRow?.(mapRow(row as RowArray))
        }
      }
    }
  }
}

async function consumeJsonArrayStream(
  stream: Readable,
  mapRow: (row: RowArray) => RowArray | RowObject,
  onEachRow: ((row: RowArray | RowObject) => void) | undefined,
  signal: AbortSignal | undefined,
  logger: FetchRowsOptions['logger'],
  logContext: Record<string, unknown>
): Promise<void> {
  // Stream JSON_ARRAY as individual rows to avoid buffering whole payloads.
  const jsonStream = stream.pipe(parser()).pipe(streamArray())

  for await (const item of jsonStream) {
    if (signal?.aborted) {
      logger?.info?.('fetchRow abort detected while streaming JSON_ARRAY rows.', {
        ...logContext,
        aborted: signal.aborted,
      })
      stream.destroy(new AbortError('Aborted'))
      throw new AbortError('Aborted')
    }

    const row = item.value
    if (!Array.isArray(row)) {
      throw new DatabricksSqlError(
        'Expected JSON_ARRAY rows to be arrays',
        'INVALID_FORMAT'
      )
    }

    onEachRow?.(mapRow(row))
  }
}
