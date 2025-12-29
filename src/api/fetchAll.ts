import type {
  AuthInfo,
  FetchAllOptions,
  FetchRowsOptions,
  RowArray,
  RowObject,
  StatementResult,
} from '../types.js'

import { fetchRow } from './fetchRow.js'

/**
 * Fetch all rows from statement result as an array.
 * Only supports INLINE results or JSON_ARRAY external links.
 */
export async function fetchAll(
  statementResult: StatementResult,
  auth: AuthInfo,
  options: FetchAllOptions = {}
): Promise<Array<RowArray | RowObject>> {
  const rows: Array<RowArray | RowObject> = []
  const statementId = statementResult.statement_id
  const manifest = statementResult.manifest
  const logContext = { statementId, manifest, requestedFormat: options.format }
  const fetchOptions: FetchRowsOptions = {
    // Collect rows as they are streamed in.
    onEachRow: (row) => {
      rows.push(row)
    },
  }
  const { logger } = options

  logger?.info?.(`fetchAll fetching all rows for statement ${statementId}.`, logContext)

  if (options.signal)
    fetchOptions.signal = options.signal

  if (options.format)
    fetchOptions.format = options.format

  if (options.logger)
    fetchOptions.logger = options.logger

  await fetchRow(statementResult, auth, fetchOptions)
  logger?.info?.(`fetchAll fetched ${rows.length} rows for statement ${statementId}.`, {
    ...logContext,
    rowCount: rows.length,
    resolvedFormat: options.format ?? manifest?.format,
  })
  return rows
}
