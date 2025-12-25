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
  const fetchOptions: FetchRowsOptions = {
    // Collect rows as they are streamed in.
    onEachRow: (row) => {
      rows.push(row)
    },
  }

  if (options.signal)
    fetchOptions.signal = options.signal

  if (options.format)
    fetchOptions.format = options.format

  await fetchRow(statementResult, auth, fetchOptions)
  return rows
}
