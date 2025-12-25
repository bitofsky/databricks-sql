import type {
  AuthInfo,
  ExecuteStatementOptions,
  ExecuteStatementRequest,
  StatementResult,
  StatementState,
} from '../types.js'
import { postStatement, getStatement, cancelStatement } from '../databricks-api.js'
import { extractWarehouseId, throwIfAborted, delay } from '../util.js'
import {
  DatabricksSqlError,
  StatementCancelledError,
  AbortError,
} from '../errors.js'

const TERMINAL_STATES = new Set<StatementState>([
  'SUCCEEDED',
  'FAILED',
  'CANCELED',
  'CLOSED',
])
const POLL_INTERVAL_MS = 500
const MAX_POLL_INTERVAL_MS = 5000

/**
 * Execute SQL statement and poll until completion
 */
export async function executeStatement(
  query: string,
  auth: AuthInfo,
  options: ExecuteStatementOptions = {}
): Promise<StatementResult> {
  const warehouseId = options.warehouse_id ?? extractWarehouseId(auth.httpPath)
  const { signal, onProgress } = options

  // Check if already aborted
  throwIfAborted(signal, 'executeStatement')

  // 1. Build request (filter out undefined values)
  // Keep payload small and aligned with the REST API contract.
  const request = Object.fromEntries(
    Object.entries({
      warehouse_id: warehouseId,
      statement: query,
      byte_limit: options.byte_limit,
      disposition: options.disposition,
      format: options.format,
      on_wait_timeout: options.on_wait_timeout,
      wait_timeout: options.wait_timeout,
      row_limit: options.row_limit,
      catalog: options.catalog,
      schema: options.schema,
      parameters: options.parameters,
    }).filter(([, v]) => v !== undefined)
  ) as ExecuteStatementRequest

  // 2. Submit statement execution request
  let result = await postStatement(auth, request, signal)

  // 3. Poll until terminal state
  let pollInterval = POLL_INTERVAL_MS

  while (!TERMINAL_STATES.has(result.status.state)) {
    // Check abort signal
    if (signal?.aborted) {
      // Try to cancel on server
      await cancelStatement(auth, result.statement_id).catch(() => {
        // Ignore cancel errors
      })
      throw new AbortError('Aborted during polling')
    }

    // Call progress callback
    onProgress?.(result.status)

    // Wait before next poll (exponential backoff)
    await delay(pollInterval, signal)
    pollInterval = Math.min(pollInterval * 1.5, MAX_POLL_INTERVAL_MS)

    // Get current status
    result = await getStatement(auth, result.statement_id, signal)
  }

  // 4. Final progress callback
  onProgress?.(result.status)

  // 5. Handle terminal states
  if (result.status.state === 'SUCCEEDED')
    return result

  if (result.status.state === 'CANCELED')
    throw new StatementCancelledError(result.statement_id)

  // FAILED or CLOSED
  throw new DatabricksSqlError(
    result.status.error?.message ?? 'Statement execution failed',
    result.status.error?.error_code,
    result.statement_id
  )
}
