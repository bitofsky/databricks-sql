import type {
  AuthInfo,
  ExecuteStatementOptions,
  ExecuteStatementRequest,
  StatementResult,
  StatementState,
  QueryMetrics,
} from '../types.js'
import { postStatement, getStatement, cancelStatement, getQueryMetrics } from '../databricks-api.js'
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
const POLL_INTERVAL_MS = 5000

async function fetchMetrics(
  auth: AuthInfo,
  statementId: string,
  signal?: AbortSignal
): Promise<QueryMetrics | undefined> {
  try {
    const queryInfo = await getQueryMetrics(auth, statementId, signal)
    return queryInfo.metrics
  } catch {
    // Ignore metrics fetch errors - non-critical
    return undefined
  }
}

/**
 * Execute SQL statement and poll until completion
 */
export async function executeStatement(
  query: string,
  auth: AuthInfo,
  options: ExecuteStatementOptions = {}
): Promise<StatementResult> {
  const warehouseId = options.warehouse_id ?? extractWarehouseId(auth.httpPath)
  const { signal, onProgress, enableMetrics, logger } = options
  const waitTimeout = options.wait_timeout ?? (onProgress ? '0s' : '50s')
  let cancelIssued = false

  // Check if already aborted
  throwIfAborted(signal, 'executeStatement')

  // Helper to call onProgress with optional metrics
  const emitProgress = onProgress
    ? async (statementId: string) => onProgress(result, enableMetrics ? await fetchMetrics(auth, statementId, signal) : undefined)
    : undefined

  // 1. Build request (filter out undefined values)
  const request = Object.fromEntries(
    Object.entries({
      warehouse_id: warehouseId,
      statement: query,
      byte_limit: options.byte_limit,
      disposition: options.disposition,
      format: options.format,
      on_wait_timeout: options.on_wait_timeout ?? 'CONTINUE',
      wait_timeout: waitTimeout,
      row_limit: options.row_limit,
      catalog: options.catalog,
      schema: options.schema,
      parameters: options.parameters,
    }).filter(([, v]) => v !== undefined)
  ) as ExecuteStatementRequest

  logger?.info?.(`executeStatement Executing statement on warehouse ${warehouseId}...`)

  // 2. Submit statement execution request
  let result = await postStatement(auth, request, signal)
  const cancelStatementSafely = async () => {
    if (cancelIssued) return
    logger?.info?.('executeStatement Abort signal received during executeStatement.')
    cancelIssued = true
    await cancelStatement(auth, result.statement_id).catch((err) => {
      logger?.error?.('executeStatement Failed to cancel statement after abort.', err)
    })
  }

  if (signal?.aborted) {
    await cancelStatementSafely()
    throw new AbortError('Aborted during polling')
  }

  const onAbort = () => cancelStatementSafely().catch(() => { })

  try {
    signal?.addEventListener('abort', onAbort, { once: true })

    // 3. Poll until terminal state
    while (!TERMINAL_STATES.has(result.status.state)) {
      logger?.info?.(`executeStatement Statement ${result.statement_id} in state ${result.status.state}; polling for status...`)
      await emitProgress?.(result.statement_id)
      await delay(POLL_INTERVAL_MS, signal)
      result = await getStatement(auth, result.statement_id, signal)
    }
  } catch (err) {
    if (err instanceof AbortError || signal?.aborted) {
      logger?.info?.('executeStatement Abort detected in executeStatement polling loop.')
      await cancelStatementSafely()
      throw new AbortError('Aborted during polling')
    }
    logger?.error?.(`executeStatement Error during executeStatement polling: ${String(err)}`)
    throw err
  } finally {
    logger?.info?.(`executeStatement Statement ${result.statement_id} reached final state: ${result.status.state}`)
    signal?.removeEventListener('abort', onAbort)
  }

  // 4. Final progress callback
  await emitProgress?.(result.statement_id)

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
