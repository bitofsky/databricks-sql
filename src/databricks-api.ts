import type {
  AuthInfo,
  ExecuteStatementRequest,
  StatementResult,
  GetChunkResponse,
  QueryInfo,
} from './types.js'
import { httpRequest } from './http.js'

// Base path for Databricks SQL Statement Execution API.
const BASE_PATH = '/api/2.0/sql/statements'
// Base path for Query History API.
const HISTORY_BASE_PATH = '/api/2.0/sql/history/queries'

/**
 * Execute SQL statement
 * POST /api/2.0/sql/statements
 */
export async function postStatement(
  auth: AuthInfo,
  request: ExecuteStatementRequest,
  signal?: AbortSignal
): Promise<StatementResult> {
  return httpRequest<StatementResult>(auth, {
    method: 'POST',
    path: BASE_PATH,
    body: request,
    ...(signal ? { signal } : {}),
  })
}

/**
 * Get statement status and result
 * GET /api/2.0/sql/statements/{statement_id}
 */
export async function getStatement(
  auth: AuthInfo,
  statementId: string,
  signal?: AbortSignal
): Promise<StatementResult> {
  return httpRequest<StatementResult>(auth, {
    method: 'GET',
    path: `${BASE_PATH}/${statementId}`,
    ...(signal ? { signal } : {}),
  })
}

/**
 * Cancel statement execution
 * POST /api/2.0/sql/statements/{statement_id}/cancel
 */
export async function cancelStatement(
  auth: AuthInfo,
  statementId: string,
  signal?: AbortSignal
): Promise<void> {
  await httpRequest<unknown>(auth, {
    method: 'POST',
    path: `${BASE_PATH}/${statementId}/cancel`,
    ...(signal ? { signal } : {}),
  })
}

/**
 * Get result chunk by index
 * GET /api/2.0/sql/statements/{statement_id}/result/chunks/{chunk_index}
 */
export async function getChunk(
  auth: AuthInfo,
  statementId: string,
  chunkIndex: number,
  signal?: AbortSignal
): Promise<GetChunkResponse> {
  return httpRequest<GetChunkResponse>(auth, {
    method: 'GET',
    path: `${BASE_PATH}/${statementId}/result/chunks/${chunkIndex}`,
    ...(signal ? { signal } : {}),
  })
}

/**
 * Get query metrics from Query History API
 * GET /api/2.0/sql/history/queries/{query_id}?include_metrics=true
 */
export async function getQueryMetrics(
  auth: AuthInfo,
  queryId: string,
  signal?: AbortSignal
): Promise<QueryInfo> {
  return httpRequest<QueryInfo>(auth, {
    method: 'GET',
    path: `${HISTORY_BASE_PATH}/${queryId}?include_metrics=true`,
    ...(signal ? { signal } : {}),
  })
}
