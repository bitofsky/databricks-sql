/** Base error for Databricks SQL operations */
export class DatabricksSqlError extends Error {
  readonly code: string
  readonly statementId: string | undefined

  constructor(message: string, code?: string, statementId?: string) {
    super(message)
    this.name = 'DatabricksSqlError'
    this.code = code ?? 'UNKNOWN_ERROR'
    this.statementId = statementId
    Error.captureStackTrace?.(this, DatabricksSqlError)
  }
}

/** Error when statement is cancelled */
export class StatementCancelledError extends DatabricksSqlError {
  constructor(statementId: string) {
    super(`Statement ${statementId} was cancelled`, 'CANCELLED', statementId)
    this.name = 'StatementCancelledError'
  }
}

/** Error when operation is aborted via AbortSignal */
export class AbortError extends DatabricksSqlError {
  constructor(message: string = 'Operation was aborted') {
    super(message, 'ABORTED')
    this.name = 'AbortError'
  }
}

/** HTTP error from API calls */
export class HttpError extends DatabricksSqlError {
  readonly status: number
  readonly statusText: string

  constructor(status: number, statusText: string, message?: string) {
    super(message ?? `HTTP ${status}: ${statusText}`, `HTTP_${status}`)
    this.name = 'HttpError'
    this.status = status
    this.statusText = statusText
  }
}

/** Authentication error (401) */
export class AuthenticationError extends HttpError {
  constructor() {
    super(401, 'Unauthorized', 'Authentication failed. Check your token.')
    this.name = 'AuthenticationError'
  }
}

/** Rate limit error (429) */
export class RateLimitError extends HttpError {
  readonly retryAfter: number | undefined

  constructor(retryAfter?: number) {
    super(429, 'Too Many Requests', 'Rate limit exceeded')
    this.name = 'RateLimitError'
    this.retryAfter = retryAfter
  }
}
