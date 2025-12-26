import type { Readable } from 'node:stream'

/** Authentication information for Databricks API */
export type AuthInfo = {
  /** Databricks Personal Access Token */
  token: string
  /** Databricks workspace host (e.g., ...cloud.databricks.com) */
  host: string
  /** SQL warehouse HTTP path (e.g., /sql/1.0/warehouses/abc123) */
  httpPath: string
}

/** Statement execution states */
export type StatementState =
  | 'PENDING'
  | 'RUNNING'
  | 'SUCCEEDED'
  | 'FAILED'
  | 'CANCELED'
  | 'CLOSED'

/** Statement status */
export type StatementStatus = {
  state: StatementState
  error?: {
    error_code: string
    message: string
  }
}

/** Column schema information */
export type ColumnInfo = {
  name: string
  type_text: string
  type_name: string
  position: number
  type_precision?: number
  type_scale?: number
}

/** Chunk information */
export type ChunkInfo = {
  chunk_index: number
  row_offset: number
  row_count: number
  byte_count?: number
}

/** Result manifest (schema and chunk info) */
export type StatementManifest = {
  format: 'JSON_ARRAY' | 'ARROW_STREAM' | 'CSV'
  schema: {
    column_count: number
    columns: ColumnInfo[]
  }
  total_chunk_count: number
  total_row_count?: number
  total_byte_count?: number
  truncated?: boolean
  chunks?: ChunkInfo[]
}

/** External link for chunked results */
export type ExternalLinkInfo = {
  chunk_index: number
  row_offset: number
  row_count: number
  byte_count: number
  external_link: string
  expiration: string
}

/** Inline result data */
export type InlineResultData = {
  data_array?: unknown[][]
  external_links?: never
}

/** External links result data */
export type ExternalLinksResultData = {
  data_array?: never
  external_links?: ExternalLinkInfo[]
}

/** Result data (inline or external links) */
export type ResultData = InlineResultData | ExternalLinksResultData

/**
 * Statement result from API
 * @see https://docs.databricks.com/api/workspace/statementexecution/getstatement
 */
export type StatementResult = {
  statement_id: string
  status: StatementStatus
  manifest?: StatementManifest
  result?: ResultData
}

/** Statement parameter */
export type StatementParameter = {
  name: string
  type?: 'STRING' | 'LONG' | 'DOUBLE' | 'BOOLEAN'
  value?: string | number | boolean
}

/**
 * Options for executeStatement
 * @see https://docs.databricks.com/api/workspace/statementexecution/executestatement
 */
export type ExecuteStatementOptions = {
  /** Progress callback (called on each poll) */
  onProgress?: (status: StatementStatus) => void
  /** Abort signal for cancellation */
  signal?: AbortSignal
  /** Result byte limit */
  byte_limit?: number
  /** Catalog name */
  catalog?: string
  /** Result disposition */
  disposition?: 'INLINE' | 'EXTERNAL_LINKS'
  /** Result format */
  format?: 'JSON_ARRAY' | 'ARROW_STREAM' | 'CSV'
  /** Behavior on wait timeout */
  on_wait_timeout?: 'CONTINUE' | 'CANCEL'
  /** Query parameters */
  parameters?: StatementParameter[]
  /** Row limit */
  row_limit?: number
  /** Schema name */
  schema?: string
  /** Server wait timeout (e.g., '10s', '50s') */
  wait_timeout?: string
  /** Warehouse ID (can be extracted from httpPath) */
  warehouse_id?: string
}

/** Base options with abort signal support */
export type SignalOptions = {
  /** Abort signal for cancellation */
  signal?: AbortSignal
}

/** Row data as array */
export type RowArray = unknown[]

/** Row data as JSON object */
export type RowObject = Record<string, unknown>

/** Format for fetchRow/fetchAll */
export type FetchRowFormat = 'JSON_ARRAY' | 'JSON_OBJECT'

/** Options for fetchStream */
export type FetchStreamOptions = SignalOptions & {
  /** Force merge even when there is only a single external link */
  forceMerge?: boolean
}

/** Options for fetchRow */
export type FetchRowsOptions = SignalOptions & {
  /** Callback for each row */
  onEachRow?: (row: RowArray | RowObject) => void
  /** Row format (default: JSON_ARRAY) */
  format?: FetchRowFormat
}

/** Options for fetchAll */
export type FetchAllOptions = SignalOptions & {
  /** Row format (default: JSON_ARRAY) */
  format?: FetchRowFormat
}

/** Result from mergeStreamToExternalLink callback */
export type MergeExternalLinksResult = {
  /** Uploaded external link URL */
  externalLink: string
  /** Uploaded byte count (actual size after compression like gzip) */
  byte_count: number
  /** Link expiration time (ISO string) */
  expiration: string
}

/** Options for mergeExternalLinks */
export type MergeExternalLinksOptions = SignalOptions & {
  /** Callback to upload merged stream to external link */
  mergeStreamToExternalLink: (stream: Readable) => Promise<MergeExternalLinksResult>
  /** Force merge even when there is only a single external link chunk */
  forceMerge?: boolean
}

/**
 * API request for executeStatement
 * @see https://docs.databricks.com/api/workspace/statementexecution/executestatement
 */
export type ExecuteStatementRequest = {
  warehouse_id: string
  statement: string
  byte_limit?: number
  catalog?: string
  disposition?: 'INLINE' | 'EXTERNAL_LINKS'
  format?: 'JSON_ARRAY' | 'ARROW_STREAM' | 'CSV'
  on_wait_timeout?: 'CONTINUE' | 'CANCEL'
  parameters?: StatementParameter[]
  row_limit?: number
  schema?: string
  wait_timeout?: string
}

/**
 * API response for getChunk
 * @see https://docs.databricks.com/api/workspace/statementexecution/getstatementresultchunkn
 */
export type GetChunkResponse = {
  chunk_index: number
  row_offset: number
  row_count: number
  data_array?: unknown[][]
  external_links?: ExternalLinkInfo[]
  next_chunk_index?: number
  next_chunk_internal_link?: string
}
