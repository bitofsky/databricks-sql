# @bitofsky/databricks-sql

[![npm version](https://img.shields.io/npm/v/@bitofsky/databricks-sql.svg)](https://www.npmjs.com/package/@bitofsky/databricks-sql)
[![npm downloads](https://img.shields.io/npm/dm/@bitofsky/databricks-sql.svg)](https://www.npmjs.com/package/@bitofsky/databricks-sql)
[![license](https://img.shields.io/npm/l/@bitofsky/databricks-sql.svg)](https://github.com/bitofsky/databricks-sql/blob/main/LICENSE)
[![node](https://img.shields.io/node/v/@bitofsky/databricks-sql.svg)](https://nodejs.org)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.9.3-blue.svg)](https://www.typescriptlang.org/)

Databricks SQL client for Node.js that talks directly to the REST API and streams large results efficiently. No SDK lock-in, no warehouse-side streaming bottlenecks.

## Why This Exists
I built this while working on an MCP Server that queries Databricks SQL for large datasets. The Databricks Node.js SDK does not support External Links, so you either roll your own REST client or give up on large results. That immediately created a new problem: dozens of presigned URLs per query, each with chunked data and format-specific quirks (CSV headers, JSON array brackets, Arrow EOS markers).

This project pairs with `@bitofsky/merge-streams` to solve that pain:
- Databricks returns N chunk URLs
- We merge them into one clean stream
- You upload once and return one URL to clients

The goal is simple: stream big results with stable memory usage and without forcing clients to juggle chunks.

## Highlights
- Direct REST calls to Statement Execution API.
- Optimized polling with server-side wait (up to 50s) before falling back to client polling.
- Query metrics support via Query History API (`enableMetrics` option).
- Efficient external link handling: merge chunks into a single stream.
- `mergeExternalLinks` supports streaming uploads and returns a new StatementResult with a presigned URL.
- `fetchRow`/`fetchAll` support `JSON_OBJECT` (schema-based row mapping).
- External links + JSON_ARRAY are supported for row iteration (streaming JSON parsing).

## Install
```bash
npm install @bitofsky/databricks-sql
```

## Sample (fetchAll)
```ts
import { executeStatement, fetchAll } from '@bitofsky/databricks-sql'

const auth = {
  token: process.env.DATABRICKS_TOKEN!,
  host: process.env.DATABRICKS_HOST!,
  httpPath: process.env.DATABRICKS_HTTP_PATH!,
}

const result = await executeStatement('SELECT 1 AS value', auth)
const rows = await fetchAll(result, auth, { format: 'JSON_OBJECT' })
console.log(rows) // [{ value: 1 }]
```

## Sample (Streaming + Presigned URL)
Stream external links into S3 with gzip compression, then return a single presigned URL:

```ts
import { executeStatement, mergeExternalLinks } from '@bitofsky/databricks-sql'
import { GetObjectCommand, HeadObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import { createGzip } from 'zlib'
import { pipeline } from 'stream/promises'
import { PassThrough } from 'stream'

const auth = {
  token: process.env.DATABRICKS_TOKEN!,
  host: process.env.DATABRICKS_HOST!,          // e.g. abc.cloud.databricks.com
  httpPath: process.env.DATABRICKS_HTTP_PATH!, // e.g. /sql/1.0/warehouses/...
}

const s3 = new S3Client({ region: process.env.AWS_REGION! })
const bucket = process.env.DATABRICKS_SQL_S3_BUCKET!

const result = await executeStatement(
  'SELECT * FROM samples.tpch.lineitem LIMIT 100000', // Large result
  auth,
  { disposition: 'EXTERNAL_LINKS', format: 'CSV' }
)

const merged = await mergeExternalLinks(result, auth, {
  mergeStreamToExternalLink: async (stream) => {
    const key = `merged-${Date.now()}.csv.gz`
    const gzip = createGzip() // Compress with gzip and upload to S3
    const passThrough = new PassThrough()

    const uploadPromise = s3.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: passThrough,
        ContentType: 'text/csv',
        ContentEncoding: 'gzip',
      })
    )

    await Promise.all([
      pipeline(stream, gzip, passThrough),
      uploadPromise,
    ])

    // Get actual uploaded size via HeadObject
    const head = await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }))
    // Generate presigned URL valid for 1 hour
    const externalLink = await getSignedUrl(s3, new GetObjectCommand({ Bucket: bucket, Key: key }),{ expiresIn: 3600 })

    return {
      externalLink, // Presigned URL to merged gzip CSV
      byte_count: head.ContentLength ?? 0, // Actual compressed size
      expiration: new Date(Date.now() + 3600 * 1000).toISOString(), // 1 hour from now
    }
  },
})

console.log(merged.result?.external_links?.[0].external_link) // Presigned URL to merged gzip CSV
console.log(merged.result?.external_links?.[0].byte_count)    // Actual compressed size
```

## Sample (Progress with Metrics)
Track query progress with execution metrics:

```ts
import { executeStatement } from '@bitofsky/databricks-sql'

const auth = {
  token: process.env.DATABRICKS_TOKEN!,
  host: process.env.DATABRICKS_HOST!,
  httpPath: process.env.DATABRICKS_HTTP_PATH!,
}

const result = await executeStatement(
  'SELECT * FROM samples.tpch.lineitem LIMIT 10000',
  auth,
  {
    enableMetrics: true,
    onProgress: (status, metrics) => {
      console.log(`State: ${status.state}`)
      if (metrics) {  // metrics is optional, only present when enableMetrics: true
        console.log(`  Execution time: ${metrics.execution_time_ms}ms`)
        console.log(`  Rows produced: ${metrics.rows_produced_count}`)
        console.log(`  Bytes read: ${metrics.read_bytes}`)
      }
    },
  }
)
```

## Sample (Abort)
Cancel a long-running query and stop polling/streaming:

```ts
import { executeStatement, fetchAll } from '@bitofsky/databricks-sql'

const auth = {
  token: process.env.DATABRICKS_TOKEN!,
  host: process.env.DATABRICKS_HOST!,
  httpPath: process.env.DATABRICKS_HTTP_PATH!,
}

const controller = new AbortController()
const timeout = setTimeout(() => controller.abort(), 5000)

try {
  const result = await executeStatement(
    'SELECT * FROM samples.tpch.lineitem',
    auth,
    { signal: controller.signal }
  )

  const rows = await fetchAll(result, auth, { signal: controller.signal })
  console.log(rows.length)
} finally {
  clearTimeout(timeout)
}
```

## API

### AuthInfo
```ts
type AuthInfo = {
  token: string
  host: string
  httpPath: string
}
```

### executeStatement(query, auth, options?)
```ts
function executeStatement(
  query: string,
  auth: AuthInfo,
  options?: ExecuteStatementOptions
): Promise<StatementResult>
```
- Calls the Databricks Statement Execution API and polls until completion.
- Server waits up to 50s (`wait_timeout`) before client-side polling begins.
- Use `options.onProgress` to receive status updates with optional metrics.
- Set `enableMetrics: true` to fetch query metrics from Query History API on each poll.
- Throws `DatabricksSqlError` on failure, `StatementCancelledError` on cancel, and `AbortError` on abort.

### fetchRow(statementResult, auth, options?)
```ts
function fetchRow(
  statementResult: StatementResult,
  auth: AuthInfo,
  options?: FetchRowsOptions
): Promise<void>
```
- Streams each row to `options.onEachRow`.
- Use `format: 'JSON_OBJECT'` to map rows into schema-based objects.
- Supports `INLINE` results or `JSON_ARRAY` formatted `EXTERNAL_LINKS` only.

### fetchAll(statementResult, auth, options?)
```ts
function fetchAll(
  statementResult: StatementResult,
  auth: AuthInfo,
  options?: FetchAllOptions
): Promise<Array<RowArray | RowObject>>
```
- Collects all rows into an array. For large results, prefer `fetchRow`/`fetchStream`.
- Supports `INLINE` results or `JSON_ARRAY` formatted `EXTERNAL_LINKS` only.

### fetchStream(statementResult, auth, options?)
```ts
function fetchStream(
  statementResult: StatementResult,
  auth: AuthInfo,
  options?: FetchStreamOptions
): Readable
```
- Merges `EXTERNAL_LINKS` into a single binary stream.
- Preserves the original format (`JSON_ARRAY`, `CSV`, `ARROW_STREAM`).
- Throws if the result is `INLINE`.
- Ends as an empty stream when no external links exist.
- `forceMerge: true` forces merge even when there is only a single external link.

### mergeExternalLinks(statementResult, auth, options)
```ts
function mergeExternalLinks(
  statementResult: StatementResult,
  auth: AuthInfo,
  options: MergeExternalLinksOptions
): Promise<StatementResult>
```
- Creates a merged stream from `EXTERNAL_LINKS`, uploads it via
  `options.mergeStreamToExternalLink`, then returns a `StatementResult`
  with a single external link.
- Returns the original result unchanged when input is `INLINE` or already a
  single external link (unless `forceMerge: true`).

### Options (Summary)
```ts
type ExecuteStatementOptions = {
  onProgress?: (status: StatementStatus, metrics?: QueryMetrics) => void
  enableMetrics?: boolean      // Fetch metrics from Query History API (default: false)
  signal?: AbortSignal
  disposition?: 'INLINE' | 'EXTERNAL_LINKS'
  format?: 'JSON_ARRAY' | 'ARROW_STREAM' | 'CSV'
  wait_timeout?: string        // Server wait time (default: '50s', max: '50s')
  row_limit?: number
  byte_limit?: number
  catalog?: string
  schema?: string
  parameters?: StatementParameter[]
  on_wait_timeout?: 'CONTINUE' | 'CANCEL'  // Default: 'CONTINUE'
  warehouse_id?: string
}

type FetchRowsOptions = {
  signal?: AbortSignal
  onEachRow?: (row: RowArray | RowObject) => void
  format?: 'JSON_ARRAY' | 'JSON_OBJECT'
}

type FetchAllOptions = {
  signal?: AbortSignal
  format?: 'JSON_ARRAY' | 'JSON_OBJECT'
}

type FetchStreamOptions = {
  signal?: AbortSignal
  forceMerge?: boolean
}

type MergeExternalLinksOptions = {
  signal?: AbortSignal
  forceMerge?: boolean
  mergeStreamToExternalLink: (stream: Readable) => Promise<{
    externalLink: string
    byte_count: number
    expiration: string
  }>
}
```

## Notes
- Databricks requires `INLINE` results to use `JSON_ARRAY` format. `INLINE + CSV` is rejected by the API.
- `EXTERNAL_LINKS` are merged using `@bitofsky/merge-streams`.
- Query metrics are fetched from `/api/2.0/sql/history/queries/{query_id}?include_metrics=true` when `enableMetrics: true`.
- Metrics may not be immediately available; `is_final: true` indicates complete metrics.
- Requires Node.js >= 20 for global `fetch` and Web streams.

## Development
```bash
npm run build:tsc
npm test
```

S3 integration tests require Databricks and AWS credentials in `.env`.
