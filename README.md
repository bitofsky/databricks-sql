# @bitofsky/databricks-sql

[![npm version](https://img.shields.io/npm/v/@bitofsky/merge-streams.svg)](https://www.npmjs.com/package/@bitofsky/merge-streams)
[![npm downloads](https://img.shields.io/npm/dm/@bitofsky/merge-streams.svg)](https://www.npmjs.com/package/@bitofsky/merge-streams)
[![license](https://img.shields.io/npm/l/@bitofsky/merge-streams.svg)](https://github.com/bitofsky/merge-streams/blob/main/LICENSE)
[![node](https://img.shields.io/node/v/@bitofsky/merge-streams.svg)](https://nodejs.org)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.0-blue.svg)](https://www.typescriptlang.org/)

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
- Polls statement execution until completion.
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
Stream external links into S3, then return a single presigned URL:

```ts
import { executeStatement, mergeExternalLinks } from '@bitofsky/databricks-sql'
import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'

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
    const key = `merged-${Date.now()}.csv`
    await s3.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: stream,
        ContentType: 'text/csv',
      })
    )

    const externalLink = await getSignedUrl(
      s3,
      new GetObjectCommand({ Bucket: bucket, Key: key }),
      { expiresIn: 3600 }
    )

    return {
      externalLink,
      byte_count: 0,
      expiration: new Date(Date.now() + 3600 * 1000).toISOString(),
    }
  },
})

console.log(merged.result?.external_links?.[0].external_link) // Presigned URL to merged CSV
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
- Use `options.onProgress` to receive status updates.
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
- Ends as an empty stream when no external links exist.

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
- Returns the original result unchanged when input is `INLINE`.

### Options (Summary)
```ts
type ExecuteStatementOptions = {
  onProgress?: (status: StatementStatus) => void
  signal?: AbortSignal
  disposition?: 'INLINE' | 'EXTERNAL_LINKS'
  format?: 'JSON_ARRAY' | 'ARROW_STREAM' | 'CSV'
  wait_timeout?: string
  row_limit?: number
  byte_limit?: number
  catalog?: string
  schema?: string
  parameters?: StatementParameter[]
  on_wait_timeout?: 'CONTINUE' | 'CANCEL'
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
}

type MergeExternalLinksOptions = {
  signal?: AbortSignal
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

## Development
```bash
npm run build:tsc
npm test
```

S3 integration tests require Databricks and AWS credentials in `.env`.
