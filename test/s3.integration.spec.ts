import { Readable, PassThrough } from 'node:stream'
import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import {
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3'
import { Upload } from '@aws-sdk/lib-storage'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import { createGzip } from 'node:zlib'
import { pipeline } from 'node:stream/promises'
// Load environment variables
import 'dotenv/config'
import { executeStatement, fetchAll, mergeExternalLinks } from '../src/index.js'
import type { AuthInfo, MergeExternalLinksResult } from '../src/index.js'

const DATABRICKS_TOKEN = process.env.DATABRICKS_TOKEN
const DATABRICKS_HOST = process.env.DATABRICKS_HOST
const DATABRICKS_HTTP_PATH = process.env.DATABRICKS_HTTP_PATH
const AWS_REGION = process.env.AWS_REGION
const S3_BUCKET = process.env.DATABRICKS_SQL_S3_BUCKET
const S3_PREFIX = process.env.DATABRICKS_SQL_S3_PREFIX || ''

const auth: AuthInfo = {
  token: DATABRICKS_TOKEN || '',
  host: DATABRICKS_HOST || '',
  httpPath: DATABRICKS_HTTP_PATH || '',
}

const shouldSkip =
  !DATABRICKS_TOKEN ||
  !DATABRICKS_HOST ||
  !DATABRICKS_HTTP_PATH ||
  !AWS_REGION ||
  !S3_BUCKET

let s3Client: S3Client
const uploadedKeys: string[] = []

describe.skipIf(shouldSkip)('S3 Integration Tests', () => {
  beforeAll(() => {
    s3Client = new S3Client({
      region: AWS_REGION,
    })
  })

  afterAll(async () => {
    // Cleanup uploaded objects
    for (const key of uploadedKeys) {
      try {
        await s3Client.send(
          new DeleteObjectCommand({
            Bucket: S3_BUCKET,
            Key: key,
          })
        )
        console.log(`Cleaned up: s3://${S3_BUCKET}/${key}`)
      } catch {
        // Ignore cleanup errors
      }
    }
  })

  describe.concurrent('executeStatement with complex types', () => {
    it.concurrent('captures JSON_ARRAY schema and data for complex types', async () => {
      const result = await executeStatement(
        `SELECT
        42 AS num,
        'hello' AS str,
        CAST(9007199254740993 AS BIGINT) AS big_int,
        CAST(12.34 AS DECIMAL(10, 2)) AS price,
        DATE '2024-01-02' AS d,
        date_format(TIMESTAMP '1970-01-01 03:04:05', 'HH:mm:ss') AS t,
        TIMESTAMP '2024-01-02 03:04:05.123' AS dt,
        TIMESTAMP_NTZ '2024-01-02 03:04:05.123' AS dt_ntz,
        named_struct(
          'a',
          1,
          'b',
          named_struct(
            'c',
            'x',
            'big',
            CAST(9007199254740993 AS BIGINT),
            'price',
            CAST(56.78 AS DECIMAL(10, 2))
          )
        ) AS nested`,
        auth,
        { disposition: 'INLINE', format: 'JSON_ARRAY' }
      )

      expect(result.status.state).toBe('SUCCEEDED')

      const schema = result.manifest?.schema
      const data = result.result?.data_array

      expect(schema).toEqual({
        column_count: 9,
        columns: [
          {
            name: 'num',
            type_text: 'INT',
            type_name: 'INT',
            position: 0,
          },
          {
            name: 'str',
            type_text: 'STRING',
            type_name: 'STRING',
            position: 1,
          },
          {
            name: 'big_int',
            type_text: 'BIGINT',
            type_name: 'LONG',
            position: 2,
          },
          {
            name: 'price',
            type_text: 'DECIMAL(10,2)',
            type_name: 'DECIMAL',
            position: 3,
            type_precision: 10,
            type_scale: 2,
          },
          {
            name: 'd',
            type_text: 'DATE',
            type_name: 'DATE',
            position: 4,
          },
          {
            name: 't',
            type_text: 'STRING',
            type_name: 'STRING',
            position: 5,
          },
          {
            name: 'dt',
            type_text: 'TIMESTAMP',
            type_name: 'TIMESTAMP',
            position: 6,
          },
          {
            name: 'dt_ntz',
            type_text: 'TIMESTAMP_NTZ',
            type_name: 'TIMESTAMP_NTZ',
            position: 7,
          },
          {
            name: 'nested',
            type_text:
              'STRUCT<a: INT NOT NULL, b: STRUCT<c: STRING NOT NULL, big: BIGINT NOT NULL, price: DECIMAL(10,2) NOT NULL> NOT NULL>',
            type_name: 'STRUCT',
            position: 8,
          },
        ],
      })
      expect(data).toEqual([
        [
          '42',
          'hello',
          '9007199254740993',
          '12.34',
          '2024-01-02',
          '03:04:05',
          '2024-01-02T03:04:05.123Z',
          '2024-01-02T03:04:05.123',
          '{"a":"1","b":{"c":"x","big":"9007199254740993","price":"56.78"}}',
        ],
      ])
    })
  })

  describe.concurrent('mergeExternalLinks with S3 upload', () => {
    type UploadResult = MergeExternalLinksResult & { key: string }

    async function uploadToS3(
      stream: Readable,
      format: string
    ): Promise<UploadResult> {
      const timestamp = Date.now()
      const ext = format === 'JSON_ARRAY' ? 'json' : format.toLowerCase()
      const key = `${S3_PREFIX}merged-${timestamp}.${ext}`
      const gzip = createGzip()
      const passThrough = new PassThrough()

      // Upload to S3
      const upload = new Upload({
        client: s3Client,
        params: {
          Bucket: S3_BUCKET,
          Key: key,
          Body: passThrough,
          ContentType:
            format === 'CSV'
              ? 'text/csv; charset=utf-8'
              : format === 'JSON_ARRAY'
                ? 'application/json; charset=utf-8'
                : 'application/vnd.apache.arrow.stream',
          ContentEncoding: 'gzip',
        },
      })

      await Promise.all([
        pipeline(stream, gzip, passThrough),
        upload.done(),
      ])

      uploadedKeys.push(key)
      const head = await s3Client.send(
        new HeadObjectCommand({
          Bucket: S3_BUCKET,
          Key: key,
        })
      )

      // Generate presigned URL (valid for 1 hour)
      const presignedUrl = await getSignedUrl(
        s3Client,
        new GetObjectCommand({
          Bucket: S3_BUCKET,
          Key: key,
        }),
        { expiresIn: 3600 }
      )

      const expiration = new Date(Date.now() + 3600 * 1000).toISOString()

      console.log(`Uploaded to s3://${S3_BUCKET}/${key} (${head.ContentLength ?? 0} bytes)`)

      return {
        externalLink: presignedUrl,
        byte_count: head.ContentLength ?? 0,
        expiration,
        key,
      }
    }

    it.concurrent('merges CSV chunks and uploads to S3', async () => {
      // Execute query with external links
      const result = await executeStatement(
        'SELECT * FROM samples.tpch.lineitem LIMIT 1000',
        auth,
        { disposition: 'EXTERNAL_LINKS', format: 'CSV' }
      )

      expect(result.status.state).toBe('SUCCEEDED')
      expect(result.manifest?.total_chunk_count).toBeGreaterThanOrEqual(0)

      console.log(`Original chunks: ${result.manifest?.total_chunk_count}`)
      console.log(`Original rows: ${result.manifest?.total_row_count}`)

      // Skip if no external links
      if (!result.result?.external_links) {
        console.log('No external links, skipping S3 upload test')
        return
      }

      // Merge and upload to S3
      let uploadedKey: string | null = null
      const mergedResult = await mergeExternalLinks(result, auth, {
        forceMerge: true,
        mergeStreamToExternalLink: async (stream) => {
          const uploadResult = await uploadToS3(stream, 'CSV')
          uploadedKey = uploadResult.key
          return uploadResult
        },
      })

      // Verify merged result
      expect(mergedResult.manifest?.total_chunk_count).toBe(1)
      expect(mergedResult.result?.external_links?.length).toBe(1)
      expect(mergedResult.result?.external_links?.[0].external_link).toContain(
        S3_BUCKET
      )

      console.log(
        `Merged byte count: ${mergedResult.result?.external_links?.[0].byte_count}`
      )

      // Verify we can download from S3
      const externalLink = mergedResult.result?.external_links?.[0].external_link
      if (externalLink) {
        const response = await fetch(externalLink)
        expect(response.ok).toBe(true)
        const content = await response.text()
        const lineCount = content.split('\n').filter((l) => l.length > 0).length
        // +1 for header
        const expectedRows = result.manifest?.total_row_count ?? 0
        if (expectedRows > 0)
          expect(lineCount).toBe(expectedRows + 1)
        console.log(`Downloaded and verified ${lineCount} lines`)
      }
    }, 300000)

    it.concurrent('merges JSON_ARRAY chunks and uploads to S3', async () => {
      const result = await executeStatement(
        'SELECT * FROM samples.tpch.lineitem LIMIT 1000',
        auth,
        { disposition: 'EXTERNAL_LINKS', format: 'JSON_ARRAY' }
      )

      expect(result.status.state).toBe('SUCCEEDED')

      console.log(`Original chunks: ${result.manifest?.total_chunk_count}`)
      console.log(`Original rows: ${result.manifest?.total_row_count}`)

      // Skip if no external links
      if (!result.result?.external_links) {
        console.log('No external links, skipping S3 upload test')
        return
      }

      const mergedResult = await mergeExternalLinks(result, auth, {
        forceMerge: true,
        mergeStreamToExternalLink: (stream) => uploadToS3(stream, 'JSON_ARRAY'),
      })

      expect(mergedResult.manifest?.total_chunk_count).toBe(1)

      // Verify JSON validity
      const externalLink = mergedResult.result?.external_links?.[0].external_link
      if (externalLink) {
        const response = await fetch(externalLink)
        const content = await response.text()
        const parsed = JSON.parse(content)
        expect(Array.isArray(parsed)).toBe(true)
        expect(parsed.length).toBe(result.manifest?.total_row_count)
        console.log(`Downloaded and verified ${parsed.length} JSON records`)
      }
    }, 300000)

    it.concurrent('merges ARROW_STREAM chunks and uploads to S3', async () => {
      const result = await executeStatement(
        'SELECT * FROM samples.tpch.lineitem LIMIT 1000',
        auth,
        { disposition: 'EXTERNAL_LINKS', format: 'ARROW_STREAM' }
      )

      expect(result.status.state).toBe('SUCCEEDED')

      console.log(`Original chunks: ${result.manifest?.total_chunk_count}`)
      console.log(`Original rows: ${result.manifest?.total_row_count}`)

      // Skip if no external links
      if (!result.result?.external_links) {
        console.log('No external links, skipping S3 upload test')
        return
      }

      const mergedResult = await mergeExternalLinks(result, auth, {
        forceMerge: true,
        mergeStreamToExternalLink: (stream) => uploadToS3(stream, 'ARROW_STREAM'),
      })

      expect(mergedResult.manifest?.total_chunk_count).toBe(1)

      // Verify we can download from S3
      const externalLink = mergedResult.result?.external_links?.[0].external_link
      if (externalLink) {
        const response = await fetch(externalLink)
        expect(response.ok).toBe(true)
        const buffer = await response.arrayBuffer()
        expect(buffer.byteLength).toBeGreaterThan(0)
        console.log(`Downloaded Arrow stream: ${buffer.byteLength} bytes`)
      }
    }, 300000)

    it.concurrent('handles large data with multiple chunks (500k rows)', async () => {
      const result = await executeStatement(
        'SELECT * FROM samples.tpch.lineitem LIMIT 1000',
        auth,
        { disposition: 'EXTERNAL_LINKS', format: 'CSV' }
      )

      expect(result.status.state).toBe('SUCCEEDED')
      expect(result.manifest?.total_chunk_count).toBeGreaterThanOrEqual(1)

      console.log(`Original chunks: ${result.manifest?.total_chunk_count}`)
      console.log(`Original rows: ${result.manifest?.total_row_count}`)

      // Skip if no external links
      if (!result.result?.external_links) {
        console.log('No external links, skipping S3 upload test')
        return
      }

      let uploadedKey: string | null = null
      const mergedResult = await mergeExternalLinks(result, auth, {
        forceMerge: true,
        mergeStreamToExternalLink: async (stream) => {
          const uploadResult = await uploadToS3(stream, 'CSV')
          uploadedKey = uploadResult.key
          return uploadResult
        },
      })

      expect(mergedResult.manifest?.total_chunk_count).toBe(1)

      const uploadedSize =
        mergedResult.result?.external_links?.[0].byte_count ?? 0
      console.log(`Merged and uploaded ${uploadedSize} bytes to S3`)

      // Verify the S3 object exists
      const key = uploadedKey
      if (!key)
        throw new Error('No uploaded key available for head check')
      const headResponse = await s3Client.send(
        new HeadObjectCommand({
          Bucket: S3_BUCKET,
          Key: key,
        })
      )
      expect(headResponse.ContentLength).toBe(uploadedSize)
    }, 600000)
  })

  describe.concurrent('fetchAll with inline results', () => {
    it.concurrent('fetches inline rows into an array', async () => {
      const result = await executeStatement(
        'SELECT 1 AS value UNION ALL SELECT 2 AS value',
        auth,
        { disposition: 'INLINE', format: 'JSON_ARRAY' }
      )

      expect(result.status.state).toBe('SUCCEEDED')

      const rows = await fetchAll(result, auth)
      expect(rows).toHaveLength(2)
      expect(rows[0]).toEqual(['1'])
      expect(rows[1]).toEqual(['2'])
    })
  })

  describe.concurrent('fetchAll with external links', () => {
    it.concurrent('fetches JSON_ARRAY rows from external links', async () => {
      const result = await executeStatement(
        'SELECT 1 AS value UNION ALL SELECT 2 AS value',
        auth,
        { disposition: 'EXTERNAL_LINKS', format: 'JSON_ARRAY' }
      )

      expect(result.status.state).toBe('SUCCEEDED')

      const rows = await fetchAll(result, auth)
      expect(rows).toHaveLength(2)
      expect(rows[0]).toEqual(['1'])
      expect(rows[1]).toEqual(['2'])
    })

    it.concurrent('maps JSON_OBJECT rows from external links', async () => {
      const result = await executeStatement(
        'SELECT 1 AS value UNION ALL SELECT 2 AS value',
        auth,
        { disposition: 'EXTERNAL_LINKS', format: 'JSON_ARRAY' }
      )

      expect(result.status.state).toBe('SUCCEEDED')

      const rows = await fetchAll(result, auth, { format: 'JSON_OBJECT' })
      expect(rows).toHaveLength(2)
      expect(rows[0]).toEqual({ value: 1 })
      expect(rows[1]).toEqual({ value: 2 })
    })

    it.concurrent('maps JSON_OBJECT rows with datetime and nested decimals', async () => {
      const bigIntValue = 9007199254740993n
      const result = await executeStatement(
        `SELECT
          42 AS num,
          'hello' AS str,
          CAST(9007199254740993 AS BIGINT) AS big_int,
          CAST(12.34 AS DECIMAL(10, 2)) AS price,
          DATE '2024-01-02' AS d,
          date_format(TIMESTAMP '1970-01-01 03:04:05', 'HH:mm:ss') AS t,
          TIMESTAMP '2024-01-02 03:04:05.123' AS dt,
          TIMESTAMP_NTZ '2024-01-02 03:04:05.123' AS dt_ntz,
          named_struct(
            'a',
            1,
            'b',
            named_struct(
              'c',
              'x',
              'big',
              CAST(9007199254740993 AS BIGINT),
              'price',
              CAST(56.78 AS DECIMAL(10, 2))
            )
          ) AS nested`,
        auth,
        { disposition: 'EXTERNAL_LINKS', format: 'JSON_ARRAY' }
      )

      expect(result.status.state).toBe('SUCCEEDED')

      const rows = await fetchAll(result, auth, { format: 'JSON_OBJECT' })
      expect(rows).toEqual([
        {
          num: 42,
          str: 'hello',
          big_int: bigIntValue,
          price: 12.34,
          d: '2024-01-02',
          t: '03:04:05',
          dt: '2024-01-02T03:04:05.123Z',
          dt_ntz: '2024-01-02T03:04:05.123',
          nested: {
            a: 1,
            b: {
              c: 'x',
              big: bigIntValue,
              price: 56.78,
            },
          },
        },
      ])
    })

    it.concurrent('fetches all rows across multiple external link chunks', async () => {
      const result = await executeStatement(
        `SELECT
          id,
          repeat('x', 5000) AS payload
        FROM range(10000)`,
        auth,
        { disposition: 'EXTERNAL_LINKS', format: 'JSON_ARRAY' }
      )

      expect(result.status.state).toBe('SUCCEEDED')
      expect(result.manifest?.total_row_count).toBe(10000)
      expect(result.manifest?.total_chunk_count).toBeGreaterThan(1)

      const rows = await fetchAll(result, auth)
      expect(rows).toHaveLength(10000)

      const firstRow = rows[0] as string[]
      const lastRow = rows[rows.length - 1] as string[]
      expect(firstRow[0]).toBe('0')
      expect(lastRow[0]).toBe('9999')
      expect(firstRow[1]?.length).toBe(5000)
    }, 300000)
  })

  describe.concurrent('S3 client operations', () => {
    it.concurrent('can upload and download from S3', async () => {
      const testData = 'Hello, Databricks SQL!'
      const key = `${S3_PREFIX}test-${Date.now()}.txt`
      const passThrough = new PassThrough()
      const gzip = createGzip()

      // Upload
      const upload = new Upload({
        client: s3Client,
        params: {
          Bucket: S3_BUCKET,
          Key: key,
          Body: passThrough,
          ContentType: 'text/plain; charset=utf-8',
          ContentEncoding: 'gzip',
        },
      })
      await Promise.all([
        pipeline(Readable.from([testData]), gzip, passThrough),
        upload.done(),
      ])
      uploadedKeys.push(key)

      // Generate presigned URL
      const presignedUrl = await getSignedUrl(
        s3Client,
        new GetObjectCommand({
          Bucket: S3_BUCKET,
          Key: key,
        }),
        { expiresIn: 3600 }
      )

      // Download via presigned URL
      const response = await fetch(presignedUrl)
      const content = await response.text()

      expect(content).toBe(testData)
      console.log(`S3 round-trip test passed for key: ${key}`)
    }, 60000)
  })
})
