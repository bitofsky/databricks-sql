import { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import type { ReadableStream as WebReadableStream } from 'node:stream/web'
import type { StatementResult, StatementManifest } from './types.js'
import { AbortError, DatabricksSqlError } from './errors.js'

/**
 * Extract warehouse_id from httpPath
 * @example "/sql/1.0/warehouses/abc123def456" -> "abc123def456"
 */
export function extractWarehouseId(httpPath: string): string {
  const match = httpPath.match(/\/sql\/\d+\.\d+\/warehouses\/([a-zA-Z0-9]+)/)
  if (!match?.[1])
    throw new Error(`Cannot extract warehouse_id from httpPath: ${httpPath}`)
  return match[1]
}

/**
 * Throw AbortError if signal is aborted
 */
export function throwIfAborted(signal: AbortSignal | undefined, context: string): void {
  if (signal?.aborted)
    throw new AbortError(`[${context}] Aborted`)
}

/**
 * Delay for specified milliseconds with AbortSignal support
 */
export async function delay(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted)
      return reject(new AbortError('Aborted before delay'))

    let settled = false

    const onAbort = () => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      reject(new AbortError('Aborted during delay'))
    }

    const timer = setTimeout(() => {
      if (settled) return
      settled = true
      signal?.removeEventListener('abort', onAbort)
      resolve()
    }, ms)

    signal?.addEventListener('abort', onAbort, { once: true })
  })
}

/**
 * Build full URL from host and path
 */
export function buildUrl(host: string, path: string): string {
  const base = host.startsWith('https://') ? host : `https://${host}`
  return new URL(path, base).href
}

/**
 * Validate statement result is in SUCCEEDED state with manifest.
 * Returns the manifest for convenience.
 * @throws {DatabricksSqlError} If state is not SUCCEEDED or manifest is missing
 */
export function validateSucceededResult(
  statementResult: StatementResult
): StatementManifest {
  if (statementResult.status.state !== 'SUCCEEDED')
    throw new DatabricksSqlError(
      `Cannot fetch from non-succeeded statement: ${statementResult.status.state}`,
      'INVALID_STATE',
      statementResult.statement_id
    )

  if (!statementResult.manifest)
    throw new DatabricksSqlError(
      'Statement result has no manifest',
      'MISSING_MANIFEST',
      statementResult.statement_id
    )

  return statementResult.manifest
}

function isWebReadableStream(body: unknown): body is WebReadableStream {
  return typeof (body as WebReadableStream).getReader === 'function'
}

export async function pipeUrlToOutput(
  url: string,
  output: NodeJS.WritableStream,
  signal?: AbortSignal
): Promise<void> {
  // Uses Node 20+ global fetch with Web streams.
  if (signal?.aborted)
    throw new AbortError('Aborted while streaming')

  const response = await fetch(url, signal ? { signal } : undefined)
  if (!response.ok) {
    throw new Error(
      `Failed to fetch external link: ${response.status} ${response.statusText}`
    )
  }

  if (!response.body)
    return void output.end()

  const body = response.body
  const input = isWebReadableStream(body)
    ? Readable.fromWeb(body)
    : (body as NodeJS.ReadableStream)

  await pipeline(input, output)
}
