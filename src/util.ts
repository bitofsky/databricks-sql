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
