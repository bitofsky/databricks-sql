import type { AuthInfo } from './types.js'
import {
  getAccessToken,
  invalidateAccessToken,
  isOAuthM2MAuthInfo,
} from './auth.js'
import {
  HttpError,
  AuthenticationError,
  RateLimitError,
  AbortError,
} from './errors.js'
import { buildUrl, delay } from './util.js'

const MAX_RETRIES = 3
const INITIAL_RETRY_DELAY_MS = 1000

type HttpMethod = 'GET' | 'POST' | 'DELETE'

type HttpRequestOptions = {
  method: HttpMethod
  path: string
  body?: unknown
  signal?: AbortSignal
}

/**
 * HTTP request wrapper with retry and error handling
 */
export async function httpRequest<T>(
  auth: AuthInfo,
  options: HttpRequestOptions
): Promise<T> {
  const { method, path, body, signal } = options
  const url = buildUrl(auth.host, path)

  let lastError: Error | undefined
  let retryDelay = INITIAL_RETRY_DELAY_MS
  let oauthRetryAttempted = false

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    if (signal?.aborted)
      throw new AbortError()

    try {
      const accessToken = await getAccessToken(auth)
      // Build a minimal fetch init, skipping undefined values.
      const fetchInit = Object.fromEntries(
        Object.entries({
          method,
          headers: {
            Authorization: `Bearer ${accessToken}`,
            'Content-Type': 'application/json',
            Accept: 'application/json',
          },
          body: body ? JSON.stringify(body) : undefined,
          signal,
        }).filter(([, v]) => v !== undefined)
      ) as RequestInit

      const response = await fetch(url, fetchInit)

      // Success
      if (response.ok)
        return (await response.json()) as T

      const isTokenRejected =
        response.status === 401 ||
        (
          response.status === 403 &&
          (await response.clone().text().catch(() => '')).includes(
            'Invalid access token'
          )
        )

      if (
        isTokenRejected &&
        isOAuthM2MAuthInfo(auth) &&
        !oauthRetryAttempted &&
        attempt < MAX_RETRIES
      ) {
        invalidateAccessToken(auth)
        oauthRetryAttempted = true
        continue
      }

      // Authentication error
      if (response.status === 401)
        throw new AuthenticationError()

      // Rate limit
      if (response.status === 429) {
        const retryAfterHeader = response.headers.get('Retry-After')
        const retryAfter = retryAfterHeader
          ? parseInt(retryAfterHeader, 10)
          : undefined
        const error = new RateLimitError(
          isNaN(retryAfter as number) ? undefined : retryAfter
        )

        if (error.retryAfter && attempt < MAX_RETRIES) {
          await delay(error.retryAfter * 1000, signal)
          continue
        }

        throw error
      }

      // Server error (can retry)
      if (response.status >= 500) {
        const errorBody = await response.text().catch(() => '')
        lastError = new HttpError(response.status, response.statusText, errorBody)

        if (attempt < MAX_RETRIES) {
          // Exponential backoff for transient server errors.
          await delay(retryDelay, signal)
          retryDelay *= 2
          continue
        }
      }

      // Other client errors
      const errorBody = await response.text().catch(() => '')

      throw new HttpError(response.status, response.statusText, errorBody)

    } catch (err) {
      if (
        err instanceof HttpError &&
        err.status >= 500 &&
        attempt < MAX_RETRIES
      ) {
        lastError = err
        await delay(retryDelay, signal)
        retryDelay *= 2
        continue
      }

      // Re-throw known errors
      if (
        err instanceof AbortError ||
        err instanceof AuthenticationError ||
        err instanceof HttpError
      )
        throw err

      // Network error
      if (err instanceof TypeError && err.message.includes('fetch')) {
        lastError = err
        if (attempt < MAX_RETRIES) {
          // Network errors are retried with backoff.
          await delay(retryDelay, signal)
          retryDelay *= 2
          continue
        }
      }

      throw err
    }
  }

  throw lastError ?? new Error('Request failed after retries')
}
