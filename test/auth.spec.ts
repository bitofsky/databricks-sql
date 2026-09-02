import { afterEach, describe, expect, it, vi } from 'vitest'
import { executeStatement } from '../src/api/index.js'
import type { AuthInfo, OAuthM2MAuthInfo } from '../src/types.js'
import { mockInlineResult } from './mocks.js'

function oauthAuth(id: string): OAuthM2MAuthInfo {
  return {
    host: 'test.cloud.databricks.com',
    httpPath: '/sql/1.0/warehouses/abc123def456',
    clientId: `client-${id}`,
    clientSecret: `secret-${id}`,
  }
}

function jsonResponse(value: unknown) {
  return {
    ok: true,
    status: 200,
    statusText: 'OK',
    json: () => Promise.resolve(value),
  }
}

function errorResponse(
  status: number,
  statusText: string,
  value: unknown
): Response {
  return new Response(JSON.stringify(value), {
    status,
    statusText,
    headers: { 'Content-Type': 'application/json' },
  })
}

afterEach(() => {
  vi.restoreAllMocks()
  vi.useRealTimers()
})

describe('authentication', () => {
  it('keeps Personal Access Token authentication unchanged', async () => {
    const mockFetch = vi.fn().mockResolvedValueOnce(jsonResponse(mockInlineResult))
    vi.stubGlobal('fetch', mockFetch)

    await executeStatement('SELECT 1', {
      host: 'test.cloud.databricks.com',
      httpPath: '/sql/1.0/warehouses/abc123def456',
      token: 'pat-token',
    })

    expect(mockFetch).toHaveBeenCalledTimes(1)
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/2.0/sql/statements'),
      expect.objectContaining({
        headers: expect.objectContaining({
          Authorization: 'Bearer pat-token',
        }),
      })
    )
  })

  it('exchanges client credentials for an OAuth access token', async () => {
    const auth = oauthAuth('exchange')
    const mockFetch = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({
        access_token: 'oauth-token',
        token_type: 'Bearer',
        expires_in: 3600,
      }))
      .mockResolvedValueOnce(jsonResponse(mockInlineResult))
    vi.stubGlobal('fetch', mockFetch)

    await executeStatement('SELECT 1', auth)

    const expectedCredentials = Buffer.from(
      `${encodeURIComponent(auth.clientId)}:${encodeURIComponent(auth.clientSecret)}`
    ).toString('base64')
    expect(mockFetch).toHaveBeenNthCalledWith(
      1,
      'https://test.cloud.databricks.com/oidc/v1/token',
      expect.objectContaining({
        method: 'POST',
        headers: expect.objectContaining({
          Authorization: `Basic ${expectedCredentials}`,
          'Content-Type': 'application/x-www-form-urlencoded',
        }),
        body: expect.any(URLSearchParams),
      })
    )
    expect(mockFetch).toHaveBeenNthCalledWith(
      2,
      expect.stringContaining('/api/2.0/sql/statements'),
      expect.objectContaining({
        headers: expect.objectContaining({
          Authorization: 'Bearer oauth-token',
        }),
      })
    )
    const tokenRequest = mockFetch.mock.calls[0]?.[1] as RequestInit
    expect(tokenRequest.body?.toString()).toBe(
      'grant_type=client_credentials&scope=query-history+sql'
    )
  })

  it('adds configured OAuth scopes to the required scopes', async () => {
    const auth: OAuthM2MAuthInfo = {
      ...oauthAuth('scopes'),
      scopes: ['jobs', 'sql'],
    }
    const mockFetch = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({ access_token: 'scoped-token', expires_in: 3600 })
      )
      .mockResolvedValueOnce(jsonResponse(mockInlineResult))
    vi.stubGlobal('fetch', mockFetch)

    await executeStatement('SELECT 1', auth)

    const tokenRequest = mockFetch.mock.calls[0]?.[1] as RequestInit
    expect(tokenRequest.body?.toString()).toBe(
      'grant_type=client_credentials&scope=jobs+query-history+sql'
    )
  })

  it('reuses an OAuth token until its refresh time', async () => {
    const auth = oauthAuth('cache')
    const mockFetch = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ access_token: 'cached-token', expires_in: 3600 }))
      .mockResolvedValueOnce(jsonResponse(mockInlineResult))
      .mockResolvedValueOnce(jsonResponse(mockInlineResult))
    vi.stubGlobal('fetch', mockFetch)

    await executeStatement('SELECT 1', auth)
    await executeStatement('SELECT 2', auth)

    expect(mockFetch).toHaveBeenCalledTimes(3)
    expect(
      mockFetch.mock.calls.filter(([url]) =>
        String(url).includes('/oidc/v1/token')
      )
    ).toHaveLength(1)
  })

  it('shares one OAuth token request across concurrent calls', async () => {
    const auth = oauthAuth('concurrent')
    let resolveToken:
      | ((response: ReturnType<typeof jsonResponse>) => void)
      | undefined
    const tokenResponse = new Promise<ReturnType<typeof jsonResponse>>((resolve) => {
      resolveToken = resolve
    })
    const mockFetch = vi.fn((url: string | URL | Request) => {
      if (String(url).includes('/oidc/v1/token'))
        return tokenResponse
      return Promise.resolve(jsonResponse(mockInlineResult))
    })
    vi.stubGlobal('fetch', mockFetch)

    const first = executeStatement('SELECT 1', auth)
    const second = executeStatement('SELECT 2', auth)

    await vi.waitFor(() => expect(mockFetch).toHaveBeenCalledTimes(1))
    resolveToken?.(jsonResponse({ access_token: 'shared-token', expires_in: 3600 }))
    await Promise.all([first, second])

    expect(mockFetch).toHaveBeenCalledTimes(3)
    expect(
      mockFetch.mock.calls.filter(([url]) =>
        String(url).includes('/oidc/v1/token')
      )
    ).toHaveLength(1)
  })

  it('refreshes an OAuth token five minutes before expiration', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T00:00:00.000Z'))
    const auth = oauthAuth('expiry')
    const mockFetch = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ access_token: 'first-token', expires_in: 3600 }))
      .mockResolvedValueOnce(jsonResponse(mockInlineResult))
      .mockResolvedValueOnce(jsonResponse({ access_token: 'second-token', expires_in: 3600 }))
      .mockResolvedValueOnce(jsonResponse(mockInlineResult))
    vi.stubGlobal('fetch', mockFetch)

    await executeStatement('SELECT 1', auth)
    await vi.advanceTimersByTimeAsync(55 * 60 * 1000)
    await executeStatement('SELECT 2', auth)

    expect(mockFetch).toHaveBeenCalledTimes(4)
    expect(mockFetch.mock.calls[3]?.[1]).toEqual(expect.objectContaining({
      headers: expect.objectContaining({ Authorization: 'Bearer second-token' }),
    }))
  })

  it('refreshes once when an OAuth access token receives 401', async () => {
    const auth = oauthAuth('unauthorized')
    const mockFetch = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ access_token: 'rejected-token', expires_in: 3600 }))
      .mockResolvedValueOnce(errorResponse(401, 'Unauthorized', {
        error_code: 401,
        message: 'Unauthorized',
      }))
      .mockResolvedValueOnce(jsonResponse({ access_token: 'renewed-token', expires_in: 3600 }))
      .mockResolvedValueOnce(jsonResponse(mockInlineResult))
    vi.stubGlobal('fetch', mockFetch)

    await executeStatement('SELECT 1', auth)

    expect(mockFetch).toHaveBeenCalledTimes(4)
    expect(mockFetch.mock.calls[3]?.[1]).toEqual(expect.objectContaining({
      headers: expect.objectContaining({ Authorization: 'Bearer renewed-token' }),
    }))
  })

  it('refreshes once when Databricks rejects an OAuth token with 403', async () => {
    const auth = oauthAuth('forbidden-token')
    const mockFetch = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({ access_token: 'rejected-token', expires_in: 3600 })
      )
      .mockResolvedValueOnce(errorResponse(403, 'Forbidden', {
        error_code: 403,
        message: 'Invalid access token.',
      }))
      .mockResolvedValueOnce(
        jsonResponse({ access_token: 'renewed-token', expires_in: 3600 })
      )
      .mockResolvedValueOnce(jsonResponse(mockInlineResult))
    vi.stubGlobal('fetch', mockFetch)

    await executeStatement('SELECT 1', auth)

    expect(mockFetch).toHaveBeenCalledTimes(4)
    expect(mockFetch.mock.calls[3]?.[1]).toEqual(expect.objectContaining({
      headers: expect.objectContaining({ Authorization: 'Bearer renewed-token' }),
    }))
  })

  it('does not refresh an OAuth token for a permission 403', async () => {
    const auth = oauthAuth('forbidden-permission')
    const mockFetch = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({ access_token: 'valid-token', expires_in: 3600 })
      )
      .mockResolvedValueOnce(errorResponse(403, 'Forbidden', {
        error_code: 403,
        message: 'Permission denied.',
      }))
    vi.stubGlobal('fetch', mockFetch)

    await expect(executeStatement('SELECT 1', auth)).rejects.toMatchObject({
      status: 403,
    })
    expect(mockFetch).toHaveBeenCalledTimes(2)
  })

  it('includes the OAuth error response in token request failures', async () => {
    const auth = oauthAuth('invalid-client')
    const mockFetch = vi.fn().mockResolvedValueOnce(
      errorResponse(401, 'Unauthorized', {
        error: 'invalid_client',
        error_description: 'Client authentication failed',
      })
    )
    vi.stubGlobal('fetch', mockFetch)

    await expect(executeStatement('SELECT 1', auth)).rejects.toThrow(
      /invalid_client.*Client authentication failed/
    )
  })

  it('retries OAuth token endpoint server errors', async () => {
    vi.useFakeTimers()
    const auth = oauthAuth('token-server-error')
    const mockFetch = vi
      .fn()
      .mockResolvedValueOnce(
        errorResponse(503, 'Service Unavailable', { error: 'unavailable' })
      )
      .mockResolvedValueOnce(
        jsonResponse({ access_token: 'oauth-token', expires_in: 3600 })
      )
      .mockResolvedValueOnce(jsonResponse(mockInlineResult))
    vi.stubGlobal('fetch', mockFetch)

    const resultPromise = executeStatement('SELECT 1', auth)
    await vi.advanceTimersByTimeAsync(1000)

    await expect(resultPromise).resolves.toMatchObject({
      status: { state: 'SUCCEEDED' },
    })
    expect(mockFetch).toHaveBeenCalledTimes(3)
  })

  it('rejects incomplete or ambiguous authentication settings', async () => {
    vi.stubGlobal('fetch', vi.fn())
    const incompleteAuth = {
      host: 'test.cloud.databricks.com',
      httpPath: '/sql/1.0/warehouses/abc123def456',
      clientId: 'client-only',
    } as unknown as AuthInfo
    const ambiguousAuth = {
      host: 'test.cloud.databricks.com',
      httpPath: '/sql/1.0/warehouses/abc123def456',
      token: 'pat-token',
      clientId: 'client',
      clientSecret: 'secret',
    } as unknown as AuthInfo

    await expect(executeStatement('SELECT 1', incompleteAuth)).rejects.toMatchObject({
      code: 'INVALID_AUTH_CONFIG',
    })
    await expect(executeStatement('SELECT 1', ambiguousAuth)).rejects.toMatchObject({
      code: 'INVALID_AUTH_CONFIG',
    })
  })
})
