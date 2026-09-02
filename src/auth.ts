import type { AuthInfo, OAuthM2MAuthInfo } from './types.js'

import { DatabricksSqlError, HttpError } from './errors.js'
import { buildUrl } from './util.js'

const OAUTH_TOKEN_PATH = '/oidc/v1/token'
const REQUIRED_OAUTH_SCOPES = ['sql', 'query-history']
const DEFAULT_TOKEN_EXPIRES_IN_SECONDS = 3600
const TOKEN_REFRESH_BUFFER_MS = 5 * 60 * 1000

type OAuthTokenResponse = {
  access_token?: string
  expires_in?: number
}

type CachedOAuthToken = {
  accessToken: string
  refreshAt: number
}

const oauthTokenCache = new Map<string, CachedOAuthToken>()
const oauthTokenRequests = new Map<string, Promise<CachedOAuthToken>>()

export function isOAuthM2MAuthInfo(auth: AuthInfo): auth is OAuthM2MAuthInfo {
  return !auth.token && Boolean(auth.clientId && auth.clientSecret)
}

export async function getAccessToken(auth: AuthInfo): Promise<string> {
  if (auth.token) {
    if (auth.clientId || auth.clientSecret || auth.scopes)
      throw new DatabricksSqlError(
        'AuthInfo cannot contain token with OAuth settings',
        'INVALID_AUTH_CONFIG'
      )

    return auth.token
  }

  if (!isOAuthM2MAuthInfo(auth))
    throw new DatabricksSqlError(
      'AuthInfo must contain either token or clientId and clientSecret',
      'INVALID_AUTH_CONFIG'
    )

  return getOAuthAccessToken(auth)
}

export function invalidateAccessToken(auth: OAuthM2MAuthInfo): void {
  oauthTokenCache.delete(getOAuthTokenCacheKey(auth))
}

async function getOAuthAccessToken(auth: OAuthM2MAuthInfo): Promise<string> {
  const cacheKey = getOAuthTokenCacheKey(auth)
  const cachedToken = oauthTokenCache.get(cacheKey)

  if (cachedToken && Date.now() < cachedToken.refreshAt)
    return cachedToken.accessToken

  const pendingRequest = oauthTokenRequests.get(cacheKey)
  if (pendingRequest)
    return (await pendingRequest).accessToken

  const request = requestOAuthToken(auth)
  oauthTokenRequests.set(cacheKey, request)

  try {
    const token = await request
    oauthTokenCache.set(cacheKey, token)
    return token.accessToken
  } finally {
    oauthTokenRequests.delete(cacheKey)
  }
}

async function requestOAuthToken(
  auth: OAuthM2MAuthInfo
): Promise<CachedOAuthToken> {
  const credentials = Buffer.from(
    `${encodeURIComponent(auth.clientId)}:${encodeURIComponent(auth.clientSecret)}`
  ).toString('base64')
  const body = new URLSearchParams({
    grant_type: 'client_credentials',
    scope: getOAuthScope(auth),
  })
  const response = await fetch(buildUrl(auth.host, OAUTH_TOKEN_PATH), {
    method: 'POST',
    headers: {
      Authorization: `Basic ${credentials}`,
      'Content-Type': 'application/x-www-form-urlencoded',
      Accept: 'application/json',
    },
    body,
  })

  if (!response.ok) {
    const errorBody = await response.text().catch(() => '')
    throw new HttpError(
      response.status,
      response.statusText,
      `OAuth token request failed: ${errorBody || response.statusText}`
    )
  }

  const result = await response.json() as OAuthTokenResponse
  if (!result.access_token)
    throw new DatabricksSqlError(
      'OAuth token response does not contain access_token',
      'INVALID_OAUTH_TOKEN_RESPONSE'
    )

  const expiresInSeconds =
    typeof result.expires_in === 'number' && Number.isFinite(result.expires_in)
      ? result.expires_in
      : DEFAULT_TOKEN_EXPIRES_IN_SECONDS
  const refreshInMs = Math.max(
    0,
    expiresInSeconds * 1000 - TOKEN_REFRESH_BUFFER_MS
  )

  return {
    accessToken: result.access_token,
    refreshAt: Date.now() + refreshInMs,
  }
}

function getOAuthTokenCacheKey(auth: OAuthM2MAuthInfo): string {
  return `${new URL(buildUrl(auth.host, '/')).origin}:${auth.clientId}:${getOAuthScope(auth)}`
}

function getOAuthScope(auth: OAuthM2MAuthInfo): string {
  return [...new Set([...REQUIRED_OAUTH_SCOPES, ...(auth.scopes ?? [])])]
    .filter(Boolean)
    .sort()
    .join(' ')
}
