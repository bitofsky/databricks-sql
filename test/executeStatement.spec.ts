import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { executeStatement } from '../src/api'
import {
  mockAuth,
  mockInlineResult,
  mockPendingResult,
  mockRunningResult,
  mockSucceededAfterPolling,
  mockFailedResult,
  mockQueryInfo,
} from './mocks.js'

describe('executeStatement', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  it('should return result immediately when statement succeeds', async () => {
    const mockFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(mockInlineResult),
    })
    vi.stubGlobal('fetch', mockFetch)

    const result = await executeStatement('SELECT 1', mockAuth)

    expect(result.statement_id).toBe(mockInlineResult.statement_id)
    expect(result.status.state).toBe('SUCCEEDED')
    expect(result.result?.data_array).toEqual([['1', 'hello']])
    expect(mockFetch).toHaveBeenCalledTimes(1)
  })

  it('should poll until statement succeeds', async () => {
    const mockFetch = vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(mockPendingResult),
      })
      .mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(mockRunningResult),
      })
      .mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(mockSucceededAfterPolling),
      })
    vi.stubGlobal('fetch', mockFetch)

    const resultPromise = executeStatement('SELECT 42', mockAuth)

    // Advance timers for polling delays (fixed 5000ms interval)
    await vi.advanceTimersByTimeAsync(5000) // First poll delay
    await vi.advanceTimersByTimeAsync(5000) // Second poll delay

    const result = await resultPromise

    expect(result.status.state).toBe('SUCCEEDED')
    expect(mockFetch).toHaveBeenCalledTimes(3)
  })

  it('should call onProgress callback during polling', async () => {
    const mockFetch = vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(mockPendingResult),
      })
      .mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(mockSucceededAfterPolling),
      })
    vi.stubGlobal('fetch', mockFetch)

    const onProgress = vi.fn()
    const resultPromise = executeStatement('SELECT 42', mockAuth, { onProgress })

    await vi.advanceTimersByTimeAsync(5000)

    await resultPromise

    expect(mockFetch).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        body: expect.stringContaining('"wait_timeout":"0s"'),
      })
    )
    expect(onProgress).toHaveBeenCalledWith(expect.objectContaining({ status: { state: 'SUCCEEDED' } }), undefined)
  })

  it('should not fetch metrics when enableMetrics is false', async () => {
    const mockFetch = vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(mockPendingResult),
      })
      .mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(mockSucceededAfterPolling),
      })
    vi.stubGlobal('fetch', mockFetch)

    const onProgress = vi.fn()
    const resultPromise = executeStatement('SELECT 42', mockAuth, { onProgress })

    await vi.advanceTimersByTimeAsync(5000)
    await resultPromise

    // Only 2 calls: postStatement + getStatement (no metrics calls)
    expect(mockFetch).toHaveBeenCalledTimes(2)
    // onProgress should be called without metrics
    expect(onProgress).toHaveBeenCalledWith(expect.objectContaining({ status: { state: 'SUCCEEDED' } }), undefined)
  })

  it('should fetch metrics when enableMetrics is true', async () => {
    const mockFetch = vi
      .fn()
      // postStatement
      .mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(mockPendingResult),
      })
      // getStatement
      .mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(mockSucceededAfterPolling),
      })
      // getQueryMetrics (during polling)
      .mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(mockQueryInfo),
      })
      // getQueryMetrics (final)
      .mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(mockQueryInfo),
      })
    vi.stubGlobal('fetch', mockFetch)

    const onProgress = vi.fn()
    const resultPromise = executeStatement('SELECT 42', mockAuth, {
      onProgress,
      enableMetrics: true,
    })

    await vi.advanceTimersByTimeAsync(5000)
    await resultPromise

    // 4 calls: postStatement + getStatement + getQueryMetrics + getQueryMetrics
    expect(mockFetch).toHaveBeenCalledTimes(4)

    expect(onProgress).toHaveBeenCalledWith(
      expect.objectContaining({ status: { state: 'SUCCEEDED' } }),
      expect.objectContaining({
        total_time_ms: 959,
      })
    )
  })

  it('should handle metrics API failure gracefully', async () => {
    const mockFetch = vi
      .fn()
      // postStatement
      .mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(mockPendingResult),
      })
      // getStatement
      .mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(mockSucceededAfterPolling),
      })
      // getQueryMetrics fails again (401 - no retry)
      .mockResolvedValueOnce({
        ok: false,
        status: 401,
        statusText: 'Unauthorized',
        text: () => Promise.resolve('Unauthorized'),
      })
      // getQueryMetrics fails again (401 - no retry)
      .mockResolvedValueOnce({
        ok: false,
        status: 401,
        statusText: 'Unauthorized',
        text: () => Promise.resolve('Unauthorized'),
      })
    vi.stubGlobal('fetch', mockFetch)

    const onProgress = vi.fn()
    const resultPromise = executeStatement('SELECT 42', mockAuth, {
      onProgress,
      enableMetrics: true,
    })

    await vi.advanceTimersByTimeAsync(5000)
    const result = await resultPromise

    // Statement should still succeed even if metrics fail
    expect(result.status.state).toBe('SUCCEEDED')
    // onProgress should be called without metrics (undefined)
    expect(onProgress).toHaveBeenCalledWith(expect.objectContaining({ status: { state: 'SUCCEEDED' } }), undefined)
  })

  it('should throw DatabricksSqlError when statement fails', async () => {
    const mockFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(mockFailedResult),
    })
    vi.stubGlobal('fetch', mockFetch)

    await expect(executeStatement('SELECT invalid', mockAuth)).rejects.toThrow(
      'Syntax error in SQL statement'
    )
  })

  it('should abort when signal is aborted before execution', async () => {
    const mockFetch = vi.fn()
    vi.stubGlobal('fetch', mockFetch)

    const controller = new AbortController()
    controller.abort() // Abort before calling

    await expect(
      executeStatement('SELECT 1', mockAuth, { signal: controller.signal })
    ).rejects.toThrow('Aborted')

    // Should not have made any fetch calls
    expect(mockFetch).not.toHaveBeenCalled()
  })

  it('should cancel statement when aborted during polling', async () => {
    const mockFetch = vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(mockPendingResult),
      })
      .mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({}),
      })
    vi.stubGlobal('fetch', mockFetch)

    const controller = new AbortController()
    const resultPromise = executeStatement('SELECT 42', mockAuth, {
      signal: controller.signal,
    })

    await Promise.resolve()
    controller.abort()

    await expect(resultPromise).rejects.toThrow('Aborted')
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining(
        `/api/2.0/sql/statements/${mockPendingResult.statement_id}/cancel`
      ),
      expect.objectContaining({ method: 'POST' })
    )
  })

  it('should extract warehouse_id from httpPath', async () => {
    const mockFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(mockInlineResult),
    })
    vi.stubGlobal('fetch', mockFetch)

    await executeStatement('SELECT 1', mockAuth)

    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/2.0/sql/statements'),
      expect.objectContaining({
        body: expect.stringContaining('"warehouse_id":"abc123def456"'),
      })
    )
  })

  it('should use provided warehouse_id over httpPath', async () => {
    const mockFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(mockInlineResult),
    })
    vi.stubGlobal('fetch', mockFetch)

    await executeStatement('SELECT 1', mockAuth, {
      warehouse_id: 'custom-warehouse',
    })

    expect(mockFetch).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        body: expect.stringContaining('"warehouse_id":"custom-warehouse"'),
      })
    )
  })
})
