import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { executeStatement } from '../src/api'
import {
  mockAuth,
  mockInlineResult,
  mockPendingResult,
  mockRunningResult,
  mockSucceededAfterPolling,
  mockFailedResult,
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

    // Advance timers for polling delays
    await vi.advanceTimersByTimeAsync(500) // First poll delay
    await vi.advanceTimersByTimeAsync(750) // Second poll delay (500 * 1.5)

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

    await vi.advanceTimersByTimeAsync(500)

    await resultPromise

    expect(onProgress).toHaveBeenCalledWith({ state: 'PENDING' })
    expect(onProgress).toHaveBeenCalledWith({ state: 'SUCCEEDED' })
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
