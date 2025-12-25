import { describe, it, expect, vi, afterEach } from 'vitest'
import { fetchStream } from '../src/api'
import type { StatementResult } from '../src/types.js'
import {
  mockAuth,
  mockExternalLinksResult,
  mockExternalLinkData,
} from './mocks.js'
import { createMockReadableStream, collectStream } from './testUtil.js'

describe('fetchStream', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('should throw error for non-succeeded statement', () => {
    const pendingResult: StatementResult = {
      statement_id: 'test',
      status: { state: 'PENDING' },
    }

    expect(() => fetchStream(pendingResult, mockAuth)).toThrow(
      'Cannot fetch from non-succeeded statement: PENDING'
    )
  })

  it('should throw error for statement without manifest', () => {
    const noManifestResult: StatementResult = {
      statement_id: 'test',
      status: { state: 'SUCCEEDED' },
    }

    expect(() => fetchStream(noManifestResult, mockAuth)).toThrow(
      'Statement result has no manifest'
    )
  })

  it('should stream data from external links', async () => {
    // Mock fetch for external link
    const mockFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      body: createMockReadableStream(JSON.stringify(mockExternalLinkData)),
    })
    vi.stubGlobal('fetch', mockFetch)

    const stream = fetchStream(mockExternalLinksResult, mockAuth)
    const data = await collectStream(stream)
    const parsed = JSON.parse(data.toString())

    expect(parsed).toEqual(mockExternalLinkData)
  })

  it('should handle empty result (no external links)', async () => {
    const emptyResult: StatementResult = {
      statement_id: 'empty-test',
      status: { state: 'SUCCEEDED' },
      manifest: {
        format: 'JSON_ARRAY',
        schema: { column_count: 1, columns: [] },
        total_chunk_count: 0,
      },
    }

    const stream = fetchStream(emptyResult, mockAuth)
    const data = await collectStream(stream)

    expect(data.length).toBe(0)
  })

  it('should abort stream when signal is aborted', async () => {
    const controller = new AbortController()

    // Mock fetch that delays
    const mockFetch = vi.fn().mockImplementation(
      () =>
        new Promise((resolve) => {
          setTimeout(() => {
            resolve({
              ok: true,
              body: createMockReadableStream('[]'),
            })
          }, 1000)
        })
    )
    vi.stubGlobal('fetch', mockFetch)

    const stream = fetchStream(mockExternalLinksResult, mockAuth, {
      signal: controller.signal,
    })

    // Abort immediately
    controller.abort()

    await expect(collectStream(stream)).rejects.toThrow('aborted')
  })

  it('should merge multiple external links in correct order', async () => {
    const multiLinkResult: StatementResult = {
      statement_id: 'multi-link-test',
      status: { state: 'SUCCEEDED' },
      manifest: {
        format: 'JSON_ARRAY',
        schema: { column_count: 2, columns: [] },
        total_chunk_count: 3,
        total_row_count: 15,
      },
      result: {
        external_links: [
          {
            chunk_index: 0,
            row_offset: 0,
            row_count: 5,
            byte_count: 100,
            external_link: 'https://mock/chunk0.json',
            expiration: '2025-12-25T12:00:00Z',
          },
          {
            chunk_index: 1,
            row_offset: 5,
            row_count: 5,
            byte_count: 100,
            external_link: 'https://mock/chunk1.json',
            expiration: '2025-12-25T12:00:00Z',
          },
          {
            chunk_index: 2,
            row_offset: 10,
            row_count: 5,
            byte_count: 100,
            external_link: 'https://mock/chunk2.json',
            expiration: '2025-12-25T12:00:00Z',
          },
        ],
      },
    }

    const mockFetch = vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        body: createMockReadableStream('[["a","1"],["b","2"]]'),
      })
      .mockResolvedValueOnce({
        ok: true,
        body: createMockReadableStream('[["c","3"],["d","4"]]'),
      })
      .mockResolvedValueOnce({
        ok: true,
        body: createMockReadableStream('[["e","5"],["f","6"]]'),
      })
    vi.stubGlobal('fetch', mockFetch)

    const stream = fetchStream(multiLinkResult, mockAuth)
    const data = await collectStream(stream)
    const content = data.toString()

    // Verify order is preserved: chunk0 -> chunk1 -> chunk2
    const aPos = content.indexOf('"a"')
    const cPos = content.indexOf('"c"')
    const ePos = content.indexOf('"e"')

    expect(aPos).toBeLessThan(cPos)
    expect(cPos).toBeLessThan(ePos)

    // Verify all data is present
    expect(content).toContain('"a"')
    expect(content).toContain('"b"')
    expect(content).toContain('"c"')
    expect(content).toContain('"d"')
    expect(content).toContain('"e"')
    expect(content).toContain('"f"')

    // Verify fetch was called 3 times in order
    expect(mockFetch).toHaveBeenCalledTimes(3)
    expect(mockFetch).toHaveBeenNthCalledWith(
      1,
      'https://mock/chunk0.json',
      expect.any(Object)
    )
    expect(mockFetch).toHaveBeenNthCalledWith(
      2,
      'https://mock/chunk1.json',
      expect.any(Object)
    )
    expect(mockFetch).toHaveBeenNthCalledWith(
      3,
      'https://mock/chunk2.json',
      expect.any(Object)
    )
  })

  it('should fetch chunks when no external_links in initial result', async () => {
    const resultWithChunks: StatementResult = {
      statement_id: 'chunk-test',
      status: { state: 'SUCCEEDED' },
      manifest: {
        format: 'JSON_ARRAY',
        schema: { column_count: 1, columns: [] },
        total_chunk_count: 2,
      },
      // No result.external_links - need to fetch chunks
    }

    const mockFetch = vi
      .fn()
      // First chunk API call
      .mockResolvedValueOnce({
        ok: true,
        json: () =>
          Promise.resolve({
            chunk_index: 0,
            external_links: [
              {
                chunk_index: 0,
                external_link: 'https://mock/chunk0.json',
                row_count: 50,
                byte_count: 500,
                row_offset: 0,
                expiration: '2025-12-25T12:00:00Z',
              },
            ],
          }),
      })
      // Second chunk API call
      .mockResolvedValueOnce({
        ok: true,
        json: () =>
          Promise.resolve({
            chunk_index: 1,
            external_links: [
              {
                chunk_index: 1,
                external_link: 'https://mock/chunk1.json',
                row_count: 50,
                byte_count: 500,
                row_offset: 50,
                expiration: '2025-12-25T12:00:00Z',
              },
            ],
          }),
      })
      // External link fetches (from merge-streams)
      .mockResolvedValueOnce({
        ok: true,
        body: createMockReadableStream('[["a"],["b"]]'),
      })
      .mockResolvedValueOnce({
        ok: true,
        body: createMockReadableStream('[["c"],["d"]]'),
      })
    vi.stubGlobal('fetch', mockFetch)

    const stream = fetchStream(resultWithChunks, mockAuth)
    const data = await collectStream(stream)

    // merge-streams merges JSON arrays
    expect(data.toString()).toContain('"a"')
    expect(data.toString()).toContain('d')
  })
})
