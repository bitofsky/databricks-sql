import { describe, it, expect, vi, afterEach } from 'vitest'
import { mergeExternalLinks } from '../src/api'
import type { MergeExternalLinksResult, StatementResult } from '../src/types.js'
import {
  mockAuth,
  mockExternalLinksResult,
  mockInlineResult,
  mockExternalLinkData,
} from './mocks.js'
import { createMockReadableStream, collectStream } from './testUtil.js'

describe('mergeExternalLinks', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('should return original for inline data', async () => {
    const mockCallback = vi.fn()

    const result = await mergeExternalLinks(mockInlineResult, mockAuth, {
      mergeStreamToExternalLink: mockCallback,
    })

    expect(result).toBe(mockInlineResult)
    expect(mockCallback).not.toHaveBeenCalled()
  })

  it('should return original for empty result', async () => {
    const emptyResult: StatementResult = {
      statement_id: 'empty-test',
      status: { state: 'SUCCEEDED' },
      manifest: {
        format: 'JSON_ARRAY',
        schema: { column_count: 1, columns: [] },
        total_chunk_count: 0,
      },
    }
    const mockCallback = vi.fn()

    const result = await mergeExternalLinks(emptyResult, mockAuth, {
      mergeStreamToExternalLink: mockCallback,
    })

    expect(result).toBe(emptyResult)
    expect(mockCallback).not.toHaveBeenCalled()
  })

  it('should merge external links and return updated StatementResult', async () => {
    const mockFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      body: createMockReadableStream(JSON.stringify(mockExternalLinkData)),
    })
    vi.stubGlobal('fetch', mockFetch)

    const uploadResult: MergeExternalLinksResult = {
      externalLink: 'https://my-storage.com/merged.json',
      byte_count: 500,
      expiration: '2025-12-31T00:00:00Z',
    }

    const mockCallback = vi.fn().mockResolvedValue(uploadResult)

    const result = await mergeExternalLinks(mockExternalLinksResult, mockAuth, {
      mergeStreamToExternalLink: mockCallback,
      forceMerge: true,
    })

    // Verify callback was called
    expect(mockCallback).toHaveBeenCalledTimes(1)

    // Verify updated StatementResult
    expect(result.statement_id).toBe(mockExternalLinksResult.statement_id)
    expect(result.status).toBe(mockExternalLinksResult.status)
    expect(result.manifest?.total_chunk_count).toBe(1)
    expect(result.manifest?.total_byte_count).toBe(500)
    expect(result.manifest?.chunks).toHaveLength(1)
    expect(result.manifest?.chunks?.[0]).toEqual({
      chunk_index: 0,
      row_offset: 0,
      row_count: 100,
      byte_count: 500,
    })

    // Verify external_links
    expect(result.result?.external_links).toHaveLength(1)
    expect(result.result?.external_links?.[0]).toEqual({
      chunk_index: 0,
      row_offset: 0,
      row_count: 100,
      byte_count: 500,
      external_link: 'https://my-storage.com/merged.json',
      expiration: '2025-12-31T00:00:00Z',
    })
  })

  it('should pass merged stream to callback', async () => {
    const mockFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      body: createMockReadableStream(JSON.stringify(mockExternalLinkData)),
    })
    vi.stubGlobal('fetch', mockFetch)

    let receivedData: string | undefined

    const mockCallback = vi.fn().mockImplementation(async (stream) => {
      const buffer = await collectStream(stream)
      receivedData = buffer.toString()
      return {
        externalLink: 'https://example.com/merged.json',
        byte_count: buffer.length,
        expiration: '2025-12-31T00:00:00Z',
      }
    })

    await mergeExternalLinks(mockExternalLinksResult, mockAuth, {
      mergeStreamToExternalLink: mockCallback,
      forceMerge: true,
    })

    // Verify callback received the merged data
    expect(receivedData).toBeDefined()
    const parsed = JSON.parse(receivedData!)
    expect(parsed).toEqual(mockExternalLinkData)
  })

  it('should handle abort signal', async () => {
    const controller = new AbortController()

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

    // Callback that tries to read the stream - will throw on abort
    const mockCallback = vi.fn().mockImplementation(async (stream) => {
      await collectStream(stream)
      return {
        externalLink: 'https://example.com/merged.json',
        byte_count: 0,
        expiration: '2025-12-31T00:00:00Z',
      }
    })

    // Abort immediately
    controller.abort()

    await expect(
      mergeExternalLinks(mockExternalLinksResult, mockAuth, {
        signal: controller.signal,
        mergeStreamToExternalLink: mockCallback,
        forceMerge: true,
      })
    ).rejects.toThrow(/abort/i)
  })

  it('should propagate callback errors', async () => {
    const mockFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      body: createMockReadableStream('[]'),
    })
    vi.stubGlobal('fetch', mockFetch)

    const mockCallback = vi.fn().mockRejectedValue(new Error('Upload failed'))

    await expect(
      mergeExternalLinks(mockExternalLinksResult, mockAuth, {
        mergeStreamToExternalLink: mockCallback,
        forceMerge: true,
      })
    ).rejects.toThrow('Upload failed')
  })

  it('should preserve original manifest properties', async () => {
    const mockFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      body: createMockReadableStream('[]'),
    })
    vi.stubGlobal('fetch', mockFetch)

    const uploadResult: MergeExternalLinksResult = {
      externalLink: 'https://example.com/merged.json',
      byte_count: 100,
      expiration: '2025-12-31T00:00:00Z',
    }

    const result = await mergeExternalLinks(mockExternalLinksResult, mockAuth, {
      mergeStreamToExternalLink: vi.fn().mockResolvedValue(uploadResult),
      forceMerge: true,
    })

    // Verify original manifest properties are preserved
    expect(result.manifest?.format).toBe(mockExternalLinksResult.manifest?.format)
    expect(result.manifest?.schema).toBe(mockExternalLinksResult.manifest?.schema)
    expect(result.manifest?.total_row_count).toBe(
      mockExternalLinksResult.manifest?.total_row_count
    )
  })

  it('should return original for single external link by default', async () => {
    const mockCallback = vi.fn()

    const result = await mergeExternalLinks(mockExternalLinksResult, mockAuth, {
      mergeStreamToExternalLink: mockCallback,
    })

    expect(result).toBe(mockExternalLinksResult)
    expect(mockCallback).not.toHaveBeenCalled()
  })
})
