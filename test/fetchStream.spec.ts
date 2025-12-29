import { describe, it, expect, vi, afterEach } from 'vitest'
import { fetchStream } from '../src/api'
import type { StatementResult } from '../src/types.js'
import { buildUrl } from '../src/util.js'
import {
  mockAuth,
  mockExternalLinksResult,
  mockExternalLinkData,
} from './mocks.js'
import {
  collectStream,
  createExternalLinkInfo,
  createGetChunkResponse,
  createStreamResponse,
} from './testUtil.js'

describe('fetchStream', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('should validate result state and manifest', () => {
    const pendingResult: StatementResult = {
      statement_id: 'test',
      status: { state: 'PENDING' },
    }
    const noManifestResult: StatementResult = {
      statement_id: 'test',
      status: { state: 'SUCCEEDED' },
    }

    expect(() => fetchStream(pendingResult, mockAuth)).toThrow(
      'Cannot fetch from non-succeeded statement: PENDING'
    )
    expect(() => fetchStream(noManifestResult, mockAuth)).toThrow(
      'Statement result has no manifest'
    )
  })

  it('should stream data from external links', async () => {
    const mockFetch = vi.fn()
      .mockResolvedValueOnce(createStreamResponse(JSON.stringify(mockExternalLinkData)))
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

    const mockFetch = vi.fn().mockImplementation(
      () =>
        new Promise((resolve) => {
          setTimeout(() => {
            resolve(createStreamResponse(JSON.stringify(mockExternalLinkData)))
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
          createExternalLinkInfo({
            chunk_index: 0,
            row_offset: 0,
            row_count: 5,
            byte_count: 100,
            external_link: 'https://mock/chunk0.json',
          }),
        ],
      },
    }

    const links = [
      createExternalLinkInfo({
        chunk_index: 0,
        row_offset: 0,
        row_count: 5,
        byte_count: 100,
        external_link: 'https://mock/chunk0.json',
      }),
      createExternalLinkInfo({
        chunk_index: 1,
        row_offset: 5,
        row_count: 5,
        byte_count: 100,
        external_link: 'https://mock/chunk1.json',
      }),
      createExternalLinkInfo({
        chunk_index: 2,
        row_offset: 10,
        row_count: 5,
        byte_count: 100,
        external_link: 'https://mock/chunk2.json',
      }),
    ]

    const mockFetch = vi.fn()
      .mockResolvedValueOnce(createGetChunkResponse([links[1]!], 1))
      .mockResolvedValueOnce(createGetChunkResponse([links[2]!], 2))
      .mockResolvedValueOnce(createStreamResponse('[["a","1"],["b","2"]]'))
      .mockResolvedValueOnce(createStreamResponse('[["c","3"],["d","4"]]'))
      .mockResolvedValueOnce(createStreamResponse('[["e","5"],["f","6"]]'))
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

    const expectedChunkUrls = [
      buildUrl(mockAuth.host, `/api/2.0/sql/statements/${multiLinkResult.statement_id}/result/chunks/1`),
      buildUrl(mockAuth.host, `/api/2.0/sql/statements/${multiLinkResult.statement_id}/result/chunks/2`),
    ]
    const expectedExternalUrls = links.map((link) => link.external_link)
    const calledUrls = mockFetch.mock.calls.map((call) => call[0] as string)

    expect(calledUrls.slice(0, 2)).toEqual(expectedChunkUrls)
    expect(calledUrls.slice(2)).toEqual(expectedExternalUrls)
  })

  it('should fetch missing chunks when only first external link is present', async () => {
    const resultWithChunks: StatementResult = {
      statement_id: 'chunk-test',
      status: { state: 'SUCCEEDED' },
      manifest: {
        format: 'JSON_ARRAY',
        schema: { column_count: 1, columns: [] },
        total_chunk_count: 2,
      },
      result: {
        external_links: [
          createExternalLinkInfo({
            chunk_index: 0,
            row_offset: 0,
            row_count: 50,
            byte_count: 500,
            external_link: 'https://mock/chunk0.json',
          }),
        ],
      },
    }

    const links = [
      createExternalLinkInfo({
        chunk_index: 1,
        row_offset: 50,
        row_count: 50,
        byte_count: 500,
        external_link: 'https://mock/chunk1.json',
      }),
    ]

    const mockFetch = vi.fn()
      .mockResolvedValueOnce(createGetChunkResponse([links[0]!], 1))
      .mockResolvedValueOnce(createStreamResponse('[["a"],["b"]]'))
      .mockResolvedValueOnce(createStreamResponse('[["c"],["d"]]'))
    vi.stubGlobal('fetch', mockFetch)

    const stream = fetchStream(resultWithChunks, mockAuth)
    const data = await collectStream(stream)

    // merge-streams merges JSON arrays
    expect(data.toString()).toContain('"a"')
    expect(data.toString()).toContain('d')
  })
})
