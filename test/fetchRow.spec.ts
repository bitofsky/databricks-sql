import { describe, it, expect, vi, afterEach } from 'vitest'
import { fetchAll, fetchRow } from '../src/api'
import type { RowArray, StatementResult } from '../src/types.js'
import {
  mockAuth,
  mockInlineResult,
  mockExternalLinksResult,
  mockPendingResult,
} from './mocks.js'
import {
  createExternalLinkInfo,
  createGetChunkResponse,
  createStreamResponse,
} from './testUtil.js'

describe('fetchRow', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('should process inline data_array with onEachRow callback', async () => {
    const rows: RowArray[] = []
    await fetchRow(mockInlineResult, mockAuth, {
      onEachRow: (row) => rows.push(row as RowArray),
    })

    expect(rows).toEqual([['1', 'hello']])
  })

  it('should map rows to JSON objects when format is JSON_OBJECT', async () => {
    const rows: Array<Record<string, unknown>> = []
    await fetchRow(mockInlineResult, mockAuth, {
      format: 'JSON_OBJECT',
      onEachRow: (row) => rows.push(row as Record<string, unknown>),
    })

    expect(rows).toEqual([{ num: 1, str: 'hello' }])
  })

  it('should process multiple rows in inline result', async () => {
    const multiRowResult: StatementResult = {
      statement_id: 'multi-row-test',
      status: { state: 'SUCCEEDED' },
      manifest: {
        format: 'JSON_ARRAY',
        schema: { column_count: 2, columns: [] },
        total_chunk_count: 1,
        total_row_count: 3,
      },
      result: {
        data_array: [
          ['1', 'first'],
          ['2', 'second'],
          ['3', 'third'],
        ],
      },
    }

    const rows: RowArray[] = []
    await fetchRow(multiRowResult, mockAuth, {
      onEachRow: (row) => rows.push(row as RowArray),
    })

    expect(rows).toHaveLength(3)
    expect(rows[0]).toEqual(['1', 'first'])
    expect(rows[1]).toEqual(['2', 'second'])
    expect(rows[2]).toEqual(['3', 'third'])
  })

  it('should validate result state and manifest', async () => {
    const noManifestResult: StatementResult = {
      statement_id: 'test',
      status: { state: 'SUCCEEDED' },
    }

    await expect(fetchRow(mockPendingResult, mockAuth)).rejects.toThrow(
      'Cannot fetch from non-succeeded statement: PENDING'
    )
    await expect(fetchRow(noManifestResult, mockAuth)).rejects.toThrow(
      'Statement result has no manifest'
    )
  })

  it('should throw error for external_links with non-JSON_ARRAY format', async () => {
    const csvExternalLinksResult: StatementResult = {
      ...mockExternalLinksResult,
      manifest: {
        ...mockExternalLinksResult.manifest!,
        format: 'CSV',
      },
    }

    await expect(fetchRow(csvExternalLinksResult, mockAuth)).rejects.toThrow(
      'fetchRow only supports JSON_ARRAY for external_links'
    )
  })

  it('should handle empty data_array', async () => {
    const emptyResult: StatementResult = {
      statement_id: 'empty-test',
      status: { state: 'SUCCEEDED' },
      manifest: {
        format: 'JSON_ARRAY',
        schema: { column_count: 1, columns: [] },
        total_chunk_count: 1,
        total_row_count: 0,
      },
      result: {
        data_array: [],
      },
    }

    const rows: RowArray[] = []
    await fetchRow(emptyResult, mockAuth, {
      onEachRow: (row) => rows.push(row as RowArray),
    })

    expect(rows).toHaveLength(0)
  })

  it('should process external links data_array', async () => {
    const mockFetch = vi.fn()
      .mockResolvedValueOnce(createStreamResponse('[["1","2"],["3","4"]]'))
    vi.stubGlobal('fetch', mockFetch)

    const rows: RowArray[] = []
    await fetchRow(mockExternalLinksResult, mockAuth, {
      onEachRow: (row) => rows.push(row as RowArray),
    })

    expect(rows).toEqual([['1', '2'], ['3', '4']])
  })

  it('should map external links rows to JSON objects', async () => {
    const mockFetch = vi.fn()
      .mockResolvedValueOnce(createStreamResponse('[["1","2"],["3","4"]]'))
    vi.stubGlobal('fetch', mockFetch)

    const rows: Array<Record<string, unknown>> = []
    await fetchRow(mockExternalLinksResult, mockAuth, {
      format: 'JSON_OBJECT',
      onEachRow: (row) => rows.push(row as Record<string, unknown>),
    })

    expect(rows).toEqual([
      { id: 1n, doubled: 2n },
      { id: 3n, doubled: 4n },
    ])
  })

  it('should map external links rows with datetime and nested decimals', async () => {
    const bigIntValue = 9007199254740993n
    const complexExternalLinksResult: StatementResult = {
      statement_id: 'external-complex-test',
      status: { state: 'SUCCEEDED' },
      manifest: {
        format: 'JSON_ARRAY',
        schema: {
          column_count: 9,
          columns: [
            { name: 'num', type_text: 'INT', type_name: 'INT', position: 0 },
            { name: 'str', type_text: 'STRING', type_name: 'STRING', position: 1 },
            {
              name: 'big_int',
              type_text: 'BIGINT',
              type_name: 'BIGINT',
              position: 2,
            },
            {
              name: 'price',
              type_text: 'DECIMAL(10,2)',
              type_name: 'DECIMAL',
              position: 3,
              type_precision: 10,
              type_scale: 2,
            },
            { name: 'd', type_text: 'DATE', type_name: 'DATE', position: 4 },
            { name: 't', type_text: 'STRING', type_name: 'STRING', position: 5 },
            {
              name: 'dt',
              type_text: 'TIMESTAMP',
              type_name: 'TIMESTAMP',
              position: 6,
            },
            {
              name: 'dt_ntz',
              type_text: 'TIMESTAMP_NTZ',
              type_name: 'TIMESTAMP_NTZ',
              position: 7,
            },
            {
              name: 'nested',
              type_text:
                'STRUCT<a: INT NOT NULL, b: STRUCT<c: STRING NOT NULL, big: BIGINT NOT NULL, price: DECIMAL(10,2) NOT NULL> NOT NULL>',
              type_name: 'STRUCT',
              position: 8,
            },
          ],
        },
        total_chunk_count: 1,
        total_row_count: 1,
      },
      result: {
        external_links: [
          {
            chunk_index: 0,
            row_offset: 0,
            row_count: 1,
            byte_count: 123,
            external_link: 'https://mock-storage.example.com/results/chunk0.json',
            expiration: '2025-12-25T12:00:00.000Z',
          },
        ],
      },
    }

    const payload = JSON.stringify([
      [
        '42',
        'hello',
        '9007199254740993',
        '12.34',
        '2024-01-02',
        '03:04:05',
        '2024-01-02T03:04:05.123Z',
        '2024-01-02T03:04:05.123',
        '{"a":"1","b":{"c":"x","big":"9007199254740993","price":"56.78"}}',
      ],
    ])

    const mockFetch = vi.fn()
      .mockResolvedValueOnce(createStreamResponse(payload))
    vi.stubGlobal('fetch', mockFetch)

    const rows = await fetchAll(complexExternalLinksResult, mockAuth, {
      format: 'JSON_OBJECT',
    })

    expect(rows).toEqual([
      {
        num: 42,
        str: 'hello',
        big_int: bigIntValue,
        price: 12.34,
        d: '2024-01-02',
        t: '03:04:05',
        dt: '2024-01-02T03:04:05.123Z',
        dt_ntz: '2024-01-02T03:04:05.123',
        nested: {
          a: 1,
          b: {
            c: 'x',
            big: bigIntValue,
            price: 56.78,
          },
        },
      },
    ])
  })

  it('should process external links across multiple chunks when only first link is present', async () => {
    const partialLinksResult: StatementResult = {
      statement_id: 'external-multi-chunk-test',
      status: { state: 'SUCCEEDED' },
      manifest: {
        format: 'JSON_ARRAY',
        schema: { column_count: 1, columns: [] },
        total_chunk_count: 3,
        total_row_count: 6,
      },
      result: {
        external_links: [
          createExternalLinkInfo({
            chunk_index: 0,
            row_offset: 0,
            row_count: 2,
            byte_count: 10,
            external_link: 'https://mock/chunk0.json',
          }),
        ],
      },
    }

    const links = [
      createExternalLinkInfo({
        chunk_index: 1,
        row_offset: 2,
        row_count: 2,
        byte_count: 10,
        external_link: 'https://mock/chunk1.json',
      }),
      createExternalLinkInfo({
        chunk_index: 2,
        row_offset: 4,
        row_count: 2,
        byte_count: 10,
        external_link: 'https://mock/chunk2.json',
      }),
    ]

    const mockFetch = vi.fn()
      .mockResolvedValueOnce(createGetChunkResponse([links[0]!], 1))
      .mockResolvedValueOnce(createGetChunkResponse([links[1]!], 2))
      .mockResolvedValueOnce(createStreamResponse('[["0"],["1"]]'))
      .mockResolvedValueOnce(createStreamResponse('[["2"],["3"]]'))
      .mockResolvedValueOnce(createStreamResponse('[["4"],["5"]]'))
    vi.stubGlobal('fetch', mockFetch)

    const rows = await fetchAll(partialLinksResult, mockAuth)

    expect(rows).toHaveLength(6)
    expect(rows[0]).toEqual(['0'])
    expect(rows[5]).toEqual(['5'])
  })

  it('should abort when signal is aborted before processing', async () => {
    const controller = new AbortController()
    controller.abort()

    await expect(
      fetchRow(mockInlineResult, mockAuth, { signal: controller.signal })
    ).rejects.toThrow('Aborted')
  })

  it('should abort during row processing', async () => {
    const multiRowResult: StatementResult = {
      statement_id: 'abort-test',
      status: { state: 'SUCCEEDED' },
      manifest: {
        format: 'JSON_ARRAY',
        schema: { column_count: 1, columns: [] },
        total_chunk_count: 1,
        total_row_count: 100,
      },
      result: {
        data_array: Array.from({ length: 100 }, (_, i) => [`${i}`]),
      },
    }

    const controller = new AbortController()
    const rows: RowArray[] = []

    const promise = fetchRow(multiRowResult, mockAuth, {
      signal: controller.signal,
      onEachRow: (row) => {
        rows.push(row as RowArray)
        if (rows.length === 5)
          controller.abort()
      },
    })

    await expect(promise).rejects.toThrow('Aborted')
    expect(rows.length).toBe(5)
  })

  it('should process multiple chunks', async () => {
    const multiChunkResult: StatementResult = {
      statement_id: 'multi-chunk-test',
      status: { state: 'SUCCEEDED' },
      manifest: {
        format: 'JSON_ARRAY',
        schema: { column_count: 1, columns: [] },
        total_chunk_count: 3,
        total_row_count: 6,
      },
      result: {
        data_array: [['chunk0-row0'], ['chunk0-row1']],
      },
    }

    // Mock getChunk API calls
    const mockFetch = vi.fn()
      .mockResolvedValueOnce({
        ok: true,
        json: () =>
          Promise.resolve({
            chunk_index: 1,
            row_offset: 2,
            row_count: 2,
            data_array: [['chunk1-row0'], ['chunk1-row1']],
          }),
      })
      .mockResolvedValueOnce({
        ok: true,
        json: () =>
          Promise.resolve({
            chunk_index: 2,
            row_offset: 4,
            row_count: 2,
            data_array: [['chunk2-row0'], ['chunk2-row1']],
          }),
      })
    vi.stubGlobal('fetch', mockFetch)

    const rows: RowArray[] = []
    await fetchRow(multiChunkResult, mockAuth, {
      onEachRow: (row) => rows.push(row as RowArray),
    })

    expect(rows).toHaveLength(6)
    expect(rows[0]).toEqual(['chunk0-row0'])
    expect(rows[1]).toEqual(['chunk0-row1'])
    expect(rows[2]).toEqual(['chunk1-row0'])
    expect(rows[3]).toEqual(['chunk1-row1'])
    expect(rows[4]).toEqual(['chunk2-row0'])
    expect(rows[5]).toEqual(['chunk2-row1'])
  })

  it('should throw error if chunk contains external_links', async () => {
    const mixedResult: StatementResult = {
      statement_id: 'mixed-test',
      status: { state: 'SUCCEEDED' },
      manifest: {
        format: 'JSON_ARRAY',
        schema: { column_count: 1, columns: [] },
        total_chunk_count: 2,
      },
      result: {
        data_array: [['inline-row']],
      },
    }

    const link = createExternalLinkInfo({
      chunk_index: 1,
      row_offset: 1,
      row_count: 10,
      byte_count: 100,
      external_link: 'https://mock/chunk1.json',
    })
    const mockFetch = vi.fn().mockResolvedValueOnce(createGetChunkResponse([link], 1))
    vi.stubGlobal('fetch', mockFetch)

    await expect(fetchRow(mixedResult, mockAuth)).rejects.toThrow(
      'fetchRow only supports INLINE results. Chunk contains external_links.'
    )
  })

  it('should work without onEachRow callback', async () => {
    // Should not throw error even without callback
    await expect(fetchRow(mockInlineResult, mockAuth)).resolves.toBeUndefined()
  })
})

describe('fetchAll', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('should return all rows as an array', async () => {
    const rows = await fetchAll(mockInlineResult, mockAuth)
    expect(rows).toEqual([['1', 'hello']])
  })

  it('should return JSON objects when format is JSON_OBJECT', async () => {
    const rows = await fetchAll(mockInlineResult, mockAuth, {
      format: 'JSON_OBJECT',
    })
    expect(rows).toEqual([{ num: 1, str: 'hello' }])
  })

  it('should map complex schema rows to JSON objects as returned', async () => {
    const bigIntValue = 9007199254740993n
    const complexResult: StatementResult = {
      statement_id: 'complex-types-test',
      status: { state: 'SUCCEEDED' },
      manifest: {
        format: 'JSON_ARRAY',
        schema: {
          column_count: 9,
          columns: [
            { name: 'num', type_text: 'INT', type_name: 'INT', position: 0 },
            { name: 'str', type_text: 'STRING', type_name: 'STRING', position: 1 },
            {
              name: 'big_int',
              type_text: 'BIGINT',
              type_name: 'BIGINT',
              position: 2,
            },
            {
              name: 'price',
              type_text: 'DECIMAL(10,2)',
              type_name: 'DECIMAL',
              position: 3,
              type_precision: 10,
              type_scale: 2,
            },
            { name: 'd', type_text: 'DATE', type_name: 'DATE', position: 4 },
            { name: 't', type_text: 'STRING', type_name: 'STRING', position: 5 },
            {
              name: 'dt',
              type_text: 'TIMESTAMP',
              type_name: 'TIMESTAMP',
              position: 6,
            },
            {
              name: 'dt_ntz',
              type_text: 'TIMESTAMP_NTZ',
              type_name: 'TIMESTAMP_NTZ',
              position: 7,
            },
            {
              name: 'nested',
              type_text: 'STRUCT<a: INT NOT NULL, b: STRUCT<c: STRING NOT NULL, big: BIGINT NOT NULL, price: DECIMAL(10,2) NOT NULL> NOT NULL>',
              type_name: 'STRUCT',
              position: 8,
            },
          ],
        },
        total_chunk_count: 1,
        total_row_count: 1,
      },
      result: {
        data_array: [
          [
            '42',
            'hello',
            '9007199254740993',
            '12.34',
            '2024-01-02',
            '03:04:05',
            '2024-01-02T03:04:05.123Z',
            '2024-01-02T03:04:05.123',
            '{"a":"1","b":{"c":"x","big":"9007199254740993","price":"56.78"}}',
          ],
        ],
      },
    }

    const rows = await fetchAll(complexResult, mockAuth, {
      format: 'JSON_OBJECT',
    })

    expect(rows).toEqual([
      {
        num: 42,
        str: 'hello',
        big_int: bigIntValue,
        price: 12.34,
        d: '2024-01-02',
        t: '03:04:05',
        dt: '2024-01-02T03:04:05.123Z',
        dt_ntz: '2024-01-02T03:04:05.123',
        nested: {
          a: 1,
          b: {
            c: 'x',
            big: bigIntValue,
            price: 56.78,
          },
        },
      },
    ])
  })

  it('should return multiple rows', async () => {
    const multiRowResult: StatementResult = {
      statement_id: 'multi-row-test',
      status: { state: 'SUCCEEDED' },
      manifest: {
        format: 'JSON_ARRAY',
        schema: { column_count: 2, columns: [] },
        total_chunk_count: 1,
        total_row_count: 3,
      },
      result: {
        data_array: [
          ['1', 'first'],
          ['2', 'second'],
          ['3', 'third'],
        ],
      },
    }

    const rows = await fetchAll(multiRowResult, mockAuth)
    expect(rows).toHaveLength(3)
    expect(rows).toEqual([
      ['1', 'first'],
      ['2', 'second'],
      ['3', 'third'],
    ])
  })

  it('should return empty array for empty result', async () => {
    const emptyResult: StatementResult = {
      statement_id: 'empty-test',
      status: { state: 'SUCCEEDED' },
      manifest: {
        format: 'JSON_ARRAY',
        schema: { column_count: 1, columns: [] },
        total_chunk_count: 1,
        total_row_count: 0,
      },
      result: {
        data_array: [],
      },
    }

    const rows = await fetchAll(emptyResult, mockAuth)
    expect(rows).toEqual([])
  })

  it('should support abort signal', async () => {
    const controller = new AbortController()
    controller.abort()

    await expect(
      fetchAll(mockInlineResult, mockAuth, { signal: controller.signal })
    ).rejects.toThrow('Aborted')
  })

  it('should throw error for external_links result', async () => {
    const csvExternalLinksResult: StatementResult = {
      ...mockExternalLinksResult,
      manifest: {
        ...mockExternalLinksResult.manifest!,
        format: 'CSV',
      },
    }

    await expect(fetchAll(csvExternalLinksResult, mockAuth)).rejects.toThrow(
      'fetchRow only supports JSON_ARRAY for external_links'
    )
  })
})
