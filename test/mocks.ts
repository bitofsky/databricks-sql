import type { AuthInfo, StatementResult } from '../src/types.js'

export const mockAuth: AuthInfo = {
  token: 'test-token',
  host: 'test.cloud.databricks.com',
  httpPath: '/sql/1.0/warehouses/abc123def456',
}

export const mockInlineResult: StatementResult = {
  statement_id: '01f0e17f-963b-11c1-a5bf-eab723674b6f',
  status: {
    state: 'SUCCEEDED',
  },
  manifest: {
    format: 'JSON_ARRAY',
    schema: {
      column_count: 2,
      columns: [
        { name: 'num', type_text: 'INT', type_name: 'INT', position: 0 },
        { name: 'str', type_text: 'STRING', type_name: 'STRING', position: 1 },
      ],
    },
    total_chunk_count: 1,
    chunks: [{ chunk_index: 0, row_offset: 0, row_count: 1 }],
    total_row_count: 1,
    truncated: false,
  },
  result: {
    data_array: [['1', 'hello']],
  },
}

export const mockExternalLinksResult: StatementResult = {
  statement_id: '01f0e17f-9702-1a78-b2e6-3d1ecf07ad65',
  status: {
    state: 'SUCCEEDED',
  },
  manifest: {
    format: 'JSON_ARRAY',
    schema: {
      column_count: 2,
      columns: [
        { name: 'id', type_text: 'BIGINT', type_name: 'LONG', position: 0 },
        { name: 'doubled', type_text: 'BIGINT', type_name: 'LONG', position: 1 },
      ],
    },
    total_chunk_count: 1,
    chunks: [{ chunk_index: 0, row_offset: 0, row_count: 100, byte_count: 1236 }],
    total_row_count: 100,
    total_byte_count: 1236,
    truncated: false,
  },
  result: {
    external_links: [
      {
        chunk_index: 0,
        row_offset: 0,
        row_count: 100,
        byte_count: 1236,
        external_link: 'https://mock-storage.example.com/results/chunk0.json',
        expiration: '2025-12-25T12:00:00.000Z',
      },
    ],
  },
}

export const mockPendingResult: StatementResult = {
  statement_id: '01f0e17f-pending-test',
  status: {
    state: 'PENDING',
  },
}

export const mockRunningResult: StatementResult = {
  statement_id: '01f0e17f-pending-test',
  status: {
    state: 'RUNNING',
  },
}

export const mockSucceededAfterPolling: StatementResult = {
  statement_id: '01f0e17f-pending-test',
  status: {
    state: 'SUCCEEDED',
  },
  manifest: {
    format: 'JSON_ARRAY',
    schema: {
      column_count: 1,
      columns: [{ name: 'result', type_text: 'INT', type_name: 'INT', position: 0 }],
    },
    total_chunk_count: 1,
    total_row_count: 1,
  },
  result: {
    data_array: [['42']],
  },
}

export const mockFailedResult: StatementResult = {
  statement_id: '01f0e17f-failed-test',
  status: {
    state: 'FAILED',
    error: {
      error_code: 'SYNTAX_ERROR',
      message: 'Syntax error in SQL statement',
    },
  },
}

// Mock external link data (JSON_ARRAY format)
export const mockExternalLinkData = [
  ['0', '0'],
  ['1', '2'],
  ['2', '4'],
  ['3', '6'],
  ['4', '8'],
]
