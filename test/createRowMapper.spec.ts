import { describe, it, expect } from 'vitest'
import { createRowMapper } from '../src/createRowMapper.js'
import type { FetchRowsOptions, RowMapperOptions, StatementManifest } from '../src/types.js'

type Column = StatementManifest['schema']['columns'][number]

const baseSchema: StatementManifest['schema'] = {
  column_count: 1,
  columns: [],
}

function buildManifest(columns: Column[]): StatementManifest {
  return {
    format: 'JSON_ARRAY',
    schema: {
      column_count: columns.length,
      columns,
    },
    total_chunk_count: 1,
  }
}

function mapRow(
  columns: Column[],
  row: unknown[],
  format: FetchRowsOptions['format'] = 'JSON_OBJECT',
  options?: RowMapperOptions
) {
  const manifest = buildManifest(columns)
  const mapper = createRowMapper(manifest, format, options)
  return mapper(row)
}

describe('createRowMapper', () => {
  it('returns row arrays when format is JSON_ARRAY', () => {
    const columns: Column[] = [
      { name: 'a', type_text: 'INT', type_name: 'INT', position: 0 },
    ]
    const row = ['1']
    expect(mapRow(columns, row, 'JSON_ARRAY')).toEqual(row)
  })

  it('converts integer and bigint-like values', () => {
    const columns: Column[] = [
      { name: 'int_col', type_text: 'INT', type_name: 'INT', position: 0 },
      { name: 'big_small', type_text: 'BIGINT', type_name: 'BIGINT', position: 1 },
      { name: 'big_col', type_text: 'BIGINT', type_name: 'BIGINT', position: 2 },
    ]
    const row = ['42', '7', '9007199254740993']
    expect(mapRow(columns, row)).toEqual({
      int_col: 42,
      big_small: 7n,
      big_col: 9007199254740993n,
    })
  })

  it('applies encodeBigInt to bigint values', () => {
    const columns: Column[] = [
      { name: 'big_col', type_text: 'BIGINT', type_name: 'BIGINT', position: 0 },
      { name: 'nested', type_text: 'STRUCT<inner: BIGINT>', type_name: 'STRUCT', position: 1 },
    ]
    const row = ['9007199254740993', '{"inner":"42"}']
    const options: RowMapperOptions = {
      encodeBigInt: (value) => value.toString(),
    }
    expect(mapRow(columns, row, 'JSON_OBJECT', options)).toEqual({
      big_col: '9007199254740993',
      nested: { inner: '42' },
    })
  })

  it('converts float and double values to numbers', () => {
    const columns: Column[] = [
      { name: 'float_col', type_text: 'FLOAT', type_name: 'FLOAT', position: 0 },
      { name: 'double_col', type_text: 'DOUBLE', type_name: 'DOUBLE', position: 1 },
    ]
    const row = ['1.25', '2.5']
    expect(mapRow(columns, row)).toEqual({
      float_col: 1.25,
      double_col: 2.5,
    })
  })

  it('keeps datetime-like values as strings', () => {
    const columns: Column[] = [
      { name: 'd', type_text: 'DATE', type_name: 'DATE', position: 0 },
      { name: 't', type_text: 'TIME', type_name: 'TIME', position: 1 },
      { name: 'ts', type_text: 'TIMESTAMP', type_name: 'TIMESTAMP', position: 2 },
      {
        name: 'ts_ntz',
        type_text: 'TIMESTAMP_NTZ',
        type_name: 'TIMESTAMP_NTZ',
        position: 3,
      },
    ]
    const row = [
      '2024-01-02',
      '03:04:05',
      '2024-01-02T03:04:05.123Z',
      '2024-01-02T03:04:05.123',
    ]
    expect(mapRow(columns, row)).toEqual({
      d: '2024-01-02',
      t: '03:04:05',
      ts: '2024-01-02T03:04:05.123Z',
      ts_ntz: '2024-01-02T03:04:05.123',
    })
  })

  it('applies encodeTimestamp to TIMESTAMP types only', () => {
    const columns: Column[] = [
      { name: 'd', type_text: 'DATE', type_name: 'DATE', position: 0 },
      { name: 't', type_text: 'TIME', type_name: 'TIME', position: 1 },
      { name: 'ts', type_text: 'TIMESTAMP', type_name: 'TIMESTAMP', position: 2 },
      { name: 'ts_ltz', type_text: 'TIMESTAMP_LTZ', type_name: 'TIMESTAMP_LTZ', position: 3 },
    ]
    const row = [
      '2024-01-02',
      '03:04:05',
      '2024-01-02T03:04:05.123Z',
      '2024-01-02T03:04:05.123Z',
    ]
    const options: RowMapperOptions = {
      encodeTimestamp: (value) => `ts:${value}`,
    }
    expect(mapRow(columns, row, 'JSON_OBJECT', options)).toEqual({
      d: '2024-01-02',
      t: '03:04:05',
      ts: 'ts:2024-01-02T03:04:05.123Z',
      ts_ltz: 'ts:2024-01-02T03:04:05.123Z',
    })
  })

  it('converts boolean string values', () => {
    const columns: Column[] = [
      { name: 'flag', type_text: 'BOOLEAN', type_name: 'BOOLEAN', position: 0 },
    ]
    expect(mapRow(columns, ['true'])).toEqual({ flag: true })
    expect(mapRow(columns, ['false'])).toEqual({ flag: false })
  })

  it('converts decimal scale values to numbers', () => {
    const columns: Column[] = [
      {
        name: 'price',
        type_text: 'DECIMAL(10,2)',
        type_name: 'DECIMAL',
        position: 0,
        type_precision: 10,
        type_scale: 2,
      },
    ]
    expect(mapRow(columns, ['12.34'])).toEqual({ price: 12.34 })
  })

  it('maps nested structs and converts nested integers', () => {
    const nestedBigInt = 9007199254740993n
    const columns: Column[] = [
      {
        name: 'nested',
        type_text:
          'STRUCT<a: INT NOT NULL, b: STRUCT<c: STRING NOT NULL, big: BIGINT NOT NULL> NOT NULL>',
        type_name: 'STRUCT',
        position: 0,
      },
    ]
    const row = [
      '{"a":"1","b":{"c":"x","big":"9007199254740993"}}',
    ]
    expect(mapRow(columns, row)).toEqual({
      nested: {
        a: 1,
        b: {
          c: 'x',
          big: nestedBigInt,
        },
      },
    })
  })

  it('accepts struct values already parsed as objects', () => {
    const columns: Column[] = [
      {
        name: 'nested',
        type_text: 'STRUCT<a: INT NOT NULL>',
        type_name: 'STRUCT',
        position: 0,
      },
    ]
    expect(mapRow(columns, [{ a: '2' }])).toEqual({ nested: { a: 2 } })
  })

  it('throws when JSON parsing fails for struct values', () => {
    const columns: Column[] = [
      {
        name: 'nested',
        type_text: 'STRUCT<a: INT NOT NULL>',
        type_name: 'STRUCT',
        position: 0,
      },
    ]
    const row = ['{not-json']
    expect(() => mapRow(columns, row)).toThrow('Failed to parse JSON value')
  })

  it('falls back to raw values for unknown types', () => {
    const columns: Column[] = [
      { name: 'raw', type_text: 'BINARY', type_name: 'BINARY', position: 0 },
    ]
    expect(mapRow(columns, ['abc'])).toEqual({ raw: 'abc' })
  })

  it('parses ARRAY<STRUCT<...>> values', () => {
    const columns: Column[] = [
      {
        name: 'items',
        type_text: 'ARRAY<STRUCT<a: INT NOT NULL, b: STRING NOT NULL>>',
        type_name: 'ARRAY',
        position: 0,
      },
    ]
    const value = '[{\"a\":\"1\",\"b\":\"x\"},{\"a\":\"2\",\"b\":\"y\"}]'
    expect(mapRow(columns, [value])).toEqual({
      items: [
        { a: 1, b: 'x' },
        { a: 2, b: 'y' },
      ],
    })
  })

  it('parses ARRAY<ARRAY<STRUCT<...>>> values', () => {
    const columns: Column[] = [
      {
        name: 'matrix',
        type_text:
          'ARRAY<ARRAY<STRUCT<a: INT NOT NULL, b: STRING NOT NULL>>>',
        type_name: 'ARRAY',
        position: 0,
      },
    ]
    const value =
      '[[{\"a\":\"1\",\"b\":\"x\"}],[{\"a\":\"2\",\"b\":\"y\"}]]'
    expect(mapRow(columns, [value])).toEqual({
      matrix: [
        [{ a: 1, b: 'x' }],
        [{ a: 2, b: 'y' }],
      ],
    })
  })

  it('parses MAP<STRING, INT> values from JSON objects', () => {
    const columns: Column[] = [
      {
        name: 'tags',
        type_text: 'MAP<STRING, INT>',
        type_name: 'MAP',
        position: 0,
      },
    ]
    const value = '{"a":"1","b":"2"}'
    expect(mapRow(columns, [value])).toEqual({
      tags: {
        a: 1,
        b: 2,
      },
    })
  })

  it('parses MAP<INT, STRUCT<...>> values from entry arrays', () => {
    const columns: Column[] = [
      {
        name: 'lookup',
        type_text: 'MAP<INT, STRUCT<a: INT NOT NULL, b: STRING NOT NULL>>',
        type_name: 'MAP',
        position: 0,
      },
    ]
    const value = '[[\"1\", {\"a\":\"2\",\"b\":\"x\"}],[\"2\", {\"a\":\"3\",\"b\":\"y\"}]]'
    expect(mapRow(columns, [value])).toEqual({
      lookup: {
        '1': { a: 2, b: 'x' },
        '2': { a: 3, b: 'y' },
      },
    })
  })

  it('throws when JSON parsing fails for map values', () => {
    const columns: Column[] = [
      {
        name: 'broken',
        type_text: 'MAP<STRING, STRING>',
        type_name: 'MAP',
        position: 0,
      },
    ]
    const value = '{not-json'
    expect(() => mapRow(columns, [value])).toThrow('Failed to parse JSON value')
  })

  it('preserves nulls in nested arrays and structs', () => {
    const columns: Column[] = [
      {
        name: 'items',
        type_text: 'ARRAY<STRUCT<a: INT NOT NULL, b: STRING NOT NULL>>',
        type_name: 'ARRAY',
        position: 0,
      },
      {
        name: 'lookup',
        type_text: 'MAP<STRING, STRUCT<a: INT NOT NULL, b: STRING NOT NULL>>',
        type_name: 'MAP',
        position: 1,
      },
    ]
    const itemsValue = '[{\"a\":null,\"b\":\"x\"},null]'
    const lookupValue = '{\"x\":null,\"y\":{\"a\":null,\"b\":\"z\"}}'
    expect(mapRow(columns, [itemsValue, lookupValue])).toEqual({
      items: [{ a: null, b: 'x' }, null],
      lookup: {
        x: null,
        y: { a: null, b: 'z' },
      },
    })
  })

  it('maps undefined cells when row is shorter than columns', () => {
    const columns: Column[] = [
      { name: 'a', type_text: 'INT', type_name: 'INT', position: 0 },
      { name: 'b', type_text: 'STRING', type_name: 'STRING', position: 1 },
    ]
    expect(mapRow(columns, ['1'])).toEqual({ a: 1, b: undefined })
  })
})
