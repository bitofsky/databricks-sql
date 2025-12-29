import { DatabricksSqlError } from './errors.js'
import type {
  ColumnInfo,
  FetchRowsOptions,
  RowArray,
  RowObject,
  RowMapperOptions,
  StatementManifest,
} from './types.js'

type RowMapper = (row: RowArray) => RowArray | RowObject

type TypeDescriptor = {
  typeName: string
  typeText: string
  precision?: number
  scale?: number
  fields?: StructField[]
  elementType?: TypeDescriptor
  keyType?: TypeDescriptor
  valueType?: TypeDescriptor
}

type StructField = {
  name: string
  type: TypeDescriptor
}

// Type buckets used for value conversion decisions.
const INTEGER_TYPES = new Set(['TINYINT', 'SMALLINT', 'INT'])
const BIGINT_TYPES = new Set(['BIGINT', 'LONG'])
const FLOAT_TYPES = new Set(['FLOAT', 'DOUBLE'])
const BOOLEAN_TYPES = new Set(['BOOLEAN'])
const TIMESTAMP_TYPES = new Set(['TIMESTAMP', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ'])
const STRING_TYPES = new Set([
  'STRING',
  'DATE',
  'TIME',
])

/**
 * Create a row mapper that converts JSON_ARRAY rows into JSON_OBJECTs.
 * Datetime-like fields are preserved as strings to avoid locale/zone surprises.
 * DECIMAL values are converted to numbers to match the Databricks SDK behavior.
 */
export function createRowMapper(
  manifest: StatementManifest,
  format: FetchRowsOptions['format'],
  options: RowMapperOptions = {}
): RowMapper {
  if (format !== 'JSON_OBJECT')
    return (row) => row

  // Precompute per-column converters for fast row mapping.
  const columnConverters = manifest.schema.columns.map((column: ColumnInfo) => ({
    name: column.name,
    convert: createColumnConverter(column, options),
  }))

  return (row) => {
    const mapped: RowObject = {}
    for (let index = 0; index < columnConverters.length; index++) {
      const converter = columnConverters[index]
      if (!converter)
        continue

      const { name, convert } = converter
      if (name)
        mapped[name] = convert(row[index])
    }
    return mapped
  }
}

function createColumnConverter(
  column: ColumnInfo,
  options: RowMapperOptions
): (value: unknown) => unknown {
  const descriptor = parseColumnType(column)
  return (value) => convertValue(descriptor, value, options)
}

function parseColumnType(column: ColumnInfo): TypeDescriptor {
  if (column.type_name === 'STRUCT' || column.type_name === 'ARRAY' || column.type_name === 'MAP')
    return parseTypeDescriptor(column.type_text)

  if (column.type_name === 'DECIMAL')
    // Prefer precision/scale provided by the API when available.
    return createDecimalDescriptor({
      typeName: column.type_name,
      typeText: column.type_text,
    }, column.type_precision, column.type_scale)

  return {
    typeName: column.type_name,
    typeText: column.type_text,
  }
}

function parseTypeDescriptor(typeText: string): TypeDescriptor {
  const trimmed = typeText.trim()
  const typeName = getTypeName(trimmed)

  if (typeName === 'STRUCT')
    // STRUCT fields are parsed recursively from type_text.
    return {
      typeName,
      typeText: trimmed,
      fields: parseStructFields(trimmed),
    }

  if (typeName === 'ARRAY') {
    const elementTypeText = parseSingleTypeArgument(trimmed)
    const descriptor: TypeDescriptor = {
      typeName,
      typeText: trimmed,
    }
    if (elementTypeText)
      descriptor.elementType = parseTypeDescriptor(elementTypeText)
    return descriptor
  }

  if (typeName === 'MAP') {
    const [keyTypeText, valueTypeText] = parseTypeArguments(trimmed, 2)
    const descriptor: TypeDescriptor = {
      typeName,
      typeText: trimmed,
    }
    if (keyTypeText)
      descriptor.keyType = parseTypeDescriptor(keyTypeText)
    if (valueTypeText)
      descriptor.valueType = parseTypeDescriptor(valueTypeText)
    return descriptor
  }

  if (typeName === 'DECIMAL') {
    // DECIMAL(precision, scale) needs explicit parsing for integer conversion.
    const { precision, scale } = parseDecimalInfo(trimmed)
    return createDecimalDescriptor({ typeName, typeText: trimmed }, precision, scale)
  }

  return {
    typeName,
    typeText: trimmed,
  }
}

function getTypeName(typeText: string): string {
  return typeText.match(/^[A-Z_]+/)?.[0] ?? typeText
}

function parseDecimalInfo(typeText: string): { precision?: number; scale?: number } {
  const match = typeText.match(/DECIMAL\((\d+),\s*(\d+)\)/)
  if (!match)
    return {}

  return {
    precision: Number(match[1]),
    scale: Number(match[2]),
  }
}

function createDecimalDescriptor(
  base: Omit<TypeDescriptor, 'precision' | 'scale'>,
  precision?: number,
  scale?: number
): TypeDescriptor {
  const descriptor: TypeDescriptor = { ...base }
  if (precision !== undefined)
    descriptor.precision = precision
  if (scale !== undefined)
    descriptor.scale = scale
  return descriptor
}

function parseStructFields(typeText: string): StructField[] {
  const start = typeText.indexOf('<')
  const end = typeText.lastIndexOf('>')
  if (start === -1 || end === -1 || end <= start)
    return []

  const inner = typeText.slice(start + 1, end)
  // Split by commas only at the top level of nested type definitions.
  const parts = splitTopLevel(inner)
  const fields: StructField[] = []

  for (const part of parts) {
    const separatorIndex = part.indexOf(':')
    if (separatorIndex === -1)
      continue

    const name = part.slice(0, separatorIndex).trim()
    let fieldTypeText = part.slice(separatorIndex + 1).trim()
    fieldTypeText = stripNotNull(fieldTypeText)

    if (!name)
      continue

    fields.push({
      name,
      type: parseTypeDescriptor(fieldTypeText),
    })
  }

  return fields
}

function parseSingleTypeArgument(typeText: string): string | null {
  const [arg] = parseTypeArguments(typeText, 1)
  return arg ?? null
}

function parseTypeArguments(typeText: string, expectedCount: number): Array<string | undefined> {
  const start = typeText.indexOf('<')
  const end = typeText.lastIndexOf('>')
  if (start === -1 || end === -1 || end <= start)
    return []

  const inner = typeText.slice(start + 1, end)
  const parts = splitTopLevel(inner)
  if (parts.length < expectedCount)
    return parts

  return parts.slice(0, expectedCount).map((part) => stripNotNull(part.trim()))
}

function splitTopLevel(value: string): string[] {
  const result: string[] = []
  let current = ''
  let angleDepth = 0
  let parenDepth = 0

  for (const char of value) {
    if (char === '<') angleDepth++
    if (char === '>') angleDepth--
    if (char === '(') parenDepth++
    if (char === ')') parenDepth--

    if (char === ',' && angleDepth === 0 && parenDepth === 0) {
      result.push(current.trim())
      current = ''
      continue
    }

    current += char
  }

  if (current.trim().length > 0)
    result.push(current.trim())

  return result
}

function stripNotNull(typeText: string): string {
  let trimmed = typeText.trim()
  while (trimmed.endsWith('NOT NULL'))
    trimmed = trimmed.slice(0, -'NOT NULL'.length).trim()
  return trimmed
}

function convertValue(
  descriptor: TypeDescriptor,
  value: unknown,
  options: RowMapperOptions
): unknown {
  if (value === null || value === undefined)
    return value

  if (descriptor.typeName === 'STRUCT' && descriptor.fields)
    // STRUCT values are JSON strings in JSON_ARRAY format.
    return convertStructValue(descriptor.fields, value, options)

  if (descriptor.typeName === 'ARRAY' && descriptor.elementType)
    return convertArrayValue(descriptor.elementType, value, options)

  if (descriptor.typeName === 'MAP' && descriptor.keyType && descriptor.valueType)
    return convertMapValue(descriptor.keyType, descriptor.valueType, value, options)

  if (descriptor.typeName === 'DECIMAL')
    return convertNumber(value)

  if (INTEGER_TYPES.has(descriptor.typeName))
    return convertNumber(value)

  if (BIGINT_TYPES.has(descriptor.typeName))
    return convertInteger(value, options.encodeBigInt)

  if (FLOAT_TYPES.has(descriptor.typeName))
    return convertNumber(value)

  if (BOOLEAN_TYPES.has(descriptor.typeName))
    return convertBoolean(value)

  if (TIMESTAMP_TYPES.has(descriptor.typeName))
    return convertTimestamp(value, options.encodeTimestamp)

  if (STRING_TYPES.has(descriptor.typeName))
    return value

  return value
}

function convertStructValue(
  fields: StructField[],
  value: unknown,
  options: RowMapperOptions
): unknown {
  const raw = parseStructValue(value)
  if (!raw || typeof raw !== 'object' || Array.isArray(raw))
    return value

  // Apply nested field conversions based on the parsed STRUCT schema.
  const mapped: RowObject = {}
  for (const field of fields)
    mapped[field.name] = convertValue(field.type, (raw as RowObject)[field.name], options)

  return mapped
}

function convertArrayValue(
  elementType: TypeDescriptor,
  value: unknown,
  options: RowMapperOptions
): unknown {
  const raw = parseJsonValue(value)
  if (!Array.isArray(raw))
    return value

  return raw.map((entry) => convertValue(elementType, entry, options))
}

function convertMapValue(
  keyType: TypeDescriptor,
  valueType: TypeDescriptor,
  value: unknown,
  options: RowMapperOptions
): unknown {
  const raw = parseJsonValue(value)
  if (!raw || typeof raw !== 'object')
    return value

  if (Array.isArray(raw)) {
    const mapped: RowObject = {}
    for (const entry of raw) {
      if (!Array.isArray(entry) || entry.length < 2)
        continue
      const convertedKey = convertValue(keyType, entry[0], options)
      mapped[String(convertedKey)] = convertValue(valueType, entry[1], options)
    }
    return mapped
  }

  const mapped: RowObject = {}
  for (const [key, entryValue] of Object.entries(raw)) {
    const convertedKey = convertValue(keyType, key, options)
    mapped[String(convertedKey)] = convertValue(valueType, entryValue, options)
  }

  return mapped
}

function parseStructValue(value: unknown): RowObject | null {
  const parsed = parseJsonValue(value)
  if (parsed && typeof parsed === 'object' && !Array.isArray(parsed))
    return parsed as RowObject

  return parsed as RowObject | null
}

function parseJsonValue(value: unknown): unknown {
  if (typeof value === 'string') {
    try {
      return JSON.parse(value)
    } catch {
      throw new DatabricksSqlError('Failed to parse JSON value', 'INVALID_JSON')
    }
  }

  return value
}

function convertNumber(value: unknown): unknown {
  if (typeof value === 'number')
    return value

  if (typeof value === 'string') {
    const parsed = Number(value)
    return Number.isNaN(parsed) ? value : parsed
  }

  return value
}

function convertInteger(value: unknown, encodeBigInt?: (value: bigint) => unknown): unknown {
  if (typeof value === 'bigint')
    return encodeBigInt ? encodeBigInt(value) : value

  if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      const bigintValue = BigInt(value)
      return encodeBigInt ? encodeBigInt(bigintValue) : bigintValue
    }
    return value
  }

  if (typeof value === 'string') {
    try {
      // Preserve integer semantics for BIGINT/DECIMAL(scale=0) by returning bigint.
      const bigintValue = BigInt(value)
      return encodeBigInt ? encodeBigInt(bigintValue) : bigintValue
    } catch {
      return value
    }
  }

  return value
}

function convertTimestamp(
  value: unknown,
  encodeTimestamp?: (value: string) => unknown
): unknown {
  if (typeof value !== 'string')
    return value

  return encodeTimestamp ? encodeTimestamp(value) : value
}

function convertBoolean(value: unknown): unknown {
  if (typeof value === 'boolean')
    return value

  if (typeof value === 'string') {
    if (value === 'true') return true
    if (value === 'false') return false
  }

  return value
}
