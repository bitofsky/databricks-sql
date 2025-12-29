import type { ExternalLinkInfo } from '../src/types.js'

import { Writable } from 'node:stream'
import { pipeline } from 'node:stream/promises'

export function createMockReadableStream(data: string): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) {
      controller.enqueue(new TextEncoder().encode(data))
      controller.close()
    },
  })
}

const DEFAULT_EXTERNAL_LINK_EXPIRATION = '2025-12-25T12:00:00Z'

export function createExternalLinkInfo(
  overrides: Partial<ExternalLinkInfo> & { external_link: string }
): ExternalLinkInfo {
  return {
    chunk_index: overrides.chunk_index ?? 0,
    row_offset: overrides.row_offset ?? 0,
    row_count: overrides.row_count ?? 100,
    byte_count: overrides.byte_count ?? 1236,
    external_link: overrides.external_link,
    expiration: overrides.expiration ?? DEFAULT_EXTERNAL_LINK_EXPIRATION,
  }
}

export function createGetChunkResponse(
  external_links: ExternalLinkInfo[],
  chunk_index?: number
): { ok: true; json: () => Promise<{ chunk_index: number; external_links: ExternalLinkInfo[] }> } {
  const resolvedChunkIndex = chunk_index ?? external_links[0]?.chunk_index ?? 0
  return {
    ok: true,
    json: () =>
      Promise.resolve({
        chunk_index: resolvedChunkIndex,
        external_links,
      }),
  }
}

export function createStreamResponse(
  data: string
): { ok: true; body: ReadableStream<Uint8Array> } {
  return {
    ok: true,
    body: createMockReadableStream(data),
  }
}

export async function collectStream(stream: NodeJS.ReadableStream): Promise<Buffer> {
  const chunks: Buffer[] = []
  const writable = new Writable({
    write(chunk, _encoding, callback) {
      chunks.push(Buffer.from(chunk))
      callback()
    },
  })
  await pipeline(stream, writable)
  return Buffer.concat(chunks)
}
