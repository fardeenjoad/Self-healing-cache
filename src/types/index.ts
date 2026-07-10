// Shared type definitions for self-healing-cache (Phase 1)
// These interfaces are imported by ring.ts and kvstore.ts to avoid type duplication.

export interface RingEntry {
    hash: bigint;
    nodeId: string;
}

export interface KVEntry {
    value: string;
    /** Absolute Unix timestamp in milliseconds at which this entry expires, or null if it never expires. */
    expiresAt: number | null;
}

// ── Phase 2 type definitions ──────────────────────────────────────────────────

/** Describes a single node in the cluster (Req 1.1). */
export interface NodeInfo {
    nodeId: string;
    host: string;
    port: number;
}

/** The static cluster topology — an ordered list of all NodeInfo entries (Req 1.2). */
export type ClusterConfig = NodeInfo[];

/** Wire-format command sent by a client to a cache node (Req 1.3, 1.5). */
export interface CacheRequest {
    command: "SET" | "GET" | "DEL" | "REPLICATE" | "REPLICATE_DEL";
    key: string;
    value?: string;
    ttl?: number;
    expiresAt?: number | null;
    isFallback?: boolean;
    /**
     * Optional correlation ID assigned by CacheClient.
     * TcpServer echoes this field back in the CacheResponse so that
     * concurrent in-flight requests can be matched to their promises.
     * Callers that do not need multiplexing may omit this field.
     */
    id?: string;
}

/**
 * Wire-format reply sent by a cache node (Req 1.4, 1.6).
 *
 * Discriminated by `ok`:
 *   - `ok: true`  → `value` MAY be present (GET hit); `error` is absent.
 *   - `ok: false` → `error` SHOULD be present; `value` is absent.
 *
 * `id` echoes the correlation ID from the originating CacheRequest (if present),
 * allowing CacheClient to route the response to the correct pending promise.
 */
export interface CacheResponse {
    ok: boolean;
    /** Present only when ok is true (e.g. a GET hit). */
    value?: string;
    /** Present only when ok is false; describes what went wrong. */
    error?: string;
    /** Echoed from CacheRequest.id for concurrent request multiplexing. */
    id?: string;
}
