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
