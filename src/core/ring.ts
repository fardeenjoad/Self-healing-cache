import type { RingEntry } from "../types/index.js";
import { hashString } from "../utils/hash.js";

/*
 * CONSISTENT HASHING vs MODULO HASHING, and the role of VIRTUAL NODES
 * -----------------------------------------------------------------------
 *
 * MODULO HASHING
 *   The naive approach maps a key to a node with:
 *
 *     nodeIndex = hash(key) % N
 *
 *   This works fine while N is constant, but when a node is added or removed
 *   the modulus changes. Because almost every possible remainder is affected,
 *   roughly (N-1)/N of all keys are remapped to different nodes — a disruptive
 *   "thundering herd" of cache misses whenever the cluster changes size.
 *
 * CONSISTENT HASHING
 *   Places both nodes and keys on a conceptual circular keyspace (the "hash
 *   ring") whose positions span [0, 2^160 − 1] (SHA-1 range). A key is
 *   owned by the first node whose hash position is greater than or equal to
 *   the key's hash, walking clockwise; if no such node exists the ring wraps
 *   around to index 0.
 *
 *   When a single node is added or removed, only the keys whose hash falls
 *   between the affected node and its predecessor need to be remapped — roughly
 *   1/N of all keys, regardless of cluster size. All other key-to-node
 *   assignments remain stable.
 *
 * VIRTUAL NODES
 *   With a small number of physical nodes, their hash positions tend to cluster
 *   unevenly around the ring, causing some nodes to own a disproportionately
 *   large arc and others a tiny one — uneven load.
 *
 *   Virtual nodes fix this: each physical node is represented by 200 synthetic
 *   ring positions, hashed as `nodeId#0`, `nodeId#1`, … `nodeId#199`. With 200
 *   points per node spread pseudo-randomly across the ring, each physical node
 *   ends up owning roughly equal arcs, producing near-uniform key distribution
 *   even with very few physical nodes.
 */

/**
 * A consistent hash ring that maps arbitrary string keys to logical node
 * identifiers using SHA-1 hashing and virtual nodes.
 *
 * Each physical node is represented by {@link ConsistentHashRing.VIRTUAL_NODES_PER_NODE}
 * synthetic ring positions to achieve near-uniform key distribution. Key
 * lookup uses binary search for O(log n) performance, where n is the total
 * number of virtual node entries.
 */
export class ConsistentHashRing {
    /** Internal ring array, always kept sorted by `hash` ascending. */
    private ring: RingEntry[] = [];

    /** Number of virtual ring positions created per physical node. */
    static readonly VIRTUAL_NODES_PER_NODE = 200;

    /**
     * Adds a physical node to the ring by inserting
     * {@link ConsistentHashRing.VIRTUAL_NODES_PER_NODE} virtual node entries
     * (hashed as `nodeId#0` … `nodeId#199`) and re-sorting the ring.
     *
     * If `nodeId` already has entries in the ring, 200 additional entries are
     * appended (duplicates are permitted per spec).
     *
     * @param nodeId - The identifier of the physical node to add.
     */
    addNode(nodeId: string): void {
        for (let i = 0; i < ConsistentHashRing.VIRTUAL_NODES_PER_NODE; i++) {
            const hash = hashString(`${nodeId}#${i}`);
            this.ring.push({ hash, nodeId });
        }
        this.ring.sort((a, b) => (a.hash < b.hash ? -1 : a.hash > b.hash ? 1 : 0));
    }

    /**
     * Removes all virtual node entries for `nodeId` from the ring.
     *
     * If `nodeId` has no entries in the ring this is a no-op; no error is
     * thrown. The remaining entries retain their existing sort order without
     * a re-sort.
     *
     * @param nodeId - The identifier of the physical node to remove.
     */
    removeNode(nodeId: string): void {
        this.ring = this.ring.filter((entry) => entry.nodeId !== nodeId);
    }

    /**
     * Returns the starting ring index for a given key hash using binary search.
     * Wraps around to 0 if no entry has hash >= keyHash.
     * Assumes ring is non-empty.
     */
    private findStartIndex(keyHash: bigint): number {
        let lo = 0;
        let hi = this.ring.length - 1;
        let result = -1;
        while (lo <= hi) {
            const mid = Math.floor((lo + hi) / 2);
            if (this.ring[mid].hash >= keyHash) {
                result = mid;
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
        }
        return result === -1 ? 0 : result;
    }

    /**
     * Looks up the node responsible for `key` using a clockwise binary search.
     *
     * The key is hashed with SHA-1 and the ring is searched for the first
     * entry whose hash is ≥ the key's hash. If the key's hash exceeds every
     * entry's hash the ring wraps around and returns the node at index 0.
     *
     * @param key - The cache key to look up.
     * @returns The `nodeId` of the owning node, or `null` if the ring is empty.
     */
    getNode(key: string): string | null {
        if (this.ring.length === 0) return null;
        const keyHash = hashString(key);
        const index = this.findStartIndex(keyHash);
        return this.ring[index].nodeId;
    }

    /**
     * Returns up to `count` distinct physical node IDs responsible for `key`,
     * walking clockwise from the key's position on the ring.
     *
     * Useful for replication: e.g. `getNodes(key, 3)` returns the primary
     * node and two replicas. If the ring contains fewer than `count` distinct
     * physical nodes, all distinct nodes are returned.
     *
     * @param key   - The cache key to look up.
     * @param count - The maximum number of distinct nodes to return.
     * @returns An ordered array of distinct `nodeId` strings, or `[]` if the
     *          ring is empty or `count ≤ 0`.
     */
    getNodes(key: string, count: number): string[] {
        if (this.ring.length === 0 || count <= 0) return [];

        const startIndex = this.findStartIndex(hashString(key));
        const seen = new Set<string>();
        const result: string[] = [];

        for (let i = 0; i < this.ring.length; i++) {
            const index = (startIndex + i) % this.ring.length;
            const nodeId = this.ring[index].nodeId;
            if (!seen.has(nodeId)) {
                seen.add(nodeId);
                result.push(nodeId);
                if (result.length === count) break;
            }
        }

        return result;
    }

    /**
     * Returns the current distribution of virtual nodes across physical nodes.
     *
     * Each key in the returned map is a `nodeId` that currently has entries in
     * the ring; each value is the count of virtual node entries for that node.
     * For any node added exactly once via {@link addNode}, that count will be
     * exactly {@link ConsistentHashRing.VIRTUAL_NODES_PER_NODE}.
     *
     * @returns A `Map<string, number>` mapping each `nodeId` to its virtual
     *          node count.
     */
    getDistribution(): Map<string, number> {
        return this.ring.reduce((map, entry) => {
            map.set(entry.nodeId, (map.get(entry.nodeId) ?? 0) + 1);
            return map;
        }, new Map<string, number>());
    }
}


