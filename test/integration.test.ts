/**
 * Multi-node integration tests for self-healing-cache Phase 2.
 *
 * Spins up all three CacheNode instances in-process on ports 17001–17003
 * (offset from production ports to avoid conflict with running Docker containers).
 * Uses real TCP sockets — no mocking.
 *
 * Requirements: 9.1 – 9.7
 */
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { CacheNode } from "../src/node/CacheNode.js";
import { CacheClient } from "../src/client/CacheClient.js";
import { ConsistentHashRing } from "../src/core/ring.js";
import type { ClusterConfig } from "../src/types/index.js";

// ── Test cluster config — ports 17001-17003 ───────────────────────────────────
const TEST_CONFIG: ClusterConfig = [
    { nodeId: "node-a", host: "0.0.0.0", port: 17001 },
    { nodeId: "node-b", host: "0.0.0.0", port: 17002 },
    { nodeId: "node-c", host: "0.0.0.0", port: 17003 },
];

// ── Helper: find a key that hashes to a specific node ────────────────────────
function findKeyForNode(targetNodeId: string): string {
    const ring = new ConsistentHashRing();
    for (const info of TEST_CONFIG) ring.addNode(info.nodeId);
    for (let i = 0; i < 10_000; i++) {
        const key = `integ-key-${i}`;
        if (ring.getNode(key) === targetNodeId) return key;
    }
    throw new Error(`No key found for node ${targetNodeId} after 10k tries`);
}

// Precompute keys for each node — used across multiple tests
const keyForA = findKeyForNode("node-a");
const keyForB = findKeyForNode("node-b");
const keyForC = findKeyForNode("node-c");

// ── Cluster state shared across all tests ────────────────────────────────────
let nodes: CacheNode[];
let clients: CacheClient[];          // clients[0]=→nodeA, [1]=→nodeB, [2]=→nodeC

// ── Lifecycle ─────────────────────────────────────────────────────────────────
beforeAll(async () => {
    // Start all three nodes
    nodes = [
        new CacheNode("node-a", TEST_CONFIG),
        new CacheNode("node-b", TEST_CONFIG),
        new CacheNode("node-c", TEST_CONFIG),
    ];
    await Promise.all(nodes.map((n) => n.start()));

    // Connect one CacheClient per node
    clients = [
        new CacheClient("127.0.0.1", 17001),
        new CacheClient("127.0.0.1", 17002),
        new CacheClient("127.0.0.1", 17003),
    ];
    await Promise.all(clients.map((c) => c.connect()));
}, 10_000);

afterAll(async () => {
    // Disconnect clients first, then stop nodes
    await Promise.all(clients.map((c) => c.disconnect()));
    await Promise.all(nodes.map((n) => n.stop()));
}, 10_000);

// ── Tests ─────────────────────────────────────────────────────────────────────

describe("Integration: multi-node cluster", () => {

    // Req 9.2 — SET on node-a for a key that hashes to node-b; GET directly on node-b
    it("SET via node-a for a key hashing to node-b is retrievable directly from node-b", async () => {
        const setRes = await clients[0].send({ command: "SET", key: keyForB, value: "cross-node-val" });
        expect(setRes.ok).toBe(true);

        const getRes = await clients[1].send({ command: "GET", key: keyForB });
        expect(getRes.ok).toBe(true);
        expect(getRes.value).toBe("cross-node-val");
    });

    // Req 9.3 — GET from any node for a key on a different node returns the correct value
    it("GET from node-c for a key stored on node-a returns the correct value", async () => {
        await clients[0].send({ command: "SET", key: keyForA, value: "node-a-value" });

        // Ask node-c — it must forward to node-a internally
        const res = await clients[2].send({ command: "GET", key: keyForA });
        expect(res.ok).toBe(true);
        expect(res.value).toBe("node-a-value");
    });

    // Req 9.4 — DEL from any node; subsequent GET returns no value
    it("DEL via node-b removes the key; subsequent GET returns ok:true with no value", async () => {
        // Set a key that lives on node-c, delete it via node-b
        await clients[2].send({ command: "SET", key: keyForC, value: "to-be-deleted" });

        const delRes = await clients[1].send({ command: "DEL", key: keyForC });
        expect(delRes.ok).toBe(true);

        const getRes = await clients[0].send({ command: "GET", key: keyForC });
        expect(getRes.ok).toBe(true);
        expect(getRes.value).toBeUndefined();
    });

    // Req 9.5 — TTL expiry: use real timer (fake timers conflict with TCP I/O)
    it("key with ttl:1 expires after ~1 second (real timer)", async () => {
        const ttlKey = keyForA;
        const setRes = await clients[0].send({ command: "SET", key: ttlKey, value: "expires-soon", ttl: 1 });
        expect(setRes.ok).toBe(true);

        // Value should be present immediately
        const beforeRes = await clients[0].send({ command: "GET", key: ttlKey });
        expect(beforeRes.ok).toBe(true);
        expect(beforeRes.value).toBe("expires-soon");

        // Wait 1100ms for TTL to elapse (real wall-clock time)
        await new Promise<void>((resolve) => setTimeout(resolve, 1100));

        const afterRes = await clients[0].send({ command: "GET", key: ttlKey });
        expect(afterRes.ok).toBe(true);
        expect(afterRes.value).toBeUndefined();
    }, 5_000);

    // Req 9.6 — 50 distinct keys SET via node-a are distributed across ≥2 nodes
    it("50 keys set via node-a are distributed across at least 2 of the 3 nodes", async () => {
        const ring = new ConsistentHashRing();
        for (const info of TEST_CONFIG) ring.addNode(info.nodeId);

        const nodeHits = new Set<string>();
        for (let i = 0; i < 50; i++) {
            const key = `dist-key-${i}`;
            await clients[0].send({ command: "SET", key, value: `v${i}` });
            const home = ring.getNode(key);
            if (home) nodeHits.add(home);
        }

        expect(nodeHits.size).toBeGreaterThanOrEqual(2);
    }, 15_000);

    // Req 9.7 — same key SET via node-a, GET via node-c returns the same value
    it("same key SET via node-a and GET via node-c returns identical value", async () => {
        const key = keyForB; // any key; cross-node path is the important thing
        await clients[0].send({ command: "SET", key, value: "consistency-check" });

        const res = await clients[2].send({ command: "GET", key });
        expect(res.ok).toBe(true);
        expect(res.value).toBe("consistency-check");
    });
});
