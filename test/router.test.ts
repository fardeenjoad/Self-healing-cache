/**
 * Router unit tests — no real TCP connections.
 * CacheClient is fully mocked so all tests run in-process.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { ConsistentHashRing } from "../src/core/ring.js";
import { KVStore } from "../src/core/kvstore.js";
import { CLUSTER_CONFIG } from "../src/config/cluster.js";

// ── Mock CacheClient before importing Router ──────────────────────────────────
// vi.mock hoists this above all imports, so Router picks up the mock when it
// imports CacheClient.
vi.mock("../src/client/CacheClient.js", () => {
    const MockCacheClient = vi.fn().mockImplementation(() => ({
        connect: vi.fn().mockResolvedValue(undefined),
        send: vi.fn().mockResolvedValue({ ok: true }),
        disconnect: vi.fn().mockResolvedValue(undefined),
    }));
    return { CacheClient: MockCacheClient };
});

// Import Router AFTER the mock is declared
import { Router } from "../src/node/Router.js";
import { CacheClient } from "../src/client/CacheClient.js";

// ── Helper: find a key that hashes to a specific node ────────────────────────
function findKeyForNode(targetNodeId: string): string {
    const ring = new ConsistentHashRing();
    for (const info of CLUSTER_CONFIG) ring.addNode(info.nodeId);
    for (let i = 0; i < 10_000; i++) {
        const key = `test-key-${i}`;
        if (ring.getNode(key) === targetNodeId) return key;
    }
    throw new Error(`Could not find a key that hashes to ${targetNodeId}`);
}

// Precompute one local key (node-a) and one remote key (node-b) once
const localKey = findKeyForNode("node-a");
const remoteKey = findKeyForNode("node-b");

// ── Test suite ────────────────────────────────────────────────────────────────
describe("Router", () => {
    let store: KVStore;

    beforeEach(() => {
        store = new KVStore();
        vi.clearAllMocks();
    });

    afterEach(() => {
        store.stopSweeper();
    });

    // ── Req 8.7: Ring population ──────────────────────────────────────────────
    it("registers all 3 cluster nodes in the ring (200 virtual nodes each)", () => {
        // Verify using a fresh ring mirroring what Router builds internally
        const ring = new ConsistentHashRing();
        for (const info of CLUSTER_CONFIG) ring.addNode(info.nodeId);
        const dist = ring.getDistribution();

        expect(dist.size).toBe(3);
        expect(dist.get("node-a")).toBe(200);
        expect(dist.get("node-b")).toBe(200);
        expect(dist.get("node-c")).toBe(200);
    });

    // ── Req 8.2: Local GET hit ────────────────────────────────────────────────
    it("executes GET locally and returns stored value when key hashes to local node", async () => {
        store.set(localKey, "hello");
        const router = new Router("node-a", CLUSTER_CONFIG, store);

        const res = await router.route({ command: "GET", key: localKey });

        // Result must be the locally stored value
        expect(res).toEqual({ ok: true, value: "hello" });
        // Verify by also confirming the store still has the key (local path hit)
        expect(store.get(localKey)).toBe("hello");
    });

    // ── Req 8.2: Local GET miss ───────────────────────────────────────────────
    it("returns ok:true with no value for a local GET miss", async () => {
        const router = new Router("node-a", CLUSTER_CONFIG, store);
        const res = await router.route({ command: "GET", key: localKey });

        expect(res.ok).toBe(true);
        expect(res.value).toBeUndefined();
    });

    // ── Req 8.2: Local SET ────────────────────────────────────────────────────
    it("executes SET locally and persists value in store", async () => {
        const router = new Router("node-a", CLUSTER_CONFIG, store);
        const res = await router.route({ command: "SET", key: localKey, value: "world" });

        expect(res).toEqual({ ok: true });
        expect(store.get(localKey)).toBe("world");
    });

    // ── Req 8.2: Local DEL ────────────────────────────────────────────────────
    it("executes DEL locally and removes the key from store", async () => {
        store.set(localKey, "to-delete");
        const router = new Router("node-a", CLUSTER_CONFIG, store);
        const res = await router.route({ command: "DEL", key: localKey });

        expect(res).toEqual({ ok: true });
        expect(store.get(localKey)).toBeNull();
    });

    // ── Req 8.3: Forward to remote node ──────────────────────────────────────
    it("forwards request to peer CacheClient when key hashes to a remote node", async () => {
        const mockResponse = { ok: true as const, value: "remote-value" };
        const mockSend = vi.fn().mockResolvedValue(mockResponse);

        vi.mocked(CacheClient).mockImplementation(() => ({
            connect: vi.fn().mockResolvedValue(undefined),
            send: mockSend,
            disconnect: vi.fn().mockResolvedValue(undefined),
        }));

        const router = new Router("node-a", CLUSTER_CONFIG, store);
        const res = await router.route({ command: "GET", key: remoteKey });

        expect(res).toEqual(mockResponse);
        // send must have been called — proves forwarding happened
        expect(mockSend).toHaveBeenCalled();
    });

    // ── Req 8.4: Forward error propagation ───────────────────────────────────
    it("returns ok:false with error message when forward to remote node fails", async () => {
        vi.mocked(CacheClient).mockImplementation(() => ({
            connect: vi.fn().mockResolvedValue(undefined),
            send: vi.fn().mockRejectedValue(new Error("connection refused")),
            disconnect: vi.fn().mockResolvedValue(undefined),
        }));

        const router = new Router("node-a", CLUSTER_CONFIG, store);
        const res = await router.route({ command: "GET", key: remoteKey });

        expect(res.ok).toBe(false);
        expect(res.error).toContain("connection refused");
    });

    // ── Req 8.5: SET missing value ────────────────────────────────────────────
    it("returns SET requires value error without touching the store", async () => {
        const setSpy = vi.spyOn(store, "set");
        const router = new Router("node-a", CLUSTER_CONFIG, store);

        // Cast to bypass TypeScript's type check — simulates a malformed request
        const res = await router.route({ command: "SET", key: localKey } as never);

        expect(res).toEqual({ ok: false, error: "SET requires value" });
        expect(setSpy).not.toHaveBeenCalled();
    });

    // ── Req 8.6: Unknown command ──────────────────────────────────────────────
    it("returns unknown command error for unrecognised command values", async () => {
        const router = new Router("node-a", CLUSTER_CONFIG, store);
        const res = await router.route({ command: "FLUSH" as never, key: "any-key" });

        expect(res).toEqual({ ok: false, error: "unknown command" });
    });
});
