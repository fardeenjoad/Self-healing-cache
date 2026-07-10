import { describe, it, expect, vi, beforeAll, afterAll } from "vitest";
import fc from "fast-check";
import { ReplicationManager } from "../src/node/ReplicationManager.js";
import { Router } from "../src/node/Router.js";
import { KVStore } from "../src/core/kvstore.js";
import { CacheNode } from "../src/node/CacheNode.js";
import { CacheClient } from "../src/client/CacheClient.js";
import type { ClusterConfig } from "../src/types/index.js";

describe("ReplicationManager (Unit)", () => {
    // Feature: self-healing-cache-phase3, Property 8: enqueue/drain round-trip
    it("enqueue/drain round-trip (Property 8)", () => {
        fc.assert(
            fc.property(
                fc.array(fc.string(), { minLength: 1, maxLength: 50 }),
                (keys) => {
                    const manager = new ReplicationManager();
                    for (const key of keys) {
                        manager.enqueue(key);
                    }
                    const drained = manager.drainQueue();
                    expect(new Set(drained)).toEqual(new Set(keys));
                    expect(manager.queueSize()).toBe(0);
                }
            ),
            { numRuns: 100 }
        );
    });

    // Feature: self-healing-cache-phase3, Property 9: enqueue deduplication
    it("enqueue deduplication (Property 9)", () => {
        fc.assert(
            fc.property(
                fc.string(),
                fc.integer({ min: 1, max: 20 }),
                (key, times) => {
                    const manager = new ReplicationManager();
                    for (let i = 0; i < times; i++) {
                        manager.enqueue(key);
                    }
                    expect(manager.queueSize()).toBe(1);
                    expect(manager.drainQueue()).toEqual([key]);
                }
            ),
            { numRuns: 100 }
        );
    });
});

describe("Router Replication Helpers (Unit)", () => {
    const TEST_CONFIG: ClusterConfig = [
        { nodeId: "node-a", host: "127.0.0.1", port: 17001 },
        { nodeId: "node-b", host: "127.0.0.1", port: 17002 },
        { nodeId: "node-c", host: "127.0.0.1", port: 17003 },
    ];

    // Feature: self-healing-cache-phase3, Property 10: getReplicaNodes returns non-local nodes
    it("getReplicaNodes returns non-local nodes (Property 10)", () => {
        const store = new KVStore();
        store.stopSweeper();
        const router = new Router("node-a", TEST_CONFIG, store);

        fc.assert(
            fc.property(
                fc.string(),
                (key) => {
                    const replicas = (router as any).getReplicaNodes(key);
                    expect(replicas).toHaveLength(2);
                    for (const node of replicas) {
                        expect(node.nodeId).not.toBe("node-a");
                        expect(TEST_CONFIG.find(c => c.nodeId === node.nodeId)).toBeDefined();
                    }
                }
            ),
            { numRuns: 100 }
        );
    });

    // Feature: self-healing-cache-phase3, Property 7: Replica fallback returns value and enqueues key
    it("Replica fallback returns value and enqueues key (Property 7)", async () => {
        await fc.assert(
            fc.asyncProperty(
                fc.string({ minLength: 1 }),
                fc.string(),
                async (key, value) => {
                    const store = new KVStore();
                    try {
                        const router = new Router("node-a", TEST_CONFIG, store);
                        vi.spyOn((router as any).ring, "getNode").mockReturnValue("node-a");
                        expect(store.get(key)).toBeNull();

                        const forwardSpy = vi.spyOn(router as any, "forwardToPeer")
                            .mockResolvedValue({ ok: true, value });

                        const res = await router.route({ command: "GET", key });

                        expect(res).toEqual({ ok: true, value });
                        expect(forwardSpy).toHaveBeenCalled();
                        expect(router.getRepairQueue().has(key)).toBe(true);
                    } finally {
                        store.stopSweeper();
                    }
                }
            ),
            { numRuns: 50 }
        );
    });
});

describe("Replication Integration", () => {
    const INT_CONFIG: ClusterConfig = [
        { nodeId: "node-a", host: "127.0.0.1", port: 17101 },
        { nodeId: "node-b", host: "127.0.0.1", port: 17102 },
        { nodeId: "node-c", host: "127.0.0.1", port: 17103 },
    ];

    let nodes: CacheNode[];
    let clients: CacheClient[];

    beforeAll(async () => {
        nodes = [
            new CacheNode("node-a", INT_CONFIG),
            new CacheNode("node-b", INT_CONFIG),
            new CacheNode("node-c", INT_CONFIG),
        ];
        await Promise.all(nodes.map((n) => n.start()));

        clients = [
            new CacheClient("127.0.0.1", 17101),
            new CacheClient("127.0.0.1", 17102),
            new CacheClient("127.0.0.1", 17103),
        ];
        await Promise.all(clients.map((c) => c.connect()));
    });

    afterAll(async () => {
        await Promise.all(clients.map((c) => c.disconnect()));
        await Promise.all(nodes.map((n) => n.stop()));
    });

    function getKeyForNode(targetNodeId: string, suffix: string): string {
        const ring = (nodes[0] as any).router.ring;
        for (let i = 0; i < 10000; i++) {
            const key = `rep-key-${suffix}-${i}`;
            if (ring.getNode(key) === targetNodeId) {
                return key;
            }
        }
        throw new Error(`No key for ${targetNodeId}`);
    }

    it("async SET replication propagates to all 3 nodes", async () => {
        const key = getKeyForNode("node-a", "t1");
        const setRes = await clients[0].send({ command: "SET", key, value: "hello-rep" });
        expect(setRes.ok).toBe(true);

        await new Promise((resolve) => setTimeout(resolve, 50));

        const resA = await clients[0].send({ command: "GET", key, isFallback: true });
        const resB = await clients[1].send({ command: "GET", key, isFallback: true });
        const resC = await clients[2].send({ command: "GET", key, isFallback: true });

        expect(resA.value).toBe("hello-rep");
        expect(resB.value).toBe("hello-rep");
        expect(resC.value).toBe("hello-rep");
    });

    it("Property 4: async SET replication convergence", async () => {
        await fc.assert(
            fc.asyncProperty(
                fc.string({ minLength: 1 }),
                fc.string(),
                async (keySuffix, value) => {
                    const key = `p4-key-${keySuffix}`;
                    const setRes = await clients[0].send({ command: "SET", key, value });
                    expect(setRes.ok).toBe(true);

                    await new Promise((resolve) => setTimeout(resolve, 50));

                    const resA = await clients[0].send({ command: "GET", key, isFallback: true });
                    const resB = await clients[1].send({ command: "GET", key, isFallback: true });
                    const resC = await clients[2].send({ command: "GET", key, isFallback: true });

                    expect(resA.value).toBe(value);
                    expect(resB.value).toBe(value);
                    expect(resC.value).toBe(value);
                }
            ),
            { numRuns: 20 }
        );
    });

    it("TTL replication stores identical expiresAt across all 3 nodes", async () => {
        const key = getKeyForNode("node-b", "t2");
        const setRes = await clients[1].send({ command: "SET", key, value: "ttl-val", ttl: 10 });
        expect(setRes.ok).toBe(true);

        await new Promise((resolve) => setTimeout(resolve, 50));

        const expA = (nodes[0] as any).kvStore.getExpiresAt(key);
        const expB = (nodes[1] as any).kvStore.getExpiresAt(key);
        const expC = (nodes[2] as any).kvStore.getExpiresAt(key);

        expect(expB).not.toBeNull();
        expect(expA).toBe(expB);
        expect(expC).toBe(expB);
    });

    it("Property 5: TTL timestamp consistency across replicas", async () => {
        await fc.assert(
            fc.asyncProperty(
                fc.string({ minLength: 1 }),
                fc.string(),
                fc.integer({ min: 10, max: 100 }),
                async (keySuffix, value, ttl) => {
                    const key = `p5-key-${keySuffix}`;
                    const setRes = await clients[0].send({ command: "SET", key, value, ttl });
                    expect(setRes.ok).toBe(true);

                    await new Promise((resolve) => setTimeout(resolve, 50));

                    const expA = (nodes[0] as any).kvStore.getExpiresAt(key);
                    const expB = (nodes[1] as any).kvStore.getExpiresAt(key);
                    const expC = (nodes[2] as any).kvStore.getExpiresAt(key);

                    expect(expA).not.toBeNull();
                    expect(expB).toBe(expA);
                    expect(expC).toBe(expA);
                }
            ),
            { numRuns: 20 }
        );
    });

    it("synchronous DEL replication deletes the key on all 3 nodes", async () => {
        const key = getKeyForNode("node-c", "t3");
        await clients[2].send({ command: "SET", key, value: "del-val" });

        await new Promise((resolve) => setTimeout(resolve, 50));

        const delRes = await clients[0].send({ command: "DEL", key });
        expect(delRes.ok).toBe(true);

        const resA = await clients[0].send({ command: "GET", key, isFallback: true });
        const resB = await clients[1].send({ command: "GET", key, isFallback: true });
        const resC = await clients[2].send({ command: "GET", key, isFallback: true });

        expect(resA.value).toBeUndefined();
        expect(resB.value).toBeUndefined();
        expect(resC.value).toBeUndefined();
    });

    it("Property 6: synchronous DEL removes key from all nodes", async () => {
        await fc.assert(
            fc.asyncProperty(
                fc.string({ minLength: 1 }),
                fc.string(),
                async (keySuffix, value) => {
                    const key = `p6-key-${keySuffix}`;
                    await clients[0].send({ command: "SET", key, value });

                    await new Promise((resolve) => setTimeout(resolve, 50));

                    const delRes = await clients[0].send({ command: "DEL", key });
                    expect(delRes.ok).toBe(true);

                    const resA = await clients[0].send({ command: "GET", key, isFallback: true });
                    const resB = await clients[1].send({ command: "GET", key, isFallback: true });
                    const resC = await clients[2].send({ command: "GET", key, isFallback: true });

                    expect(resA.value).toBeUndefined();
                    expect(resB.value).toBeUndefined();
                    expect(resC.value).toBeUndefined();
                }
            ),
            { numRuns: 20 }
        );
    });

    it("GET replica fallback recovers value and enqueues to repair queue", async () => {
        const key = getKeyForNode("node-a", "t4");
        (nodes[1] as any).kvStore.set(key, "fallback-val");

        const getRes = await clients[0].send({ command: "GET", key });
        expect(getRes.ok).toBe(true);
        expect(getRes.value).toBe("fallback-val");

        const repairQueue = (nodes[0] as any).router.getRepairQueue();
        expect(repairQueue.has(key)).toBe(true);
    });
});
