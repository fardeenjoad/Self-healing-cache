import { describe, it, expect } from "vitest";
import fc from "fast-check";
import { ReplicationManager } from "../src/node/ReplicationManager.js";
import { Router } from "../src/node/Router.js";
import { KVStore } from "../src/core/kvstore.js";
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
});
