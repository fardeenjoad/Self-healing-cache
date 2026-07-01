// Unit tests for ConsistentHashRing
// Validates: Requirements 6.1 – 6.10

import { describe, it, expect, beforeEach } from "vitest";
import { ConsistentHashRing } from "../src/core/ring.js";

describe("ConsistentHashRing", () => {
    // ─── Test 1: Single-node — all keys route to that node (Req 6.2) ─────────

    it("single-node: all keys route to that node", () => {
        const ring = new ConsistentHashRing();
        ring.addNode("node-A");

        const keys = Array.from({ length: 10 }, (_, i) => `key-${i}`);
        for (const key of keys) {
            expect(ring.getNode(key)).toBe("node-A");
        }
    });

    // ─── Test 2: Three-node — keys route to one of the three nodes (Req 6.3) ─

    it("three-node: keys route to one of the three nodes", () => {
        const ring = new ConsistentHashRing();
        ring.addNode("node-A");
        ring.addNode("node-B");
        ring.addNode("node-C");

        const validNodes = new Set(["node-A", "node-B", "node-C"]);
        for (let i = 0; i < 100; i++) {
            const result = ring.getNode(`key-${i}`);
            expect(result).not.toBeNull();
            expect(validNodes.has(result as string)).toBe(true);
        }
    });

    // ─── Test 3: Remove node — keys no longer route to removed node (Req 6.4) ─

    it("remove node: keys never route to removed node", () => {
        const ring = new ConsistentHashRing();
        ring.addNode("node-A");
        ring.addNode("node-B");
        ring.removeNode("node-B");

        for (let i = 0; i < 50; i++) {
            expect(ring.getNode(`key-${i}`)).not.toBe("node-B");
        }
    });

    // ─── Test 4: Empty ring returns null (Req 6.5) ───────────────────────────

    it("empty ring: getNode returns null", () => {
        const ring = new ConsistentHashRing();
        expect(ring.getNode("some-key")).toBeNull();
    });

    // ─── Test 5: Determinism — same key returns same node (Req 6.6) ──────────

    it("determinism: same key always returns the same node", () => {
        const ring = new ConsistentHashRing();
        ring.addNode("node-A");
        ring.addNode("node-B");
        ring.addNode("node-C");

        const first = ring.getNode("my-key");
        const second = ring.getNode("my-key");
        expect(first).toBe(second);
    });

    // ─── Test 6: Wrap-around / stress — 1000 keys never null (Req 6.7) ───────

    it("wrap-around / stress: 1000 keys never return null and distribution is correct", () => {
        const ring = new ConsistentHashRing();
        ring.addNode("node-A");
        ring.addNode("node-B");
        ring.addNode("node-C");

        // Confirm each node has exactly 200 virtual entries
        const dist = ring.getDistribution();
        expect(dist.get("node-A")).toBe(200);
        expect(dist.get("node-B")).toBe(200);
        expect(dist.get("node-C")).toBe(200);

        // 1000 distinct keys must all resolve to a non-null node without throwing
        for (let i = 0; i < 1000; i++) {
            let result: string | null;
            expect(() => {
                result = ring.getNode(`key-${i}`);
            }).not.toThrow();
            expect(ring.getNode(`key-${i}`)).not.toBeNull();
        }
    });

    // ─── Test 7: getNodes(key, 2) on 3-node ring returns 2 distinct (Req 6.8) ─

    it("getNodes(key, 2) on 3-node ring returns 2 distinct nodes", () => {
        const ring = new ConsistentHashRing();
        ring.addNode("node-A");
        ring.addNode("node-B");
        ring.addNode("node-C");

        const nodes = ring.getNodes("some-key", 2);
        expect(nodes).toHaveLength(2);
        expect(new Set(nodes).size).toBe(2); // all distinct
    });

    // ─── Test 8: getNodes(key, 5) on 3-node ring capped at 3 (Req 6.9) ───────

    it("getNodes(key, 5) on 3-node ring is capped at 3 distinct nodes", () => {
        const ring = new ConsistentHashRing();
        ring.addNode("node-A");
        ring.addNode("node-B");
        ring.addNode("node-C");

        const nodes = ring.getNodes("some-key", 5);
        expect(nodes).toHaveLength(3);
    });

    // ─── Test 9: getDistribution after 3 adds — 3 entries each 200 (Req 6.10) ─

    it("getDistribution after 3 adds: 3 entries each with count 200", () => {
        const ring = new ConsistentHashRing();
        ring.addNode("node-A");
        ring.addNode("node-B");
        ring.addNode("node-C");

        const dist = ring.getDistribution();
        expect(dist.size).toBe(3);
        expect(dist.get("node-A")).toBe(200);
        expect(dist.get("node-B")).toBe(200);
        expect(dist.get("node-C")).toBe(200);
    });
});
