import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import fc from "fast-check";
import { KVStore } from "../src/core/kvstore.js";

describe("KVStore", () => {
    let store: KVStore;

    beforeEach(() => {
        vi.useFakeTimers();
        store = new KVStore();
    });

    afterEach(() => {
        store.stopSweeper();
        vi.useRealTimers();
    });

    // Req 7.3 — set with no TTL, get returns the stored value
    it("no-TTL set/get returns the stored value", () => {
        store.set("k", "v");
        expect(store.get("k")).toBe("v");
    });

    // Req 7.4 — get on a key that was never set returns null
    it("get on a missing key returns null", () => {
        expect(store.get("never-set")).toBeNull();
    });

    // Req 7.5 — TTL not yet elapsed, value is still accessible
    it("get returns value when TTL has not yet elapsed", () => {
        store.set("k", "v", 5);
        vi.advanceTimersByTime(4999);
        expect(store.get("k")).toBe("v");
    });

    // Req 7.6 — TTL elapsed, lazy expiry returns null
    it("get returns null after TTL has elapsed (lazy expiry)", () => {
        store.set("k", "v", 5);
        vi.advanceTimersByTime(5001);
        expect(store.get("k")).toBeNull();
    });

    // Req 7.7 — TTL elapsed + sweep fired, active expiry removes entry
    it("size() is 0 after TTL elapsed and sweep fires (active expiry)", () => {
        store.set("k", "v", 5);
        // Advance past both the 5 s TTL and the 1 s sweep interval
        vi.advanceTimersByTime(6000);
        expect(store.size()).toBe(0);
    });

    // Req 7.8 — del existing key returns true and entry is gone
    it("del existing key returns true and subsequent get returns null", () => {
        store.set("k", "v");
        expect(store.del("k")).toBe(true);
        expect(store.get("k")).toBeNull();
    });

    // Req 7.9 — del missing key returns false
    it("del on a key that was never set returns false", () => {
        expect(store.del("never-set")).toBe(false);
    });

    // Req 7.10 — stopSweeper halts the sweep; expired entry stays in store
    it("stopSweeper halts sweep so expired entries are not removed", () => {
        store.set("k", "v", 1);
        store.stopSweeper();
        vi.advanceTimersByTime(10000);
        // Sweep is stopped — entry remains (size includes unswept expired entries)
        expect(store.size()).toBe(1);
    });

    // Feature: self-healing-cache-phase3, Property 1: setRaw stores exact expiresAt
    it("setRaw stores exact expiresAt (Property 1)", () => {
        fc.assert(
            fc.property(
                fc.string(),
                fc.string(),
                fc.oneof(fc.integer(), fc.constant(null)),
                (key, value, expiresAt) => {
                    const tempStore = new KVStore();
                    try {
                        tempStore.setRaw(key, value, expiresAt);
                        expect(tempStore.getExpiresAt(key)).toBe(expiresAt);
                    } finally {
                        tempStore.stopSweeper();
                    }
                }
            ),
            { numRuns: 100 }
        );
    });
});
