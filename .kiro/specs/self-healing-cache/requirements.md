# Requirements Document

## Introduction

This document defines requirements for Phase 1 of the **self-healing-cache** project — a distributed cache system implemented in TypeScript (Node.js). Phase 1 establishes the foundational in-process modules: a consistent hash ring for key distribution across nodes, and a key-value store with TTL-based expiry. There is no networking, no multi-process communication, and no HTTP/TCP code in this phase. All components run in a single process as isolated, testable units. Networking and clustering are deferred to later phases.

The project is structured for testability, correctness, and future extensibility. Unit tests use Vitest with fake timers for deterministic TTL verification. A standalone distribution harness script validates key distribution characteristics and compares consistent hashing against naive modulo hashing.

---

## Glossary

- **Cache**: An in-memory store that maps string keys to string values with optional time-to-live (TTL) expiry.
- **ConsistentHashRing**: The ring data structure that maps keys to physical nodes using consistent hashing with virtual nodes.
- **KVStore**: The key-value store class providing `set`, `get`, `del`, `size`, and sweep-based active expiry.
- **Node**: A logical cache participant identified by a string `nodeId`. In Phase 1, nodes are abstract identifiers — no network endpoints exist.
- **Virtual Node**: A synthetic ring entry representing a physical node, used to improve key distribution uniformity. Each physical node produces 200 virtual node entries.
- **Hash Ring**: A conceptual circular keyspace where both keys and virtual nodes occupy positions determined by their SHA-1 hash values, and a key is routed to the first node clockwise from the key's position.
- **TTL (Time-to-Live)**: A duration in seconds after which a cache entry is considered expired and must not be returned.
- **Lazy Expiry**: Eviction of expired entries at read time (during `get`) rather than proactively.
- **Active Sweep**: A periodic background pass that removes all expired entries, preventing unbounded memory growth.
- **Clockwise Walk**: The ring traversal that starts at a key's hash position and advances to the next higher hash entry, wrapping around to index 0 if no higher entry exists.
- **Distribution Harness**: A standalone executable script that benchmarks key distribution across nodes and compares consistent hashing with naive modulo hashing.
- **SHA-1**: The cryptographic hash function used to produce 160-bit (20-byte) digests, converted to `bigint` for numeric comparison on the ring.
- **Vitest**: The unit test framework used for all tests in this project.
- **tsx**: A TypeScript execution tool used to run `.ts` scripts directly without a separate compile step.

---

## Requirements

### Requirement 1: Project Initialization and Structure

**User Story:** As a developer, I want a properly initialized TypeScript Node.js project with strict mode and the correct directory layout, so that the codebase is consistent, type-safe, and ready for future phases.

#### Acceptance Criteria

1. THE Project SHALL contain the following files at their exact paths:
   - `src/utils/hash.ts`
   - `src/core/kvstore.ts`
   - `src/core/ring.ts`
   - `src/types/index.ts`
   - `test/kvstore.test.ts`
   - `test/ring.test.ts`
   - `test/distribution-harness.ts`
   - `package.json`
   - `tsconfig.json`
   - `README.md`

2. THE `tsconfig.json` SHALL enable `strict: true`, set `target` to `ES2020`, set `module` to `Node16` or `NodeNext`, set `moduleResolution` to `Node16` or `NodeNext`, and set `outDir` to `dist`.

3. THE `package.json` SHALL declare `vitest`, `tsx`, and `typescript` as `devDependencies` only, with no runtime dependencies in `dependencies`.

4. THE `package.json` SHALL define the following npm scripts:
   - `build`: compiles TypeScript to JavaScript using `tsc`, outputting to the `outDir` declared in `tsconfig.json`
   - `test`: runs all Vitest unit tests
   - `harness`: executes `test/distribution-harness.ts` via `tsx`

5. THE Project SHALL contain no HTTP, TCP, WebSocket, `child_process`, or `worker_threads` usage in any source file under `src/` or `test/`.

---

### Requirement 2: SHA-1 Hash Utility

**User Story:** As a developer, I want a deterministic string hashing function that returns a `bigint`, so that hash ring positions can be computed and sorted numerically.

#### Acceptance Criteria

1. THE `hash.ts` module SHALL export a function `hashString(input: string): bigint`.

2. WHEN `hashString` is called with a string input, THE `Hash_Utility` SHALL compute a SHA-1 digest of that input using Node.js's built-in `crypto` module.

3. WHEN `hashString` computes a SHA-1 digest, THE `Hash_Utility` SHALL convert the full 40-character hexadecimal digest string into a `bigint` using `BigInt("0x" + hexDigest)` and return it, yielding a value in the range [0, 2^160 − 1].

4. WHEN `hashString` is called twice with the same input string, THE `Hash_Utility` SHALL return the same `bigint` value both times (determinism).

5. WHEN `hashString` is called with two input strings that differ by at least one character, THE `Hash_Utility` SHALL return two unequal `bigint` values.

6. THE `Hash_Utility` SHALL use only Node.js built-in modules and SHALL NOT import any third-party packages.

---

### Requirement 3: Shared Type Definitions

**User Story:** As a developer, I want shared TypeScript interfaces for ring entries and KV store entries, so that types are consistent across all modules without duplication.

#### Acceptance Criteria

1. THE `src/types/index.ts` module SHALL export an interface named `RingEntry` with fields: `hash: bigint` and `nodeId: string`.

2. THE `src/types/index.ts` module SHALL export an interface named `KVEntry` with fields: `value: string` and `expiresAt: number | null`, where `expiresAt` is an absolute Unix timestamp in milliseconds or `null` for non-expiring entries.

3. THE `ring.ts` module SHALL import `RingEntry` from `src/types/index.ts` and use it as the element type for the internal ring entries array, with no locally-defined duplicate type for ring entries.

4. THE `kvstore.ts` module SHALL import `KVEntry` from `src/types/index.ts` and use it as the value type for the internal `Map` backing store, with no locally-defined duplicate type for KV store entries.

---

### Requirement 4: Consistent Hash Ring

**User Story:** As a developer, I want a consistent hash ring implementation with virtual nodes, so that cache keys are distributed uniformly across nodes and node additions/removals minimally disrupt key assignments.

#### Acceptance Criteria

1. THE `ConsistentHashRing` class SHALL maintain an internal sorted array of `RingEntry` objects, sorted by `hash` value ascending at all times.

2. THE `ConsistentHashRing` class SHALL define a constant `VIRTUAL_NODES_PER_NODE = 200`.

3. WHEN `addNode(nodeId: string)` is called with a `nodeId` not already present in the ring, THE `ConsistentHashRing` SHALL generate exactly 200 virtual node entries by hashing the string `` `${nodeId}#${i}` `` for `i` from `0` to `199` inclusive, append all 200 entries into the ring array, and re-sort the entire array by `hash` ascending.

4. WHEN `addNode(nodeId: string)` is called with a `nodeId` that already has entries in the ring, THE `ConsistentHashRing` SHALL add 200 additional virtual node entries for that `nodeId` (duplicates are permitted) and re-sort.

5. WHEN `removeNode(nodeId: string)` is called with a `nodeId` that has entries in the ring, THE `ConsistentHashRing` SHALL remove all ring entries whose `nodeId` field matches the given value, preserving the sort order of remaining entries without re-sorting.

6. WHEN `removeNode(nodeId: string)` is called with a `nodeId` that has no entries in the ring, THE `ConsistentHashRing` SHALL leave the ring unchanged with no error thrown.

7. WHEN `getNode(key: string)` is called on an empty ring, THE `ConsistentHashRing` SHALL return `null`.

8. WHEN `getNode(key: string)` is called on a non-empty ring, THE `ConsistentHashRing` SHALL hash the key using `hashString`, perform a binary search for the first ring entry whose `hash` is greater than or equal to the key's hash, and return that entry's `nodeId`.

9. WHEN `getNode(key: string)` is called and the key's hash is greater than every entry's hash in the ring, THE `ConsistentHashRing` SHALL return the `nodeId` of the entry at index 0 (wrap-around behavior).

10. WHEN `getNode(key: string)` is called with the same key after `addNode` and `removeNode` operations that do not affect the assigned node, THE `ConsistentHashRing` SHALL return the same `nodeId` (determinism).

11. WHEN `getNodes(key: string, count: number)` is called on an empty ring, THE `ConsistentHashRing` SHALL return an empty array `[]`.

12. WHEN `getNodes(key: string, count: number)` is called with `count ≤ 0`, THE `ConsistentHashRing` SHALL return an empty array `[]`.

13. WHEN `getNodes(key: string, count: number)` is called on a non-empty ring with a positive `count`, THE `ConsistentHashRing` SHALL perform a clockwise walk starting from the key's hash position, collecting distinct physical `nodeId` values until either `count` unique node IDs are found or all unique physical nodes in the ring have been visited, and SHALL return the collected node IDs as a `string[]`.

14. WHEN `getDistribution()` is called, THE `ConsistentHashRing` SHALL return a `Map<string, number>` mapping each distinct `nodeId` to the count of virtual node entries it currently occupies in the ring; for any node added exactly once via `addNode`, that count SHALL be exactly 200.

15. THE `ConsistentHashRing` class SHALL include a block comment above the class declaration explaining consistent hashing versus modulo hashing and the purpose of virtual nodes.

16. THE `ConsistentHashRing` class and all public methods SHALL have JSDoc doc comments.

---

### Requirement 5: Key-Value Store with TTL

**User Story:** As a developer, I want a key-value store with optional TTL expiry and active sweeping, so that cache entries expire correctly and memory is reclaimed even for keys that are never read again.

#### Acceptance Criteria

1. THE `KVStore` class SHALL maintain an internal `Map<string, KVEntry>` as its backing store.

2. WHEN `set(key: string, value: string, ttlSeconds?: number)` is called without a `ttlSeconds` argument, THE `KVStore` SHALL store the entry with `expiresAt` set to `null`, indicating the entry never expires.

3. WHEN `set(key: string, value: string, ttlSeconds?: number)` is called with a `ttlSeconds` value greater than `0`, THE `KVStore` SHALL store the entry with `expiresAt` set to `Date.now() + ttlSeconds * 1000`.

4. WHEN `set(key: string, value: string, ttlSeconds?: number)` is called with a `ttlSeconds` value of `0` or a negative number, THE `KVStore` SHALL store the entry with `expiresAt` set to `null` (treated as no TTL).

5. WHEN `set(key, value, ttlSeconds)` is called for a key that already exists in the store, THE `KVStore` SHALL overwrite the existing entry's `value` and `expiresAt` with the new values.

6. WHEN `get(key: string)` is called for a key that does not exist, THE `KVStore` SHALL return `null`.

7. WHEN `get(key: string)` is called for a key whose `expiresAt` is `null`, THE `KVStore` SHALL return the stored value.

8. WHEN `get(key: string)` is called for a key whose `expiresAt` is strictly greater than `Date.now()`, THE `KVStore` SHALL return the stored value.

9. WHEN `get(key: string)` is called for a key whose `expiresAt` is less than or equal to `Date.now()`, THE `KVStore` SHALL delete the entry from the backing store and return `null` (lazy expiry).

10. WHEN `del(key: string)` is called for a key that exists, THE `KVStore` SHALL remove the entry and return `true`.

11. WHEN `del(key: string)` is called for a key that does not exist, THE `KVStore` SHALL return `false`.

12. THE `KVStore` SHALL start a background sweep interval of exactly 1000 milliseconds upon construction that calls the private `sweep()` method.

13. WHEN the sweep interval fires, THE `KVStore` SHALL iterate over all entries in the backing store and delete every entry whose `expiresAt` is not `null` and is less than or equal to `Date.now()` (active expiry).

14. WHEN `stopSweeper()` is called, THE `KVStore` SHALL clear the sweep interval timer, preventing any further sweep executions.

15. WHEN `size()` is called, THE `KVStore` SHALL return the number of entries currently in the backing store, including entries that have expired but not yet been swept or lazily evicted.

16. THE `KVStore` class and all public methods SHALL have JSDoc doc comments, each including a description, parameter documentation, and return value documentation.

---

### Requirement 6: Ring Unit Tests

**User Story:** As a developer, I want comprehensive unit tests for `ConsistentHashRing`, so that correctness of all ring operations is verified automatically on every change.

#### Acceptance Criteria

1. THE `ring.test.ts` file SHALL use Vitest with one isolated `ConsistentHashRing` instance per test (created in `beforeEach` or inline).

2. WHEN a single node is added to the ring, THE test SHALL call `getNode` with at least 10 distinct keys and verify every result equals that node's `nodeId`.

3. WHEN three nodes are added to the ring, THE test SHALL call `getNode` with at least 100 distinct keys and verify every result is a non-null string that matches one of the three node IDs.

4. WHEN a node is added and then removed from a ring containing at least one other node, THE test SHALL call `getNode` with 50 distinct keys and verify none of the results equals the removed node's `nodeId`.

5. WHEN `getNode` is called on an empty ring, THE test SHALL verify the return value is `null`.

6. WHEN `getNode` is called twice with the same key on a ring containing three nodes, THE test SHALL verify both calls return the same `nodeId` (determinism).

7. WHEN wrap-around is tested, THE test SHALL use `getDistribution()` to confirm all nodes have 200 entries, then call `getNode` with 1,000 distinct random keys and verify none throws and none returns `null`.

8. WHEN `getNodes` is called with `count = 2` on a ring containing exactly 3 nodes, THE test SHALL verify the result array has length 2 and contains 2 distinct `nodeId` strings.

9. WHEN `getNodes` is called with `count = 5` on a ring containing exactly 3 nodes, THE test SHALL verify the result array has length 3 (all available distinct nodes returned, not 5).

10. WHEN `getDistribution()` is called after adding 3 nodes, THE test SHALL verify the map has 3 entries and each maps to exactly 200.

---

### Requirement 7: KV Store Unit Tests

**User Story:** As a developer, I want comprehensive unit tests for `KVStore`, so that correctness of all store operations including TTL and sweep behavior is verified automatically.

#### Acceptance Criteria

1. THE `kvstore.test.ts` file SHALL call `vi.useFakeTimers()` before any test that exercises TTL expiry or sweep behavior, and SHALL call `vi.useRealTimers()` in `afterEach` to restore the real clock.

2. THE `kvstore.test.ts` SHALL call `store.stopSweeper()` in `afterEach` (or equivalent teardown) to clear the sweep interval timer and prevent Vitest from hanging on open handles.

3. WHEN `set("k", "v")` is called with no TTL, THE test SHALL verify `get("k")` returns `"v"`.

4. WHEN `get` is called for a key that was never set, THE test SHALL verify the return value is `null`.

5. WHEN `set("k", "v", 5)` is called and `get("k")` is called before 5000 ms have elapsed (fake-timer-controlled), THE test SHALL verify `get("k")` returns `"v"`.

6. WHEN `set("k", "v", 5)` is called and `vi.advanceTimersByTime(5001)` is called, THE test SHALL verify `get("k")` returns `null` (lazy expiry triggered).

7. WHEN `set("k", "v", 5)` is called and `vi.advanceTimersByTime(6000)` is called (past both TTL and sweep interval of 1000 ms), THE test SHALL verify `store.size()` returns `0` (active sweep removed the entry without any `get` call).

8. WHEN `del("k")` is called for an existing key, THE test SHALL verify the return value is `true` and a subsequent `get("k")` returns `null`.

9. WHEN `del("k")` is called for a key that was never set, THE test SHALL verify the return value is `false`.

10. WHEN `stopSweeper()` is called and `vi.advanceTimersByTime(10000)` is called, THE test SHALL verify an expired key's entry remains in the store (size does not drop), confirming no further sweeps ran.

---

### Requirement 8: Distribution Harness

**User Story:** As a developer, I want a standalone harness script that visualizes key distribution and compares consistent hashing with modulo hashing, so that I can validate the ring's uniformity and include results in the README.

#### Acceptance Criteria

1. THE `distribution-harness.ts` script SHALL be executable via `npm run harness` (using `tsx`) and SHALL exit with code 0 on success.

2. WHEN executed, THE `Distribution_Harness` SHALL create a `ConsistentHashRing` and add exactly three physical nodes: `node-A`, `node-B`, and `node-C`.

3. WHEN executed, THE `Distribution_Harness` SHALL generate exactly 10,000 random UUID v4 strings as cache keys using `crypto.randomUUID()`.

4. WHEN executed, THE `Distribution_Harness` SHALL call `getNode(key)` for all 10,000 keys and print a table showing each node's key count and percentage share, formatted to one decimal place, with columns left-padded to align (e.g., node name padded to 10 chars, count padded to 5 chars).

5. WHEN executed, THE `Distribution_Harness` SHALL record the `nodeId` for each key from the 3-node configuration, add `node-D` to the ring, re-call `getNode(key)` for all 10,000 keys, count keys whose assigned node changed, and print the remapped count, total, and percentage formatted to one decimal place.

6. WHEN executed, THE `Distribution_Harness` SHALL compute key assignment for the same 10,000 keys using naive modulo hashing (inline function: `Number(hashString(key) % BigInt(nodeCount))` mapped to a node list index), run the same 3-node → 4-node comparison, and print the remapped count, total, and percentage formatted to one decimal place.

7. THE `Distribution_Harness` SHALL end with a printed summary paragraph contrasting the consistent-hashing remapping percentage (~25%) with the modulo-hashing remapping percentage (~75%), making the difference explicit.

8. THE `Distribution_Harness` SHALL NOT import any packages other than Node.js built-ins and the project's own `src/` modules.
