# Implementation Plan: self-healing-cache (Phase 1)

## Overview

Implement two foundational in-process modules — `ConsistentHashRing` and `KVStore` — along with shared types, a SHA-1 hash utility, comprehensive tests (unit + property-based with fast-check), and a distribution harness script. All code is TypeScript targeting Node.js with no networking or multi-process concerns.

## Tasks

- [x] 1. Initialize project structure and configuration
  - Create `package.json` with `devDependencies` (`vitest`, `tsx`, `typescript`, `fast-check`) and npm scripts (`build`, `test`, `harness`)
  - Create `tsconfig.json` with `strict: true`, `target: "ES2020"`, `module: "Node16"` or `"NodeNext"`, `moduleResolution: "Node16"` or `"NodeNext"`, `outDir: "dist"`
  - Create placeholder files at all required paths: `src/utils/hash.ts`, `src/core/ring.ts`, `src/core/kvstore.ts`, `src/types/index.ts`, `test/ring.test.ts`, `test/kvstore.test.ts`, `test/distribution-harness.ts`, `README.md`
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

- [ ] 2. Implement shared types and hash utility
  - [ ] 2.1 Create shared type definitions in `src/types/index.ts`
    - Export `RingEntry` interface with `hash: bigint` and `nodeId: string`
    - Export `KVEntry` interface with `value: string` and `expiresAt: number | null`
    - _Requirements: 3.1, 3.2_

  - [ ] 2.2 Implement `hashString` in `src/utils/hash.ts`
    - Export `hashString(input: string): bigint`
    - Use `node:crypto` `createHash('sha1')`, convert 40-char hex digest to bigint via `BigInt("0x" + hexDigest)`
    - No third-party imports
    - _Requirements: 2.1, 2.2, 2.3, 2.6_

  - [ ]* 2.3 Write property tests for hash utility in `test/ring.test.ts`
    - **Property 1: Hash Determinism** — `fc.string()` → `hashString(s) === hashString(s)`
    - **Validates: Requirements 2.4**
    - **Property 2: Hash Collision Resistance** — `fc.tuple(fc.string(), fc.string()).filter(([a,b]) => a !== b)` → `hashString(a) !== hashString(b)`
    - **Validates: Requirements 2.5**

- [ ] 3. Implement `ConsistentHashRing`
  - [ ] 3.1 Create `ConsistentHashRing` class skeleton in `src/core/ring.ts`
    - Import `RingEntry` from `src/types/index.ts` and `hashString` from `src/utils/hash.ts`
    - Add class block comment explaining consistent hashing vs modulo hashing and the purpose of virtual nodes
    - Declare private `ring: RingEntry[] = []` and `static readonly VIRTUAL_NODES_PER_NODE = 200`
    - Add JSDoc to class and all public method stubs (`addNode`, `removeNode`, `getNode`, `getNodes`, `getDistribution`)
    - _Requirements: 3.3, 4.1, 4.2, 4.15, 4.16_

  - [ ] 3.2 Implement `addNode` and `removeNode`
    - `addNode`: for `i` in `[0, 199]` hash `` `${nodeId}#${i}` ``, push `{ hash, nodeId }`, then sort by `hash` ascending
    - `removeNode`: filter out all entries where `entry.nodeId === nodeId`; assign result back (no re-sort)
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6_

  - [ ] 3.3 Implement `getNode` with binary search
    - Return `null` for empty ring
    - Hash the key, binary-search for lowest index where `ring[i].hash >= keyHash`
    - Wrap to index 0 when key hash exceeds all entries
    - _Requirements: 4.7, 4.8, 4.9, 4.10_

  - [ ] 3.4 Implement `getNodes` and `getDistribution`
    - `getNodes`: return `[]` for empty ring or `count <= 0`; clockwise walk from `getNode` start position collecting distinct `nodeId` values up to `count` or ring exhaustion
    - `getDistribution`: reduce `ring` to a `Map<string, number>` counting virtual nodes per physical node
    - _Requirements: 4.11, 4.12, 4.13, 4.14_

  - [ ]* 3.5 Write property tests for `ConsistentHashRing` in `test/ring.test.ts`
    - **Property 3: Ring Sort Invariant** — random sequence of addNode/removeNode ops → internal array sorted ascending
    - **Validates: Requirements 4.1, 4.3, 4.5**
    - **Property 4: Virtual Node Count on Add** — `fc.string()` nodeId added once → `getDistribution().get(nodeId) === 200`
    - **Validates: Requirements 4.3, 4.14**
    - **Property 5: Remove Cleans All Entries** — add then remove nodeId → absent from distribution and no `getNode` returns it
    - **Validates: Requirements 4.5**
    - **Property 6: getNode Returns a Known Node** — `fc.array(fc.string(), {minLength:1})` nodes + `fc.string()` key → result is in the known set
    - **Validates: Requirements 4.7, 4.8, 4.9**
    - **Property 7: getNodes Distinct-Node Guarantee** — random ring + key + count → length = min(count, N), all distinct, all in ring
    - **Validates: Requirements 4.11, 4.12, 4.13**

- [ ] 4. Write unit tests for `ConsistentHashRing` in `test/ring.test.ts`
  - [ ] 4.1 Write example-based unit tests for ring operations
    - Single-node ring: 10+ distinct keys all resolve to that node
    - Three-node ring: 100 distinct keys each return a non-null string matching one of the three node IDs
    - Add then remove a node from a 2-node ring: 50 keys return no result for the removed node
    - `getNode` on empty ring returns `null`
    - Same key queried twice returns the same `nodeId` (determinism)
    - Wrap-around: use `getDistribution()` to confirm 200 virtual nodes per node, then 1,000 random keys never throw and never return `null`
    - `getNodes(key, 2)` on 3-node ring returns array of length 2 with 2 distinct IDs
    - `getNodes(key, 5)` on 3-node ring returns array of length 3 (all available nodes)
    - `getDistribution()` after 3 adds returns map with 3 entries each mapping to 200
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 6.8, 6.9, 6.10_

- [ ] 5. Checkpoint — Ensure all ring tests pass
  - Run `npm test` and confirm all ring tests are green. Ask the user if any questions arise.

- [ ] 6. Implement `KVStore`
  - [ ] 6.1 Create `KVStore` class skeleton in `src/core/kvstore.ts`
    - Import `KVEntry` from `src/types/index.ts`
    - Declare private `store: Map<string, KVEntry>` and `private sweepTimer`
    - Add JSDoc to class and all public method stubs (`set`, `get`, `del`, `size`, `stopSweeper`)
    - _Requirements: 3.4, 5.1, 5.16_

  - [ ] 6.2 Implement constructor with sweep interval and `stopSweeper`
    - Constructor: `this.sweepTimer = setInterval(this.sweep.bind(this), 1000)`
    - Private `sweep()`: iterate store entries, delete those with `expiresAt !== null && expiresAt <= Date.now()`
    - `stopSweeper()`: calls `clearInterval(this.sweepTimer)`
    - _Requirements: 5.12, 5.13, 5.14_

  - [ ] 6.3 Implement `set`, `get`, `del`, and `size`
    - `set`: `expiresAt = null` for no TTL or `ttlSeconds <= 0`; `Date.now() + ttlSeconds * 1000` for positive TTL; overwrites existing entry
    - `get`: return `null` for missing key; return `null` and delete if `expiresAt !== null && expiresAt <= Date.now()`; otherwise return value
    - `del`: delete key, return `true`; return `false` if absent
    - `size`: return `this.store.size`
    - _Requirements: 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8, 5.9, 5.10, 5.11, 5.15_

  - [ ]* 6.4 Write property tests for `KVStore` in `test/kvstore.test.ts`
    - **Property 8: Non-Expiring Entry Always Readable** — `fc.string()` key/value, no TTL → `get` returns value after any time advance
    - **Validates: Requirements 5.2, 5.7**
    - **Property 9: TTL Expiry Boundary** — `fc.nat({min:1, max:3600})` ttl → value before expiry, `null` at or after
    - **Validates: Requirements 5.3, 5.8, 5.9**
    - **Property 10: Zero or Negative TTL Treated as No Expiry** — `fc.integer({max:0})` ttl → entry never expires
    - **Validates: Requirements 5.4**
    - **Property 11: Overwrite Replaces Both Value and Expiry** — set key twice with different value/TTL → second value and expiry govern
    - **Validates: Requirements 5.5**
    - **Property 12: Active Sweep Removes All Expired Entries** — random entries with positive TTLs → `size() === 0` after sweep fires post-expiry (no `get` call)
    - **Validates: Requirements 5.12, 5.13**
    - **Property 13: del Removes Entry and Returns Correct Boolean** — `fc.string()` key → `del` returns `true` for present, `false` for absent
    - **Validates: Requirements 5.10, 5.11**

- [ ] 7. Write unit tests for `KVStore` in `test/kvstore.test.ts`
  - [ ] 7.1 Write example-based unit tests for KVStore operations
    - Set up `vi.useFakeTimers()` in `beforeEach`, `store.stopSweeper()` + `vi.useRealTimers()` in `afterEach`
    - `set("k", "v")` with no TTL → `get("k")` returns `"v"`
    - `get` for never-set key returns `null`
    - `set("k", "v", 5)` then `get` before 5000 ms → returns `"v"`
    - `set("k", "v", 5)` then `vi.advanceTimersByTime(5001)` → `get("k")` returns `null` (lazy expiry)
    - `set("k", "v", 5)` then `vi.advanceTimersByTime(6000)` → `store.size() === 0` (active sweep, no `get` call)
    - `del("k")` for existing key → returns `true`, subsequent `get` returns `null`
    - `del("k")` for never-set key → returns `false`
    - `stopSweeper()` then `vi.advanceTimersByTime(10000)` → expired key entry remains in store (size unchanged)
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 7.9, 7.10_

- [ ] 8. Checkpoint — Ensure all KVStore tests pass
  - Run `npm test` and confirm all KVStore tests are green. Ask the user if any questions arise.

- [ ] 9. Implement distribution harness script
  - [ ] 9.1 Implement `test/distribution-harness.ts`
    - Import `ConsistentHashRing` from `src/core/ring.ts` and `hashString` from `src/utils/hash.ts`; no other non-built-in imports
    - Add `node-A`, `node-B`, `node-C` to a new ring; generate 10,000 `crypto.randomUUID()` keys
    - Map each key via `getNode`, tally per-node counts; print aligned table (node name padded 10 chars, count padded 5 chars, percentage to 1 dp)
    - Add `node-D`, re-map all 10,000 keys, count remapped keys; print remapped %, total, count
    - Repeat the same 3-node → 4-node workflow with modulo hashing (`Number(hashString(key) % BigInt(nodeCount))` → node list index)
    - Print summary paragraph contrasting ~25% consistent-hashing remapping vs ~75% modulo-hashing remapping
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8_

- [ ] 10. Final checkpoint — Ensure all tests pass and harness runs
  - Run `npm test` to confirm the full test suite is green
  - Run `npm run harness` to verify the distribution harness exits with code 0 and prints expected output
  - Run `npm run build` to confirm TypeScript compilation succeeds with no errors
  - Ask the user if any questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for a faster MVP
- Each task references specific requirements for traceability
- Checkpoints (tasks 5, 8, 10) provide incremental validation gates
- Property tests (Properties 1–13) validate universal correctness; unit tests cover specific examples and edge cases
- All property tests use `fc.assert(fc.property(...))` with a minimum of 100 iterations via fast-check
- Fake timer protocol for KVStore: `vi.useFakeTimers()` in `beforeEach`, `store.stopSweeper()` + `vi.useRealTimers()` in `afterEach`
- The `distribution-harness.ts` is a standalone `tsx` script — not a Vitest test file

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["2.1", "2.2"] },
    { "id": 1, "tasks": ["2.3", "3.1"] },
    { "id": 2, "tasks": ["3.2"] },
    { "id": 3, "tasks": ["3.3"] },
    { "id": 4, "tasks": ["3.4"] },
    { "id": 5, "tasks": ["3.5", "4.1"] },
    { "id": 6, "tasks": ["6.1"] },
    { "id": 7, "tasks": ["6.2"] },
    { "id": 8, "tasks": ["6.3"] },
    { "id": 9, "tasks": ["6.4", "7.1"] },
    { "id": 10, "tasks": ["9.1"] }
  ]
}
```
