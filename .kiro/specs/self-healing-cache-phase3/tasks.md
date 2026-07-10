# Implementation Plan: self-healing-cache-phase3

## Overview

Extend the 3-node TCP cluster established in Phase 2 with key replication. Every key will be stored on all 3 nodes (1 primary + 2 replicas), enabling reads to succeed even when the primary's local copy is absent. Implementation proceeds in dependency order: types → KVStore extension → ReplicationManager → Router changes → integration tests → smoke-test additions.

## Tasks

- [x] 1. Extend type definitions in `src/types/index.ts`
  - [x] 1.1 Add `"REPLICATE"` and `"REPLICATE_DEL"` to the `command` union in `CacheRequest`
    - Extend the string union on the `command` field from `"SET" | "GET" | "DEL"` to `"SET" | "GET" | "DEL" | "REPLICATE" | "REPLICATE_DEL"`
    - All other fields (`id`, `key`, `value`, `ttl`, `ok`, `error`) on both `CacheRequest` and `CacheResponse` remain unchanged
    - _Requirements: 1.1, 1.4_

  - [x] 1.2 Add optional `expiresAt` field to `CacheRequest`
    - Add `expiresAt?: number | null` to the `CacheRequest` interface
    - The field must accept both `number` (a concrete timestamp) and `null` (no expiry)
    - _Requirements: 1.2, 1.3_

- [x] 2. Add `setRaw` and `getExpiresAt` to `KVStore` in `src/core/kvstore.ts`
  - [x] 2.1 Implement `setRaw(key, value, expiresAt)` method
    - Signature: `setRaw(key: string, value: string, expiresAt: number | null): void`
    - Store `{ value, expiresAt }` verbatim — no arithmetic on `expiresAt`
    - If the key already exists, overwrite its `value` and `expiresAt` completely
    - Add JSDoc comment describing purpose, parameters, and return value
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_

  - [x] 2.2 Implement `getExpiresAt(key)` method
    - Signature: `getExpiresAt(key: string): number | null | undefined`
    - Return the stored `expiresAt` for the key, `null` if key never expires, `undefined` if key does not exist
    - No expiry check or deletion should be performed in this method
    - Add JSDoc comment
    - _Requirements: 2.1 (supports SET replication path per design §2)_

  - [x]* 2.3 Write property test for `setRaw` / `getExpiresAt` round-trip
    - **Property 1: `setRaw` Stores Exact `expiresAt` (Round-Trip)**
    - **Validates: Requirements 2.1, 2.2, 2.3, 2.4**
    - Add to `test/kvstore.test.ts` using `fast-check`; tag comment: `// Feature: self-healing-cache-phase3, Property 1: setRaw stores exact expiresAt`
    - Generate arbitrary (key, value, expiresAt) triples including `null`; assert `store.getExpiresAt(key) === expiresAt` after `setRaw`
    - Use `numRuns: 100`

- [x] 3. Create `src/node/ReplicationManager.ts`
  - [x] 3.1 Implement the `ReplicationManager` class
    - Create `src/node/ReplicationManager.ts` exporting class `ReplicationManager`
    - Internal field: `private queue: Set<string> = new Set()`
    - Method `enqueue(key: string): void` — adds key to queue (Set deduplication applies)
    - Method `drainQueue(): string[]` — returns `Array.from(queue)`, then clears queue
    - Method `queueSize(): number` — returns `queue.size` without mutating
    - Method `getQueue(): Set<string>` — returns the internal Set for test/Phase-5 inspection
    - All public methods must have JSDoc comments
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5_

  - [x]* 3.2 Write property test for enqueue/drain round-trip
    - **Property 8: Repair Queue Enqueue/Drain Round-Trip**
    - **Validates: Requirements 8.3, 8.4, 8.5**
    - Add in `test/replication.test.ts` (unit section); tag: `// Feature: self-healing-cache-phase3, Property 8: enqueue/drain round-trip`
    - Generate `fc.array(fc.string(), { minLength: 1, maxLength: 50 })`; after enqueuing all, assert `new Set(drained)` equals `new Set(keys)` and `queueSize() === 0`
    - Use `numRuns: 100`

  - [x]* 3.3 Write property test for enqueue deduplication
    - **Property 9: Repair Queue Deduplication**
    - **Validates: Requirements 8.2, 8.3**
    - Add in `test/replication.test.ts` (unit section); tag: `// Feature: self-healing-cache-phase3, Property 9: enqueue deduplication`
    - Generate `(fc.string(), fc.integer({ min: 1, max: 20 }))`; enqueue same key `times` times; assert `queueSize() === 1` and `drainQueue()` equals `[key]`
    - Use `numRuns: 100`

- [x] 4. Update `src/node/Router.ts` — validation, REPLICATE dispatch, and helpers
  - [x] 4.1 Update `route()` validation block and REPLICATE/REPLICATE_DEL bypass
    - Import `ReplicationManager` and add `private readonly repairQueue: ReplicationManager = new ReplicationManager()` field
    - Extend the valid-commands check to include `"REPLICATE"` and `"REPLICATE_DEL"` (they must not trigger the `"unknown command"` error)
    - Add `"REPLICATE requires value"` validation for `REPLICATE` commands missing `value`
    - Add early-return branch: if `req.command === "REPLICATE" || req.command === "REPLICATE_DEL"`, call `this.executeLocally(req)` immediately, bypassing the ring lookup
    - _Requirements: 3.3, 3.4, 3.5_

  - [x] 4.2 Implement `getReplicaNodes(key)` private method
    - Signature: `private getReplicaNodes(key: string): NodeInfo[]`
    - Call `this.ring.getNodes(key, 3)`, filter out `this.localNodeId`, map remaining IDs to `NodeInfo` via `this.config.find(...)`, and apply `filter(Boolean)` as a defensive guard
    - Add JSDoc comment as specified in design §4.4
    - _Requirements: 4.7_

  - [x]* 4.3 Write property test for `getReplicaNodes`
    - **Property 10: `getReplicaNodes` Returns Non-Local Nodes**
    - **Validates: Requirements 4.7**
    - Add in `test/replication.test.ts`; tag: `// Feature: self-healing-cache-phase3, Property 10: getReplicaNodes returns non-local nodes`
    - For any key, assert result length is 2, no entry has `nodeId === localNodeId`, and all entries are present in the cluster config

  - [x] 4.4 Implement `replicateToNode()` public method
    - Signature: `public async replicateToNode(targetNodeId: string, key: string, value: string, expiresAt: number | null): Promise<void>`
    - Call `forwardToPeer(targetNodeId, { command: "REPLICATE", key, value, expiresAt })`; enforce 5-second timeout using the existing `Promise.race` pattern in `forwardToPeer`
    - If peer responds `ok: true`, resolve without throwing
    - If peer is unreachable, times out, or responds `ok: false`, catch the error, log warning in format `[Replication] WARNING: failed to replicate key <key> to <targetNodeId>: <msg>`, and resolve (fire-and-forget — never rejects)
    - Add JSDoc comment as specified in design §4.5
    - _Requirements: 4.1, 4.2, 4.3, 4.8, 4.9_

  - [x] 4.5 Implement `replicateDelToNode()` public method
    - Signature: `public async replicateDelToNode(targetNodeId: string, key: string): Promise<void>`
    - Call `forwardToPeer(targetNodeId, { command: "REPLICATE_DEL", key })`; enforce 5-second timeout
    - If `resp.ok === true`, resolve without throwing
    - If peer is unreachable, times out, or `resp.ok === false`, **throw** an `Error` so the DEL path can propagate it
    - Add JSDoc comment as specified in design §4.6
    - _Requirements: 4.4, 4.5, 4.6, 4.8, 4.9_

  - [x] 4.6 Implement `getRepairQueue()` public accessor
    - Signature: `public getRepairQueue(): Set<string>`
    - Delegate to `this.repairQueue.getQueue()` and return the result
    - _Requirements: 8.6, 8.7_

- [ ] 5. Update `executeLocally()` in `Router.ts` — REPLICATE, REPLICATE_DEL, async SET, sync DEL, and GET fallback
  - [ ] 5.1 Handle `REPLICATE` and `REPLICATE_DEL` cases in `executeLocally`
    - Change `executeLocally` return type signature to `async` (`private async executeLocally(req: CacheRequest): Promise<CacheResponse>`)
    - Add `case "REPLICATE"`: call `this.localStore.setRaw(req.key, req.value!, req.expiresAt ?? null)` and return `{ ok: true }`
    - Add `case "REPLICATE_DEL"`: call `this.localStore.del(req.key)` and return `{ ok: true }`
    - _Requirements: 3.1, 3.2_

  - [ ]* 5.2 Write property test for REPLICATE bypasses ring routing
    - **Property 2: REPLICATE Bypasses Ring Routing**
    - **Validates: Requirements 3.1, 3.5**
    - Add in `test/router.test.ts`; tag: `// Feature: self-healing-cache-phase3, Property 2: REPLICATE bypasses ring routing`
    - For any key (regardless of which node the ring designates), sending a `REPLICATE` command to a node must store the key locally and return `{ ok: true }`

  - [ ]* 5.3 Write property test for REPLICATE_DEL bypasses ring routing
    - **Property 3: REPLICATE_DEL Bypasses Ring Routing**
    - **Validates: Requirements 3.2, 3.5**
    - Add in `test/router.test.ts`; tag: `// Feature: self-healing-cache-phase3, Property 3: REPLICATE_DEL bypasses ring routing`
    - For any key present in a node's local store, sending `REPLICATE_DEL` must remove it and return `{ ok: true }` regardless of ring assignment

  - [ ] 5.4 Update `SET` case in `executeLocally` for async fan-out replication
    - After `this.localStore.set(req.key, req.value!, req.ttl)`, read `const expiresAt = this.localStore.getExpiresAt(req.key) ?? null`
    - Call `getReplicaNodes(req.key)` and fire `void this.replicateToNode(replica.nodeId, req.key, req.value!, expiresAt)` for each replica (no await)
    - Return `{ ok: true }` immediately — the replication runs in the background
    - When a key has no TTL, `expiresAt` will be `null`; pass it as-is
    - _Requirements: 5.1, 5.2, 5.3, 5.4_

  - [ ] 5.5 Update `DEL` case in `executeLocally` for synchronous fan-out replication
    - After `this.localStore.del(req.key)`, call `getReplicaNodes(req.key)`, then `await Promise.all(replicas.map(r => this.replicateDelToNode(r.nodeId, req.key)))`
    - Wrap in `try/catch`: on success return `{ ok: true }`; on any throw return `{ ok: false, error: <message> }`
    - Both replica DEL operations must run concurrently (via `Promise.all`)
    - _Requirements: 6.1, 6.2, 6.3, 6.4_

  - [ ] 5.6 Update `GET` case in `executeLocally` for replica fallback
    - If `localStore.get(req.key)` returns non-null, return `{ ok: true, value }` immediately
    - If null, iterate `getReplicaNodes(req.key)` in order; for each replica call `await this.forwardToPeer(replica.nodeId, req)`
    - On first replica hit (`resp.ok && resp.value !== undefined`): call `this.repairQueue.enqueue(req.key)` and return `{ ok: true, value: resp.value }`
    - If all replicas miss or are unreachable, return `{ ok: true }` (full miss, no `value` field)
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5_

  - [ ]* 5.7 Write property test for replica fallback and repair queue enqueue
    - **Property 7: Replica Fallback Returns Value and Enqueues Key**
    - **Validates: Requirements 7.2, 7.3**
    - Add in `test/replication.test.ts`; tag: `// Feature: self-healing-cache-phase3, Property 7: replica fallback returns value and enqueues key`
    - Set up a key via `setRaw` on replicas only; issue a GET on the primary; assert response contains the value and `router.getRepairQueue()` contains the key

- [ ] 6. Checkpoint — verify Router unit tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 7. Create `test/replication.test.ts` — integration tests
  - [ ] 7.1 Set up isolated 3-node cluster on ports 17001–17003
    - Create `test/replication.test.ts`; define `TEST_CONFIG` with `test-node-a/b/c` on ports 17001–17003
    - Use `beforeAll` / `afterAll` to start all 3 `CacheNode` instances and connect 3 `CacheClient` instances
    - Call `node.stop()` (which invokes `kvStore.stopSweeper()`) in `afterAll` to prevent timer leaks
    - Do NOT import or modify `ring.test.ts`, `kvstore.test.ts`, `router.test.ts`, or `integration.test.ts`
    - _Requirements: 9.1, 9.6, 9.7_

  - [ ] 7.2 Integration test: async SET replication — all 3 nodes hold the value
    - SET a key on any node; wait 50 ms; GET the key from all 3 ports directly
    - Assert all 3 GETs return the expected value
    - _Requirements: 9.2_

  - [ ]* 7.3 Write property-based integration test for async SET replication convergence
    - **Property 4: Async SET Replication Convergence**
    - **Validates: Requirements 5.1, 5.2**
    - Tag: `// Feature: self-healing-cache-phase3, Property 4: async SET replication convergence`
    - Generate arbitrary key/value pairs; SET on primary; wait 50 ms; assert all 3 nodes return the value

  - [ ] 7.4 Integration test: TTL replication — all 3 nodes have the same `expiresAt`
    - SET a key with a TTL; after async delay, access each node's Router `getExpiresAt` (or via direct `localStore` inspection)
    - Assert all 3 `expiresAt` values are identical
    - _Requirements: 9.3_

  - [ ]* 7.5 Write property-based integration test for TTL timestamp consistency
    - **Property 5: TTL Timestamp Consistency Across Replicas**
    - **Validates: Requirements 5.3, 5.4**
    - Tag: `// Feature: self-healing-cache-phase3, Property 5: TTL timestamp consistency across replicas`
    - Generate arbitrary key/value/TTL combinations; assert all 3 nodes report the same `expiresAt`

  - [ ] 7.6 Integration test: synchronous DEL replication — all 3 nodes return miss
    - SET a key, wait for async replication, DEL the key; assert all 3 nodes return a cache miss immediately (no delay needed)
    - _Requirements: 9.4_

  - [ ]* 7.7 Write property-based integration test for sync DEL across all nodes
    - **Property 6: Synchronous DEL Removes Key from All Nodes**
    - **Validates: Requirements 6.1, 6.2**
    - Tag: `// Feature: self-healing-cache-phase3, Property 6: synchronous DEL removes key from all nodes`
    - Generate arbitrary key/value pairs; SET then DEL; assert all 3 nodes GET returns miss

  - [ ] 7.8 Integration test: GET replica fallback + repair queue enqueue
    - Use `setRaw` to place a key on replica nodes only (bypassing the primary's local store)
    - Issue a GET through the normal routing path (routed to the primary)
    - Assert the response contains the correct value
    - Assert the key appears in `router.getRepairQueue()` after the GET
    - _Requirements: 9.5_

- [ ] 8. Checkpoint — verify all replication integration tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 9. Extend `scripts/smoke-test.ts` with the `── REPLICATE ──` section
  - [ ] 9.1 Add the `── REPLICATE ──` smoke-test section after the existing DEL section
    - Add a clearly labelled section `── REPLICATE ──` after the DEL section
    - Use the existing `pass` / `fail` helper functions; contribute to `passed` / `failed` counters
    - Do NOT modify the existing SET, GET cross-node, or DEL sections
    - _Requirements: 10.1, 10.5, 10.6_

  - [ ] 9.2 Smoke test: verify async replication for at least 3 keys
    - SET at least 3 keys; wait ~50 ms; GET each key from all 3 nodes
    - Assert each key is readable from all 3 nodes, recording pass/fail for each check
    - _Requirements: 10.2_

  - [ ] 9.3 Smoke test: verify TTL expiry consistency across all nodes
    - SET at least 1 key with a short TTL; wait for TTL to elapse; GET from all 3 nodes
    - Assert all 3 nodes return a cache miss after expiry
    - _Requirements: 10.3_

  - [ ] 9.4 Smoke test: verify synchronous DEL replication across all nodes
    - DEL at least 1 previously-replicated key; GET from all 3 nodes
    - Assert all 3 nodes return a miss immediately after DEL
    - _Requirements: 10.4_

- [ ] 10. Final checkpoint — ensure all tests pass
  - Ensure all tests pass (`npm test`), ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP delivery
- Each task references specific requirements for traceability
- The design is TypeScript; all implementation uses TypeScript with the existing project conventions
- `fast-check` is already installed as a dev dependency — use it for all property-based tests
- Property test tag format: `// Feature: self-healing-cache-phase3, Property N: <title>`
- `executeLocally` must be changed to `async` in task 5.1 — this change is required before implementing tasks 5.4, 5.5, 5.6
- Existing files NOT modified: `ring.ts`, `hash.ts`, `CacheClient.ts`, `cluster.ts`, `TcpServer.ts`, `node-entry.ts`, `CacheNode.ts`, all existing test files
- New files: `src/node/ReplicationManager.ts`, `test/replication.test.ts`
- Modified files: `src/types/index.ts`, `src/core/kvstore.ts`, `src/node/Router.ts`, `scripts/smoke-test.ts`

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["2.1", "2.2", "3.1"] },
    { "id": 2, "tasks": ["2.3", "3.2", "3.3", "4.1"] },
    { "id": 3, "tasks": ["4.2", "4.4", "4.5", "4.6"] },
    { "id": 4, "tasks": ["4.3", "5.1"] },
    { "id": 5, "tasks": ["5.2", "5.3", "5.4", "5.5", "5.6"] },
    { "id": 6, "tasks": ["5.7", "7.1"] },
    { "id": 7, "tasks": ["7.2", "7.4", "7.6", "7.8"] },
    { "id": 8, "tasks": ["7.3", "7.5", "7.7", "9.1"] },
    { "id": 9, "tasks": ["9.2", "9.3", "9.4"] }
  ]
}
```
