# Requirements Document

## Introduction

This document defines requirements for **Phase 3** of the self-healing-cache project — adding key replication across the 3-node TCP cluster established in Phase 2.

Every key must live on 3 nodes (1 primary + 2 replicas) so that reads succeed even when the primary is unavailable. Phase 3 implements:

- **Async replication on SET** — the primary writes locally and immediately returns `OK` to the client, then fans out to the 2 replica nodes in the background (fire-and-forget).
- **Replica fallback on GET** — if the primary cannot be reached, the Router tries the 2 replica nodes in order. A successful replica hit is added to a read-repair queue (processing deferred to Phase 5).
- **Synchronous replication on DEL** — deletion is propagated to all 3 nodes before returning `OK`, preventing ghost reads.
- **TTL propagation by absolute timestamp** — replicas receive the exact `expiresAt` value already computed by the primary, so TTL skew is impossible.
- **REPLICATE / REPLICATE_DEL wire commands** — new internal commands handled only on the receiving node (bypass routing).
- **ReplicationManager** — a lightweight queue that records keys needing read-repair.

Phases 1 and 2 source files (`ring.ts`, `kvstore.ts`, `hash.ts`, `cluster.ts`, `TcpServer.ts`, `CacheClient.ts`) are **not modified** except for the explicit additions listed below. No gossip, no failure detection, and no automatic node removal are in scope — those belong to Phase 4.

---

## Glossary

- **Primary**: The first node returned by `ring.getNodes(key, 3)` — the canonical owner of a key.
- **Replica**: The second and third nodes returned by `ring.getNodes(key, 3)`. Each key always has exactly 2 replicas in a 3-node cluster.
- **Replication Factor**: The total number of nodes that hold a copy of a key. Fixed at 3 in Phase 3.
- **Async Replication**: A replication strategy where the primary acknowledges the client immediately and propagates to replicas in the background without waiting for confirmation.
- **Synchronous Replication**: A replication strategy where the primary waits for all replicas to confirm before responding to the client. Used exclusively for DEL in Phase 3.
- **REPLICATE**: A new internal wire command that instructs a node to store a key-value pair with an absolute `expiresAt` timestamp, bypassing routing.
- **REPLICATE_DEL**: A new internal wire command that instructs a node to delete a key, bypassing routing.
- **Fire-and-Forget**: Sending a replication request without awaiting its result. Failures are logged as warnings but do not affect the client-facing response.
- **Read-Repair**: A consistency recovery technique where a value found on a replica (after a primary miss) is queued for later re-synchronisation to the primary. Queueing is in scope for Phase 3; processing is deferred to Phase 5.
- **RepairQueue**: An in-memory `Set<string>` of keys that were served from a replica and need to be re-synchronised with the primary.
- **expiresAt**: An absolute Unix timestamp in milliseconds at which a key expires, or `null` for non-expiring keys.
- **setRaw**: A new `KVStore` method that stores a key with a pre-computed absolute `expiresAt` value, bypassing the relative-TTL calculation in `set`.
- **ReplicationManager**: A new class (`src/node/ReplicationManager.ts`) that owns the repair queue and exposes enqueue / drain / size operations.
- **Router**: The existing `src/node/Router.ts` class, extended in Phase 3 to perform replication and replica fallback.
- **NodeConfig**: An alias used in this document for the `NodeInfo` interface from `src/types/index.ts`.
- **CacheClient**: The existing TCP client in `src/client/CacheClient.ts`, reused without modification for inter-node replication calls.
- **5-Second Timeout**: The maximum time a replication call may take before it is abandoned. Applies to all replication paths (both REPLICATE and REPLICATE_DEL).

---

## Requirements

### Requirement 1: Type System Extensions

**User Story:** As a developer, I want the shared type definitions to include the new replication commands and the `expiresAt` field, so that the TypeScript compiler enforces correctness across all callers.

#### Acceptance Criteria

1. THE `src/types/index.ts` module SHALL extend the `command` field of `CacheRequest` to the union `"SET" | "GET" | "DEL" | "REPLICATE" | "REPLICATE_DEL"`.

2. THE `src/types/index.ts` module SHALL add an optional field `expiresAt?: number | null` to the `CacheRequest` interface, representing an absolute expiry timestamp in milliseconds.

3. WHEN `expiresAt` is present on a `CacheRequest`, THE `CacheRequest` type SHALL accept both `number` (a concrete timestamp) and `null` (no expiry) as valid values.

4. THE existing fields `id`, `key`, `value`, `ttl`, `ok`, `error` on `CacheRequest` and `CacheResponse` SHALL remain unchanged and backward-compatible.

---

### Requirement 2: KVStore `setRaw` Method

**User Story:** As a developer, I want a `setRaw` method on `KVStore` that accepts a pre-computed absolute `expiresAt` timestamp, so that replica nodes can store keys with the exact same expiry as the primary without recalculating from a relative TTL.

#### Acceptance Criteria

1. THE `KVStore` class SHALL expose a public method `setRaw(key: string, value: string, expiresAt: number | null): void`.

2. WHEN `setRaw` is called with a numeric `expiresAt`, THE `KVStore` SHALL store the entry with `expiresAt` set to that exact numeric value, without performing any arithmetic on it.

3. WHEN `setRaw` is called with `expiresAt` equal to `null`, THE `KVStore` SHALL store the entry with `expiresAt` set to `null`, indicating no expiry.

4. WHEN `setRaw` is called for a key that already exists, THE `KVStore` SHALL overwrite the existing entry's `value` and `expiresAt` with the supplied values.

5. THE `setRaw` method SHALL have a JSDoc comment describing its purpose, parameters, and return value.

---

### Requirement 3: REPLICATE and REPLICATE_DEL Wire Commands

**User Story:** As a developer, I want two new internal wire commands so that nodes can instruct peer nodes to store or delete a key directly, bypassing the normal routing logic.

#### Acceptance Criteria

1. WHEN a node receives a request with `command: "REPLICATE"`, THE `Router` SHALL call `localStore.setRaw(key, value, expiresAt)` using the values from the request and return `{ ok: true }`, without performing any ring lookup or further replication.

2. WHEN a node receives a request with `command: "REPLICATE_DEL"`, THE `Router` SHALL call `localStore.del(key)` and return `{ ok: true }`, without performing any ring lookup or further replication.

3. IF a `REPLICATE` request is missing the `value` field, THEN THE `Router` SHALL return `{ ok: false, error: "REPLICATE requires value" }`.

4. THE `route` method validation block SHALL accept `"REPLICATE"` and `"REPLICATE_DEL"` as valid commands so they do not trigger the "unknown command" error path.

5. WHEN a `REPLICATE` or `REPLICATE_DEL` command is handled, THE `Router` SHALL NOT forward the request to another node regardless of which node owns the key.

---

### Requirement 4: Router Replication Helpers

**User Story:** As a developer, I want helper methods on `Router` to encapsulate all outbound replication logic, so that SET and DEL execution paths can fan out to replicas in a clean, testable way.

#### Acceptance Criteria

1. THE `Router` class SHALL expose a public method `replicateToNode(targetNodeId: string, key: string, value: string, expiresAt: number | null): Promise<void>` that sends a `REPLICATE` command to the specified peer and returns a promise that resolves when the request completes or the 5-second timeout fires.

2. WHEN `replicateToNode` is called and the peer responds with `ok: true`, THE method SHALL resolve without throwing.

3. WHEN `replicateToNode` is called and the peer is unreachable, times out, or responds with `ok: false`, THE method SHALL catch the error, log a warning in the format `[Replication] WARNING: failed to replicate key <key> to <targetNodeId>: <error message>`, and resolve without rethrowing (fire-and-forget safe).

4. THE `Router` class SHALL expose a public method `replicateDelToNode(targetNodeId: string, key: string): Promise<void>` that sends a `REPLICATE_DEL` command to the specified peer.

5. WHEN `replicateDelToNode` is called and the peer responds with `ok: true`, THE method SHALL resolve without throwing.

6. WHEN `replicateDelToNode` is called and the peer is unreachable, times out, or responds with an error, THE method SHALL throw so the caller can treat DEL replication failures as fatal to the operation.

7. THE `Router` class SHALL expose a private method `getReplicaNodes(key: string): NodeInfo[]` that calls `ring.getNodes(key, 3)`, removes the local node's ID from the result, and returns the remaining `NodeInfo` objects resolved from the cluster config, representing the 2 replica nodes.

8. THE `replicateToNode` and `replicateDelToNode` methods SHALL each enforce a 5-second timeout using the same pattern already used in `forwardToPeer`.

9. THE `replicateToNode` and `replicateDelToNode` methods SHALL have JSDoc comments.

---

### Requirement 5: Async SET Replication

**User Story:** As a developer, I want SET operations to replicate to the 2 replica nodes asynchronously after acknowledging the client, so that write latency is unaffected by replication.

#### Acceptance Criteria

1. WHEN `executeLocally` handles a `SET` command, THE `Router` SHALL call `localStore.set(key, value, ttl)`, then read back the stored entry's `expiresAt` timestamp, return `{ ok: true }` to the caller immediately, and then — without awaiting — call `replicateToNode` for each of the 2 replica nodes in the background.

2. WHEN replication to a replica fails on the SET path, THE `Router` SHALL log a warning (as defined in Req 4.3) and continue. THE client-facing response SHALL remain `{ ok: true }`.

3. WHEN `executeLocally` handles a `SET` command, THE absolute `expiresAt` value retrieved from the local store entry SHALL be passed to `replicateToNode` (not a relative TTL), ensuring replicas store the same absolute expiry timestamp.

4. WHEN a key has no TTL, THE `Router` SHALL pass `expiresAt: null` to `replicateToNode`.

---

### Requirement 6: Synchronous DEL Replication

**User Story:** As a developer, I want DEL operations to propagate to all replica nodes before the client receives a response, so that deleted keys cannot be read from any node after deletion.

#### Acceptance Criteria

1. WHEN `executeLocally` handles a `DEL` command, THE `Router` SHALL call `localStore.del(key)`, then `await` `replicateDelToNode` for each of the 2 replica nodes before returning the response.

2. WHEN all replica DEL replication calls succeed, THE `Router` SHALL return `{ ok: true }`.

3. WHEN any replica DEL replication call fails (throws), THE `Router` SHALL return `{ ok: false, error: <error message> }` to the client.

4. WHILE DEL replication is awaited, THE `Router` SHALL call both replica DEL operations concurrently (e.g., using `Promise.all`).

---

### Requirement 7: Replica Fallback on GET

**User Story:** As a developer, I want GET operations to fall back to replica nodes when the primary store has no value, so that reads succeed even when the primary's local copy is missing.

#### Acceptance Criteria

1. WHEN `executeLocally` handles a `GET` command and `localStore.get(key)` returns a non-null value, THE `Router` SHALL return `{ ok: true, value: <value> }` immediately without contacting any replica.

2. WHEN `executeLocally` handles a `GET` command and `localStore.get(key)` returns `null`, THE `Router` SHALL attempt to retrieve the value from each replica node in order by forwarding a GET request to each peer using the existing `forwardToPeer` mechanism.

3. WHEN a replica returns a non-null value, THE `Router` SHALL add the key to the repair queue (via `ReplicationManager`) and return `{ ok: true, value: <value> }` without trying remaining replicas.

4. WHEN all replicas also return null or are unreachable, THE `Router` SHALL return `{ ok: true }` with no `value` field (a cache miss).

5. THE `Router` SHALL NOT modify the routing path for GET requests received from the network — a GET is still routed to the primary node first; the replica fallback only applies when executing locally on the primary.

---

### Requirement 8: ReplicationManager

**User Story:** As a developer, I want a dedicated ReplicationManager class to own the read-repair queue, so that queue operations are encapsulated and testable independently of the Router.

#### Acceptance Criteria

1. THE project SHALL contain a new file `src/node/ReplicationManager.ts` that exports a class named `ReplicationManager`.

2. THE `ReplicationManager` class SHALL maintain an internal `Set<string>` as its repair queue.

3. THE `ReplicationManager` class SHALL expose a public method `enqueue(key: string): void` that adds `key` to the repair queue.

4. THE `ReplicationManager` class SHALL expose a public method `drainQueue(): string[]` that returns all keys currently in the repair queue as a `string[]` and clears the queue.

5. THE `ReplicationManager` class SHALL expose a public method `queueSize(): number` that returns the current number of keys in the repair queue without modifying it.

6. THE `Router` class SHALL own a single `ReplicationManager` instance and use it for all read-repair queue operations.

7. THE `Router` class SHALL expose a public method `getRepairQueue(): Set<string>` that returns the current repair queue set (for testing and Phase 5 processing).

8. All public methods of `ReplicationManager` SHALL have JSDoc comments.

---

### Requirement 9: Integration Tests — Replication

**User Story:** As a developer, I want a dedicated integration test file for replication behavior, so that all replication scenarios are verified automatically on every change.

#### Acceptance Criteria

1. THE project SHALL contain a new file `test/replication.test.ts` that uses Vitest and real TCP sockets on ports 17001–17003 to spin up 3 `CacheNode` instances using an isolated cluster config.

2. WHEN a key is SET on any node, THE test SHALL verify the key can be retrieved from all 3 nodes directly (bypassing routing) after a short async delay.

3. WHEN a key is SET with a TTL, THE test SHALL verify that all 3 nodes store the same absolute `expiresAt` value (TTL replication consistency).

4. WHEN a key is DEL'd, THE test SHALL verify that all 3 nodes no longer hold the key (synchronous DEL replication).

5. WHEN a key exists on replica nodes but the primary's local store is empty, THE test SHALL verify that a GET request routed to the primary returns the value (replica fallback), AND that the key appears in the Router's repair queue after the GET.

6. THE test file SHALL use `beforeEach`/`afterEach` (or `beforeAll`/`afterAll`) to start and stop all 3 nodes cleanly, and SHALL call `kvStore.stopSweeper()` to prevent timer leaks.

7. THE test file SHALL NOT modify or import from any Phase 1 or Phase 2 test files (`ring.test.ts`, `kvstore.test.ts`, `router.test.ts`, `integration.test.ts`).

---

### Requirement 10: Smoke-Test Replication Section

**User Story:** As a developer, I want the smoke-test script to verify replication behavior against the live Docker cluster, so that end-to-end replication correctness is validated in the real deployed environment.

#### Acceptance Criteria

1. THE `scripts/smoke-test.ts` file SHALL include a new section labelled `── REPLICATE ──` after the existing DEL section.

2. WHEN the smoke test runs the replication section, THE `Smoke_Test` SHALL SET at least 3 keys and verify that each key is readable from all 3 nodes after a short delay (e.g., 50 ms), confirming async replication propagated.

3. WHEN the smoke test runs the replication section, THE `Smoke_Test` SHALL SET at least 1 key with a TTL and verify that all 3 nodes report a cache miss after the TTL has elapsed, confirming TTL consistency.

4. WHEN the smoke test runs the replication section, THE `Smoke_Test` SHALL DEL at least 1 key and verify that all 3 nodes return a miss for that key, confirming synchronous DEL replication.

5. THE smoke-test replication section SHALL use the existing `pass` / `fail` helper functions and contribute to the final summary `passed` / `failed` counters.

6. THE existing smoke-test sections (SET, GET cross-node, DEL) SHALL remain unchanged.
