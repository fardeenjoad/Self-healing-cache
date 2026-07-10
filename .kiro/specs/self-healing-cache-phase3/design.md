# Design Document

## Feature: self-healing-cache-phase3

---

## Overview

Phase 3 extends the 3-node TCP cluster established in Phase 2 with **key replication**. Every key is stored on all 3 nodes (1 primary + 2 replicas), enabling reads to succeed even when the primary's local copy is absent.

**What Phase 3 adds:**

- `setRaw(key, value, expiresAt)` on `KVStore` — stores a key with a pre-computed absolute `expiresAt` timestamp so replicas never recalculate TTL.
- `REPLICATE` and `REPLICATE_DEL` internal wire commands — received by a node and executed locally, bypassing ring routing.
- Async SET replication — primary writes locally, returns `OK` immediately, then fans out to the 2 replica nodes in the background (fire-and-forget).
- Synchronous DEL replication — primary deletes locally and awaits confirmation from both replicas before returning `OK`.
- Replica fallback on GET — if the primary's local store returns `null`, the Router tries each replica in order; a hit enqueues the key for later read-repair.
- `ReplicationManager` (`src/node/ReplicationManager.ts`) — owns the in-memory repair queue.
- Integration test file `test/replication.test.ts` on ports 17001-17003.
- Extended `scripts/smoke-test.ts` with a `── REPLICATE ──` section.

**What is unchanged:** `ring.ts`, `hash.ts`, `CacheClient.ts`, `cluster.ts`, `TcpServer.ts`, all existing test files (`ring.test.ts`, `kvstore.test.ts`, `router.test.ts`, `integration.test.ts`). No gossip, no failure detection, no automatic node removal.


---

## Architecture

### Component Overview

```
┌────────────────────────────────────────────────────────────────────────┐
│  node-a (port 7001)                                                    │
│                                                                        │
│  ┌──────────┐   route()   ┌──────────────────────────────────────┐    │
│  │TcpServer │────────────▶│            Router                    │    │
│  └──────────┘             │                                      │    │
│                           │  ┌──────────────────────────────┐   │    │
│                           │  │  ConsistentHashRing           │   │    │
│                           │  └──────────────────────────────┘   │    │
│                           │  ┌──────────────────────────────┐   │    │
│                           │  │  KVStore (local)              │   │    │
│                           │  │  + setRaw() [NEW]             │   │    │
│                           │  └──────────────────────────────┘   │    │
│                           │  ┌──────────────────────────────┐   │    │
│                           │  │  ReplicationManager [NEW]     │   │    │
│                           │  │  repair queue: Set<string>    │   │    │
│                           │  └──────────────────────────────┘   │    │
│                           │  ┌──────────────────────────────┐   │    │
│                           │  │  peers: Map<nodeId,           │   │    │
│                           │  │    CacheClient>               │   │    │
│                           │  └──────────────────────────────┘   │    │
│                           └──────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────────────────┘
```

### New Data-Flow Paths

```
── Async SET fan-out ────────────────────────────────────────────────────
Client ──SET──▶ node-a (primary)
                  │ 1. localStore.set(key, value, ttl)
                  │ 2. read expiresAt = localStore.getExpiresAt(key)
                  │ 3. return {ok:true} to client  ◀── immediate
                  │ 4. [background] replicateToNode(node-b, REPLICATE)
                  │ 5. [background] replicateToNode(node-c, REPLICATE)
                  ├──REPLICATE──▶ node-b: setRaw(key, value, expiresAt)
                  └──REPLICATE──▶ node-c: setRaw(key, value, expiresAt)

── Sync DEL fan-out ─────────────────────────────────────────────────────
Client ──DEL──▶ node-a (primary)
                  │ 1. localStore.del(key)
                  │ 2. await Promise.all([
                  │      replicateDelToNode(node-b, REPLICATE_DEL),
                  │      replicateDelToNode(node-c, REPLICATE_DEL)
                  │    ])
                  │ 3. return {ok:true} (or {ok:false} on failure)
                  ├──REPLICATE_DEL──▶ node-b: localStore.del(key)
                  └──REPLICATE_DEL──▶ node-c: localStore.del(key)

── GET replica fallback ─────────────────────────────────────────────────
Client ──GET──▶ node-x ──[ring routes]──▶ node-a (primary)
                                              │ 1. localStore.get(key) → null
                                              │ 2. forwardToPeer(node-b, GET)
                                              │    → value found?
                                              │    yes: repairQueue.enqueue(key)
                                              │         return {ok:true, value}
                                              │    no:  forwardToPeer(node-c, GET)
                                              │         → value found? return it
                                              │         → miss: return {ok:true}

── REPLICATE / REPLICATE_DEL receive path ───────────────────────────────
node-b TCP ──REPLICATE──▶ TcpServer ──▶ Router.route()
                                           │ validation: command is REPLICATE
                                           │ bypass ring lookup
                                           ▼
                                        executeLocally()
                                           │ localStore.setRaw(key, value, expiresAt)
                                           └─▶ return {ok:true}
```


---

## Components and Interfaces

### 1. `src/types/index.ts` — Extended CacheRequest

**Change:** extend `command` union and add `expiresAt` field.

```typescript
export interface CacheRequest {
  command: "SET" | "GET" | "DEL" | "REPLICATE" | "REPLICATE_DEL";  // extended
  key: string;
  value?: string;
  ttl?: number;
  expiresAt?: number | null;  // NEW — absolute expiry timestamp in ms, or null
  id?: string;
}
```

All other interfaces (`CacheResponse`, `KVEntry`, `RingEntry`, `NodeInfo`, `ClusterConfig`) are unchanged.

**Invariant:** `expiresAt` is only meaningful on `REPLICATE` commands. For `SET`, `GET`, `DEL`, `REPLICATE_DEL` the field is ignored if present.


---

### 2. `src/core/kvstore.ts` — New `setRaw` and `getExpiresAt` Methods

**Design decision:** rather than exposing the raw `KVEntry` object (which would leak internal structure), two targeted methods are added:

- `setRaw(key, value, expiresAt)` — stores with a pre-computed absolute timestamp.
- `getExpiresAt(key): number | null | undefined` — returns the stored `expiresAt` for a key, or `undefined` if the key does not exist. This is used by the `SET` replication path to read back the exact `expiresAt` that was stored.

Rationale for `getExpiresAt` over exposing the full `KVEntry`: `KVEntry` is an internal representation detail. Exposing a single typed accessor preserves encapsulation and gives the Router exactly the one piece of data it needs without importing `KVEntry`.

**New method signatures:**

```typescript
/**
 * Stores a key-value pair with a pre-computed absolute expiry timestamp.
 *
 * Unlike `set`, no arithmetic is performed on the expiry value — the supplied
 * `expiresAt` is stored verbatim. Use this method on replica nodes to mirror
 * the exact expiry that the primary computed when the key was first written.
 *
 * @param key       - The string key to store.
 * @param value     - The string value to store.
 * @param expiresAt - Absolute Unix timestamp in milliseconds, or null for no expiry.
 */
setRaw(key: string, value: string, expiresAt: number | null): void

/**
 * Returns the stored expiresAt value for the given key without performing any
 * expiry check or deletion.
 *
 * @param key - The string key to inspect.
 * @returns The absolute expiry timestamp in ms, null if the key never expires,
 *          or undefined if the key does not exist in the store.
 */
getExpiresAt(key: string): number | null | undefined
```

**Implementation:**

```
setRaw(key, value, expiresAt):
  store.set(key, { value, expiresAt })   // direct assignment, no arithmetic

getExpiresAt(key):
  entry = store.get(key)
  if entry === undefined: return undefined
  return entry.expiresAt
```

**Invariant:** `setRaw` never modifies `expiresAt` — the exact value passed in is the exact value stored. The existing `sweep()` and lazy-expiry logic in `get()` still applies to entries created by `setRaw`.


---

### 3. `src/node/ReplicationManager.ts` — New File

Full class design:

```typescript
/**
 * Manages the read-repair queue for the Router.
 *
 * When a GET request is served from a replica (because the primary's local
 * store returned null), the key is enqueued here so that a future repair
 * pass (Phase 5) can re-synchronise the primary's local store.
 *
 * The queue is an in-memory Set<string>. Duplicate enqueues for the same key
 * are deduplicated automatically by the Set semantics.
 */
export class ReplicationManager {
  private queue: Set<string> = new Set();

  /**
   * Adds `key` to the repair queue. If the key is already in the queue,
   * this is a no-op (Set deduplication).
   */
  enqueue(key: string): void

  /**
   * Returns all keys currently in the repair queue as an array, then clears
   * the queue. Subsequent calls to queueSize() will return 0 until new keys
   * are enqueued.
   *
   * @returns A string array of all queued keys (order not guaranteed).
   */
  drainQueue(): string[]

  /**
   * Returns the number of keys currently in the repair queue without
   * modifying it.
   */
  queueSize(): number
}
```

**Implementation:**

```
enqueue(key):
  queue.add(key)

drainQueue():
  result = Array.from(queue)
  queue.clear()
  return result

queueSize():
  return queue.size
```

**Key properties:**
- Enqueuing the same key twice results in a queue size of 1 (Set semantics).
- `drainQueue()` is destructive and idempotent: calling it twice returns `[]` the second time.
- `queueSize()` is a pure read — it never mutates state.


---

### 4. `src/node/Router.ts` — Extended

#### 4.1 Constructor Changes

The `Router` constructor gains one new field:

```typescript
private readonly repairQueue: ReplicationManager = new ReplicationManager();
```

No other constructor changes.

#### 4.2 `route()` — Updated Validation Block

The validation block must accept the two new commands:

```typescript
async route(req: CacheRequest): Promise<CacheResponse> {
  // Validation — extended to include REPLICATE and REPLICATE_DEL
  const validCommands = ["GET", "SET", "DEL", "REPLICATE", "REPLICATE_DEL"];
  if (!validCommands.includes(req.command)) {
    return { ok: false, error: "unknown command" };
  }
  if (req.command === "SET" && req.value === undefined) {
    return { ok: false, error: "SET requires value" };
  }
  if (req.command === "REPLICATE" && req.value === undefined) {
    return { ok: false, error: "REPLICATE requires value" };
  }

  // REPLICATE and REPLICATE_DEL bypass ring routing entirely — always local
  if (req.command === "REPLICATE" || req.command === "REPLICATE_DEL") {
    return this.executeLocally(req);
  }

  // Normal routing for SET / GET / DEL
  const homeNodeId = this.ring.getNode(req.key);
  if (homeNodeId === null) {
    return { ok: false, error: "ring is empty" };
  }
  if (homeNodeId === this.localNodeId) {
    return this.executeLocally(req);
  }
  return this.forwardToPeer(homeNodeId, req);
}
```

#### 4.3 `executeLocally()` — Now `async`, Extended Switch

`executeLocally` becomes `async` because the `GET` replica fallback path calls `await forwardToPeer(...)`:

```typescript
private async executeLocally(req: CacheRequest): Promise<CacheResponse> {
  switch (req.command) {
    case "GET": {
      const val = this.localStore.get(req.key);
      if (val !== null) {
        return { ok: true, value: val };
      }
      // Replica fallback
      const replicas = this.getReplicaNodes(req.key);
      for (const replica of replicas) {
        const resp = await this.forwardToPeer(replica.nodeId, req);
        if (resp.ok && resp.value !== undefined) {
          this.repairQueue.enqueue(req.key);
          return { ok: true, value: resp.value };
        }
      }
      return { ok: true }; // full miss
    }

    case "SET": {
      this.localStore.set(req.key, req.value!, req.ttl);
      const expiresAt = this.localStore.getExpiresAt(req.key) ?? null;
      // Fire-and-forget replication
      const replicas = this.getReplicaNodes(req.key);
      for (const replica of replicas) {
        void this.replicateToNode(replica.nodeId, req.key, req.value!, expiresAt);
      }
      return { ok: true };
    }

    case "DEL": {
      this.localStore.del(req.key);
      const replicas = this.getReplicaNodes(req.key);
      try {
        await Promise.all(
          replicas.map((r) => this.replicateDelToNode(r.nodeId, req.key))
        );
        return { ok: true };
      } catch (err: unknown) {
        return {
          ok: false,
          error: err instanceof Error ? err.message : String(err),
        };
      }
    }

    case "REPLICATE": {
      // req.value is guaranteed present (validated in route())
      this.localStore.setRaw(req.key, req.value!, req.expiresAt ?? null);
      return { ok: true };
    }

    case "REPLICATE_DEL": {
      this.localStore.del(req.key);
      return { ok: true };
    }

    default:
      return { ok: false, error: "unknown command" };
  }
}
```


#### 4.4 `getReplicaNodes(key)` — New Private Method

```typescript
/**
 * Returns the NodeInfo objects for the 2 replica nodes responsible for `key`.
 *
 * Calls ring.getNodes(key, 3) to get the ordered list of up to 3 distinct
 * physical nodes clockwise from the key's ring position (primary first).
 * Filters out localNodeId and maps the remaining IDs to NodeInfo objects
 * from the cluster config.
 *
 * In a 3-node cluster this always returns exactly 2 NodeInfo objects.
 * If the ring has fewer than 3 distinct nodes, fewer are returned.
 *
 * @param key - The cache key whose replicas are needed.
 * @returns Array of NodeInfo for replica nodes (excluding this node).
 */
private getReplicaNodes(key: string): NodeInfo[] {
  return this.ring
    .getNodes(key, 3)
    .filter((nodeId) => nodeId !== this.localNodeId)
    .map((nodeId) => this.config.find((n) => n.nodeId === nodeId)!)
    .filter(Boolean);  // defensive: drop any unresolved IDs
}
```

**Invariant:** In a 3-node cluster where all nodes are in the ring, this always returns exactly 2 `NodeInfo` objects. The order matches the clockwise ring order, so the "closest" replica is tried first in the GET fallback path.

#### 4.5 `replicateToNode()` — New Public Method

```typescript
/**
 * Sends a REPLICATE command to the specified target node, instructing it to
 * store key/value with the given absolute expiresAt timestamp.
 *
 * This is fire-and-forget safe: failures are caught, a warning is logged,
 * and the returned Promise always resolves (never rejects). This allows callers
 * to use `void replicateToNode(...)` without risk of unhandled rejection.
 *
 * Enforces a 5-second timeout using the same race pattern as forwardToPeer.
 *
 * @param targetNodeId - The node to replicate to.
 * @param key          - The cache key.
 * @param value        - The value to store.
 * @param expiresAt    - Absolute expiry timestamp in ms, or null.
 */
public async replicateToNode(
  targetNodeId: string,
  key: string,
  value: string,
  expiresAt: number | null
): Promise<void> {
  try {
    const resp = await this.forwardToPeer(targetNodeId, {
      command: "REPLICATE",
      key,
      value,
      expiresAt,
    });
    if (!resp.ok) {
      throw new Error(resp.error ?? "REPLICATE returned ok:false");
    }
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    console.warn(`[Replication] WARNING: failed to replicate key ${key} to ${targetNodeId}: ${msg}`);
    // Resolve (not reject) — fire-and-forget
  }
}
```

#### 4.6 `replicateDelToNode()` — New Public Method

```typescript
/**
 * Sends a REPLICATE_DEL command to the specified target node, instructing it
 * to delete the given key.
 *
 * Unlike replicateToNode, this method THROWS on failure so that the DEL path
 * can treat replication failures as fatal and return {ok:false} to the client.
 *
 * Enforces a 5-second timeout using the same race pattern as forwardToPeer.
 *
 * @param targetNodeId - The node to send the delete command to.
 * @param key          - The key to delete.
 * @throws {Error} If the target is unreachable, times out, or returns ok:false.
 */
public async replicateDelToNode(targetNodeId: string, key: string): Promise<void> {
  const resp = await this.forwardToPeer(targetNodeId, {
    command: "REPLICATE_DEL",
    key,
  });
  if (!resp.ok) {
    throw new Error(
      `REPLICATE_DEL to ${targetNodeId} failed: ${resp.error ?? "unknown error"}`
    );
  }
}
```

**Note:** `forwardToPeer` already handles the 5-second timeout and returns `{ ok: false, error: "..." }` on network failure or timeout. `replicateDelToNode` inspects the response and throws when `!resp.ok`, which covers both network failures and explicit error responses.

#### 4.7 `getRepairQueue()` — New Public Accessor

```typescript
/**
 * Returns the current repair queue set (for testing and Phase 5 processing).
 *
 * @returns The internal Set<string> of keys pending read-repair.
 */
public getRepairQueue(): Set<string> {
  return this.repairQueue["queue"] as Set<string>;  // or repairManager exposes it
}
```

A cleaner alternative is to expose `getRepairQueue()` from `ReplicationManager` directly and delegate:

```typescript
public getRepairQueue(): Set<string> {
  // ReplicationManager exposes its internal queue for inspection
  return this.repairQueue.getQueue();
}
```

Either approach works; the design adds `getQueue(): Set<string>` to `ReplicationManager` as a test-only accessor.


---

## Wire Protocol Changes

`REPLICATE` and `REPLICATE_DEL` are regular NDJSON frames sent over the existing TCP connections managed by `CacheClient`. No new sockets, no new ports, no changes to `TcpServer.ts` or `CacheClient.ts`.

**REPLICATE frame (sender → receiver):**

```json
{"command":"REPLICATE","key":"foo","value":"bar","expiresAt":1700000000000,"id":"<uuid>"}
```

Fields:
- `command`: `"REPLICATE"` (required)
- `key`: cache key (required)
- `value`: value to store (required — missing value returns error)
- `expiresAt`: absolute Unix ms timestamp or `null` (optional; defaults to `null` if absent)
- `id`: correlation UUID added by `CacheClient.send()` (used for response matching)

**REPLICATE response:**

```json
{"ok":true,"id":"<uuid>"}
```

or on error:

```json
{"ok":false,"error":"REPLICATE requires value","id":"<uuid>"}
```

**REPLICATE_DEL frame:**

```json
{"command":"REPLICATE_DEL","key":"foo","id":"<uuid>"}
```

Fields:
- `command`: `"REPLICATE_DEL"` (required)
- `key`: cache key to delete (required)
- `id`: correlation UUID

**REPLICATE_DEL response:**

```json
{"ok":true,"id":"<uuid>"}
```

**Flow through existing infrastructure:**

```
replicateToNode() / replicateDelToNode()
  │
  ▼
forwardToPeer(targetNodeId, req)
  │  lazy connect via peers Map (existing CacheClient)
  ▼
CacheClient.send(req)  — adds UUID id, writes NDJSON to TCP socket
  │
  ▼  [TCP wire]
TcpServer._handleConnection()  — parses NDJSON frame
  │
  ▼
Router.route(req)  — sees REPLICATE/REPLICATE_DEL, bypasses ring
  │
  ▼
executeLocally(req)  — calls setRaw() or del()
  │
  ▼
TcpServer  — writes response NDJSON back
  │
  ▼  [TCP wire]
CacheClient._drainBuffer()  — matches id, resolves promise
  │
  ▼
replicateToNode() / replicateDelToNode() — checks resp.ok
```


---

## Data Models

### Updated `CacheRequest`

```typescript
interface CacheRequest {
  command: "SET" | "GET" | "DEL" | "REPLICATE" | "REPLICATE_DEL";
  key: string;
  value?: string;       // required for SET and REPLICATE
  ttl?: number;         // used by SET only (relative seconds)
  expiresAt?: number | null;  // used by REPLICATE only (absolute ms)
  id?: string;          // UUID correlation ID (CacheClient adds this)
}
```

### `KVEntry` (unchanged, but `setRaw` now writes it directly)

```typescript
interface KVEntry {
  value: string;
  expiresAt: number | null;  // absolute ms timestamp, or null
}
```

`setRaw` writes `{ value, expiresAt }` verbatim, while `set` computes `expiresAt = Date.now() + ttl * 1000`.

### Repair Queue

Owned by `ReplicationManager` as `private queue: Set<string>`. The `Set` deduplicated by key string — enqueuing the same key multiple times costs nothing extra and doesn't create duplicates in the drain result.


---

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system — essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

**Property Reflection:**

Before listing properties, redundancy was checked:

- Requirements 2.2 (setRaw stores exact expiresAt) and 2.4 (setRaw overwrites existing entry) are both subsumed by a single round-trip property: "for any (key, value, expiresAt), setRaw then getExpiresAt returns the same expiresAt." This covers both the exact-storage and the overwrite cases.
- Requirements 3.1 (REPLICATE stores value) and 3.5 (REPLICATE bypasses routing) both describe the same behavior: REPLICATE executes locally regardless of ring assignment. They are combined into one property.
- Requirements 5.1 (async SET replication propagates) and 5.3 (absolute expiresAt propagates) are separate concerns with distinct verifiable outcomes, so they remain as separate properties.
- Requirements 8.3, 8.4, 8.5 (enqueue/drain/queueSize) form a single logical round-trip property and one idempotence property; merged accordingly.

---

### Property 1: `setRaw` Stores Exact `expiresAt` (Round-Trip)

*For any* key, value, and `expiresAt` value (including `null`), calling `setRaw(key, value, expiresAt)` and then calling `getExpiresAt(key)` must return the exact value that was passed to `setRaw` — no arithmetic transformation is applied.

**Validates: Requirements 2.1, 2.2, 2.3, 2.4**

---

### Property 2: REPLICATE Bypasses Ring Routing

*For any* key (regardless of which node the consistent hash ring designates as its primary) and *any* receiving node, sending a `REPLICATE` command directly to that node must cause the key/value to be stored in that node's local store, returning `{ ok: true }`.

**Validates: Requirements 3.1, 3.5**

---

### Property 3: REPLICATE_DEL Bypasses Ring Routing

*For any* key present in a node's local store and *any* receiving node, sending a `REPLICATE_DEL` command to that node must remove the key from that node's local store and return `{ ok: true }`, regardless of ring assignment.

**Validates: Requirements 3.2, 3.5**

---

### Property 4: Async SET Replication Convergence

*For any* key and value SET on the primary node, after a bounded delay both replica nodes must have the same value stored in their local stores, accessible via `localStore.get(key)`.

**Validates: Requirements 5.1, 5.2**

---

### Property 5: TTL Timestamp Consistency Across Replicas

*For any* key SET with a TTL on the primary node, the `expiresAt` value stored on all 3 nodes must be identical — the replicas receive the primary's pre-computed absolute timestamp, not a re-computed one.

**Validates: Requirements 5.3, 5.4**

---

### Property 6: Synchronous DEL Removes Key from All Nodes

*For any* key that has been replicated to all 3 nodes, after a DEL command on the primary returns `{ ok: true }`, calling `localStore.get(key)` on all 3 nodes must return `null`.

**Validates: Requirements 6.1, 6.2**

---

### Property 7: Replica Fallback Returns Value and Enqueues Key

*For any* key stored on replica nodes (via `setRaw`) but absent from the primary's local store, a GET request routed to the primary must return the value found on the first available replica, and the key must appear in the Router's repair queue after the response.

**Validates: Requirements 7.2, 7.3**

---

### Property 8: Repair Queue Enqueue/Drain Round-Trip

*For any* non-empty set of distinct key strings enqueued via `ReplicationManager.enqueue`, calling `drainQueue()` must return a collection containing exactly those keys (as a set), and a subsequent call to `queueSize()` must return 0.

**Validates: Requirements 8.3, 8.4, 8.5**

---

### Property 9: Repair Queue Deduplication

*For any* key enqueued multiple times into `ReplicationManager`, the repair queue must treat it as a single entry — `queueSize()` must return 1, and `drainQueue()` must contain the key exactly once.

**Validates: Requirements 8.2, 8.3**

---

### Property 10: `getReplicaNodes` Returns Non-Local Nodes

*For any* key and *any* local node in a 3-node cluster, `getReplicaNodes(key)` must return exactly 2 `NodeInfo` objects, none of which has `nodeId` equal to `localNodeId`, and all of which are present in the cluster config.

**Validates: Requirements 4.7**

---

## Error Handling

| Failure Mode | Component | Response |
|---|---|---|
| `REPLICATE` received with no `value` field | `Router.route()` | `{ ok: false, error: "REPLICATE requires value" }` |
| Unknown command string | `Router.route()` | `{ ok: false, error: "unknown command" }` |
| `replicateToNode` — peer unreachable | `Router.replicateToNode()` | Catches error, logs `[Replication] WARNING: failed to replicate key <key> to <nodeId>: <msg>`, resolves (fire-and-forget). Client receives `{ ok: true }` for the SET. |
| `replicateToNode` — peer times out (5 s) | `Router.replicateToNode()` | Same as above — `forwardToPeer` returns `{ ok: false, error: "timed out" }`, `replicateToNode` catches and logs. |
| `replicateDelToNode` — peer unreachable | `Router.replicateDelToNode()` | `forwardToPeer` returns `{ ok: false }`, method throws. DEL path returns `{ ok: false, error: <msg> }` to client. |
| `replicateDelToNode` — peer times out (5 s) | `Router.replicateDelToNode()` | `forwardToPeer` times out and returns `{ ok: false }`, method throws. DEL returns error to client. |
| GET — all replicas miss or unreachable | `Router.executeLocally()` | Returns `{ ok: true }` with no `value` field (cache miss, not an error). |
| GET — replica response `ok: false` | `Router.executeLocally()` | Treated as a miss; tries next replica. |
| `getReplicaNodes` — config has no matching `NodeInfo` | `Router.getReplicaNodes()` | `filter(Boolean)` silently drops unresolvable IDs. In a healthy cluster this never happens. |
| Ring is empty | `Router.route()` | Returns `{ ok: false, error: "ring is empty" }` (unchanged from Phase 2). |


---

## Testing Strategy

### Frameworks

- **Test runner**: Vitest (existing)
- **Property-based testing**: `fast-check` (existing, already used in Phase 1 tests)
- **Integration tests**: real TCP sockets on ports 17001-17003

### Test Files

| File | Type | Purpose |
|---|---|---|
| `test/replication.test.ts` | Integration + PBT | Phase 3 replication scenarios on isolated ports |
| `test/kvstore.test.ts` | Unit + PBT | Extended with `setRaw` and `getExpiresAt` property tests |
| `test/router.test.ts` | Unit | Extended with REPLICATE/REPLICATE_DEL routing tests |

Existing test files (`ring.test.ts`, `integration.test.ts`, `distribution-harness.ts`) are **not modified**.

### Isolated Test Cluster Config

`test/replication.test.ts` uses a private cluster config to avoid port conflicts with the live cluster and Phase 2 integration tests:

```typescript
const TEST_CONFIG: ClusterConfig = [
  { nodeId: "test-node-a", host: "127.0.0.1", port: 17001 },
  { nodeId: "test-node-b", host: "127.0.0.1", port: 17002 },
  { nodeId: "test-node-c", host: "127.0.0.1", port: 17003 },
];
```

### Lifecycle in `replication.test.ts`

```typescript
let nodes: CacheNode[];
let clients: CacheClient[];

beforeAll(async () => {
  nodes = TEST_CONFIG.map((n) => new CacheNode(n.nodeId, TEST_CONFIG));
  await Promise.all(nodes.map((n) => n.start()));
  clients = TEST_CONFIG.map((n) => new CacheClient(n.host, n.port));
  await Promise.all(clients.map((c) => c.connect()));
});

afterAll(async () => {
  await Promise.all(clients.map((c) => c.disconnect()));
  await Promise.all(nodes.map((n) => n.stop()));
});
```

`node.stop()` calls `kvStore.stopSweeper()` (via `CacheNode.stop()`), preventing timer leaks.

For tests that need a clean state between runs, a helper clears all nodes between scenarios via `REPLICATE_DEL` or by using `beforeEach` / `afterEach` with fresh node instances.

### Integration Test Scenarios

| Scenario | Property Validated | Key Assertions |
|---|---|---|
| SET on any node → verify all 3 nodes after 50 ms delay | P4: Async replication convergence | `clients[i].send({command:"GET", key})` returns value from all 3 ports |
| SET with TTL → verify expiresAt consistency | P5: TTL timestamp consistency | Each node's Router exposes `localStore.getExpiresAt(key)` equality |
| SET with TTL → wait for expiry → verify all miss | P5 + P6 continuation | After TTL elapses, all 3 nodes return no value |
| DEL after replication → verify all nodes miss | P6: Sync DEL | After DEL returns ok, all 3 nodes GET returns miss |
| GET from primary when key absent → replica fallback + repair queue | P7: Replica fallback | GET returns value; `router.getRepairQueue()` contains key |
| GET full miss (all nodes empty) | Error handling | Returns `{ ok: true }` with no `value` |

### Unit / PBT Tests for New KVStore Methods

In `test/kvstore.test.ts`, add property tests using `fast-check`:

```typescript
// Feature: self-healing-cache-phase3, Property 1: setRaw stores exact expiresAt
fc.assert(fc.property(
  fc.string(), fc.string(),
  fc.option(fc.integer({ min: Date.now(), max: Date.now() + 1e9 }), { nil: null }),
  (key, value, expiresAt) => {
    const store = new KVStore();
    store.setRaw(key, value, expiresAt);
    expect(store.getExpiresAt(key)).toBe(expiresAt);
    store.stopSweeper();
  }
), { numRuns: 100 });
```

### PBT for ReplicationManager

```typescript
// Feature: self-healing-cache-phase3, Property 8: enqueue/drain round-trip
fc.assert(fc.property(
  fc.array(fc.string(), { minLength: 1, maxLength: 50 }),
  (keys) => {
    const mgr = new ReplicationManager();
    for (const k of keys) mgr.enqueue(k);
    const drained = mgr.drainQueue();
    const unique = new Set(keys);
    expect(new Set(drained)).toEqual(unique);
    expect(mgr.queueSize()).toBe(0);
  }
), { numRuns: 100 });

// Feature: self-healing-cache-phase3, Property 9: enqueue deduplication
fc.assert(fc.property(
  fc.string(), fc.integer({ min: 1, max: 20 }),
  (key, times) => {
    const mgr = new ReplicationManager();
    for (let i = 0; i < times; i++) mgr.enqueue(key);
    expect(mgr.queueSize()).toBe(1);
    expect(mgr.drainQueue()).toEqual([key]);
  }
), { numRuns: 100 });
```

Each property-based test runs a minimum of **100 iterations** (`numRuns: 100`).

Tag format for all property tests:
```
// Feature: self-healing-cache-phase3, Property N: <property title>
```


---

## Constraints — What Is NOT Changing

The following files are **explicitly out of scope** for Phase 3 modifications:

| File | Reason |
|---|---|
| `src/core/ring.ts` | Hash ring logic is complete; `getNodes(key, 3)` is already implemented |
| `src/utils/hash.ts` | SHA-1 hashing utility is stable |
| `src/client/CacheClient.ts` | TCP client already supports all needed operations; reused as-is for replication |
| `src/config/cluster.ts` | Production cluster config is unchanged; tests use their own config |
| `src/node/TcpServer.ts` | NDJSON framing and request dispatch are unchanged |
| `src/node/node-entry.ts` | Process entry point unchanged |
| `src/node/CacheNode.ts` | Composition layer unchanged (Router already wired to TcpServer) |
| `test/ring.test.ts` | Phase 1 ring tests are not modified |
| `test/kvstore.test.ts` | Phase 1 KVStore tests are not modified (new tests are additions only) |
| `test/router.test.ts` | Phase 2 router tests are not modified (new tests are additions only) |
| `test/integration.test.ts` | Phase 2 integration tests are not modified |
| `test/distribution-harness.ts` | Distribution benchmark is unchanged |

No gossip protocol, failure detection, automatic node removal, or read-repair processing is implemented in Phase 3. The repair queue is built and filled; processing is deferred to Phase 5.

