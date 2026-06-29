# Design Document

## Feature: self-healing-cache (Phase 1)

---

## Overview

Phase 1 delivers two foundational in-process modules for a distributed cache system:

1. **ConsistentHashRing** — a hash ring that maps arbitrary string keys to logical node identifiers using consistent hashing with virtual nodes.
2. **KVStore** — a string key/value store with optional per-entry TTL, lazy expiry at read time, and active background sweep to reclaim memory from unread expired entries.

Both modules run entirely within a single Node.js process. There is no networking, no inter-process communication, and no HTTP/TCP code in Phase 1. The design prioritises testability (every behaviour is exercisable with Vitest and fake timers), correctness (formally stated properties covering all acceptance criteria), and clean extensibility for future phases.

A standalone distribution harness script benchmarks key distribution and compares consistent hashing against naïve modulo hashing over 10,000 UUID keys, providing empirical validation that virtual nodes produce near-uniform distribution.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Single Node.js Process                │
│                                                         │
│  ┌───────────────────┐     ┌────────────────────────┐   │
│  │  ConsistentHashRing│     │       KVStore          │   │
│  │  (src/core/ring.ts)│     │  (src/core/kvstore.ts) │   │
│  │                   │     │                        │   │
│  │ ┌─────────────┐   │     │ ┌────────────────────┐ │   │
│  │ │  RingEntry[]│   │     │ │ Map<string,KVEntry> │ │   │
│  │ │  (sorted ↑) │   │     │ └────────────────────┘ │   │
│  │ └─────────────┘   │     │ ┌────────────────────┐ │   │
│  └─────────┬─────────┘     │ │  sweep: setInterval │ │   │
│            │               │ └────────────────────┘ │   │
│            ▼               └────────────────────────┘   │
│  ┌───────────────────┐                                   │
│  │  src/utils/hash.ts │                                   │
│  │  hashString()      │                                   │
│  │  (SHA-1 → bigint)  │                                   │
│  └───────────────────┘                                   │
│                                                         │
│  ┌───────────────────────────────────────────────────┐   │
│  │  src/types/index.ts  (RingEntry, KVEntry)         │   │
│  └───────────────────────────────────────────────────┘   │
│                                                         │
│  ┌───────────────────────────────────────────────────┐   │
│  │  test/distribution-harness.ts  (tsx script)       │   │
│  └───────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

**Dependency flow** (no circular dependencies):

```
hash.ts  ←  ring.ts  ←  distribution-harness.ts
types/index.ts  ←  ring.ts
types/index.ts  ←  kvstore.ts
```

---

## Components and Interfaces

### `src/utils/hash.ts`

Exports a single pure function:

```typescript
export function hashString(input: string): bigint
```

- Uses `node:crypto` `createHash('sha1')` to produce a 40-character hex digest.
- Returns `BigInt("0x" + hexDigest)`, yielding a value in `[0, 2¹⁶⁰ − 1]`.
- No state; called from both `ring.ts` and the distribution harness.

### `src/types/index.ts`

```typescript
export interface RingEntry {
  hash: bigint;
  nodeId: string;
}

export interface KVEntry {
  value: string;
  expiresAt: number | null; // ms since epoch, or null = never expires
}
```

### `src/core/ring.ts` — `ConsistentHashRing`

Public API:

| Method | Signature | Description |
|---|---|---|
| `addNode` | `(nodeId: string): void` | Adds 200 virtual nodes for `nodeId`, re-sorts ring. |
| `removeNode` | `(nodeId: string): void` | Removes all entries for `nodeId`, preserves sort. |
| `getNode` | `(key: string): string \| null` | Binary-search clockwise lookup; `null` on empty ring. |
| `getNodes` | `(key: string, count: number): string[]` | Returns up to `count` distinct physical nodes clockwise. |
| `getDistribution` | `(): Map<string, number>` | Returns per-node virtual node count. |

Internal state:

```typescript
private ring: RingEntry[] = [];  // always sorted by hash ascending
static readonly VIRTUAL_NODES_PER_NODE = 200;
```

**addNode algorithm:**
1. For `i` in `[0, 199]`, compute `hashString(`${nodeId}#${i}`)` and push `{ hash, nodeId }`.
2. Sort `ring` by `hash` ascending.

**removeNode algorithm:**
1. Filter `ring` to exclude entries where `entry.nodeId === nodeId`.
2. Assign filtered array back (preserves sort order; no re-sort needed).

**getNode algorithm (binary search):**
1. If `ring.length === 0` return `null`.
2. Compute `keyHash = hashString(key)`.
3. Binary search for the lowest index `i` where `ring[i].hash >= keyHash`.
4. If no such index exists (key hash exceeds all), wrap to index 0.
5. Return `ring[i].nodeId`.

**getNodes algorithm:**
1. If ring is empty or `count <= 0` return `[]`.
2. Starting from the position found by `getNode`'s binary search, walk clockwise (modular).
3. Collect `nodeId` values skipping duplicates until `count` unique physical nodes collected or ring exhausted.
4. Return collected node IDs.

### `src/core/kvstore.ts` — `KVStore`

Public API:

| Method | Signature | Description |
|---|---|---|
| `set` | `(key: string, value: string, ttlSeconds?: number): void` | Store entry; `ttlSeconds <= 0` or omitted → `expiresAt = null`. |
| `get` | `(key: string): string \| null` | Returns value or `null`; lazy-deletes expired entries. |
| `del` | `(key: string): boolean` | Deletes entry; returns `true` if existed. |
| `size` | `(): number` | Raw map size (includes unswept expired entries). |
| `stopSweeper` | `(): void` | Clears the sweep `setInterval`. |

Internal state:

```typescript
private store: Map<string, KVEntry> = new Map();
private sweepTimer: ReturnType<typeof setInterval>;
```

Constructor starts `setInterval(this.sweep.bind(this), 1000)` immediately.

**sweep() private method:**
```
for each [key, entry] in store:
  if entry.expiresAt !== null && entry.expiresAt <= Date.now():
    store.delete(key)
```

### `test/distribution-harness.ts`

Standalone executable (`tsx test/distribution-harness.ts`). Not imported by any other module.

**Execution flow:**
1. Create `ConsistentHashRing`, add `node-A`, `node-B`, `node-C`.
2. Generate 10,000 `crypto.randomUUID()` keys.
3. Map each key → node via `getNode`; tally counts per node.
4. Print aligned table (node name padded to 10 chars, count padded to 5 chars, percentage to 1 dp).
5. Add `node-D`; re-map all 10,000 keys; count remapped keys; print remapped %, total, count.
6. Repeat steps 1-5 with modulo hashing (`Number(hashString(key) % BigInt(nodeCount))` → node list index).
7. Print summary paragraph contrasting ~25% (consistent) vs ~75% (modulo) remapping.

---

## Data Models

### RingEntry

```typescript
interface RingEntry {
  hash: bigint;    // SHA-1(nodeId#i) as bigint in [0, 2^160-1]
  nodeId: string;  // Physical node identifier
}
```

The ring is stored as `RingEntry[]` sorted by `hash` ascending. Binary search operates on this array. The ring's conceptual circularity is implemented by wrapping to index 0 when no entry with `hash >= keyHash` exists.

### KVEntry

```typescript
interface KVEntry {
  value: string;
  expiresAt: number | null;
  // null  → never expires
  // N     → epoch ms at which entry becomes invalid
}
```

`Date.now()` is used for all expiry comparisons. Vitest's `vi.useFakeTimers()` intercepts `Date.now()` and `setInterval`, enabling deterministic test control.

### Virtual Node Naming

Virtual nodes are named `${nodeId}#${i}` for `i ∈ [0, 199]`. This means for a node `"node-A"`, the virtual node keys are `"node-A#0"`, `"node-A#1"`, …, `"node-A#199"`. Each produces a distinct hash position on the ring (SHA-1 collision probability across 200 strings is negligible).

---

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system — essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

---

### Property 1: Hash Determinism

*For any* string input, calling `hashString` twice in succession must return the same `bigint` value.

**Validates: Requirements 2.4**

---

### Property 2: Hash Collision Resistance on Random Inputs

*For any* two distinct randomly generated strings, `hashString` must return two unequal `bigint` values.

**Validates: Requirements 2.5**

---

### Property 3: Ring Sort Invariant

*For any* sequence of `addNode` and `removeNode` operations on a `ConsistentHashRing`, the internal ring array must remain sorted by `hash` value in ascending order at all times.

**Validates: Requirements 4.1, 4.3, 4.5**

---

### Property 4: Virtual Node Count on Add

*For any* distinct `nodeId` string added to an initially empty ring exactly once, `getDistribution()` must return exactly 200 for that node.

**Validates: Requirements 4.3, 4.14**

---

### Property 5: Remove Cleans All Entries

*For any* `nodeId` that has been added to the ring, after calling `removeNode(nodeId)`, `getDistribution()` must not contain that `nodeId` as a key, and `getNode` must never return that `nodeId` for any key.

**Validates: Requirements 4.5**

---

### Property 6: getNode Returns a Known Node

*For any* non-empty ring with a known set of physical nodes and *any* key string, `getNode(key)` must return a non-null string that is one of the physical nodes currently in the ring.

**Validates: Requirements 4.7, 4.8, 4.9**

---

### Property 7: getNodes Distinct-Node Guarantee

*For any* non-empty ring with `N` distinct physical nodes, `getNodes(key, count)` with `count > 0` must return an array of length `min(count, N)` containing only distinct `nodeId` strings that are members of the ring.

**Validates: Requirements 4.11, 4.12, 4.13**

---

### Property 8: Non-Expiring Entry Always Readable

*For any* key/value pair stored via `set(key, value)` with no TTL, `get(key)` must return `value` regardless of how much time has passed (and regardless of sweep firing).

**Validates: Requirements 5.2, 5.7**

---

### Property 9: TTL Expiry Boundary

*For any* key stored with a positive `ttlSeconds`:
- `get(key)` must return the stored value at any time strictly before `expiresAt`.
- `get(key)` must return `null` at any time at or after `expiresAt`, and the key must be deleted from the store.

**Validates: Requirements 5.3, 5.8, 5.9**

---

### Property 10: Zero or Negative TTL Treated as No Expiry

*For any* non-positive `ttlSeconds` value passed to `set`, the entry must behave identically to a no-TTL entry — `get` must return the value after any time advance.

**Validates: Requirements 5.4**

---

### Property 11: Overwrite Replaces Both Value and Expiry

*For any* key that already exists in the store, calling `set` again with a different value and/or TTL must result in `get` returning the new value and applying the new expiry policy — the old expiry must no longer govern the entry.

**Validates: Requirements 5.5**

---

### Property 12: Active Sweep Removes All Expired Entries

*For any* set of entries added to the store with positive TTLs, after the sweep interval fires and all TTLs have elapsed, `size()` must reflect that all expired entries have been removed without any `get` call having been made.

**Validates: Requirements 5.12, 5.13**

---

### Property 13: del Removes Entry and Returns Correct Boolean

*For any* key that exists in the store, `del(key)` must return `true` and a subsequent `get(key)` must return `null`. For any key that does not exist, `del(key)` must return `false`.

**Validates: Requirements 5.10, 5.11**

---

## Error Handling

| Scenario | Behaviour |
|---|---|
| `getNode` on empty ring | Returns `null` (no exception). |
| `getNodes` on empty ring | Returns `[]` (no exception). |
| `getNodes` with `count <= 0` | Returns `[]` (no exception). |
| `removeNode` for absent nodeId | No-op; ring unchanged; no exception. |
| `hashString` with empty string | Valid SHA-1 of `""` is computed; no special case needed. |
| `set` with `ttlSeconds = 0` | Treated as no TTL (`expiresAt = null`). |
| `set` with negative `ttlSeconds` | Treated as no TTL (`expiresAt = null`). |
| `stopSweeper` called twice | `clearInterval` on an already-cleared timer is a safe no-op in Node.js. |

All public methods follow a "return a sentinel value rather than throw" contract for expected edge cases. Only truly unexpected failures (e.g., `crypto` module unavailable) would propagate as uncaught exceptions — these are considered environmental failures outside the scope of the module.

---

## Testing Strategy

### Frameworks and Libraries

- **Test runner**: Vitest (`vitest` devDependency)
- **Property-based testing**: [`fast-check`](https://github.com/dubzzz/fast-check) — the most widely used PBT library for TypeScript/JavaScript, with first-class Vitest integration via `fc.assert(fc.property(...))`. Each property test runs a minimum of **100 iterations**.
- **Fake timers**: Vitest's built-in `vi.useFakeTimers()` / `vi.useRealTimers()` for deterministic TTL and sweep control.
- **Script execution**: `tsx` for the distribution harness.

### Test Files

| File | Purpose |
|---|---|
| `test/ring.test.ts` | Unit + property tests for `ConsistentHashRing` |
| `test/kvstore.test.ts` | Unit + property tests for `KVStore` with fake timers |
| `test/distribution-harness.ts` | Standalone script; not a Vitest test file |

### Unit Tests (example-based)

Unit tests cover specific scenarios and edge cases that are not naturally expressed as universal properties:

- `getNode` returns `null` on empty ring (edge case).
- `getNodes` returns `[]` for empty ring and `count <= 0` (edge cases).
- `get` returns `null` for a never-set key (edge case).
- `del` returns `false` for a never-set key (edge case).
- `stopSweeper` halts the sweep interval (verified by advancing fake timers 10 seconds and checking that an expired key's entry is not removed from `size()`).
- Single-node ring: all 10+ distinct keys resolve to that node.

### Property Tests (fast-check)

Each property test corresponds to a numbered property above. Tag format: `// Feature: self-healing-cache, Property N: <property title>`.

| Property | fast-check Arbitraries | Assertion |
|---|---|---|
| P1 Hash Determinism | `fc.string()` | `hashString(s) === hashString(s)` |
| P2 Collision Resistance | `fc.tuple(fc.string(), fc.string()).filter([a,b] => a !== b)` | `hashString(a) !== hashString(b)` |
| P3 Ring Sort Invariant | Random sequence of addNode/removeNode ops | Array is sorted ascending |
| P4 Virtual Node Count | `fc.string()` as nodeId | `getDistribution().get(nodeId) === 200` |
| P5 Remove Cleans Entries | `fc.string()` as nodeId | Node absent from distribution and getNode results |
| P6 getNode Returns Known Node | `fc.array(fc.string(), {minLength:1})` + `fc.string()` | Result is in known node set |
| P7 getNodes Distinct Guarantee | Random ring + key + count | Length = min(count, N), all distinct, all in ring |
| P8 Non-Expiring Entry | `fc.string()` key/value pairs | get returns value after any time advance |
| P9 TTL Expiry Boundary | `fc.nat({min:1, max:3600})` ttl | Before expiry returns value; after returns null |
| P10 Zero/Negative TTL | `fc.integer({max:0})` | Entry never expires |
| P11 Overwrite Replaces | Two different key/value/ttl triples | Second value and expiry govern |
| P12 Active Sweep | Random sets of entries with TTLs | size() = 0 after sweep fires post-expiry |
| P13 del Correctness | `fc.string()` key | del returns true for present, false for absent |

### Fake Timer Protocol for KVStore Tests

```typescript
beforeEach(() => {
  vi.useFakeTimers();
  store = new KVStore();
});

afterEach(() => {
  store.stopSweeper();
  vi.useRealTimers();
});
```

`vi.advanceTimersByTime(ms)` controls both `Date.now()` and `setInterval` firing deterministically.

### Distribution Harness Validation

The harness is validated manually by running `npm run harness` and verifying:
- Output contains three data rows for `node-A`, `node-B`, `node-C`.
- Percentages sum to ~100%.
- Consistent hashing remapping is ~25% (1 of 4 nodes worth of keys).
- Modulo hashing remapping is ~75% (3 of 4 nodes' assignments change).

### Test Coverage Goals

- All 13 correctness properties have a corresponding property test.
- All edge cases (empty ring, empty store, missing keys, boundary TTLs) have unit tests.
- The `distribution-harness.ts` exercises `ConsistentHashRing` and `hashString` end-to-end.
