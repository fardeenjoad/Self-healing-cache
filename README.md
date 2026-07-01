# self-healing-cache

A distributed in-memory cache cluster built from scratch in TypeScript — sharded via consistent hashing, replicated for fault tolerance, and capable of surviving node failure without manual intervention.

This is not a Redis wrapper. It is an implementation of the concepts underneath Redis Cluster: consistent hashing, gossip-based failure detection, replication, and automatic rebalancing.

---

## Why This Project Exists

Anyone can call `SET` and `GET` on Redis. Almost nobody has built the thing underneath. This project exists to demonstrate a working understanding of distributed systems primitives — not just how to use distributed tools, but how they work internally.

---

## Architecture

The system is composed of multiple independent cache nodes that together form one logical cluster. Keys are distributed across nodes using a consistent hash ring, ensuring minimal disruption when the cluster topology changes. Each key is replicated to additional nodes clockwise on the ring, so the cluster can survive individual node failures without data loss. A gossip protocol — modelled on SWIM — propagates membership state across nodes without a central coordinator, allowing any node to detect failures and trigger automatic rebalancing. The result is a cluster that heals itself: when a node dies or a new one joins, the system converges to a correct state without human intervention.

---

## Phase 1 — Consistent Hashing Foundation ✓ Complete

### What was built

Phase 1 delivers the two foundational modules that everything else builds on: a consistent hash ring that maps arbitrary string keys to logical node identifiers using SHA-1 hashing and virtual nodes, and a key-value store with optional per-entry TTL expiry backed by both lazy eviction and active background sweeping. Both modules run entirely within a single Node.js process as isolated, fully-tested units — there is no networking, no inter-process communication, and no HTTP or TCP code in this phase. Networking and clustering are the subject of later phases; Phase 1 is about getting the core primitives correct and provably so.

---

### Key design decisions

**SHA-1 for hashing.** The hash ring uses SHA-1, converting the 40-character hex digest to a `bigint` for numeric ring position comparison. SHA-1 is collision-resistant at the scale of a cache cluster, produces the same value deterministically across process restarts, and is the same hash function referenced in the Redis Cluster specification — making this implementation directly comparable to production systems.

**200 virtual nodes per physical node.** Without virtual nodes, a small number of physical nodes produces highly uneven key distribution because their SHA-1 positions cluster randomly on the ring. Each physical node is instead represented by 200 synthetic ring positions — hashed as `nodeId#0` through `nodeId#199` — which spread each node's ownership pseudo-randomly across the full ring. With 200 virtual nodes, even a 3-node cluster achieves near-uniform distribution (within a few percent of the theoretical 33.3% share per node), and adding or removing a node only moves approximately 1/N of all keys.

**Absolute timestamps for TTL.** Entry expiry is stored as `expiresAt: number | null` — an absolute Unix timestamp in milliseconds rather than a relative duration. Storing a relative TTL like "expires in 5 seconds" would be correct for a single-node store, but becomes ambiguous when entries are later replicated over the network: a replica receiving a TTL value needs to know when the key expires in wall-clock time, not how many seconds remain from the moment the primary wrote it. Absolute timestamps eliminate this ambiguity and make TTL propagation across replicas straightforward in future phases.

---

### Consistent hashing vs modulo hashing

The naive approach to key distribution — `nodeIndex = hash(key) % N` — breaks whenever the cluster size changes. Adding or removing a single node changes the modulus, causing the vast majority of keys to resolve to different nodes and triggering a thundering herd of cache misses across the entire cluster. Consistent hashing solves this by placing both keys and nodes on a circular keyspace: only the keys whose hash falls between the new node and its predecessor need to move, which is approximately 1/N of all keys.

The distribution harness validates this empirically over 10,000 random UUID keys:

```
=== Consistent Hashing: Key Distribution (3 nodes) ===

Node          Keys    Share
----------  ------  -------
node-A        3252    32.5%
node-B        3286    32.9%
node-C        3462    34.6%
----------  ------  -------
Total        10000   100.0%

=== Consistent Hashing: Remapping (3 → 4 nodes) ===

  Keys remapped : 2209 / 10000 (22.1%)
  Expected      : ~25% (1 of 4 nodes worth of keys)

=== Modulo Hashing: Remapping (3 → 4 nodes) ===

  Keys remapped : 7485 / 10000 (74.9%)
  Expected      : ~75% ((N-1)/N of all keys)

=== Summary ===

  Consistent hashing remapped ~22.1% of keys when adding a 4th node.
  Modulo hashing remapped ~74.9% of keys for the same operation.
  Consistent hashing is ~3.4x more stable — only keys previously owned
  by the new node need to move, regardless of total cluster size.
```

These numbers match theoretical predictions — consistent hashing guarantees that only 1/N keys move when a new node joins a cluster of N nodes.

---

### TTL expiry strategy

Phase 1 implements two complementary expiry mechanisms: lazy expiry, which evicts a key at read time if its `expiresAt` timestamp has passed, and active sweeping, which runs every 1000ms and removes all expired entries regardless of whether they are ever read again. Lazy expiry alone is insufficient — keys that are written with a TTL but never subsequently read would remain in memory indefinitely, causing unbounded memory growth. The active sweep acts as a safety net that reclaims memory for the long tail of keys that expire silently.

---

## Running Phase 1

```bash
npm install
npm test        # 17 unit tests (9 ring + 8 KVStore)
npm run harness # distribution proof — consistent vs modulo hashing
npm run build   # TypeScript compile check
```

---

## Roadmap

- [x] **Phase 1 — Consistent Hashing Foundation**: Hash ring with 200 virtual nodes, KV store with TTL, distribution proof
- [ ] **Phase 2 — Multi-Node Cluster**: Multiple independent Node.js processes, coordinator-less routing (any node accepts any request and proxies internally), Docker Compose setup
- [ ] **Phase 3 — Replication**: Every key replicated to N+1 nodes clockwise on the ring, replica fallback on primary miss, TTL propagation across replicas
- [ ] **Phase 4 — Gossip + Failure Detection**: SWIM-style heartbeat gossip, membership state machine (alive → suspect → dead), cluster-wide failure propagation
- [ ] **Phase 5 — Failover + Rebalancing**: Automatic traffic rerouting to replicas on node failure, zero-downtime key rebalancing when nodes join or rejoin
- [ ] **Phase 6 — Chaos Demo**: Live kill of a random node mid-traffic, zero failed client requests beyond one configurable retry

---

## Stretch Goals

- LRU/LFU eviction policy under memory pressure, per node
- Cluster-wide quorum reads/writes as an optional strict consistency mode

---

## Tech Stack

Node.js · TypeScript · Vitest · SHA-1 (node:crypto) · fast-check (property tests)
