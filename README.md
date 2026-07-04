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

## Phase 2 — Multi-Node TCP Cluster ✓ Complete

### What was built

Phase 2 adds a real distributed cluster on top of the Phase 1 foundation. Each cache node runs as its own OS process listening on a TCP port, implements coordinator-less routing using the Phase 1 `ConsistentHashRing`, and can forward requests to peer nodes transparently. The cluster is containerised with Docker Compose and includes a smoke-test script for end-to-end validation.

New files added — Phase 1 source is completely unchanged:

| File | Purpose |
|------|---------|
| `src/node/TcpServer.ts` | NDJSON-framed TCP server; buffers partial chunks, echoes correlation IDs, applies backpressure |
| `src/node/Router.ts` | Coordinator-less routing: local execution or peer forwarding with 5-second timeout |
| `src/node/CacheNode.ts` | Wires `KVStore`, `Router`, and `TcpServer` into one startable/stoppable unit |
| `src/node/node-entry.ts` | Process entry point; reads `NODE_ID` env var, handles `SIGTERM`/`SIGINT` for graceful shutdown |
| `src/client/CacheClient.ts` | TCP client with concurrent pending-requests Map keyed by UUID correlation ID |
| `src/config/cluster.ts` | Static 3-node topology (`node-a:7001`, `node-b:7002`, `node-c:7003`) |
| `Dockerfile` + `docker-compose.yml` | Three-container cluster on a shared bridge network |
| `scripts/smoke-test.ts` | Live cluster E2E verifier |

---

### Wire protocol

All messages are newline-delimited JSON (NDJSON) over raw TCP — one JSON object per line. The client assigns a UUID `id` to each request; the server echoes it back in the response so that concurrent in-flight requests can be matched to their promises without serialising calls.

```
→  {"command":"SET","key":"foo","value":"bar","id":"abc-123"}\n
←  {"ok":true,"id":"abc-123"}\n

→  {"command":"GET","key":"foo","id":"def-456"}\n
←  {"ok":true,"value":"bar","id":"def-456"}\n

→  {"command":"DEL","key":"foo","id":"ghi-789"}\n
←  {"ok":true,"id":"ghi-789"}\n
```

---

### Coordinator-less routing

Every node in the cluster runs an identical `ConsistentHashRing` built from the same static `ClusterConfig`. Because the ring is deterministic, any node can receive any request and independently compute which node owns the key — no central coordinator is needed.

```
Client → node-a:  SET key="hello" value="world"
  node-a: ring.getNode("hello") → "node-b"   ← not me
  node-a: forward to node-b:7002 via CacheClient
    node-b: ring.getNode("hello") → "node-b" ← me, execute locally
    node-b: kvStore.set("hello", "world")
    node-b: respond { ok: true }
  node-a: proxy response back to client
Client ← node-a:  { ok: true }
```

The client issued one request and received one response. The internal forwarding hop is invisible. Any of the three nodes can receive any command for any key — routing is always correct.

Forwarding uses a lazy-connect pattern: peer `CacheClient` instances are created at startup but connected on first use and kept open. A 5-second timeout guard destroys the socket and evicts the peer from the connected set if the remote node does not respond, preventing indefinite hangs.

---

### Running the cluster

**Prerequisites:** Docker and Docker Compose installed.

```bash
# Build images and start all three nodes
docker compose up --build

# In a separate terminal — run the smoke test against the live cluster
npm run smoke
```

Expected smoke-test output:

```
[INFO] Connected to node-a on port 7001
[INFO] Connected to node-b on port 7002
[INFO] Connected to node-c on port 7003

── SET ──
[PASS] SET smoke:a:1 → node-a
[PASS] SET smoke:a:2 → node-a
...

── GET (cross-node) ──
[PASS] GET smoke:a:1 from node-b
[PASS] GET smoke:a:2 from node-c
...

── DEL ──
[PASS] DEL smoke:a:1 via node-b
...

All smoke tests passed. (27/27)
```

To start a single node locally (without Docker):

```bash
npm run build
NODE_ID=node-a npm run node:start
```

---

### Phase 2 test results

```
Test Files  4 passed (4)
Tests       32 passed (32)

  test/ring.test.ts        9 tests   — Phase 1 ring (unchanged)
  test/kvstore.test.ts     8 tests   — Phase 1 KVStore (unchanged)
  test/router.test.ts      9 tests   — Router unit tests (CacheClient mocked)
  test/integration.test.ts 6 tests   — Real TCP, 3 nodes in-process, ports 17001-17003
```

Integration tests spin up all three `CacheNode` instances in-process and verify: cross-node SET/GET, DEL and subsequent miss, TTL expiry, key distribution across ≥2 nodes, and routing consistency across all entry points.

---

## Running Phase 2

```bash
npm test           # 32 tests — Phase 1 + Phase 2 unit and integration tests
npm run build      # TypeScript compile check
docker compose up --build   # Start 3-node cluster
npm run smoke      # Smoke test against live cluster (27/27 operations)
docker compose down         # Tear down cluster
```

---

## Roadmap

- [x] **Phase 1 — Consistent Hashing Foundation**: Hash ring with 200 virtual nodes, KV store with TTL, distribution proof
- [x] **Phase 2 — Multi-Node Cluster**: Multiple independent Node.js processes, coordinator-less routing (any node accepts any request and proxies internally), Docker Compose setup
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

Node.js · TypeScript · Vitest · SHA-1 (node:crypto) · fast-check (property tests) · Docker Compose
