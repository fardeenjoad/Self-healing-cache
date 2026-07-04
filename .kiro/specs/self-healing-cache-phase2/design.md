# Design Document

## Feature: self-healing-cache (Phase 2)

---

## Overview

Phase 2 adds a real distributed cache cluster on top of the Phase 1 foundation. Each node runs as its own OS process, listens on a TCP port, and independently routes every request using the same `ConsistentHashRing` from Phase 1. No coordinator process exists — any node can handle any key by forwarding to the correct owner.

The wire protocol is newline-delimited JSON (NDJSON) over raw TCP. Three nodes run on ports 7001–7003. Docker Compose brings up the full cluster with a single command. A `CacheClient` class drives tests and a smoke-test script exercises the live cluster end-to-end.

**Phase 1 source files are completely unchanged.** Phase 2 adds only new files.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  Docker Compose / Local Process Group                               │
│                                                                     │
│  ┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐
│  │     node-a :7001     │  │     node-b :7002     │  │     node-c :7003     │
│  │  ┌────────────────┐  │  │  ┌────────────────┐  │  │  ┌────────────────┐  │
│  │  │   CacheNode    │  │  │  │   CacheNode    │  │  │  │   CacheNode    │  │
│  │  │  ┌──────────┐  │  │  │  │  ┌──────────┐  │  │  │  │  ┌──────────┐  │
│  │  │  │TcpServer │  │  │  │  │  │TcpServer │  │  │  │  │  │TcpServer │  │
│  │  │  └────┬─────┘  │  │  │  │  └────┬─────┘  │  │  │  │  └────┬─────┘  │
│  │  │  ┌────▼─────┐  │  │  │  │  ┌────▼─────┐  │  │  │  │  ┌────▼─────┐  │
│  │  │  │  Router  │  │  │  │  │  │  Router  │  │  │  │  │  │  Router  │  │
│  │  │  │  (ring)  │  │  │  │  │  │  (ring)  │  │  │  │  │  │  (ring)  │  │
│  │  │  └────┬─────┘  │  │  │  │  └────┬─────┘  │  │  │  │  └────┬─────┘  │
│  │  │  ┌────▼─────┐  │  │  │  │  ┌────▼─────┐  │  │  │  │  ┌────▼─────┐  │
│  │  │  │  KVStore │  │  │  │  │  │  KVStore │  │  │  │  │  │  KVStore │  │
│  │  │  └──────────┘  │  │  │  │  └──────────┘  │  │  │  │  └──────────┘  │
│  │  └────────────────┘  │  │  └────────────────┘  │  │  └────────────────┘  │
│  └──────────────────────┘  └──────────────────────┘  └──────────────────────┘
│           │                         │                         │               │
│           └─────────────────────────┴─────────────────────────┘               │
│                              Docker bridge network                            │
└─────────────────────────────────────────────────────────────────────────────┘

External:  CacheClient ──TCP──► any node port
           scripts/smoke-test.ts ──► CacheClient × 3
```

**Dependency flow** (no circular dependencies):

```
src/types/index.ts          ← (Phase 1, extended)
src/utils/hash.ts           ← (Phase 1, unchanged)
src/core/ring.ts            ← (Phase 1, unchanged)
src/core/kvstore.ts         ← (Phase 1, unchanged)
src/config/cluster.ts       ← types/index.ts
src/client/CacheClient.ts   ← types/index.ts
src/node/TcpServer.ts       ← types/index.ts
src/node/Router.ts          ← types/index.ts, core/ring.ts, core/kvstore.ts, client/CacheClient.ts, config/cluster.ts
src/node/CacheNode.ts       ← types/index.ts, core/kvstore.ts, node/Router.ts, node/TcpServer.ts, config/cluster.ts
scripts/smoke-test.ts       ← client/CacheClient.ts, config/cluster.ts
test/router.test.ts         ← node/Router.ts, core/kvstore.ts, config/cluster.ts
test/integration.test.ts    ← node/CacheNode.ts, client/CacheClient.ts, config/cluster.ts
```

---

## Wire Protocol

All messages are **NDJSON** — a single-line JSON object terminated by `\n`.

### CacheRequest (client → node)

```typescript
{ command: "SET", key: string, value: string, ttl?: number }
{ command: "GET", key: string }
{ command: "DEL", key: string }
```

### CacheResponse (node → client)

```typescript
{ ok: true, value?: string }   // GET with hit, SET, DEL
{ ok: false, error: string }   // any error
```

The response is written back on the same TCP connection immediately after the request is processed.

---

## Components and Interfaces

### `src/types/index.ts` (extended)

Phase 1 exports are preserved. Phase 2 adds:

```typescript
export interface NodeInfo {
  nodeId: string;
  host: string;
  port: number;
}

export type ClusterConfig = NodeInfo[];

export interface CacheRequest {
  command: "SET" | "GET" | "DEL";
  key: string;
  value?: string;
  ttl?: number;
}

export interface CacheResponse {
  ok: boolean;
  value?: string;
  error?: string;
}
```

### `src/config/cluster.ts`

```typescript
export const CLUSTER_CONFIG: ClusterConfig = [
  { nodeId: "node-a", host: "0.0.0.0", port: 7001 },
  { nodeId: "node-b", host: "0.0.0.0", port: 7002 },
  { nodeId: "node-c", host: "0.0.0.0", port: 7003 },
];

export function getNodeInfo(nodeId: string): NodeInfo | undefined
```

### `src/node/TcpServer.ts` — `TcpServer`

Owns a `net.Server`. One per `CacheNode`.

**State per connection:**
```typescript
let buffer = "";
```

**Data flow:**
```
socket 'data' → append to buffer
                 → split on '\n'
                 → JSON.parse each complete segment
                 → requestHandler(parsed) → CacheResponse
                 → JSON.stringify(response) + '\n' → socket.write()
```

**Backpressure:** if `socket.write()` returns `false`, pause socket reads until `'drain'`.

**Error handling:** malformed JSON → write `{ ok: false, error: "invalid JSON" }\n`, continue.

Public API:
```typescript
constructor(port: number, requestHandler: (req: CacheRequest) => Promise<CacheResponse>)
listen(): Promise<void>
close(): Promise<void>
```

### `src/node/Router.ts` — `Router`

Owns a `ConsistentHashRing` (populated from `ClusterConfig` in constructor) and a `CacheClient` per remote peer (lazily connected on first forward, reused thereafter).

**Route decision tree:**
```
ring.getNode(req.key) === localNodeId ?
  ├── yes → execute locally against KVStore
  └── no  → forward via CacheClient to home node
```

**Local execution:**
| Command | Action | Response |
|---------|--------|----------|
| GET | `store.get(key)` | `{ ok: true, value }` or `{ ok: true }` if null |
| SET | `store.set(key, value, ttl)` | `{ ok: true }` |
| DEL | `store.del(key)` | `{ ok: true }` |

**Validation (before routing):**
- Unknown command → `{ ok: false, error: "unknown command" }`
- SET with no `value` → `{ ok: false, error: "SET requires value" }`

**Forward error:** network failure → `{ ok: false, error: message }`, no retry.

Public API:
```typescript
constructor(localNodeId: string, config: ClusterConfig, localStore: KVStore)
route(req: CacheRequest): Promise<CacheResponse>
```

### `src/node/CacheNode.ts` — `CacheNode`

Composes `KVStore`, `Router`, `TcpServer` into a single unit.

```typescript
constructor(nodeId: string, config: ClusterConfig)
start(): Promise<void>   // calls TcpServer.listen(), logs ready
stop(): Promise<void>    // calls TcpServer.close(), KVStore.stopSweeper()
```

Also exports `main()` for the process entry point:
```typescript
async function main() {
  const nodeId = process.env.NODE_ID ?? "node-a";
  const node = new CacheNode(nodeId, CLUSTER_CONFIG);
  await node.start();
  process.on("SIGINT",  async () => { await node.stop(); process.exit(0); });
  process.on("SIGTERM", async () => { await node.stop(); process.exit(0); });
}
```

### `src/client/CacheClient.ts` — `CacheClient`

Thin TCP client. One connection per instance. FIFO request queue — only one in-flight request at a time per connection.

**State:**
```typescript
private socket: net.Socket | null = null;
private buffer = "";
private queue: Array<{ req: CacheRequest; resolve: (r: CacheResponse) => void; reject: (e: Error) => void }> = [];
private inflight = false;
```

**send() flow:**
1. Enqueue `{ req, resolve, reject }`.
2. If not inflight, dequeue and send.
3. Buffer incoming data; on `\n`, JSON.parse, resolve the pending promise, dequeue next.

Public API:
```typescript
constructor(host: string, port: number)
connect(): Promise<void>
send(req: CacheRequest): Promise<CacheResponse>
disconnect(): Promise<void>
```

---

## Data Models

### NodeInfo

```typescript
interface NodeInfo {
  nodeId: string;  // e.g. "node-a"
  host: string;    // e.g. "0.0.0.0" (server bind) or "127.0.0.1" (client connect)
  port: number;    // 7001 | 7002 | 7003
}
```

`host` in `CLUSTER_CONFIG` is used for binding. When `CacheClient` connects between nodes inside Docker, it uses the Docker service name as host. For local/test usage, `127.0.0.1` is used.

### CacheRequest / CacheResponse

```typescript
// Request (client → server)
{ command: "SET", key: "foo", value: "bar", ttl: 60 }
{ command: "GET", key: "foo" }
{ command: "DEL", key: "foo" }

// Response (server → client)
{ ok: true, value: "bar" }  // GET hit
{ ok: true }                // SET, DEL, GET miss
{ ok: false, error: "..." } // any error
```

---

## Routing Flow (Step-by-Step)

**Example: Client connects to node-a, sets key "hello" which hashes to node-b**

```
CacheClient → node-a TcpServer: { command:"SET", key:"hello", value:"world" }\n
node-a TcpServer → node-a Router.route({ command:"SET", key:"hello", value:"world" })
node-a Router: ring.getNode("hello") === "node-b" ≠ "node-a"
  → forward via CacheClient(127.0.0.1, 7002).send(req)
node-a CacheClient → node-b TcpServer: { command:"SET", key:"hello", value:"world" }\n
node-b Router: ring.getNode("hello") === "node-b" === "node-b"
  → store.set("hello", "world")
  → return { ok: true }
node-b TcpServer → node-a CacheClient: { ok:true }\n
node-a Router → node-a TcpServer: { ok: true }
node-a TcpServer → original CacheClient: { ok:true }\n
```

---

## Docker Setup

### Dockerfile

```
FROM node:22-alpine
WORKDIR /app
COPY package.json tsconfig.json ./
RUN npm install
COPY src/ ./src/
RUN npm run build
CMD ["node", "dist/node/CacheNode.js"]
```

### docker-compose.yml structure

Three services (`node-a`, `node-b`, `node-c`) each:
- Sets `NODE_ID` env var
- Exposes its port (7001/7002/7003)
- Shares a single bridge network `cache-net`

Inter-node forwarding inside Docker uses service names as hostnames (e.g. `node-a`, `node-b`, `node-c`). The `CLUSTER_CONFIG` hosts need to be overridable for Docker — the `CacheClient` used for forwarding should resolve the service hostname from `NodeInfo.host`. For Docker, each service's `NODE_HOST` or the service name convention is used.

> **Design note**: `CLUSTER_CONFIG` uses `"0.0.0.0"` for server bind. For inter-node forwarding inside Docker, the `Router` resolves peer addresses using Docker service names (matching `nodeId`, e.g., `"node-a"`). An environment variable `PEER_HOST_<NODEID>` override pattern can be used, or the config can be split into bind host vs. connect host. For Phase 2, the simplest approach is: `getNodeInfo` returns the service name as host when running in Docker (driven by env), and `127.0.0.1` in tests.

---

## Correctness Properties (Phase 2)

### Property 14: Routing Consistency

*For any* key and *any* entry-point node, `ring.getNode(key)` must return the same node ID on every node in the cluster (all nodes use the same ring state).

### Property 15: Local Execution Completeness

*For any* `SET(key, value)` routed to the home node, a subsequent `GET(key)` on the home node must return `value` (local store round-trip).

### Property 16: Cross-Node Forwarding Transparency

*For any* key set via node X (which forwards to home node Y), a `GET` for that key sent directly to Y must return the same value.

### Property 17: NDJSON Framing Correctness

*For any* sequence of `CacheRequest` objects written as NDJSON frames to a `TcpServer`, each must produce exactly one `CacheResponse` frame in the same order.

### Property 18: Validation Rejects Before Routing

*For any* malformed request (unknown command, SET without value), the error response must be returned before any ring lookup or store operation.

---

## Error Handling

| Scenario | Behaviour |
|---|---|
| Malformed JSON frame | `{ ok: false, error: "invalid JSON" }`, connection kept alive |
| SET missing value | `{ ok: false, error: "SET requires value" }` |
| Unknown command | `{ ok: false, error: "unknown command" }` |
| Forward TCP error | `{ ok: false, error: <Node.js error message> }` |
| CacheClient not connected | Throw/reject with `"Not connected"` |
| Port already in use | `CacheNode.start()` rejects with descriptive error |
| Unexpected socket close during send | Pending `CacheClient.send()` rejects |

---

## Testing Strategy

### Test Files

| File | Type | Description |
|---|---|---|
| `test/router.test.ts` | Unit (mocked) | Router logic with `CacheClient` mocked — no TCP |
| `test/integration.test.ts` | Integration (real TCP) | All three `CacheNode` instances in-process, real sockets |
| `scripts/smoke-test.ts` | E2E script | Live Docker cluster, not a Vitest file |

### Router Unit Tests

`CacheClient` is replaced with a Vitest mock (`vi.mock`). Tests inject keys known to hash to specific nodes (precomputed) or stub `ring.getNode` directly. All 6 acceptance criteria scenarios are covered:
- Local GET hit / miss
- Local SET / DEL
- Forward path (mocked `CacheClient.send`)
- Forward error propagation
- SET validation
- Unknown command

### Integration Tests

All three `CacheNode` instances start in `beforeAll` with a 200 ms settle delay. A `CacheClient` per node is used to drive commands. Tests exercise:
- Cross-node SET → GET consistency
- DEL and subsequent GET miss
- Short TTL expiry (1 s TTL, 1.5 s wait)
- Distribution across nodes (50 keys across 3 nodes)
- Same value returned regardless of which node receives the GET

Ports 7001–7003 are used in tests (same as production). Tests must be run serially or with unique port offsets if parallelism is needed.

### Smoke-Test Script

Not a Vitest file. Run with `npx tsx scripts/smoke-test.ts` against a live Docker cluster. Connects to all three nodes, performs 27 operations (9 SET + 9 GET from different nodes + 9 DEL), prints `[PASS]`/`[FAIL]` per operation, exits 0 or 1.
