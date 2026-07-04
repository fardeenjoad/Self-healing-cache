# Implementation Plan: self-healing-cache (Phase 2)

## Overview

Add the distributed networking layer on top of Phase 1. Phase 1 files are **never modified** — only new files are created. The deliverables are: extended shared types, a static cluster config, a TCP server with NDJSON framing, a coordinator-less router, a cache node entry point, a TCP client, Docker/Dockerfile, router unit tests, multi-node integration tests, and a smoke-test script.

## Tasks

- [x] 1. Extend shared type definitions
  - Open `src/types/index.ts` and append (do not remove Phase 1 exports) the following new exports:
    - `NodeInfo` interface: `nodeId: string`, `host: string`, `port: number`
    - `ClusterConfig` type alias: `NodeInfo[]`
    - `CacheRequest` interface: `command: "SET" | "GET" | "DEL"`, `key: string`, `value?: string`, `ttl?: number`
    - `CacheResponse` interface: `ok: boolean`, `value?: string`, `error?: string`
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6_

- [x] 2. Implement static cluster configuration
  - Create `src/config/cluster.ts`
    - Import `NodeInfo` and `ClusterConfig` from `src/types/index.ts`
    - Export `CLUSTER_CONFIG: ClusterConfig` with exactly three entries: `node-a:7001`, `node-b:7002`, `node-c:7003`, all bound to `"0.0.0.0"`
    - Export `getNodeInfo(nodeId: string): NodeInfo | undefined` using `Array.find`
    - No local type aliases — use only the imported types
  - _Requirements: 2.1, 2.2, 2.3, 2.4_

- [x] 3. Implement `CacheClient`
  - Create `src/client/CacheClient.ts`
    - Import `CacheRequest`, `CacheResponse` from `src/types/index.ts`
    - Use `node:net` `Socket`; no third-party packages
    - `constructor(host: string, port: number)` — stores config, initialises buffer and FIFO queue
    - `connect(): Promise<void>` — opens TCP socket, resolves on `'connect'` event, rejects on `'error'`
    - `send(req: CacheRequest): Promise<CacheResponse>` — serialise as `JSON.stringify(req) + '\n'`, enqueue; only one request in-flight at a time; resolve on first complete NDJSON frame received; reject if socket closes while pending
    - `disconnect(): Promise<void>` — calls `socket.end()`, resolves on `'close'` event
    - Guard: `send()` before `connect()` rejects with `"Not connected"`
    - Unexpected close while inflight → reject pending promise
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7_

- [x] 4. Implement `TcpServer`
  - Create `src/node/TcpServer.ts`
    - Import `CacheRequest`, `CacheResponse` from `src/types/index.ts`; use `node:net`
    - `constructor(port: number, requestHandler: (req: CacheRequest) => Promise<CacheResponse>)`
    - `listen(): Promise<void>` — creates `net.Server`, binds to `0.0.0.0:port`, resolves on `'listening'`
    - Per-connection: maintain `buffer: string`; on `'data'` append to buffer; split on `'\n'`; for each non-empty segment call `JSON.parse`; on success call `requestHandler`, then write `JSON.stringify(response) + '\n'`; on parse error write `{ ok: false, error: "invalid JSON" }\n`
    - Backpressure: if `socket.write()` returns `false`, pause socket, resume on `'drain'`
    - On connection `'close'` discard buffer
    - `close(): Promise<void>` — calls `server.close()`, resolves when all connections are done
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9_

- [ ] 5. Implement `Router`
  - Create `src/node/Router.ts`
    - Import `ConsistentHashRing` from `src/core/ring.ts`, `KVStore` from `src/core/kvstore.ts`, `CacheClient` from `src/client/CacheClient.ts`, and all needed types from `src/types/index.ts` and `src/config/cluster.ts`
    - `constructor(localNodeId: string, config: ClusterConfig, localStore: KVStore)`
      - Build `ConsistentHashRing` and `addNode` for every `NodeInfo` in `config`
      - Create a `Map<string, CacheClient>` for peers (lazily connected)
    - `route(req: CacheRequest): Promise<CacheResponse>`
      - Validate first: unknown command → `{ ok: false, error: "unknown command" }`; SET missing value → `{ ok: false, error: "SET requires value" }`
      - `homeNode = ring.getNode(req.key)`
      - If `homeNode === localNodeId` → execute locally:
        - GET: `store.get(key)` → `{ ok: true, value }` or `{ ok: true }` if null
        - SET: `store.set(key, value, ttl)` → `{ ok: true }`
        - DEL: `store.del(key)` → `{ ok: true }`
      - If `homeNode !== localNodeId` → get or create `CacheClient` for home node, `connect()` if needed, `send(req)`, return response; on network error return `{ ok: false, error: message }`
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 4.9, 4.10, 4.11_

- [ ] 6. Implement `CacheNode`
  - Create `src/node/CacheNode.ts`
    - Import `KVStore`, `Router`, `TcpServer`, `CLUSTER_CONFIG`, `getNodeInfo`, and all types needed
    - `constructor(nodeId: string, config: ClusterConfig)`
      - Look up port via `getNodeInfo(nodeId)` — throw if not found
      - Instantiate `KVStore`, `Router(nodeId, config, kvStore)`, `TcpServer(port, req => router.route(req))`
    - `start(): Promise<void>` — calls `tcpServer.listen()`, logs `"Node <nodeId> listening on port <port>"` to stdout; rejects with descriptive message if port in use
    - `stop(): Promise<void>` — calls `tcpServer.close()` and `kvStore.stopSweeper()`
    - Export named `main()` function: reads `process.env.NODE_ID ?? "node-a"`, constructs `CacheNode(nodeId, CLUSTER_CONFIG)`, calls `start()`, registers `SIGINT`/`SIGTERM` → `stop()` then `process.exit(0)`
    - Call `main()` if this file is run directly (check `import.meta.url` vs `process.argv[1]`)
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7_

- [ ] 7. Add Docker support
  - Create `Dockerfile` at repository root:
    - Base: `node:22-alpine`
    - `WORKDIR /app`
    - Copy `package.json`, `package-lock.json`, `tsconfig.json`
    - `RUN npm ci`
    - Copy `src/`
    - `RUN npm run build`
    - `CMD ["node", "dist/node/CacheNode.js"]`
  - Create `docker-compose.yml` at repository root:
    - Define services `node-a`, `node-b`, `node-c`
    - Each: `build: .`, sets `NODE_ID` env var, maps host port = container port (7001/7002/7003), joins shared network `cache-net`
    - Define `networks: cache-net: driver: bridge`
    - Inter-node host resolution: peer hostnames match service names (`node-a`, `node-b`, `node-c`)
  - Update `package.json` to add:
    - `"smoke": "tsx scripts/smoke-test.ts"`
    - `"node:start": "node dist/node/CacheNode.js"`
  - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 11.2, 11.3, 11.4_

- [ ] 8. Implement smoke-test script
  - Create `scripts/smoke-test.ts`
    - Import `CacheClient` from `src/client/CacheClient.ts`; no other non-built-in imports
    - Connect a `CacheClient` to each of the three nodes (ports 7001, 7002, 7003)
    - SET 9 distinct keys (3 per node), each value encodes the originating node ID
    - GET all 9 keys from a **different** node than they were set on; assert values match
    - DEL all 9 keys; assert each returns `{ ok: true }`
    - Print `[PASS]`/`[FAIL]` per operation
    - Print `"All smoke tests passed."` + exit 0 on full pass; `"Smoke tests FAILED."` + exit 1 on any failure
    - Disconnect all clients cleanly before exit
  - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.8, 10.9_

- [ ] 9. Write Router unit tests
  - Create `test/router.test.ts`
    - Use `vi.mock('../../src/client/CacheClient.js')` to prevent real TCP connections
    - Test local GET hit: key hashes to local node → `store.get` called, no forwarding
    - Test local GET miss: key hashes to local node, store returns null → `{ ok: true }` with no value
    - Test local SET: key hashes to local node → `store.set` called, `{ ok: true }`
    - Test local DEL: key hashes to local node → `store.del` called, `{ ok: true }`
    - Test forward path: key hashes to remote node → mocked `CacheClient.send` called with original request, response returned unchanged
    - Test forward error: mocked `CacheClient.send` rejects → `{ ok: false, error: message }`
    - Test SET missing value: `{ ok: false, error: "SET requires value" }`, `store.set` not called
    - Test unknown command: `{ ok: false, error: "unknown command" }`
    - Test ring population: verify all three node IDs appear in `ring.getDistribution()`
  - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7_

- [ ] 10. Write multi-node integration tests
  - Create `test/integration.test.ts`
    - In `beforeAll`: start all three `CacheNode` instances and a `CacheClient` per node; allow 200 ms for sockets to bind
    - In `afterAll`: `stop()` all nodes, `disconnect()` all clients
    - Test: SET via node-a for a key that hashes to node-b → GET directly on node-b returns correct value
    - Test: GET from any node for a key on a different node returns the value (cross-node forwarding)
    - Test: DEL from any node → subsequent GET returns `{ ok: true }` with no value
    - Test: SET with `ttl: 1` → after 1500 ms real wait, GET returns no value
    - Test: 50 distinct keys SET via node-a → `getDistribution()` on the ring shows at least 2 of 3 nodes have keys
    - Test: same key SET via node-a and GET via node-c → same value returned
  - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7_

- [ ] 11. Final checkpoint — Build, test, and verify
  - Run `npm run build` and confirm TypeScript compilation succeeds with no errors
  - Run `npm test` and confirm all tests pass (Phase 1 + Phase 2 `router.test.ts` + `integration.test.ts`)
  - Verify `npm run harness` still exits 0 (Phase 1 harness unaffected)
  - Run `docker compose up --build -d` and then `npm run smoke`; confirm all smoke tests pass and script exits 0
  - Run `docker compose down` to clean up
  - _Requirements: 11.1, 11.4_

## Notes

- Phase 1 files (`src/core/ring.ts`, `src/core/kvstore.ts`, `src/utils/hash.ts`) are **never modified**
- `src/types/index.ts` is extended (appended) but existing Phase 1 exports are preserved
- `CacheClient` uses a FIFO queue — one request in-flight per connection — for simplicity; this is sufficient for the test and demo load
- Integration tests run on ports 7001–7003; ensure no other process occupies these ports during `npm test`
- The `main()` in `CacheNode.ts` should only execute when the file is the process entry point (ESM `import.meta.url` check), not when imported by tests
- Docker inter-node hostnames: inside Docker, each node's peers are reachable via service name (e.g. `node-b`). The `Router` uses `NodeInfo.host` from `CLUSTER_CONFIG` for forwarding. For Docker, set `host` to the service name in each container's env, or accept that the static config uses `"0.0.0.0"` for bind and override peer addresses via `PEER_<NODEID>_HOST` env vars — pick the simpler approach and document it

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1"] },
    { "id": 1, "tasks": ["2", "3"] },
    { "id": 2, "tasks": ["4"] },
    { "id": 3, "tasks": ["5"] },
    { "id": 4, "tasks": ["6"] },
    { "id": 5, "tasks": ["7", "8", "9"] },
    { "id": 6, "tasks": ["10"] },
    { "id": 7, "tasks": ["11"] }
  ]
}
```
