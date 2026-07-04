# Requirements Document

## Introduction

This document defines requirements for Phase 2 of the **self-healing-cache** project — the networking layer built on top of the Phase 1 foundation. Phase 2 adds a real distributed cache cluster: each node runs as its own OS process listening on a TCP port, a coordinator-less routing layer uses the Phase 1 `ConsistentHashRing` to forward requests to the correct node, and a thin TCP client is provided for testing and demos.

All Phase 1 source files (`src/core/ring.ts`, `src/core/kvstore.ts`, `src/utils/hash.ts`, `src/types/index.ts`) are **unchanged** by this phase. Phase 2 adds new files only.

The wire protocol is newline-delimited JSON over raw TCP (not HTTP). Docker Compose brings up exactly three nodes on ports 7001, 7002, and 7003. The cluster topology is static — there is no gossip, discovery, or replication in this phase.

---

## Glossary

- **CacheNode**: A single cache server process that owns one `KVStore` instance, accepts TCP connections, and delegates routing decisions to a `Router`.
- **Router**: The module responsible for inspecting a request's key, consulting the `ConsistentHashRing`, and either serving the request locally (key is owned by this node) or forwarding the request to the correct peer node over TCP.
- **TcpServer**: The TCP transport layer that listens on a port, buffers incoming bytes, splits the stream on newline characters, parses JSON frames, and dispatches them to the `CacheNode`.
- **CacheClient**: A TCP client that connects to a single node endpoint and sends/receives newline-delimited JSON frames. Intended for tests, scripts, and demos.
- **NodeInfo**: A record describing a cluster member: its `nodeId` string, `host` string, and `port` number.
- **ClusterConfig**: The static cluster topology — an array of `NodeInfo` objects known to every node at startup.
- **CacheRequest**: The wire-format command sent by a client: `{ command: "SET" | "GET" | "DEL", key: string, value?: string, ttl?: number }`.
- **CacheResponse**: The wire-format reply sent by a node: `{ ok: boolean, value?: string, error?: string }`.
- **Coordinator-less routing**: An architecture where no single node acts as a coordinator; every node independently uses the same `ConsistentHashRing` to determine key ownership, and any node can forward a misrouted request to the correct owner.
- **Home node**: The node identified by `ConsistentHashRing.getNode(key)` as the owner for a given key.
- **Forwarding**: The act of a non-home node relaying a client request to the home node over a TCP connection and proxying the response back to the client.
- **NDJSON (Newline-Delimited JSON)**: A framing protocol where each message is a complete JSON object followed by a newline character (`\n`). Partial frames are buffered until a newline is received.
- **Frame**: A single complete NDJSON message (one JSON object + newline).
- **Smoke-test script**: A standalone `tsx`-executable script that exercises the live cluster end-to-end by setting, getting, and deleting keys across all three nodes.
- **Virtual node**: Inherited from Phase 1 — each physical `nodeId` occupies 200 synthetic positions on the hash ring.

---

## Requirements

### Requirement 1: Extended Type Definitions

**User Story:** As a developer, I want shared TypeScript interfaces for all Phase 2 network and cluster types, so that `CacheNode`, `Router`, `TcpServer`, and `CacheClient` all use a single source of truth for message shapes.

#### Acceptance Criteria

1. THE `src/types/index.ts` module SHALL be extended (Phase 1 exports preserved) to additionally export an interface `NodeInfo` with fields: `nodeId: string`, `host: string`, and `port: number`.

2. THE `src/types/index.ts` module SHALL export a type alias `ClusterConfig` defined as `NodeInfo[]`.

3. THE `src/types/index.ts` module SHALL export an interface `CacheRequest` with fields: `command: "SET" | "GET" | "DEL"`, `key: string`, `value?: string`, and `ttl?: number`.

4. THE `src/types/index.ts` module SHALL export an interface `CacheResponse` with fields: `ok: boolean`, `value?: string`, and `error?: string`.

5. THE `CacheRequest` interface SHALL constrain `command` to the union `"SET" | "GET" | "DEL"` with no other values permitted by the type system.

6. THE `CacheResponse` interface SHALL allow `value` to be present only when `ok` is `true` and SHALL allow `error` to be present only when `ok` is `false`; this SHOULD be enforced via a discriminated union or documented clearly for implementers.

---

### Requirement 2: Static Cluster Configuration

**User Story:** As a developer, I want a single static configuration file that declares all cluster nodes by ID, host, and port, so that every node and client loads the same topology at startup without any discovery mechanism.

#### Acceptance Criteria

1. THE `src/config/cluster.ts` module SHALL export a constant `CLUSTER_CONFIG` of type `ClusterConfig` containing exactly three entries: `{ nodeId: "node-a", host: "0.0.0.0", port: 7001 }`, `{ nodeId: "node-b", host: "0.0.0.0", port: 7002 }`, and `{ nodeId: "node-c", host: "0.0.0.0", port: 7003 }`.

2. THE `src/config/cluster.ts` module SHALL export a function `getNodeInfo(nodeId: string): NodeInfo | undefined` that returns the `NodeInfo` for the given `nodeId` or `undefined` if not found.

3. THE `src/config/cluster.ts` module SHALL import `NodeInfo` and `ClusterConfig` from `src/types/index.ts` and SHALL NOT define any local type aliases that duplicate those interfaces.

4. WHEN `CLUSTER_CONFIG` is imported by any module, THE `Cluster_Config` SHALL be treated as immutable; no module SHALL mutate the array or its entries at runtime.

---

### Requirement 3: TCP Server and NDJSON Framing

**User Story:** As a developer, I want a TCP server that buffers raw byte streams and emits complete parsed JSON frames to the application layer, so that the rest of the system never sees partial messages or raw bytes.

#### Acceptance Criteria

1. THE `TcpServer` class SHALL accept a `port: number` and a `requestHandler: (req: CacheRequest) => Promise<CacheResponse>` callback in its constructor.

2. WHEN `TcpServer.listen()` is called, THE `TcpServer` SHALL bind a Node.js `net.Server` to `0.0.0.0` on the configured port and begin accepting TCP connections.

3. WHEN a TCP connection is established, THE `TcpServer` SHALL maintain a per-connection string buffer and append all incoming data chunks to it.

4. WHEN the per-connection buffer contains one or more newline characters, THE `TcpServer` SHALL split on each `\n`, attempt `JSON.parse` on every non-empty segment before the final newline, and invoke the `requestHandler` with each successfully parsed `CacheRequest`.

5. WHEN `JSON.parse` fails on an incoming frame, THE `TcpServer` SHALL write a `CacheResponse` of `{ ok: false, error: "invalid JSON" }` followed by `\n` to the connection and discard that frame without closing the connection.

6. WHEN the `requestHandler` returns a `CacheResponse`, THE `TcpServer` SHALL serialize it with `JSON.stringify`, append `\n`, and write the resulting string to the same TCP connection.

7. WHEN a TCP connection is closed by the client, THE `TcpServer` SHALL discard the per-connection buffer and release all associated resources.

8. WHEN `TcpServer.close()` is called, THE `TcpServer` SHALL stop accepting new connections and close the underlying `net.Server`; existing connections MAY be allowed to drain.

9. THE `TcpServer` SHALL handle backpressure by respecting the return value of `socket.write()` — WHEN `socket.write()` returns `false`, THE `TcpServer` SHALL pause reading from that socket until the `drain` event fires.

---

### Requirement 4: Router — Coordinator-less Key Routing

**User Story:** As a developer, I want a Router module that uses the ConsistentHashRing to decide whether to serve a request locally or forward it to the correct peer node, so that any node in the cluster can handle any key correctly without a central coordinator.

#### Acceptance Criteria

1. THE `Router` class SHALL accept a `localNodeId: string`, a `ClusterConfig`, and a `localStore: KVStore` in its constructor and SHALL construct a `ConsistentHashRing` internally, adding all nodes from `ClusterConfig` to the ring during construction.

2. WHEN `Router.route(req: CacheRequest): Promise<CacheResponse>` is called, THE `Router` SHALL call `ring.getNode(req.key)` to determine the home node.

3. WHEN the home node equals `localNodeId`, THE `Router` SHALL execute the request against `localStore` directly and return the result as a `CacheResponse`.

4. WHEN the home node differs from `localNodeId`, THE `Router` SHALL forward the request to the home node's TCP endpoint (as specified in `ClusterConfig`) using `CacheClient` and return the response it receives.

5. WHEN a `GET` request is executed locally and `localStore.get(key)` returns a non-null value, THE `Router` SHALL return `{ ok: true, value: <storedValue> }`.

6. WHEN a `GET` request is executed locally and `localStore.get(key)` returns `null`, THE `Router` SHALL return `{ ok: true, value: undefined }` (key not found is not an error).

7. WHEN a `SET` request is executed locally, THE `Router` SHALL call `localStore.set(key, value, ttl)` and return `{ ok: true }`.

8. WHEN a `DEL` request is executed locally, THE `Router` SHALL call `localStore.del(key)` and return `{ ok: true }`.

9. WHEN a forwarded request fails due to a network error, THE `Router` SHALL return `{ ok: false, error: <errorMessage> }` without retrying.

10. WHEN `Router.route` receives a `CacheRequest` with a `command` value other than `"SET"`, `"GET"`, or `"DEL"`, THE `Router` SHALL return `{ ok: false, error: "unknown command" }`.

11. WHEN a `SET` request is missing a `value` field, THE `Router` SHALL return `{ ok: false, error: "SET requires value" }` without writing to the store.

---

### Requirement 5: CacheNode — Node Server Entry Point

**User Story:** As a developer, I want a CacheNode class that wires together a KVStore, a Router, and a TcpServer into a single startable/stoppable unit, so that each cluster member can be launched as an independent OS process.

#### Acceptance Criteria

1. THE `CacheNode` class SHALL accept a `nodeId: string` and a `ClusterConfig` in its constructor and SHALL instantiate a `KVStore`, a `Router` (passing the `KVStore`), and a `TcpServer` internally.

2. THE `CacheNode` constructor SHALL look up its own port from `ClusterConfig` using `nodeId` and configure the `TcpServer` with that port.

3. WHEN `CacheNode.start()` is called, THE `CacheNode` SHALL call `TcpServer.listen()` and log `"Node <nodeId> listening on port <port>"` to stdout.

4. WHEN `CacheNode.stop()` is called, THE `CacheNode` SHALL call `TcpServer.close()` and `KVStore.stopSweeper()` to release all resources.

5. WHEN the `TcpServer` invokes its `requestHandler`, THE `CacheNode` SHALL delegate the request to `Router.route()` and return the resulting `CacheResponse`.

6. WHEN `CacheNode.start()` is called and the port is already in use, THE `CacheNode` SHALL emit an error event or reject the returned promise with a descriptive message; it SHALL NOT crash the process silently.

7. THE `src/node/CacheNode.ts` module SHALL export `CacheNode` as a named export and SHALL also contain a `main()` function that reads `NODE_ID` from `process.env`, constructs a `CacheNode` with `CLUSTER_CONFIG`, calls `start()`, and registers `SIGINT`/`SIGTERM` handlers that call `stop()` before exiting.

---

### Requirement 6: CacheClient — TCP Client

**User Story:** As a developer, I want a CacheClient that connects to a single node's TCP endpoint and can send typed cache commands and receive typed responses, so that tests and demo scripts can interact with the cluster programmatically.

#### Acceptance Criteria

1. THE `CacheClient` class SHALL accept a `host: string` and `port: number` in its constructor.

2. WHEN `CacheClient.connect()` is called, THE `CacheClient` SHALL open a TCP connection to the configured host and port and resolve when the connection is established.

3. WHEN `CacheClient.send(req: CacheRequest): Promise<CacheResponse>` is called, THE `CacheClient` SHALL serialize `req` as NDJSON (JSON + `\n`), write it to the socket, and resolve the promise with the first complete NDJSON frame received in response.

4. THE `CacheClient` SHALL queue concurrent `send` calls and match each response to its corresponding request in FIFO order (one request in-flight at a time per connection, or a clearly documented multiplexing strategy).

5. WHEN `CacheClient.disconnect()` is called, THE `CacheClient` SHALL close the TCP connection gracefully (send FIN) and resolve when the connection is fully closed.

6. WHEN the TCP connection is closed unexpectedly while a `send` call is pending, THE `CacheClient` SHALL reject the pending promise with a descriptive error.

7. WHEN `CacheClient.send()` is called before `connect()` has been called, THE `CacheClient` SHALL throw or reject with `"Not connected"`.

---

### Requirement 7: Docker Compose Cluster Topology

**User Story:** As a developer, I want a Docker Compose file that spins up all three cache nodes with the correct ports and environment variables, so that I can run a real multi-node cluster locally with a single command.

#### Acceptance Criteria

1. THE `docker-compose.yml` file SHALL define exactly three services named `node-a`, `node-b`, and `node-c`.

2. THE `node-a` service SHALL bind container port 7001 to host port 7001; `node-b` SHALL bind container port 7002 to host port 7002; `node-c` SHALL bind container port 7003 to host port 7003.

3. EACH service SHALL set the environment variable `NODE_ID` to the respective node ID (`node-a`, `node-b`, or `node-c`).

4. EACH service SHALL build from the project `Dockerfile` located at the repository root.

5. THE `Dockerfile` SHALL use an official Node.js LTS base image, copy only the files needed to run the node (`package.json`, `tsconfig.json`, `src/`), install production dependencies, compile TypeScript, and set the default `CMD` to execute the compiled `CacheNode` entry point.

6. THE three services SHALL be networked together on a shared Docker bridge network so that inter-node TCP forwarding works without host-mode networking.

7. WHEN `docker compose up` is executed from the repository root, THE `Docker_Compose` setup SHALL start all three nodes and each node SHALL begin accepting TCP connections on its configured port within 10 seconds.

---

### Requirement 8: Router Unit Tests

**User Story:** As a developer, I want unit tests for the Router that mock the network layer, so that routing logic is verified without requiring live TCP connections.

#### Acceptance Criteria

1. THE `test/router.test.ts` file SHALL use Vitest and SHALL mock `CacheClient` so that no real TCP connections are opened during the test suite.

2. WHEN a request's key hashes to the local node, THE router test SHALL verify `localStore.set/get/del` is called and no `CacheClient` forwarding occurs.

3. WHEN a request's key hashes to a remote node, THE router test SHALL verify `CacheClient.send` is called with the original request and the response is returned unchanged.

4. WHEN `CacheClient.send` rejects during a forward, THE router test SHALL verify the `Router` returns `{ ok: false, error: <message> }`.

5. WHEN a `SET` request is missing `value`, THE router test SHALL verify the `Router` returns `{ ok: false, error: "SET requires value" }` without calling `localStore.set`.

6. WHEN an unknown command is sent, THE router test SHALL verify the `Router` returns `{ ok: false, error: "unknown command" }`.

7. THE router test SHALL verify that all three node IDs (`node-a`, `node-b`, `node-c`) are registered in the ring by calling `ring.getDistribution()` via the `Router`'s exposed ring (or by testing with keys known to hash to each node).

---

### Requirement 9: Multi-Node Integration Tests

**User Story:** As a developer, I want integration tests that start real CacheNode instances in the same process and exercise cross-node routing over actual TCP, so that the end-to-end request path is verified without Docker.

#### Acceptance Criteria

1. THE `test/integration.test.ts` file SHALL use Vitest and SHALL start all three `CacheNode` instances (`node-a`, `node-b`, `node-c`) using `beforeAll`, connecting them via `CacheClient`, and shut them down in `afterAll`.

2. WHEN a `SET` command is sent to `node-a` for a key that hashes to `node-b`, THE integration test SHALL verify the value is retrievable via a direct `GET` on `node-b`.

3. WHEN a `GET` command is sent to any node for a key stored on a different node, THE integration test SHALL verify the correct value is returned (cross-node forwarding works).

4. WHEN a `DEL` command is sent to any node for a key, THE integration test SHALL verify a subsequent `GET` for that key returns `{ ok: true }` with no `value` field.

5. WHEN a `SET` command includes a `ttl`, THE integration test SHALL verify the value expires after the TTL elapses (using real timers with a short TTL of 1 second and a 1500 ms wait).

6. WHEN 50 distinct keys are set through `node-a`, THE integration test SHALL verify that `getDistribution()` on the cluster ring shows keys distributed across at least 2 of the 3 nodes (cross-node distribution confirmed).

7. WHEN the same key is `SET` via `node-a` and `GET` via `node-c`, THE integration test SHALL return the same value (routing consistency across all entry points).

---

### Requirement 10: Smoke-Test Script

**User Story:** As a developer, I want a runnable demo script that exercises the live Docker cluster end-to-end, so that I can verify the deployment is working correctly after `docker compose up`.

#### Acceptance Criteria

1. THE `scripts/smoke-test.ts` script SHALL be executable via `npx tsx scripts/smoke-test.ts` and SHALL exit with code 0 if all assertions pass or code 1 if any assertion fails.

2. WHEN executed, THE `Smoke_Test` SHALL connect a `CacheClient` to each of the three nodes (`node-a` on port 7001, `node-b` on 7002, `node-c` on 7003).

3. WHEN executed, THE `Smoke_Test` SHALL `SET` at least 9 distinct keys — 3 sent to each node — with values that include the originating node ID.

4. WHEN executed, THE `Smoke_Test` SHALL `GET` each of the 9 keys from a **different** node than it was set on and assert the response value matches.

5. WHEN executed, THE `Smoke_Test` SHALL `DEL` all 9 keys and assert each `DEL` returns `{ ok: true }`.

6. WHEN executed, THE `Smoke_Test` SHALL print a result line for each operation in the format `[PASS] SET key → node-x` or `[FAIL] GET key expected "v" got "w"`.

7. WHEN all operations pass, THE `Smoke_Test` SHALL print a summary `"All smoke tests passed."` and exit with code 0.

8. WHEN any operation fails, THE `Smoke_Test` SHALL print a summary `"Smoke tests FAILED."` and exit with code 1.

9. THE `Smoke_Test` SHALL NOT import any packages other than Node.js built-ins and the project's own `src/` modules.

---

### Requirement 11: npm Script Integration

**User Story:** As a developer, I want the new Phase 2 scripts and test suites wired into `package.json`, so that I can build, test, and run the cluster with familiar npm commands.

#### Acceptance Criteria

1. THE `package.json` `test` script SHALL continue to run all Vitest tests, including the new `test/router.test.ts` and `test/integration.test.ts` files, without any additional flags.

2. THE `package.json` SHALL add a `smoke` script that executes `scripts/smoke-test.ts` via `tsx`.

3. THE `package.json` SHALL add a `node:start` script (or equivalent) that reads `NODE_ID` from the environment and starts a single `CacheNode` via the compiled entry point.

4. THE existing Phase 1 scripts (`build`, `test`, `harness`) SHALL remain unchanged and continue to work correctly after Phase 2 files are added.
