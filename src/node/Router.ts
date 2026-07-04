import { ConsistentHashRing } from "../core/ring.js";
import { KVStore } from "../core/kvstore.js";
import { CacheClient } from "../client/CacheClient.js";
import type { CacheRequest, CacheResponse, ClusterConfig } from "../types/index.js";

/**
 * Routes incoming cache requests to the correct node in the cluster using a
 * coordinator-less consistent hashing strategy.
 *
 * **Coordinator-less routing pattern:**
 * Every node in the cluster runs an identical `ConsistentHashRing` initialised
 * from the same static `ClusterConfig`. Because the ring is deterministic, any
 * node can receive any request and independently compute which node is the
 * "home" owner for a given key — no central coordinator is needed. If the
 * request arrives at the home node it is executed locally against the node's
 * own `KVStore`. If it arrives at a non-home node, the request is transparently
 * forwarded over a TCP connection to the home node and the response is proxied
 * back to the original caller. The client is completely unaware of which node
 * actually stored or retrieved the value.
 *
 * **Execution paths:**
 * 1. **Local execution** — `ring.getNode(req.key) === localNodeId`: the request
 *    is handled directly by `executeLocally`, which reads from or writes to the
 *    node's own in-process `KVStore`. This path is synchronous and returns
 *    immediately.
 * 2. **Forwarding** — `ring.getNode(req.key) !== localNodeId`: the request is
 *    serialised and sent over TCP to the home node via `forwardToPeer`. The
 *    home node processes the request as a local execution and replies; this
 *    node proxies that reply back to the original caller.
 */
export class Router {
    private readonly localNodeId: string;
    private readonly ring: ConsistentHashRing;
    private readonly localStore: KVStore;
    private readonly peers: Map<string, CacheClient>;
    private readonly config: ClusterConfig;
    private readonly connectedPeers: Set<string> = new Set();

    constructor(localNodeId: string, config: ClusterConfig, localStore: KVStore) {
        this.localNodeId = localNodeId;
        this.config = config;
        this.localStore = localStore;

        // Build the consistent hash ring with all cluster nodes.
        this.ring = new ConsistentHashRing();
        for (const info of config) {
            this.ring.addNode(info.nodeId);
        }

        // Build a CacheClient for every remote peer (not for this node itself).
        // Use 127.0.0.1 when host is "0.0.0.0" — that is a server bind address,
        // not a routable client address.
        this.peers = new Map();
        for (const info of config) {
            if (info.nodeId !== localNodeId) {
                const connectHost = info.host === "0.0.0.0" ? "127.0.0.1" : info.host;
                this.peers.set(info.nodeId, new CacheClient(connectHost, info.port));
            }
        }
    }

    /**
     * Determines the home node for `req.key` and either executes the request
     * locally or forwards it to the correct peer, then returns the response.
     *
     * **Coordinator-less routing pattern:** every node in the cluster runs an
     * identical ring with identical state; any node can receive any request; the
     * ring deterministically maps each key to exactly one "home" node. If the
     * request arrives at a non-home node, it is transparently forwarded over TCP
     * to the home node and the response is proxied back — the client never knows
     * which node actually stored the key.
     *
     * **Two execution paths:**
     * 1. `homeNodeId === localNodeId` → `executeLocally(req)`: handled in-process
     *    against this node's own KVStore.
     * 2. `homeNodeId !== localNodeId` → `forwardToPeer(homeNodeId, req)`: the
     *    request is sent over TCP to the home node and the response is returned.
     *
     * Validation is performed **before** any ring lookup: an unknown command or a
     * SET missing its `value` field returns an error immediately.
     *
     * @param req - The incoming cache request to route.
     * @returns A promise that resolves to a `CacheResponse`.
     */
    async route(req: CacheRequest): Promise<CacheResponse> {
        // 1. Validate before any ring lookup.
        if (req.command !== "GET" && req.command !== "SET" && req.command !== "DEL") {
            return { ok: false, error: "unknown command" };
        }
        if (req.command === "SET" && req.value === undefined) {
            return { ok: false, error: "SET requires value" };
        }

        // 2. Determine the home node.
        const homeNodeId = this.ring.getNode(req.key);
        if (homeNodeId === null) {
            return { ok: false, error: "ring is empty" };
        }

        // 3. Execute locally or forward.
        if (homeNodeId === this.localNodeId) {
            return this.executeLocally(req);
        }
        return this.forwardToPeer(homeNodeId, req);
    }

    private executeLocally(req: CacheRequest): CacheResponse {
        switch (req.command) {
            case "GET": {
                const val = this.localStore.get(req.key);
                return { ok: true, value: val ?? undefined };
            }
            case "SET": {
                this.localStore.set(req.key, req.value!, req.ttl);
                return { ok: true };
            }
            case "DEL": {
                this.localStore.del(req.key);
                return { ok: true };
            }
            default:
                // Should never reach here after validation in route().
                return { ok: false, error: "unknown command" };
        }
    }

    /**
     * Forwards `req` to the peer node identified by `homeNodeId` and returns its
     * response.
     *
     * **Coordinator-less routing:** any node can forward to any other node; the
     * client that issued the original request is completely unaware of this
     * internal hop — from the client's perspective it sent one request and
     * received one response.
     *
     * **Lazy-connect pattern:** `CacheClient` instances are created in the
     * constructor but not connected until their first use. On the first forward
     * to a given peer, `client.connect()` is called and the peer's id is recorded
     * in `connectedPeers`. Subsequent forwards to the same peer reuse the open
     * connection without reconnecting.
     *
     * **5-second timeout:** if the remote node does not respond within 5 000 ms,
     * the underlying socket is destroyed, the peer is removed from
     * `connectedPeers` (so the next call will reconnect), and a descriptive
     * `{ ok: false, error: "..." }` response is returned. This prevents
     * indefinite hangs when a peer is down or slow.
     *
     * **Error handling:** any network error (connection refused, socket reset,
     * timeout) is caught and returned as `{ ok: false, error: message }` without
     * retrying. The peer is evicted from `connectedPeers` so that a later call
     * can attempt a fresh connection.
     *
     * @param homeNodeId - The node ID that owns the key.
     * @param req        - The original cache request to forward.
     * @returns A promise that resolves to the peer's `CacheResponse`, or an
     *          error response if the forward failed.
     */
    private async forwardToPeer(homeNodeId: string, req: CacheRequest): Promise<CacheResponse> {
        const client = this.peers.get(homeNodeId);
        if (!client) {
            return { ok: false, error: `No peer client for node ${homeNodeId}` };
        }

        try {
            // Lazy connect: connect on first use, reuse on subsequent calls.
            if (!this.connectedPeers.has(homeNodeId)) {
                await client.connect();
                this.connectedPeers.add(homeNodeId);
            }

            // 5-second timeout race.
            const TIMEOUT_MS = 5000;
            const timeoutPromise = new Promise<CacheResponse>((_, reject) =>
                setTimeout(() => {
                    // Destroy the socket so the pending send() rejects and
                    // the connection is reset for the next attempt.
                    (client as unknown as { socket?: { destroy(): void } }).socket?.destroy();
                    this.connectedPeers.delete(homeNodeId);
                    reject(new Error(`Forward to ${homeNodeId} timed out after ${TIMEOUT_MS}ms`));
                }, TIMEOUT_MS)
            );

            const response = await Promise.race([client.send(req), timeoutPromise]);
            return response;

        } catch (err: unknown) {
            // On any error (timeout, network failure), evict from connected set
            // so the next call will attempt a fresh connection.
            this.connectedPeers.delete(homeNodeId);
            return {
                ok: false,
                error: err instanceof Error ? err.message : String(err),
            };
        }
    }
}
