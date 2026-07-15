import { ConsistentHashRing } from "../core/ring.js";
import { KVStore } from "../core/kvstore.js";
import { CacheClient } from "../client/CacheClient.js";
import { ReplicationManager } from "./ReplicationManager.js";
import type { CacheRequest, CacheResponse, ClusterConfig, NodeInfo } from "../types/index.js";
import { GossipManager } from "./GossipManager.js";

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
    private readonly repairQueue: ReplicationManager = new ReplicationManager();
    private readonly gossipManager?: GossipManager;

    constructor(
        localNodeId: string,
        config: ClusterConfig,
        localStore: KVStore,
        gossipManager?: GossipManager
    ) {
        this.localNodeId = localNodeId;
        this.config = config;
        this.localStore = localStore;
        this.gossipManager = gossipManager;

        // Build the consistent hash ring with all cluster nodes.
        this.ring = new ConsistentHashRing();
        for (const info of config) {
            this.ring.addNode(info.nodeId);
        }

        // Build a CacheClient for every remote peer (not for this node itself).
        //
        // Host resolution priority (for connecting to peers, not binding):
        //   1. PEER_<NODEID>_HOST env var — set by docker-compose for Docker
        //      inter-node routing (e.g. PEER_NODE_B_HOST=node-b).
        //   2. NodeInfo.host if it is a routable address (not the bind wildcard).
        //   3. "127.0.0.1" — fallback for local / test environments.
        //
        // The env var name is derived from nodeId: "node-b" → "PEER_NODE_B_HOST"
        // (upper-cased, hyphens replaced with underscores, wrapped in PEER_…_HOST).
        this.peers = new Map();
        for (const info of config) {
            if (info.nodeId !== localNodeId) {
                const envKey = `PEER_${info.nodeId.toUpperCase().replace(/-/g, "_")}_HOST`;
                const connectHost =
                    process.env[envKey] ??
                    (info.host !== "0.0.0.0" ? info.host : "127.0.0.1");
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
        const validCommands = ["GET", "SET", "DEL", "REPLICATE", "REPLICATE_DEL", "MEMBERSHIP_QUERY"];
        if (!validCommands.includes(req.command)) {
            return { ok: false, error: "unknown command" };
        }
        if (req.command === "SET" && req.value === undefined) {
            return { ok: false, error: "SET requires value" };
        }
        if (req.command === "REPLICATE" && req.value === undefined) {
            return { ok: false, error: "REPLICATE requires value" };
        }

        // REPLICATE, REPLICATE_DEL, and MEMBERSHIP_QUERY bypass ring routing entirely — always local
        // GET with isFallback: true also bypasses ring routing to retrieve the value from replica's local store
        if (
            req.command === "REPLICATE" ||
            req.command === "REPLICATE_DEL" ||
            req.command === "MEMBERSHIP_QUERY" ||
            req.isFallback
        ) {
            return this.executeLocally(req);
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

    private async executeLocally(req: CacheRequest): Promise<CacheResponse> {
        switch (req.command) {
            case "GET": {
                const val = this.localStore.get(req.key);
                if (val !== null) {
                    return { ok: true, value: val };
                }
                // Replica fallback — only if this is NOT already a fallback request
                if (!req.isFallback) {
                    const replicas = this.getReplicaNodes(req.key);
                    for (const replica of replicas) {
                        const resp = await this.forwardToPeer(replica.nodeId, { ...req, isFallback: true });
                        if (resp.ok && resp.value !== undefined) {
                            this.repairQueue.enqueue(req.key);
                            return { ok: true, value: resp.value };
                        }
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
                this.localStore.setRaw(req.key, req.value!, req.expiresAt ?? null);
                return { ok: true };
            }
            case "REPLICATE_DEL": {
                this.localStore.del(req.key);
                return { ok: true };
            }
            case "MEMBERSHIP_QUERY": {
                const membersRecord: Record<string, string> = {};
                if (this.gossipManager) {
                    for (const [nodeId, info] of this.gossipManager.getAllMembers().entries()) {
                        membersRecord[nodeId] = info.state;
                    }
                } else {
                    for (const node of this.config) {
                        membersRecord[node.nodeId] = "ALIVE";
                    }
                }
                return {
                    ok: true,
                    status: "OK",
                    members: membersRecord,
                };
            }
            default:
                // Should never reach here after validation in route().
                return { ok: false, error: "unknown command" };
        }
    }

    /**
     * Returns true if the node is reachable (ALIVE or SUSPECT — not DEAD).
     */
    public isNodeReachable(nodeId: string): boolean {
        if (nodeId === this.localNodeId) {
            return true;
        }
        if (!this.gossipManager) {
            return true;
        }
        return this.gossipManager.isAlive(nodeId);
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
        if (!this.isNodeReachable(homeNodeId)) {
            // If the target is DEAD, and it's a fallback request or replication command, fail immediately
            if (req.isFallback || req.command === "REPLICATE" || req.command === "REPLICATE_DEL") {
                return { ok: false, error: `Node ${homeNodeId} is DEAD` };
            }

            // Otherwise, immediately try next replica clockwise on the ring
            const allNodes = this.ring.getNodes(req.key, 3);
            const replicas = allNodes.slice(1);

            const reachableReplicas = replicas.filter(
                (nodeId) => nodeId === this.localNodeId || this.isNodeReachable(nodeId)
            );

            if (reachableReplicas.length === 0) {
                return {
                    ok: false,
                    error: "all replicas unavailable",
                    status: "ERROR",
                    message: "all replicas unavailable",
                };
            }

            for (const replicaId of reachableReplicas) {
                let resp: CacheResponse;
                if (replicaId === this.localNodeId) {
                    resp = await this.executeLocally({ ...req, isFallback: true });
                } else {
                    resp = await this.forwardToPeer(replicaId, { ...req, isFallback: true });
                }

                if (resp.ok) {
                    if (req.command !== "GET" || resp.value !== undefined) {
                        return resp;
                    }
                }
            }

            if (req.command === "GET") {
                return { ok: true };
            }
            return {
                ok: false,
                error: "all replicas unavailable",
                status: "ERROR",
                message: "all replicas unavailable",
            };
        }

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
            let timeoutId: ReturnType<typeof setTimeout> | undefined;
            const timeoutPromise = new Promise<CacheResponse>((_, reject) => {
                timeoutId = setTimeout(() => {
                    // Destroy the socket so the pending send() rejects and
                    // the connection is reset for the next attempt.
                    (client as unknown as { socket?: { destroy(): void } }).socket?.destroy();
                    this.connectedPeers.delete(homeNodeId);
                    reject(new Error(`Forward to ${homeNodeId} timed out after ${TIMEOUT_MS}ms`));
                }, TIMEOUT_MS);
            });

            const response = await Promise.race([client.send(req), timeoutPromise]);
            if (timeoutId) {
                clearTimeout(timeoutId);
            }
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
            .filter(Boolean);
    }

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
        if (!this.isNodeReachable(targetNodeId)) {
            console.warn(`[Replication] WARNING: skipping replication of key ${key} to dead node ${targetNodeId}`);
            return;
        }
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
        }
    }

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
        if (!this.isNodeReachable(targetNodeId)) {
            console.warn(`[Replication] WARNING: skipping replicate delete of key ${key} to dead node ${targetNodeId}`);
            return;
        }
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

    /**
     * Returns the current repair queue set (for testing and Phase 5 processing).
     *
     * @returns The internal Set<string> of keys pending read-repair.
     */
    public getRepairQueue(): Set<string> {
        return this.repairQueue.getQueue();
    }

    /**
     * Closes and disconnects all peer clients.
     */
    public async stop(): Promise<void> {
        for (const client of this.peers.values()) {
            await client.disconnect();
        }
        this.connectedPeers.clear();
    }
}
