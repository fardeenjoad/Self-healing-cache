import { KVStore } from "../core/kvstore.js";
import { Router } from "./Router.js";
import { TcpServer } from "./TcpServer.js";
import { CLUSTER_CONFIG, getNodeInfo } from "../config/cluster.js";
import type { ClusterConfig } from "../types/index.js";

/**
 * Composes KVStore, Router, and TcpServer into a single startable/stoppable
 * cache node unit.
 *
 * Each CacheNode represents one member of the cluster. It owns:
 * - A KVStore for local key-value storage (with background TTL sweep)
 * - A Router that uses a ConsistentHashRing to route requests locally or forward
 *   them to the correct peer node
 * - A TcpServer that accepts NDJSON-framed requests on the configured port and
 *   delegates them to the Router
 *
 * Usage:
 * ```typescript
 * const node = new CacheNode("node-a", CLUSTER_CONFIG);
 * await node.start();
 * // ... later ...
 * await node.stop();
 * ```
 */
export class CacheNode {
    private readonly nodeId: string;
    private readonly port: number;
    private readonly kvStore: KVStore;
    private readonly router: Router;
    private readonly tcpServer: TcpServer;

    constructor(nodeId: string, config: ClusterConfig) {
        // Look up this node's port from the provided config.
        const nodeInfo = config.find((n) => n.nodeId === nodeId);
        if (!nodeInfo) {
            throw new Error(`Unknown nodeId: ${nodeId}`);
        }

        this.nodeId = nodeId;
        this.port = nodeInfo.port;

        // Wire together the three internal components.
        this.kvStore = new KVStore();
        this.router = new Router(nodeId, config, this.kvStore);
        this.tcpServer = new TcpServer(this.port, (req) => this.router.route(req));
    }

    /**
     * Starts the TCP server and begins accepting connections.
     *
     * Rejects if the port is already in use (EADDRINUSE) — the error propagates
     * to the caller; it is NOT swallowed.
     */
    async start(): Promise<void> {
        await this.tcpServer.listen();
        console.log(`[CacheNode] ${this.nodeId} listening on port ${this.port}`);
    }

    /**
     * Stops the TCP server and clears the KVStore background sweep timer,
     * releasing all held resources.
     */
    async stop(): Promise<void> {
        await this.router.stop();
        await this.tcpServer.close();
        this.kvStore.stopSweeper();
        console.log(`[CacheNode] ${this.nodeId} stopped`);
    }
}

/**
 * Process entry point. Reads NODE_ID from the environment, constructs a
 * CacheNode with the static CLUSTER_CONFIG, starts it, and registers
 * SIGINT/SIGTERM handlers for graceful shutdown.
 *
 * Docker sends SIGTERM when stopping a container (docker stop / docker compose down).
 * SIGINT handles Ctrl+C in local development.
 */
export async function main(): Promise<void> {
    const nodeId = process.env.NODE_ID ?? "node-a";
    const node = new CacheNode(nodeId, CLUSTER_CONFIG);

    // Graceful shutdown handlers — critical for Docker container lifecycle.
    const shutdown = async (signal: string): Promise<void> => {
        console.log(`[CacheNode] Received ${signal}, shutting down...`);
        await node.stop();
        process.exit(0);
    };

    process.on("SIGTERM", () => void shutdown("SIGTERM"));
    process.on("SIGINT", () => void shutdown("SIGINT"));

    try {
        await node.start();
    } catch (err: unknown) {
        console.error(
            `[CacheNode] Failed to start node "${nodeId}":`,
            err instanceof Error ? err.message : String(err)
        );
        process.exit(1);
    }
}

// ESM: import.meta.url is the file:// URL of this module.
// process.argv[1] is the path of the script Node.js was launched with.
// When they match, this file is the entry point.
import { fileURLToPath } from "node:url";
const __filename = fileURLToPath(import.meta.url);
if (process.argv[1] === __filename) {
    void main();
}
