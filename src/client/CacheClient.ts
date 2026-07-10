import * as net from "node:net";
import { randomUUID } from "node:crypto";
import type { CacheRequest, CacheResponse } from "../types/index.js";

interface PendingEntry {
    resolve: (r: CacheResponse) => void;
    reject: (e: Error) => void;
}

/**
 * Thin TCP client for communicating with a single CacheNode over NDJSON.
 *
 * Uses a concurrent pending-requests Map keyed by UUID correlation ID.
 * Multiple requests can be in-flight simultaneously — each outgoing frame
 * is tagged with a unique `id`, and the server echoes that `id` back in
 * the response so the correct promise can be resolved.
 *
 * Usage:
 *   const client = new CacheClient("127.0.0.1", 7001);
 *   await client.connect();
 *   const res = await client.send({ command: "SET", key: "foo", value: "bar" });
 *   await client.disconnect();
 *
 * Guarantees:
 *  - Fully concurrent: N requests may be in-flight at once.
 *  - send() before connect() rejects with "Not connected".
 *  - Unexpected socket close rejects all pending promises.
 */
export class CacheClient {
    private readonly host: string;
    private readonly port: number;

    private socket: net.Socket | null = null;
    private buffer = "";

    /** Map from correlation id → pending promise callbacks. */
    private pending: Map<string, PendingEntry> = new Map();
    private connectPromise: Promise<void> | null = null;

    constructor(host: string, port: number) {
        this.host = host;
        this.port = port;
    }

    /**
     * Opens a TCP connection to the configured host:port.
     * Resolves when the connection is fully established.
     * Rejects on any connection error.
     */
    connect(): Promise<void> {
        if (this.socket !== null) {
            return Promise.resolve();
        }
        if (this.connectPromise !== null) {
            return this.connectPromise;
        }

        this.connectPromise = new Promise<void>((resolve, reject) => {
            const socket = new net.Socket();

            socket.on("data", (chunk: Buffer) => {
                this.buffer += chunk.toString();
                this._drainBuffer();
            });

            socket.on("close", () => {
                // Reject all pending promises — connection is gone.
                const err = new Error("Socket closed unexpectedly while requests were pending");
                for (const entry of this.pending.values()) {
                    entry.reject(err);
                }
                this.pending.clear();
                this.socket = null;
                this.connectPromise = null;
            });

            socket.on("error", (err: Error) => {
                reject(err);
            });

            socket.connect(this.port, this.host, () => {
                // Swap in a post-connect error handler (errors → 'close' cleanup).
                socket.removeAllListeners("error");
                socket.on("error", (_err: Error) => {
                    // 'close' will follow and clean up pending promises.
                });
                this.socket = socket;
                resolve();
            });
        }).finally(() => {
            this.connectPromise = null;
        });

        return this.connectPromise;
    }

    /**
     * Tags `req` with a UUID correlation id, serialises it as NDJSON, and
     * writes it to the socket. Resolves when a response frame with the same
     * id arrives. Multiple calls may be in-flight concurrently.
     *
     * @throws {Error} "Not connected" if called before connect().
     * @throws {Error} If the socket closes while this call is pending.
     */
    send(req: CacheRequest): Promise<CacheResponse> {
        if (this.socket === null) {
            return Promise.reject(new Error("Not connected"));
        }

        const id = randomUUID();
        const taggedReq: CacheRequest = { ...req, id };

        return new Promise<CacheResponse>((resolve, reject) => {
            this.pending.set(id, { resolve, reject });
            this.socket!.write(JSON.stringify(taggedReq) + "\n");
        });
    }

    /**
     * Sends a TCP FIN and resolves when the connection is fully closed.
     * Any still-pending send() calls will be rejected via the 'close' handler.
     */
    disconnect(): Promise<void> {
        return new Promise<void>((resolve) => {
            if (this.socket === null) {
                resolve();
                return;
            }
            this.socket.once("close", () => resolve());
            this.socket.end();
        });
    }

    // ─── Private helpers ────────────────────────────────────────────────────────

    /**
     * Scans the receive buffer for complete NDJSON frames (terminated by `\n`).
     * Each parsed frame's `id` field is looked up in the pending map to resolve
     * the correct promise. Frames with unknown or missing ids are discarded.
     */
    private _drainBuffer(): void {
        let newlineIndex: number;

        while ((newlineIndex = this.buffer.indexOf("\n")) !== -1) {
            const frame = this.buffer.slice(0, newlineIndex);
            this.buffer = this.buffer.slice(newlineIndex + 1);

            if (frame.length === 0) continue;

            let parsed: CacheResponse;
            try {
                parsed = JSON.parse(frame) as CacheResponse;
            } catch {
                // Malformed frame — no id to look up, discard.
                continue;
            }

            const id = parsed.id;
            if (id === undefined) {
                // Server sent a frame without an id — discard.
                continue;
            }

            const entry = this.pending.get(id);
            if (entry === undefined) {
                // Unknown correlation id — discard.
                continue;
            }

            this.pending.delete(id);
            entry.resolve(parsed);
        }
    }
}
