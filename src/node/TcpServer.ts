import * as net from "node:net";
import type { CacheRequest, CacheResponse } from "../types/index.js";

/**
 * TCP server with NDJSON framing.
 *
 * Maintains a per-connection string buffer and scans for '\n' delimiters to
 * extract complete frames. Each parsed CacheRequest is dispatched to the
 * requestHandler, and the resulting CacheResponse is written back on the same
 * connection with the correlation id (req.id) echoed verbatim so that
 * CacheClient can route the response to the correct pending promise.
 *
 * Backpressure: if socket.write() returns false, reads are paused until the
 * 'drain' event fires.
 */
export class TcpServer {
    private readonly port: number;
    private readonly requestHandler: (req: CacheRequest) => Promise<CacheResponse>;
    private server: net.Server | null = null;

    constructor(
        port: number,
        requestHandler: (req: CacheRequest) => Promise<CacheResponse>
    ) {
        this.port = port;
        this.requestHandler = requestHandler;
    }

    /** Bind to 0.0.0.0:port and begin accepting connections. */
    listen(): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            this.server = net.createServer((socket) => {
                this._handleConnection(socket);
            });

            this.server.on("error", reject);

            this.server.listen(this.port, "0.0.0.0", () => {
                resolve();
            });
        });
    }

    /** Stop accepting new connections and close the underlying server. */
    close(): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            if (!this.server) {
                resolve();
                return;
            }
            this.server.close((err) => {
                if (err) {
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    }

    // ─── Private helpers ────────────────────────────────────────────────────────

    private _handleConnection(socket: net.Socket): void {
        let buffer = "";

        socket.on("data", (chunk: Buffer) => {
            buffer += chunk.toString();

            // Scan for complete NDJSON frames (terminated by '\n').
            let nl: number;
            while ((nl = buffer.indexOf("\n")) !== -1) {
                const frame = buffer.slice(0, nl);
                buffer = buffer.slice(nl + 1);

                // Skip empty frames (e.g. bare '\n').
                if (frame.trim() === "") continue;

                // Attempt to parse the frame as a CacheRequest.
                let req: CacheRequest;
                try {
                    req = JSON.parse(frame) as CacheRequest;
                } catch {
                    // Parse failed — no correlation id to echo; return error.
                    const errResp: CacheResponse = { ok: false, error: "invalid JSON" };
                    socket.write(JSON.stringify(errResp) + "\n");
                    continue;
                }

                // Dispatch to handler and echo req.id into every response path.
                this.requestHandler(req)
                    .then((resp) => {
                        const withId: CacheResponse =
                            req.id !== undefined ? { ...resp, id: req.id } : resp;

                        const wrote = socket.write(JSON.stringify(withId) + "\n");
                        if (!wrote) {
                            // Apply backpressure: pause reads until the socket drains.
                            socket.pause();
                            socket.once("drain", () => socket.resume());
                        }
                    })
                    .catch((err: unknown) => {
                        const errResp: CacheResponse = {
                            ok: false,
                            error: err instanceof Error ? err.message : String(err),
                            ...(req.id !== undefined ? { id: req.id } : {}),
                        };
                        socket.write(JSON.stringify(errResp) + "\n");
                    });
            }
        });

        socket.on("close", () => {
            // Release the per-connection buffer.
            buffer = "";
        });

        socket.on("error", () => {
            // Errors are followed by a 'close' event which cleans up above.
        });
    }
}
