/**
 * Smoke-test script for the self-healing-cache Phase 2 cluster.
 *
 * Run against a live Docker cluster:
 *   docker compose up -d
 *   npx tsx scripts/smoke-test.ts
 *
 * Exits 0 if all assertions pass, 1 if any fail.
 * Uses only Node.js built-ins and project src/ modules — no third-party deps.
 */
import { CacheClient } from "../src/client/CacheClient.js";

const NODES = [
    { nodeId: "node-a", host: "127.0.0.1", port: 7001 },
    { nodeId: "node-b", host: "127.0.0.1", port: 7002 },
    { nodeId: "node-c", host: "127.0.0.1", port: 7003 },
];

// 9 keys: 3 per node. Value encodes the originating node ID.
// getNode index is always different from setNode index for cross-node verification.
const KEYS = [
    { key: "smoke:a:1", value: "from-node-a-1", setNode: 0, getNode: 1 },
    { key: "smoke:a:2", value: "from-node-a-2", setNode: 0, getNode: 2 },
    { key: "smoke:a:3", value: "from-node-a-3", setNode: 0, getNode: 1 },
    { key: "smoke:b:1", value: "from-node-b-1", setNode: 1, getNode: 2 },
    { key: "smoke:b:2", value: "from-node-b-2", setNode: 1, getNode: 0 },
    { key: "smoke:b:3", value: "from-node-b-3", setNode: 1, getNode: 2 },
    { key: "smoke:c:1", value: "from-node-c-1", setNode: 2, getNode: 0 },
    { key: "smoke:c:2", value: "from-node-c-2", setNode: 2, getNode: 1 },
    { key: "smoke:c:3", value: "from-node-c-3", setNode: 2, getNode: 0 },
];

async function main(): Promise<void> {
    let passed = 0;
    let failed = 0;

    const pass = (label: string): void => {
        console.log(`[PASS] ${label}`);
        passed++;
    };

    const fail = (label: string, expected: string, got: string): void => {
        console.log(`[FAIL] ${label} expected "${expected}" got "${got}"`);
        failed++;
    };

    // Connect all three clients
    const clients = NODES.map((n) => new CacheClient(n.host, n.port));
    for (let i = 0; i < clients.length; i++) {
        await clients[i].connect();
        console.log(`[INFO] Connected to ${NODES[i].nodeId} on port ${NODES[i].port}`);
    }

    // ── Phase 1: SET all 9 keys ─────────────────────────────────────────────
    console.log("\n── SET ──");
    for (const { key, value, setNode } of KEYS) {
        const res = await clients[setNode].send({ command: "SET", key, value });
        if (res.ok) {
            pass(`SET ${key} → ${NODES[setNode].nodeId}`);
        } else {
            fail(
                `SET ${key} → ${NODES[setNode].nodeId}`,
                "ok:true",
                `ok:false error:${res.error ?? "(none)"}`
            );
        }
    }

    // ── Phase 2: GET all 9 keys from a DIFFERENT node ───────────────────────
    console.log("\n── GET (cross-node) ──");
    for (const { key, value, getNode } of KEYS) {
        const res = await clients[getNode].send({ command: "GET", key });
        if (res.ok && res.value === value) {
            pass(`GET ${key} from ${NODES[getNode].nodeId}`);
        } else {
            fail(
                `GET ${key} from ${NODES[getNode].nodeId}`,
                value,
                res.value ?? "(undefined)"
            );
        }
    }

    // ── Phase 3: DEL all 9 keys (from a third node where possible) ──────────
    console.log("\n── DEL ──");
    for (const { key, setNode } of KEYS) {
        const delNode = (setNode + 1) % 3; // always a different node than setNode
        const res = await clients[delNode].send({ command: "DEL", key });
        if (res.ok) {
            pass(`DEL ${key} via ${NODES[delNode].nodeId}`);
        } else {
            fail(
                `DEL ${key} via ${NODES[delNode].nodeId}`,
                "ok:true",
                `ok:false error:${res.error ?? "(none)"}`
            );
        }
    }

    // Disconnect all clients cleanly
    for (const client of clients) {
        await client.disconnect();
    }

    // ── Summary ──────────────────────────────────────────────────────────────
    console.log("");
    if (failed === 0) {
        console.log(`All smoke tests passed. (${passed}/${passed + failed})`);
        process.exit(0);
    } else {
        console.log(
            `Smoke tests FAILED. (${passed} passed, ${failed} failed)`
        );
        process.exit(1);
    }
}

main().catch((err: unknown) => {
    console.error(
        "[FATAL]",
        err instanceof Error ? err.message : String(err)
    );
    process.exit(1);
});
