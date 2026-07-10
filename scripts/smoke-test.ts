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

    // ── Phase 4: REPLICATE ──────────────────────────────────────────────────
    console.log("\n── REPLICATE ──");
    const repKeys = [
        { key: "smoke:rep:1", value: "rep-val-1" },
        { key: "smoke:rep:2", value: "rep-val-2" },
        { key: "smoke:rep:3", value: "rep-val-3" },
    ];
    for (const { key, value } of repKeys) {
        const setRes = await clients[0].send({ command: "SET", key, value });
        if (setRes.ok) {
            pass(`SET ${key} for replication`);
        } else {
            fail(
                `SET ${key} for replication`,
                "ok:true",
                `ok:false error:${setRes.error ?? "(none)"}`
            );
        }
    }

    // Wait ~50 ms for replication
    await new Promise((resolve) => setTimeout(resolve, 50));

    for (const { key, value } of repKeys) {
        for (let i = 0; i < 3; i++) {
            const getRes = await clients[i].send({ command: "GET", key });
            if (getRes.ok && getRes.value === value) {
                pass(`GET ${key} from ${NODES[i].nodeId} (replicated)`);
            } else {
                fail(
                    `GET ${key} from ${NODES[i].nodeId} (replicated)`,
                    value,
                    getRes.value ?? "(undefined)"
                );
            }
        }
    }

    // Verify TTL expiry consistency across all nodes
    const ttlKey = "smoke:rep:ttl";
    const ttlVal = "ttl-rep-val";
    const setTtlRes = await clients[0].send({ command: "SET", key: ttlKey, value: ttlVal, ttl: 1 });
    if (setTtlRes.ok) {
        pass(`SET ${ttlKey} with TTL 1s`);
    } else {
        fail(
            `SET ${ttlKey} with TTL 1s`,
            "ok:true",
            `ok:false error:${setTtlRes.error ?? "(none)"}`
        );
    }

    // Wait for TTL to elapse
    await new Promise((resolve) => setTimeout(resolve, 1200));

    for (let i = 0; i < 3; i++) {
        const getRes = await clients[i].send({ command: "GET", key: ttlKey });
        if (getRes.ok && getRes.value === undefined) {
            pass(`GET expired ${ttlKey} from ${NODES[i].nodeId} returns miss`);
        } else {
            fail(
                `GET expired ${ttlKey} from ${NODES[i].nodeId} returns miss`,
                "(undefined)",
                getRes.value ?? "(value present)"
            );
        }
    }

    // Verify synchronous DEL replication across all nodes
    const syncDelKey = "smoke:rep:1";
    const delRes = await clients[0].send({ command: "DEL", key: syncDelKey });
    if (delRes.ok) {
        pass(`DEL ${syncDelKey} synchronously`);
    } else {
        fail(
            `DEL ${syncDelKey} synchronously`,
            "ok:true",
            `ok:false error:${delRes.error ?? "(none)"}`
        );
    }

    for (let i = 0; i < 3; i++) {
        const getRes = await clients[i].send({ command: "GET", key: syncDelKey });
        if (getRes.ok && getRes.value === undefined) {
            pass(`GET deleted ${syncDelKey} from ${NODES[i].nodeId} returns miss`);
        } else {
            fail(
                `GET deleted ${syncDelKey} from ${NODES[i].nodeId} returns miss`,
                "(undefined)",
                getRes.value ?? "(value present)"
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
