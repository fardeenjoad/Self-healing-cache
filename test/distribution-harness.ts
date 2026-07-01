// Distribution harness — standalone tsx script
// Compares consistent hashing vs modulo hashing key distribution over 10,000 UUID keys.
// Run with: npm run harness

import { randomUUID } from "node:crypto";
import { ConsistentHashRing } from "../src/core/ring.js";
import { hashString } from "../src/utils/hash.js";

// ─── Setup ────────────────────────────────────────────────────────────────────

const ring = new ConsistentHashRing();
ring.addNode("node-A");
ring.addNode("node-B");
ring.addNode("node-C");

const TOTAL = 10_000;
const keys: string[] = Array.from({ length: TOTAL }, () => randomUUID());

// ─── Helpers ──────────────────────────────────────────────────────────────────

/** Left-aligns text, padding with spaces to `width` chars. */
function padL(text: string, width: number): string {
    return text.padEnd(width, " ");
}

/** Right-aligns text, padding with spaces to `width` chars. */
function padR(text: string, width: number): string {
    return text.padStart(width, " ");
}

// ─── Section 1 — Consistent Hashing: Key Distribution (3 nodes) ──────────────

const counts3: Map<string, number> = new Map([
    ["node-A", 0],
    ["node-B", 0],
    ["node-C", 0],
]);

const assignments3 = new Map<string, string>();
for (const key of keys) {
    const node = ring.getNode(key)!;
    assignments3.set(key, node);
    counts3.set(node, (counts3.get(node) ?? 0) + 1);
}

console.log("=== Consistent Hashing: Key Distribution (3 nodes) ===\n");
console.log(`${padL("Node", 10)}  ${padR("Keys", 6)}  ${padR("Share", 7)}`);
console.log(`${"-".repeat(10)}  ${"-".repeat(6)}  ${"-".repeat(7)}`);
for (const [node, count] of counts3) {
    const share = ((count / TOTAL) * 100).toFixed(1) + "%";
    console.log(`${padL(node, 10)}  ${padR(String(count), 6)}  ${padR(share, 7)}`);
}
console.log(`${"-".repeat(10)}  ${"-".repeat(6)}  ${"-".repeat(7)}`);
console.log(`${padL("Total", 10)}  ${padR(String(TOTAL), 6)}  ${padR("100.0%", 7)}`);

// ─── Section 2 — Consistent Hashing: Remapping (3 → 4 nodes) ─────────────────

ring.addNode("node-D");

let chRemapped = 0;
for (const key of keys) {
    const newNode = ring.getNode(key)!;
    if (newNode !== assignments3.get(key)) chRemapped++;
}

const chPct = ((chRemapped / TOTAL) * 100).toFixed(1);

console.log("\n=== Consistent Hashing: Remapping (3 → 4 nodes) ===\n");
console.log(`  Keys remapped : ${chRemapped} / ${TOTAL} (${chPct}%)`);
console.log(`  Expected      : ~25% (1 of 4 nodes worth of keys)`);

// ─── Section 3 — Modulo Hashing: Remapping (3 → 4 nodes) ─────────────────────

const nodes3 = ["node-A", "node-B", "node-C"];
const nodes4 = ["node-A", "node-B", "node-C", "node-D"];

const moduloAssign3 = new Map<string, string>();
for (const key of keys) {
    const idx = Number(hashString(key) % BigInt(nodes3.length));
    moduloAssign3.set(key, nodes3[idx]);
}

let modRemapped = 0;
for (const key of keys) {
    const idx = Number(hashString(key) % BigInt(nodes4.length));
    const newNode = nodes4[idx];
    if (newNode !== moduloAssign3.get(key)) modRemapped++;
}

const modPct = ((modRemapped / TOTAL) * 100).toFixed(1);

console.log("\n=== Modulo Hashing: Remapping (3 → 4 nodes) ===\n");
console.log(`  Keys remapped : ${modRemapped} / ${TOTAL} (${modPct}%)`);
console.log(`  Expected      : ~75% ((N-1)/N of all keys)`);

// ─── Section 4 — Summary ──────────────────────────────────────────────────────

const ratio = (modRemapped / chRemapped).toFixed(1);

console.log("\n=== Summary ===\n");
console.log(`  Consistent hashing remapped ~${chPct}% of keys when adding a 4th node.`);
console.log(`  Modulo hashing remapped ~${modPct}% of keys for the same operation.`);
console.log(`  Consistent hashing is ~${ratio}x more stable — only keys previously owned`);
console.log(`  by the new node need to move, regardless of total cluster size.`);

process.exit(0);
