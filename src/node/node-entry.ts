/**
 * Docker / process entry point for a single cache node.
 *
 * This file exists solely to be the unconditional entry point — it calls
 * main() without any ESM guard, so it works reliably in all environments
 * including Docker, compiled JS, and tsx.
 *
 * Usage:
 *   NODE_ID=node-a node dist/src/node/node-entry.js
 */
import { main } from "./CacheNode.js";

void main();
