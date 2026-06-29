# self-healing-cache (Phase 1)

A distributed cache system implemented in TypeScript (Node.js).

Phase 1 delivers two foundational in-process modules:

- **ConsistentHashRing** — maps string keys to logical node identifiers using consistent hashing with virtual nodes.
- **KVStore** — string key/value store with optional per-entry TTL, lazy expiry, and active background sweep.

## Getting Started

```bash
npm install
npm run build   # compile TypeScript → dist/
npm test        # run all Vitest unit + property tests
npm run harness # run the distribution comparison harness
```

## Project Structure

```
src/
  core/
    ring.ts          # ConsistentHashRing
    kvstore.ts       # KVStore
  utils/
    hash.ts          # hashString (SHA-1 → bigint)
  types/
    index.ts         # RingEntry, KVEntry interfaces
test/
  ring.test.ts       # ConsistentHashRing unit + property tests
  kvstore.test.ts    # KVStore unit + property tests
  distribution-harness.ts  # standalone distribution comparison script
```

## Distribution Harness Results

_(populated after running `npm run harness`)_
