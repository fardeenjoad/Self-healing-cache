import type { KVEntry } from "../types/index.js";

/**
 * An in-memory key-value store with optional per-entry TTL expiry.
 *
 * Expired entries are evicted in two ways:
 * - **Lazy expiry**: removed on `get` when the entry is found to be past its TTL.
 * - **Active sweep**: a background `setInterval` fires every 1 000 ms and deletes
 *   all entries whose TTL has elapsed, reclaiming memory even for keys that are
 *   never read again.
 *
 * Call `stopSweeper()` when the store is no longer needed to clear the interval
 * and allow the process to exit cleanly.
 */
export class KVStore {
    private store: Map<string, KVEntry> = new Map();
    private sweepTimer: ReturnType<typeof setInterval>;

    constructor() {
        this.sweepTimer = setInterval(this.sweep.bind(this), 1000);
    }

    /**
     * Stores a key-value pair with an optional time-to-live.
     *
     * If the key already exists its value and expiry are overwritten.
     * When `ttlSeconds` is omitted, `0`, or negative the entry never expires.
     *
     * @param key - The string key under which the value is stored.
     * @param value - The string value to store.
     * @param ttlSeconds - Optional TTL in seconds. Values ≤ 0 are treated as no TTL.
     * @returns void
     */
    set(key: string, value: string, ttlSeconds?: number): void {
        const expiresAt =
            ttlSeconds === undefined || ttlSeconds <= 0
                ? null
                : Date.now() + ttlSeconds * 1000;
        this.store.set(key, { value, expiresAt });
    }

    /**
     * Stores a key-value pair verbatim with a specific absolute expiration timestamp.
     * Overwrites the value and expiresAt if the key already exists.
     *
     * @param key - The string key under which the value is stored.
     * @param value - The string value to store.
     * @param expiresAt - The absolute Unix timestamp (ms) at which the entry expires, or null if it never expires.
     */
    setRaw(key: string, value: string, expiresAt: number | null): void {
        this.store.set(key, { value, expiresAt });
    }

    /**
     * Retrieves the absolute expiration timestamp for a key.
     * Does not perform expiry checks or lazy deletion.
     *
     * @param key - The string key to inspect.
     * @returns The stored expiresAt timestamp (number or null), or undefined if the key is not present.
     */
    getExpiresAt(key: string): number | null | undefined {
        const entry = this.store.get(key);
        if (entry === undefined) return undefined;
        return entry.expiresAt;
    }

    /**
     * Retrieves the value associated with `key`, or `null` if the key does not
     * exist or has expired.
     *
     * If the entry is found to be expired at call time it is lazily deleted from
     * the backing store before `null` is returned.
     *
     * @param key - The string key to look up.
     * @returns The stored string value, or `null` if absent or expired.
     */
    get(key: string): string | null {
        const entry = this.store.get(key);
        if (entry === undefined) return null;
        if (entry.expiresAt !== null && entry.expiresAt <= Date.now()) {
            this.store.delete(key);
            return null;
        }
        return entry.value;
    }

    /**
     * Deletes the entry for `key` from the store.
     *
     * @param key - The string key to delete.
     * @returns `true` if the key existed and was deleted; `false` if it was not present.
     */
    del(key: string): boolean {
        return this.store.delete(key);
    }

    /**
     * Returns the raw number of entries currently held in the backing store.
     *
     * This count includes entries that have expired but have not yet been
     * removed by a sweep or a lazy `get`.
     *
     * @returns The number of entries in the store.
     */
    size(): number {
        return this.store.size;
    }

    /**
     * Clears the background sweep interval, preventing any further sweep
     * executions.
     *
     * After this call, expired entries will only be removed lazily via `get`.
     * Safe to call multiple times — `clearInterval` on an already-cleared timer
     * is a no-op in Node.js.
     *
     * @returns void
     */
    stopSweeper(): void {
        clearInterval(this.sweepTimer);
    }

    /**
     * Background sweep: iterates all entries and deletes those that have passed
     * their expiry timestamp.
     */
    private sweep(): void {
        const now = Date.now();
        for (const [key, entry] of this.store) {
            if (entry.expiresAt !== null && entry.expiresAt <= now) {
                this.store.delete(key);
            }
        }
    }
}
