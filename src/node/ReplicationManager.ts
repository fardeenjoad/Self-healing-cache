/**
 * ReplicationManager manages a queue of keys that need background repair.
 */
export class ReplicationManager {
    private queue: Set<string> = new Set();

    /**
     * Adds a key to the repair queue. Deduplication is handled automatically.
     *
     * @param key - The key to enqueue.
     */
    enqueue(key: string): void {
        this.queue.add(key);
    }

    /**
     * Drains the repair queue, returning all enqueued keys and clearing the queue.
     *
     * @returns An array of all keys that were in the queue.
     */
    drainQueue(): string[] {
        const keys = Array.from(this.queue);
        this.queue.clear();
        return keys;
    }

    /**
     * Returns the number of keys currently in the queue without mutating it.
     *
     * @returns The number of keys in the queue.
     */
    queueSize(): number {
        return this.queue.size;
    }

    /**
     * Returns the internal Set containing the queued keys for inspection.
     *
     * @returns The internal Set representing the queue.
     */
    getQueue(): Set<string> {
        return this.queue;
    }
}
