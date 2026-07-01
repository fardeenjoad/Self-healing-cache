// SHA-1 hash utility for self-healing-cache (Phase 1)
// Provides a deterministic string → bigint mapping used by the consistent hash ring.

import { createHash } from "node:crypto";

/**
 * Computes the SHA-1 digest of the given input string and returns it as a
 * `bigint` in the range [0, 2^160 − 1].
 *
 * The 40-character hexadecimal digest produced by SHA-1 is converted using
 * `BigInt("0x" + hexDigest)`, preserving the full precision of the 160-bit
 * hash value.
 *
 * @param input - The string to hash.
 * @returns A `bigint` representation of the SHA-1 digest.
 */
export function hashString(input: string): bigint {
    const hexDigest = createHash("sha1").update(input).digest("hex");
    return BigInt("0x" + hexDigest);
}
