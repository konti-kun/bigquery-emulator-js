/**
 * SAFE_NEGATE custom function for BigQuery emulator
 * Returns the negation of a number.
 * Returns NULL if overflow occurs or if input is NULL.
 * Signature: SAFE_NEGATE(x)
 */
export function safeNegate(...args: any[]): number | null {
  try {
    if (args.length !== 1) {
      console.log("SAFE_NEGATE: Expected 1 argument");
      return null;
    }

    const [x] = args;

    // Handle NULL values
    if (x === null || x === undefined) {
      return null;
    }

    const num = Number(x);

    // Check if conversion failed
    if (isNaN(num)) {
      return null;
    }

    const result = -num;

    // Check for overflow (JavaScript number safe integer range)
    if (!Number.isFinite(result)) {
      return null;
    }

    // Check for integer overflow if input is an integer
    if (Number.isInteger(num)) {
      // Check if result exceeds safe integer range
      const MAX_SAFE_INT64 = 9223372036854775807n;
      const MIN_SAFE_INT64 = -9223372036854775808n;

      try {
        const bigInt = BigInt(Math.floor(num));
        const bigIntResult = -bigInt;

        if (bigIntResult > MAX_SAFE_INT64 || bigIntResult < MIN_SAFE_INT64) {
          return null;
        }
      } catch {
        return null;
      }
    }

    return result;
  } catch (error) {
    console.error("SAFE_NEGATE error:", error);
    return null;
  }
}
