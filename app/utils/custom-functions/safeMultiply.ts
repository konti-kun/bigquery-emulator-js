/**
 * SAFE_MULTIPLY custom function for BigQuery emulator
 * Returns the result of multiplying two numbers.
 * Returns NULL if overflow occurs or if either input is NULL.
 * Signature: SAFE_MULTIPLY(x, y)
 */
export function safeMultiply(...args: any[]): number | null {
  try {
    if (args.length !== 2) {
      console.log("SAFE_MULTIPLY: Expected 2 arguments");
      return null;
    }

    const [x, y] = args;

    // Handle NULL values
    if (x === null || x === undefined || y === null || y === undefined) {
      return null;
    }

    const num1 = Number(x);
    const num2 = Number(y);

    // Check if conversion failed
    if (isNaN(num1) || isNaN(num2)) {
      return null;
    }

    const result = num1 * num2;

    // Check for overflow (JavaScript number safe integer range)
    if (!Number.isFinite(result)) {
      return null;
    }

    // Check for integer overflow if both inputs are integers
    const isInt1 = Number.isInteger(num1);
    const isInt2 = Number.isInteger(num2);

    if (isInt1 && isInt2) {
      // Check if result exceeds safe integer range
      const MAX_SAFE_INT64 = 9223372036854775807n;
      const MIN_SAFE_INT64 = -9223372036854775808n;

      try {
        const bigInt1 = BigInt(Math.floor(num1));
        const bigInt2 = BigInt(Math.floor(num2));
        const bigIntResult = bigInt1 * bigInt2;

        if (bigIntResult > MAX_SAFE_INT64 || bigIntResult < MIN_SAFE_INT64) {
          return null;
        }
      } catch {
        return null;
      }
    }

    return result;
  } catch (error) {
    console.error("SAFE_MULTIPLY error:", error);
    return null;
  }
}
