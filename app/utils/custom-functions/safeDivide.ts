/**
 * SAFE_DIVIDE custom function for BigQuery emulator
 * Returns the result of dividing x by y.
 * Returns NULL if division by zero, overflow occurs, or if either input is NULL.
 * Signature: SAFE_DIVIDE(x, y)
 */
export function safeDivide(...args: any[]): number | null {
  try {
    if (args.length !== 2) {
      console.log("SAFE_DIVIDE: Expected 2 arguments");
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

    // Check for division by zero
    if (num2 === 0) {
      return null;
    }

    const result = num1 / num2;

    // Check for overflow (JavaScript number safe integer range)
    if (!Number.isFinite(result)) {
      return null;
    }

    return result;
  } catch (error) {
    console.error("SAFE_DIVIDE error:", error);
    return null;
  }
}
