import { formatInTimeZone } from "date-fns-tz";

/**
 * CURRENT_TIMESTAMP custom function for BigQuery emulator
 * Returns the current timestamp in UTC
 */
export function currentTimestamp(...args: any[]): string {
  try {
    console.log("CURRENT_TIMESTAMP function called with args:", args);

    // Get current date/time
    const now = new Date();

    // Format as 'YYYY-MM-DD HH:MM:SS.SSSXXX' (BigQuery TIMESTAMP format in UTC)
    const result = formatInTimeZone(
      now,
      "UTC",
      "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
    );

    console.log("CURRENT_TIMESTAMP result:", result);
    return result;
  } catch (error) {
    console.error("CURRENT_TIMESTAMP error:", error);
    throw error;
  }
}
