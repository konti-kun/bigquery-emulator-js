import { formatInTimeZone } from "date-fns-tz";

/**
 * CURRENT_DATE custom function for BigQuery emulator
 * Returns the current date as a string in YYYY-MM-DD format
 */
export function currentDate(...args: any[]): string {
  try {
    console.log("CURRENT_DATE function called with args:", args);

    // Get current date
    const now = new Date();

    const tz = args.length > 0 ? String(args[0]) : "UTC";

    // Format as 'YYYY-MM-DD' and return as simple string
    const result = formatInTimeZone(now, tz, "yyyy-MM-dd");

    console.log("CURRENT_DATE result:", result);
    return result;
  } catch (error) {
    console.error("CURRENT_DATE error:", error);
    throw error;
  }
}
