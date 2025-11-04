import { format, parseISO } from "date-fns";
import { fromZonedTime } from "date-fns-tz";

/**
 * TIMESTAMP custom function for BigQuery emulator
 * Converts a string or datetime value to a timestamp
 */
export function timestamp(...args: any[]): string | null {
  try {
    console.log("TIMESTAMP function called with args:", args);
    const [dateString, timezone] = args;

    if (!dateString) {
      console.log("TIMESTAMP: No date string provided");
      return null;
    }

    const inputStr = String(dateString);

    // Parse the input string to Date object
    let date: Date;

    if (timezone) {
      // When timezone is specified, interpret the input as being in that timezone
      // and convert to UTC
      const timezoneStr = String(timezone);

      // Normalize the input string format
      let normalizedStr = inputStr;
      if (!normalizedStr.includes("T")) {
        // Add time component if only date is provided
        if (normalizedStr.length === 10) {
          normalizedStr += " 00:00:00";
        }
        // Replace space with T for ISO format
        normalizedStr = normalizedStr.replace(" ", "T");
      }

      // Remove 'Z' suffix if present as we're treating it as a local time in the given timezone
      normalizedStr = normalizedStr.replace("Z", "");

      // Parse as local time in the given timezone and convert to UTC
      const localDate = parseISO(normalizedStr);
      date = fromZonedTime(localDate, timezoneStr);
    } else {
      // When no timezone is specified, treat as UTC
      let normalizedStr = inputStr;

      // Add time component if only date is provided
      if (normalizedStr.length === 10) {
        normalizedStr += " 00:00:00";
      }

      // Normalize to ISO format
      if (!normalizedStr.includes("T")) {
        normalizedStr = normalizedStr.replace(" ", "T");
      }

      // If no 'Z' suffix, add it to indicate UTC
      if (!normalizedStr.endsWith("Z") && !normalizedStr.includes("+")) {
        normalizedStr += "Z";
      }

      date = parseISO(normalizedStr);
    }

    // Format as 'YYYY-MM-DD HH:MM:SS' (BigQuery TIMESTAMP format in UTC)
    const result = format(date, "yyyy-MM-dd HH:mm:ss+00:00");
    console.log("TIMESTAMP function result:", result);
    return result;
  } catch (error) {
    console.error("TIMESTAMP error:", error);
    return null;
  }
}
