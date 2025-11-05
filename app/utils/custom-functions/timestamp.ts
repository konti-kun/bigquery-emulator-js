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

    let date: Date;

    if (timezone) {
      const timezoneStr = String(timezone);

      let normalizedStr = inputStr;
      if (!normalizedStr.includes("T")) {
        if (normalizedStr.length === 10) {
          normalizedStr += " 00:00:00";
        }
        normalizedStr = normalizedStr.replace(" ", "T");
      }

      normalizedStr = normalizedStr.replace("Z", "");

      const localDate = parseISO(normalizedStr);
      date = fromZonedTime(localDate, timezoneStr);
    } else {
      let normalizedStr = inputStr;

      if (normalizedStr.length === 10) {
        normalizedStr += " 00:00:00";
      }

      if (!normalizedStr.includes("T")) {
        normalizedStr = normalizedStr.replace(" ", "T");
      }

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
