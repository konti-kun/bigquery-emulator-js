import {
  format,
  parseISO,
  startOfMinute,
  startOfHour,
  startOfDay,
  startOfMonth,
  startOfYear,
} from "date-fns";

/**
 * TIMESTAMP_TRUNC custom function for BigQuery emulator
 * Truncates a timestamp to the specified date part
 */
export function timestampTrunc(...args: any[]): string | null {
  try {
    const [timestampString, datePart] = args;

    if (!timestampString || !datePart) {
      return null;
    }

    const dateStr = String(timestampString).replace(" ", "T");
    const date = parseISO(dateStr);

    let truncatedDate: Date;
    const datePartUpper = String(datePart).toUpperCase();

    switch (datePartUpper) {
      case "YEAR":
        truncatedDate = startOfYear(date);
        break;
      case "MONTH":
        truncatedDate = startOfMonth(date);
        break;
      case "DAY":
        truncatedDate = startOfDay(date);
        break;
      case "HOUR":
        truncatedDate = startOfHour(date);
        break;
      case "MINUTE":
        truncatedDate = startOfMinute(date);
        break;
      default:
        console.error(`TIMESTAMP_TRUNC: Unsupported date part: ${datePart}`);
        return null;
    }

    // Format as 'YYYY-MM-DD HH:MM:SS' (BigQuery TIMESTAMP format)
    return format(truncatedDate, "yyyy-MM-dd HH:mm:ss");
  } catch (error) {
    console.error("TIMESTAMP_TRUNC error:", error);
    return null;
  }
}
