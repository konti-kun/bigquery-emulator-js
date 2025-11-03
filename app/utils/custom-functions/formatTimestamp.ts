import { format, parseISO } from "date-fns";
import { formatInTimeZone } from "date-fns-tz";
import { convertBigQueryFormatToDateFns } from "../bigquery-format-helpers";

/**
 * FORMAT_TIMESTAMP custom function for BigQuery emulator
 * Formats a timestamp according to the specified format string and optional timezone
 */
export function formatTimestamp(...args: any[]): string | null {
  try {
    const [formatString, timestampString, timezone] = args;

    if (!formatString || !timestampString) {
      return null;
    }

    // BigQuery TIMESTAMP format: 'YYYY-MM-DD HH:MM:SS' (always in UTC)
    // Convert BigQuery format string to date-fns format
    const convertedFormat = convertBigQueryFormatToDateFns(
      String(formatString)
    );

    // Format with or without timezone
    if (timezone) {
      // When timezone is specified, parse as UTC and convert to target timezone
      const dateStr = String(timestampString).replace(" ", "T") + "Z";
      const date = parseISO(dateStr);
      return formatInTimeZone(date, String(timezone), convertedFormat);
    } else {
      // When no timezone is specified, treat as UTC and format in UTC
      const dateStr = String(timestampString).replace(" ", "T");
      const date = parseISO(dateStr);
      return format(date, convertedFormat);
    }
  } catch (error) {
    console.error("FORMAT_TIMESTAMP error:", error);
    return null;
  }
}
