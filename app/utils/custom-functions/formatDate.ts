import { format, parseISO } from "date-fns";
import { convertBigQueryFormatToDateFns } from "../bigquery-format-helpers";

/**
 * FORMAT_DATE custom function for BigQuery emulator
 * Formats a date according to the specified format string
 */
export function formatDate(...args: any[]): string | null {
  try {
    const [formatString, dateString] = args;

    if (!formatString || !dateString) {
      return null;
    }

    // Convert BigQuery format string to date-fns format
    const convertedFormat = convertBigQueryFormatToDateFns(
      String(formatString)
    );

    // DATE format: 'YYYY-MM-DD'
    // Parse the date string and format it
    const date = parseISO(String(dateString));
    return format(date, convertedFormat);
  } catch (error) {
    console.error("FORMAT_DATE error:", error);
    return null;
  }
}
