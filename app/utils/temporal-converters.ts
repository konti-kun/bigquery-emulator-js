import { parseISO, fromUnixTime, isValid, format } from "date-fns";
import { formatInTimeZone } from "date-fns-tz";

/**
 * Convert a value to TIMESTAMP format (UTC ISO8601 with timezone)
 * Accepts: ISO8601 string, BigQuery format string, Unix timestamp (seconds)
 * Returns: yyyy-MM-ddTHH:mm:ss.SSSZ format
 */
export function convertToTimestamp(value: any): string | null {
  if (value === null || value === undefined) {
    return null;
  }

  // Handle Unix timestamp (number in seconds)
  if (typeof value === "number") {
    const date = fromUnixTime(value);
    if (!isValid(date)) {
      return null;
    }
    // Use toISOString() to ensure UTC output
    return date.toISOString();
  }
  // Handle string formats
  if (typeof value === "string") {
    // Convert BigQuery format to ISO8601
    let isoString = value;
    if (value.includes(" ") && !value.includes("T")) {
      // "2023-12-25 10:30:00" -> "2023-12-25T10:30:00"
      isoString = value.replace(" ", "T");
    }

    // Ensure UTC timezone
    if (
      !isoString.endsWith("Z") &&
      !isoString.includes("+") &&
      !isoString.includes("-", 10)
    ) {
      isoString += "Z";
    }

    const utcDate = parseISO(isoString);
    if (!isValid(utcDate)) {
      return null;
    }
    return formatInTimeZone(utcDate, "UTC", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
  }

  return null;
}

/**
 * Convert a value to DATE format
 * Accepts: Date object, YYYY-MM-DD string, ISO8601 string
 * Returns: yyyy-MM-dd format
 */
export function convertToDate(value: any): string | null {
  if (value === null || value === undefined) {
    return null;
  }

  // Handle Date object
  if (value instanceof Date) {
    if (!isValid(value)) {
      return null;
    }
    return format(value, "yyyy-MM-dd");
  }

  // Handle string format
  if (typeof value === "string") {
    // Check if it's already YYYY-MM-DD format
    const dateRegex = /^(\d{4})-(\d{2})-(\d{2})$/;
    if (dateRegex.test(value)) {
      return value;
    }

    // Try to parse as ISO8601 string and extract date part
    let isoString = value;
    if (value.includes(" ") && !value.includes("T")) {
      isoString = value.replace(" ", "T");
    }

    const date = parseISO(isoString);
    if (isValid(date)) {
      return format(date, "yyyy-MM-dd");
    }

    return null;
  }

  // Handle object (could be a serialized Date)
  if (typeof value === "object" && value !== null) {
    // Try to convert to Date
    const date = new Date(value);
    if (isValid(date)) {
      return format(date, "yyyy-MM-dd");
    }
  }

  return null;
}

/**
 * Convert a value to DATETIME format (no timezone)
 * Accepts: ISO8601 string, BigQuery format string
 * Returns: yyyy-MM-ddTHH:mm:ss format
 */
export function convertToDatetime(value: any): string | null {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === "string") {
    // Convert BigQuery format to ISO8601
    let isoString = value;
    if (value.includes(" ") && !value.includes("T")) {
      // "2023-12-25 10:30:00" -> "2023-12-25T10:30:00"
      isoString = value.replace(" ", "T");
    }

    // Remove timezone info if present (DATETIME doesn't have timezone)
    isoString = isoString.replace(/Z$/, "").replace(/[+-]\d{2}:\d{2}$/, "");

    // Validate datetime format with simple regex
    const datetimeRegex =
      /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,3}))?$/;
    const match = isoString.match(datetimeRegex);

    if (!match) {
      return null;
    }

    const [, year, month, day, hours, minutes, seconds, ms] = match;

    // Build formatted string
    let formatted = `${year}-${month}-${day}T${hours}:${minutes}:${seconds}`;

    // Add milliseconds only if present and non-zero
    if (ms && parseInt(ms) > 0) {
      const msStr = ms.padEnd(3, "0").replace(/0+$/, "");
      formatted += `.${msStr}`;
    }

    return formatted;
  }

  return null;
}
