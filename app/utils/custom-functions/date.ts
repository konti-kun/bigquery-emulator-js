import { format, parseISO } from "date-fns";

/**
 * DATE custom function for BigQuery emulator
 * Creates a DATE value from various inputs:
 * - DATE(year, month, day): Creates a date from integers
 * - DATE(timestamp): Extracts date from timestamp
 * - DATE(date_string): Parses date string
 */
export function date(...args: any[]): string | null {
  try {
    console.log("DATE function called with args:", args);

    if (args.length === 0) {
      console.log("DATE: No arguments provided");
      return null;
    }

    // Case 1: DATE(year, month, day)
    if (args.length === 3) {
      const [year, month, day] = args.map((arg) => {
        const num = Number(arg);
        if (isNaN(num)) {
          throw new Error(`Invalid number: ${arg}`);
        }
        return num;
      });

      // JavaScript Date uses 0-based months, so subtract 1
      const dateObj = new Date(year, month - 1, day);

      // Validate the date
      if (
        dateObj.getFullYear() !== year ||
        dateObj.getMonth() !== month - 1 ||
        dateObj.getDate() !== day
      ) {
        throw new Error(`Invalid date: ${year}-${month}-${day}`);
      }

      // Format as 'YYYY-MM-DD'
      const result = format(dateObj, "yyyy-MM-dd");
      console.log("DATE result (from year/month/day):", result);
      return result;
    }

    // Case 2: DATE(timestamp) or DATE(date_string)
    if (args.length === 1) {
      const input = String(args[0]);

      // Check if input is already a date string (YYYY-MM-DD format)
      if (/^\d{4}-\d{2}-\d{2}$/.test(input)) {
        console.log("DATE result (date string):", input);
        return input;
      }

      // Parse as timestamp or datetime string
      let normalizedStr = input;

      // Handle ISO 8601 format with timezone
      if (
        normalizedStr.includes("T") ||
        normalizedStr.includes("+") ||
        normalizedStr.endsWith("Z")
      ) {
        const dateObj = parseISO(normalizedStr);
        const result = format(dateObj, "yyyy-MM-dd");
        console.log("DATE result (from ISO timestamp):", result);
        return result;
      }

      // Handle datetime string (YYYY-MM-DD HH:MM:SS)
      if (normalizedStr.includes(" ")) {
        const datePart = normalizedStr.split(" ")[0];
        if (/^\d{4}-\d{2}-\d{2}$/.test(datePart)) {
          console.log("DATE result (from datetime):", datePart);
          return datePart;
        }
      }

      // Try to parse as date
      const dateObj = parseISO(normalizedStr);
      const result = format(dateObj, "yyyy-MM-dd");
      console.log("DATE result (parsed):", result);
      return result;
    }

    console.log("DATE: Unsupported number of arguments:", args.length);
    return null;
  } catch (error) {
    console.error("DATE error:", error);
    return null;
  }
}
