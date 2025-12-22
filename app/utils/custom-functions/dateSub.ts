import { format, parseISO, subDays, subMonths, subYears } from "date-fns";

/**
 * DATE_SUB custom function for BigQuery emulator
 * Subtracts a specified time interval from a DATE.
 * Signature: DATE_SUB(date_expression, INTERVAL int64_expression date_part)
 */
export function dateSub(...args: any[]): string | null {
  try {
    console.log("DATE_SUB function called with args:", args);

    // We expect 3 arguments: date, interval value, and interval unit (DAY, MONTH, YEAR)
    if (args.length !== 3) {
      console.log(
        "DATE_SUB: Expected 3 arguments (date, interval value, date_part)"
      );
      return null;
    }

    const [dateInput, intervalValue, datePart] = args;

    // Parse the date input
    let dateStr = String(dateInput);
    if (!dateStr || dateStr === "null" || dateStr === "undefined") {
      console.log("DATE_SUB: Invalid date input");
      return null;
    }

    // Remove DATE keyword if present
    dateStr = dateStr.replace(/^DATE\s+/i, "").trim();
    // Remove quotes if present
    dateStr = dateStr.replace(/^['"]|['"]$/g, "");

    // Parse interval value
    const interval = Number(intervalValue);
    if (isNaN(interval)) {
      console.log("DATE_SUB: Invalid interval value");
      return null;
    }

    // Parse the date
    let dateObj: Date;
    try {
      if (/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
        // Already in YYYY-MM-DD format
        dateObj = parseISO(dateStr);
      } else if (dateStr.includes("T") || dateStr.includes(" ")) {
        // Contains time component
        dateObj = parseISO(dateStr);
      } else {
        dateObj = parseISO(dateStr);
      }
    } catch (error) {
      console.error("DATE_SUB: Failed to parse date:", error);
      return null;
    }

    // Subtract the interval based on the date part
    let resultDate: Date;
    const datePartUpper = String(datePart).toUpperCase();

    switch (datePartUpper) {
      case "DAY":
        resultDate = subDays(dateObj, interval);
        break;
      case "MONTH":
        resultDate = subMonths(dateObj, interval);
        break;
      case "YEAR":
        resultDate = subYears(dateObj, interval);
        break;
      default:
        console.log("DATE_SUB: Unsupported date part:", datePart);
        return null;
    }

    // Format as 'YYYY-MM-DD'
    const result = format(resultDate, "yyyy-MM-dd");
    console.log("DATE_SUB result:", result);
    return result;
  } catch (error) {
    console.error("DATE_SUB error:", error);
    return null;
  }
}
