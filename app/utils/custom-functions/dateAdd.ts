import { format, parseISO, addDays, addMonths, addYears } from "date-fns";

/**
 * DATE_ADD custom function for BigQuery emulator
 * Adds a specified time interval to a DATE.
 * Signature: DATE_ADD(date_expression, INTERVAL int64_expression date_part)
 */
export function dateAdd(...args: any[]): string | null {
  try {
    console.log("DATE_ADD function called with args:", args);

    // We expect 3 arguments: date, interval value, and interval unit (DAY, MONTH, YEAR)
    if (args.length !== 3) {
      console.log(
        "DATE_ADD: Expected 3 arguments (date, interval value, date_part)"
      );
      return null;
    }

    const [dateInput, intervalValue, datePart] = args;

    // Parse the date input
    let dateStr = String(dateInput);
    if (!dateStr || dateStr === "null" || dateStr === "undefined") {
      console.log("DATE_ADD: Invalid date input");
      return null;
    }

    // Remove DATE keyword if present
    dateStr = dateStr.replace(/^DATE\s+/i, "").trim();
    // Remove quotes if present
    dateStr = dateStr.replace(/^['"]|['"]$/g, "");

    // Parse interval value
    const interval = Number(intervalValue);
    if (isNaN(interval)) {
      console.log("DATE_ADD: Invalid interval value");
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
      console.error("DATE_ADD: Failed to parse date:", error);
      return null;
    }

    // Add the interval based on the date part
    let resultDate: Date;
    const datePartUpper = String(datePart).toUpperCase();

    switch (datePartUpper) {
      case "DAY":
        resultDate = addDays(dateObj, interval);
        break;
      case "MONTH":
        resultDate = addMonths(dateObj, interval);
        break;
      case "YEAR":
        resultDate = addYears(dateObj, interval);
        break;
      default:
        console.log("DATE_ADD: Unsupported date part:", datePart);
        return null;
    }

    // Format as 'YYYY-MM-DD'
    const result = format(resultDate, "yyyy-MM-dd");
    console.log("DATE_ADD result:", result);
    return result;
  } catch (error) {
    console.error("DATE_ADD error:", error);
    return null;
  }
}
