import type { Database } from "better-sqlite3";
import { formatTimestamp } from "./formatTimestamp";
import { timestampTrunc } from "./timestampTrunc";
import { timestamp } from "./timestamp";
import { date } from "./date";
import { currentTimestamp } from "./currentTimestamp";
import { currentDate } from "./currentDate";

/**
 * Register all BigQuery custom functions to the database
 */
export function registerCustomFunctions(db: Database): void {
  // Register FORMAT_TIMESTAMP custom function
  db.function("FORMAT_TIMESTAMP", { varargs: true }, formatTimestamp);

  // Register TIMESTAMP_TRUNC custom function
  db.function("TIMESTAMP_TRUNC", { varargs: true }, timestampTrunc);

  // Register CURRENT_TIMESTAMP custom function
  db.function("CURRENT_TIMESTAMP", { varargs: true }, currentTimestamp);

  // Register CURRENT_DATE custom function
  db.function("CURRENT_DATE", { varargs: true }, currentDate);

  // Register _CURRENT_DATE as an internal alias for CURRENT_DATE with parameters
  db.function("_CURRENT_DATE", { varargs: true }, currentDate);

  // Register TIMESTAMP custom function
  db.function("TIMESTAMP", { varargs: true }, timestamp);

  // Register DATE custom function
  db.function("DATE", { varargs: true }, date);
}
