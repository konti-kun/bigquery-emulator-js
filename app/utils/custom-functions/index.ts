import type { Database } from "better-sqlite3";
import { formatTimestamp } from "./formatTimestamp";
import { timestampTrunc } from "./timestampTrunc";

/**
 * Register all BigQuery custom functions to the database
 */
export function registerCustomFunctions(db: Database): void {
  // Register FORMAT_TIMESTAMP custom function
  db.function("FORMAT_TIMESTAMP", { varargs: true }, formatTimestamp);

  // Register TIMESTAMP_TRUNC custom function
  db.function("TIMESTAMP_TRUNC", { varargs: true }, timestampTrunc);
}
