import type { Database } from "better-sqlite3";
import { formatTimestamp } from "./formatTimestamp";
import { formatDate } from "./formatDate";
import { timestampTrunc } from "./timestampTrunc";
import { timestamp } from "./timestamp";
import { date } from "./date";
import { currentTimestamp } from "./currentTimestamp";
import { currentDate } from "./currentDate";
import { dateAdd } from "./dateAdd";
import { dateSub } from "./dateSub";
import { safeAdd } from "./safeAdd";
import { safeSubtract } from "./safeSubtract";
import { safeMultiply } from "./safeMultiply";
import { safeDivide } from "./safeDivide";
import { safeNegate } from "./safeNegate";

/**
 * Register all BigQuery custom functions to the database
 */
export function registerCustomFunctions(db: Database): void {
  // Register FORMAT_TIMESTAMP custom function
  db.function("FORMAT_TIMESTAMP", { varargs: true }, formatTimestamp);

  // Register FORMAT_DATE custom function
  db.function("FORMAT_DATE", { varargs: true }, formatDate);

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

  // Register DATE_ADD custom function
  db.function("DATE_ADD", { varargs: true }, dateAdd);

  // Register DATE_SUB custom function
  db.function("DATE_SUB", { varargs: true }, dateSub);

  // Register SAFE_ADD custom function
  db.function("SAFE_ADD", { varargs: true }, safeAdd);

  // Register SAFE_SUBTRACT custom function
  db.function("SAFE_SUBTRACT", { varargs: true }, safeSubtract);

  // Register SAFE_MULTIPLY custom function
  db.function("SAFE_MULTIPLY", { varargs: true }, safeMultiply);

  // Register SAFE_DIVIDE custom function
  db.function("SAFE_DIVIDE", { varargs: true }, safeDivide);

  // Register SAFE_NEGATE custom function
  db.function("SAFE_NEGATE", { varargs: true }, safeNegate);

  // Register COUNTIF as an aggregate function
  db.aggregate("COUNTIF", {
    start: () => 0,
    step: (aggregate: number, condition: any) => {
      console.log("COUNTIF step called with condition:", condition);
      if (condition) {
        aggregate++;
      }
      return aggregate;
    },
    result: (aggregate: number) => aggregate,
  });
}
