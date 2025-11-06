import type { Route } from "./+types/bigquery.v2.projects.($projectId).datasets.($datasetId).tables.$tableId.insertAll";
import { dbSession } from "~/utils/db.server";
import { parseISO, fromUnixTime, isValid } from "date-fns";
import { format, formatInTimeZone } from "date-fns-tz";

type InsertAllRequest = {
  rows: Array<{
    insertId?: string;
    json: Record<string, any>;
  }>;
  skipInvalidRows?: boolean;
  ignoreUnknownValues?: boolean;
  templateSuffix?: string;
};

/**
 * Convert a value to TIMESTAMP format (UTC ISO8601 with timezone)
 * Accepts: ISO8601 string, BigQuery format string, Unix timestamp (seconds)
 * Returns: yyyy-MM-ddTHH:mm:ss.SSSZ format
 */
function convertToTimestamp(value: any): string | null {
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
 * Convert a value to DATETIME format (no timezone)
 * Accepts: ISO8601 string, BigQuery format string
 * Returns: yyyy-MM-ddTHH:mm:ss format
 */
function convertToDatetime(value: any): string | null {
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

export const action = async ({ request, params }: Route.ActionArgs) => {
  const projectId = params.projectId ?? "dummy-project";
  const datasetId = params.datasetId ?? "";
  const tableId = params.tableId ?? "";

  // Check if dataset exists
  const dataset = dbSession()
    .prepare(
      "SELECT dataset_id FROM datasets WHERE dataset_id = ? AND project_id = ?"
    )
    .get(datasetId, projectId);

  if (!dataset) {
    return new Response(`Not found: Dataset ${projectId}:${datasetId}`, {
      status: 404,
    });
  }

  // Check if table exists and get schema
  const table = dbSession()
    .prepare(
      "SELECT table_id, schema FROM tables WHERE table_id = ? AND dataset_id = ? AND project_id = ?"
    )
    .get(tableId, datasetId, projectId) as
    | {
        table_id: string;
        schema: string;
      }
    | undefined;

  if (!table) {
    return new Response(
      `Not found: Table ${projectId}:${datasetId}.${tableId}`,
      {
        status: 404,
      }
    );
  }

  const body = (await request.json()) as InsertAllRequest;

  if (!body.rows || body.rows.length === 0) {
    return Response.json({ kind: "bigquery#tableDataInsertAllResponse" });
  }

  const schema = JSON.parse(table.schema);
  const sqlTableName = `\`${datasetId}.${tableId}\``;

  // Insert each row
  for (const row of body.rows) {
    const rowData = row.json;

    // Get field names and values
    const fieldNames = schema.fields.map((f: any) => f.name);
    const values = fieldNames.map((name: string) => {
      const field = schema.fields.find((f: any) => f.name === name);
      const value = rowData[name];

      // Handle null/undefined
      if (value === null || value === undefined) {
        return null;
      }

      // Handle TIMESTAMP type
      if (field?.type === "TIMESTAMP") {
        return convertToTimestamp(value);
      }

      // Handle DATETIME type
      if (field?.type === "DATETIME") {
        return convertToDatetime(value);
      }

      // Handle arrays and objects (JSON types)
      if (typeof value === "object") {
        return JSON.stringify(value);
      }

      return value;
    });

    // Build INSERT statement
    const placeholders = fieldNames.map(() => "?").join(", ");
    const insertSQL = `INSERT INTO ${sqlTableName} (${fieldNames.join(", ")}) VALUES (${placeholders})`;

    dbSession()
      .prepare(insertSQL)
      .run(...values);
  }

  return Response.json({
    kind: "bigquery#tableDataInsertAllResponse",
  });
};
