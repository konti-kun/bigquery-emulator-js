import type { Route } from "./+types/bigquery.v2.projects.($projectId).datasets.($datasetId).tables.$tableId.insertAll";
import { dbSession } from "~/utils/db.server";

type InsertAllRequest = {
  rows: Array<{
    insertId?: string;
    json: Record<string, any>;
  }>;
  skipInvalidRows?: boolean;
  ignoreUnknownValues?: boolean;
  templateSuffix?: string;
};

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
      const value = rowData[name];

      // Handle null/undefined
      if (value === null || value === undefined) {
        return null;
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

    const result = dbSession().prepare(`SELECT * from ${sqlTableName}`).all();
    console.log("Current table data:", result);
  }

  return Response.json({
    kind: "bigquery#tableDataInsertAllResponse",
  });
};
