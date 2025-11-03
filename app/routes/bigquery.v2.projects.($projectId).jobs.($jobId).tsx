import { dbSession } from "~/utils/db.server";
import type { Route } from "./+types/bigquery.v2.projects.($projectId).jobs.($jobId)";
import type {
  QueryRequest,
  QueryResponse,
  TableFieldSchema,
} from "types/query";
import { nanoid } from "nanoid";
import sqlParser from "node-sql-parser";
import { array_to_json } from "~/utils/changer";
import type { Job } from "types/job";

export function loader({ params }: Route.LoaderArgs) {
  return Response.json({ kind: "bigquery#job", id: params.jobId } as any);
}

export const action = async ({ request, params }: Route.ActionArgs) => {
  const body = (await request.json()) as Job;
  const response = { ...body } as Job;
  const jobResponse = {} as QueryResponse;
  jobResponse.kind = "bigquery#queryResponse";
  jobResponse.cacheHit = false;
  jobResponse.totalSlotMs = "1";
  jobResponse.startTime = new Date().getTime().toString();
  const jobId = body.jobReference?.jobId ?? `job_${nanoid()}`;
  jobResponse.jobReference = {
    jobId,
    location: "US",
    projectId: params.projectId!,
  };
  jobResponse.pageToken = "";
  jobResponse.errors = [];

  switch (request.method) {
    case "POST": {
      jobResponse.creationTime = new Date().getTime().toString();
      const parser = new sqlParser.Parser();
      const query = (body as any).configuration?.query?.query ?? "";
      let ast = parser.astify(query, {
        database: "BigQuery",
      });
      ast = array_to_json(ast);
      const sqlQuery = parser.sqlify(ast, { database: "sqlite" });
      const result = dbSession().prepare(sqlQuery).all() as Record<
        string,
        any
      >[];

      console.log("SQL Query:", sqlQuery);
      console.log("Query Result:", result);

      if (result.length === 0) {
        jobResponse.totalRows = "0";
        jobResponse.schema = { fields: [] };
        jobResponse.rows = [];
        jobResponse.jobComplete = true;
        jobResponse.endTime = new Date().getTime().toString();
        return jobResponse;
      }

      jobResponse.totalRows = result.length.toString();
      const keys = Object.keys(result[0]);
      if (keys.length === 0) {
        return jobResponse;
      }

      // Try to get table schema info from query
      let tableSchema: { fields: TableFieldSchema[] } = { fields: [] };

      // Extract table name from query (simple pattern matching)
      const tableMatch = query.match(/FROM\s+`([^`]+)`/i);

      if (tableMatch) {
        console.log("Matched table:", tableMatch[1]);
        const fullTableName = tableMatch[1];
        const parts = fullTableName.split(".");

        if (parts.length === 2) {
          const [datasetId, tableId] = parts;

          const tableInfo = dbSession()
            .prepare(
              "SELECT schema FROM tables WHERE table_id = ? AND dataset_id = ?"
            )
            .get(tableId, datasetId) as { schema: string } | undefined;

          if (tableInfo) {
            tableSchema = JSON.parse(tableInfo.schema);
          }
        }
      }

      const schema: TableFieldSchema[] = [];
      const processedResult = result.map((row) => {
        const processedRow: Record<string, any> = {};

        keys.forEach((key) => {
          let value = row[key];

          // Check if this field is JSON type in the table schema
          let fieldSchema: TableFieldSchema | undefined;
          if (tableSchema) {
            fieldSchema = tableSchema.fields.find((f) => f.name === key);
          }

          if (fieldSchema && typeof value === "string") {
            if (
              fieldSchema.mode === "REPEATED" ||
              fieldSchema.type === "STRUCT"
            ) {
              try {
                value = JSON.parse(value);
              } catch (e) {
                // If parsing fails, keep as string
              }
            }
          }

          processedRow[key] = value;
        });

        return processedRow;
      });

      // Build schema
      keys.forEach((key, index) => {
        const keyName = isNaN(Number(key)) ? key : `f${index}_`;
        const value = processedResult[0]?.[key];

        // Use table schema if available
        let fieldSchema: TableFieldSchema | undefined;
        if (tableSchema) {
          fieldSchema = tableSchema.fields.find((f) => f.name === key);
        }

        if (fieldSchema) {
          schema.push(fieldSchema);
        } else {
          // Fallback to type inference
          switch (typeof value) {
            case "string":
              schema.push({ name: keyName, type: "STRING", mode: "NULLABLE" });
              break;
            case "number":
              if (Number.isInteger(value)) {
                schema.push({
                  name: keyName,
                  type: "INTEGER",
                  mode: "NULLABLE",
                });
              } else {
                schema.push({ name: keyName, type: "FLOAT", mode: "NULLABLE" });
              }
              break;
            case "object":
              if (Array.isArray(value)) {
                schema.push({
                  name: keyName,
                  type: "STRING",
                  mode: "REPEATED",
                });
              } else {
                schema.push({
                  name: keyName,
                  type: "STRUCT",
                  mode: "NULLABLE",
                });
              }
              break;
            default:
              schema.push({ name: keyName, type: "STRING", mode: "NULLABLE" });
          }
        }
      });

      // Helper function to convert arrays to BigQuery format
      const convertArrayToBigQueryFormat = (value: any): any => {
        if (Array.isArray(value)) {
          return value.map((item) => ({
            v: convertArrayToBigQueryFormat(item),
          }));
        }
        if (typeof value === "object" && value !== null) {
          const converted: Record<string, any> = {};
          for (const [k, v] of Object.entries(value)) {
            converted[k] = convertArrayToBigQueryFormat(v);
          }
          return converted;
        }
        return value;
      };

      jobResponse.schema = {
        fields: schema,
      };
      jobResponse.rows = processedResult.map((row) => ({
        f: keys.map((key) => ({
          v: convertArrayToBigQueryFormat(row[key]),
        })),
      }));
      jobResponse.pageToken = "";
      jobResponse.errors = [];
    }
  }
  jobResponse.endTime = new Date().getTime().toString();
  jobResponse.jobComplete = true;
  dbSession()
    .prepare("INSERT INTO jobs (job_id, project_id, response) VALUES (?, ?, ?)")
    .run(jobId, params.projectId, JSON.stringify(jobResponse));
  response.status = { state: "DONE" };
  return response;
};
