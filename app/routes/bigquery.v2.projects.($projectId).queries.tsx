import { dbSession } from "~/utils/db.server";
import type { Route } from "./+types/bigquery.v2.projects.($projectId).datasets";
import type {
  QueryRequest,
  QueryResponse,
  TableFieldSchema,
} from "types/query";
import { nanoid } from "nanoid";
import sqlParser from "node-sql-parser";
import { array_to_json, bigquery_to_sqlite_types } from "~/utils/changer";

export const action = async ({ request, params }: Route.ActionArgs) => {
  const body = (await request.json()) as QueryRequest;
  const response = {} as QueryResponse;
  response.kind = "bigquery#queryResponse";
  response.cacheHit = false;
  response.totalSlotMs = "1";
  response.startTime = new Date().getTime().toString();
  const jobId = `job_${nanoid()}`;
  response.jobReference = {
    jobId,
    location: "US",
    projectId: params.projectId!,
  };
  response.pageToken = "";
  response.errors = [];

  switch (request.method) {
    case "POST": {
      response.creationTime = new Date().getTime().toString();
      try {
        const parser = new sqlParser.Parser();
        let ast = parser.astify(body.query, { database: "BigQuery" });
        ast = array_to_json(ast);
        ast = bigquery_to_sqlite_types(ast);
        const sqlQuery = parser.sqlify(ast, { database: "sqlite" });

        console.log("SQL Query:", sqlQuery);

        const result = dbSession().prepare(sqlQuery).all() as Record<
          string,
          any
        >[];

        console.log("Query Result:", result);

        if (result.length === 0) {
          response.totalRows = "0";
          response.schema = { fields: [] };
          response.rows = [];
          response.jobComplete = true;
          response.endTime = new Date().getTime().toString();
          return response;
        }

        response.totalRows = result.length.toString();
        const keys = Object.keys(result[0]);
        if (keys.length === 0) {
          return response;
        }

        // Try to get table schema info from query
        let tableSchema: { fields: TableFieldSchema[] } = { fields: [] };

        // Extract table name from query (simple pattern matching)
        const tableMatch = body.query.match(/FROM\s+`([^`]+)`/i);

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
              if (["INTEGER", "INT64"].includes(fieldSchema.type)) {
                value = value.toString();
              }
              if (fieldSchema.type === "TIMESTAMP") {
                const date = new Date(value);
                if (!isNaN(date.getTime())) {
                  value = (date.getTime() * 1000).toString();
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
                schema.push({
                  name: keyName,
                  type: "STRING",
                  mode: "NULLABLE",
                });
                break;
              case "number":
                if (Number.isInteger(value)) {
                  schema.push({
                    name: keyName,
                    type: "INTEGER",
                    mode: "NULLABLE",
                  });
                } else {
                  schema.push({
                    name: keyName,
                    type: "FLOAT",
                    mode: "NULLABLE",
                  });
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
                schema.push({
                  name: keyName,
                  type: "STRING",
                  mode: "NULLABLE",
                });
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

        response.schema = {
          fields: schema,
        };
        response.rows = processedResult.map((row) => ({
          f: keys.map((key) => ({
            v: convertArrayToBigQueryFormat(row[key]),
          })),
        }));
        response.pageToken = "";
        response.errors = [];
      } catch (error) {
        console.error("Query execution error:", error);
        response.errors = [
          {
            reason: "invalidQuery",
            location: "",
            debugInfo: "",
            message: error instanceof Error ? error.message : String(error),
          },
        ];
        response.jobComplete = true;
      }
    }
  }
  response.endTime = new Date().getTime().toString();
  response.jobComplete = true;
  dbSession()
    .prepare("INSERT INTO jobs (job_id, project_id, response) VALUES (?, ?, ?)")
    .run(jobId, params.projectId, JSON.stringify(response));
  console.log(JSON.stringify(response, null, 2));
  return response;
};
