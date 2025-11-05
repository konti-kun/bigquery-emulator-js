import { dbSession } from "~/utils/db.server";
import type { QueryResponse, TableFieldSchema } from "types/query";
import sqlParser from "node-sql-parser";
import { array_to_json, bigquery_to_sqlite_types } from "~/utils/changer";
import type { JobConfigurationQuery } from "types/job";
import { toZonedTime } from "date-fns-tz";

/**
 * テーブルスキーマをデータベースから取得
 */
function getTableSchema(query: string): { fields: TableFieldSchema[] } {
  const tableMatch = query.match(/FROM\s+`([^`]+)`/i);

  if (!tableMatch) {
    return { fields: [] };
  }

  const fullTableName = tableMatch[1];
  const parts = fullTableName.split(".");

  if (parts.length !== 2) {
    return { fields: [] };
  }

  const [datasetId, tableId] = parts;
  const tableInfo = dbSession()
    .prepare("SELECT schema FROM tables WHERE table_id = ? AND dataset_id = ?")
    .get(tableId, datasetId) as { schema: string } | undefined;

  if (!tableInfo) {
    return { fields: [] };
  }

  return JSON.parse(tableInfo.schema);
}

/**
 * 値の型変換処理
 */
function convertValueByFieldSchema(
  value: any,
  fieldSchema: TableFieldSchema | undefined
): any {
  if (!fieldSchema || typeof value !== "string") {
    return value;
  }

  // REPEATED または STRUCT の場合は JSON パース
  if (fieldSchema.mode === "REPEATED" || fieldSchema.type === "STRUCT") {
    try {
      return JSON.parse(value);
    } catch (e) {
      return value;
    }
  }

  // INTEGER, INT64 の場合は文字列に変換
  if (["INTEGER", "INT64"].includes(fieldSchema.type)) {
    return value.toString();
  }

  // TIMESTAMP の場合はマイクロ秒に変換
  if (fieldSchema.type === "TIMESTAMP") {
    // UTC として処理するため、date-fns-tz を使用
    const utcDate = toZonedTime(value, "UTC");
    if (!isNaN(utcDate.getTime())) {
      return (utcDate.getTime() * 1000).toString();
    }
  }

  return value;
}

/**
 * 結果データを処理
 */
function processResultRows(
  result: Record<string, any>[],
  tableSchema: { fields: TableFieldSchema[] }
): Record<string, any>[] {
  return result.map((row) => {
    const processedRow: Record<string, any> = {};
    const keys = Object.keys(row);

    keys.forEach((key) => {
      const value = row[key];
      const fieldSchema = tableSchema.fields.find((f) => f.name === key);
      processedRow[key] = convertValueByFieldSchema(value, fieldSchema);
    });

    return processedRow;
  });
}

/**
 * スキーマを構築
 */
function buildSchema(
  keys: string[],
  processedResult: Record<string, any>[],
  tableSchema: { fields: TableFieldSchema[] }
): TableFieldSchema[] {
  const schema: TableFieldSchema[] = [];

  keys.forEach((key, index) => {
    const keyName = isNaN(Number(key)) ? key : `f${index}_`;
    const value = processedResult[0]?.[key];

    // テーブルスキーマが利用可能な場合はそれを使用
    const fieldSchema = tableSchema.fields.find((f) => f.name === key);
    if (fieldSchema) {
      schema.push(fieldSchema);
      return;
    }

    // フォールバック: 型推論
    switch (typeof value) {
      case "string":
        const timestampPattern =
          /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$/;
        if (timestampPattern.test(value)) {
          schema.push({ name: keyName, type: "TIMESTAMP", mode: "NULLABLE" });
        } else {
          schema.push({ name: keyName, type: "STRING", mode: "NULLABLE" });
        }
        break;
      case "number":
        if (Number.isInteger(value)) {
          schema.push({ name: keyName, type: "INTEGER", mode: "NULLABLE" });
        } else {
          schema.push({ name: keyName, type: "FLOAT", mode: "NULLABLE" });
        }
        break;
      case "object":
        if (Array.isArray(value)) {
          schema.push({ name: keyName, type: "STRING", mode: "REPEATED" });
        } else {
          schema.push({ name: keyName, type: "STRUCT", mode: "NULLABLE" });
        }
        break;
      default:
        schema.push({ name: keyName, type: "STRING", mode: "NULLABLE" });
    }
  });

  return schema;
}

/**
 * 配列をBigQueryフォーマットに変換
 */
function convertArrayToBigQueryFormat(value: any): any {
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
}

/**
 * クエリ実行の共通処理
 */
export function executeQuery(
  query: string,
  queryParameters: JobConfigurationQuery["queryParameters"],
  jobResponse: QueryResponse
): QueryResponse {
  try {
    const parser = new sqlParser.Parser();
    let ast = parser.astify(query, { database: "BigQuery" });
    ast = array_to_json(ast);
    ast = bigquery_to_sqlite_types(ast);
    const sqlQuery = parser.sqlify(ast, { database: "sqlite" });

    console.log("SQL Query:", sqlQuery);
    const sqlParams =
      queryParameters?.map((p) => ({ [p.name!]: p.parameterValue.value })) ||
      [];

    console.log("SQL Parameters:", sqlParams);
    const result = dbSession()
      .prepare(sqlQuery)
      .all(...sqlParams) as Record<string, any>[];

    console.log("Query Result:", result);

    // 結果が空の場合
    if (result.length === 0) {
      jobResponse.totalRows = "0";
      jobResponse.schema = { fields: [] };
      jobResponse.rows = [];
      jobResponse.jobComplete = true;
      jobResponse.endTime = new Date().getTime().toString();
      return jobResponse;
    }

    // 結果を処理
    jobResponse.totalRows = result.length.toString();
    const keys = Object.keys(result[0]);
    if (keys.length === 0) {
      return jobResponse;
    }

    // テーブルスキーマを取得
    const tableSchema = getTableSchema(query);

    // スキーマを構築
    const schema = buildSchema(keys, result, tableSchema);

    // 結果データを処理
    const processedResult = processResultRows(result, { fields: schema });

    // レスポンスを構築
    jobResponse.schema = { fields: schema };
    jobResponse.rows = processedResult.map((row) => ({
      f: keys.map((key) => ({
        v: convertArrayToBigQueryFormat(row[key]),
      })),
    }));
    jobResponse.pageToken = "";
    jobResponse.errors = [];
  } catch (error) {
    console.error("Query execution error:", error);
    jobResponse.errors = [
      {
        reason: "invalidQuery",
        location: "",
        debugInfo: "",
        message: error instanceof Error ? error.message : String(error),
      },
    ];
    jobResponse.jobComplete = true;
  }
  return jobResponse;
}
