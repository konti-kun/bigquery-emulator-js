import { dbSession } from "~/utils/db.server";
import type { QueryResponse, TableFieldSchema } from "types/query";
import sqlParser from "node-sql-parser";
import {
  array_to_json,
  bigquery_to_sqlite_types,
  unnest_to_json_each,
} from "~/utils/changer";
import type { JobConfigurationQuery } from "types/job";
import { parseISO } from "date-fns";

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
    const date = parseISO(value.replace(" ", "T").replace("+00:00", "Z"));
    if (!isNaN(date.getTime())) {
      return (date.getTime() * 1000).toString();
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
  tableSchema: { fields: TableFieldSchema[] },
  ast: any
): TableFieldSchema[] {
  const schema: TableFieldSchema[] = [];

  keys.forEach((key, index) => {
    const keyName = isNaN(Number(key)) ? key : `f${index}_`;
    const value = processedResult[0]?.[key];

    const column = ast?.columns?.find(
      (c: any) => c.as === keyName || c.as?.value === keyName
    );
    // テーブルスキーマが利用可能な場合はそれを使用
    const fieldSchema = tableSchema.fields.find((f) => f.name === key);
    if (fieldSchema && (!column || column.expr.type === "column_ref")) {
      schema.push(fieldSchema);
      return;
    }
    // フォールバック: 型推論
    switch (typeof value) {
      case "string":
        const isTimestamp =
          column?.expr?.type === "function" &&
          column?.expr?.name.schema.value.toUpperCase() === "TIMESTAMP";
        if (isTimestamp) {
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
    // UNNESTをjson_each()に変換（BigQueryのSQL文字列を先に変換）
    let modifiedQuery = unnest_to_json_each(query);

    const parser = new sqlParser.Parser();
    let ast = parser.astify(modifiedQuery, { database: "BigQuery" });
    ast = array_to_json(ast);
    ast = bigquery_to_sqlite_types(ast);
    const sqlQuery = parser.sqlify(ast, { database: "sqlite" });

    console.log("SQL Query:", sqlQuery);
    console.log("SQL Parameters:", JSON.stringify(queryParameters, null, 2));

    // パラメータを単一のオブジェクトに統合し、配列をJSON文字列に変換
    const sqlParams = (queryParameters || []).reduce(
      (acc, p) => {
        let value = p.parameterValue.value;

        // 配列パラメータの場合、JSON文字列に変換
        if (p.parameterValue.arrayValues) {
          // arrayValuesの各要素から実際の値を抽出
          const arrayData = p.parameterValue.arrayValues.map(
            (item: any) => item.value ?? item
          );
          value = JSON.stringify(arrayData);
        }
        // 構造体パラメータの場合もJSON文字列に変換
        else if (p.parameterValue.structValues) {
          value = JSON.stringify(p.parameterValue.structValues);
        }

        acc[p.name!] = value;
        return acc;
      },
      {} as Record<string, any>
    );

    let result: Record<string, any>[] = [];
    switch ((ast as any)?.type) {
      case "insert":
      case "update":
      case "delete":
        dbSession().prepare(sqlQuery).run(sqlParams);
        break;
      case "select":
        result = dbSession().prepare(sqlQuery).all(sqlParams) as Record<
          string,
          any
        >[];
        break;
    }

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
    const schema = buildSchema(keys, result, tableSchema, ast);

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
