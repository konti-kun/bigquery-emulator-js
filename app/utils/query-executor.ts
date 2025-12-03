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

  // DATE の場合は文字列として返す（BigQuery APIは DATE を文字列として返す）
  if (fieldSchema.type === "DATE") {
    return value;
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
 * CTEやサブクエリから列のスキーマを取得
 */
function getSchemaFromCTE(
  columnName: string,
  ast: any
): TableFieldSchema | null {
  // WITH句を確認
  if (ast?.with) {
    for (const cte of ast.with) {
      // cte.stmt.ast.columns にアクセス
      const columns = cte.stmt?.ast?.columns || cte.stmt?.columns;
      if (columns) {
        // CTEの中から列を探す
        for (let i = 0; i < columns.length; i++) {
          const col = columns[i];
          const colName = col.as || `f${i}_`;
          if (colName === columnName && col.expr) {
            const inferredType = inferTypeFromExpr(col.expr);
            if (inferredType) {
              return { name: columnName, ...inferredType };
            }
          }
        }
      }
    }
  }

  // サブクエリを確認（FROM句内）
  if (ast?.from) {
    for (const fromItem of ast.from) {
      if (fromItem.expr?.type === "select" && fromItem.expr.columns) {
        for (let i = 0; i < fromItem.expr.columns.length; i++) {
          const col = fromItem.expr.columns[i];
          const colName = col.as || `f${i}_`;
          if (colName === columnName && col.expr) {
            const inferredType = inferTypeFromExpr(col.expr);
            if (inferredType) {
              return { name: columnName, ...inferredType };
            }
          }
        }
      }
    }
  }

  return null;
}

/**
 * AST式から型を推論
 */
function inferTypeFromExpr(expr: any): {
  type: string;
  mode: string;
} | null {
  if (!expr) return null;

  // リテラル値からの推論
  switch (expr.type) {
    case "number":
      return Number.isInteger(expr.value)
        ? { type: "INTEGER", mode: "NULLABLE" }
        : { type: "FLOAT", mode: "NULLABLE" };
    case "bool":
      return { type: "BOOL", mode: "NULLABLE" };
    case "single_quote_string":
    case "string":
      return { type: "STRING", mode: "NULLABLE" };
    case "timestamp":
      return { type: "TIMESTAMP", mode: "NULLABLE" };
  }

  // CAST式からの推論
  if (expr.type === "cast" && expr.target?.[0]?.dataType) {
    let dataType = expr.target[0].dataType.toUpperCase();
    // 型の正規化: BigQueryの型名をエミュレータの型名に変換
    switch (dataType) {
      case "TEXT":
        dataType = "STRING";
        break;
      case "INT64":
      case "INT":
        dataType = "INTEGER";
        break;
      case "FLOAT64":
        dataType = "FLOAT";
        break;
      case "DECIMAL":
        dataType = "NUMERIC";
        break;
      // NUMERIC, BIGNUMERIC はそのまま維持
    }
    return { type: dataType, mode: "NULLABLE" };
  }

  // 関数からの推論
  if (expr.type === "function") {
    // 関数名の取得（name.schema.value または name.name[0].value）
    const funcName = (
      expr.name?.schema?.value ||
      expr.name?.name?.[0]?.value ||
      ""
    ).toUpperCase();

    switch (funcName) {
      case "TIMESTAMP":
      case "TIMESTAMP_TRUNC":
        return { type: "TIMESTAMP", mode: "NULLABLE" };
      case "DATE":
      case "CURRENT_DATE":
      case "DATE_TRUNC":
        return { type: "DATE", mode: "NULLABLE" };
      case "FORMAT_TIMESTAMP":
      case "CONCAT":
        return { type: "STRING", mode: "NULLABLE" };
      case "COUNT":
      case "COUNTIF":
      case "SUM":
        return { type: "INTEGER", mode: "NULLABLE" };
      case "AVG":
        return { type: "FLOAT", mode: "NULLABLE" };
      case "JSON_ARRAY":
        // json_array は配列に変換されたもの
        // 引数の型から要素の型を推論
        if (expr.args?.value?.[0]) {
          const elementType = inferTypeFromExpr(expr.args.value[0]);
          if (elementType) {
            return { type: elementType.type, mode: "REPEATED" };
          }
        }
        return { type: "STRING", mode: "REPEATED" };
    }
  }

  // 配列からの推論
  if (expr.type === "array" && expr.array_path) {
    // 配列の最初の要素から型を推論
    if (expr.array_path.length > 0) {
      const firstElement = expr.array_path[0].expr;
      const elementType = inferTypeFromExpr(firstElement);
      if (elementType) {
        return { type: elementType.type, mode: "REPEATED" };
      }
    }
    return { type: "STRING", mode: "REPEATED" };
  }

  // 演算式からの推論
  if (expr.type === "binary_expr") {
    const leftType = inferTypeFromExpr(expr.left);
    const rightType = inferTypeFromExpr(expr.right);

    // 両方の型が同じ場合はその型を返す
    if (leftType && rightType && leftType.type === rightType.type) {
      return leftType;
    }

    // どちらかがFLOATの場合はFLOATを返す
    if (
      (leftType?.type === "FLOAT" || rightType?.type === "FLOAT") &&
      (leftType?.type === "FLOAT" || leftType?.type === "INTEGER") &&
      (rightType?.type === "FLOAT" || rightType?.type === "INTEGER")
    ) {
      return { type: "FLOAT", mode: "NULLABLE" };
    }

    // デフォルトはINTEGER
    if (leftType?.type === "INTEGER" && rightType?.type === "INTEGER") {
      return { type: "INTEGER", mode: "NULLABLE" };
    }
  }

  return null;
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

  // ASTが配列の場合は最初の要素を使用
  const actualAst = Array.isArray(ast) ? ast[0] : ast;

  keys.forEach((key, index) => {
    const keyName = isNaN(Number(key)) ? key : `f${index}_`;
    const value = processedResult[0]?.[key];

    // columnを探す: asがnullの場合、column_refの列名と比較
    const column = actualAst?.columns?.find((c: any) => {
      if (c.as === keyName || c.as?.value === keyName) return true;
      // asがnullでcolumn_refの場合、列名を直接比較
      if (!c.as && c.expr?.type === "column_ref") {
        const colName = c.expr.column?.expr?.value;
        return colName === keyName;
      }
      return false;
    });

    // 1. SELECT句のAST式から型を推論（最優先）
    if (column?.expr) {
      // column_refの場合は、CTEやサブクエリから型を取得
      if (column.expr.type === "column_ref") {
        // column_refの実際の列名を取得
        const refColumnName = column.expr.column?.expr?.value || keyName;
        const cteSchema = getSchemaFromCTE(refColumnName, actualAst);
        if (cteSchema) {
          schema.push({ ...cteSchema, name: keyName });
          return;
        }
      } else {
        const inferredType = inferTypeFromExpr(column.expr);
        if (inferredType) {
          schema.push({ name: keyName, ...inferredType });
          return;
        }
      }
    }

    // 2. テーブルスキーマから型を取得（次点）
    const fieldSchema = tableSchema.fields.find((f) => f.name === key);
    if (fieldSchema && (!column || column.expr.type === "column_ref")) {
      schema.push(fieldSchema);
      return;
    }

    // 3. フォールバック: 実行結果の値から型推論
    switch (typeof value) {
      case "string":
        schema.push({ name: keyName, type: "STRING", mode: "NULLABLE" });
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
    let modifiedQuery = query;

    // UNNESTをjson_each()に変換（BigQueryのSQL文字列を先に変換）
    modifiedQuery = unnest_to_json_each(modifiedQuery);

    console.log("Modified Query after UNNEST conversion:", modifiedQuery);
    // .で繋いだテーブル名を`でくくる (例: dataset.table -> `dataset.table`)
    // DELETE文の特殊処理: DELETE FROM table -> DELETE table (node-sql-parserの制限のため)
    // バッククォートありなし両方に対応
    modifiedQuery = modifiedQuery.replace(
      /\bDELETE\s+FROM\s+`?([a-zA-Z0-9_.]+)`?/gi,
      (_match, tableName) => {
        // バッククォートを除去してから再度追加
        const cleanTableName = tableName.replace(/`/g, "");
        return `DELETE \`${cleanTableName}\``;
      }
    );

    // テーブル名を処理
    modifiedQuery = modifiedQuery.replace(
      /\b(FROM|JOIN|INSERT\sINTO|CREATE\sTABLE)\s+([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)\b/gi,
      (match, keyword, tableName) => {
        // 既にバッククォートで囲まれている場合はスキップ
        if (modifiedQuery.includes(`\`${tableName}\``)) {
          return match;
        }
        return `${keyword} \`${tableName}\``;
      }
    );

    console.log("Modified Query:", modifiedQuery);

    const parser = new sqlParser.Parser();
    let ast = parser.astify(modifiedQuery, { database: "BigQuery" });
    ast = array_to_json(ast);
    ast = bigquery_to_sqlite_types(ast);
    let sqlQuery = parser.sqlify(ast, { database: "sqlite" });

    // CURRENT_TIMESTAMP() (引数なし) を CURRENT_TIMESTAMP に置換
    // 引数がある場合はそのまま（例: CURRENT_TIMESTAMP('Asia/Tokyo')は置換しない）
    sqlQuery = sqlQuery.replaceAll(
      /CURRENT_TIMESTAMP\(\s*\)/gi,
      "CURRENT_TIMESTAMP"
    );
    sqlQuery = sqlQuery.replaceAll(
      /current_timestamp\(\s*\)/gi,
      "CURRENT_TIMESTAMP"
    );

    // CURRENT_DATE() (引数なし) を CURRENT_DATE に置換
    // CURRENT_DATE(引数) の場合は _CURRENT_DATE(引数) に置換してカスタム関数として実行
    // 注意: node-sql-parserが既に_CURRENT_DATEに変換している可能性があるため、
    // _で始まらないCURRENT_DATEのみを対象とする
    sqlQuery = sqlQuery.replaceAll(
      /(?<!_)CURRENT_DATE\(([^)]+)\)/gi,
      "_CURRENT_DATE($1)"
    );
    sqlQuery = sqlQuery.replaceAll(
      /(?<!_)current_date\(([^)]+)\)/gi,
      "_CURRENT_DATE($1)"
    );
    sqlQuery = sqlQuery.replaceAll(
      /(?<!_)CURRENT_DATE\(\s*\)/gi,
      "CURRENT_DATE"
    );
    sqlQuery = sqlQuery.replaceAll(
      /(?<!_)current_date\(\s*\)/gi,
      "CURRENT_DATE"
    );

    // DELETE文の修正: node-sql-parserがSQLiteに変換する際にFROMを省略するため、追加する
    sqlQuery = sqlQuery.replace(/\bDELETE\s+(`[^`]+`)/gi, "DELETE FROM $1");

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
    sqlQuery.split(";").forEach((queryPart, index) => {
      switch ((ast as any)?.type || (ast as any)[index]?.type) {
        case "create":
          const tableId = queryPart
            .match(/CREATE\s+TABLE\s+`?([a-zA-Z0-9_]+)`?/i)?.[1]
            .trim();
          const datasetId = queryPart
            .match(
              /CREATE\s+TABLE\s+`?([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)`?/i
            )?.[1]
            .trim();
          if (tableId) {
            // スキーマ情報を抽出
            const fields: TableFieldSchema[] = [];

            dbSession()
              .prepare(
                "INSERT INTO tables (table_id, dataset_id, project_id, schema) VALUES (?, ?, ?, ?)"
              )
              .run(
                tableId,
                datasetId,
                jobResponse.jobReference.projectId,
                JSON.stringify(fields)
              );
          }
        case "insert":
        case "update":
        case "delete":
          dbSession().prepare(queryPart).run(sqlParams);
          break;
        case "select":
          console.log("Executing SELECT query part:", queryPart);
          result = dbSession().prepare(queryPart).all(sqlParams) as Record<
            string,
            any
          >[];
          break;
      }
    });

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
