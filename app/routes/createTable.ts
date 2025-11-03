import type { TableFieldSchema } from "../../types/query";

/**
 * BigQueryのスキーマ定義文字列をパースして、TableFieldSchemaの配列に変換する
 * 例: "id INT64, name STRING" -> [{ name: "id", type: "INT64" }, { name: "name", type: "STRING" }]
 */
export function parseSchema(schema: string): TableFieldSchema[] {
  // 空白を削除してトリム
  const trimmedSchema = schema.trim();
  if (!trimmedSchema) {
    return [];
  }

  const fields: TableFieldSchema[] = [];
  let currentIndex = 0;

  while (currentIndex < trimmedSchema.length) {
    const { field, nextIndex } = parseField(trimmedSchema, currentIndex);
    if (field) {
      fields.push(field);
    }
    currentIndex = nextIndex;

    // カンマをスキップ
    while (
      currentIndex < trimmedSchema.length &&
      trimmedSchema[currentIndex] === ","
    ) {
      currentIndex++;
    }
    // 空白をスキップ
    while (
      currentIndex < trimmedSchema.length &&
      /\s/.test(trimmedSchema[currentIndex])
    ) {
      currentIndex++;
    }
  }

  return fields;
}

/**
 * フィールド定義をパースする
 */
function parseField(
  schema: string,
  startIndex: number
): { field: TableFieldSchema | null; nextIndex: number } {
  let index = startIndex;

  // 空白をスキップ
  while (index < schema.length && /\s/.test(schema[index])) {
    index++;
  }

  // フィールド名を取得
  const nameMatch = schema.substring(index).match(/^([a-zA-Z_][a-zA-Z0-9_]*)/);
  if (!nameMatch) {
    return { field: null, nextIndex: index };
  }

  const fieldName = nameMatch[1];
  index += fieldName.length;

  // 空白をスキップ
  while (index < schema.length && /\s/.test(schema[index])) {
    index++;
  }

  // 型情報を解析
  const {
    type,
    mode,
    fields,
    maxLength,
    precision,
    scale,
    nextIndex: typeEndIndex,
  } = parseType(schema, index);
  index = typeEndIndex;

  // NOT NULLチェック
  let fieldMode = mode;
  while (index < schema.length && /\s/.test(schema[index])) {
    index++;
  }

  if (schema.substring(index, index + 8).toUpperCase() === "NOT NULL") {
    fieldMode = "REQUIRED";
    index += 8;
  }

  const field: TableFieldSchema = {
    name: fieldName,
    type,
    mode: fieldMode || "NULLABLE", // デフォルトはNULLABLE
  };
  if (fields && fields.length > 0) {
    field.fields = fields;
  }
  if (maxLength) {
    field.maxLength = maxLength;
  }
  if (precision) {
    field.precision = precision;
  }
  if (scale) {
    field.scale = scale;
  }

  return { field, nextIndex: index };
}

/**
 * 型情報をパースする（ARRAY、STRUCT、基本型などを処理）
 */
function parseType(
  schema: string,
  startIndex: number
): {
  type: string;
  mode?: string;
  fields?: TableFieldSchema[];
  maxLength?: string;
  precision?: string;
  scale?: string;
  nextIndex: number;
} {
  let index = startIndex;

  // ARRAYかどうかをチェック
  if (schema.substring(index, index + 5).toUpperCase() === "ARRAY") {
    index += 5;
    // 空白をスキップ
    while (index < schema.length && /\s/.test(schema[index])) {
      index++;
    }

    // <を探す
    if (schema[index] === "<") {
      index++;
      const {
        type,
        fields,
        nextIndex: innerEndIndex,
      } = parseType(schema, index);

      // >を探す
      index = innerEndIndex;
      while (index < schema.length && /\s/.test(schema[index])) {
        index++;
      }
      if (schema[index] === ">") {
        index++;
      }

      return {
        type,
        mode: "REPEATED",
        fields,
        nextIndex: index,
      };
    }
  }

  // STRUCTかどうかをチェック
  if (schema.substring(index, index + 6).toUpperCase() === "STRUCT") {
    index += 6;
    // 空白をスキップ
    while (index < schema.length && /\s/.test(schema[index])) {
      index++;
    }

    // <を探す
    if (schema[index] === "<") {
      index++;
      const structFields: TableFieldSchema[] = [];

      while (index < schema.length && schema[index] !== ">") {
        const { field, nextIndex } = parseField(schema, index);
        if (field) {
          structFields.push(field);
        }
        index = nextIndex;

        // 空白をスキップ
        while (index < schema.length && /\s/.test(schema[index])) {
          index++;
        }

        // カンマをスキップ
        if (schema[index] === ",") {
          index++;
        }

        // 空白をスキップ
        while (index < schema.length && /\s/.test(schema[index])) {
          index++;
        }
      }

      // >をスキップ
      if (schema[index] === ">") {
        index++;
      }

      return {
        type: "STRUCT",
        fields: structFields,
        nextIndex: index,
      };
    }
  }

  // 基本型を取得（INT64, STRING, NUMERIC, etc.）
  const typeMatch = schema.substring(index).match(/^([A-Z0-9]+)/i);
  if (!typeMatch) {
    return { type: "STRING", nextIndex: index };
  }

  const baseType = typeMatch[1].toUpperCase();
  index += baseType.length;

  // 型パラメータをチェック（STRING(100), NUMERIC(10, 2)など）
  let maxLength: string | undefined;
  let precision: string | undefined;
  let scale: string | undefined;

  // 空白をスキップ
  while (index < schema.length && /\s/.test(schema[index])) {
    index++;
  }

  if (schema[index] === "(") {
    index++;
    // パラメータを取得
    const paramsMatch = schema
      .substring(index)
      .match(/^([0-9]+)(?:\s*,\s*([0-9]+))?\)/);
    if (paramsMatch) {
      const param1 = paramsMatch[1];
      const param2 = paramsMatch[2];

      if (baseType === "STRING" || baseType === "BYTES") {
        maxLength = param1;
      } else if (baseType === "NUMERIC" || baseType === "BIGNUMERIC") {
        precision = param1;
        if (param2) {
          scale = param2;
        }
      }

      index += paramsMatch[0].length;
    }
  }

  return {
    type: baseType,
    maxLength,
    precision,
    scale,
    nextIndex: index,
  };
}

/**
 * フィールド定義の文字列を生成する
 */
function generateFieldDefinition(
  field: TableFieldSchema,
  indentLevel: number
): string {
  const indent = "  ".repeat(indentLevel);
  let fieldDef = `${indent}${field.name} `;

  // ARRAY型の場合
  if (field.mode === "REPEATED") {
    if (field.type === "STRUCT" && field.fields) {
      // ARRAY<STRUCT<...>>
      const structFields = field.fields
        .map((f) => `${f.name} ${generateTypeDefinition(f)}`)
        .join(", ");
      fieldDef += `ARRAY<STRUCT<${structFields}>>`;
    } else {
      // ARRAY<type>
      fieldDef += `ARRAY<${field.type}>`;
    }
  } else if (field.type === "STRUCT" && field.fields) {
    // STRUCT<...>
    const structFields = field.fields
      .map((f) => `${f.name} ${generateTypeDefinition(f)}`)
      .join(", ");
    fieldDef += `STRUCT<${structFields}>`;
  } else {
    // 通常の型
    fieldDef += generateTypeDefinition(field);
  }

  // NOT NULLを追加
  if (field.mode === "REQUIRED") {
    fieldDef += " NOT NULL";
  }

  return fieldDef;
}

/**
 * 型定義を生成する（パラメータを含む）
 */
function generateTypeDefinition(field: TableFieldSchema): string {
  // STRUCT型の場合、再帰的に処理
  if (field.type === "STRUCT" && field.fields) {
    const structFields = field.fields
      .map((f) => `${f.name} ${generateTypeDefinition(f)}`)
      .join(", ");
    return `STRUCT<${structFields}>`;
  }

  let typeDef = field.type;

  // 型パラメータを追加
  if (field.maxLength) {
    typeDef += `(${field.maxLength})`;
  } else if (field.precision) {
    if (field.scale) {
      typeDef += `(${field.precision}, ${field.scale})`;
    } else {
      typeDef += `(${field.precision})`;
    }
  }

  return typeDef;
}

/**
 * SQLite互換のCREATE TABLE文を生成する
 * ARRAYとSTRUCTはJSON型として保存
 */
export function generateSQLiteCreateTableSQL(
  datasetId: string,
  tableId: string,
  fields: TableFieldSchema[]
): string {
  const tableName = `\`${datasetId}.${tableId}\``;
  const fieldDefinitions = fields
    .map((field) => {
      const fieldName = field.name;
      let sqliteType = "TEXT"; // デフォルトはTEXT

      // ARRAY型とSTRUCT型はJSON型として保存
      if (field.mode === "REPEATED" || field.type === "STRUCT") {
        sqliteType = "JSON";
      } else {
        // 基本型をSQLite型にマッピング
        switch (field.type) {
          case "INT64":
          case "INTEGER":
            sqliteType = "INTEGER";
            break;
          case "FLOAT":
          case "FLOAT64":
          case "NUMERIC":
          case "BIGNUMERIC":
            sqliteType = "REAL";
            break;
          case "BOOL":
          case "BOOLEAN":
            sqliteType = "INTEGER"; // SQLiteではBOOLEANは0/1として保存
            break;
          case "STRING":
          case "BYTES":
          case "DATE":
          case "DATETIME":
          case "TIME":
          case "TIMESTAMP":
          default:
            sqliteType = "TEXT";
            break;
        }
      }

      // NOT NULL制約
      const notNull = field.mode === "REQUIRED" ? " NOT NULL" : "";

      return `  ${fieldName} ${sqliteType}${notNull}`;
    })
    .join(",\n");

  return `CREATE TABLE ${tableName} (\n${fieldDefinitions}\n);`;
}
