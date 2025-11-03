import sqlParser from "node-sql-parser";

// host変数に`が入っているときは削除する
export const removeBackticksFromHost = (sql: string): string => {
  return sql.replace(/@`([^`]*)`/g, (match, p1) => {
    return `@${p1}`;
  });
};

// BigQueryの型をSQLite用に変換
const convertDataType = (dataType: any): any => {
  if (!dataType) return dataType;

  // 配列の場合
  if (Array.isArray(dataType)) {
    return dataType.map((item) => convertDataType(item));
  }

  if (typeof dataType === "string") {
    // STRING -> TEXT
    if (dataType.toUpperCase() === "STRING") {
      return "TEXT";
    }
    return dataType;
  }

  // オブジェクト形式の場合
  if (dataType.dataType) {
    const upperType = dataType.dataType.toUpperCase();
    if (upperType === "STRING") {
      return { ...dataType, dataType: "TEXT" };
    }
  }

  return dataType;
};

// ASTを再帰的に走査して型変換を適用
const traverseAndConvertTypes = (node: any): any => {
  if (!node || typeof node !== "object") return node;

  // CAST関数の型変換
  if (node.type === "cast" && node.target) {
    node.target = convertDataType(node.target);
  }

  // TIMESTAMPリテラルを文字列に変換
  if (node.type === "timestamp") {
    return {
      type: "single_quote_string",
      value: node.value,
    };
  }

  // TIMESTAMP_TRUNC関数の処理
  if (node.type === "function" && node.name) {
    const funcName = Array.isArray(node.name.name)
      ? (node.name.name[0]?.value ?? node.name.schema.value)
      : node.name.name;

    if (
      typeof funcName === "string" &&
      funcName.toUpperCase() === "TIMESTAMP_TRUNC"
    ) {
      // 2番目の引数（date part）を文字列リテラルに変換
      if (node.args && node.args.value && node.args.value.length >= 2) {
        const datePartArg = node.args.value[1];
        // 識別子の場合は文字列リテラルに変換
        if (datePartArg.type === "column_ref" && datePartArg.column) {
          const columnName = Array.isArray(datePartArg.column)
            ? datePartArg.column[0]?.value || datePartArg.column[0]
            : datePartArg.column.expr?.value;
          node.args.value[1] = {
            type: "single_quote_string",
            value: columnName,
          };
        }
      }
    }
  }

  // すべてのプロパティを再帰的に処理
  for (const key in node) {
    if (node.hasOwnProperty(key) && typeof node[key] === "object") {
      if (Array.isArray(node[key])) {
        node[key] = node[key].map((item: any) => traverseAndConvertTypes(item));
      } else {
        node[key] = traverseAndConvertTypes(node[key]);
      }
    }
  }

  return node;
};

export const bigquery_to_sqlite_types = (
  ast: sqlParser.AST | sqlParser.AST[]
): sqlParser.AST | sqlParser.AST[] => {
  if (Array.isArray(ast)) {
    return ast.map(bigquery_to_sqlite_types) as sqlParser.AST[];
  }

  return traverseAndConvertTypes(ast) as sqlParser.AST;
};

export const array_to_json = (
  ast: sqlParser.AST | sqlParser.AST[]
): sqlParser.AST | sqlParser.AST[] => {
  if (Array.isArray(ast)) {
    return ast.map(array_to_json) as sqlParser.AST[];
  }
  if (ast.type === "select") {
    ast.columns.forEach((col, index) => {
      if (col.expr.type === "array") {
        ast.columns[index].expr = {
          type: "function",
          name: { name: [{ type: "default", value: "json_array" }] },
          args: {
            type: "expr_list",
            value: col.expr.array_path.map(
              ({ expr }: { expr: { type: string; value: any } }) => ({
                type: expr.type,
                value: expr.value,
              })
            ),
          },
          over: null,
        };
      }
    });
    if ("_next" in ast) {
      (ast as any)._next = array_to_json(ast._next as sqlParser.AST);
    }
  }
  if ((ast as any).with) {
    (ast as any).with.forEach((withItem: any, index: number) => {
      (ast as any).with[index].stmt.ast = array_to_json(withItem.stmt.ast);
    });
  }
  return ast;
};
