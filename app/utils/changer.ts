import sqlParser from "node-sql-parser";

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
