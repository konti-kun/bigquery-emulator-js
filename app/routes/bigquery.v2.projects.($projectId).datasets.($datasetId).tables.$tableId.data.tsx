import type { Route } from "./+types/bigquery.v2.projects.($projectId).datasets.($datasetId).tables.$tableId";
import type { Table } from "types/table";
import type { TableDataList, TableRow } from "types/tabledata";
import { dbSession } from "~/utils/db.server";

export const loader = async ({ params, request }: Route.LoaderArgs) => {
  const projectId = params.projectId ?? "dummy-project";
  const datasetId = params.datasetId ?? "";
  const tableId = params.tableId ?? "";
  console.log(request.url);
  const response: TableDataList = {
    kind: "bigquery#tableDataList",
    etag: "dummy-etag",
    rows: [],
  };
  const tableInfo = dbSession()
    .prepare(
      "SELECT schema FROM tables WHERE project_id = ? AND dataset_id = ? AND table_id = ?"
    )
    .get(projectId, datasetId, tableId) as { schema: string } | undefined;

  if (!tableInfo) {
    return Response.json(response);
  }

  const result = dbSession()
    .prepare(
      `SELECT * FROM \`${datasetId.replace(/"/g, '""')}.${tableId.replace(/"/g, '""')}\``
    )
    .all() as Record<string, any>[];

  response.rows = result.map((row) => {
    const tableSchema: Table["schema"] = JSON.parse(tableInfo.schema);
    const processedRow: TableRow = { f: [] };
    for (const field of tableSchema?.fields ?? []) {
      const cellValue = row[field.name];
      switch (field.type) {
        case "INTEGER":
        case "INT64":
          // INT64は文字列で返す
          processedRow.f.push({
            v:
              cellValue !== null && cellValue !== undefined
                ? cellValue.toString()
                : null,
          });
          break;
        case "BOOLEAN":
          // BOOLEANは文字列で "true" or "false" で返す
          if (cellValue !== null && cellValue !== undefined) {
            processedRow.f.push({ v: cellValue === 1 || cellValue === true ? "true" : "false" });
          } else {
            processedRow.f.push({ v: null });
          }
          break;
        case "TIMESTAMP":
          // TIMESTAMPはマイクロ秒で返す
          if (cellValue !== null && cellValue !== undefined) {
            const date = new Date(cellValue);
            processedRow.f.push({ v: (date.getTime() * 1000).toString() });
          } else {
            processedRow.f.push({ v: null });
          }
          break;
        default:
          processedRow.f.push({ v: cellValue });
      }
    }
    return processedRow;
  });

  console.log("Response:", JSON.stringify(response, null, 2));

  return Response.json(response);
};
