import type { Route } from "./+types/bigquery.v2.projects.($projectId).datasets.($datasetId).tables.$tableId";
import type { Table } from "types/table";
import { dbSession } from "~/utils/db.server";

const getTable = (
  projectId: string,
  datasetId: string,
  tableId: string
): Response => {
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

  // Check if table exists and get table data
  const table = dbSession()
    .prepare(
      "SELECT table_id, dataset_id, project_id, schema, strftime('%s', created_at) * 1000 as created_at, strftime('%s', updated_at) * 1000 as updated_at FROM tables WHERE table_id = ? AND dataset_id = ? AND project_id = ?"
    )
    .get(tableId, datasetId, projectId) as
    | {
        table_id: string;
        dataset_id: string;
        project_id: string;
        schema: string;
        created_at: number;
        updated_at: number;
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

  // Construct the response
  const response: Table = {
    kind: "bigquery#table",
    etag: "etag",
    id: `${projectId}:${datasetId}.${tableId}`,
    selfLink: `https://bigquery.googleapis.com/bigquery/v2/projects/${projectId}/datasets/${datasetId}/tables/${tableId}`,
    tableReference: {
      projectId: table.project_id,
      datasetId: table.dataset_id,
      tableId: table.table_id,
    },
    schema: JSON.parse(table.schema),
    creationTime: table.created_at.toString(),
    lastModifiedTime: table.updated_at.toString(),
    type: "TABLE",
    location: "US",
  };

  return Response.json(response);
};

export const loader = async ({ params }: Route.LoaderArgs) => {
  const projectId = params.projectId ?? "dummy-project";
  const datasetId = params.datasetId ?? "";
  const tableId = params.tableId ?? "";
  return getTable(projectId, datasetId, tableId);
};

export const action = async ({ request, params }: Route.ActionArgs) => {
  const projectId = params.projectId ?? "dummy-project";
  const datasetId = params.datasetId ?? "";
  const tableId = params.tableId ?? "";

  switch (request.method) {
    case "POST":
      return getTable(projectId, datasetId, tableId);
    case "DELETE": {
      const result = dbSession()
        .prepare(
          "DELETE FROM tables WHERE table_id = ? AND dataset_id = ? AND project_id = ?"
        )
        .run(tableId, datasetId, projectId);

      if (result.changes === 0) {
        return new Response(
          `Not found: Table ${projectId}:${datasetId}.${tableId}`,
          {
            status: 404,
          }
        );
      }
      dbSession()
        .prepare("DROP TABLE IF EXISTS `" + `${datasetId}.${tableId}` + "`")
        .run();
      return new Response(null, { status: 204 });
    }
    default:
      return new Response(`Method Not Allowed: ${request.method}`, {
        status: 405,
      });
  }
};
