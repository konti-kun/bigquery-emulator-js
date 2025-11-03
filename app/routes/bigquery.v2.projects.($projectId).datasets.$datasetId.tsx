import type { Route } from "./+types/bigquery.v2.projects.($projectId).datasets";
import type { Dataset, PostDatasetRequest } from "types/dataset";
import { dbSession } from "~/utils/db.server";

export const action = async ({ request, params }: Route.ActionArgs) => {
  const datasetId = params.datasetId;
  const projectId = params.projectId;
  switch (request.method) {
    case "DELETE": {
      const dataset = dbSession()
        .prepare(
          "SELECT id FROM datasets WHERE dataset_id = ? AND project_id = ?"
        )
        .get(datasetId, projectId);
      if (!dataset) {
        return new Response(
          "Not found: Dataset " + projectId + ":" + datasetId,
          { status: 404 }
        );
      }

      const tables = dbSession()
        .prepare(
          "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name LIKE ?"
        )
        .all(`${datasetId}.%`) as { name: string }[] | undefined;
      console.log("Deleting tables:", tables);
      if (tables) {
        for (const table of tables) {
          try {
            dbSession().exec(`DROP TABLE IF EXISTS \`${table.name}\``);
          } catch (e) {
            console.error(`Failed to drop table ${table.name}:`, e);
          }
        }
      }
      dbSession()
        .prepare("DELETE FROM tables WHERE dataset_id = ? AND project_id = ?")
        .run(datasetId, projectId);

      dbSession()
        .prepare("DELETE FROM datasets WHERE dataset_id = ? AND project_id = ?")
        .run(datasetId, projectId);
    }
  }
  return {};
};
