import type { Route } from "./+types/bigquery.v2.projects.($projectId).datasets";
import type { Dataset, PostDatasetRequest } from "types/dataset";
import { dbSession } from "~/utils/db.server";

export const action = async ({ request, params }: Route.ActionArgs) => {
  const body = (await request.json()) as PostDatasetRequest;
  const response = { ...body } as Dataset;
  const datasetId = body.datasetReference.datasetId ?? params.datasetId;
  const projectId = body.datasetReference.projectId ?? params.projectId;
  switch (request.method) {
    case "POST": {
      dbSession()
        .prepare("INSERT INTO datasets (dataset_id, project_id) VALUES (?, ?)")
        .run(datasetId, projectId);
      const stmt = dbSession().prepare(
        "SELECT id FROM datasets WHERE rowid = last_insert_rowid()"
      );
      response.id = (stmt.get() as { id: string }).id;
    }
  }
  return response;
};

export function loader({ params }: Route.LoaderArgs) {
  const datasets = dbSession()
    .prepare(
      "SELECT id, dataset_id AS datasetId, project_id AS projectId FROM datasets WHERE project_id = ?"
    )
    .all(params.projectId) as
    | {
        id: string;
        datasetId: string;
        projectId: string;
      }[]
    | undefined;
  return { nextPageToken: "", datasets: datasets ?? [], unreachable: [] };
}
