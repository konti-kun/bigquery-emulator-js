import type { Route } from "./+types/bigquery.v2.projects.($projectId).datasets";
import type { Dataset, PostDatasetRequest } from "types/dataset";
import { dbSession } from "~/utils/db.server";

export const action = async ({ request, params }: Route.ActionArgs) => {
  const body = (await request.json()) as PostDatasetRequest;
  const response = { ...body } as Dataset;
  switch (request.method) {
    case "POST": {
      dbSession()
        .prepare("INSERT INTO datasets (dataset_id, project_id) VALUES (?, ?)")
        .run(
          body.datasetReference.datasetId,
          body.datasetReference.projectId ?? params.projectId
        );
      const stmt = dbSession().prepare(
        "SELECT id FROM datasets WHERE rowid = last_insert_rowid()"
      );
      response.id = (stmt.get() as { id: string }).id;
      console.log(response);
    }
  }
  return response;
};
