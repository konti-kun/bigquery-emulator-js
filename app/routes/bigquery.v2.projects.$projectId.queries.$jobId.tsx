import { dbSession } from "~/utils/db.server";
import type { Route } from "./+types/bigquery.v2.projects.$projectId.queries.$jobId";
import type {
  QueryRequest,
  QueryResponse,
  TableFieldSchema,
} from "types/query";

export function loader({ params }: Route.LoaderArgs) {
  return Response.json({ kind: "bigquery#query", id: params.jobId } as any);
}

export const action = async ({ request, params }: Route.ActionArgs) => {
  console.log("queries.jobId", params);
  const jobId = params.jobId!;
  const result = dbSession()
    .prepare("SELECT response FROM jobs WHERE job_id = ?")
    .get(jobId) as { response: any };
  console.log(result);
  return Response.json({ ...result.response });
};
