import { dbSession } from "~/utils/db.server";
import type { Route } from "./+types/bigquery.v2.projects.($projectId).jobs.($jobId)";
import type {
  QueryRequest,
  QueryResponse,
  TableFieldSchema,
} from "types/query";

export function loader({ params }: Route.LoaderArgs) {
  return Response.json({ kind: "bigquery#job", id: params.jobId } as any);
}
