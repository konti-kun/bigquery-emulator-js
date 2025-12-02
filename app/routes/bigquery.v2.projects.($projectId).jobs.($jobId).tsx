import { dbSession } from "~/utils/db.server";
import type { Route } from "./+types/bigquery.v2.projects.($projectId).jobs.($jobId)";
import type { QueryResponse } from "types/query";
import { nanoid } from "nanoid";
import { removeBackticksFromHost } from "~/utils/changer";
import type { Job } from "types/job";
import { executeQuery } from "~/utils/query-executor";

export function loader({ params }: Route.LoaderArgs) {
  const response: Job = {
    kind: "bigquery#job",
    etag: "etag-placeholder",
    id: params.jobId!,
    jobReference: {
      jobId: params.jobId!,
      projectId: params.projectId!,
      location: "US",
    },
    configuration: {
      jobType: "QUERY",
      query: {
        query: "",
        useLegacySql: false,
      },
    },
    statistics: {
      creationTime: new Date().getTime().toString(),
      startTime: new Date().getTime().toString(),
      endTime: new Date().getTime().toString(),
    },
    selfLink: `https://bigquery.googleapis.com/bigquery/v2/projects/${params.projectId}/jobs/${params.jobId}`,
    user_email: "<user_email>",
    status: { state: "DONE" },
  };
  return Response.json(response);
}

export const action = async ({ request, params }: Route.ActionArgs) => {
  const body = (await request.json()) as Job;
  const response = { ...body } as Job;
  const jobResponse = {} as QueryResponse;
  jobResponse.kind = "bigquery#queryResponse";
  jobResponse.cacheHit = false;
  jobResponse.totalSlotMs = "1";
  jobResponse.startTime = new Date().getTime().toString();
  const jobId = body.jobReference?.jobId ?? `job_${nanoid()}`;
  jobResponse.jobReference = {
    jobId,
    location: "US",
    projectId: params.projectId!,
  };
  // jobResponse.pageToken = "";
  jobResponse.queryId = jobId;
  // jobResponse.errors = [];

  switch (request.method) {
    case "POST": {
      jobResponse.creationTime = new Date().getTime().toString();
      const query = (body as any).configuration?.query?.query ?? "";
      console.log("Executing query:", removeBackticksFromHost(query));
      const queryParameters =
        (body as any).configuration?.query?.queryParameters ?? [];
      console.log("With parameters:", queryParameters);
      executeQuery(
        removeBackticksFromHost(query),
        queryParameters as any,
        jobResponse
      );
    }
  }

  jobResponse.endTime = new Date().getTime().toString();
  jobResponse.jobComplete = true;
  dbSession()
    .prepare("INSERT INTO jobs (job_id, project_id, response) VALUES (?, ?, ?)")
    .run(jobId, params.projectId, JSON.stringify(jobResponse));
  response.status = { state: "DONE" };
  return response;
};
