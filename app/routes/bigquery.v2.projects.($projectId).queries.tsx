import { dbSession } from "~/utils/db.server";
import type { Route } from "./+types/bigquery.v2.projects.($projectId).datasets";
import type { QueryRequest, QueryResponse } from "types/query";
import { nanoid } from "nanoid";
import { executeQuery } from "~/utils/query-executor";

export const action = async ({ request, params }: Route.ActionArgs) => {
  const body = (await request.json()) as QueryRequest;
  const response = {} as QueryResponse;
  response.kind = "bigquery#queryResponse";
  response.cacheHit = false;
  response.totalSlotMs = "1";
  response.startTime = new Date().getTime().toString();
  const jobId = `job_${nanoid()}`;
  response.jobReference = {
    jobId,
    location: "US",
    projectId: params.projectId!,
  };
  // response.pageToken = "";
  response.queryId = jobId;

  switch (request.method) {
    case "POST": {
      response.creationTime = new Date().getTime().toString();
      const queryParameters = body.queryParameters;
      executeQuery(body.query, queryParameters as any, response);
    }
  }

  response.endTime = new Date().getTime().toString();
  response.jobComplete = true;
  dbSession()
    .prepare("INSERT INTO jobs (job_id, project_id, response) VALUES (?, ?, ?)")
    .run(jobId, params.projectId, JSON.stringify(response));
  console.log(JSON.stringify(response, null, 2));
  return response;
};
