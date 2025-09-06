import { dbSession } from "~/utils/db.server";
import type { Route } from "./+types/bigquery.v2.projects.($projectId).datasets";
import type {
  QueryRequest,
  QueryResponse,
  TableFieldSchema,
} from "types/query";
import { nanoid } from "nanoid";

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
  response.pageToken = "";
  response.errors = [];

  switch (request.method) {
    case "POST": {
      response.creationTime = new Date().getTime().toString();
      console.log(body);
      const result = dbSession().prepare(body.query).all() as Record<
        string,
        number | string
      >[];
      console.log(result);
      if (result.length === 0) {
        response.totalRows = "0";
        response.schema = { fields: [] };
        response.rows = [];
        response.jobComplete = true;
        response.endTime = new Date().getTime().toString();
        return response;
      }
      response.totalRows = result.length.toString();
      const keys = Object.keys(result[0]);
      if (keys.length === 0) {
        return response;
      }
      const schema: TableFieldSchema[] = [];
      keys.forEach((key, index) => {
        const keyName = isNaN(Number(key)) ? key : `f${index}_`;
        switch (typeof result[0]?.[key]) {
          case "string":
            schema.push({ name: keyName, type: "STRING", mode: "NULLABLE" });
            break;
          case "number":
            if (Number.isInteger(result[0]?.[key])) {
              schema.push({ name: keyName, type: "INTEGER", mode: "NULLABLE" });
            } else {
              schema.push({ name: keyName, type: "FLOAT", mode: "NULLABLE" });
            }
        }
      });
      response.schema = {
        fields: schema,
      };
      response.rows = result.map((row) => ({
        f: keys.map((key) => ({ v: row[key] })),
      }));
      response.pageToken = "";
      response.errors = [];
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
