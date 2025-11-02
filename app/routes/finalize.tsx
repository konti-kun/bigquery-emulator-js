import type { Route } from "./+types/initialize";
import dedent from "dedent";
import { dbSession } from "~/utils/db.server";

const DROP_TABLE_DATASETS = dedent`
  DROP table datasets;
`;
const DROP_TABLE_TABLES = dedent`
  DROP table tables;
`;
const DROP_TABLE_JOBS = dedent`
  DROP table jobs;
`;

export const action = async ({ request }: Route.ActionArgs) => {
  switch (request.method) {
    case "POST": {
      dbSession().exec(DROP_TABLE_DATASETS);
      dbSession().exec(DROP_TABLE_TABLES);
      dbSession().exec(DROP_TABLE_JOBS);
    }
  }
  return "ok";
};
