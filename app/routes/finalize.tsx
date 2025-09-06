import type { Route } from "./+types/initialize";
import dedent from "dedent";
import { dbSession } from "~/utils/db.server";

const DROP_TABLE_DATASETS = dedent`
  DROP table datasets;
`;

export const action = async ({ request }: Route.ActionArgs) => {
  switch (request.method) {
    case "POST": {
      dbSession().exec(DROP_TABLE_DATASETS);
    }
  }
  return "ok";
};
