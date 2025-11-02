import type { Route } from "./+types/initialize";
import dedent from "dedent";
import { dbSession } from "~/utils/db.server";

const CREATE_TABLE_DATASETS = dedent`
  CREATE TABLE IF NOT EXISTS datasets (
    id INTEGER PRIMARY KEY,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    dataset_id TEXT NOT NULL,
    project_id TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_datasets_dataset_id ON datasets (dataset_id);
`;

const CREATE_TABLE_TABLES = dedent`
  CREATE TABLE IF NOT EXISTS tables (
    id INTEGER PRIMARY KEY,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    table_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    schema JSON NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_tables_table_id ON tables (table_id);
`;

const CREATE_TABLE_JOBS = dedent`
  CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    job_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    response JSON NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_jobs_job_id ON jobs (job_id);
`;

export const action = async ({ request }: Route.ActionArgs) => {
  switch (request.method) {
    case "POST": {
      dbSession().exec(CREATE_TABLE_DATASETS);
      dbSession().exec(CREATE_TABLE_TABLES);
      dbSession().exec(CREATE_TABLE_JOBS);
    }
  }
  return "ok";
};
