import { singleton } from "./singleton.server";
import database from "better-sqlite3";
import { registerCustomFunctions } from "./custom-functions";

export const dbSession = (session?: string) =>
  singleton(session ?? "sqlite", () => {
    const db = new database(":memory:");

    // Register all BigQuery custom functions
    registerCustomFunctions(db);

    return db;
  });
