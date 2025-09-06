import { singleton } from "./singleton.server";
import database from "better-sqlite3";

export const dbSession = (session?: string) =>
  singleton(session ?? "sqlite", () => new database(":memory:"));
