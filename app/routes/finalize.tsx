import type { Route } from "./+types/initialize";
import { dbSession } from "~/utils/db.server";

export const action = async ({ request }: Route.ActionArgs) => {
  switch (request.method) {
    case "POST": {
      // Get all user tables (exclude sqlite internal tables)
      const tables = dbSession()
        .prepare(
          "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        .all() as { name: string }[];

      // Drop all tables
      for (const table of tables) {
        try {
          dbSession().exec(`DROP TABLE IF EXISTS \`${table.name}\``);
        } catch (e) {
          console.error(`Failed to drop table ${table.name}:`, e);
        }
      }
    }
  }
  return "ok";
};
