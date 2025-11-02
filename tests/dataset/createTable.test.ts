import { getBigQueryClient } from "tests/utils";
import { afterEach, beforeEach, describe, expect, test } from "vitest";
import { dbSession } from "~/utils/db.server";

describe("dataset", () => {
  beforeEach(async () => {
    await fetch("http://localhost:9050/initialize", {
      method: "POST",
    });
  });
  afterEach(async () => {
    await fetch("http://localhost:9050/finalize", {
      method: "POST",
    });
  });

  test("Error create table when no dataset", async () => {
    const bigQuery = getBigQueryClient();
    await expect(
      bigQuery.dataset("test_dataset").createTable("test_table", {
        schema: "id INT64, name STRING",
        location: "US",
      })
    ).rejects.toThrow("Not found: Dataset dummy-project:test_dataset");
  });

  describe("exists dataset", () => {
    beforeEach(async () => {
      const bigQuery = getBigQueryClient();
      await bigQuery.createDataset("test_dataset");
    });

    test("create table", async () => {
      const bigQuery = getBigQueryClient();
      const [table] = await bigQuery
        .dataset("test_dataset")
        .createTable("test_table", {
          schema: "id INT64, name STRING",
          location: "US",
        });
      expect(table.id).toBe("test_table");
      const gotTable = bigQuery.dataset("test_dataset").table("test_table");
      const [metadata] = await gotTable.getMetadata();
      expect(metadata.schema).toEqual({
        fields: [
          { name: "id", type: "INT64", mode: "NULLABLE" },
          { name: "name", type: "STRING", mode: "NULLABLE" },
        ],
      });
    });
  });
});
