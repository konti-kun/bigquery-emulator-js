import { getBigQueryClient } from "tests/utils";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

describe("table get", () => {
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

  test("Error get table when no dataset", async () => {
    const bigQuery = getBigQueryClient();
    await expect(
      bigQuery.dataset("test_dataset").table("test_table").get()
    ).rejects.toThrow("Not found: Dataset dummy-project:test_dataset");
  });

  describe("exists dataset", () => {
    beforeEach(async () => {
      const bigQuery = getBigQueryClient();
      await bigQuery.createDataset("test_dataset");
    });

    test("Error get table when no table", async () => {
      const bigQuery = getBigQueryClient();
      await expect(
        bigQuery.dataset("test_dataset").table("test_table").get()
      ).rejects.toThrow("Not found: Table dummy-project:test_dataset.test_table");
    });

    test("get table", async () => {
      const bigQuery = getBigQueryClient();
      await bigQuery.dataset("test_dataset").createTable("test_table", {
        schema: "id INT64, name STRING",
        location: "US",
      });

      const table = bigQuery.dataset("test_dataset").table("test_table");
      const [tableInstance] = await table.get();

      // Check Table instance
      expect(tableInstance.id).toBe("test_table");

      // Check API response metadata
      expect(tableInstance.metadata.id).toBe("dummy-project:test_dataset.test_table");
      expect(tableInstance.metadata.tableReference).toEqual({
        projectId: "dummy-project",
        datasetId: "test_dataset",
        tableId: "test_table",
      });
      expect(tableInstance.metadata.schema).toEqual({
        fields: [
          { name: "id", type: "INT64", mode: "NULLABLE" },
          { name: "name", type: "STRING", mode: "NULLABLE" },
        ],
      });
      expect(tableInstance.metadata.type).toBe("TABLE");
      expect(tableInstance.metadata.creationTime).toBeDefined();
      expect(tableInstance.metadata.lastModifiedTime).toBeDefined();
    });
  });
});
