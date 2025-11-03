import { getBigQueryClient } from "tests/utils";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

describe("dataset delete", () => {
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

  test("delete dataset that does not exist", async () => {
    const bigQuery = getBigQueryClient();
    await expect(
      bigQuery.dataset("non_existent_dataset").delete()
    ).rejects.toThrow();
  });

  describe("exists dataset", () => {
    beforeEach(async () => {
      const bigQuery = getBigQueryClient();
      await bigQuery.createDataset("test_dataset");
    });

    test("delete dataset without tables", async () => {
      const bigQuery = getBigQueryClient();
      await bigQuery.dataset("test_dataset").delete();

      const [datasets] = await bigQuery.getDatasets();
      const datasetIds = datasets.map((ds) => ds.id);
      expect(datasetIds).not.toContain("test_dataset");
    });

    test("delete dataset with tables", async () => {
      const bigQuery = getBigQueryClient();

      await bigQuery.dataset("test_dataset").createTable("test_table", {
        schema: "id INT64, name STRING",
        location: "US",
      });

      await bigQuery.dataset("test_dataset").delete();

      const [datasets] = await bigQuery.getDatasets();
      const datasetIds = datasets.map((ds) => ds.id);
      expect(datasetIds).not.toContain("test_dataset");
    });

    test("delete dataset with multiple tables", async () => {
      const bigQuery = getBigQueryClient();

      await bigQuery.dataset("test_dataset").createTable("test_table_1", {
        schema: "id INT64, name STRING",
        location: "US",
      });
      await bigQuery.dataset("test_dataset").createTable("test_table_2", {
        schema: "user_id INT64, email STRING",
        location: "US",
      });

      await bigQuery.dataset("test_dataset").delete();

      const [datasets] = await bigQuery.getDatasets();
      const datasetIds = datasets.map((ds) => ds.id);
      expect(datasetIds).not.toContain("test_dataset");
    });
  });
});
