import { getBigQueryClient } from "tests/utils";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

describe("tabledata.insertAll", () => {
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

  describe("basic data insertion", () => {
    beforeEach(async () => {
      const bigQuery = getBigQueryClient();
      await bigQuery.createDataset("test_dataset");
      await bigQuery.dataset("test_dataset").createTable("test_table", {
        schema: "id INT64, name STRING, age INT64",
        location: "US",
      });
    });

    test("insert single row", async () => {
      const bigQuery = getBigQueryClient();
      const table = bigQuery.dataset("test_dataset").table("test_table");

      // Insert data
      await table.insert([{ id: 1, name: "Alice", age: 30 }]);

      // Query to verify
      const [rows] = await bigQuery.query({
        query: "SELECT * FROM `test_dataset.test_table` ORDER BY id",
      });

      expect(rows).toEqual([{ id: 1, name: "Alice", age: 30 }]);
    });

    test("insert multiple rows", async () => {
      const bigQuery = getBigQueryClient();
      const table = bigQuery.dataset("test_dataset").table("test_table");

      // Insert data
      await table.insert([
        { id: 1, name: "Alice", age: 30 },
        { id: 2, name: "Bob", age: 25 },
        { id: 3, name: "Charlie", age: 35 },
      ]);

      // Query to verify
      const [rows] = await bigQuery.query({
        query: "SELECT * FROM `test_dataset.test_table` ORDER BY id",
      });

      expect(rows).toEqual([
        { id: 1, name: "Alice", age: 30 },
        { id: 2, name: "Bob", age: 25 },
        { id: 3, name: "Charlie", age: 35 },
      ]);
    });

    test("insert row with null values", async () => {
      const bigQuery = getBigQueryClient();
      const table = bigQuery.dataset("test_dataset").table("test_table");

      // Insert data with null
      await table.insert([{ id: 1, name: "Alice", age: null }]);

      // Query to verify
      const [rows] = await bigQuery.query({
        query: "SELECT * FROM `test_dataset.test_table`",
      });

      expect(rows).toEqual([{ id: 1, name: "Alice", age: null }]);
    });

    test("insert row with missing optional fields", async () => {
      const bigQuery = getBigQueryClient();
      const table = bigQuery.dataset("test_dataset").table("test_table");

      // Insert data with missing field (age)
      await table.insert([{ id: 1, name: "Alice" }]);

      // Query to verify
      const [rows] = await bigQuery.query({
        query: "SELECT * FROM `test_dataset.test_table`",
      });

      expect(rows).toEqual([{ id: 1, name: "Alice", age: null }]);
    });
  });

  describe("complex data types", () => {
    test("insert data with ARRAY type", async () => {
      const bigQuery = getBigQueryClient();
      await bigQuery.createDataset("test_dataset");
      await bigQuery.dataset("test_dataset").createTable("test_table_array", {
        schema: "id INT64, tags ARRAY<STRING>",
        location: "US",
      });

      const table = bigQuery.dataset("test_dataset").table("test_table_array");

      // Insert data with array
      await table.insert([
        { id: 1, tags: ["tag1", "tag2", "tag3"] },
        { id: 2, tags: ["tag4"] },
      ]);

      // Query to verify
      const [rows] = await bigQuery.query({
        query: "SELECT * FROM `test_dataset.test_table_array` ORDER BY id",
      });

      expect(rows).toEqual([
        { id: 1, tags: ["tag1", "tag2", "tag3"] },
        { id: 2, tags: ["tag4"] },
      ]);
    });

    test("insert data with STRUCT type", async () => {
      const bigQuery = getBigQueryClient();
      await bigQuery.createDataset("test_dataset");
      await bigQuery.dataset("test_dataset").createTable("test_table_struct", {
        schema: "id INT64, address STRUCT<city STRING, zipcode INT64>",
        location: "US",
      });

      const table = bigQuery.dataset("test_dataset").table("test_table_struct");

      // Insert data with struct
      await table.insert([
        { id: 1, address: { city: "Tokyo", zipcode: 1000001 } },
        { id: 2, address: { city: "Osaka", zipcode: 5400001 } },
      ]);

      // Query to verify
      const [rows] = await bigQuery.query({
        query: "SELECT * FROM `test_dataset.test_table_struct` ORDER BY id",
      });

      expect(rows).toEqual([
        { id: 1, address: { city: "Tokyo", zipcode: 1000001 } },
        { id: 2, address: { city: "Osaka", zipcode: 5400001 } },
      ]);
    });
  });

  describe("error cases", () => {
    beforeEach(async () => {
      const bigQuery = getBigQueryClient();
      await bigQuery.createDataset("test_dataset");
      await bigQuery.dataset("test_dataset").createTable("test_table", {
        schema: "id INT64, name STRING",
        location: "US",
      });
    });

    test("insert to non-existent table should return error", async () => {
      const bigQuery = getBigQueryClient();
      const table = bigQuery
        .dataset("test_dataset")
        .table("non_existent_table");

      await expect(table.insert([{ id: 1, name: "Alice" }])).rejects.toThrow();
    });

    test("insert to non-existent dataset should return error", async () => {
      const bigQuery = getBigQueryClient();
      const table = bigQuery
        .dataset("non_existent_dataset")
        .table("test_table");

      await expect(table.insert([{ id: 1, name: "Alice" }])).rejects.toThrow();
    });
  });
});
