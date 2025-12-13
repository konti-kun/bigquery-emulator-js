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

  describe("date types", () => {
    test("insert DATE data - YYYY-MM-DD string", async () => {
      const bigQuery = getBigQueryClient();
      await bigQuery.createDataset("test_dataset");
      await bigQuery.dataset("test_dataset").createTable("test_table_date", {
        schema: "id INT64, birth_date DATE",
        location: "US",
      });

      const table = bigQuery.dataset("test_dataset").table("test_table_date");

      // Insert date data
      await table.insert([
        { id: 1, birth_date: "2023-12-25" },
        { id: 2, birth_date: "1990-01-01" },
      ]);

      // Query to verify - should be stored as YYYY-MM-DD
      const [rows] = await bigQuery.query({
        query: "SELECT * FROM `test_dataset.test_table_date` ORDER BY id",
      });

      expect(rows).toEqual([
        { id: 1, birth_date: bigQuery.date("2023-12-25") },
        { id: 2, birth_date: bigQuery.date("1990-01-01") },
      ]);
    });

    test("insert DATE data - Date object", async () => {
      const bigQuery = getBigQueryClient();
      await bigQuery.createDataset("test_dataset");
      await bigQuery.dataset("test_dataset").createTable("test_table_date2", {
        schema: "id INT64, event_date DATE",
        location: "US",
      });

      const table = bigQuery.dataset("test_dataset").table("test_table_date2");

      // Insert date data as Date object
      const testDate = new Date("2024-06-15T00:00:00Z");
      await table.insert([{ id: 1, event_date: testDate }]);

      // Query to verify
      const [rows] = await bigQuery.query({
        query: "SELECT * FROM `test_dataset.test_table_date2`",
      });

      expect(rows).toEqual([{ id: 1, event_date: bigQuery.date("2024-06-15") }]);
    });

    test("insert DATE data with null value", async () => {
      const bigQuery = getBigQueryClient();
      await bigQuery.createDataset("test_dataset");
      await bigQuery.dataset("test_dataset").createTable("test_table_date_null", {
        schema: "id INT64, event_date DATE",
        location: "US",
      });

      const table = bigQuery
        .dataset("test_dataset")
        .table("test_table_date_null");

      // Insert with null date
      await table.insert([{ id: 1, event_date: null }]);

      // Query to verify
      const [rows] = await bigQuery.query({
        query: "SELECT * FROM `test_dataset.test_table_date_null`",
      });

      expect(rows).toEqual([{ id: 1, event_date: null }]);
    });
  });

  describe("timestamp and datetime types", () => {
    test("insert TIMESTAMP data - ISO8601 string with Z", async () => {
      const bigQuery = getBigQueryClient();
      await bigQuery.createDataset("test_dataset");
      await bigQuery.dataset("test_dataset").createTable("test_table_ts", {
        schema: "id INT64, created_at TIMESTAMP",
        location: "US",
      });

      const table = bigQuery.dataset("test_dataset").table("test_table_ts");

      // Insert timestamp data with ISO8601 format
      await table.insert([
        { id: 1, created_at: "2023-12-25T10:30:00Z" },
        { id: 2, created_at: "2023-12-25T15:45:30.123Z" },
      ]);

      // Query to verify - should be stored as UTC ISO8601
      const [rows] = await bigQuery.query({
        query: "SELECT * FROM `test_dataset.test_table_ts` ORDER BY id",
      });

      expect(rows).toEqual([
        { id: 1, created_at: bigQuery.timestamp("2023-12-25T10:30:00Z") },
        { id: 2, created_at: bigQuery.timestamp("2023-12-25T15:45:30.123Z") },
      ]);
    });

    test("insert TIMESTAMP data - BigQuery format string", async () => {
      const bigQuery = getBigQueryClient();
      await bigQuery.createDataset("test_dataset");
      await bigQuery.dataset("test_dataset").createTable("test_table_ts2", {
        schema: "id INT64, created_at TIMESTAMP",
        location: "US",
      });

      const table = bigQuery.dataset("test_dataset").table("test_table_ts2");

      // Insert timestamp data with BigQuery format (space-separated)
      await table.insert([{ id: 1, created_at: "2023-12-25 10:30:00" }]);

      // Query to verify - should be stored as UTC ISO8601
      const [rows] = await bigQuery.query({
        query: "SELECT * FROM `test_dataset.test_table_ts2` ORDER BY id",
      });

      expect(rows).toEqual([
        { id: 1, created_at: bigQuery.timestamp("2023-12-25T10:30:00.000Z") },
      ]);
    });

    test("insert TIMESTAMP data - Unix timestamp number", async () => {
      const bigQuery = getBigQueryClient();
      await bigQuery.createDataset("test_dataset");
      await bigQuery.dataset("test_dataset").createTable("test_table_ts3", {
        schema: "id INT64, created_at TIMESTAMP",
        location: "US",
      });

      const table = bigQuery.dataset("test_dataset").table("test_table_ts3");

      // Insert timestamp data as Unix timestamp (seconds since epoch)
      // 1703500200 = 2023-12-25T10:30:00Z
      await table.insert([{ id: 1, created_at: 1703500200 }]);

      // Query to verify - should be stored as UTC ISO8601
      const [rows] = await bigQuery.query({
        query: "SELECT * FROM `test_dataset.test_table_ts3` ORDER BY id",
      });

      expect(rows).toEqual([
        { id: 1, created_at: bigQuery.timestamp("2023-12-25T10:30:00.000Z") },
      ]);
    });

    test("insert DATETIME data - ISO8601 string without timezone", async () => {
      const bigQuery = getBigQueryClient();
      await bigQuery.createDataset("test_dataset");
      await bigQuery.dataset("test_dataset").createTable("test_table_dt", {
        schema: "id INT64, event_time DATETIME",
        location: "US",
      });

      const table = bigQuery.dataset("test_dataset").table("test_table_dt");

      // Insert datetime data
      await table.insert([
        { id: 1, event_time: "2023-12-25T10:30:00" },
        { id: 2, event_time: "2023-12-25T15:45:30.123" },
      ]);

      // Query to verify - should be stored without timezone
      const [rows] = await bigQuery.query({
        query: "SELECT * FROM `test_dataset.test_table_dt` ORDER BY id",
      });

      expect(rows).toEqual([
        { id: 1, event_time: bigQuery.datetime("2023-12-25T10:30:00") },
        { id: 2, event_time: bigQuery.datetime("2023-12-25T15:45:30.123") },
      ]);
    });

    test("insert DATETIME data - BigQuery format string", async () => {
      const bigQuery = getBigQueryClient();
      await bigQuery.createDataset("test_dataset");
      await bigQuery.dataset("test_dataset").createTable("test_table_dt2", {
        schema: "id INT64, event_time DATETIME",
        location: "US",
      });

      const table = bigQuery.dataset("test_dataset").table("test_table_dt2");

      // Insert datetime data with BigQuery format (space-separated)
      await table.insert([{ id: 1, event_time: "2023-12-25 10:30:00" }]);

      // Query to verify - should be stored without timezone
      const [rows] = await bigQuery.query({
        query: "SELECT * FROM `test_dataset.test_table_dt2` ORDER BY id",
      });

      expect(rows).toEqual([
        { id: 1, event_time: bigQuery.datetime("2023-12-25T10:30:00") },
      ]);
    });

    test("insert TIMESTAMP with null value", async () => {
      const bigQuery = getBigQueryClient();
      await bigQuery.createDataset("test_dataset");
      await bigQuery.dataset("test_dataset").createTable("test_table_ts_null", {
        schema: "id INT64, created_at TIMESTAMP",
        location: "US",
      });

      const table = bigQuery
        .dataset("test_dataset")
        .table("test_table_ts_null");

      // Insert with null timestamp
      await table.insert([{ id: 1, created_at: null }]);

      // Query to verify
      const [rows] = await bigQuery.query({
        query: "SELECT * FROM `test_dataset.test_table_ts_null`",
      });

      expect(rows).toEqual([{ id: 1, created_at: null }]);
    });

    test("insert DATETIME with null value", async () => {
      const bigQuery = getBigQueryClient();
      await bigQuery.createDataset("test_dataset");
      await bigQuery.dataset("test_dataset").createTable("test_table_dt_null", {
        schema: "id INT64, event_time DATETIME",
        location: "US",
      });

      const table = bigQuery
        .dataset("test_dataset")
        .table("test_table_dt_null");

      // Insert with null datetime
      await table.insert([{ id: 1, event_time: null }]);

      // Query to verify
      const [rows] = await bigQuery.query({
        query: "SELECT * FROM `test_dataset.test_table_dt_null`",
      });

      expect(rows).toEqual([{ id: 1, event_time: null }]);
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
