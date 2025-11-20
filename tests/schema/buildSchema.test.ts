import dedent from "dedent";
import { getBigQueryClient } from "tests/utils";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

describe("buildSchema - type inference", () => {
  const bigQuery = getBigQueryClient();

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

  describe("SELECT clause type inference - literals", () => {
    test("should infer INTEGER from integer literal", async () => {
      const response = await fetch(
        "http://localhost:9050/bigquery/v2/projects/dummy-project/queries",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ query: "SELECT 123 AS num" }),
        }
      );
      const result = await response.json();

      expect(result.schema.fields).toEqual([
        { name: "num", type: "INTEGER", mode: "NULLABLE" },
      ]);
    });

    test("should infer FLOAT from float literal", async () => {
      const response = await fetch(
        "http://localhost:9050/bigquery/v2/projects/dummy-project/queries",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ query: "SELECT 123.45 AS value" }),
        }
      );
      const result = await response.json();

      expect(result.schema.fields).toEqual([
        { name: "value", type: "FLOAT", mode: "NULLABLE" },
      ]);
    });

    test("should infer STRING from string literal", async () => {
      const response = await fetch(
        "http://localhost:9050/bigquery/v2/projects/dummy-project/queries",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ query: "SELECT 'hello' AS greeting" }),
        }
      );
      const result = await response.json();

      expect(result.schema.fields).toEqual([
        { name: "greeting", type: "STRING", mode: "NULLABLE" },
      ]);
    });

    test("should infer BOOL from boolean literal", async () => {
      const response = await fetch(
        "http://localhost:9050/bigquery/v2/projects/dummy-project/queries",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ query: "SELECT true AS flag" }),
        }
      );
      const result = await response.json();

      // 現状はINTEGERと推論されているが、BOOLと推論されるべき
      expect(result.schema.fields).toEqual([
        { name: "flag", type: "BOOL", mode: "NULLABLE" },
      ]);
    });
  });

  describe("SELECT clause type inference - functions", () => {
    test("should infer TIMESTAMP from TIMESTAMP function", async () => {
      const response = await fetch(
        "http://localhost:9050/bigquery/v2/projects/dummy-project/queries",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            query: "SELECT TIMESTAMP('2024-01-01 00:00:00') AS ts",
          }),
        }
      );
      const result = await response.json();

      expect(result.schema.fields).toEqual([
        { name: "ts", type: "TIMESTAMP", mode: "NULLABLE" },
      ]);
    });

    test("should infer DATE from DATE function", async () => {
      const response = await fetch(
        "http://localhost:9050/bigquery/v2/projects/dummy-project/queries",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            query: "SELECT DATE(2024, 1, 1) AS date_value",
          }),
        }
      );
      const result = await response.json();

      expect(result.schema.fields).toEqual([
        { name: "date_value", type: "DATE", mode: "NULLABLE" },
      ]);
    });

    test("should infer STRING from FORMAT_TIMESTAMP function", async () => {
      const response = await fetch(
        "http://localhost:9050/bigquery/v2/projects/dummy-project/queries",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            query:
              "SELECT FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP '2024-01-01') AS formatted",
          }),
        }
      );
      const result = await response.json();

      expect(result.schema.fields).toEqual([
        { name: "formatted", type: "STRING", mode: "NULLABLE" },
      ]);
    });

    test("should infer TIMESTAMP from TIMESTAMP_TRUNC function", async () => {
      const response = await fetch(
        "http://localhost:9050/bigquery/v2/projects/dummy-project/queries",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            query:
              "SELECT TIMESTAMP_TRUNC(TIMESTAMP '2024-01-01 10:30:45', DAY) AS truncated",
          }),
        }
      );
      const result = await response.json();

      expect(result.schema.fields).toEqual([
        { name: "truncated", type: "TIMESTAMP", mode: "NULLABLE" },
      ]);
    });
  });

  describe("SELECT clause type inference - CAST", () => {
    test("should infer STRING from CAST AS STRING", async () => {
      const response = await fetch(
        "http://localhost:9050/bigquery/v2/projects/dummy-project/queries",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            query: "SELECT CAST(123 AS STRING) AS text_value",
          }),
        }
      );
      const result = await response.json();

      expect(result.schema.fields).toEqual([
        { name: "text_value", type: "STRING", mode: "NULLABLE" },
      ]);
    });

    test("should infer INT64 from CAST AS INT64", async () => {
      const response = await fetch(
        "http://localhost:9050/bigquery/v2/projects/dummy-project/queries",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            query: "SELECT CAST('123' AS INT64) AS num_value",
          }),
        }
      );
      const result = await response.json();

      // INT64とINTEGERは同じものとして扱う
      expect(result.schema.fields).toEqual([
        { name: "num_value", type: "INTEGER", mode: "NULLABLE" },
      ]);
    });

    test("should infer FLOAT64 from CAST AS FLOAT64", async () => {
      const response = await fetch(
        "http://localhost:9050/bigquery/v2/projects/dummy-project/queries",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            query: "SELECT CAST('123.45' AS FLOAT64) AS float_value",
          }),
        }
      );
      const result = await response.json();

      // FLOAT64とFLOATは同じものとして扱う
      expect(result.schema.fields).toEqual([
        { name: "float_value", type: "FLOAT", mode: "NULLABLE" },
      ]);
    });
  });

  describe("SELECT clause type inference - expressions", () => {
    test("should infer INTEGER from arithmetic expression with integers", async () => {
      const response = await fetch(
        "http://localhost:9050/bigquery/v2/projects/dummy-project/queries",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            query: "SELECT 10 + 20 AS sum",
          }),
        }
      );
      const result = await response.json();

      expect(result.schema.fields).toEqual([
        { name: "sum", type: "INTEGER", mode: "NULLABLE" },
      ]);
    });

    test("should infer FLOAT from arithmetic expression with floats", async () => {
      const response = await fetch(
        "http://localhost:9050/bigquery/v2/projects/dummy-project/queries",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            query: "SELECT 10.5 + 20.3 AS sum",
          }),
        }
      );
      const result = await response.json();

      expect(result.schema.fields).toEqual([
        { name: "sum", type: "FLOAT", mode: "NULLABLE" },
      ]);
    });

    test("should infer STRING from CONCAT expression", async () => {
      const response = await fetch(
        "http://localhost:9050/bigquery/v2/projects/dummy-project/queries",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            query: "SELECT CONCAT('hello', ' ', 'world') AS text",
          }),
        }
      );
      const result = await response.json();

      expect(result.schema.fields).toEqual([
        { name: "text", type: "STRING", mode: "NULLABLE" },
      ]);
    });
  });

  describe("FROM clause type inference", () => {
    test("should inherit type from table column", async () => {
      // まずテーブルを作成
      const dataset = bigQuery.dataset("test_dataset");
      await dataset.create();
      const table = dataset.table("test_table");
      await table.create({
        schema: {
          fields: [
            { name: "id", type: "INT64", mode: "REQUIRED" },
            { name: "name", type: "STRING", mode: "NULLABLE" },
            { name: "created_at", type: "TIMESTAMP", mode: "NULLABLE" },
          ],
        },
      });

      // データを挿入
      await table.insert([
        {
          id: "1",
          name: "Alice",
          created_at: bigQuery.timestamp("2024-01-01 00:00:00"),
        },
      ]);

      // クエリを実行してスキーマを確認
      const response = await fetch(
        "http://localhost:9050/bigquery/v2/projects/dummy-project/queries",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            query: "SELECT id, name, created_at FROM `test_dataset.test_table`",
          }),
        }
      );
      const result = await response.json();

      expect(result.schema.fields).toEqual([
        { name: "id", type: "INT64", mode: "REQUIRED" },
        { name: "name", type: "STRING", mode: "NULLABLE" },
        { name: "created_at", type: "TIMESTAMP", mode: "NULLABLE" },
      ]);
    });

    test("should inherit type from subquery", async () => {
      const response = await fetch(
        "http://localhost:9050/bigquery/v2/projects/dummy-project/queries",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            query: dedent`
              SELECT num, text FROM (
                SELECT 123 AS num, 'hello' AS text
              )
            `,
          }),
        }
      );
      const result = await response.json();

      expect(result.schema.fields).toEqual([
        { name: "num", type: "INTEGER", mode: "NULLABLE" },
        { name: "text", type: "STRING", mode: "NULLABLE" },
      ]);
    });

    test("should inherit type from CTE", async () => {
      const response = await fetch(
        "http://localhost:9050/bigquery/v2/projects/dummy-project/queries",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            query: dedent`
              WITH test_cte AS (
                SELECT 123 AS num, 'hello' AS text
              )
              SELECT num, text FROM test_cte
            `,
          }),
        }
      );
      const result = await response.json();

      expect(result.schema.fields).toEqual([
        { name: "num", type: "INTEGER", mode: "NULLABLE" },
        { name: "text", type: "STRING", mode: "NULLABLE" },
      ]);
    });
  });

  describe("Complex type inference scenarios", () => {
    test("should prioritize CAST over value type", async () => {
      const response = await fetch(
        "http://localhost:9050/bigquery/v2/projects/dummy-project/queries",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            query: dedent`
              WITH test AS (
                SELECT 123 AS num
              )
              SELECT CAST(num AS STRING) AS text FROM test
            `,
          }),
        }
      );
      const result = await response.json();

      expect(result.schema.fields).toEqual([
        { name: "text", type: "STRING", mode: "NULLABLE" },
      ]);
    });

    test("should handle CASE expression with consistent types", async () => {
      const response = await fetch(
        "http://localhost:9050/bigquery/v2/projects/dummy-project/queries",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            query: dedent`
              SELECT
                CASE
                  WHEN 1 > 0 THEN 'yes'
                  ELSE 'no'
                END AS result
            `,
          }),
        }
      );
      const result = await response.json();

      expect(result.schema.fields).toEqual([
        { name: "result", type: "STRING", mode: "NULLABLE" },
      ]);
    });

    test("should handle array type correctly", async () => {
      const response = await fetch(
        "http://localhost:9050/bigquery/v2/projects/dummy-project/queries",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            query: dedent`
              WITH data AS (
                SELECT [1, 2, 3] AS numbers
              )
              SELECT numbers FROM data
            `,
          }),
        }
      );
      const result = await response.json();

      // 配列型はREPEATEDモードで推論されるべき
      // 要素が整数なので型はINTEGERになる
      expect(result.schema.fields).toEqual([
        { name: "numbers", type: "INTEGER", mode: "REPEATED" },
      ]);
    });
  });

  describe("Fallback to value-based inference", () => {
    test("should fallback to value type when AST analysis fails", async () => {
      const response = await fetch(
        "http://localhost:9050/bigquery/v2/projects/dummy-project/queries",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            query: dedent`
              WITH data AS (
                SELECT 1 AS a, 2 AS b
              )
              SELECT a + b AS sum FROM data
            `,
          }),
        }
      );
      const result = await response.json();

      // 演算の結果はINTEGERと推論されるべき
      expect(result.schema.fields).toEqual([
        { name: "sum", type: "INTEGER", mode: "NULLABLE" },
      ]);
    });
  });
});
