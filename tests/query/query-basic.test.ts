import dedent from "dedent";
import { getBigQueryClient } from "tests/utils";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

describe("query - basic", () => {
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

  test("run simple query", async () => {
    const [response] = await bigQuery.query("SELECT 1");
    expect(response).toEqual([{ f0_: 1 }]);
    const [response2] = await bigQuery.query("SELECT 'hello' as greeting");
    expect(response2).toEqual([{ greeting: "hello" }]);
    const [response3] = await bigQuery.query(
      "SELECT 1, 1.5 as value, 'text' as text"
    );
    expect(response3).toEqual([{ value: 1.5, text: "text", f0_: 1 }]);
    const [response4] = await bigQuery.query(
      "WITH nums AS (SELECT 1, 2, 3) SELECT * FROM nums"
    );
    expect(response4).toEqual([{ f0_: 1, f1_: 2, f2_: 3 }]);
  });

  test("ROW_NUMBER() OVER()", async () => {
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

    const [response] = await bigQuery.query(
      dedent`
      WITH TestData AS (
        SELECT 'A' AS category UNION ALL
        SELECT 'B' UNION ALL
        SELECT 'A' UNION ALL
        SELECT 'B' UNION ALL
        SELECT 'C'
      )
      SELECT
        category,
        ROW_NUMBER() OVER (ORDER BY category) AS row_num
      FROM TestData
      ORDER BY category, row_num`
    );
    expect(response).toEqual([
      { category: "A", row_num: 1 },
      { category: "A", row_num: 2 },
      { category: "B", row_num: 3 },
      { category: "B", row_num: 4 },
      { category: "C", row_num: 5 },
    ]);
  });

  test("run query with array columns", async () => {
    const [response1] = await bigQuery.query(
      dedent`
      WITH
      Sequences AS (
        SELECT [0, 1, 2] AS some_numbers UNION ALL
        SELECT [2, 4, 8, 16, 32] UNION ALL
        SELECT [5, 10]
      )
      SELECT * FROM Sequences`
    );
    expect(response1).toEqual([
      { some_numbers: "[0,1,2]" },
      { some_numbers: "[2,4,8,16,32]" },
      { some_numbers: "[5,10]" },
    ]);
  });

  test("run query with CAST AS STRING", async () => {
    const [response1] = await bigQuery.query(
      "SELECT CAST(123 AS STRING) AS text_value"
    );
    expect(response1).toEqual([{ text_value: "123" }]);
  });

  test("run query with CASE and CAST AS STRING", async () => {
    const [response1] = await bigQuery.query(
      dedent`
      WITH TestData AS (
        SELECT 1 AS column1 UNION ALL
        SELECT 2 UNION ALL
        SELECT NULL
      )
      SELECT
        CASE
          WHEN column1 IS NULL THEN 'N/A'
          ELSE CAST(column1 AS STRING)
        END AS item,
        COUNT(*) AS count
      FROM TestData
      GROUP BY column1
      ORDER BY item`
    );
    expect(response1).toEqual([
      { item: "1", count: 1 },
      { item: "2", count: 1 },
      { item: "N/A", count: 1 },
    ]);
  });
});
