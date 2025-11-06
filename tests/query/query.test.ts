import dedent from "dedent";
import { getBigQueryClient } from "tests/utils";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

describe("query", () => {
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

  test("run query with FORMAT_TIMESTAMP", async () => {
    const [response1] = await bigQuery.query(
      "SELECT FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP '2023-12-25 10:30:00') AS formatted_date"
    );
    expect(response1).toEqual([{ formatted_date: "2023-12-25" }]);
  });

  test("run query with FORMAT_TIMESTAMP with time", async () => {
    const [response2] = await bigQuery.query(
      "SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP '2023-12-25 10:30:45') AS formatted_datetime"
    );
    expect(response2).toEqual([{ formatted_datetime: "2023-12-25 10:30:45" }]);
  });

  test("run query with FORMAT_TIMESTAMP with timezone", async () => {
    const [response3] = await bigQuery.query(
      "SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP '2023-12-25 10:30:00', 'Asia/Tokyo') AS formatted_with_tz"
    );
    // UTC 10:30:00 = JST 19:30:00
    expect(response3).toEqual([{ formatted_with_tz: "2023-12-25 19:30:00" }]);
  });

  test("run query with TIMESTAMP_TRUNC to DAY", async () => {
    const query =
      "SELECT TIMESTAMP_TRUNC(TIMESTAMP '2023-12-25 10:30:45', DAY) AS truncated_day";

    // Make direct HTTP request to see the full response including errors
    const response = await fetch(
      "http://localhost:9050/bigquery/v2/projects/dummy-project/queries",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query }),
      }
    );
    const result = await response.json();

    if (result.errors && result.errors.length > 0) {
      console.log("API Errors:", JSON.stringify(result.errors, null, 2));
    }

    const [response1] = await bigQuery.query(query);
    expect(response1).toEqual([{ truncated_day: "2023-12-25 00:00:00" }]);
  });

  test("run query with TIMESTAMP_TRUNC to HOUR", async () => {
    const [response2] = await bigQuery.query(
      "SELECT TIMESTAMP_TRUNC(TIMESTAMP '2023-12-25 10:30:45', HOUR) AS truncated_hour"
    );
    expect(response2).toEqual([{ truncated_hour: "2023-12-25 10:00:00" }]);
  });

  test("run query with TIMESTAMP_TRUNC to MINUTE", async () => {
    const [response3] = await bigQuery.query(
      "SELECT TIMESTAMP_TRUNC(TIMESTAMP '2023-12-25 10:30:45', MINUTE) AS truncated_minute"
    );
    expect(response3).toEqual([{ truncated_minute: "2023-12-25 10:30:00" }]);
  });

  test("run query with TIMESTAMP_TRUNC to MONTH", async () => {
    const [response4] = await bigQuery.query(
      "SELECT TIMESTAMP_TRUNC(TIMESTAMP '2023-12-25 10:30:45', MONTH) AS truncated_month"
    );
    expect(response4).toEqual([{ truncated_month: "2023-12-01 00:00:00" }]);
  });

  test("run query with TIMESTAMP_TRUNC to YEAR", async () => {
    const [response5] = await bigQuery.query(
      "SELECT TIMESTAMP_TRUNC(TIMESTAMP '2023-12-25 10:30:45', YEAR) AS truncated_year"
    );
    expect(response5).toEqual([{ truncated_year: "2023-01-01 00:00:00" }]);
  });

  describe("TIMESTAMP function", () => {
    test("run query with TIMESTAMP function from string", async () => {
      const [response1] = await bigQuery.query(
        "SELECT TIMESTAMP('2023-12-25 10:30:45+00:00') AS ts"
      );
      expect(response1).toEqual([
        { ts: bigQuery.timestamp("2023-12-25 10:30:45+00:00") },
      ]);
    });

    test("run query with TIMESTAMP function from date string", async () => {
      const [response2] = await bigQuery.query(
        "SELECT TIMESTAMP('2023-12-25') AS ts"
      );
      expect(response2).toEqual([
        { ts: bigQuery.timestamp("2023-12-25 00:00:00+00:00") },
      ]);
    });

    test("run query with TIMESTAMP function with timezone", async () => {
      const [response3] = await bigQuery.query(
        "SELECT TIMESTAMP('2023-12-25 10:30:45', 'Asia/Tokyo') AS ts"
      );
      expect(response3).toEqual([
        { ts: bigQuery.timestamp("2023-12-25 01:30:45+00:00") },
      ]);
    });

    test("run query with TIMESTAMP function with ISO 8601 format", async () => {
      const [response4] = await bigQuery.query(
        "SELECT TIMESTAMP('2023-12-25T10:30:45Z') AS ts"
      );
      expect(response4).toEqual([
        { ts: bigQuery.timestamp("2023-12-25T10:30:45Z") },
      ]);
    });
  });

  describe("UNNEST function", () => {
    test("run simple UNNEST with array literal", async () => {
      const [response1] = await bigQuery.query(
        "SELECT * FROM UNNEST([1, 2, 3]) AS num"
      );
      expect(response1).toEqual([{ num: 1 }, { num: 2 }, { num: 3 }]);
    });

    test("run UNNEST with WITH OFFSET", async () => {
      const [response2] = await bigQuery.query(
        "SELECT num, offset FROM UNNEST([10, 20, 30]) AS num WITH OFFSET"
      );
      expect(response2).toEqual([
        { num: 10, offset: 0 },
        { num: 20, offset: 1 },
        { num: 30, offset: 2 },
      ]);
    });

    test("run UNNEST with string array", async () => {
      const [response3] = await bigQuery.query(
        "SELECT * FROM UNNEST(['a', 'b', 'c']) AS letter"
      );
      expect(response3).toEqual([
        { letter: "a" },
        { letter: "b" },
        { letter: "c" },
      ]);
    });

    test("run UNNEST with table column", async () => {
      const [response4] = await bigQuery.query(
        dedent`
        WITH TestData AS (
          SELECT [1, 2, 3] AS nums UNION ALL
          SELECT [4, 5]
        )
        SELECT num FROM TestData, UNNEST(nums) AS num`
      );
      expect(response4).toEqual([
        { num: 1 },
        { num: 2 },
        { num: 3 },
        { num: 4 },
        { num: 5 },
      ]);
    });

    test("run UNNEST with multiple arrays", async () => {
      const [response5] = await bigQuery.query(
        "SELECT a, b FROM UNNEST([1, 2]) AS a, UNNEST([3, 4]) AS b"
      );
      expect(response5).toEqual([
        { a: 1, b: 3 },
        { a: 1, b: 4 },
        { a: 2, b: 3 },
        { a: 2, b: 4 },
      ]);
    });
    test("run UNNEST in where clause", async () => {
      const [response6] = await bigQuery.query(
        dedent`
        WITH TestData AS (
          SELECT 1 AS id, 10 AS num UNION ALL
          SELECT 2 AS id, 20 AS num UNION ALL
          SELECT 3 AS id, 30 AS num UNION ALL
          SELECT 4 AS id, 20 AS num
        )
        SELECT id, num FROM TestData
        WHERE num IN UNNEST([10, 20])`
      );
      expect(response6).toEqual([
        { id: 1, num: 10 },
        { id: 2, num: 20 },
        { id: 4, num: 20 },
      ]);
    });

    test("run UNNEST with array parameter in WHERE clause", async () => {
      const [response7] = await bigQuery.query({
        query: dedent`
        WITH TestData AS (
          SELECT 1 AS id, 10 AS num UNION ALL
          SELECT 2 AS id, 20 AS num UNION ALL
          SELECT 3 AS id, 30 AS num UNION ALL
          SELECT 4 AS id, 20 AS num
        )
        SELECT id, num FROM TestData
        WHERE num IN UNNEST(@nums)`,
        params: {
          nums: [10, 20],
        },
      });
      expect(response7).toEqual([
        { id: 1, num: 10 },
        { id: 2, num: 20 },
        { id: 4, num: 20 },
      ]);
    });

    test("run UNNEST with array parameter in FROM clause", async () => {
      const [response8] = await bigQuery.query({
        query: "SELECT * FROM UNNEST(@items) AS item",
        params: {
          items: ["apple", "banana", "cherry"],
        },
      });
      expect(response8).toEqual([
        { item: "apple" },
        { item: "banana" },
        { item: "cherry" },
      ]);
    });

    test("run UNNEST with array parameter and WITH OFFSET", async () => {
      const [response9] = await bigQuery.query({
        query: "SELECT item, offset FROM UNNEST(@items) AS item WITH OFFSET",
        params: {
          items: [100, 200, 300],
        },
      });
      expect(response9).toEqual([
        { item: 100, offset: 0 },
        { item: 200, offset: 1 },
        { item: 300, offset: 2 },
      ]);
    });
  });
});
