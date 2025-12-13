import { format } from "date-fns";
import { formatInTimeZone } from "date-fns-tz";
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
    expect(response1).toEqual([
      { truncated_day: bigQuery.timestamp("2023-12-25 00:00:00") },
    ]);
  });

  test("run query with TIMESTAMP_TRUNC to HOUR", async () => {
    const [response2] = await bigQuery.query(
      "SELECT TIMESTAMP_TRUNC(TIMESTAMP '2023-12-25 10:30:45', HOUR) AS truncated_hour"
    );
    expect(response2).toEqual([
      { truncated_hour: bigQuery.timestamp("2023-12-25 10:00:00") },
    ]);
  });

  test("run query with TIMESTAMP_TRUNC to MINUTE", async () => {
    const [response3] = await bigQuery.query(
      "SELECT TIMESTAMP_TRUNC(TIMESTAMP '2023-12-25 10:30:45', MINUTE) AS truncated_minute"
    );
    expect(response3).toEqual([
      { truncated_minute: bigQuery.timestamp("2023-12-25 10:30:00") },
    ]);
  });

  test("run query with TIMESTAMP_TRUNC to MONTH", async () => {
    const [response4] = await bigQuery.query(
      "SELECT TIMESTAMP_TRUNC(TIMESTAMP '2023-12-25 10:30:45', MONTH) AS truncated_month"
    );
    expect(response4).toEqual([
      { truncated_month: bigQuery.timestamp("2023-12-01 00:00:00") },
    ]);
  });

  test("run query with TIMESTAMP_TRUNC to YEAR", async () => {
    const [response5] = await bigQuery.query(
      "SELECT TIMESTAMP_TRUNC(TIMESTAMP '2023-12-25 10:30:45', YEAR) AS truncated_year"
    );
    expect(response5).toEqual([
      { truncated_year: bigQuery.timestamp("2023-01-01 00:00:00") },
    ]);
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

  describe("CURRENT_TIMESTAMP function", () => {
    test("run query with CURRENT_TIMESTAMP function", async () => {
      const beforeQuery = new Date();
      const [response1] = await bigQuery.query(
        "SELECT CURRENT_TIMESTAMP() AS current_ts"
      );
      const afterQuery = new Date();

      // CURRENT_TIMESTAMPの結果がクエリ実行前後の時刻の範囲内にあることを確認
      expect(response1).toHaveLength(1);
      expect(response1[0]).toHaveProperty("current_ts");

      const resultTimestamp = new Date(response1[0].current_ts);
      expect(resultTimestamp.getTime()).toBeGreaterThanOrEqual(
        beforeQuery.getTime() - 1000
      ); // 1秒の誤差を許容
      expect(resultTimestamp.getTime()).toBeLessThanOrEqual(
        afterQuery.getTime() + 1000
      );
    });

    test("run query with CURRENT_TIMESTAMP without parentheses", async () => {
      const beforeQuery = new Date();
      const [response2] = await bigQuery.query(
        "SELECT CURRENT_TIMESTAMP AS current_ts"
      );
      const afterQuery = new Date();

      // CURRENT_TIMESTAMPの結果がクエリ実行前後の時刻の範囲内にあることを確認
      expect(response2).toHaveLength(1);
      expect(response2[0]).toHaveProperty("current_ts");

      const resultTimestamp = new Date(response2[0].current_ts);
      expect(resultTimestamp.getTime()).toBeGreaterThanOrEqual(
        beforeQuery.getTime() - 1000
      );
      expect(resultTimestamp.getTime()).toBeLessThanOrEqual(
        afterQuery.getTime() + 1000
      );
    });
  });

  describe("DATE function", () => {
    test("run query with DATE function from year, month, day", async () => {
      const [response1] = await bigQuery.query(
        "SELECT DATE(2023, 12, 25) AS date_value"
      );
      expect(response1).toEqual([{ date_value: bigQuery.date("2023-12-25") }]);
    });

    test("run query with DATE function from timestamp", async () => {
      const [response2] = await bigQuery.query(
        "SELECT DATE(TIMESTAMP('2023-12-25 10:30:45+00:00')) AS date_value"
      );
      expect(response2).toEqual([{ date_value: bigQuery.date("2023-12-25") }]);
    });

    test("run query with DATE function from date string", async () => {
      const [response3] = await bigQuery.query(
        "SELECT DATE('2023-12-25') AS date_value"
      );
      expect(response3).toEqual([{ date_value: bigQuery.date("2023-12-25") }]);
    });

    test("run query with DATE function handling edge cases", async () => {
      const [response4] = await bigQuery.query(
        "SELECT DATE(2024, 2, 29) AS leap_year"
      );
      expect(response4).toEqual([{ leap_year: bigQuery.date("2024-02-29") }]);
    });

    test("run query with DATE function handling single digit month and day", async () => {
      const [response5] = await bigQuery.query(
        "SELECT DATE(2023, 1, 5) AS date_value"
      );
      expect(response5).toEqual([{ date_value: bigQuery.date("2023-01-05") }]);
    });
  });

  describe("CURRENT_DATE function", () => {
    test("run query with CURRENT_DATE function", async () => {
      const beforeQuery = new Date();
      const [response1] = await bigQuery.query("SELECT CURRENT_DATE() AS cd");
      const afterQuery = new Date();

      // CURRENT_DATEの結果がクエリ実行前後の日付の範囲内にあることを確認
      expect(response1).toHaveLength(1);
      expect(response1[0]).toHaveProperty("cd");

      // 結果が日付形式であることを確認
      const resultDate = response1[0].cd;

      // 現在の日付と一致することを確認(タイムゾーンを考慮)
      const resultParsed = new Date(resultDate.value);

      const beforeDate = new Date(
        beforeQuery.getFullYear(),
        beforeQuery.getMonth(),
        beforeQuery.getDate()
      );
      const afterDate = new Date(
        afterQuery.getFullYear(),
        afterQuery.getMonth(),
        afterQuery.getDate()
      );

      expect(resultParsed.getTime()).toBeGreaterThanOrEqual(
        beforeDate.getTime() - 86400000
      ); // 1日の誤差を許容
      expect(resultParsed.getTime()).toBeLessThanOrEqual(
        afterDate.getTime() + 86400000
      );
    });

    test("run query with CURRENT_DATE without parentheses", async () => {
      const [response2] = await bigQuery.query("SELECT CURRENT_DATE AS cd");

      // CURRENT_DATEの結果がクエリ実行前後の日付の範囲内にあることを確認
      expect(response2).toHaveLength(1);
      expect(response2[0]).toHaveProperty("cd");

      // 結果が日付形式であることを確認
      const resultDate = response2[0].cd;

      expect(resultDate).toEqual(
        bigQuery.date(format(new Date(), "yyyy-MM-dd"))
      );
    });

    test("run query with CURRENT_DATE calls with params", async () => {
      const [response3] = await bigQuery.query(
        "SELECT CURRENT_DATE('Asia/Tokyo') AS d1"
      );
      expect(response3).toHaveLength(1);
      expect(response3[0]).toHaveProperty("d1");
      // Asia/Tokyoのタイムゾーンで現在の日付を取得
      const tokyoDate = formatInTimeZone(
        new Date(),
        "Asia/Tokyo",
        "yyyy-MM-dd"
      );
      expect(response3[0].d1).toEqual(bigQuery.date(tokyoDate));
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

  describe("COUNT function", () => {
    test("run COUNT with DISTINCT", async () => {
      const [response1] = await bigQuery.query(
        dedent`
        WITH TestData AS (
          SELECT 'A' AS column1 UNION ALL
          SELECT 'B' UNION ALL
          SELECT 'A' UNION ALL
          SELECT 'C' UNION ALL
          SELECT 'B'
        )
        SELECT COUNT(DISTINCT column1) AS column1
        FROM TestData`
      );
      expect(response1).toEqual([{ column1: 3 }]);
    });

    test("run COUNT with DISTINCT and alias", async () => {
      const [response2] = await bigQuery.query(
        dedent`
        WITH TestData AS (
          SELECT 1 AS id UNION ALL
          SELECT 2 UNION ALL
          SELECT 1 UNION ALL
          SELECT 3 UNION ALL
          SELECT 2
        )
        SELECT COUNT(DISTINCT id) AS unique_count
        FROM TestData`
      );
      expect(response2).toEqual([{ unique_count: 3 }]);
    });

    test("run COUNT with DISTINCT and NULL values", async () => {
      const [response3] = await bigQuery.query(
        dedent`
        WITH TestData AS (
          SELECT 'X' AS val UNION ALL
          SELECT NULL UNION ALL
          SELECT 'X' UNION ALL
          SELECT 'Y' UNION ALL
          SELECT NULL
        )
        SELECT COUNT(DISTINCT val) AS distinct_count
        FROM TestData`
      );
      expect(response3).toEqual([{ distinct_count: 2 }]);
    });

    test("run COUNT with DISTINCT from actual table", async () => {
      const dataset = bigQuery.dataset("voices");
      await dataset.create();
      const table = dataset.table("analyze_AAAA");
      await table.create({
        schema: {
          fields: [
            { name: "column1", type: "STRING", mode: "NULLABLE" },
            { name: "column2", type: "INT64", mode: "NULLABLE" },
          ],
        },
      });

      await table.insert([
        { column1: "A", column2: 1 },
        { column1: "B", column2: 2 },
        { column1: "A", column2: 3 },
        { column1: "C", column2: 4 },
        { column1: "B", column2: 5 },
      ]);

      const [response4] = await bigQuery.query(
        "SELECT count(distinct column1) as column1 FROM voices.analyze_AAAA"
      );
      expect(response4).toEqual([{ column1: 3 }]);
    });
  });

  describe("COUNTIF function", () => {
    test("run simple COUNTIF with boolean condition", async () => {
      const [response1] = await bigQuery.query(
        dedent`
        WITH TestData AS (
          SELECT -5 AS x UNION ALL
          SELECT 3 UNION ALL
          SELECT -1 UNION ALL
          SELECT 7 UNION ALL
          SELECT 0 UNION ALL
          SELECT -2 UNION ALL
          SELECT 4
        )
        SELECT COUNTIF(x < 0) AS num_negative, COUNTIF(x > 0) AS num_positive
        FROM TestData`
      );
      expect(response1).toEqual([{ num_negative: 3, num_positive: 3 }]);
    });

    test("run COUNTIF with string comparison", async () => {
      const [response2] = await bigQuery.query(
        dedent`
        WITH TestData AS (
          SELECT 'story' AS type UNION ALL
          SELECT 'comment' UNION ALL
          SELECT 'story' UNION ALL
          SELECT 'poll' UNION ALL
          SELECT 'story'
        )
        SELECT
          COUNTIF(type = 'story') AS story_count,
          COUNTIF(type = 'comment') AS comment_count,
          COUNTIF(type = 'poll') AS poll_count
        FROM TestData`
      );
      expect(response2).toEqual([
        { story_count: 3, comment_count: 1, poll_count: 1 },
      ]);
    });

    test("run COUNTIF with complex condition", async () => {
      const [response3] = await bigQuery.query(
        dedent`
        WITH TestData AS (
          SELECT 1 AS id, 1500 AS amount, 'active' AS status UNION ALL
          SELECT 2, 500, 'active' UNION ALL
          SELECT 3, 2000, 'inactive' UNION ALL
          SELECT 4, 1200, 'active' UNION ALL
          SELECT 5, 300, 'inactive'
        )
        SELECT
          COUNTIF(amount > 1000 AND status = 'active') AS high_value_active
        FROM TestData`
      );
      expect(response3).toEqual([{ high_value_active: 2 }]);
    });

    test("run COUNTIF with NULL values", async () => {
      const [response4] = await bigQuery.query(
        dedent`
        WITH TestData AS (
          SELECT 1 AS value UNION ALL
          SELECT NULL UNION ALL
          SELECT 2 UNION ALL
          SELECT NULL UNION ALL
          SELECT 3
        )
        SELECT
          COUNTIF(value IS NOT NULL) AS non_null_count,
          COUNTIF(value IS NULL) AS null_count
        FROM TestData`
      );
      expect(response4).toEqual([{ non_null_count: 3, null_count: 2 }]);
    });
  });

  describe("query parameters with temporal types", () => {
    test("query with DATE parameter using types", async () => {
      const bigQuery = getBigQueryClient();
      const dataset = bigQuery.dataset("test_dataset");
      await dataset.create();
      const table = dataset.table("test_table");
      await table.create({
        schema: {
          fields: [
            { name: "id", type: "INT64", mode: "NULLABLE" },
            { name: "event_date", type: "DATE", mode: "NULLABLE" },
          ],
        },
      });

      await table.insert([
        { id: 1, event_date: "2024-01-15" },
        { id: 2, event_date: "2024-01-16" },
        { id: 3, event_date: "2024-01-17" },
      ]);

      const [response] = await bigQuery.createQueryJob({
        query:
          "SELECT * FROM test_dataset.test_table WHERE event_date = @target_date ORDER BY id",
        params: {
          target_date: bigQuery.date("2024-01-16"),
        },
        types: {
          target_date: "DATE",
        },
      });
      const [rows] = await response.getQueryResults();
      expect(rows).toEqual([{ id: 2, event_date: "2024-01-16" }]);
    });

    test("query with DATETIME parameter using types", async () => {
      const bigQuery = getBigQueryClient();
      const dataset = bigQuery.dataset("test_dataset");
      await dataset.create();
      const table = dataset.table("test_table");
      await table.create({
        schema: {
          fields: [
            { name: "id", type: "INT64", mode: "NULLABLE" },
            { name: "event_time", type: "DATETIME", mode: "NULLABLE" },
          ],
        },
      });

      await table.insert([
        { id: 1, event_time: "2024-01-15T10:30:00" },
        { id: 2, event_time: "2024-01-15T14:45:00" },
        { id: 3, event_time: "2024-01-16T09:00:00" },
      ]);

      const [response] = await bigQuery.query({
        query:
          "SELECT * FROM test_dataset.test_table WHERE event_time = @target_time ORDER BY id",
        params: {
          target_time: bigQuery.date("2024-01-15T14:45:00"),
        },
        types: {
          target_time: "DATETIME",
        },
      });

      expect(response).toEqual([{ id: 2, event_time: "2024-01-15T14:45:00" }]);
    });

    test("query with TIMESTAMP parameter using types", async () => {
      const bigQuery = getBigQueryClient();
      const dataset = bigQuery.dataset("test_dataset");
      await dataset.create();
      const table = dataset.table("test_table");
      await table.create({
        schema: {
          fields: [
            { name: "id", type: "INT64", mode: "NULLABLE" },
            { name: "created_at", type: "TIMESTAMP", mode: "NULLABLE" },
          ],
        },
      });

      await table.insert([
        { id: 1, created_at: "2024-01-15T10:30:00Z" },
        { id: 2, created_at: "2024-01-15T14:45:00Z" },
        { id: 3, created_at: "2024-01-16T09:00:00Z" },
      ]);

      const [response] = await bigQuery.query({
        query:
          "SELECT * FROM test_dataset.test_table WHERE created_at = @target_timestamp ORDER BY id",
        params: {
          target_timestamp: bigQuery.date("2024-01-15T14:45:00Z"),
        },
        types: {
          target_timestamp: "TIMESTAMP",
        },
      });

      expect(response).toEqual([
        { id: 2, created_at: "2024-01-15T14:45:00.000Z" },
      ]);
    });
  });

  describe("DATE type in WHERE clause", () => {
    beforeEach(async () => {
      // events.daily_logs テーブル
      const eventsDataset = bigQuery.dataset("events");
      await eventsDataset.create();
      const dailyLogsTable = eventsDataset.table("daily_logs");
      await dailyLogsTable.create({
        schema: {
          fields: [
            { name: "id", type: "INT64", mode: "NULLABLE" },
            { name: "event_date", type: "DATE", mode: "NULLABLE" },
            { name: "description", type: "STRING", mode: "NULLABLE" },
          ],
        },
      });
      await dailyLogsTable.insert([
        { id: 1, event_date: "2024-01-15", description: "Event 1" },
        { id: 2, event_date: "2024-01-16", description: "Event 2" },
        { id: 3, event_date: "2024-01-17", description: "Event 3" },
        { id: 4, event_date: "2024-01-15", description: "Event 4" },
      ]);

      // logs.activity テーブル
      const logsDataset = bigQuery.dataset("logs");
      await logsDataset.create();
      const activityTable = logsDataset.table("activity");
      await activityTable.create({
        schema: {
          fields: [
            { name: "id", type: "INT64", mode: "NULLABLE" },
            { name: "log_date", type: "DATE", mode: "NULLABLE" },
            { name: "action", type: "STRING", mode: "NULLABLE" },
          ],
        },
      });
      await activityTable.insert([
        { id: 1, log_date: "2024-03-01", action: "login" },
        { id: 2, log_date: "2024-03-02", action: "logout" },
        { id: 3, log_date: "2024-03-01", action: "view" },
      ]);

      // schedules.tasks テーブル
      const schedulesDataset = bigQuery.dataset("schedules");
      await schedulesDataset.create();
      const tasksTable = schedulesDataset.table("tasks");
      await tasksTable.create({
        schema: {
          fields: [
            { name: "task_id", type: "INT64", mode: "NULLABLE" },
            { name: "due_date", type: "DATE", mode: "NULLABLE" },
            { name: "title", type: "STRING", mode: "NULLABLE" },
          ],
        },
      });
      await tasksTable.insert([
        { task_id: 1, due_date: "2024-06-10", title: "Task A" },
        { task_id: 2, due_date: "2024-06-15", title: "Task B" },
        { task_id: 3, due_date: "2024-06-20", title: "Task C" },
      ]);

      // projects.milestones テーブル
      const projectsDataset = bigQuery.dataset("projects");
      await projectsDataset.create();
      const milestonesTable = projectsDataset.table("milestones");
      await milestonesTable.create({
        schema: {
          fields: [
            { name: "id", type: "INT64", mode: "NULLABLE" },
            { name: "milestone_date", type: "DATE", mode: "NULLABLE" },
            { name: "name", type: "STRING", mode: "NULLABLE" },
          ],
        },
      });
      await milestonesTable.insert([
        { id: 1, milestone_date: "2024-09-01", name: "Start" },
        { id: 2, milestone_date: "2024-09-15", name: "Midpoint" },
        { id: 3, milestone_date: "2024-09-30", name: "End" },
        { id: 4, milestone_date: "2024-08-30", name: "Prep" },
      ]);
    });

    test("run query with DATE equality comparison using string literal", async () => {
      const [response] = await bigQuery.query(
        "SELECT * FROM events.daily_logs WHERE event_date = '2024-01-15' ORDER BY id"
      );
      expect(response).toEqual([
        { id: 1, event_date: "2024-01-15", description: "Event 1" },
        { id: 4, event_date: "2024-01-15", description: "Event 4" },
      ]);
    });

    test("run query with DATE equality comparison using DATE function", async () => {
      const [response] = await bigQuery.query(
        "SELECT * FROM logs.activity WHERE log_date = DATE('2024-03-01') ORDER BY id"
      );
      expect(response).toEqual([
        { id: 1, log_date: "2024-03-01", action: "login" },
        { id: 3, log_date: "2024-03-01", action: "view" },
      ]);
    });

    test("run query with DATE inequality comparison", async () => {
      const [response] = await bigQuery.query(
        "SELECT * FROM schedules.tasks WHERE due_date > '2024-06-10' ORDER BY task_id"
      );
      expect(response).toEqual([
        { task_id: 2, due_date: "2024-06-15", title: "Task B" },
        { task_id: 3, due_date: "2024-06-20", title: "Task C" },
      ]);
    });

    test("run query with DATE using multiple comparison operators", async () => {
      const [response] = await bigQuery.query({
        query:
          "SELECT * FROM projects.milestones WHERE milestone_date >= @start_date AND milestone_date <= @end_date ORDER BY id",
        params: {
          start_date: bigQuery.date("2024-09-01"),
          end_date: bigQuery.date("2024-09-20"),
        },
        types: {
          start_date: "DATE",
          end_date: "DATE",
        },
      });
      expect(response).toEqual([
        { id: 1, milestone_date: "2024-09-01", name: "Start" },
        { id: 2, milestone_date: "2024-09-15", name: "Midpoint" },
      ]);
    });
  });
});
