import { format } from "date-fns";
import { formatInTimeZone } from "date-fns-tz";
import { getBigQueryClient } from "tests/utils";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

describe("query - temporal functions", () => {
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

  test("run query with FORMAT_DATE", async () => {
    const [response1] = await bigQuery.query(
      "SELECT FORMAT_DATE('%Y-%m-%d', DATE '2023-12-25') AS formatted_date"
    );
    expect(response1).toEqual([{ formatted_date: "2023-12-25" }]);
  });

  test("run query with FORMAT_DATE with custom format", async () => {
    const [response2] = await bigQuery.query(
      "SELECT FORMAT_DATE('%Y/%m/%d', DATE '2023-12-25') AS formatted_date"
    );
    expect(response2).toEqual([{ formatted_date: "2023/12/25" }]);
  });

  test("run query with FORMAT_DATE with year and month only", async () => {
    const [response3] = await bigQuery.query(
      "SELECT FORMAT_DATE('%Y-%m', DATE '2023-12-25') AS formatted_date"
    );
    expect(response3).toEqual([{ formatted_date: "2023-12" }]);
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

  describe("DATE_ADD and DATE_SUB functions", () => {
    test("run query with DATE_ADD adding days", async () => {
      const [response1] = await bigQuery.query(
        "SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 10 DAY) AS result_date"
      );
      expect(response1).toEqual([{ result_date: bigQuery.date("2024-01-25") }]);
    });

    test("run query with DATE_ADD adding months", async () => {
      const [response2] = await bigQuery.query(
        "SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 2 MONTH) AS result_date"
      );
      expect(response2).toEqual([{ result_date: bigQuery.date("2024-03-15") }]);
    });

    test("run query with DATE_ADD adding years", async () => {
      const [response3] = await bigQuery.query(
        "SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 1 YEAR) AS result_date"
      );
      expect(response3).toEqual([{ result_date: bigQuery.date("2025-01-15") }]);
    });

    test("run query with DATE_ADD crossing year boundary", async () => {
      const [response4] = await bigQuery.query(
        "SELECT DATE_ADD(DATE '2024-12-25', INTERVAL 10 DAY) AS result_date"
      );
      expect(response4).toEqual([{ result_date: bigQuery.date("2025-01-04") }]);
    });

    test("run query with DATE_SUB subtracting days", async () => {
      const [response5] = await bigQuery.query(
        "SELECT DATE_SUB(DATE '2024-01-25', INTERVAL 10 DAY) AS result_date"
      );
      expect(response5).toEqual([{ result_date: bigQuery.date("2024-01-15") }]);
    });

    test("run query with DATE_SUB subtracting months", async () => {
      const [response6] = await bigQuery.query(
        "SELECT DATE_SUB(DATE '2024-03-15', INTERVAL 2 MONTH) AS result_date"
      );
      expect(response6).toEqual([{ result_date: bigQuery.date("2024-01-15") }]);
    });

    test("run query with DATE_SUB subtracting years", async () => {
      const [response7] = await bigQuery.query(
        "SELECT DATE_SUB(DATE '2025-01-15', INTERVAL 1 YEAR) AS result_date"
      );
      expect(response7).toEqual([{ result_date: bigQuery.date("2024-01-15") }]);
    });

    test("run query with DATE_SUB crossing year boundary", async () => {
      const [response8] = await bigQuery.query(
        "SELECT DATE_SUB(DATE '2024-01-05', INTERVAL 10 DAY) AS result_date"
      );
      expect(response8).toEqual([{ result_date: bigQuery.date("2023-12-26") }]);
    });

    test("run query with DATE_ADD using string date", async () => {
      const [response9] = await bigQuery.query(
        "SELECT DATE_ADD('2024-06-15', INTERVAL 7 DAY) AS result_date"
      );
      expect(response9).toEqual([{ result_date: bigQuery.date("2024-06-22") }]);
    });

    test("run query with DATE_SUB using string date", async () => {
      const [response10] = await bigQuery.query(
        "SELECT DATE_SUB('2024-06-15', INTERVAL 7 DAY) AS result_date"
      );
      expect(response10).toEqual([{ result_date: bigQuery.date("2024-06-08") }]);
    });
  });
});
