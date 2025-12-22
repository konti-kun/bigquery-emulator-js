import dedent from "dedent";
import { getBigQueryClient } from "tests/utils";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

describe("query - UNNEST", () => {
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
