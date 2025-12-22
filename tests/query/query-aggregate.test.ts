import dedent from "dedent";
import { getBigQueryClient } from "tests/utils";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

describe("query - aggregate functions", () => {
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
});
