import dedent from "dedent";
import { getBigQueryClient } from "tests/utils";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

describe("query - safe math functions", () => {
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

  test("SAFE_ADD - basic addition", async () => {
    const [response] = await bigQuery.query(dedent`
      SELECT
        SAFE_ADD(5, 3) as result1,
        SAFE_ADD(0, 0) as result2,
        SAFE_ADD(-10, 5) as result3,
        SAFE_ADD(10.5, 20.5) as result4
    `);
    expect(response).toEqual([
      {
        result1: 8,
        result2: 0,
        result3: -5,
        result4: 31
      }
    ]);
  });

  test("SAFE_ADD - overflow returns null", async () => {
    const [response] = await bigQuery.query(dedent`
      SELECT
        SAFE_ADD(9223372036854775807, 1) as overflow_result,
        SAFE_ADD(5, 3) as normal_result
    `);
    expect(response).toEqual([
      {
        overflow_result: null,
        normal_result: 8
      }
    ]);
  });

  test("SAFE_ADD - null handling", async () => {
    const [response] = await bigQuery.query(dedent`
      SELECT
        SAFE_ADD(NULL, 5) as result1,
        SAFE_ADD(5, NULL) as result2,
        SAFE_ADD(NULL, NULL) as result3
    `);
    expect(response).toEqual([
      {
        result1: null,
        result2: null,
        result3: null
      }
    ]);
  });

  test("SAFE_SUBTRACT - basic subtraction", async () => {
    const [response] = await bigQuery.query(dedent`
      SELECT
        SAFE_SUBTRACT(10, 3) as result1,
        SAFE_SUBTRACT(0, 5) as result2,
        SAFE_SUBTRACT(-10, -5) as result3,
        SAFE_SUBTRACT(30.5, 10.5) as result4
    `);
    expect(response).toEqual([
      {
        result1: 7,
        result2: -5,
        result3: -5,
        result4: 20
      }
    ]);
  });

  test("SAFE_SUBTRACT - overflow returns null", async () => {
    const [response] = await bigQuery.query(dedent`
      SELECT
        SAFE_SUBTRACT(-9223372036854775808, 1) as overflow_result,
        SAFE_SUBTRACT(10, 3) as normal_result
    `);
    expect(response).toEqual([
      {
        overflow_result: null,
        normal_result: 7
      }
    ]);
  });

  test("SAFE_SUBTRACT - null handling", async () => {
    const [response] = await bigQuery.query(dedent`
      SELECT
        SAFE_SUBTRACT(NULL, 5) as result1,
        SAFE_SUBTRACT(5, NULL) as result2,
        SAFE_SUBTRACT(NULL, NULL) as result3
    `);
    expect(response).toEqual([
      {
        result1: null,
        result2: null,
        result3: null
      }
    ]);
  });

  test("SAFE_MULTIPLY - basic multiplication", async () => {
    const [response] = await bigQuery.query(dedent`
      SELECT
        SAFE_MULTIPLY(5, 3) as result1,
        SAFE_MULTIPLY(0, 100) as result2,
        SAFE_MULTIPLY(-4, 5) as result3,
        SAFE_MULTIPLY(2.5, 4) as result4
    `);
    expect(response).toEqual([
      {
        result1: 15,
        result2: 0,
        result3: -20,
        result4: 10
      }
    ]);
  });

  test("SAFE_MULTIPLY - overflow returns null", async () => {
    const [response] = await bigQuery.query(dedent`
      SELECT
        SAFE_MULTIPLY(9223372036854775807, 2) as overflow_result,
        SAFE_MULTIPLY(5, 3) as normal_result
    `);
    expect(response).toEqual([
      {
        overflow_result: null,
        normal_result: 15
      }
    ]);
  });

  test("SAFE_MULTIPLY - null handling", async () => {
    const [response] = await bigQuery.query(dedent`
      SELECT
        SAFE_MULTIPLY(NULL, 5) as result1,
        SAFE_MULTIPLY(5, NULL) as result2,
        SAFE_MULTIPLY(NULL, NULL) as result3
    `);
    expect(response).toEqual([
      {
        result1: null,
        result2: null,
        result3: null
      }
    ]);
  });

  test("SAFE_DIVIDE - basic division", async () => {
    const [response] = await bigQuery.query(dedent`
      SELECT
        SAFE_DIVIDE(10, 2) as result1,
        SAFE_DIVIDE(7, 2) as result2,
        SAFE_DIVIDE(-10, 5) as result3,
        SAFE_DIVIDE(20.5, 2) as result4
    `);
    expect(response).toEqual([
      {
        result1: 5,
        result2: 3.5,
        result3: -2,
        result4: 10.25
      }
    ]);
  });

  test("SAFE_DIVIDE - division by zero returns null", async () => {
    const [response] = await bigQuery.query(dedent`
      SELECT
        SAFE_DIVIDE(10, 0) as div_by_zero,
        SAFE_DIVIDE(0, 0) as zero_by_zero,
        SAFE_DIVIDE(10, 2) as normal_result
    `);
    expect(response).toEqual([
      {
        div_by_zero: null,
        zero_by_zero: null,
        normal_result: 5
      }
    ]);
  });

  test("SAFE_DIVIDE - null handling", async () => {
    const [response] = await bigQuery.query(dedent`
      SELECT
        SAFE_DIVIDE(NULL, 5) as result1,
        SAFE_DIVIDE(10, NULL) as result2,
        SAFE_DIVIDE(NULL, NULL) as result3
    `);
    expect(response).toEqual([
      {
        result1: null,
        result2: null,
        result3: null
      }
    ]);
  });

  test("SAFE_NEGATE - basic negation", async () => {
    const [response] = await bigQuery.query(dedent`
      SELECT
        SAFE_NEGATE(5) as result1,
        SAFE_NEGATE(-10) as result2,
        SAFE_NEGATE(0) as result3,
        SAFE_NEGATE(3.14) as result4
    `);
    expect(response).toEqual([
      {
        result1: -5,
        result2: 10,
        result3: 0,
        result4: -3.14
      }
    ]);
  });

  test("SAFE_NEGATE - overflow returns null", async () => {
    const [response] = await bigQuery.query(dedent`
      SELECT
        SAFE_NEGATE(-9223372036854775808) as overflow_result,
        SAFE_NEGATE(5) as normal_result
    `);
    expect(response).toEqual([
      {
        overflow_result: null,
        normal_result: -5
      }
    ]);
  });

  test("SAFE_NEGATE - null handling", async () => {
    const [response] = await bigQuery.query(dedent`
      SELECT
        SAFE_NEGATE(NULL) as result
    `);
    expect(response).toEqual([
      {
        result: null
      }
    ]);
  });

  test("SAFE functions - combined usage", async () => {
    const [response] = await bigQuery.query(dedent`
      SELECT
        SAFE_ADD(SAFE_MULTIPLY(5, 3), SAFE_DIVIDE(10, 2)) as complex1,
        SAFE_SUBTRACT(SAFE_ADD(10, 5), SAFE_NEGATE(-3)) as complex2
    `);
    expect(response).toEqual([
      {
        complex1: 20,  // 15 + 5
        complex2: 12   // 15 - (-3) = 15 + 3
      }
    ]);
  });

  test("SAFE functions - with table data", async () => {
    const [response] = await bigQuery.query(dedent`
      WITH TestData AS (
        SELECT 10 AS x, 2 AS y UNION ALL
        SELECT 15, 0 UNION ALL
        SELECT 20, 4 UNION ALL
        SELECT NULL, 5
      )
      SELECT
        x,
        y,
        SAFE_ADD(x, y) as sum,
        SAFE_SUBTRACT(x, y) as diff,
        SAFE_MULTIPLY(x, y) as product,
        SAFE_DIVIDE(x, y) as quotient,
        SAFE_NEGATE(x) as negated
      FROM TestData
      ORDER BY x
    `);
    expect(response).toEqual([
      { x: null, y: 5, sum: null, diff: null, product: null, quotient: null, negated: null },
      { x: 10, y: 2, sum: 12, diff: 8, product: 20, quotient: 5, negated: -10 },
      { x: 15, y: 0, sum: 15, diff: 15, product: 0, quotient: null, negated: -15 },
      { x: 20, y: 4, sum: 24, diff: 16, product: 80, quotient: 5, negated: -20 }
    ]);
  });
});
