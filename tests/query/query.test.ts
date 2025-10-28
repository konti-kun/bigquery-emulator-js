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

  test("run query", async () => {
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
});
