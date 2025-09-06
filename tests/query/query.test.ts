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
  });
});
