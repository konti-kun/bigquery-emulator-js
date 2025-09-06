import { getBigQueryClient } from "tests/utils";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

describe.skip("dataset", () => {
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

  test("create dataset", async () => {
    const bigQuery = getBigQueryClient();
    const [dataset] = await bigQuery.createDataset("test_dataset");
    expect(dataset.id).toBe("test_dataset");
  });
});
