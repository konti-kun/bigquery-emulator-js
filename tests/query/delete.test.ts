import dedent from "dedent";
import { getBigQueryClient } from "tests/utils";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

describe("DELETE query", () => {
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

  test("DELETE from table with dot notation", async () => {
    // データセットとテーブルを作成
    const dataset = bigQuery.dataset("posts");
    await dataset.create();
    const table = dataset.table("test_project");
    await table.create({
      schema: {
        fields: [
          { name: "analysisId", type: "STRING", mode: "NULLABLE" },
          { name: "content", type: "STRING", mode: "NULLABLE" },
        ],
      },
    });

    // テストデータを挿入
    await table.insert([
      { analysisId: "test_1", content: "First post" },
      { analysisId: "test_2", content: "Second post" },
      { analysisId: "test_3", content: "Third post" },
    ]);

    // DELETEクエリを実行
    await bigQuery.query(
      "DELETE from `posts.test_project` where analysisId in ('test_1','test_2')"
    );

    // 残りのデータを確認
    const [response] = await bigQuery.query(
      "SELECT * FROM `posts.test_project` ORDER BY analysisId"
    );
    expect(response).toEqual([{ analysisId: "test_3", content: "Third post" }]);
  });

  test("DELETE from table without backticks", async () => {
    // データセットとテーブルを作成
    const dataset = bigQuery.dataset("users");
    await dataset.create();
    const table = dataset.table("accounts");
    await table.create({
      schema: {
        fields: [
          { name: "id", type: "INT64", mode: "NULLABLE" },
          { name: "name", type: "STRING", mode: "NULLABLE" },
        ],
      },
    });

    // テストデータを挿入
    await table.insert([
      { id: 1, name: "Alice" },
      { id: 2, name: "Bob" },
      { id: 3, name: "Charlie" },
    ]);

    // DELETEクエリを実行（バッククォートなし）
    await bigQuery.query("DELETE from users.accounts where id = 2");

    // 残りのデータを確認
    const [response] = await bigQuery.query(
      "SELECT * FROM users.accounts ORDER BY id"
    );
    expect(response).toEqual([
      { id: 1, name: "Alice" },
      { id: 3, name: "Charlie" },
    ]);
  });

  test("DELETE all rows from table", async () => {
    // データセットとテーブルを作成
    const dataset = bigQuery.dataset("temp");
    await dataset.create();
    const table = dataset.table("data");
    await table.create({
      schema: {
        fields: [{ name: "value", type: "INT64", mode: "NULLABLE" }],
      },
    });

    // テストデータを挿入
    await table.insert([{ value: 1 }, { value: 2 }, { value: 3 }]);

    // 全行DELETEクエリを実行
    await bigQuery.query("DELETE from temp.data where 1=1");

    // データが空であることを確認
    const [response] = await bigQuery.query("SELECT * FROM temp.data");
    expect(response).toEqual([]);
  });

  test("DELETE with complex WHERE condition", async () => {
    // データセットとテーブルを作成
    const dataset = bigQuery.dataset("sales");
    await dataset.create();
    const table = dataset.table("orders");
    await table.create({
      schema: {
        fields: [
          { name: "order_id", type: "INT64", mode: "NULLABLE" },
          { name: "amount", type: "INT64", mode: "NULLABLE" },
          { name: "status", type: "STRING", mode: "NULLABLE" },
        ],
      },
    });

    // テストデータを挿入
    await table.insert([
      { order_id: 1, amount: 100, status: "pending" },
      { order_id: 2, amount: 200, status: "completed" },
      { order_id: 3, amount: 150, status: "pending" },
      { order_id: 4, amount: 300, status: "completed" },
    ]);

    // 複雑な条件でDELETEクエリを実行
    // amount > 150 AND status = 'completed' に該当するのは order_id 2 (amount=200) と order_id 4 (amount=300)
    await bigQuery.query(
      "DELETE from sales.orders where amount > 150 AND status = 'completed'"
    );

    // 残りのデータを確認
    const [response] = await bigQuery.query(
      "SELECT * FROM sales.orders ORDER BY order_id"
    );
    expect(response).toEqual([
      { order_id: 1, amount: 100, status: "pending" },
      { order_id: 3, amount: 150, status: "pending" },
    ]);
  });
});
