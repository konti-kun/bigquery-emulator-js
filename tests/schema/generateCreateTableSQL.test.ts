import { describe, expect, test } from "vitest";
import { generateCreateTableSQL } from "../../app/routes/createTable";
import type { TableFieldSchema } from "../../types/query";

describe("generateCreateTableSQL", () => {
  test("simple schema with STRING and INT64", () => {
    const schema: TableFieldSchema[] = [
      { name: "id", type: "INT64" },
      { name: "name", type: "STRING" },
    ];
    const result = generateCreateTableSQL("my_project", "my_dataset", "users", schema);
    expect(result).toBe(
      "CREATE TABLE `my_project_my_dataset.users` (\n  id INT64,\n  name STRING\n);"
    );
  });

  test("schema with REQUIRED mode (NOT NULL)", () => {
    const schema: TableFieldSchema[] = [
      { name: "id", type: "INT64", mode: "REQUIRED" },
      { name: "name", type: "STRING" },
    ];
    const result = generateCreateTableSQL("my_project", "my_dataset", "users", schema);
    expect(result).toBe(
      "CREATE TABLE `my_project_my_dataset.users` (\n  id INT64 NOT NULL,\n  name STRING\n);"
    );
  });

  test("schema with REPEATED mode (ARRAY)", () => {
    const schema: TableFieldSchema[] = [
      { name: "id", type: "INT64" },
      { name: "tags", type: "STRING", mode: "REPEATED" },
    ];
    const result = generateCreateTableSQL("my_project", "my_dataset", "users", schema);
    expect(result).toBe(
      "CREATE TABLE `my_project_my_dataset.users` (\n  id INT64,\n  tags ARRAY<STRING>\n);"
    );
  });

  test("schema with STRUCT", () => {
    const schema: TableFieldSchema[] = [
      { name: "id", type: "INT64" },
      {
        name: "address",
        type: "STRUCT",
        fields: [
          { name: "street", type: "STRING" },
          { name: "city", type: "STRING" },
        ],
      },
    ];
    const result = generateCreateTableSQL("my_project", "my_dataset", "users", schema);
    expect(result).toBe(
      "CREATE TABLE `my_project_my_dataset.users` (\n  id INT64,\n  address STRUCT<street STRING, city STRING>\n);"
    );
  });

  test("schema with nested STRUCT", () => {
    const schema: TableFieldSchema[] = [
      { name: "id", type: "INT64" },
      {
        name: "person",
        type: "STRUCT",
        fields: [
          { name: "name", type: "STRING" },
          {
            name: "address",
            type: "STRUCT",
            fields: [
              { name: "street", type: "STRING" },
              { name: "city", type: "STRING" },
            ],
          },
        ],
      },
    ];
    const result = generateCreateTableSQL("my_project", "my_dataset", "users", schema);
    expect(result).toBe(
      "CREATE TABLE `my_project_my_dataset.users` (\n  id INT64,\n  person STRUCT<name STRING, address STRUCT<street STRING, city STRING>>\n);"
    );
  });

  test("schema with ARRAY of STRUCT", () => {
    const schema: TableFieldSchema[] = [
      { name: "id", type: "INT64" },
      {
        name: "items",
        type: "STRUCT",
        mode: "REPEATED",
        fields: [
          { name: "name", type: "STRING" },
          { name: "price", type: "NUMERIC" },
        ],
      },
    ];
    const result = generateCreateTableSQL("my_project", "my_dataset", "users", schema);
    expect(result).toBe(
      "CREATE TABLE `my_project_my_dataset.users` (\n  id INT64,\n  items ARRAY<STRUCT<name STRING, price NUMERIC>>\n);"
    );
  });

  test("schema with STRING(length)", () => {
    const schema: TableFieldSchema[] = [
      { name: "id", type: "INT64" },
      { name: "name", type: "STRING", maxLength: "100" },
    ];
    const result = generateCreateTableSQL("my_project", "my_dataset", "users", schema);
    expect(result).toBe(
      "CREATE TABLE `my_project_my_dataset.users` (\n  id INT64,\n  name STRING(100)\n);"
    );
  });

  test("schema with NUMERIC(precision, scale)", () => {
    const schema: TableFieldSchema[] = [
      { name: "id", type: "INT64" },
      { name: "price", type: "NUMERIC", precision: "10", scale: "2" },
    ];
    const result = generateCreateTableSQL("my_project", "my_dataset", "users", schema);
    expect(result).toBe(
      "CREATE TABLE `my_project_my_dataset.users` (\n  id INT64,\n  price NUMERIC(10, 2)\n);"
    );
  });

  test("schema with multiple data types", () => {
    const schema: TableFieldSchema[] = [
      { name: "id", type: "INT64" },
      { name: "name", type: "STRING" },
      { name: "is_active", type: "BOOL" },
      { name: "created_at", type: "TIMESTAMP" },
      { name: "price", type: "NUMERIC" },
    ];
    const result = generateCreateTableSQL("my_project", "my_dataset", "users", schema);
    expect(result).toBe(
      "CREATE TABLE `my_project_my_dataset.users` (\n  id INT64,\n  name STRING,\n  is_active BOOL,\n  created_at TIMESTAMP,\n  price NUMERIC\n);"
    );
  });

  test("complex schema with mixed types", () => {
    const schema: TableFieldSchema[] = [
      { name: "id", type: "INT64", mode: "REQUIRED" },
      { name: "email", type: "STRING", maxLength: "255", mode: "REQUIRED" },
      { name: "tags", type: "STRING", mode: "REPEATED" },
      {
        name: "metadata",
        type: "STRUCT",
        fields: [
          { name: "created_at", type: "TIMESTAMP" },
          { name: "updated_at", type: "TIMESTAMP" },
        ],
      },
    ];
    const result = generateCreateTableSQL("my_project", "my_dataset", "users", schema);
    expect(result).toBe(
      "CREATE TABLE `my_project_my_dataset.users` (\n  id INT64 NOT NULL,\n  email STRING(255) NOT NULL,\n  tags ARRAY<STRING>,\n  metadata STRUCT<created_at TIMESTAMP, updated_at TIMESTAMP>\n);"
    );
  });
});
