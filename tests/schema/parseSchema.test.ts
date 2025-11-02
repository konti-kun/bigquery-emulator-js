import { describe, expect, test } from "vitest";
import { parseSchema } from "../../app/routes/createTable";

describe("parseSchema", () => {
  test("simple schema with STRING and INT64", () => {
    const schema = "id INT64, name STRING";
    const result = parseSchema(schema);
    expect(result).toEqual([
      { name: "id", type: "INT64", mode: "NULLABLE" },
      { name: "name", type: "STRING", mode: "NULLABLE" },
    ]);
  });

  test("schema with multiple types", () => {
    const schema =
      "id INT64, name STRING, age INT64, email STRING, created_at TIMESTAMP";
    const result = parseSchema(schema);
    expect(result).toEqual([
      { name: "id", type: "INT64", mode: "NULLABLE" },
      { name: "name", type: "STRING", mode: "NULLABLE" },
      { name: "age", type: "INT64", mode: "NULLABLE" },
      { name: "email", type: "STRING", mode: "NULLABLE" },
      { name: "created_at", type: "TIMESTAMP", mode: "NULLABLE" },
    ]);
  });

  test("schema with REQUIRED mode", () => {
    const schema = "id INT64 NOT NULL, name STRING";
    const result = parseSchema(schema);
    expect(result).toEqual([
      { name: "id", type: "INT64", mode: "REQUIRED" },
      { name: "name", type: "STRING", mode: "NULLABLE" },
    ]);
  });

  test("schema with REPEATED mode (ARRAY)", () => {
    const schema = "id INT64, tags ARRAY<STRING>";
    const result = parseSchema(schema);
    expect(result).toEqual([
      { name: "id", type: "INT64", mode: "NULLABLE" },
      { name: "tags", type: "STRING", mode: "REPEATED" },
    ]);
  });

  test("schema with NUMERIC and BIGNUMERIC", () => {
    const schema = "id INT64, price NUMERIC, large_price BIGNUMERIC";
    const result = parseSchema(schema);
    expect(result).toEqual([
      { name: "id", type: "INT64", mode: "NULLABLE" },
      { name: "price", type: "NUMERIC", mode: "NULLABLE" },
      { name: "large_price", type: "BIGNUMERIC", mode: "NULLABLE" },
    ]);
  });

  test("schema with STRUCT", () => {
    const schema = "id INT64, address STRUCT<street STRING, city STRING>";
    const result = parseSchema(schema);
    expect(result).toEqual([
      { name: "id", type: "INT64", mode: "NULLABLE" },
      {
        name: "address",
        type: "STRUCT",
        mode: "NULLABLE",
        fields: [
          { name: "street", type: "STRING", mode: "NULLABLE" },
          { name: "city", type: "STRING", mode: "NULLABLE" },
        ],
      },
    ]);
  });

  test("schema with nested STRUCT", () => {
    const schema =
      "id INT64, person STRUCT<name STRING, address STRUCT<street STRING, city STRING>>";
    const result = parseSchema(schema);
    expect(result).toEqual([
      { name: "id", type: "INT64", mode: "NULLABLE" },
      {
        name: "person",
        type: "STRUCT",
        mode: "NULLABLE",
        fields: [
          { name: "name", type: "STRING", mode: "NULLABLE" },
          {
            name: "address",
            type: "STRUCT",
            mode: "NULLABLE",
            fields: [
              { name: "street", type: "STRING", mode: "NULLABLE" },
              { name: "city", type: "STRING", mode: "NULLABLE" },
            ],
          },
        ],
      },
    ]);
  });

  test("schema with ARRAY of STRUCT", () => {
    const schema = "id INT64, items ARRAY<STRUCT<name STRING, price NUMERIC>>";
    const result = parseSchema(schema);
    expect(result).toEqual([
      { name: "id", type: "INT64", mode: "NULLABLE" },
      {
        name: "items",
        type: "STRUCT",
        mode: "REPEATED",
        fields: [
          { name: "name", type: "STRING", mode: "NULLABLE" },
          { name: "price", type: "NUMERIC", mode: "NULLABLE" },
        ],
      },
    ]);
  });

  test("schema with various data types", () => {
    const schema =
      "bool_col BOOL, bytes_col BYTES, date_col DATE, datetime_col DATETIME, time_col TIME, geography_col GEOGRAPHY, json_col JSON";
    const result = parseSchema(schema);
    expect(result).toEqual([
      { name: "bool_col", type: "BOOL", mode: "NULLABLE" },
      { name: "bytes_col", type: "BYTES", mode: "NULLABLE" },
      { name: "date_col", type: "DATE", mode: "NULLABLE" },
      { name: "datetime_col", type: "DATETIME", mode: "NULLABLE" },
      { name: "time_col", type: "TIME", mode: "NULLABLE" },
      { name: "geography_col", type: "GEOGRAPHY", mode: "NULLABLE" },
      { name: "json_col", type: "JSON", mode: "NULLABLE" },
    ]);
  });

  test("schema with STRING(length)", () => {
    const schema = "id INT64, name STRING(100)";
    const result = parseSchema(schema);
    expect(result).toEqual([
      { name: "id", type: "INT64", mode: "NULLABLE" },
      { name: "name", type: "STRING", maxLength: "100", mode: "NULLABLE" },
    ]);
  });

  test("schema with NUMERIC(precision, scale)", () => {
    const schema = "id INT64, price NUMERIC(10, 2)";
    const result = parseSchema(schema);
    expect(result).toEqual([
      { name: "id", type: "INT64", mode: "NULLABLE" },
      {
        name: "price",
        type: "NUMERIC",
        precision: "10",
        scale: "2",
        mode: "NULLABLE",
      },
    ]);
  });

  test("schema with whitespace variations", () => {
    const schema = "  id   INT64  ,   name   STRING  ";
    const result = parseSchema(schema);
    expect(result).toEqual([
      { name: "id", type: "INT64", mode: "NULLABLE" },
      { name: "name", type: "STRING", mode: "NULLABLE" },
    ]);
  });
});
