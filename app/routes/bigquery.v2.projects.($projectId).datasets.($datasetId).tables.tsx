import type { Route } from "./+types/bigquery.v2.projects.($projectId).datasets";
import type { Table } from "types/table";
import { dbSession } from "~/utils/db.server";
import { parseSchema, generateSQLiteCreateTableSQL } from "./createTable";

export const action = async ({ request, params }: Route.ActionArgs) => {
  const body = (await request.json()) as Table;
  const response = { ...body } as Table;

  const result = dbSession()
    .prepare(
      "SELECT dataset_id FROM datasets WHERE dataset_id = ? AND project_id = ?"
    )
    .get(
      body.tableReference?.datasetId ?? params.datasetId,
      body.tableReference?.projectId ?? params.projectId
    );
  if (!result) {
    return new Response(
      "Not found: Dataset " + params.projectId + ":" + params.datasetId,
      { status: 404 }
    );
  }
  switch (request.method) {
    case "POST": {
      // スキーマをパースして正しい形式で保存
      let schemaToSave = body.schema;

      // スキーマが文字列として送信された場合はパース
      if (body.schema && typeof body.schema === "string") {
        const schemaString = (body.schema as string).trim();

        // 空のスキーマはエラー
        if (!schemaString) {
          return new Response("Invalid schema: Schema cannot be empty", {
            status: 400,
          });
        }

        const parsedFields = parseSchema(schemaString);

        // パース後にフィールドが空の場合もエラー
        if (!parsedFields || parsedFields.length === 0) {
          return new Response("Invalid schema: Schema cannot be empty", {
            status: 400,
          });
        }

        schemaToSave = { fields: parsedFields };
      } else if (
        body.schema &&
        typeof body.schema === "object" &&
        "fields" in body.schema
      ) {
        // スキーマがオブジェクトとして送信された場合
        // fieldsの各要素を確認し、nameに型情報が含まれている場合はパースし直す
        const fields = body.schema.fields;
        if (fields && Array.isArray(fields) && fields.length > 0) {
          const firstField = fields[0];
          // nameに型情報が含まれているか確認（例: "id INT64"）
          if (firstField.name && firstField.name.includes(" ")) {
            // 全フィールドを再パース
            const schemaString = fields.map((f) => f.name).join(", ");
            const parsedFields = parseSchema(schemaString);
            schemaToSave = { fields: parsedFields };
          }
        }
      }

      // 重複テーブルのチェック
      const existingTable = dbSession()
        .prepare(
          "SELECT table_id FROM tables WHERE table_id = ? AND dataset_id = ? AND project_id = ?"
        )
        .get(
          body.tableReference?.tableId,
          body.tableReference?.datasetId ?? params.datasetId,
          body.tableReference?.projectId ?? params.projectId
        );

      if (existingTable) {
        return new Response(
          `Already Exists: Table ${params.projectId}:${params.datasetId}.${body.tableReference?.tableId}`,
          { status: 409 }
        );
      }

      // スキーマが存在しない場合はエラー
      if (!schemaToSave || !schemaToSave.fields) {
        return new Response("Invalid schema: Schema is required", {
          status: 400,
        });
      }

      // メタデータテーブルに保存
      dbSession()
        .prepare(
          "INSERT INTO tables (table_id, dataset_id, project_id, schema) VALUES (?, ?, ?, ?)"
        )
        .run(
          body.tableReference?.tableId,
          body.tableReference?.datasetId ?? params.datasetId,
          body.tableReference?.projectId ?? params.projectId,
          JSON.stringify(schemaToSave)
        );

      // 実際のSQLiteテーブルを作成
      const createTableSQL = generateSQLiteCreateTableSQL(
        body.tableReference?.datasetId ?? params.datasetId,
        body.tableReference?.tableId ?? "",
        schemaToSave.fields
      );

      dbSession().exec(createTableSQL);
      console.log("Created table with SQL:", createTableSQL);
    }
  }
  return response;
};

export function loader({ params }: Route.LoaderArgs) {}
