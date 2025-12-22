import { getBigQueryClient } from "tests/utils";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

describe("query - parameters", () => {
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

  describe("query parameters with temporal types", () => {
    test("query with DATE parameter using types", async () => {
      const bigQuery = getBigQueryClient();
      const dataset = bigQuery.dataset("test_dataset");
      await dataset.create();
      const table = dataset.table("test_table");
      await table.create({
        schema: {
          fields: [
            { name: "id", type: "INT64", mode: "NULLABLE" },
            { name: "event_date", type: "DATE", mode: "NULLABLE" },
          ],
        },
      });

      await table.insert([
        { id: 1, event_date: "2024-01-15" },
        { id: 2, event_date: "2024-01-16" },
        { id: 3, event_date: "2024-01-17" },
      ]);

      const [response] = await bigQuery.createQueryJob({
        query:
          "SELECT * FROM test_dataset.test_table WHERE event_date = @target_date ORDER BY id",
        params: {
          target_date: bigQuery.date("2024-01-16"),
        },
        types: {
          target_date: "DATE",
        },
      });
      const [rows] = await response.getQueryResults();
      expect(rows).toEqual([{ id: 2, event_date: "2024-01-16" }]);
    });

    test("query with DATETIME parameter using types", async () => {
      const bigQuery = getBigQueryClient();
      const dataset = bigQuery.dataset("test_dataset");
      await dataset.create();
      const table = dataset.table("test_table");
      await table.create({
        schema: {
          fields: [
            { name: "id", type: "INT64", mode: "NULLABLE" },
            { name: "event_time", type: "DATETIME", mode: "NULLABLE" },
          ],
        },
      });

      await table.insert([
        { id: 1, event_time: "2024-01-15T10:30:00" },
        { id: 2, event_time: "2024-01-15T14:45:00" },
        { id: 3, event_time: "2024-01-16T09:00:00" },
      ]);

      const [response] = await bigQuery.query({
        query:
          "SELECT * FROM test_dataset.test_table WHERE event_time = @target_time ORDER BY id",
        params: {
          target_time: bigQuery.date("2024-01-15T14:45:00"),
        },
        types: {
          target_time: "DATETIME",
        },
      });

      expect(response).toEqual([{ id: 2, event_time: "2024-01-15T14:45:00" }]);
    });

    test("query with TIMESTAMP parameter using types", async () => {
      const bigQuery = getBigQueryClient();
      const dataset = bigQuery.dataset("test_dataset");
      await dataset.create();
      const table = dataset.table("test_table");
      await table.create({
        schema: {
          fields: [
            { name: "id", type: "INT64", mode: "NULLABLE" },
            { name: "created_at", type: "TIMESTAMP", mode: "NULLABLE" },
          ],
        },
      });

      await table.insert([
        { id: 1, created_at: "2024-01-15T10:30:00Z" },
        { id: 2, created_at: "2024-01-15T14:45:00Z" },
        { id: 3, created_at: "2024-01-16T09:00:00Z" },
      ]);

      const [response] = await bigQuery.query({
        query:
          "SELECT * FROM test_dataset.test_table WHERE created_at = @target_timestamp ORDER BY id",
        params: {
          target_timestamp: bigQuery.date("2024-01-15T14:45:00Z"),
        },
        types: {
          target_timestamp: "TIMESTAMP",
        },
      });

      expect(response).toEqual([
        { id: 2, created_at: "2024-01-15T14:45:00.000Z" },
      ]);
    });
  });

  describe("DATE type in WHERE clause", () => {
    beforeEach(async () => {
      // events.daily_logs テーブル
      const eventsDataset = bigQuery.dataset("events");
      await eventsDataset.create();
      const dailyLogsTable = eventsDataset.table("daily_logs");
      await dailyLogsTable.create({
        schema: {
          fields: [
            { name: "id", type: "INT64", mode: "NULLABLE" },
            { name: "event_date", type: "DATE", mode: "NULLABLE" },
            { name: "description", type: "STRING", mode: "NULLABLE" },
          ],
        },
      });
      await dailyLogsTable.insert([
        { id: 1, event_date: "2024-01-15", description: "Event 1" },
        { id: 2, event_date: "2024-01-16", description: "Event 2" },
        { id: 3, event_date: "2024-01-17", description: "Event 3" },
        { id: 4, event_date: "2024-01-15", description: "Event 4" },
      ]);

      // logs.activity テーブル
      const logsDataset = bigQuery.dataset("logs");
      await logsDataset.create();
      const activityTable = logsDataset.table("activity");
      await activityTable.create({
        schema: {
          fields: [
            { name: "id", type: "INT64", mode: "NULLABLE" },
            { name: "log_date", type: "DATE", mode: "NULLABLE" },
            { name: "action", type: "STRING", mode: "NULLABLE" },
          ],
        },
      });
      await activityTable.insert([
        { id: 1, log_date: "2024-03-01", action: "login" },
        { id: 2, log_date: "2024-03-02", action: "logout" },
        { id: 3, log_date: "2024-03-01", action: "view" },
      ]);

      // schedules.tasks テーブル
      const schedulesDataset = bigQuery.dataset("schedules");
      await schedulesDataset.create();
      const tasksTable = schedulesDataset.table("tasks");
      await tasksTable.create({
        schema: {
          fields: [
            { name: "task_id", type: "INT64", mode: "NULLABLE" },
            { name: "due_date", type: "DATE", mode: "NULLABLE" },
            { name: "title", type: "STRING", mode: "NULLABLE" },
          ],
        },
      });
      await tasksTable.insert([
        { task_id: 1, due_date: "2024-06-10", title: "Task A" },
        { task_id: 2, due_date: "2024-06-15", title: "Task B" },
        { task_id: 3, due_date: "2024-06-20", title: "Task C" },
      ]);

      // projects.milestones テーブル
      const projectsDataset = bigQuery.dataset("projects");
      await projectsDataset.create();
      const milestonesTable = projectsDataset.table("milestones");
      await milestonesTable.create({
        schema: {
          fields: [
            { name: "id", type: "INT64", mode: "NULLABLE" },
            { name: "milestone_date", type: "DATE", mode: "NULLABLE" },
            { name: "name", type: "STRING", mode: "NULLABLE" },
          ],
        },
      });
      await milestonesTable.insert([
        { id: 1, milestone_date: "2024-09-01", name: "Start" },
        { id: 2, milestone_date: "2024-09-15", name: "Midpoint" },
        { id: 3, milestone_date: "2024-09-30", name: "End" },
        { id: 4, milestone_date: "2024-08-30", name: "Prep" },
      ]);
    });

    test("run query with DATE equality comparison using string literal", async () => {
      const [response] = await bigQuery.query(
        "SELECT * FROM events.daily_logs WHERE event_date = '2024-01-15' ORDER BY id"
      );
      expect(response).toEqual([
        { id: 1, event_date: "2024-01-15", description: "Event 1" },
        { id: 4, event_date: "2024-01-15", description: "Event 4" },
      ]);
    });

    test("run query with DATE equality comparison using DATE function", async () => {
      const [response] = await bigQuery.query(
        "SELECT * FROM logs.activity WHERE log_date = DATE('2024-03-01') ORDER BY id"
      );
      expect(response).toEqual([
        { id: 1, log_date: "2024-03-01", action: "login" },
        { id: 3, log_date: "2024-03-01", action: "view" },
      ]);
    });

    test("run query with DATE inequality comparison", async () => {
      const [response] = await bigQuery.query(
        "SELECT * FROM schedules.tasks WHERE due_date > '2024-06-10' ORDER BY task_id"
      );
      expect(response).toEqual([
        { task_id: 2, due_date: "2024-06-15", title: "Task B" },
        { task_id: 3, due_date: "2024-06-20", title: "Task C" },
      ]);
    });

    test("run query with DATE using multiple comparison operators", async () => {
      const [response] = await bigQuery.query({
        query:
          "SELECT * FROM projects.milestones WHERE milestone_date >= @start_date AND milestone_date <= @end_date ORDER BY id",
        params: {
          start_date: bigQuery.date("2024-09-01"),
          end_date: bigQuery.date("2024-09-20"),
        },
        types: {
          start_date: "DATE",
          end_date: "DATE",
        },
      });
      expect(response).toEqual([
        { id: 1, milestone_date: "2024-09-01", name: "Start" },
        { id: 2, milestone_date: "2024-09-15", name: "Midpoint" },
      ]);
    });
  });
});
