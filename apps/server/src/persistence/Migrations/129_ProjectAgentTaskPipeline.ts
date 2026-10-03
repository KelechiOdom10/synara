// Tag Hub tasks with the GitHub issue and pipeline role they serve. Additive and nullable, so
// Stable, which never writes the tag, reads every existing task unchanged.
import { Effect } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { columnExists } from "./schemaHelpers.ts";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  if (!(yield* columnExists(sql, "project_agent_tasks", "pipeline_json"))) {
    yield* sql`ALTER TABLE project_agent_tasks ADD COLUMN pipeline_json TEXT`;
  }
});
