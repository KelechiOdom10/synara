import { Schema } from "effect";

import { PositiveInt, TrimmedNonEmptyString } from "./baseSchemas";

// An issue pipeline takes one GitHub issue through plan, build, and review workers in a Hub.
// Each worker's task carries the issue and its role so the server can enforce the gates.

/** A worker's job in an issue pipeline: plan the issue, build it, or review the build. */
export const ProjectTaskPipelineRole = Schema.Literals(["plan", "build", "review"]);
export type ProjectTaskPipelineRole = typeof ProjectTaskPipelineRole.Type;

export const GitHubIssueRef = Schema.Struct({
  repository: TrimmedNonEmptyString.check(Schema.isPattern(/^[\w.-]+\/[\w.-]+$/)),
  number: PositiveInt,
});
export type GitHubIssueRef = typeof GitHubIssueRef.Type;

export const ProjectTaskPipeline = Schema.Struct({
  issue: GitHubIssueRef,
  role: ProjectTaskPipelineRole,
});
export type ProjectTaskPipeline = typeof ProjectTaskPipeline.Type;
