// FILE: issuePipeline.ts
// Purpose: The gates of a Hub issue pipeline. A coordinator tags each worker with the GitHub issue
//          it serves and its role (plan, build, review); these rules decide whether that worker
//          may start, from the Hub's queued work and recorded tasks.
// Layer: Pure domain logic (no I/O) shared by Hub work submission and queue start.

import type {
  GitHubIssueRef,
  HubWorkRecord,
  ProjectTask,
  ProjectTaskPipeline,
  ProjectTaskStatus,
  ProviderKind,
} from "@synara/contracts";

export const MAX_PIPELINE_ISSUES_IN_FLIGHT = 2;

export type PipelineStartDecision =
  | { readonly kind: "allowed" }
  | { readonly kind: "refused"; readonly reason: string };

interface PipelineEntry {
  readonly issueKey: string;
  readonly role: ProjectTaskPipeline["role"];
  readonly status: ProjectTaskStatus | "queued";
  readonly provider: ProviderKind;
  readonly createdAt: string;
}

const TERMINAL_STATUSES: ReadonlySet<PipelineEntry["status"]> = new Set(["done", "cancelled"]);
const IN_PROGRESS_STATUSES: ReadonlySet<PipelineEntry["status"]> = new Set([
  "queued",
  "planned",
  "ready",
  "running",
]);

export const pipelineIssueKey = (issue: GitHubIssueRef): string =>
  `${issue.repository.toLowerCase()}#${issue.number}`;

// A reviewer is finished once it reports; the coordinator never accepts review tasks, so a
// reported review must not keep its issue in flight.
const isOpen = (entry: PipelineEntry) =>
  !TERMINAL_STATUSES.has(entry.status) && !(entry.role === "review" && entry.status === "review");

/**
 * Every pipeline worker is Hub work, so the queue records hold the tag and the provider. The task
 * a started worker received, matched by thread, holds its progress; a worker that has not started
 * yet is "queued". Failed and cancelled work never ran and does not count.
 */
function collectEntries(input: {
  readonly records: ReadonlyArray<HubWorkRecord>;
  readonly tasks: ReadonlyArray<ProjectTask>;
}): PipelineEntry[] {
  const taskByThread = new Map(
    input.tasks.flatMap((task) =>
      task.assignedThreadId === null ? [] : [[task.assignedThreadId, task] as const],
    ),
  );
  return input.records.flatMap((record) => {
    const pipeline = record.creationSpec.pipeline;
    if (!pipeline || record.state === "failed" || record.state === "cancelled") return [];
    const task = record.workerThreadId === null ? null : taskByThread.get(record.workerThreadId);
    return [
      {
        issueKey: pipelineIssueKey(pipeline.issue),
        role: pipeline.role,
        status: task?.status ?? "queued",
        provider: record.creationSpec.target.provider,
        createdAt: record.createdAt,
      },
    ];
  });
}

const refused = (reason: string): PipelineStartDecision => ({ kind: "refused", reason });

export function decidePipelineStart(input: {
  readonly request: { readonly pipeline: ProjectTaskPipeline; readonly provider: ProviderKind };
  readonly records: ReadonlyArray<HubWorkRecord>;
  readonly tasks: ReadonlyArray<ProjectTask>;
  readonly maxRepairRounds: number;
}): PipelineStartDecision {
  const { pipeline, provider } = input.request;
  const issueKey = pipelineIssueKey(pipeline.issue);
  const label = `${pipeline.issue.repository}#${pipeline.issue.number}`;
  const entries = collectEntries(input);
  const forIssue = entries.filter((entry) => entry.issueKey === issueKey);
  const ofRole = (role: PipelineEntry["role"]) => forIssue.filter((entry) => entry.role === role);

  switch (pipeline.role) {
    case "plan": {
      if (forIssue.some(isOpen)) {
        return refused(`${label} already has pipeline work in progress.`);
      }
      const otherIssuesInFlight = new Set(
        entries
          .filter((entry) => entry.issueKey !== issueKey && isOpen(entry))
          .map((e) => e.issueKey),
      );
      if (otherIssuesInFlight.size >= MAX_PIPELINE_ISSUES_IN_FLIGHT) {
        return refused(
          `At most ${MAX_PIPELINE_ISSUES_IN_FLIGHT} issues can be in progress. Finish one before planning ${label}.`,
        );
      }
      return { kind: "allowed" };
    }
    case "build": {
      if (!ofRole("plan").some((entry) => entry.status === "done")) {
        return refused(`${label} needs a plan the user has approved before building.`);
      }
      if (ofRole("build").some(isOpen)) {
        return refused(`${label} already has a build worker. Send fixes to that thread.`);
      }
      return { kind: "allowed" };
    }
    case "review": {
      const builds = ofRole("build").toSorted((a, b) => a.createdAt.localeCompare(b.createdAt));
      const build = builds.at(-1);
      if (!build || build.status !== "review") {
        return refused(`${label} has no reported build to review yet.`);
      }
      if (build.provider === provider) {
        return refused(`${label} was built with ${provider}. Review it with a different provider.`);
      }
      const reviews = ofRole("review");
      if (reviews.some((entry) => IN_PROGRESS_STATUSES.has(entry.status))) {
        return refused(`${label} already has a review in progress.`);
      }
      if (reviews.length > input.maxRepairRounds) {
        return refused(
          `${label} has used its ${input.maxRepairRounds} fix rounds. Mark it blocked and ask the user.`,
        );
      }
      return { kind: "allowed" };
    }
  }
}

/** Plan approval is the user's gate: neither the coordinator nor a worker may accept a plan task. */
export const pipelineAcceptanceRequiresUser = (task: Pick<ProjectTask, "pipeline">): boolean =>
  task.pipeline?.role === "plan";
