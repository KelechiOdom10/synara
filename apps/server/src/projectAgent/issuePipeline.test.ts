import {
  HubWorkRecord,
  ProjectGoalId,
  ProjectId,
  ProjectTaskId,
  ThreadId,
  type ProjectTask,
  type ProjectTaskPipeline,
  type ProjectTaskStatus,
  type ProviderKind,
} from "@synara/contracts";
import { Schema } from "effect";
import { describe, expect, it } from "vitest";

import {
  decidePipelineStart,
  pipelineAcceptanceRequiresUser,
  type PipelineStartDecision,
} from "./issuePipeline.ts";

const HUB = ProjectId.makeUnsafe("hub-1");
const issue3 = { repository: "kelechiodom/work-achievements-tracker", number: 3 };
const issue4 = { repository: "kelechiodom/work-achievements-tracker", number: 4 };
const issue5 = { repository: "kelechiodom/work-achievements-tracker", number: 5 };

type Worker = {
  readonly pipeline: ProjectTaskPipeline;
  readonly provider: ProviderKind;
  /** Absent while the work is still queued. */
  readonly taskStatus?: ProjectTaskStatus;
  readonly state?: HubWorkRecord["state"];
};

let sequence = 0;

function hub(workers: ReadonlyArray<Worker>) {
  const records: HubWorkRecord[] = [];
  const tasks: ProjectTask[] = [];
  for (const worker of workers) {
    sequence += 1;
    const createdAt = new Date(Date.UTC(2026, 9, 3, 12, 0, sequence)).toISOString();
    const threadId = worker.taskStatus ? ThreadId.makeUnsafe(`worker-${sequence}`) : null;
    records.push(
      Schema.decodeUnknownSync(HubWorkRecord)({
        id: `hub-work:${sequence}`,
        projectId: HUB,
        targetProjectId: HUB,
        requestId: `request-${sequence}`,
        scopeKey: `scope-${sequence}`,
        fingerprint: `fingerprint-${sequence}`,
        taskIndex: 0,
        sourceThreadId: "coordinator",
        sourceMessageId: `message-${sequence}`,
        sourceMessages: [],
        creationSpec: {
          prompt: "Do the work",
          target: { provider: worker.provider, model: "fixture" },
          pipeline: worker.pipeline,
        },
        title: "Pipeline worker",
        workerThreadId: threadId,
        state: worker.state ?? (threadId ? "working" : "queued"),
        queueReason: null,
        resultSummary: null,
        progress: null,
        slotHeld: threadId !== null,
        admittedAt: null,
        admissionCommandId: null,
        admissionMessageId: null,
        admissionPreviousState: null,
        admissionPreviousSlotHeld: false,
        revision: 0,
        createdAt,
        updatedAt: createdAt,
      }),
    );
    if (threadId && worker.taskStatus) {
      tasks.push({
        id: ProjectTaskId.makeUnsafe(`task-${sequence}`),
        projectId: HUB,
        goalId: ProjectGoalId.makeUnsafe("goal-1"),
        title: "Pipeline worker",
        description: null,
        acceptanceCriteria: null,
        status: worker.taskStatus,
        dependsOnTaskIds: [],
        assignedThreadId: threadId,
        pipeline: worker.pipeline,
        repairCount: 0,
        archivedAt: null,
        revision: 1,
        createdAt,
        updatedAt: createdAt,
      });
    }
  }
  return { records, tasks };
}

function decide(
  workers: ReadonlyArray<Worker>,
  pipeline: ProjectTaskPipeline,
  provider: ProviderKind = "claudeAgent",
): PipelineStartDecision {
  return decidePipelineStart({
    request: { pipeline, provider },
    ...hub(workers),
    maxRepairRounds: 2,
  });
}

const allowed = { kind: "allowed" };
const refusedWith = (text: string) => ({ kind: "refused", reason: expect.stringContaining(text) });

describe("issue pipeline gates", () => {
  it("plans a new issue when nothing else is in flight", () => {
    expect(decide([], { issue: issue3, role: "plan" })).toEqual(allowed);
  });

  it("refuses a second plan while the issue still has open work, including queued work", () => {
    expect(
      decide([{ pipeline: { issue: issue3, role: "plan" }, provider: "claudeAgent" }], {
        issue: issue3,
        role: "plan",
      }),
    ).toEqual(refusedWith("already has pipeline work"));
  });

  it("matches the issue repository case-insensitively", () => {
    expect(
      decide(
        [
          {
            pipeline: {
              issue: { ...issue3, repository: "KelechiOdom/Work-Achievements-Tracker" },
              role: "plan",
            },
            provider: "claudeAgent",
            taskStatus: "running",
          },
        ],
        { issue: issue3, role: "plan" },
      ),
    ).toEqual(refusedWith("already has pipeline work"));
  });

  it("caps issues in flight at two", () => {
    const busy: Worker[] = [
      { pipeline: { issue: issue3, role: "plan" }, provider: "claudeAgent", taskStatus: "running" },
      { pipeline: { issue: issue4, role: "build" }, provider: "codex", taskStatus: "review" },
    ];
    expect(decide(busy, { issue: issue5, role: "plan" })).toEqual(refusedWith("At most 2 issues"));
  });

  it("ignores failed and cancelled work when counting issues in flight", () => {
    const settled: Worker[] = [
      { pipeline: { issue: issue3, role: "plan" }, provider: "claudeAgent", state: "failed" },
      { pipeline: { issue: issue4, role: "plan" }, provider: "claudeAgent", state: "cancelled" },
    ];
    expect(decide(settled, { issue: issue5, role: "plan" })).toEqual(allowed);
  });

  it("frees an issue's slot once its build is resolved, even with reported reviews left behind", () => {
    const finished: Worker[] = [
      { pipeline: { issue: issue3, role: "plan" }, provider: "claudeAgent", taskStatus: "done" },
      { pipeline: { issue: issue3, role: "build" }, provider: "codex", taskStatus: "done" },
      {
        pipeline: { issue: issue3, role: "review" },
        provider: "claudeAgent",
        taskStatus: "review",
      },
      { pipeline: { issue: issue4, role: "plan" }, provider: "claudeAgent", taskStatus: "running" },
    ];
    expect(decide(finished, { issue: issue5, role: "plan" })).toEqual(allowed);
  });

  it("builds only after the plan is done", () => {
    const reported: Worker = {
      pipeline: { issue: issue3, role: "plan" },
      provider: "claudeAgent",
      taskStatus: "review",
    };
    expect(decide([reported], { issue: issue3, role: "build" }, "codex")).toEqual(
      refusedWith("approved before building"),
    );
    expect(
      decide([{ ...reported, taskStatus: "done" }], { issue: issue3, role: "build" }, "codex"),
    ).toEqual(allowed);
  });

  it("keeps one build worker per issue", () => {
    const workers: Worker[] = [
      { pipeline: { issue: issue3, role: "plan" }, provider: "claudeAgent", taskStatus: "done" },
      { pipeline: { issue: issue3, role: "build" }, provider: "codex", taskStatus: "running" },
    ];
    expect(decide(workers, { issue: issue3, role: "build" }, "codex")).toEqual(
      refusedWith("already has a build worker"),
    );
  });

  describe("review", () => {
    const planned: Worker = {
      pipeline: { issue: issue3, role: "plan" },
      provider: "claudeAgent",
      taskStatus: "done",
    };
    const reportedBuild: Worker = {
      pipeline: { issue: issue3, role: "build" },
      provider: "codex",
      taskStatus: "review",
    };
    const review = (taskStatus: ProjectTaskStatus): Worker => ({
      pipeline: { issue: issue3, role: "review" },
      provider: "claudeAgent",
      taskStatus,
    });

    it("waits for the build to report", () => {
      expect(
        decide([planned, { ...reportedBuild, taskStatus: "running" }], {
          issue: issue3,
          role: "review",
        }),
      ).toEqual(refusedWith("no reported build"));
    });

    it("requires a different provider from the builder", () => {
      expect(decide([planned, reportedBuild], { issue: issue3, role: "review" }, "codex")).toEqual(
        refusedWith("Review it with a different provider"),
      );
      expect(
        decide([planned, reportedBuild], { issue: issue3, role: "review" }, "claudeAgent"),
      ).toEqual(allowed);
    });

    it("refuses a second review while one is running", () => {
      expect(
        decide([planned, reportedBuild, review("running")], { issue: issue3, role: "review" }),
      ).toEqual(refusedWith("review in progress"));
    });

    it("allows the first review plus two fix rounds, then stops", () => {
      const twoReviews = [planned, reportedBuild, review("review"), review("review")];
      expect(decide(twoReviews, { issue: issue3, role: "review" })).toEqual(allowed);
      expect(decide([...twoReviews, review("review")], { issue: issue3, role: "review" })).toEqual(
        refusedWith("used its 2 fix rounds"),
      );
    });
  });

  it("reserves plan acceptance for the user", () => {
    expect(pipelineAcceptanceRequiresUser({ pipeline: { issue: issue3, role: "plan" } })).toBe(
      true,
    );
    expect(pipelineAcceptanceRequiresUser({ pipeline: { issue: issue3, role: "build" } })).toBe(
      false,
    );
    expect(pipelineAcceptanceRequiresUser({ pipeline: null })).toBe(false);
  });
});
