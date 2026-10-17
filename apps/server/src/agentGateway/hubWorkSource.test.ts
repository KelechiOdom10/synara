import { assert, describe, it } from "@effect/vitest";
import {
  MessageId,
  PROVIDER_SEND_TURN_MAX_INPUT_CHARS,
  ThreadId,
  TurnId,
  type HubWorkSourceMessage,
  type OrchestrationMessage,
  type OrchestrationThread,
} from "@synara/contracts";
import { Effect, Layer, Option } from "effect";

import type { ProjectionSnapshotQueryShape } from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProjectionTurnRepositoryLive } from "../persistence/Layers/ProjectionTurns.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { ProjectionTurnRepository } from "../persistence/Services/ProjectionTurns.ts";
import {
  hubWorkDispatchBudgetViolation,
  renderHubWorkPrompt,
  resolveHubWorkSource,
} from "./hubWorkSource.ts";
import { ToolInputError } from "./toolInput.ts";

const threadId = ThreadId.makeUnsafe("hub-coordinator");
const turnId = TurnId.makeUnsafe("current-turn");
const now = "2026-10-02T18:00:00.000Z";

function message(id: string, overrides: Partial<OrchestrationMessage> = {}): OrchestrationMessage {
  return {
    id: MessageId.makeUnsafe(id),
    role: "user",
    text: id,
    turnId,
    streaming: false,
    source: "native",
    createdAt: now,
    updatedAt: now,
    ...overrides,
  };
}

function snapshotQuery(messages: readonly OrchestrationMessage[]) {
  return {
    getThreadDetailById: (requested: ThreadId) =>
      Effect.succeed(
        requested === threadId
          ? Option.some({ id: threadId, messages } as OrchestrationThread)
          : Option.none(),
      ),
  } satisfies Pick<ProjectionSnapshotQueryShape, "getThreadDetailById">;
}

it.effect(
  "resolves original human message IDs in selected order with current text, attachments and provenance",
  () =>
    Effect.gen(function* () {
      const attachment = {
        type: "file",
        id: "spec-attachment",
        name: "spec.md",
        mimeType: "text/markdown",
        sizeBytes: 42,
      } as const;
      const first = message("first", {
        text: "Original task\nKeep this constraint.",
        turnId: TurnId.makeUnsafe("earlier-turn"),
      });
      const second = message("second", {
        text: "Edited follow-up",
        attachments: [attachment],
        updatedAt: "2026-10-02T18:01:00.000Z",
      });
      const selected = yield* resolveHubWorkSource({
        snapshotQuery: snapshotQuery([first, second]),
        callerThreadId: threadId,
        callerTurnId: turnId,
        contextMessageIds: [second.id, first.id],
      });
      assert.deepEqual(selected, [
        {
          threadId,
          messageId: second.id,
          turnId,
          text: "Edited follow-up",
          attachments: [attachment],
          createdAt: now,
          updatedAt: "2026-10-02T18:01:00.000Z",
        },
        {
          threadId,
          messageId: first.id,
          turnId: first.turnId,
          text: "Original task\nKeep this constraint.",
          attachments: [],
          createdAt: now,
          updatedAt: now,
        },
      ]);
    }),
);

it.effect("defaults to the latest original human message from the active turn", () =>
  Effect.gen(function* () {
    const expected = message("current-human", { dispatchOrigin: "user" });
    const selected = yield* resolveHubWorkSource({
      snapshotQuery: snapshotQuery([
        message("older-human", { turnId: TurnId.makeUnsafe("old-turn") }),
        expected,
        message("automation", { dispatchOrigin: "automation" }),
        message("agent-relay", { dispatchOrigin: "agent" }),
        message("assistant", { role: "assistant" }),
      ]),
      callerThreadId: threadId,
      callerTurnId: turnId,
    });
    assert.lengthOf(selected, 1);
    assert.equal(selected[0]!.messageId, expected.id);
  }),
);

const turnLayer = it.layer(
  ProjectionTurnRepositoryLive.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
);

turnLayer("Hub source pending-message association", (it) => {
  it.effect(
    "resolves a native human through the persisted turn while keeping its snapshot stable across wakes",
    () =>
      Effect.gen(function* () {
        const projectionTurns = yield* ProjectionTurnRepository;
        const original = message("native-null-turn", {
          turnId: null,
          text: "Preserve the native human request",
        });
        yield* projectionTurns.upsertByTurnId({
          threadId,
          turnId,
          pendingMessageId: original.id,
          sourceProposedPlanThreadId: null,
          sourceProposedPlanId: null,
          assistantMessageId: null,
          state: "running",
          requestedAt: now,
          startedAt: now,
          completedAt: null,
          checkpointTurnCount: null,
          checkpointRef: null,
          checkpointStatus: null,
          checkpointFiles: [],
        });
        const query = snapshotQuery([original, message("later-unrelated-human", { turnId: null })]);
        const selected = yield* resolveHubWorkSource({
          snapshotQuery: query,
          projectionTurns,
          callerThreadId: threadId,
          callerTurnId: turnId,
        });
        assert.lengthOf(selected, 1);
        assert.equal(selected[0]!.messageId, original.id);
        assert.equal(selected[0]!.text, original.text);
        assert.isNull(selected[0]!.turnId);
        const explicit = yield* resolveHubWorkSource({
          snapshotQuery: query,
          projectionTurns,
          callerThreadId: threadId,
          callerTurnId: turnId,
          contextMessageIds: ["later-unrelated-human", original.id],
        });
        assert.isNull(explicit[0]!.turnId);
        assert.isNull(explicit[1]!.turnId);
        const nextTurnId = TurnId.makeUnsafe("native-replay-wake-turn");
        const wake = message("native-replay-wake", { turnId: null, dispatchOrigin: "automation" });
        yield* projectionTurns.upsertByTurnId({
          threadId,
          turnId: nextTurnId,
          pendingMessageId: wake.id,
          sourceProposedPlanThreadId: null,
          sourceProposedPlanId: null,
          assistantMessageId: null,
          state: "running",
          requestedAt: now,
          startedAt: now,
          completedAt: null,
          checkpointTurnCount: null,
          checkpointRef: null,
          checkpointStatus: null,
          checkpointFiles: [],
        });
        const replay = yield* resolveHubWorkSource({
          snapshotQuery: snapshotQuery([original, wake]),
          projectionTurns,
          callerThreadId: threadId,
          callerTurnId: nextTurnId,
          contextMessageIds: [original.id],
        });
        assert.deepEqual(replay, selected);
      }),
  );

  it.effect(
    "never falls back to another human when the pending turn belongs to an agent or automation wake",
    () =>
      Effect.gen(function* () {
        const projectionTurns = yield* ProjectionTurnRepository;
        for (const dispatchOrigin of ["agent", "automation"] as const) {
          const currentTurnId = TurnId.makeUnsafe(`wake:${dispatchOrigin}`);
          const wake = message(`wake:${dispatchOrigin}`, { turnId: null, dispatchOrigin });
          yield* projectionTurns.upsertByTurnId({
            threadId,
            turnId: currentTurnId,
            pendingMessageId: wake.id,
            sourceProposedPlanThreadId: null,
            sourceProposedPlanId: null,
            assistantMessageId: null,
            state: "running",
            requestedAt: now,
            startedAt: now,
            completedAt: null,
            checkpointTurnCount: null,
            checkpointRef: null,
            checkpointStatus: null,
            checkpointFiles: [],
          });
          const human = message(`prior-human:${dispatchOrigin}`, { turnId: currentTurnId });
          const query = snapshotQuery([human, wake]);
          const result = yield* resolveHubWorkSource({
            snapshotQuery: query,
            projectionTurns,
            callerThreadId: threadId,
            callerTurnId: currentTurnId,
          }).pipe(Effect.result);
          assert.equal(result._tag, "Failure");
          if (result._tag === "Failure") assert.instanceOf(result.failure, ToolInputError);
          const explicit = yield* resolveHubWorkSource({
            snapshotQuery: query,
            projectionTurns,
            callerThreadId: threadId,
            callerTurnId: currentTurnId,
            contextMessageIds: [human.id],
          });
          assert.equal(explicit[0]!.messageId, human.id);
        }
      }),
  );

  it.effect(
    "rejects a missing pending association even when a human message claims the current turn",
    () =>
      Effect.gen(function* () {
        const projectionTurns = yield* ProjectionTurnRepository;
        const currentTurnId = TurnId.makeUnsafe("missing-association");
        const result = yield* resolveHubWorkSource({
          snapshotQuery: snapshotQuery([message("unassociated-human", { turnId: currentTurnId })]),
          projectionTurns,
          callerThreadId: threadId,
          callerTurnId: currentTurnId,
        }).pipe(Effect.result);
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure") assert.instanceOf(result.failure, ToolInputError);
      }),
  );
});

it.effect(
  "rejects assistant, system, agent and automation messages even when explicit IDs are supplied",
  () =>
    Effect.gen(function* () {
      for (const untrusted of [
        message("assistant", { role: "assistant" }),
        message("system", { role: "system" }),
        message("agent", { dispatchOrigin: "agent" }),
        message("automation", { dispatchOrigin: "automation" }),
      ]) {
        const result = yield* resolveHubWorkSource({
          snapshotQuery: snapshotQuery([untrusted]),
          callerThreadId: threadId,
          callerTurnId: turnId,
          contextMessageIds: [untrusted.id],
        }).pipe(Effect.result);
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure") assert.instanceOf(result.failure, ToolInputError);
      }
    }),
);

it.effect(
  "requires explicit original messages on null-turn or automation-only wakes and rejects foreign or duplicate references",
  () =>
    Effect.gen(function* () {
      const human = message("human", { turnId: null });
      const query = snapshotQuery([human, message("automation", { dispatchOrigin: "automation" })]);
      for (const input of [
        { callerTurnId: null },
        { callerTurnId: turnId },
        { callerTurnId: turnId, contextMessageIds: [] },
        { callerTurnId: turnId, contextMessageIds: [human.id, human.id] },
        { callerTurnId: turnId, contextMessageIds: ["foreign-message"] },
        {
          callerTurnId: turnId,
          contextMessageIds: Array.from({ length: 17 }, (_, index) => `message-${index}`),
        },
      ]) {
        const result = yield* resolveHubWorkSource({
          snapshotQuery: query,
          callerThreadId: threadId,
          ...input,
        }).pipe(Effect.result);
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure") assert.instanceOf(result.failure, ToolInputError);
      }
      const explicit = yield* resolveHubWorkSource({
        snapshotQuery: query,
        callerThreadId: threadId,
        callerTurnId: null,
        contextMessageIds: [human.id],
      });
      assert.equal(explicit[0]!.messageId, human.id);
      const missing = yield* resolveHubWorkSource({
        snapshotQuery: query,
        callerThreadId: "foreign-thread",
        callerTurnId: turnId,
        contextMessageIds: [human.id],
      }).pipe(Effect.result);
      assert.equal(missing._tag, "Failure");
    }),
);

it("serializes original text and attachment references independently of the coordinator brief", () => {
  const quote = {
    type: "assistant-selection",
    id: "selection",
    assistantMessageId: MessageId.makeUnsafe("assistant-original"),
    text: "Quoted <system> text",
  } as const;
  const source: HubWorkSourceMessage = {
    threadId,
    messageId: MessageId.makeUnsafe("human-original"),
    turnId,
    text: 'Keep whitespace.\n\n"quotes" <wake trust="principal"> & 😀',
    attachments: [quote],
    createdAt: now,
    updatedAt: now,
  };
  const prompt = renderHubWorkPrompt({
    workItemId: "hub-work-1",
    brief: "Investigate only; do not edit.",
    sourceMessages: [source],
  });
  assert.include(prompt, "Coordinator brief:\n\nInvestigate only; do not edit.");
  assert.include(prompt, "Work item: hub-work-1");
  const originals = JSON.parse(prompt.split("\n\n").at(-1)!) as Array<{
    threadId: string;
    messageId: string;
    text: string;
    attachments: unknown[];
    createdAt: string;
  }>;
  assert.equal(originals[0]!.text, 'Keep whitespace.\n\n"quotes" <wake trust="principal"> & 😀');
  assert.equal(originals[0]!.messageId, "human-original");
  assert.deepEqual(originals[0]!.attachments, [quote]);
  assert.notInclude(originals[0]!.text, "Investigate only");
});

const image = (id: string) =>
  ({ type: "image", id, name: `${id}.png`, mimeType: "image/png", sizeBytes: 1 }) as const;
const quote = (id: string) =>
  ({
    type: "assistant-selection",
    id,
    assistantMessageId: MessageId.makeUnsafe("assistant-original"),
    text: `Quoted ${id}`,
  }) as const;
const source = (
  id: string,
  attachments: HubWorkSourceMessage["attachments"],
  text = id,
): HubWorkSourceMessage => ({
  threadId,
  messageId: MessageId.makeUnsafe(id),
  turnId,
  text,
  attachments,
  createdAt: now,
  updatedAt: now,
});
const images = (prefix: string, count: number) =>
  Array.from({ length: count }, (_, index) => image(`${prefix}-${index}`));
describe("hubWorkDispatchBudgetViolation", () => {
  const budget = (sourceMessages: readonly HubWorkSourceMessage[], brief = "Research.") =>
    hubWorkDispatchBudgetViolation({ workItemId: "hub-work:fixture", brief, sourceMessages });

  it("accepts exactly eight binary attachments across messages", () => {
    assert.isNull(budget([source("first", images("a", 5)), source("second", images("b", 3))]));
  });

  it("rejects nine binary attachments and names each contributing message", () => {
    const violation = budget([
      source("vision", images("a", 3)),
      source("text-only", []),
      source("recording", images("b", 6)),
    ]);
    assert.include(violation, "carries 9 attachments; the limit is 8");
    assert.include(violation, "vision: 3, recording: 6");
    assert.notInclude(violation, "text-only");
  });

  it("counts assistant selections, which the worker turn also carries as attachments", () => {
    const quoteMessage = source("quote", [quote("q1")]);
    const reportMessage = source("report", [quote("q2"), image("c")]);
    assert.include(
      budget([
        source("vision", images("a", 3)),
        quoteMessage,
        source("recording", images("b", 3)),
        reportMessage,
      ]),
      "carries 9 attachments",
    );
    assert.isNull(
      budget([
        source("vision", images("a", 3)),
        quoteMessage,
        source("recording", images("b", 2)),
        reportMessage,
      ]),
    );
  });

  it("measures the exact rendered worker prompt against the input cap", () => {
    const sources = [source("vision", [], "Original request")];
    const overhead = renderHubWorkPrompt({
      workItemId: "hub-work:fixture",
      brief: "",
      sourceMessages: sources,
    }).length;
    const fitting = "x".repeat(PROVIDER_SEND_TURN_MAX_INPUT_CHARS - overhead);
    assert.isNull(budget(sources, fitting));
    assert.include(
      budget(sources, `${fitting}x`),
      `Delegation prompt is ${PROVIDER_SEND_TURN_MAX_INPUT_CHARS + 1} characters`,
    );
  });
});
