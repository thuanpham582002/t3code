import {
  type OrchestrationV2ThreadProjection,
  type RunId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type * as Orchestrator from "./Orchestrator.ts";
import {
  ProjectionStoreReadError,
  type ProjectionRecordField,
  type ProjectionRecords,
  type ProjectionRecordFilter,
  type ProjectionStoreV2Error,
} from "./ProjectionStore.ts";
type ControlReadService = {
  readonly getThreadRecords: <K extends ProjectionRecordField>(
    threadId: ThreadId,
    fields: ReadonlyArray<K>,
    filter?: ProjectionRecordFilter,
  ) => Effect.Effect<
    ProjectionRecords<K>,
    ProjectionStoreV2Error | Orchestrator.OrchestratorV2Error
  >;
};

/** Metadata and exact-run evidence, without historical tool output or duplicate visible bodies. */
export function readControlProjection(
  service: ControlReadService,
  threadId: ThreadId,
  options: { readonly statusOnly?: boolean; readonly runId?: RunId } = {},
) {
  return Effect.gen(function* () {
    const base = yield* service.getThreadRecords(
      threadId,
      ["runs", "runtimeRequests", "providerSessions", "providerThreads", "subagents"],
      {
        recentRunLimit: 20,
        ...(options.runId ? { includeRunId: options.runId } : {}),
        runtimeRequestStatuses: ["pending", "waiting"],
      },
    );
    const work =
      options.runId === undefined
        ? (base.runs
            .filter((r) => ["preparing", "starting", "running", "waiting"].includes(r.status))
            .at(-1) ??
          base.runs
            .filter(
              (r) => r.status !== "queued" && !(r.status === "cancelled" && r.startedAt === null),
            )
            .at(-1))
        : base.runs.find((r) => r.id === options.runId);
    if (options.runId !== undefined && work === undefined)
      return yield* new ProjectionStoreReadError({ threadId, cause: "CONTROL_RUN_NOT_FOUND" });
    const queued = base.runs.filter((r) => r.status === "queued");
    const messages = yield* service.getThreadRecords(threadId, ["messages"], {
      messageIds: queued.map((r) => r.userMessageId),
    });
    const projection: OrchestrationV2ThreadProjection = {
      ...base,
      messages: messages.messages,
      attempts: [],
      nodes: [],
      providerTurns: [],
      plans: [],
      turnItems: [],
      visibleTurnItems: [],
      checkpointScopes: [],
      checkpoints: [],
      contextHandoffs: [],
      contextTransfers: [],
      updatedAt: base.thread.updatedAt,
    };
    if (options.statusOnly) return projection;
    const runIds = work ? [work.id] : [];
    const evidence = yield* service.getThreadRecords(
      threadId,
      ["turnItems", "attempts", "providerTurns"],
      {
        relatedRunIds: runIds,
        turnItemRunIds: runIds,
        turnItemTypes: ["user_message", "assistant_message", "error"],
        turnItemPayloadBudgetBytes: 8 * 1024 * 1024,
      },
    );
    const boundary = evidence.turnItems.length
      ? Math.max(...evidence.turnItems.map((i) => i.ordinal))
      : undefined;
    // Preserve the real parent across hundreds of continuations. A byte budget
    // fails explicitly before decoding; it never substitutes a recent support message.
    const parents = yield* service.getThreadRecords(threadId, ["turnItems"], {
      turnItemTypes: ["user_message"],
      ...(boundary === undefined ? {} : { turnItemBeforeOrdinal: boundary }),
      turnItemPayloadBudgetBytes: 8 * 1024 * 1024,
    });
    const questions = yield* service.getThreadRecords(threadId, ["turnItems"], {
      turnItemTypes: ["user_input_request"],
      turnItemPayloadBudgetBytes: 8 * 1024 * 1024,
    });
    const items = new Map(
      [...parents.turnItems, ...evidence.turnItems, ...questions.turnItems].map((i) => [i.id, i]),
    );
    return {
      ...projection,
      attempts: evidence.attempts,
      providerTurns: evidence.providerTurns,
      turnItems: [...items.values()].sort((a, b) => a.ordinal - b.ordinal),
    };
  });
}

export function readControlTimeline(
  service: ControlReadService,
  threadId: ThreadId,
  options: { readonly afterOrdinal: number; readonly limit: number; readonly view: string },
) {
  return service
    .getThreadRecords(threadId, ["turnItems"], {
      turnItemAfterOrdinal: options.afterOrdinal,
      turnItemLimit: options.limit + 1,
      ...(options.view === "activity"
        ? {}
        : { turnItemTypes: ["user_message", "assistant_message", "proposed_plan"] as const }),
      turnItemPayloadBudgetBytes: 8 * 1024 * 1024,
    })
    .pipe(
      Effect.map((records) => ({
        items: records.turnItems.slice(0, options.limit),
        hasMore: records.turnItems.length > options.limit,
      })),
    );
}
