import {
  AuthOrchestrationReadScope,
  EnvironmentHttpApi,
  ThreadId,
  TurnItemId,
  RunId,
  type OrchestrationProjectShell,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as SqlClient from "effect/sql/SqlClient";
import * as Semaphore from "effect/Semaphore";

import {
  annotateEnvironmentRequest,
  failEnvironmentInternal,
  failEnvironmentInvalidRequest,
  failEnvironmentNotFound,
  requireEnvironmentScope,
} from "../auth/http.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import { traceLocalHandlerWork } from "../cloud/traceRelayRequest.ts";
import * as OrchestrationEventStore from "../persistence/OrchestrationEventStore.ts";
import * as ProjectEnrichmentService from "../project/ProjectEnrichmentService.ts";
import {
  buildBoundedThreadProjection,
  decodeThreadHistoryCursor,
  InvalidThreadHistoryCursorError,
  THREAD_HISTORY_SNAPSHOT_ROW_LIMIT,
  THREAD_HISTORY_PAGE_POLICY,
  OLDER_THREAD_USER_TURN_LIMIT,
} from "./threadHistoryPaging.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ProjectStore from "./ProjectStore.ts";
import { buildActiveShellSnapshot, loadShellSnapshotParts } from "./ShellStream.ts";
import { boundedSnapshotResponseFields } from "./ThreadStream.ts";
import { projectThreadProjectionForWire } from "./WireProjection.ts";

const isThreadTranscriptTooLargeError = Schema.is(
  ThreadManagementService.ThreadTranscriptTooLargeError,
);

function isThreadNotFound(error: unknown): boolean {
  return (
    Predicate.hasProperty(error, "cause") &&
    Predicate.hasProperty(error.cause, "_tag") &&
    error.cause._tag === "ProjectionStoreThreadNotFoundError"
  );
}

export const layer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "orchestration",
  Effect.fnUntraced(function* (handlers) {
    const sql = yield* SqlClient.SqlClient;
    const environment = yield* ServerEnvironment.ServerEnvironmentIdentity;
    const reads = yield* Semaphore.make(2);
    const threadManagement = yield* ThreadManagementService.ThreadManagementService;
    const applicationEvents = yield* OrchestrationEventStore.OrchestrationEventStore;
    const projectStore = yield* ProjectStore.ProjectStoreV2;
    const projectEnrichment = yield* ProjectEnrichmentService.ProjectEnrichmentService;

    const enrichProjectShells = Effect.fn("http.orchestration.enrichProjectShells")(
      (projects: ReadonlyArray<OrchestrationProjectShell>) =>
        Effect.forEach(
          projects,
          (project) =>
            // Use immediately available enrichment only. Awaiting git-backed
            // identity resolution can exceed the client shell-snapshot budget
            // (ProcessRunner allows probes up to one minute).
            projectEnrichment.getAvailable(project.workspaceRoot).pipe(
              Effect.map((enrichment) => ({
                ...project,
                repositoryIdentity: enrichment.repositoryIdentity,
              })),
            ),
          { concurrency: 16 },
        ),
    );

    const loadShellSnapshot = Effect.fn("http.orchestration.loadShellSnapshot")(function* () {
      const base = buildActiveShellSnapshot(
        yield* loadShellSnapshotParts({
          sql,
          readThreads: threadManagement.readShellSnapshot({ location: "active" }),
          listProjects: projectStore.listShells(),
          latestSequence: applicationEvents.latestApplicationSequence,
        }),
      );
      const projects = yield* enrichProjectShells(base.projects);
      return { ...base, projects };
    });

    const loadThreadSnapshot = Effect.fn("http.orchestration.loadThreadSnapshot")(function* (
      threadId: Parameters<typeof threadManagement.getThreadSnapshot>[0],
      failureReason:
        | "orchestration_thread_snapshot_failed"
        | "orchestration_thread_bounded_snapshot_failed"
        | "orchestration_thread_history_failed",
    ) {
      // Legacy full snapshots have an explicit pre-decode budget. Large histories
      // must use control or paginated history reads instead of exhausting V8.
      const size = yield* sql<{
        bytes: number;
      }>`SELECT COALESCE(SUM(length(CAST(payload_json AS BLOB))),0) AS bytes
        FROM orchestration_v2_projection_turn_items WHERE thread_id = ${threadId}`.pipe(
        Effect.catch((cause) => failEnvironmentInternal(failureReason, cause)),
      );
      if ((size[0]?.bytes ?? 0) > 8 * 1024 * 1024)
        return yield* failEnvironmentInternal(
          "orchestration_thread_snapshot_failed",
          "Full snapshot exceeds byte budget; use /control or /timeline",
        );
      return yield* reads
        .withPermits(1)(threadManagement.getThreadSnapshot(threadId))
        .pipe(
          Effect.map((snapshot) => ({
            ...snapshot,
            projection: projectThreadProjectionForWire(snapshot.projection),
          })),
          Effect.catch(
            Effect.fnUntraced(function* (error) {
              if (isThreadNotFound(error)) {
                return yield* failEnvironmentNotFound("thread_not_found");
              }
              return yield* failEnvironmentInternal(failureReason, error);
            }),
          ),
        );
    });

    const loadThreadSnapshotWindow = Effect.fn("http.orchestration.loadThreadSnapshotWindow")(
      function* (
        threadId: Parameters<typeof threadManagement.getThreadSnapshot>[0],
        anchorItemId?: Parameters<
          typeof threadManagement.getThreadSnapshotWindow
        >[1]["anchorItemId"],
        anchorThreadId?: Parameters<
          typeof threadManagement.getThreadSnapshotWindow
        >[1]["anchorThreadId"],
      ) {
        return yield* threadManagement
          .getThreadSnapshotWindow(threadId, {
            rowLimit: THREAD_HISTORY_SNAPSHOT_ROW_LIMIT,
            userTurnLimit:
              anchorItemId === undefined
                ? THREAD_HISTORY_PAGE_POLICY.maxUserTurns
                : OLDER_THREAD_USER_TURN_LIMIT,
            ...(anchorItemId === undefined ? {} : { anchorItemId }),
            ...(anchorThreadId === undefined ? {} : { anchorThreadId }),
          })
          .pipe(
            Effect.map((snapshot) => ({
              ...snapshot,
              projection: projectThreadProjectionForWire(snapshot.projection),
            })),
            Effect.catch(
              Effect.fnUntraced(function* (error) {
                if (isThreadNotFound(error)) {
                  return yield* failEnvironmentNotFound("thread_not_found");
                }
                return yield* failEnvironmentInternal("orchestration_thread_history_failed", error);
              }),
            ),
          );
      },
    );

    return handlers
      .handle(
        "threadControlSnapshot",
        Effect.fn("environment.orchestration.threadControlSnapshot")(function* (args) {
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          const projection = yield* reads
            .withPermits(1)(
              threadManagement.getControlProjection(args.params.threadId, {
                statusOnly: args.query.statusOnly === "1",
                ...(args.query.runId ? { runId: RunId.make(args.query.runId) } : {}),
              }),
            )
            .pipe(
              Effect.catch(
                Effect.fnUntraced(function* (cause) {
                  if (isThreadNotFound(cause))
                    return yield* failEnvironmentNotFound("thread_not_found");
                  return yield* failEnvironmentInternal(
                    "orchestration_thread_snapshot_failed",
                    cause,
                  );
                }),
              ),
            );
          return { projection };
        }),
      )
      .handle(
        "threadControlWait",
        Effect.fn("environment.orchestration.threadControlWait")(function* (args) {
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          const thread = yield* threadManagement
            .getThreadRecords(args.params.threadId, [])
            .pipe(
              Effect.catch((cause) =>
                failEnvironmentInternal("orchestration_thread_snapshot_failed", cause),
              ),
            );
          const timeoutMs = Number(args.query.timeoutMs ?? 30000);
          if (!Number.isInteger(timeoutMs) || timeoutMs < 0 || timeoutMs > 60000)
            return yield* failEnvironmentInvalidRequest("invalid_command");
          return yield* threadManagement
            .waitForThread({
              projectId: thread.thread.projectId,
              threadId: args.params.threadId,
              timeoutMs,
              ...(args.query.runId ? { runId: RunId.make(args.query.runId) } : {}),
            })
            .pipe(
              Effect.catch((cause) =>
                failEnvironmentInternal("orchestration_thread_snapshot_failed", cause),
              ),
            );
        }),
      )
      .handle(
        "threadTimelinePage",
        Effect.fn("environment.orchestration.threadTimelinePage")(function* (args) {
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          const afterOrdinal = Number(args.query.afterOrdinal ?? 0),
            limit = Number(args.query.limit ?? 200);
          if (
            !Number.isInteger(afterOrdinal) ||
            afterOrdinal < 0 ||
            !Number.isInteger(limit) ||
            limit < 1 ||
            limit > 1000
          )
            return yield* failEnvironmentInvalidRequest("invalid_command");
          return yield* reads
            .withPermits(1)(
              threadManagement.getControlTimeline(args.params.threadId, {
                afterOrdinal,
                limit,
                view: args.query.view === "activity" ? "activity" : "messages",
              }),
            )
            .pipe(
              Effect.catch((cause) =>
                failEnvironmentInternal("orchestration_thread_history_failed", cause),
              ),
            );
        }),
      )
      .handle(
        "shellSnapshot",
        Effect.fn("environment.orchestration.shellSnapshot")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          return yield* loadShellSnapshot().pipe(
            traceLocalHandlerWork,
            Effect.catch((cause) =>
              failEnvironmentInternal("orchestration_snapshot_failed", cause),
            ),
          );
        }),
      )
      .handle(
        "threadSnapshot",
        Effect.fn("environment.orchestration.threadSnapshot")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          const snapshot = yield* loadThreadSnapshot(
            args.params.threadId,
            "orchestration_thread_snapshot_failed",
          ).pipe(traceLocalHandlerWork);
          return {
            snapshotSequence: snapshot.snapshotSequence,
            projection: snapshot.projection,
          };
        }),
      )
      .handle(
        "threadTranscript",
        Effect.fn("environment.orchestration.threadTranscript")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          return yield* threadManagement.getThreadTranscript(args.params.threadId).pipe(
            Effect.provideService(ServerEnvironment.ServerEnvironmentIdentity, environment),
            traceLocalHandlerWork,
            Effect.catch(
              Effect.fnUntraced(function* (error) {
                if (isThreadTranscriptTooLargeError(error)) {
                  return yield* failEnvironmentInvalidRequest("thread_transcript_too_large");
                }
                if (isThreadNotFound(error)) {
                  return yield* failEnvironmentNotFound("thread_not_found");
                }
                return yield* failEnvironmentInternal(
                  "orchestration_thread_snapshot_failed",
                  error,
                );
              }),
            ),
          );
        }),
      )
      .handle(
        "threadBoundedSnapshot",
        Effect.fn("environment.orchestration.threadBoundedSnapshot")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          const snapshot = yield* loadThreadSnapshotWindow(args.params.threadId).pipe(
            traceLocalHandlerWork,
          );
          const bounded = buildBoundedThreadProjection({
            projection: snapshot.projection,
            snapshotSequence: snapshot.snapshotSequence,
          });
          return {
            snapshotSequence: snapshot.snapshotSequence,
            ...boundedSnapshotResponseFields({
              bounded,
              compactTurnItems: args.query.compactTurnItems === "1",
            }),
          };
        }),
      )
      .handle(
        "threadHistoryPage",
        Effect.fn("environment.orchestration.threadHistoryPage")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          try {
            decodeThreadHistoryCursor(args.query.cursor);
          } catch (cause) {
            if (cause instanceof InvalidThreadHistoryCursorError)
              return yield* failEnvironmentInvalidRequest("invalid_history_cursor");
            return yield* failEnvironmentInternal("orchestration_thread_history_failed", cause);
          }
          return yield* threadManagement
            .getThreadHistoryPage(
              args.params.threadId,
              args.query.cursor,
              args.query.throughEntryId,
              args.query.view === "conversation",
            )
            .pipe(
              traceLocalHandlerWork,
              Effect.catch(
                Effect.fnUntraced(function* (cause) {
                  if (isThreadNotFound(cause))
                    return yield* failEnvironmentNotFound("thread_not_found");
                  return yield* failEnvironmentInternal(
                    "orchestration_thread_history_failed",
                    cause,
                  );
                }),
              ),
            );
        }),
      );
  }),
);
