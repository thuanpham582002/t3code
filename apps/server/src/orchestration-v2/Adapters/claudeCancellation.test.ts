import { assert, describe, it } from "@effect/vitest";
import { claudeCancellationDisplayText } from "./claudeCancellation.ts";

const text =
  "The user doesn't want to take this action right now. STOP what you are doing and wait for the user to tell you how to proceed.";
describe("Claude cancellation provenance", () => {
  it("explains cancellation during a confirmed steer", () => {
    assert.include(
      claudeCancellationDisplayText({ text, nonExecutionKind: "cancelled", steered: true }) ?? "",
      "incoming instruction",
    );
  });
  it("does not invent the cause when no steer is known", () => {
    assert.include(
      claudeCancellationDisplayText({ text, nonExecutionKind: "cancelled", steered: false }) ?? "",
      "does not identify",
    );
  });
  it("preserves actual denials, unknown provenance and other cancellation output", () => {
    assert.isNull(
      claudeCancellationDisplayText({ text, nonExecutionKind: "denied", steered: true }),
    );
    assert.isNull(
      claudeCancellationDisplayText({ text, nonExecutionKind: undefined, steered: true }),
    );
    assert.isNull(
      claudeCancellationDisplayText({
        text: "Partial result",
        nonExecutionKind: "cancelled",
        steered: true,
      }),
    );
  });
});
