const legacyCancellation =
  "The user doesn't want to take this action right now. STOP what you are doing and wait for the user to tell you how to proceed.";

export const CLAUDE_CANCELLATION_INSTRUCTIONS = `
Runtime tool cancellation: Claude may emit the generic tool-result text "The user doesn't want to take this action right now" when a pending tool is cancelled by an incoming instruction or runtime interruption. That text alone is not a user refusal or an instruction to stop the task. Follow the latest actual user instruction, including an explicit stop or rejection. Explain runtime cancellation as cancellation, not as a decision the user made. Do not automatically replay a cancelled operation: inspect its outcome first if it could have had side effects.
`;

export function claudeCancellationDisplayText(input: {
  readonly text: string;
  readonly nonExecutionKind: string | undefined;
  readonly steered: boolean;
}): string | null {
  if (input.nonExecutionKind !== "cancelled" || input.text !== legacyCancellation) return null;
  return input.steered
    ? "Tool cancelled while an incoming instruction was steering this run. This tool result does not record a user refusal. Follow the latest instruction; inspect the operation's outcome before retrying."
    : "Tool cancelled by the runtime. This tool result does not identify a user refusal or the reason for cancellation. Inspect the operation's outcome before retrying.";
}
