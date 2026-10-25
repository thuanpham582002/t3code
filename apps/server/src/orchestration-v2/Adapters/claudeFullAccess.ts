// Oracle fork: in full access no Claude ask stops the thread. Bypass already
// allowed what it could; an ask that still reaches the callback without a
// matched ask rule is one of the CLI's own safety checks. That holds after the
// agent enters plan mode by itself too. Ask rules from the user's settings, and
// a CLI that policy moved off bypass, keep asking.
export function claudeFullAccessAutoApproves(input: {
  readonly permissionMode: string;
  readonly livePermissionMode: string | undefined;
  readonly matchedAskRule: unknown;
}): boolean {
  return (
    input.permissionMode === "bypassPermissions" &&
    (input.livePermissionMode === "bypassPermissions" || input.livePermissionMode === "plan") &&
    input.matchedAskRule === undefined
  );
}
