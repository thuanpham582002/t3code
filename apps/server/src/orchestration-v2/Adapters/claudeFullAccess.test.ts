import { assert, describe, it } from "@effect/vitest";
import { claudeFullAccessAutoApproves } from "./claudeFullAccess.ts";

const bypass = { permissionMode: "bypassPermissions", livePermissionMode: "bypassPermissions" };

describe("Claude full access approvals", () => {
  it("approves the CLI's own safety checks under bypass", () => {
    assert.isTrue(claudeFullAccessAutoApproves({ ...bypass, matchedAskRule: undefined }));
  });
  it("keeps asking for a user ask rule", () => {
    assert.isFalse(
      claudeFullAccessAutoApproves({
        ...bypass,
        matchedAskRule: { source: "userSettings", toolName: "Bash", ruleContent: "rm:*" },
      }),
    );
  });
  it("approves after the agent entered plan mode from full access", () => {
    assert.isTrue(
      claudeFullAccessAutoApproves({
        permissionMode: "bypassPermissions",
        livePermissionMode: "plan",
        matchedAskRule: undefined,
      }),
    );
  });
  it("keeps asking once policy moved the CLI off bypass", () => {
    for (const livePermissionMode of ["default", "auto", "acceptEdits", undefined]) {
      assert.isFalse(
        claudeFullAccessAutoApproves({
          permissionMode: "bypassPermissions",
          livePermissionMode,
          matchedAskRule: undefined,
        }),
      );
    }
  });
  it("leaves other permission modes to a person", () => {
    for (const permissionMode of ["default", "acceptEdits", "auto", "dontAsk", "plan"]) {
      assert.isFalse(
        claudeFullAccessAutoApproves({
          permissionMode,
          livePermissionMode: permissionMode,
          matchedAskRule: undefined,
        }),
      );
    }
  });
});
