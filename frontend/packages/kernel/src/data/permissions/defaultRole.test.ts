import { describe, expect, it } from "vitest";
import { createRoot, flush } from "solid-js";
import { Actions, type components } from "@kui/api";
import { grantsAllow, grantsAllowAny, grantsFromWire } from "./store.js";
import { createSession } from "../../state/session.js";

type Grant = components["schemas"]["PermissionDto"];
const defaults = [
  { resource: "TOPIC", clusters: ["*"], actions: ["VIEW", "CREATE"], value: ".*", defaultRole: true },
  { resource: "CONNECT", clusters: ["*"], actions: ["EDIT"], value: "payments", defaultRole: true },
  { resource: "AUDIT", clusters: ["*"], actions: ["VIEW"], defaultRole: true },
] satisfies readonly Grant[];
const explicit = { resource: "CONSUMER", clusters: ["prod"], actions: ["VIEW"], value: "checkout", defaultRole: false } satisfies Grant;

describe("default-role fallback parity", () => {
  it("preserves the default marker and suppresses it across resource kinds only on covered clusters", () => {
    const grants = grantsFromWire([...defaults, explicit]);
    expect(grants[0]?.defaultRole).toBe(true);
    expect(grantsAllow(grants, "prod", Actions.TopicView, "orders")).toBe(false);
    expect(grantsAllowAny(grants, "prod", Actions.TopicCreate)).toBe(false);
    expect(grantsAllow(grants, "staging", Actions.TopicView, "orders")).toBe(true);
    expect(grantsAllowAny(grants, "staging", Actions.TopicCreate)).toBe(true);
    expect(grantsAllow(grants, "prod", Actions.ConsumerGroupView, "checkout")).toBe(true);
  });

  it("chooses explicit grants before pattern, action, or connector-parent matching", () => {
    const grants = grantsFromWire([...defaults, {
      resource: "CONNECTOR", clusters: ["prod"], actions: ["VIEW"], value: "payments/source", defaultRole: false,
    }]);
    expect(grantsAllow(grants, "prod", Actions.ConnectorEdit, "payments/sink")).toBe(false);
    expect(grantsAllow(grants, "staging", Actions.ConnectorEdit, "payments/sink")).toBe(true);
    const parent = grantsFromWire([...defaults, {
      resource: "CONNECT", clusters: ["prod"], actions: ["EDIT"], value: "billing", defaultRole: false,
    }]);
    expect(grantsAllow(parent, "prod", Actions.ConnectorEdit, "billing/sink")).toBe(true);
    expect(grantsAllow(parent, "prod", Actions.ConnectorEdit, "payments/sink")).toBe(false);
  });

  it("uses all clusters when selecting grants for a global request", () => {
    const grants = grantsFromWire([...defaults, explicit]);
    expect(grantsAllowAny(grants, undefined, Actions.AuditView)).toBe(false);
    expect(grantsAllow(grants, undefined, Actions.AuditView, undefined)).toBe(false);
    expect(grantsAllowAny(grantsFromWire(defaults), undefined, Actions.AuditView)).toBe(true);
    expect(grantsAllowAny(grantsFromWire([{
      resource: "AUDIT", clusters: ["prod"], actions: ["VIEW"], defaultRole: false,
    }]), undefined, Actions.AuditView)).toBe(true);
  });

  it("retains fallback provenance through session adoption and standalone store mapping", () => {
    let dispose = (): void => undefined;
    const session = createRoot((cleanup) => {
      dispose = cleanup;
      return createSession({ settleCsrf: () => undefined, invalidateCsrf: () => undefined });
    });
    try {
      session.accept({ authType: "basic", csrfToken: "token", principal: { kind: "user", name: "reader" }, permissions: [...defaults, explicit] });
      flush();
      expect(session.identity()?.permissions[0]?.defaultRole).toBe(true);
      expect(session.permits("TOPIC", "VIEW", "prod", "orders")).toBe(false);
      expect(session.permits("TOPIC", "VIEW", "staging", "orders")).toBe(true);
      expect(session.permits("AUDIT", "VIEW")).toBe(false);
      expect(grantsFromWire(session.identity()?.permissions)[0]?.defaultRole).toBe(true);
    } finally {
      dispose();
    }
  });
});
