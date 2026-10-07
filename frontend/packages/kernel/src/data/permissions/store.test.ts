import { describe, expect, it } from "vitest";
import { createRoot, flush } from "solid-js";
import { Actions, Resources } from "@kui/api";

import { createPermissions, grantsFromWire, type PermissionGrant } from "./store.js";

function grant(overrides: Partial<PermissionGrant> = {}): PermissionGrant {
  return {
    clusters: ["prod"],
    resource: Resources.Topic,
    value: ".*",
    actions: ["VIEW"],
    ...overrides,
  };
}

describe("the permission store", () => {
  it("refuses everything before the session has answered", () => {
    createRoot((dispose) => {
      const permissions = createPermissions();

      // The right way round: a control that starts enabled and disables itself can be clicked in
      // the gap, and the click goes to a server that will refuse it.
      expect(permissions.allows("prod", Actions.TopicView, "orders")).toBe(false);
      expect(permissions.allowsAny("prod", Actions.TopicCreate)).toBe(false);
      dispose();
    });
  });

  it("answers from the grant list without re-deriving anything", () => {
    createRoot((dispose) => {
      const permissions = createPermissions();
      // The server expands the closure: granting DELETE already carries the VIEW it implies. The
      // browser must not expand anything of its own, or the two can disagree.
      permissions.adopt([grant({ actions: ["DELETE", "VIEW"], value: "payments\\..*" })]);
      flush();

      expect(permissions.allows("prod", Actions.TopicDelete, "payments.orders")).toBe(true);
      expect(permissions.allows("prod", Actions.TopicView, "payments.orders")).toBe(true);
      expect(permissions.allows("prod", Actions.TopicEdit, "payments.orders")).toBe(false);
      expect(permissions.allows("prod", Actions.TopicDelete, "orders")).toBe(false);
      dispose();
    });
  });

  it("matches a pattern in full, so one topic's grant does not cover another's", () => {
    createRoot((dispose) => {
      const permissions = createPermissions();
      permissions.adopt([grant({ value: "orders" })]);
      flush();

      expect(permissions.allows("prod", Actions.TopicView, "orders")).toBe(true);
      // A search rather than a full match would grant this, which is how a grant quietly widens.
      expect(permissions.allows("prod", Actions.TopicView, "orders-dlq")).toBe(false);
      dispose();
    });
  });

  it("anchors an alternation as a whole", () => {
    createRoot((dispose) => {
      const permissions = createPermissions();
      permissions.adopt([grant({ value: "orders|payments" })]);
      flush();

      expect(permissions.allows("prod", Actions.TopicView, "orders")).toBe(true);
      expect(permissions.allows("prod", Actions.TopicView, "payments")).toBe(true);
      // `^orders|payments$` — the version without the group — would allow this one.
      expect(permissions.allows("prod", Actions.TopicView, "orders-and-more")).toBe(false);
      dispose();
    });
  });

  it("scopes a grant to its clusters, and honours the every-cluster wildcard", () => {
    createRoot((dispose) => {
      const permissions = createPermissions();
      permissions.adopt([
        grant({ clusters: ["prod"], actions: ["DELETE", "VIEW"] }),
        grant({ clusters: ["*"], resource: Resources.ConsumerGroup, actions: ["VIEW"] }),
      ]);
      flush();

      // The same topic name is deletable on one cluster and not on another, which is the whole
      // point of scoping a role.
      expect(permissions.allows("prod", Actions.TopicDelete, "orders")).toBe(true);
      expect(permissions.allows("staging", Actions.TopicDelete, "orders")).toBe(false);
      expect(permissions.allows("staging", Actions.ConsumerGroupView, "checkout")).toBe(true);
      dispose();
    });
  });

  it("treats a grant with no pattern as being about the unnamed resource only", () => {
    createRoot((dispose) => {
      const permissions = createPermissions();
      permissions.adopt([
        grant({ resource: Resources.Audit, value: undefined, actions: ["VIEW"] }),
        grant({ resource: Resources.Topic, value: undefined, actions: ["VIEW"] }),
      ]);
      flush();

      // There is one audit trail, so its permission names nothing.
      expect(permissions.allows("prod", Actions.AuditView)).toBe(true);
      // A TOPIC permission with no pattern grants nothing, because every topic access names a
      // topic. The asymmetry is the server's, and it makes a forgotten `value` deny rather than
      // grant.
      expect(permissions.allows("prod", Actions.TopicView, "orders")).toBe(false);
      dispose();
    });
  });

  it("lets a grant on the connect cluster cover the connectors inside it", () => {
    createRoot((dispose) => {
      const permissions = createPermissions();
      permissions.adopt([
        grant({ resource: Resources.Connect, value: "payments", actions: ["EDIT", "VIEW"] }),
      ]);
      flush();

      // Granting EDIT on `payments` must not have to be repeated for each of its forty connectors.
      expect(permissions.allows("prod", Actions.ConnectorEdit, "payments/sink")).toBe(true);
      expect(permissions.allows("prod", Actions.ConnectorEdit, "billing/sink")).toBe(false);
      dispose();
    });
  });

  it("answers the question a create asks, which has no name to match yet", () => {
    createRoot((dispose) => {
      const permissions = createPermissions();
      permissions.adopt([grant({ value: "payments\\..*", actions: ["CREATE", "VIEW"] })]);
      flush();

      // The same weakening the gateway applies when the resource is named only in the request body:
      // somebody with a topic grant may reach the create endpoint, and is refused by the owning
      // service, with the name in hand, if they ask for something outside their pattern.
      expect(permissions.allowsAny("prod", Actions.TopicCreate)).toBe(true);
      expect(permissions.allowsAny("staging", Actions.TopicCreate)).toBe(false);
      expect(permissions.allowsAny("prod", Actions.TopicDelete)).toBe(false);
      dispose();
    });
  });

  it("explains a refusal in a sentence a control can show", () => {
    createRoot((dispose) => {
      const permissions = createPermissions();
      permissions.adopt([grant({ actions: ["VIEW"] })]);
      flush();

      const decision = permissions.decide("prod", Actions.TopicMessagesProduce, "orders");
      expect(decision).toEqual({
        allowed: false,
        // It names the action and the thing, and never the grant: telling somebody which pattern
        // they would need is telling them about a policy they cannot read.
        reason: "You do not have permission to messages produce 'orders'.",
      });
      dispose();
    });
  });

  it("filters a list rather than refusing it", () => {
    createRoot((dispose) => {
      const permissions = createPermissions();
      permissions.adopt([grant({ value: "payments\\..*" })]);
      flush();

      const topics = [{ name: "payments.orders" }, { name: "billing.invoices" }, { name: "payments.refunds" }];
      // Somebody who may see two of three topics sees two topics, not an error — and the third's
      // existence is not leaked.
      expect(permissions.visible("prod", Actions.TopicView, topics, (topic) => topic.name)).toEqual([
        { name: "payments.orders" },
        { name: "payments.refunds" },
      ]);
      dispose();
    });
  });

  it("denies rather than throws when a pattern will not compile in this engine", () => {
    createRoot((dispose) => {
      const permissions = createPermissions();
      permissions.adopt([grant({ value: "payments(" })]);
      flush();

      // The server validated the pattern when it read the configuration, so reaching here means the
      // two engines disagree about the syntax. Denying is the safe direction.
      expect(permissions.allows("prod", Actions.TopicView, "payments.orders")).toBe(false);
      dispose();
    });
  });

  it("reads the wire shape, including the fields the server omits when they are empty", () => {
    expect(
      grantsFromWire([
        { resource: "TOPIC", clusters: ["prod"], actions: ["VIEW"], value: ".*", defaultRole: false },
        { resource: "AUDIT", defaultRole: false },
      ]),
    ).toEqual([
      { resource: "TOPIC", clusters: ["prod"], actions: ["VIEW"], value: ".*", defaultRole: false },
      { resource: "AUDIT", clusters: [], actions: [], value: undefined, defaultRole: false },
    ]);
    expect(grantsFromWire(undefined)).toEqual([]);
  });
});
