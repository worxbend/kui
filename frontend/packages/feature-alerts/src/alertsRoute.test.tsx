/**
 * The alerts route: the wiring, which is the part a component test cannot see.
 *
 * ## Why the route is mounted rather than the card
 *
 * `AlertsFeed` refuses correctly when it is handed no `onAcknowledge` — `alerts.test.tsx` pins that
 * — and refusing correctly is worth nothing if nothing decides when to hand it one. Wave 5 shipped a
 * destructive consumer-group control in exactly that state: the component was tested, the story was
 * drawn, and `GroupRoute.tsx`'s `mayReset() ?` could be replaced with `true ?` with all 1449 cases
 * in the workspace green, because no case mounted the route without the permission. So the first
 * case below mounts this route with a principal who holds every permission except
 * `ALERTS:ACKNOWLEDGE`, and asserts through the rendered control and through the requests the client
 * received.
 *
 * ## The router is real
 *
 * The default export reads `clusterId` from the address, so the cases go through a router with a
 * memory history rather than calling `AlertsScreen` directly. The path is the one the shell will
 * mount this feature at; a route that only works when it is handed its parameters by a test is a
 * route nobody has checked.
 */
import { describe, expect, it, vi } from "vitest";
import { createSignal, flush, onCleanup } from "solid-js";
import { createRouter, memoryHistory } from "@solidjs/router";
import {
  AlertsProvider,
  KuiProvider,
  createAlerts,
  type SseHandle,
  type KuiContextValue,
} from "@kui/kernel";
import { Actions, ErrorCodes, type KuiApiClient } from "@kui/api";

import { mount, findViolations, describeViolations, testContext } from "./testing.js";
import Alerts, { ACKNOWLEDGEMENT_PATH, SEVERITIES } from "./index.jsx";
import { CLUSTER_PATH } from "./data.js";
import openDocument from "./documents/events-open.json" with { type: "json" };
import notConfiguredDocument from "./documents/events-not-configured.json" with { type: "json" };
import neverEvaluatedDocument from "./documents/events-never-evaluated.json" with { type: "json" };

const BASE = "/ui";

/**
 * The feed's address, spelled out here because this file is the harness and not the product.
 *
 * The read that fills this screen is the **shell's**: `packages/shell/src/App.tsx` builds one
 * `createAlerts` store for the whole application against `packages/shell/src/data/alerts.ts`'s
 * `loadAlertFeed`, so that the bell in the chrome and the card here cannot disagree about how many
 * alerts are open. The address that function sends is module-private there, deliberately — a
 * constant asserted against a copy of itself is a rule that cannot fail — so `open()` below spells
 * it out the way the shell does rather than importing anything.
 */
const EVENTS_PATH = "/api/v1/clusters/{clusterId}/alerts/events";

/** Solid batches writes onto a microtask; a router navigation takes a couple of turns to settle. */
async function settle(times = 8): Promise<void> {
  for (let index = 0; index < times; index += 1) await flush();
}

interface Call {
  readonly method: "get" | "post";
  readonly path: string;
  readonly params: Record<string, string>;
  /** Recorded so that a query parameter nobody meant to send is visible to a case. */
  readonly query: Record<string, unknown> | undefined;
}

interface Stub {
  readonly api: KuiApiClient;
  readonly calls: Call[];
  /** Just the feed reads. The screen also asks the cluster service whether writing is allowed. */
  readonly feedReads: () => Call[];
  /** Replaces what the next `GET …/events` answers with. */
  answerWith: (document: unknown) => void;
}

/**
 * A client that answers the two alerts paths and records what it was asked.
 *
 * Cast at one boundary, like every stub client in this workspace: `KuiApiClient`'s methods are typed
 * from the OpenAPI document, and a fake satisfying all of that would be a second copy of the schema.
 * The paths it matches are the literal constants the product passes, so a renamed endpoint fails
 * here rather than drawing an empty feed.
 */
function stub(
  options: {
    readonly acknowledgement?: unknown;
    readonly hold?: boolean;
    readonly holdRead?: boolean;
    /** ADR-047's flag, as the cluster service answers it. `false` is the quickstart's answer. */
    readonly readOnly?: boolean;
  } = {},
): Stub {
  const calls: Call[] = [];
  let document: unknown = openDocument;

  const answer =
    (method: "get" | "post") =>
    async (
      path: string,
      init: { params: { path: Record<string, string>; query?: Record<string, unknown> } },
    ) => {
      calls.push({ method, path, params: init.params.path, query: init.params.query });
      if (method === "get") {
        /* The cluster document, which is a different service and a different question: whether this
           deployment lets anything write here at all. Answered eagerly even when the feed is held,
           because the two reads are independent and a screen waiting for one is not waiting for the
           other. */
        if (path === CLUSTER_PATH) {
          return { ok: true, value: { cluster: { readOnly: options.readOnly === true } } };
        }
        // A read that never answers, for the one case about what the screen says before anything
        // has been established.
        if (options.holdRead === true) return new Promise<never>(() => {});
        return { ok: true, value: document };
      }
      // A request that never answers, for the one case about what the screen does *while* a write
      // is out. `never` rather than a slow timer: a case that waits on a clock is a case that is
      // slow when it passes and flaky when the machine is busy.
      if (options.hold === true) return new Promise<never>(() => {});
      const refusal = options.acknowledgement;
      return refusal === undefined ? { ok: true, value: {} } : refusal;
    };

  return {
    api: { get: answer("get"), post: answer("post") } as unknown as KuiApiClient,
    calls,
    feedReads: () => calls.filter((call) => call.method === "get" && call.path === EVENTS_PATH),
    answerWith: (next) => {
      document = next;
    },
  };
}

function open(
  cluster: string,
  api: KuiApiClient,
  permits: (action: { readonly resource: string; readonly action: string }) => boolean = () => true,
  writeBlocked?: KuiContextValue["writeBlocked"],
): { readonly container: HTMLElement; readonly dispose: () => void } {
  const history = memoryHistory(`${BASE}/clusters/${cluster}/alerts`);
  const Router = createRouter({
    routes: [{ path: "/clusters/:clusterId/alerts", component: Alerts }],
    base: BASE,
    history,
    scrollRestoration: false,
  });
  const Host = () => {
    const alerts = createAlerts({
      load: async (markRead) => {
        const get = api.get as unknown as (
          path: string,
          init: { params: { path: Record<string, string>; query: Record<string, unknown> } },
        ) => ReturnType<KuiApiClient["get"]>;
        return get(EVENTS_PATH, {
          params: { path: { clusterId: cluster }, query: { markRead } },
        });
      },
      openStream: () =>
        ({
          connection: () => ({ phase: "closed", reason: "test stream" }),
          close: () => {},
          endMarker: () => undefined,
        }) as SseHandle,
      cluster: () => cluster,
    });
    alerts.start();
    onCleanup(() => alerts.stop());
    return (
      <AlertsProvider value={alerts}>
        <KuiProvider value={{ ...testContext(api, permits), ...(writeBlocked === undefined ? {} : { writeBlocked }) }}>
          <Router />
        </KuiProvider>
      </AlertsProvider>
    );
  };
  return mount(Host);
}

/**
 * The same default export, at a path that carries no `clusterId`.
 *
 * The route reads the cluster off the address, so "no cluster is selected" is a real address, not
 * a prop nobody passes: the shell mounts this feature under a cluster, and a bookmark, a sign-out,
 * or a cluster that has been removed lands a reader on the feature with nothing to read.
 */
function openWithoutCluster(api: KuiApiClient): {
  readonly container: HTMLElement;
  readonly dispose: () => void;
} {
  const history = memoryHistory(`${BASE}/alerts`);
  const Router = createRouter({
    routes: [{ path: "/alerts", component: Alerts }],
    base: BASE,
    history,
    scrollRestoration: false,
  });
  const Host = () => (
    <KuiProvider value={testContext(api)}>
      <Router />
    </KuiProvider>
  );
  return mount(Host);
}

function rows(container: HTMLElement): HTMLElement[] {
  return [...container.querySelectorAll<HTMLElement>('[data-testid="alert-row"]')];
}

/** The acknowledge control on one row, whether it is enabled or disabled with its reason. */
function acknowledgeButton(container: HTMLElement, event: string): HTMLButtonElement | undefined {
  const row = rows(container).find((candidate) => candidate.dataset["event"] === event);
  return [...(row?.querySelectorAll("button") ?? [])].find((button) =>
    (button.textContent ?? "").includes("Acknowledge"),
  );
}

describe("the alerts route", () => {
  it("77: the shared write policy overrides the legacy read-only query reactively", async () => {
    const client = stub({ readOnly: false });
    const [reason, setReason] = createSignal<string | undefined>("Cluster is read-only");
    const policy = vi.fn(() => reason());
    const view = open("quickstart", client.api, () => true, policy);
    try {
      await settle();
      const buttons = () => [...view.container.querySelectorAll("button")].filter(b => b.textContent?.includes("Acknowledge"));
      expect(buttons().length).toBeGreaterThan(0);
      expect(buttons().every(b => b.getAttribute("aria-disabled") === "true")).toBe(true);
      expect(policy).toHaveBeenCalledWith("quickstart", Actions.AlertsAcknowledge);
      setReason(undefined); await settle();
      expect(buttons().some(b => b.getAttribute("aria-disabled") !== "true")).toBe(true);
    } finally { view.dispose(); }
  });
  it("reads this cluster's events, at the path the service publishes", async () => {
    const client = stub();
    const { container, dispose } = open("quickstart", client.api);
    await settle();

    /* One feed read. The screen also asks the cluster service whether this deployment lets anything
       write here, which is a different question of a different service — `feedReads` keeps the two
       apart so that neither can hide the other going missing. */
    expect(client.feedReads()).toHaveLength(1);
    expect(client.calls.map((call) => call.path)).toContain(CLUSTER_PATH);
    /*
     * The address as a literal, not as `EVENTS_PATH`.
     *
     * The stub answers whatever path the product passes, so a case comparing the request against
     * the same constant the product built it from asserts that a constant equals itself — the
     * self-referential shape an adversary looks for first. `AlertsEndpoints.events` composes
     * `clusters / clusterId / alerts / events` under the gateway's public `/api/v1` prefix, and that
     * is what is written out here.
     */
    expect(client.feedReads()[0]).toEqual({
      method: "get",
      path: "/api/v1/clusters/{clusterId}/alerts/events",
      params: { clusterId: "quickstart" },
      /*
       * And `markRead: false`. The endpoint defaults it to false and its description says why:
       * *"so a card polling the feed does not clear somebody's bell"*.
       *
       * This screen does **not** poll — there is no interval in this package or in the store, and
       * `useQuery` is given none — and the sentence that said it did was wrong for two waves. The
       * reason to send `false` is the one that survives: this read is re-issued by things the
       * reader did not do. Every frame on the alerts stream makes the store re-read the feed, and
       * so does every successful acknowledgement, so a `true` here would clear the unread marker of
       * somebody who left a tab open at the moment an alert *opened* — the bell going quiet exactly
       * when it should have rung. `unreadCount` is per-principal, and marking read is a deliberate
       * act with its own control, `markAllRead()`, beside the bell in the shell's chrome.
       */
      query: { markRead: false },
    });
    expect(EVENTS_PATH).toBe("/api/v1/clusters/{clusterId}/alerts/events");
    expect(rows(container)).toHaveLength(5);
    dispose();
  });

  it("writes no voice line until something has answered", async () => {
    const client = stub({ holdRead: true });
    const { container, dispose } = open("silent-cluster", client.api);
    await settle();

    /*
     * A sentence about how much is open, written over a document nobody has read yet, is the
     * product asserting something it does not know — and "Nothing is open" is the one it would
     * write, because an unread feed and an empty one are the same object until the read lands.
     */
    expect(container.querySelector(".kui-page-head__voice")).toBeNull();
    expect(container.textContent).not.toContain("Nothing is open");
    expect(container.textContent).not.toContain("open alerts");
    dispose();
  });

  it("says the count the service answered, in the voice line and in the pill", async () => {
    const client = stub();
    const { container, dispose } = open("voice-cluster", client.api);
    await settle();

    // Three open of five rows: the two figures come from different fields and neither is derived
    // from the other. The design's second sentence — "One is the usual suspect" — is a joke about
    // which one, and the browser does not know that, so it is not written.
    expect(container.textContent).toContain("3 open alerts.");
    expect(container.textContent).not.toContain("usual suspect");
    expect(
      container.querySelector('[data-testid="alerts-open-count"]')?.textContent,
    ).toContain("3 open");
    dispose();
  });

  /**
   * **This packet's owned rule.** The voice line over a cluster nobody has looked at.
   *
   * `feedVoice`'s first arm is the one that separates *"the rules ran here and opened nothing"*
   * from *"the rules have never run here"*, and it is the only thing between an unevaluated
   * cluster and the sentence "Nothing is open. The bell is quiet." printed in the largest type on
   * its own Alerts screen — over an `openCount: 0` that measures nothing at all.
   *
   * The card below the header was already gated for this document; the `PageHeader` above it was
   * not, because no case mounted the route over an unevaluated feed. Delete that arm and this case
   * is what fails.
   *
   * The whole route is mounted rather than `feedVoice` called, for the reason wave 5's
   * consumer-group control taught: a function that returns the right sentence is worth nothing if
   * nothing puts it on the screen. `voiceOf` decides whether the line is drawn at all, and it is
   * between the store and the header.
   */
  it("a cluster the rules have never run on is told so in the voice line", async () => {
    const client = stub();
    client.answerWith(neverEvaluatedDocument);
    const { container, dispose } = open("unevaluated-cluster", client.api);
    await settle();

    const voice = container.querySelector(".kui-page-head__voice");
    expect(voice?.textContent).toBe("KUI has not run its alert rules on this cluster yet.");
    /* The two sentences this must never be. Both are about a cluster somebody has checked, and this
       one has not been: "nothing is open" is a claim about the cluster, and "0 open alerts" is a
       count of a thing nobody counted. */
    expect(container.textContent).not.toContain("Nothing is open");
    expect(container.textContent).not.toContain("open alerts");
    // And the card underneath agrees with the header, which is the point of one store.
    expect(
      container.querySelector('[data-testid="alerts-open-count"]')?.textContent,
    ).toContain("Not evaluated yet");
    dispose();
  });

  /**
   * The severity chips, against the list they are built from.
   *
   * `SEVERITIES` is two entries and `model.ts` spends two paragraphs arguing why: of §3.8's four
   * dots, `success` is a *resolved* row and `primary` is an informational one that **no rule this
   * service ships opens**, so a third chip is a control that can only ever answer nothing. Nothing
   * checked the argument — the list could gain `"info"` with every case in the workspace green —
   * and the chip bar is where it is visible, so the chip bar is what this reads.
   *
   * The labels are written out rather than mapped from `SEVERITIES` through `severityChip`: a case
   * that built its expectation from the same list the product built the chips from would assert
   * that a list equals itself, and would stay green for every one of the mutations that matter.
   */
  it("the severity filter offers one chip per severity this service opens events at, and no others", async () => {
    const client = stub();
    const { container, dispose } = open("chip-cluster", client.api);
    await settle();

    const bar = container.querySelector('[data-testid="alerts-severity-filter"]');
    const labels = [...(bar?.querySelectorAll("button") ?? [])].map((chip) =>
      (chip.textContent ?? "").trim(),
    );
    expect(labels).toEqual(["All severities", "Critical", "Warning"]);
    // The list behind them, said once, so that the failure names the cause rather than the symptom.
    expect([...SEVERITIES]).toEqual(["critical", "warning"]);

    // And each chip filters to a non-empty set on this document, which is what "a chip that can
    // never match a row" would break: five events, two critical and three warning.
    const critical = [...(bar?.querySelectorAll("button") ?? [])].find(
      (chip) => (chip.textContent ?? "").trim() === "Critical",
    );
    critical?.click();
    await settle();
    expect(rows(container).length).toBeGreaterThan(0);
    dispose();
  });

  it("a principal without ALERTS:ACKNOWLEDGE is never handed an enabled acknowledge control", async () => {
    const client = stub();
    const { container, dispose } = open(
      "unpermitted-cluster",
      client.api,
      (action) =>
        !(
          action.resource === Actions.AlertsAcknowledge.resource &&
          action.action === Actions.AlertsAcknowledge.action
        ),
    );
    await settle();

    const button = acknowledgeButton(container, "evt-1");
    expect(button).toBeDefined();
    // Disabled and *readable*: `aria-disabled` rather than the attribute, so the reason stays
    // reachable by keyboard and by hover. A hidden control would tell the operator the product
    // cannot do this at all.
    expect(button?.getAttribute("aria-disabled")).toBe("true");
    /*
     * And the reason is reachable, which is the half of the rule that makes the other half humane:
     * `Tooltip` renders its bubble on focus and hover, so the sentence is asserted by focusing the
     * control the way a keyboard user reaches it, not by reading a title attribute nobody hears.
     */
    expect(button?.getAttribute("aria-describedby")).not.toBeNull();
    button?.dispatchEvent(new FocusEvent("focusin", { bubbles: true }));
    await settle();
    expect(document.body.textContent).toContain(
      "You do not have permission to acknowledge alerts on this cluster.",
    );

    button?.click();
    await settle();
    // The gate is the wiring, not the button's own opinion: no request left the browser.
    expect(client.calls.filter((call) => call.method === "post")).toHaveLength(0);
    dispose();
  });

  /**
   * ADR-047's flag, which is the *deployment's* answer and not the principal's.
   *
   * `writeBlockedReason` has taken both facts since wave 5 and this screen passed the literal
   * `false` for the second one until wave 8 — so a cluster somebody had deliberately registered
   * read-only handed a fully permitted operator a live `Acknowledge`, issued the `POST`, and let
   * the gateway refuse it. The two sentences are different on purpose: sending an operator to ask
   * for a permission they already hold wastes their afternoon, and this asserts the read-only one
   * rather than merely that *something* closed the button.
   */
  it("a read-only cluster offers no acknowledgement, however permitted the reader", async () => {
    const client = stub({ readOnly: true });
    const { container, dispose } = open("frozen-cluster", client.api);
    await settle();

    const button = acknowledgeButton(container, "evt-1");
    expect(button).toBeDefined();
    expect(button?.getAttribute("aria-disabled")).toBe("true");

    button?.dispatchEvent(new FocusEvent("focusin", { bubbles: true }));
    await settle();
    expect(document.body.textContent).toContain(
      "This cluster is configured read-only in KUI, so nothing here can acknowledge alerts on " +
        "this cluster.",
    );
    // Not the permission sentence: this principal holds `ALERTS:ACKNOWLEDGE` and would be sent to
    // an administrator who has nothing to give them.
    expect(document.body.textContent).not.toContain(
      "You do not have permission to acknowledge alerts",
    );

    button?.click();
    await settle();
    expect(client.calls.filter((call) => call.method === "post")).toHaveLength(0);
    dispose();
  });

  it("a writable cluster reads as writable, which makes the case above a gate", async () => {
    const client = stub({ readOnly: false });
    const { container, dispose } = open("thawed-cluster", client.api);
    await settle();

    // The same read, answering `false`, and the control is live. Without this half a screen that
    // refused every cluster would satisfy the case above.
    expect(client.calls.map((call) => call.path)).toContain(CLUSTER_PATH);
    expect(acknowledgeButton(container, "evt-1")?.getAttribute("aria-disabled")).not.toBe("true");
    dispose();
  });

  it("acknowledging an event the API refuses leaves the row unacknowledged and says why", async () => {
    const client = stub({
      acknowledgement: {
        ok: false,
        error: {
          kind: "envelope",
          // The event closed between the feed being drawn and the button being pressed.
          code: ErrorCodes.InvalidState,
          message: "This event is already closed, so it cannot be acknowledged.",
          details: [],
          correlationId: "c-1",
          retryable: false,
        },
      },
    });
    const { container, dispose } = open("refused-cluster", client.api);
    await settle();

    acknowledgeButton(container, "evt-1")?.click();
    await settle();

    expect(container.textContent).toContain("This event is already closed");
    // The code, because it is what an operator quotes to whoever they escalate to.
    expect(container.textContent).toContain(ErrorCodes.InvalidState);
    // The row is exactly as it was: still open, still offering the control, and carrying nobody's
    // name. A refusal that quietly marked the row acknowledged would be the screen lying about a
    // request the server declined.
    const row = rows(container).find((candidate) => candidate.dataset["event"] === "evt-1");
    expect(row?.textContent).not.toContain("Acknowledged");
    expect(acknowledgeButton(container, "evt-1")).toBeDefined();
    // And the feed was not re-read: refetching would replace the reason with the same row and read
    // as though nothing had been pressed.
    expect(client.feedReads()).toHaveLength(1);
    dispose();
  });

  it("acknowledges through the event's own sub-resource and re-reads the feed", async () => {
    const client = stub();
    const { container, dispose } = open("accepting-cluster", client.api);
    await settle();

    acknowledgeButton(container, "evt-1")?.click();
    await settle();

    const posts = client.calls.filter((call) => call.method === "post");
    expect(posts).toHaveLength(1);
    // The literal again, for the reason the read's case gives: `AlertsEndpoints.acknowledge` posts
    // to the event's own `acknowledgement` sub-resource, and no request body goes with it.
    expect(posts[0]).toEqual({
      method: "post",
      path: "/api/v1/clusters/{clusterId}/alerts/events/{eventId}/acknowledgement",
      params: { clusterId: "accepting-cluster", eventId: "evt-1" },
      query: undefined,
    });
    expect(ACKNOWLEDGEMENT_PATH).toBe(
      "/api/v1/clusters/{clusterId}/alerts/events/{eventId}/acknowledgement",
    );
    // The row's new state comes from re-reading the feed rather than from a body this build would
    // have to guess the shape of.
    expect(client.calls.filter((call) => call.method === "get").length).toBeGreaterThan(1);
    dispose();
  });

  it("sends one acknowledgement however many times the control is pressed", async () => {
    const client = stub();
    const { container, dispose } = open("double-press-cluster", client.api);
    await settle();

    const button = acknowledgeButton(container, "evt-1");
    button?.click();
    button?.click();
    await settle();

    // Two presses in one turn is the shape a real double click has, and `createMutation`'s guard is
    // a plain variable for exactly that reason: a signal read would still say `idle`.
    expect(client.calls.filter((call) => call.method === "post")).toHaveLength(1);
    dispose();
  });

  it("marks the row whose acknowledgement is in flight, and only that row", async () => {
    const client = stub({ hold: true });
    const { container, dispose } = open("in-flight-cluster", client.api);
    await settle();

    acknowledgeButton(container, "evt-1")?.click();
    await settle();

    /*
     * The busy mark is on the row that was pressed and on no other. It is what stops a second press
     * — `Button` swallows a click while `busy` — and it is the only thing on screen that says the
     * request is out; without it the operator presses again, which is the duplicate-mutation shape
     * `createMutation`'s own guard exists for one layer down.
     */
    expect(acknowledgeButton(container, "evt-1")?.getAttribute("aria-busy")).toBe("true");
    expect(acknowledgeButton(container, "evt-3")?.getAttribute("aria-busy")).toBeNull();
    dispose();
  });

  it("carries a not-configured feed to the screen as hidden, and not as a failure", async () => {
    const client = stub();
    client.answerWith(notConfiguredDocument);
    const { container, dispose } = open("no-alerts-cluster", client.api);
    await settle();

    /*
     * The seam between the section's status and the screen's six-case state, which the component's
     * own case cannot see: `fromSection` is what keeps `not_configured` from arriving as `failed`,
     * and a screen that collapsed the two would put a red panel and a Retry button in front of
     * every operator whose deployment simply runs no alerts service (ADR-032).
     */
    expect(container.querySelector('[data-testid="alerts-feed"]')).toBeNull();
    expect(container.textContent).not.toContain("Retry");
    expect(container.textContent).not.toContain("did not answer");
    // The heading stays: a bookmark has to land somewhere, and the row that vanishes is the
    // navigation's, which is the shell's to draw.
    expect(container.querySelector("h1")?.textContent).toBe("Alerts");
    dispose();
  });

  /**
   * The rules panel, on the route.
   *
   * `RuleReports` has its own cases over a document, and they are worth nothing if the screen hands
   * it the wrong list: `reports={[]}` draws no panel at all, and a screen with no "What KUI
   * checked" on it looks exactly like a screen whose service reported no rules. That is the seam this case
   * covers and no case did — the panel is the answer to the question an empty feed raises, so the
   * screen losing it is the failure that matters most on the day nothing is open.
   */
  it("hands the rules panel the reports the feed carried, and not an empty list", async () => {
    const client = stub();
    const { container, dispose } = open("rules-cluster", client.api);
    await settle();

    const named = [...container.querySelectorAll<HTMLElement>('[data-testid="alert-rule"]')].map(
      (row) => row.dataset["rule"],
    );
    // The four rules `events-open.json` carries, in the order the document lists them.
    expect(named).toEqual([
      "offline-partitions",
      "under-replicated-partitions",
      "stuck-rebalance",
      "disk-usage",
    ]);
    expect(container.querySelector('[data-testid="alerts-rules"]')).not.toBeNull();
    dispose();
  });

  /**
   * The address with no cluster on it.
   *
   * Reachable by a bookmark, by a sign-out, and by a cluster being removed from the configuration
   * while somebody has its Alerts screen open. Nothing mounted this branch, so the sentence it
   * draws and the way out of it were both unasserted: a screen that said nothing here would leave
   * a reader on an empty page with no explanation and no link, which is the one state a router can produce
   * that no amount of care in the feed can rescue.
   *
   * And no read is issued, because there is no cluster to read.
   */
  it("says which cluster is missing rather than drawing an empty feed", async () => {
    const client = stub();
    const { container, dispose } = openWithoutCluster(client.api);
    await settle();

    expect(container.textContent).toContain("No cluster is selected");
    const away = container.querySelector("a");
    expect(away?.getAttribute("href")).toBe("/ui/clusters");
    expect(away?.textContent).toBe("Choose a cluster");
    // No card, no rules panel, and nothing asked of the API.
    expect(container.querySelector('[data-testid="alerts-feed"]')).toBeNull();
    expect(container.querySelector('[data-testid="alerts-rules"]')).toBeNull();
    expect(client.calls).toHaveLength(0);
    dispose();
  });

  it("draws only the rows the reader's filter names, and keeps the service's count", async () => {
    const client = stub();
    const { container, dispose } = open("filter-cluster", client.api);
    await settle();

    const critical = [...container.querySelectorAll("button")].find(
      (button) => (button.textContent ?? "").trim() === "Critical",
    );
    critical?.click();
    await settle();

    // Two of the five rows opened `critical`; one of those two is resolved, which the severity
    // filter has no opinion about — that is the other chip bar's question.
    expect(rows(container)).toHaveLength(2);
    expect(
      container.querySelector('[data-testid="alerts-open-count"]')?.textContent,
    ).toContain("3 open");
    expect(container.textContent).toContain("2 of the 5 events on this page match.");
    dispose();
  });

  /**
   * The *Alert state* bar, which no case in this package had ever clicked.
   *
   * `filterEvents` is gated at the model level and `AlertsFeed`'s application of the prop is gated,
   * so both ends of this wire had cases and the wire between them had none: swapping the `open` and
   * `resolved` chip values, replacing `onChange` with a no-op, and pinning the memo's `state` to
   * `"all"` each left all 61 cases in the package green. The first of those three is the worst — a
   * reader clicks *Open* during an incident and is shown the events that are already closed.
   *
   * So this reads the rows **by event id** rather than by counting them: the open and resolved
   * halves of `events-open.json` are three and two, and a count alone cannot tell a swapped pair
   * from a working one when the two halves happen to be the same size. It also asserts the pressed
   * chip, because a bar that filtered correctly and never moved its own highlight is a control that
   * looks broken to the person using it.
   */
  it("the Alert state chips choose which half of the feed is on screen", async () => {
    const client = stub();
    const { container, dispose } = open("lifecycle-cluster", client.api);
    await settle();

    const bar = container.querySelector<HTMLElement>('[data-testid="alerts-state-filter"]');
    expect(bar, "the state bar is the control this case is about").not.toBeNull();
    const chip = (label: string): HTMLButtonElement => {
      const found = [...(bar?.querySelectorAll<HTMLButtonElement>("button") ?? [])].find(
        (one) => (one.textContent ?? "").trim() === label,
      );
      if (found === undefined) throw new Error(`no ${label} chip on the Alert state bar`);
      return found;
    };
    const shown = (): string[] => rows(container).map((row) => row.dataset["event"] ?? "");

    // Everything, before anybody touches the bar.
    expect(shown()).toEqual(["evt-1", "evt-2", "evt-3", "evt-4", "evt-5"]);

    chip("Open").click();
    await settle();
    // The three with no `resolution` on the file, by name. `evt-4` and `evt-5` carry one.
    expect(shown()).toEqual(["evt-1", "evt-2", "evt-3"]);
    expect(chip("Open").getAttribute("aria-pressed")).toBe("true");
    expect(container.textContent).toContain("3 of the 5 events on this page match.");

    chip("Resolved").click();
    await settle();
    expect(shown()).toEqual(["evt-4", "evt-5"]);
    expect(chip("Resolved").getAttribute("aria-pressed")).toBe("true");
    expect(chip("Open").getAttribute("aria-pressed")).toBe("false");

    // And back, so the bar is a filter and not a one-way door.
    chip("All").click();
    await settle();
    expect(shown()).toHaveLength(5);

    // The service's own figure, untouched by any of it: the pill is what the API counted.
    expect(container.querySelector('[data-testid="alerts-open-count"]')?.textContent).toContain(
      "3 open",
    );
    dispose();
  });

  it("has no accessibility violations with a feed, its filters and its controls", async () => {
    const client = stub();
    const { container, dispose } = open("a11y-cluster", client.api);
    await settle();

    const violations = await findViolations(container);
    expect(describeViolations(violations)).toBe("");
    dispose();
  });
});
