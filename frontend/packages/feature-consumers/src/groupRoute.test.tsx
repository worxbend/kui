/**
 * The group page's destructive controls: who is offered them, what they ask before they run, and
 * the toast each success owes the operator.
 *
 * ## What was added in wave 6, and why it is in this file
 *
 * The file began as the two toasts and grew the forget control's receipt. What it never had was the
 * half **above** the receipt: whether the control is offered at all. `GroupDetail` renders each of
 * the three disabled-with-a-reason when it is handed no callback, and `consumers.test.tsx` covers
 * that rendering — but the three ternaries that decide which it is handed are here, in the route,
 * and the harness could only mount the route with a `permits` that said yes. All three were
 * therefore constants that no case in the repository could observe, and the worst of them handed an
 * account with no reset permission a live destructive button. `testContext` takes a `permits` now,
 * for exactly that.
 *
 * ## Why this file exists rather than another case in `consumers.test.tsx`
 *
 * "A toast on every destructive success" is one of M6's bullets, and until this file both of
 * `GroupRoute`'s `notify(…)` calls could be deleted with all 68 of this package's tests still
 * green. Nothing mounted the route: the list screen has a suite, the wizard has one, the detail
 * page has one, and the file that joins them to a client — the only place a mutation's *result* is
 * observed — had none. So the rule was written down twice, in two comments arguing for it, and
 * asserted nowhere.
 *
 * The seam is therefore the route with a router around it and a recording underneath, and the
 * assertion is on `toasts()`, the kernel's own store, which is what `ToastRegion` draws from in the
 * shell. Asserting against markup this package renders would prove nothing: this package renders no
 * toast, and a case that mounted `ToastRegion` beside the route would be asserting that the kernel
 * can draw a toast it was handed.
 *
 * ## Why the detail answer is the recorded document, edited
 *
 * `recorded/group.json` came off a running gateway, so the shape the mapping reads is the shape a
 * coordinator sends. The two cases below need states the quickstart cannot be put into — a group
 * with no members, because a group with members cannot be deleted and the button says so before the
 * click — so the recording is copied and the one field under test is changed. Everything else,
 * including every field name, stays the server's.
 */

import { beforeEach, describe, expect, it } from "vitest";
import { flush } from "solid-js";
import { createRouter, memoryHistory } from "@solidjs/router";
import { KuiProvider, clearToasts, toasts, type KuiContextValue } from "@kui/kernel";
import { Actions, type KuiApiClient } from "@kui/api";

import { mount, testContext } from "./testing.js";
import { GroupRoute, consequenceOfDelete, consequenceOfForget } from "./GroupRoute.jsx";
import { SAMPLE_GROUP_DETAIL } from "./fixtures.js";
import groupDocument from "./recorded/group.json" with { type: "json" };

const CLUSTER = "quickstart";
const GROUP = "analytics-indexer";
/** Where the product is mounted. The route composes addresses against it, as it does in a browser. */
const BASE = "/ui";

/** Solid batches writes onto a microtask; a router navigation takes a couple of turns to settle. */
async function settle(times = 8): Promise<void> {
  for (let index = 0; index < times; index += 1) await flush();
}

/** The recorded group, with no members: the state in which the group may be deleted at all. */
function memberlessGroup(): unknown {
  const copy = JSON.parse(JSON.stringify(groupDocument)) as Record<string, unknown>;
  copy["members"] = [];
  return copy;
}

/**
 * A second group id, held by nobody in the recording.
 *
 * Chosen so that it is not a substring of anything else the recorded document carries — the member
 * ids contain `indexer`, the topics contain `analytics` — because the case that uses it asks
 * whether the *other* group's name is on the page.
 */
const OTHER_GROUP = "orders-writer";

/** The recorded group renamed, so that "which group is drawn" is readable off the page. */
function groupNamed(groupId: string): unknown {
  const copy = memberlessGroup() as Record<string, unknown>;
  copy["groupId"] = groupId;
  return copy;
}

/** The one topic the recording holds offsets on, and the twelve partitions it holds them on. */
const FIRST_TOPIC = "analytics.pageviews";
/** A second topic, with a partition count that is neither the first's nor one. */
const SECOND_TOPIC = "orders.events";
/** A third, holding exactly one partition — the case a plural template gets wrong. */
const SINGLE_PARTITION_TOPIC = "audit.trail";

/**
 * The recorded group with two more topics, holding different numbers of partitions.
 *
 * The quickstart's group subscribes to one topic, so the recording has one row in its forget list
 * and every case that presses "the button" presses the only one there is. That is precisely the
 * arrangement in which "the count of the topic whose button was pressed" and "the count of the
 * first topic" are the same number, and the difference between them is the whole content of the
 * confirmation an operator is about to approve. The extra entries are the first one copied — every
 * field name stays the server's — with the topic renamed and the partition list cut.
 */
function groupHoldingThreeTopics(): unknown {
  const copy = JSON.parse(JSON.stringify(groupDocument)) as Record<string, unknown>;
  const topics = copy["topics"] as { topic: string; partitions: unknown[] }[];
  const first = topics[0];
  if (first === undefined) throw new Error("the recorded group holds no topics to copy");
  const held = (topic: string, count: number): { topic: string; partitions: unknown[] } => {
    const entry = JSON.parse(JSON.stringify(first)) as { topic: string; partitions: unknown[] };
    entry.topic = topic;
    entry.partitions = entry.partitions.slice(0, count);
    return entry;
  };
  topics.push(held(SECOND_TOPIC, 3), held(SINGLE_PARTITION_TOPIC, 1));
  return copy;
}

/**
 * A `permits` that answers yes to everything except one action — optionally, except on one group.
 *
 * Compared field by field rather than by identity: `Actions.…` is a frozen literal today, and a
 * case that relied on the two being the same object would start passing for the wrong reason the
 * day the constants are generated as a mapped type.
 *
 * ## `exceptOn`, and why this helper had to grow a second parameter
 *
 * For two waves this compared `{resource, action}` and **discarded the name**, so it answered the
 * same thing to `permits(action)` and to `permits(action, groupId)`. That made the subject in
 * `GroupRoute`'s `mayReset`/`mayDelete` unobservable: dropping `props.groupId` from either call
 * left every case in this package green. A grant carries a *pattern* —
 * `kernel/src/state/session.ts` is explicit that the subjectless form asks "the weaker question …
 * the right answer for a list heading and the wrong one for a row's delete button" — so an account
 * granted `CONSUMER_GROUP:RESET_OFFSETS` on `analytics.*` answers **yes** to the subjectless
 * question and **no** on `orders-writer`. `exceptOn` is that principal: denied the action everywhere except on
 * the group it names, and yes when nobody names a group at all.
 */
function permitsAllBut(
  denied: {
    readonly resource: string;
    readonly action: string;
  },
  options: { readonly exceptOn?: string } = {},
): KuiContextValue["permits"] {
  return (asked, name) => {
    if (asked.resource !== denied.resource || asked.action !== denied.action) return true;
    if (options.exceptOn === undefined) return false;
    // The weaker question, answered the way the server answers it: they do hold this action on
    // something, and only a named subject can turn that into a refusal.
    return name === undefined || name === options.exceptOn;
  };
}

/** What `POST …/offsets/plan` and `POST …/offsets` both answer with: a plan, and its token. */
function plan(noOp = false): unknown {
  return {
    topic: "analytics.pageviews",
    token: "plan-token-1",
    expiresAt: "2099-01-01T00:00:00Z",
    noOp,
    partitions: [{ partition: 0, current: 40, proposed: 0, delta: -40 }],
    warnings: [],
  };
}

interface Stub {
  readonly api: KuiApiClient;
  readonly calls: string[];
  /** The `topic` query each `DELETE …/offsets` carried, in order. */
  readonly forgotten: string[];
}

/**
 * A client that answers this group's four endpoints from a table and records what it was asked.
 *
 * Cast at one boundary, like every stub client in this workspace: `KuiApiClient`'s methods are typed
 * from the OpenAPI document, and a fake satisfying all of that would be a second copy of the schema.
 * The paths it matches are the literal strings the product passes, so a renamed endpoint fails here.
 */
function stub(options: {
  readonly detail?: unknown;
  readonly plan?: unknown;
  readonly applyFails?: string;
  readonly deleteFails?: string;
  /**
   * What `DELETE …/offsets` answers with. Absent means it removed every partition it was asked.
   *
   * `partitions` is optional here because it is optional on the wire — `DeletedOffsetsDto` declares
   * it so in `schema.d.ts` — and an answer that names none is therefore a state the server can
   * really be in, not a malformed document.
   */
  readonly forgetAnswer?: { readonly topic: string; readonly partitions?: readonly number[] };
  readonly forgetFails?: string;
  /** Held until this resolves, so a case can press the confirmation while the DELETE is out. */
  readonly forgetPending?: Promise<void>;
  /**
   * A detail document per group id, for the one case that visits two groups.
   *
   * The route's fetch is keyed by `{clusterId, groupId}` and the effect that runs it has a
   * cancellation guard; the only way to observe that guard is to have two groups answering
   * differently and to make the *first* one land last. `detail` stays for every other case, which
   * asks about one group and does not care which.
   */
  readonly detailByGroup?: Readonly<Record<string, unknown>>;
  /** Held until this resolves, per group id, so a case can move on while a GET is still out. */
  readonly detailPending?: Readonly<Record<string, Promise<void>>>;
}): Stub {
  const calls: string[] = [];
  const forgotten: string[] = [];
  const refuse = (message: string) => ({
    ok: false,
    error: {
      kind: "envelope",
      code: "KUI-CONFLICT",
      message,
      details: [],
      correlationId: "test",
      retryable: false,
    },
  });

  const get = async (
    path: string,
    init?: { readonly params?: { readonly path?: { readonly groupId?: string } } },
  ) => {
    calls.push(`GET ${path}`);
    if (path === "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}") {
      const groupId = init?.params?.path?.groupId ?? GROUP;
      const held = options.detailPending?.[groupId];
      if (held !== undefined) await held;
      const named = options.detailByGroup?.[groupId];
      return { ok: true, value: named ?? options.detail ?? memberlessGroup() };
    }
    return { ok: false, error: { kind: "unreachable", cause: "nothing answers that here" } };
  };
  const post = async (path: string) => {
    calls.push(`POST ${path}`);
    if (path === "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/offsets/plan") {
      return { ok: true, value: options.plan ?? plan() };
    }
    if (path === "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/offsets") {
      return options.applyFails === undefined
        ? { ok: true, value: plan() }
        : refuse(options.applyFails);
    }
    return { ok: false, error: { kind: "unreachable", cause: "nothing answers that here" } };
  };
  const remove = async (
    path: string,
    init?: { readonly params?: { readonly query?: { readonly topic?: string } } },
  ) => {
    calls.push(`DELETE ${path}`);
    if (path === "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}") {
      return options.deleteFails === undefined
        ? { ok: true, value: {} }
        : refuse(options.deleteFails);
    }
    if (path === "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/offsets") {
      /* The topic is a *query* parameter, not a path segment, because the resource is "this
         group's offsets" narrowed by topic. Recorded rather than ignored: a request that reached
         the endpoint without it would forget nothing and answer 400, and a stub that dropped it
         would let this suite pass over that. */
      const topic = init?.params?.query?.topic ?? "";
      forgotten.push(topic);
      if (options.forgetPending !== undefined) await options.forgetPending;
      if (options.forgetFails !== undefined) return refuse(options.forgetFails);
      return {
        ok: true,
        value: options.forgetAnswer ?? { topic, partitions: [0, 1, 2] },
      };
    }
    return { ok: false, error: { kind: "unreachable", cause: "nothing answers that here" } };
  };

  return {
    calls,
    forgotten,
    api: {
      get,
      post,
      put: post,
      patch: post,
      delete: remove,
      raw: {},
    } as unknown as KuiApiClient,
  };
}

/**
 * Mounts the route at this group's own address, inside a real router.
 *
 * `memoryHistory` rather than the browser's, so nothing writes into the runner's own address bar —
 * but it is the same adapter contract, so the delete's navigation is observable here exactly as it
 * is in a browser. That is the point of `url()`: the route used to leave with
 * `window.location.assign`, which is a *document* navigation, and a document navigation destroys
 * the module-level toast store the line above it had just written to.
 */
function openGroup(
  api: KuiApiClient,
  /** Yes to everything unless a case says otherwise — see `testContext`. */
  permits: KuiContextValue["permits"] = () => true,
  /** Where to start. Only the case that visits a second group ever passes this. */
  at: string = `${BASE}/clusters/${CLUSTER}/consumer-groups/${GROUP}`,
): {
  readonly container: HTMLElement;
  readonly dispose: () => void;
  readonly url: () => string;
  /**
   * Moves the mounted route to another address, the way the group list's link does.
   *
   * A push onto the same history rather than a second mount: the route's cancellation guard is on
   * the *effect that re-runs when `groupId` changes*, and a case that mounted twice would never run
   * that effect a second time and could not see the guard at all.
   */
  readonly goTo: (address: string) => void;
} {
  const history = memoryHistory(at);
  const Router = createRouter({
    routes: [
      { path: "/clusters/:clusterId/consumer-groups/:groupId", component: GroupRoute },
      /* The list's address has to be a route this router knows, or the navigation the delete makes
         resolves to nothing and the case would pass on a screen showing a 404. */
      { path: "/clusters/:clusterId/consumer-groups", component: () => <p>The list</p> },
    ],
    base: BASE,
    history,
    scrollRestoration: false,
  });
  const mounted = mount(() => (
    <KuiProvider value={testContext(api, permits)}>
      <Router />
    </KuiProvider>
  ));
  return {
    ...mounted,
    url: () => history.get(),
    goTo: (address: string) => history.set({ value: address }),
  };
}

/** A button inside one root, by the words on it. */
function press(root: HTMLElement, label: RegExp): void {
  const button = [...root.querySelectorAll("button")].find((candidate) =>
    label.test((candidate.textContent ?? "").trim()),
  );
  if (button === undefined) {
    const seen = [...root.querySelectorAll("button")]
      .map((candidate) => (candidate.textContent ?? "").trim())
      .join(" | ");
    throw new Error(`no button matching ${String(label)}; this root offers: ${seen}`);
  }
  button.click();
}

/**
 * The confirmation on screen.
 *
 * `Dialog` renders through a `Portal` into `document.body`, so it is not inside the container the
 * route was mounted in — and Solid tears a portal down on its own schedule rather than synchronously
 * in `dispose()`, so a query across the whole body can find the *previous* case's dialog. The last
 * match is unambiguously the one this case opened. Scoping matters twice over here: the page's own
 * `Delete group` action and the confirmation's `Delete group` button read identically, and a helper
 * that took the first match would press the action again and confirm nothing.
 */
function confirmation(): HTMLElement {
  const all = document.body.querySelectorAll<HTMLElement>("[role='dialog']");
  const last = all[all.length - 1];
  if (last === undefined) throw new Error("no confirmation is open");
  return last;
}

const titles = (): readonly string[] => toasts().map((toast) => toast.title);

/**
 * The forget confirmations currently in the document — normally none or one.
 *
 * Scoped by `testId` rather than by `role`, because the page's *other* confirmation is a
 * `[role="dialog"]` too and "the forget confirmation closed" must not be satisfiable by the delete
 * one having never opened.
 */
function forgetConfirmations(): readonly HTMLElement[] {
  return [...document.body.querySelectorAll<HTMLElement>("[data-testid='group-forget-confirm']")];
}

/** The one open forget confirmation. */
function forgetConfirmation(): HTMLElement {
  const open = forgetConfirmations();
  const last = open[open.length - 1];
  if (last === undefined) throw new Error("no forget confirmation is open");
  return last;
}

/**
 * The forget row for one topic, by the topic it names.
 *
 * The list is per topic and its rows differ only in their topic and their held count, so a helper
 * that took the first button would press `analytics.pageviews`'s no matter which topic the case is
 * about — which is the exact confusion the `find` in `consequenceOfForget` exists to prevent.
 */
function forgetRow(root: HTMLElement, topic: string): HTMLElement {
  const rows = [...root.querySelectorAll<HTMLElement>("li.kui-cg-forget__row")];
  const row = rows.find(
    (one) => (one.querySelector(".kui-cg-forget__topic")?.textContent ?? "").trim() === topic,
  );
  if (row === undefined) {
    const seen = rows
      .map((one) => (one.querySelector(".kui-cg-forget__topic")?.textContent ?? "").trim())
      .join(" | ");
    throw new Error(`no forget row for ${topic}; the page offers: ${seen}`);
  }
  return row;
}

/**
 * The reason under one disabled control, read through that control's own `aria-describedby`.
 *
 * Not `document.body.querySelector('[role="tooltip"]')`: the bubble is a portal into `body`, and an
 * earlier case's can still be there — so the first match in the document is not necessarily this
 * button's, and a case reading it would assert a sentence from the case before it.
 */
async function reasonUnder(button: HTMLButtonElement): Promise<string> {
  button.dispatchEvent(new FocusEvent("focusin", { bubbles: true }));
  await settle();
  const id = button.getAttribute("aria-describedby") ?? "";
  const bubble = id === "" ? null : document.getElementById(id);
  if (bubble === null) throw new Error(`no tooltip is attached to ${button.textContent ?? ""}`);
  return bubble.textContent ?? "";
}

/** Every "Forget offsets" button on the page, in the order the topics are listed. */
function forgetButtons(root: HTMLElement): readonly HTMLButtonElement[] {
  return [...root.querySelectorAll<HTMLButtonElement>("button")].filter(
    (candidate) => (candidate.textContent ?? "").trim() === "Forget offsets",
  );
}

describe("the group page's destructive controls", () => {
  beforeEach(() => {
    // Module-level state, shared by the whole process: one case's confirmation would otherwise be
    // the next case's evidence.
    clearToasts();
  });

  it("raises a toast when a reset is applied, naming the group the offsets belong to", async () => {
    const { api } = stub({});
    const { container, dispose } = openGroup(api);
    await settle();

    press(container, /^Reset offsets$/);
    await settle();
    press(container, /^Preview the plan$/);
    await settle();
    press(container, /^Apply this plan$/);
    await settle();

    /*
     * The wizard's own receipt is on screen and says what the broker wrote — but once the operator
     * closes the wizard, the page behind it looks exactly as it did before with different numbers.
     * The toast is what survives that, which is why the rule is "on the destructive success", not
     * "somewhere on the screen". The receipt itself is the next case's subject.
     */
    expect(titles()).toContain("Offsets reset");
    const raised = toasts().find((toast) => toast.title === "Offsets reset");
    expect(raised?.message).toContain(GROUP);
    // Not `danger`: this succeeded. A `danger` toast never auto-dismisses, and one that stays on
    // screen after a success reads as an error nobody explained.
    expect(raised?.tone).toBe("success");

    dispose();
  });

  it("keeps the wizard's receipt on screen while the page re-reads the group behind it", async () => {
    /*
     * The receipt is the operator's one record of what the broker wrote. The route re-reads the
     * group after a successful apply, and that re-read used to drop the page to `loading`: the
     * `<Show when={group()}>` around the whole page fell back to its loading sentence, disposed
     * `GroupDetail` and the wizard inside it, and the page that came back had a fresh, closed
     * wizard. The toast case above stays green over that, because the toast lives in the kernel.
     */
    const { api, calls } = stub({});
    const { container, dispose } = openGroup(api);
    await settle();
    const readsBefore = calls.filter((call) => call.startsWith("GET ")).length;

    press(container, /^Reset offsets$/);
    await settle();
    press(container, /^Preview the plan$/);
    await settle();
    press(container, /^Apply this plan$/);
    // Long enough for the re-read the apply starts to come back and redraw the page.
    await settle(24);

    // The re-read still happens: the figures behind the wizard describe the group before the reset.
    expect(calls.filter((call) => call.startsWith("GET ")).length).toBeGreaterThan(readsBefore);
    const receipt = container.querySelector('[data-testid="group-reset-receipt"]');
    expect(receipt, "the receipt is gone from the page").not.toBeNull();
    const rows = [...(receipt?.querySelectorAll('[data-testid="group-reset-receipt-table"] tbody tr') ?? [])];
    const cells = rows.map((row) =>
      [...row.querySelectorAll("td")].map((cell) => (cell.textContent ?? "").trim()),
    );
    // The stub's answer to the apply: partition 0, written from 40 to 0.
    expect(cells).toHaveLength(1);
    expect(cells[0]?.slice(0, 3)).toEqual(["0", "40", "0"]);

    dispose();
  });

  it("re-reads the group after a forget without tearing the page down", async () => {
    /*
     * The same re-read, on the forget path. The page an operator is reading should change its
     * figures, not blank to "Asking the coordinator…" and be rebuilt from scratch.
     */
    const { api, calls } = stub({});
    const { container, dispose } = openGroup(api);
    await settle();
    const page = container.querySelector('[data-testid="group-assignments-card"]');
    expect(page).not.toBeNull();
    const readsBefore = calls.filter((call) => call.startsWith("GET ")).length;

    press(forgetRow(container, FIRST_TOPIC), /^Forget offsets$/);
    await settle();
    press(forgetConfirmation(), /^Forget offsets$/);
    await settle(24);

    expect(titles()).toContain("Committed offsets forgotten");
    expect(calls.filter((call) => call.startsWith("GET ")).length).toBeGreaterThan(readsBefore);
    // The same element, not a new one drawn after a loading sentence.
    expect(container.querySelector('[data-testid="group-assignments-card"]')).toBe(page);

    dispose();
  });

  it("raises no toast when the cluster refuses the reset", async () => {
    /*
     * The other half of the rule, and the half that makes the first one mean something: a toast is
     * the confirmation of a thing that *happened*. A route that raised one from the click rather
     * than from the answer would pass the case above and be wrong here.
     */
    const { api } = stub({ applyFails: "the plan token has expired" });
    const { container, dispose } = openGroup(api);
    await settle();

    press(container, /^Reset offsets$/);
    await settle();
    press(container, /^Preview the plan$/);
    await settle();
    press(container, /^Apply this plan$/);
    await settle();

    expect(titles()).toEqual([]);
    // And the refusal is on the screen, where the operator is looking.
    expect(container.textContent).toContain("the plan token has expired");

    dispose();
  });

  it("shows a partition the group never committed on as a dash, never as offset zero", async () => {
    /*
     * `write.ts`'s most argued-for line, asserted where the product applies it rather than where a
     * test arranges it. `toPlannedPartition` is not exported, so the only honest seam is the
     * server's own payload going in and the plan table coming out — and the payload here is the
     * one shape the recorded plan cannot be: a partition whose `current` field is **absent**,
     * which is how the server says this group has never committed a position there.
     *
     * `current: payload.current ?? 0` satisfies every type on that path and draws a `0` in the
     * From column, which tells an operator the group has consumed the first record when it has
     * consumed nothing — and then the reset they are about to approve reads as a rewind of one
     * record instead of a first commit.
     */
    const { api } = stub({
      plan: {
        topic: "analytics.pageviews",
        token: "plan-token-1",
        expiresAt: "2099-01-01T00:00:00Z",
        noOp: false,
        partitions: [
          { partition: 0, current: 40, proposed: 0, delta: -40 },
          // No `current` and no `delta`: nothing has ever been committed here.
          { partition: 1, proposed: 0 },
        ],
        warnings: [],
      },
    });
    const { container, dispose } = openGroup(api);
    await settle();

    press(container, /^Reset offsets$/);
    await settle();
    press(container, /^Preview the plan$/);
    await settle();

    const rows = [...container.querySelectorAll('[data-testid="group-reset-plan"] tbody tr')];
    expect(rows).toHaveLength(2);
    const cells = (row: Element | undefined): readonly string[] =>
      [...(row?.querySelectorAll("td") ?? [])].map((cell) => (cell.textContent ?? "").trim());

    // The partition that has a position keeps its number, so this is not a table of dashes.
    expect(cells(rows[0])[1]).toBe("40");
    // And the one that has none draws the em dash the column's own comment promises.
    expect(cells(rows[1])[1]).toBe("—");
    expect(cells(rows[1])[1]).not.toBe("0");

    dispose();
  });

  it("raises a toast when the group is deleted, and leaves through the router so it survives", async () => {
    /*
     * `GroupRoute` navigates away on success. The list it arrives at has no trace of what happened —
     * the group is simply not there, which is indistinguishable from having mistyped the address —
     * so the toast is the only account of it.
     *
     * Which is why the address is asserted beside the toast. The route left with
     * `window.location.assign`, a document navigation: the toast was raised into a module-level
     * store and the store was thrown away with the document a moment later. A case that only
     * checked `toasts()` would have gone green over a confirmation nobody could ever see.
     */
    const { api } = stub({});
    const { container, dispose, url } = openGroup(api);
    await settle();

    press(container, /^Delete group$/);
    await settle();
    press(confirmation(), /^Delete group$/);
    await settle();

    expect(titles()).toContain("Consumer group deleted");
    const raised = toasts().find((toast) => toast.title === "Consumer group deleted");
    expect(raised?.message).toContain(GROUP);
    // The sentence an operator needs beside a red button: the offsets went, the records did not.
    expect(raised?.message).toContain("No records were deleted.");
    // The list, through the router — and with the mount point exactly once. A `KuiPaths` address
    // already carries the base, so a navigation that resolved it again would land on `/ui/ui/…`.
    expect(url()).toBe(`${BASE}/clusters/${CLUSTER}/consumer-groups`);

    dispose();
  });

  it("forgets one topic's offsets and reports how many partitions the server removed", async () => {
    /*
     * `CG-005`'s exit, in one case: the control is on the page per topic, and the receipt is
     * asserted at the route.
     *
     * Three things are checked and none of them can be met by the others. The request carries the
     * topic as a **query** parameter, which is where the endpoint puts it — a request without it
     * forgets nothing and comes back 400. The toast quotes the *server's* partition count and not
     * the figure the page was drawing, because the two are the same only until somebody else has
     * reset the group between the page load and the click. And the group is still here afterwards:
     * this removes committed positions, not the group, and a screen that navigated away would be
     * telling the operator it had done the other thing.
     */
    const { api, calls, forgotten } = stub({
      forgetAnswer: { topic: "analytics.pageviews", partitions: [0, 1, 2, 3, 4] },
    });
    const { container, dispose, url } = openGroup(api);
    await settle();

    press(container, /^Forget offsets$/);
    await settle();

    /* The consequence, before the click, in this group's own figures. The recorded group holds
       twelve partitions of `analytics.pageviews`, and the number is what makes this a consequence
       rather than an adjective — the dialog's own contract, and the sentence beside it is the one
       that decides whether an operator should press the button at all. */
    const asking = confirmation().textContent ?? "";
    expect(asking).toContain("12 partitions of analytics.pageviews");
    expect(asking).toContain("No records are deleted.");
    expect(asking).toContain("auto.offset.reset");

    press(confirmation(), /^Forget offsets$/);
    await settle();

    expect(forgotten).toEqual(["analytics.pageviews"]);

    expect(titles()).toContain("Committed offsets forgotten");
    const raised = toasts().find((toast) => toast.title === "Committed offsets forgotten");
    expect(raised?.tone).toBe("success");
    // The server's five, not the twelve the recorded group holds — so the sentence cannot be
    // composed from anything the browser already knew.
    expect(raised?.message).toContain("5 partitions");
    expect(raised?.message).toContain("analytics.pageviews");
    // The sentence that decides whether this was safe, and the one an operator forgets.
    expect(raised?.message).toContain("No records were deleted.");

    expect(url()).toBe(`${BASE}/clusters/${CLUSTER}/consumer-groups/${GROUP}`);

    /* And the page behind the dialog was re-read. The assignments table is drawn from offsets that
       have just changed, so without this it keeps showing committed positions Kafka no longer
       holds — figures that were true a second ago, which is the most convincing kind of wrong. */
    const reads = calls.filter(
      (call) => call === "GET /api/v1/clusters/{clusterId}/consumer-groups/{groupId}",
    );
    expect(reads).toHaveLength(2);

    dispose();
  });

  it("says one partition rather than 1 partitions when the server removed exactly one", async () => {
    // The singular, which the plural template gets wrong on every single-partition topic — most of
    // what a scratch cluster holds. Its twin on the way in (`1 partition held`) has a case in
    // `consumers.test.tsx`; this is the receipt's.
    const { api } = stub({ forgetAnswer: { topic: "analytics.pageviews", partitions: [3] } });
    const { container, dispose } = openGroup(api);
    await settle();

    press(container, /^Forget offsets$/);
    await settle();
    press(confirmation(), /^Forget offsets$/);
    await settle();

    const raised = toasts().find((toast) => toast.title === "Committed offsets forgotten");
    expect(raised?.message).toContain("1 partition of analytics.pageviews");
    expect(raised?.message).not.toContain("1 partitions");

    dispose();
  });

  it("does not show the last refusal over a confirmation that has just been reopened", async () => {
    /*
     * A refusal belongs to the attempt that earned it. The mutation's state outlives the dialog —
     * the dialog is a `Show`, the mutation is not — so without a reset on the way in, reopening
     * this confirmation puts "cluster is read-only" over a fresh question about a topic the
     * operator has just chosen, which reads as a refusal of the thing they have not asked yet.
     */
    const { api } = stub({ forgetFails: "cluster 'quickstart' is read-only" });
    const { container, dispose } = openGroup(api);
    await settle();

    press(container, /^Forget offsets$/);
    await settle();
    press(confirmation(), /^Forget offsets$/);
    await settle();
    expect(confirmation().textContent).toContain("read-only");

    // Closed, and asked again.
    press(confirmation(), /^Cancel$/);
    await settle();
    press(container, /^Forget offsets$/);
    await settle();

    expect(confirmation().textContent).not.toContain("read-only");

    dispose();
  });

  it("says nothing was forgotten, in a warning, when the group held no offsets there", async () => {
    /*
     * The shape this whole product keeps meeting: a 200 whose entire meaning is in a figure.
     *
     * `DELETE …/offsets` answers 200 with an **empty** partition list when the group held no
     * committed position on that topic — the endpoint's own description says the body exists so
     * that "the group had none" and "they were deleted" stay distinguishable, which a bare status
     * code cannot do. A green tick over the empty case sends an operator away believing a position
     * they can still see elsewhere was removed.
     */
    const { api } = stub({ forgetAnswer: { topic: "analytics.pageviews", partitions: [] } });
    const { container, dispose } = openGroup(api);
    await settle();

    press(container, /^Forget offsets$/);
    await settle();
    press(confirmation(), /^Forget offsets$/);
    await settle();

    expect(titles()).toContain("Nothing was forgotten");
    const raised = toasts().find((toast) => toast.title === "Nothing was forgotten");
    // The tone is the assertion. The wording is what a reader skims; the colour is what they see
    // from across the room.
    expect(raised?.tone).toBe("warning");
    expect(raised?.message).toContain("held no committed offset");
    expect(titles()).not.toContain("Committed offsets forgotten");
    // And above all not a zero dressed as a result: `0 partitions` reads as an action that ran.
    expect(raised?.message).not.toContain("0 partitions");

    dispose();
  });

  it("raises no toast when the cluster refuses to forget the offsets", async () => {
    const { api } = stub({ forgetFails: "cluster 'quickstart' is read-only" });
    const { container, dispose } = openGroup(api);
    await settle();

    press(container, /^Forget offsets$/);
    await settle();
    press(confirmation(), /^Forget offsets$/);
    await settle();

    expect(titles()).toEqual([]);
    // The confirmation stays open carrying the server's own words, where the operator is looking.
    expect(confirmation().textContent).toContain("read-only");

    dispose();
  });

  it("a principal without ConsumerGroupResetOffsets is never handed an enabled forget control", async () => {
    /*
     * The most serious hole this package had, and the reason a component case was not enough.
     *
     * `GroupDetail` renders the control disabled with a reason when it is handed no callback, and
     * that rendering has its own case in `consumers.test.tsx`. What decides whether it is handed
     * one is a ternary in `GroupRoute` — `onForgetOffsets={mayReset() ? … : undefined}` — and until
     * this case nothing in the repository ever mounted the route unpermitted. Replacing the whole
     * gate with `true` left 1449 frontend cases green while handing an account that may not reset
     * offsets an **enabled** destructive button, and made `forgetRefusal` unreachable.
     *
     * Both arrangements are constructed, in one case, because a screen that disabled the control
     * for everybody would satisfy the refusal half and be just as wrong.
     */
    const permitted = stub({});
    const withPermission = openGroup(permitted.api);
    await settle();

    const allowed = forgetButtons(withPermission.container);
    expect(allowed.length).toBeGreaterThan(0);
    for (const button of allowed) expect(button.getAttribute("aria-disabled")).toBeNull();

    withPermission.dispose();

    const refused = stub({});
    const { container, dispose } = openGroup(
      refused.api,
      permitsAllBut(Actions.ConsumerGroupResetOffsets),
    );
    await settle();

    /* Present, and disabled. A control that vanished would teach the operator that KUI cannot do
       this at all, when the truth is that this account may not. */
    const buttons = forgetButtons(container);
    expect(buttons).toHaveLength(allowed.length);
    for (const button of buttons) expect(button.getAttribute("aria-disabled")).toBe("true");

    // And the sentence is reachable from a keyboard, which is why the control is `aria-disabled`
    // rather than `disabled` — the tooltip is the only place the reason is written.
    const first = buttons[0];
    if (first === undefined) throw new Error("the forget list drew no control at all");
    expect(await reasonUnder(first)).toContain(
      "You do not have permission to change this group's committed offsets.",
    );

    // Pressing it does nothing: no confirmation, and above all no request. `aria-disabled` does not
    // stop the browser dispatching a click, so "the button is marked disabled" and "the button does
    // nothing" are two different facts and this product has shipped the first without the second.
    first.click();
    await settle();
    expect(forgetConfirmations()).toHaveLength(0);
    expect(refused.forgotten).toEqual([]);

    dispose();
  });

  it("gates each of this page's three controls on its own action", async () => {
    /*
     * The same hole as the case above, at the other two controls on the page, and it is a separate
     * case because a single boolean over all three satisfies any one of them.
     *
     * `Reset offsets` and `Forget offsets` are both `ConsumerGroupResetOffsets` — they write the
     * same committed positions, which is what the endpoints' own `EndpointAuthorization` says —
     * while `Delete group` is `ConsumerGroupDelete`. So the arrangement that separates them is an
     * account trusted to delete a group and not to move its offsets: it must be offered the delete
     * and refused both of the others. Wave 5's topic packet named exactly this shape — "four
     * controls wired to four actions being indistinguishable under a single boolean" — as its own
     * worst finding, and every one of these three was a constant no case could observe.
     */
    const reset = (root: HTMLElement): HTMLButtonElement => {
      const button = [...root.querySelectorAll<HTMLButtonElement>("button")].find(
        (candidate) => (candidate.textContent ?? "").trim() === "Reset offsets",
      );
      if (button === undefined) throw new Error("the page offers no reset control");
      return button;
    };
    const remove = (root: HTMLElement): HTMLButtonElement => {
      const button = [...root.querySelectorAll<HTMLButtonElement>("button")].find(
        (candidate) => (candidate.textContent ?? "").trim() === "Delete group",
      );
      if (button === undefined) throw new Error("the page offers no delete control");
      return button;
    };

    // Everything held: all three are live. Without this the two refusals below are met by a screen
    // that offers nothing to anybody.
    const all = openGroup(stub({}).api);
    await settle();
    expect(reset(all.container).getAttribute("aria-disabled")).toBeNull();
    expect(remove(all.container).getAttribute("aria-disabled")).toBeNull();
    expect(forgetButtons(all.container)[0]?.getAttribute("aria-disabled")).toBeNull();
    all.dispose();

    // May delete, may not move offsets: the delete stays live and both offset controls refuse.
    const offsets = openGroup(stub({}).api, permitsAllBut(Actions.ConsumerGroupResetOffsets));
    await settle();
    expect(remove(offsets.container).getAttribute("aria-disabled")).toBeNull();
    const wizard = reset(offsets.container);
    expect(wizard.getAttribute("aria-disabled")).toBe("true");
    expect(await reasonUnder(wizard)).toContain(
      "You do not have permission to reset this group's offsets.",
    );
    expect(forgetButtons(offsets.container)[0]?.getAttribute("aria-disabled")).toBe("true");
    offsets.dispose();

    // The mirror: may move offsets, may not delete the group.
    const deletion = openGroup(stub({}).api, permitsAllBut(Actions.ConsumerGroupDelete));
    await settle();
    expect(reset(deletion.container).getAttribute("aria-disabled")).toBeNull();
    expect(forgetButtons(deletion.container)[0]?.getAttribute("aria-disabled")).toBeNull();
    const destroy = remove(deletion.container);
    expect(destroy.getAttribute("aria-disabled")).toBe("true");
    expect(await reasonUnder(destroy)).toContain(
      "You do not have permission to delete this consumer group.",
    );
    /* And it opens nothing. The delete confirmation is the page's other `[role="dialog"]`, so this
       is asserted on the *count* of dialogs rather than on the forget testId. */
    destroy.click();
    await settle();
    expect(document.body.querySelectorAll("[role='dialog']")).toHaveLength(0);
    deletion.dispose();
  });

  it("asks about this group, not about groups in general, before offering a delete", async () => {
    /*
     * ## The rule this case exists for
     *
     * `GroupRoute` asks `kui.permits(Actions.ConsumerGroupResetOffsets, props.groupId)` and
     * `kui.permits(Actions.ConsumerGroupDelete, props.groupId)`. The **subject** is the rule: a
     * grant carries a pattern, so an account granted `CONSUMER_GROUP:RESET_OFFSETS` on
     * `analytics.*` holds the action and may not use it here. `kernel/src/state/session.ts` says
     * it in the accessor's own docstring — the subjectless form asks "the weaker question … the
     * right answer for a list heading and the wrong one for a row's delete button" — and this page
     * is nothing but row-level destructive buttons.
     *
     * ## Why nothing could see it until now
     *
     * The two cases above drive `permitsAllBut`, which compared `{resource, action}` and threw the
     * name away, so it answered identically to `permits(action)` and `permits(action, groupId)`.
     * Dropping `props.groupId` from either call therefore left all 110 of this package's cases
     * green while putting a **live** Forget button and a live Delete in front of an account whose
     * grant names another group. The helper takes `exceptOn` now, which is that principal.
     *
     * ## The arrangement
     *
     * Both directions, in one case, because a screen that refused everybody would satisfy the
     * refusal half and be just as wrong. The grant is on `analytics-indexer` in the first half and
     * on `orders-writer` in the second, and the page is `analytics-indexer` throughout.
     */
    const heldHere = openGroup(
      stub({}).api,
      permitsAllBut(Actions.ConsumerGroupResetOffsets, { exceptOn: GROUP }),
    );
    await settle();
    const resetHere = [...heldHere.container.querySelectorAll<HTMLButtonElement>("button")].find(
      (one) => (one.textContent ?? "").trim() === "Reset offsets",
    );
    expect(resetHere?.getAttribute("aria-disabled")).toBeNull();
    expect(forgetButtons(heldHere.container)[0]?.getAttribute("aria-disabled")).toBeNull();
    heldHere.dispose();

    // The same account, with the same action, granted on a group that is not this one.
    const heldElsewhere = openGroup(
      stub({}).api,
      permitsAllBut(Actions.ConsumerGroupResetOffsets, { exceptOn: OTHER_GROUP }),
    );
    await settle();
    const resetElsewhere = [
      ...heldElsewhere.container.querySelectorAll<HTMLButtonElement>("button"),
    ].find((one) => (one.textContent ?? "").trim() === "Reset offsets");
    expect(resetElsewhere).toBeDefined();
    expect(resetElsewhere?.getAttribute("aria-disabled")).toBe("true");
    if (resetElsewhere !== undefined) {
      expect(await reasonUnder(resetElsewhere)).toContain(
        "You do not have permission to reset this group's offsets.",
      );
    }
    const forgetElsewhere = forgetButtons(heldElsewhere.container);
    expect(forgetElsewhere.length).toBeGreaterThan(0);
    for (const button of forgetElsewhere) {
      expect(button.getAttribute("aria-disabled")).toBe("true");
    }
    /* And it does nothing when pressed. `aria-disabled` does not stop a click reaching the
       handler, so "marked disabled" and "does nothing" are two facts and this product has shipped
       the first without the second. */
    forgetElsewhere[0]?.click();
    await settle();
    expect(forgetConfirmations()).toHaveLength(0);
    heldElsewhere.dispose();

    // The delete control's own subject, which is a second `permits` call on a second action.
    const deleteElsewhere = openGroup(
      stub({}).api,
      permitsAllBut(Actions.ConsumerGroupDelete, { exceptOn: OTHER_GROUP }),
    );
    await settle();
    const destroy = [
      ...deleteElsewhere.container.querySelectorAll<HTMLButtonElement>("button"),
    ].find((one) => (one.textContent ?? "").trim() === "Delete group");
    expect(destroy?.getAttribute("aria-disabled")).toBe("true");
    if (destroy !== undefined) {
      expect(await reasonUnder(destroy)).toContain(
        "You do not have permission to delete this consumer group.",
      );
    }
    deleteElsewhere.dispose();
  });

  it("discards a group's answer that arrives after the reader has moved on", async () => {
    /*
     * The `if (!cancelled)` guard in `GroupRoute`'s fetch effect, which nothing could reach while
     * every case mounted one group and waited.
     *
     * The failure it prevents is the worst kind this product has: real figures for the wrong
     * subject. The group list links straight to a group, so following two links in quick
     * succession is an ordinary gesture; without the guard the slower request wins, and the page
     * then shows one group's title with another group's committed offsets underneath it — which is
     * exactly what a reset is composed from.
     *
     * `analytics-indexer` is held until this case releases it, so it lands *after* `orders-writer`
     * has already been drawn. Deleting the guard leaves the last write standing and the page
     * reverts to the group nobody is looking at.
     */
    let release = (): void => {};
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    const { api } = stub({
      detailByGroup: { [GROUP]: groupNamed(GROUP), [OTHER_GROUP]: groupNamed(OTHER_GROUP) },
      detailPending: { [GROUP]: held },
    });
    const { container, dispose, goTo } = openGroup(api);
    await settle();
    // Still out: the page is on its loading sentence and has drawn no group at all.
    expect(container.textContent).toContain("Asking the coordinator");

    goTo(`${BASE}/clusters/${CLUSTER}/consumer-groups/${OTHER_GROUP}`);
    await settle();
    expect(container.textContent).toContain(OTHER_GROUP);

    release();
    await settle();

    // The heading is the assertion: `GroupDetail` draws `group.groupId`, so this is the document
    // that reached the screen and not merely the address in the bar.
    const heading = container.querySelector(".kui-page-head__title")?.textContent ?? "";
    expect(heading).toBe(OTHER_GROUP);
    expect(container.textContent).not.toContain(GROUP);

    dispose();
  });

  it("a successful forget closes its confirmation", async () => {
    /*
     * The dialog is a `<Show when={forgetting()}>`, and the success path both clears that signal
     * and re-reads the group. Without the clear, the armed confirmation stays open over the
     * re-read page, now describing a group that no longer holds those offsets, with a `Forget
     * offsets` button that would take the branch the receipt calls impossible. Deleting
     * `setForgetting(undefined)` left every other case here green because they all assert on
     * toasts, which are raised either way.
     */
    const { api } = stub({});
    const { container, dispose } = openGroup(api);
    await settle();

    press(forgetRow(container, FIRST_TOPIC), /^Forget offsets$/);
    await settle();
    expect(forgetConfirmations()).toHaveLength(1);

    press(forgetConfirmation(), /^Forget offsets$/);
    // Long enough for the refetch the success path starts to come back and redraw the page: the
    // reopening this guards against happens *after* the reload, not instead of it.
    await settle(24);

    expect(titles()).toContain("Committed offsets forgotten");
    expect(forgetConfirmations()).toHaveLength(0);

    dispose();
  });

  it("the confirmation names the partition count of the topic whose button was pressed", async () => {
    /*
     * `consequenceOfForget` looks its topic up by name. Replaced with the first subscription, it
     * stays green on every case above, because the recorded group holds exactly one topic — so the
     * first row and the pressed row are the same row and the two readings cannot be told apart.
     *
     * Here the group holds two, with different partition counts, and the second one's button is
     * pressed. The number is the whole content of the confirmation: it is what the receipt
     * afterwards is read against, and quoting another topic's count would make the operator agree
     * to a consequence that is not the one they are about to cause.
     */
    const { api } = stub({ detail: groupHoldingThreeTopics() });
    const { container, dispose } = openGroup(api);
    await settle();

    press(forgetRow(container, SECOND_TOPIC), /^Forget offsets$/);
    await settle();

    const asking = forgetConfirmation().textContent ?? "";
    expect(asking).toContain(`Forget ${GROUP}'s offsets on ${SECOND_TOPIC}?`);
    expect(asking).toContain(`3 partitions of ${SECOND_TOPIC}`);
    // The first topic's twelve, which is what a lookup that ignored the pressed row would quote.
    expect(asking).not.toContain("12 partitions");

    dispose();
  });

  it("lists the topics a group holds offsets on in topic order", async () => {
    /*
     * `subscriptions()` sorts, and nothing asserted it. Unsorted, the rows come out in the order the
     * coordinator happened to send them — which is stable for one call and not across calls, so the
     * row an operator's hand is going towards moves between two loads of the same page. On a page
     * whose every row carries a destructive button, the order is part of the safety of the control,
     * not a tidiness preference.
     *
     * The fixture is built in wire order `analytics.pageviews, orders.events, audit.trail`
     * precisely so that sorted and unsorted differ.
     */
    const { api } = stub({ detail: groupHoldingThreeTopics() });
    const { container, dispose } = openGroup(api);
    await settle();

    const drawn = [...container.querySelectorAll(".kui-cg-forget__topic")].map((one) =>
      (one.textContent ?? "").trim(),
    );
    expect(drawn).toEqual([FIRST_TOPIC, SINGLE_PARTITION_TOPIC, SECOND_TOPIC]);

    dispose();
  });

  it("says one partition rather than 1 partitions in the confirmation", async () => {
    /*
     * The singular on the way *in*. Its twin on the receipt has had a case since wave 5; this one
     * did not, and a plural template is wrong on every single-partition topic — which is most of
     * what a scratch cluster holds. It is the sentence the operator reads *before* deciding, so
     * "1 partitions" is a typo in the one paragraph the product asks them to trust.
     */
    const { api } = stub({ detail: groupHoldingThreeTopics() });
    const { container, dispose } = openGroup(api);
    await settle();

    press(forgetRow(container, SINGLE_PARTITION_TOPIC), /^Forget offsets$/);
    await settle();

    const asking = forgetConfirmation().textContent ?? "";
    expect(asking).toContain(`1 partition of ${SINGLE_PARTITION_TOPIC}`);
    expect(asking).not.toContain("1 partitions");

    dispose();
  });

  it("an answer naming no partitions is reported as nothing forgotten", async () => {
    /*
     * `DeletedOffsetsDto.partitions` is **optional** in `schema.d.ts`, so a 200 that carries no
     * partitions field at all is a wire state the server can really produce — distinct from the
     * empty array the case above covers, and the one the `?? []` in `write.ts` exists for. With a
     * default of `[0]` the browser reports "1 partition" for a server that named none: a green
     * success toast over an action that removed nothing, which is the exact failure the two
     * sentences were written to prevent.
     */
    const { api } = stub({ forgetAnswer: { topic: FIRST_TOPIC } });
    const { container, dispose } = openGroup(api);
    await settle();

    press(forgetRow(container, FIRST_TOPIC), /^Forget offsets$/);
    await settle();
    press(forgetConfirmation(), /^Forget offsets$/);
    await settle();

    expect(titles()).toContain("Nothing was forgotten");
    expect(titles()).not.toContain("Committed offsets forgotten");
    const raised = toasts().find((toast) => toast.title === "Nothing was forgotten");
    expect(raised?.tone).toBe("warning");
    expect(raised?.message).toContain("held no committed offset");
    // Not a count invented from a missing field: neither "1 partition" nor a zero dressed as one.
    expect(raised?.message).not.toContain("1 partition");
    expect(raised?.message).not.toContain("0 partitions");

    dispose();
  });

  it("a second press while the request is in flight sends one request", async () => {
    /*
     * The double-submit guard, which is `busy={forget.busy()}` on the confirmation: `canConfirm()`
     * is `confirmationSatisfied() && props.busy !== true`, so while the DELETE is out the confirm
     * button is the disabled spelling and swallows the press.
     *
     * Both halves are asserted, and they are not the same half. That the control **says** it is
     * running is what `busy` decides on its own — replace it with `false` and the operator is
     * looking at a live, undisabled destructive button over a request they cannot see. That only
     * one request goes out is defended twice: here, and by `createMutation`'s own re-entry guard
     * one layer down. Asserting it anyway is the point — the rule is "one press, one DELETE", and a
     * case that only watched the button would go green the day the two guards were reorganised.
     */
    let release = (): void => {};
    const pending = new Promise<void>((resolve) => {
      release = () => resolve();
    });
    const { api, forgotten } = stub({ forgetPending: pending });
    const { container, dispose } = openGroup(api);
    await settle();

    press(forgetRow(container, FIRST_TOPIC), /^Forget offsets$/);
    await settle();
    press(forgetConfirmation(), /^Forget offsets$/);
    await settle();

    // In flight: the DELETE has been sent and is being held by the stub.
    expect(forgotten).toEqual([FIRST_TOPIC]);
    const confirm = [...forgetConfirmation().querySelectorAll<HTMLButtonElement>("button")].find(
      (candidate) => (candidate.textContent ?? "").trim() === "Forget offsets",
    );
    expect(confirm?.getAttribute("aria-disabled")).toBe("true");
    expect(confirm?.getAttribute("aria-busy")).toBe("true");

    // The impatient second press, which a browser dispatches whatever `aria-disabled` says.
    confirm?.click();
    await settle();
    expect(forgotten).toEqual([FIRST_TOPIC]);

    release();
    await settle(24);

    // And the one request that did go out was reported once, as a success.
    expect(forgotten).toEqual([FIRST_TOPIC]);
    expect(titles()).toEqual(["Committed offsets forgotten"]);

    dispose();
  });

  it("raises no toast, and goes nowhere, when the cluster refuses the delete", async () => {
    const { api } = stub({ deleteFails: "the group still has members" });
    const { container, dispose, url } = openGroup(api);
    await settle();

    press(container, /^Delete group$/);
    await settle();
    press(confirmation(), /^Delete group$/);
    await settle();

    expect(titles()).toEqual([]);
    // The confirmation stays open carrying the server's own words, which name what to do next.
    expect(confirmation().textContent).toContain("the group still has members");
    expect(url()).toBe(`${BASE}/clusters/${CLUSTER}/consumer-groups/${GROUP}`);

    dispose();
  });
});

/**
 * The two sentences a confirmation is composed from, read directly.
 *
 * Both are exported from `GroupRoute` and both are called by it — `consequence={…}` on each of the
 * page's two `ConfirmDialog`s. They are asserted here rather than only through the screen because
 * one of `consequenceOfForget`'s two branches cannot be reached from the screen at all: the forget
 * list is built from the offsets the group holds, so a topic it holds none on has no row to press.
 * That branch exists for the race its own docstring names — the page was fetched before somebody
 * else's reset landed — and deleting it is green through every case that goes via the DOM.
 */
describe("what a destructive confirmation promises", () => {
  it("says KUI last read the group as holding nothing there, rather than printing a zero", () => {
    /*
     * The never-zero rule, in the one paragraph this product asks an operator to read before an
     * irreversible action. With the `held === 0` branch gone the sentence reads "Removes this
     * group's committed offsets on 0 partitions of orders.events" — a confirmation that describes
     * an action on nothing as though it were an action, over a button that will then report
     * "Nothing was forgotten". The two halves of that exchange have to agree before the click as
     * well as after it, and only the receipt half had a case.
     */
    const nothingHeld = consequenceOfForget(SAMPLE_GROUP_DETAIL, "orders.events");
    expect(nothingHeld).toContain(
      "KUI last read this group as holding no committed offset on orders.events",
    );
    expect(nothingHeld).not.toContain("0 partitions");
    expect(nothingHeld).not.toContain("Removes this group's committed offsets");

    // And the branch beside it still names the figure, so this is a gate on the *choice* rather
    // than an assertion that the function says something.
    const held = consequenceOfForget(SAMPLE_GROUP_DETAIL, "clickstream");
    expect(held).toContain("Removes this group's committed offsets on 4 partitions of clickstream");
    // The singular, on a topic holding exactly one — `sessions` in the fixture.
    expect(consequenceOfForget(SAMPLE_GROUP_DETAIL, "sessions")).toContain(
      "1 partition of sessions",
    );

    // Both branches carry the part an operator forgets and the part that decides whether this is
    // safe: nothing is deleted, and a consumer that comes back follows its own auto.offset.reset.
    for (const sentence of [nothingHeld, held]) {
      expect(sentence).toContain("No records are deleted");
      expect(sentence).toContain("auto.offset.reset");
    }
  });

  it("counts the partitions the group delete costs, in the group's own figures", () => {
    /* The fixture holds five committed positions across two topics; the delete is about all of
       them, which is why this counts `offsets` and not `subscriptions`. */
    expect(consequenceOfDelete(SAMPLE_GROUP_DETAIL)).toContain(
      "Removes the group and its committed offsets on 5 partitions",
    );
    expect(
      consequenceOfDelete({
        ...SAMPLE_GROUP_DETAIL,
        offsets: SAMPLE_GROUP_DETAIL.offsets.slice(0, 1),
      }),
    ).toContain("on 1 partition.");
    expect(consequenceOfDelete(SAMPLE_GROUP_DETAIL)).toContain("No records are deleted");
  });
});
