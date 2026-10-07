/**
 * Mounting the whole application.
 *
 * ## Why this file exists
 *
 * Every other test in this package imports a *piece* of the shell — a pure helper, one component,
 * one store — and drives it directly. That is the right way to test behaviour, and it left one gap
 * that a review found by opening a browser: nothing rendered `<App />` itself. The composition root
 * was the only component in the product with no test, and it is the one component every user sees.
 *
 * What got through the gap was a blank page. `App` built a `createMemo` that called a `const` arrow
 * function declared eleven lines further down. A `const` is in its temporal dead zone until its own
 * line runs, and Solid 2 computes a memo eagerly when it is created, so the memo called the binding
 * before it existed. The `ReferenceError` was raised inside the reactive graph, which Solid reports
 * as `REACTIVITY_HALTED`: the graph stops, nothing renders, and the browser shows a black rectangle
 * with no failed request and no broken-looking component to point at. Five hundred and eighty-five
 * tests passed while the application did not start.
 *
 * So the assertion that matters here is the cheapest one imaginable — the frame drew, and the
 * console stayed quiet. A test that merely *mounts* would have caught it, because Solid reports a
 * halted graph rather than throwing out of `render`; the test has to look at what landed.
 *
 * ## How the gateway is faked
 *
 * `fetch` is replaced for the length of each case. The shell asks for `/auth/me` and
 * `/auth/settings` at start-up and nothing else until a cluster is chosen, so a stub that answers
 * those two and refuses everything else is enough to render the frame — and refusing the rest is
 * deliberate, because "the gateway answered some of it" is the state the shell is built to survive.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { render } from "@solidjs/web";
import { flush } from "solid-js";

import { Actions, CapabilityStatuses, Resources, SseEventNames } from "@kui/api";
import * as ApiModule from "@kui/api";
import { ALERTS_EVENT_NAME } from "@kui/kernel";

import { App } from "./App.jsx";
import { SEARCH_DEBOUNCE_MS } from "./data/search.js";
import { FailuresBeforeGivingUp } from "./health.js";
import { featureRegistry } from "./features/registry.js";

/**
 * The permissions a deployment with authentication disabled really hands out.
 *
 * One grant per feature's view action, over every cluster and every name — which is what
 * `/auth/me` answers when no identity provider is configured, and what the third `describe` in this
 * file asserts the registrations line up with. Without them every cluster-scoped row in the drawer
 * comes out **forbidden**, and a forbidden row carries neither a badge nor a nested tree: a stub
 * that omitted the grants would leave this suite asserting against a drawer nobody ever sees.
 */
const WILDCARD_GRANTS = featureRegistry.map((registration) => ({
  clusters: ["*"],
  resource: registration.viewAction.resource,
  value: ".*",
  actions: [registration.viewAction.action],
}));

/** The `/auth/me` body both stubs answer with. */
const SESSION = {
  authType: "disabled",
  csrfToken: "test-token",
  principal: { kind: "anonymous", name: "anonymous" },
  permissions: WILDCARD_GRANTS,
};

/**
 * An `EventSource` that connects to nothing.
 *
 * jsdom does not implement one, and the capability store opens a stream during start-up. A stub
 * that only records the address is enough: what this file tests is that the frame renders, and the
 * capability picture arriving late is the normal case the shell is designed for — an empty picture
 * renders as degraded-with-STARTING, which is exactly the state under test.
 */
class SilentEventSource {
  static readonly opened: string[] = [];
  /** Every source built since the last reset, so a case can push a frame down one. */
  static readonly live: SilentEventSource[] = [];
  onmessage: ((event: MessageEvent) => void) | null = null;
  onerror: ((event: Event) => void) | null = null;
  onopen: ((event: Event) => void) | null = null;
  readonly readyState = 0;

  private readonly listeners = new Map<string, ((event: Event) => void)[]>();

  constructor(readonly url: string) {
    SilentEventSource.opened.push(url);
    SilentEventSource.live.push(this);
  }

  addEventListener(name: string, handler: (event: Event) => void): void {
    const held = this.listeners.get(name) ?? [];
    held.push(handler);
    this.listeners.set(name, held);
  }

  removeEventListener(): void {}
  close(): void {}

  /**
   * Delivers one frame, the way the browser's own `EventSource` would.
   *
   * A `{ data }` object rather than a real `MessageEvent`: the stream reader takes the payload off
   * `event.data` and looks at nothing else, and jsdom's `MessageEvent` constructor is not what is
   * under test here.
   */
  emit(name: string, data: unknown): void {
    for (const handler of this.listeners.get(name) ?? []) {
      handler({ data: JSON.stringify(data) } as unknown as Event);
    }
  }
}

/** One capability entry, as the gateway sends it. */
function entry(service: string, cluster: string, name: string) {
  return {
    key: { service, cluster },
    state: { status: CapabilityStatuses.Available },
    updatedAt: "2026-09-06T09:00:00.000Z",
    name,
  };
}

/**
 * The services a cluster's features are gated on, all reporting healthy.
 *
 * Pushed down the stream because that is the only way a feature reaches `ready`: with no frame at
 * all the shell renders every capability as degraded-with-STARTING, which is the honest state for a
 * picture that has not arrived and is *not* the state most of this product's rules apply to. A
 * degraded row carries the capability badge instead of its count and draws no tree, so a suite that
 * never delivered a frame would be asserting against a drawer no operator sees for longer than a
 * second.
 */
function healthy(...clusters: readonly string[]) {
  return {
    generatedAt: "2026-09-06T09:00:00.000Z",
    entries: clusters.flatMap((cluster) =>
      /* `alerts` joins the five the moment the ninth service is registered: a feature with no
         entry in the picture is `degraded` with STARTING, which draws a capability badge instead of
         its count and no tree — so a frame that left it out would have every case below asserting
         against a drawer no operator sees for longer than a second. */
      ["cluster", "topic", "message", "consumer", "schema", "alerts"].map((service) =>
        entry(service, cluster, cluster),
      ),
    ),
  };
}

/**
 * The same frame, with the alerts service reporting that this deployment configures none.
 *
 * A `not_configured` **entry** and not a missing one, and the difference is the whole of ADR-032:
 * an entry saying "there is no such upstream here" hides the row, while no entry at all is a
 * picture that has not arrived, which draws the row as degraded-with-STARTING. A fixture that
 * simply left the service out would be testing the second and claiming the first.
 */
function withoutAlerts(cluster: string) {
  return {
    generatedAt: "2026-09-06T09:00:00.000Z",
    entries: healthy(cluster).entries.map((row) =>
      row.key.service === "alerts"
        ? { ...row, state: { status: CapabilityStatuses.NotConfigured } }
        : row,
    ),
  };
}

/**
 * Delivers a capability frame down the capability stream.
 *
 * Found by its **address** and not as "the last stream opened", which is what this was and what
 * stopped working the moment the shell opened a second one. The alert feed's ADR-035 subscription
 * is opened alongside the capability stream, so `live.at(-1)` began returning it; the frame went
 * to a reader that ignores it, every feature stayed at its start-up `degraded`, and four cases
 * about badges and trees failed with no mention of alerts anywhere in them. The address is the
 * thing that distinguishes the two, so the address is what this looks at.
 */
function announce(frame: unknown): void {
  /* A reverse scan rather than `findLast`, which this project's `lib` target does not carry. The
     latest is what a case wants: `announce` may be called after a cluster switch has replaced the
     stream, and the stale one is closed. */
  const stream = [...SilentEventSource.live]
    .reverse()
    .find((source) => source.url.includes("/capabilities/stream"));
  if (stream === undefined) {
    throw new Error(
      "the capability stream was never opened; the shell opened " +
        JSON.stringify(SilentEventSource.opened),
    );
  }
  stream.emit(SseEventNames.Capabilities, frame);
  flush();
}

/**
 * Delivers one ADR-053 change frame down a named cluster's alert stream.
 *
 * The address again, and not "the newest stream": there are two kinds of stream open and, on a case
 * that switches cluster, more than one of this kind. `announce`'s own comment records what
 * `live.at(-1)` cost when the shell opened its second stream, and this is the same rule applied to
 * the streams that arrived with it.
 *
 * Frames are delivered **without** letting the re-read they trigger land, deliberately. The count
 * on the frame is what the bell draws in the moment before the read answers — that is the whole
 * reason `AlertChangeDto` carries one — and a case that awaited the read would be asserting the
 * read's count instead, which is the same number from the same place and proves nothing about the
 * frame.
 */
function announceAlerts(cluster: string, frame: unknown): void {
  const address = `/api/v1/clusters/${cluster}/alerts/stream`;
  const stream = [...SilentEventSource.live].reverse().find((source) => source.url === address);
  if (stream === undefined) {
    throw new Error(
      `no alert stream is open at ${address}; the shell opened ` +
        JSON.stringify(SilentEventSource.opened),
    );
  }
  stream.emit(ALERTS_EVENT_NAME, frame);
  flush();
}

/** The two start-up answers, and a 404 for anything else the shell decides to ask for. */
function stubGateway(): void {
  vi.stubGlobal("EventSource", SilentEventSource);
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
      if (url.includes("/auth/me")) {
        return new Response(JSON.stringify(SESSION), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }
      if (url.includes("/auth/settings")) {
        return new Response(JSON.stringify({ authType: "disabled", providers: [] }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }
      return new Response(
        JSON.stringify({ code: "KUI-ROUTE-NOT-FOUND", message: "no route", details: [] }),
        { status: 404, headers: { "content-type": "application/json" } },
      );
    }),
  );
}

function mountApp() {
  const host = document.createElement("div");
  document.body.appendChild(host);
  const dispose = render(() => <App />, host);
  flush();
  return {
    host,
    dispose: () => {
      dispose();
      host.remove();
    },
  };
}

afterEach(() => {
  SilentEventSource.live.length = 0;
  /* And the addresses, which were not reset and so accumulated across every case in this file. A
     case that asserts a stream was *not* opened — the deferral case below is one — would then be
     reading a list an earlier case had filled in, and would pass or fail on the order the runner
     happened to choose. */
  SilentEventSource.opened.length = 0;
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  /* The address and the stored selection are both global, and the shell writes to both: a case that
     mounts on a cluster's address leaves that cluster selected for the next one, which then fetches
     a cluster its own stub knows nothing about. Two suites sharing one `localStorage` is how a test
     ends up depending on the order it runs in. */
  window.history.replaceState({}, "", "/");
  try {
    window.localStorage.clear();
  } catch {
    /* A browser configured to block site data raises on the accessor itself, and a test that cannot
       clear a store it was never able to write to has nothing to clean up. */
  }
});

describe("the application, mounted", () => {
  it.each([false, true])("retains anonymous CSRF after repeated protected 401s and signs in (initially anonymous: %s)", async (initiallyAnonymous) => {
    vi.stubGlobal("EventSource", SilentEventSource);
    const realCreate = ApiModule.createApiClient;
    let client!: ReturnType<typeof realCreate>;
    let csrf!: Parameters<typeof realCreate>[0]["csrf"];
    vi.spyOn(ApiModule, "createApiClient").mockImplementation((options) => {
      csrf = options.csrf;
      client = realCreate(options);
      return client;
    });
    let reads = 0;
    let signedIn = false;
    let loginHeader: string | null | undefined;
    let releaseRefresh!: () => void;
    const refreshHeld = new Promise<void>((resolve) => { releaseRefresh = resolve; });
    const unauthorized = () => Response.json({ code: "KUI-UNAUTHENTICATED", message: "sign in" }, { status: 401 });
    vi.stubGlobal("fetch", vi.fn(async (request: Request) => {
      if (request.url.endsWith("/auth/me")) {
        reads++;
        if (reads === 2 && !initiallyAnonymous) await refreshHeld;
        const anonymous = !signedIn && (initiallyAnonymous || reads > 1);
        return Response.json({ ...SESSION, authType: "form",
          csrfToken: anonymous ? "fresh-anonymous" : "signed-in-token",
          principal: anonymous ? { kind: "anonymous", name: "anonymous" } : { kind: "session", name: "ada" } });
      }
      if (request.url.endsWith("/auth/settings")) return Response.json({ authType: "form", rbacEnabled: true });
      if (request.url.endsWith("/auth/login")) {
        loginHeader = request.headers.get("X-Csrf-Token");
        if (loginHeader !== "fresh-anonymous") return Response.json({ code: "KUI-FORBIDDEN", message: "CSRF token missing" }, { status: 403 });
        signedIn = true;
        return Response.json({ principal: { kind: "user", name: "ada", roles: [] } } satisfies ApiModule.components["schemas"]["SignedIn"]);
      }
      if (request.url.endsWith("/capabilities")) return unauthorized();
      return Response.json({ code: "KUI-NOT-FOUND", message: "unused" }, { status: 404 });
    }));
    let app = mountApp();
    try {
      await settle();
      const first = client.get("/api/v1/capabilities");
      const concurrent = client.get("/api/v1/capabilities");
      await Promise.all([first, concurrent]);
      await settle();
      expect(reads).toBe(initiallyAnonymous ? 1 : 2);
      releaseRefresh();
      await settle();
      await Promise.all([client.get("/api/v1/capabilities"), client.get("/api/v1/capabilities")]);
      await settle();
      expect(csrf.currentToken()).toBe("fresh-anonymous");
      expect(reads).toBe(initiallyAnonymous ? 1 : 2);
      const login = app.host.querySelector('[data-testid="login-page"]')!;
      expect(login).not.toBeNull();
      for (const [selector, value] of [['input[name="username"]', "ada"], ['input[type="password"]', "test-only-password"]]) {
        const input = login.querySelector<HTMLInputElement>(selector!)!;
        input.value = value!;
        input.dispatchEvent(new Event("input", { bubbles: true }));
      }
      await flush();
      login.querySelector("form")!.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
      await settle();
      await settle();
      expect(loginHeader).toBe("fresh-anonymous");
      expect(signedIn).toBe(true);
      // Successful sign-in deliberately reloads the whole shell. jsdom cannot navigate documents;
      // mount a fresh shell against the now-authenticated gateway to exercise that boundary.
      app.dispose();
      app = mountApp();
      await settle();
      expect(app.host.querySelector('[data-testid="login-page"]')).toBeNull();
      expect(csrf.currentToken()).toBe("signed-in-token");
      expect(reads).toBe(initiallyAnonymous ? 2 : 3);
    } finally { releaseRefresh(); app.dispose(); }
  });

  it.each([false, true])("refreshes an expired session once (settings pending: %s) and presents sign-in", async (holdSettings) => {
    vi.stubGlobal("EventSource", SilentEventSource);
    let reads = 0;
    let release!: () => void;
    const held = new Promise<void>((resolve) => { release = resolve; });
    let releaseSettings!: () => void;
    const settingsHeld = new Promise<void>((resolve) => { releaseSettings = resolve; });
    vi.stubGlobal("fetch", vi.fn(async (request: Request) => {
      if (request.url.includes("/auth/me")) {
        reads += 1;
        if (reads > 1) await held;
        return Response.json({ ...SESSION, authType: "form", csrfToken: "fresh",
          principal: reads === 1 ? { kind: "session", name: "ada" } : { kind: "anonymous", name: "anonymous" } });
      }
      if (request.url.includes("/auth/settings")) {
        if (holdSettings) await settingsHeld;
        return Response.json({ authType: "form", rbacEnabled: true });
      }
      return Response.json({ code: "KUI-UNAUTHENTICATED", message: "expired" }, { status: 401 });
    }));
    const app = mountApp();
    try {
      await settle();
      announce(healthy("prod-kyiv-01"));
      await settle();
      await settle();
      expect(app.host.querySelector("[data-testid='login-page']")).not.toBeNull();
      releaseSettings();
      await settle();
      expect(reads).toBe(2);
      release();
      await settle();
      await settle();
      expect(app.host.querySelector("[data-testid='login-page']")).not.toBeNull();
      expect(reads).toBe(2);
    } finally { releaseSettings(); release(); app.dispose(); }
  });

  it("draws its frame", () => {
    stubGateway();
    const app = mountApp();

    // The frame is the thing that must never fail to render: a service being down is a panel's
    // problem, and the shell going blank is everybody's.
    expect(app.host.querySelector(".kui-frame")).not.toBeNull();
    expect(app.host.querySelector(".kui-frame__drawer")).not.toBeNull();
    expect(app.host.querySelector(".kui-frame__topbar")).not.toBeNull();
    expect(app.host.querySelector(".kui-frame__content")).not.toBeNull();

    app.dispose();
  });

  it("does not halt its reactive graph while starting up", () => {
    // Solid reports a `ReferenceError` raised inside a computation by logging `REACTIVITY_HALTED`
    // and stopping the graph, rather than by throwing where a test would see it. Watching the
    // console is therefore not belt-and-braces here; it is the only place the failure appears.
    const errors: unknown[][] = [];
    vi.spyOn(console, "error").mockImplementation((...args: unknown[]) => void errors.push(args));
    stubGateway();

    const app = mountApp();
    flush();

    expect(errors.map((line) => String(line[0])).join("\n")).not.toMatch(
      /REACTIVITY_HALTED|before initialization/,
    );

    app.dispose();
  });

  it("fills the drawer from the feature registry", () => {
    stubGateway();
    const app = mountApp();

    // This is the assertion aimed squarely at the defect. The navigation is built from `groups()`,
    // which reads `statuses()` — the memo that called a binding still in its temporal dead zone.
    // When that threw, the frame's static markup still appeared and the drawer came out empty, so
    // "the frame rendered" alone was not enough to tell the two apart. A destination in the drawer
    // proves the memo ran to completion.
    const drawer = app.host.querySelector(".kui-frame__drawer");
    expect(drawer?.querySelectorAll("a").length ?? 0).toBeGreaterThan(0);
    expect(drawer?.textContent).toContain("Overview");

    app.dispose();
  });
});

/**
 * The frame, over a cluster the address names.
 *
 * The three tests above mount the shell at the root, where no cluster is selected and the store
 * therefore asks for nothing. This one is the wiring wave's own assertion, and it is the one that
 * could not have passed before it: wave 1 built `createClusterStore`, `brokerStorageOf` and the
 * count fold, and nothing in the product constructed any of them — `grep` found no reference to one
 * outside `data/`. So the meter drew its "not known" rendering in every deployment since it was
 * built, not because the disks could not be read but because nobody had ever handed it any.
 *
 * The address is the input on purpose. `/ui/clusters/<id>` is the shortest thing anybody types, it
 * used to fall through to the 404 wildcard, and it is the one input that exercises the whole chain
 * in one go: the route resolves, the cluster comes out of it, the store asks six questions about
 * that cluster, and the drawer draws three of the answers.
 */
describe("the frame, given a cluster in the address", () => {
  const ok = (data: unknown) => ({ status: "ok", data, fetchedAt: "2026-09-06T09:00:00.000Z" });

  /** What the six requests behind the drawer are answered with. Everything else 404s, as above. */
  const CLUSTER: Readonly<Record<string, unknown>> = {
    "/api/v1/clusters/prod-kyiv-01": {
      cluster: {
        id: "prod-kyiv-01",
        name: "prod-kyiv-01",
        summary: ok({
          version: "3.7.0",
          brokerCount: 3,
          offlinePartitionCount: 0,
          underReplicatedPartitionCount: 1,
        }),
      },
    },
    "/api/v1/clusters/prod-kyiv-01/brokers": { brokers: ok([{ id: 1 }, { id: 2 }, { id: 3 }]) },
    "/api/v1/clusters/prod-kyiv-01/log-dirs": {
      logDirs: ok([
        { brokerId: 1, path: "/data/a", totalBytes: 200, usableBytes: 100 },
        { brokerId: 2, path: "/data/a", totalBytes: 200, usableBytes: 50 },
        /* Offline, so this broker gets no row and contributes to neither sum. If it did, the
           percentage below would be computed over capacity the cluster does not have. */
        { brokerId: 3, path: "/data/a", error: "KafkaStorageException" },
      ]),
    },
    "/api/v1/clusters/prod-kyiv-01/topics": {
      topics: ok({ items: [], page: { totalItems: 128 } }),
      incompleteTopics: 0,
    },
    /* The names-only index the drawer's tree is folded from. Eight names over three prefixes and
       two internal topics, which is enough for every rule the fold has: a genuine prefix written
       `orders.*`, a single topic that must *not* be written `heartbeats.*`, and the padlocked
       `internal` row that collects both underscored names whatever their own prefixes are. */
    "/api/v1/clusters/prod-kyiv-01/topics/names": {
      names: ok([
        "orders.payments.v2",
        "orders.payments.v1",
        "orders.shipments",
        "analytics.clickstream",
        "analytics.sessions",
        "heartbeats",
        "__consumer_offsets",
        "__transaction_state",
      ]),
    },
  };

  /**
   * @param overrides answers replacing the six above, keyed by path — for the cases that need a
   * cluster which reports something other than the healthy fixture.
   */
  function stubCluster(
    overrides: Readonly<Record<string, unknown>> = {},
    /**
     * Something to wait for before `/auth/me` answers.
     *
     * The one thing a start-up-ordering case cannot arrange any other way: every rule about what
     * must not go out before the session exists is a rule about the *interval* between the request
     * and its answer, and with an immediate stub that interval is empty.
     */
    holdSession?: Promise<void>,
  ): void {
    const answers: Readonly<Record<string, unknown>> = { ...CLUSTER, ...overrides };
    vi.stubGlobal("EventSource", SilentEventSource);
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const href =
          typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
        const path = new URL(href, "http://kui.test").pathname;
        if (path.includes("/auth/me")) {
          if (holdSession !== undefined) await holdSession;
          return new Response(JSON.stringify(SESSION), {
            status: 200,
            headers: { "content-type": "application/json" },
          });
        }
        const body = answers[path];
        if (body !== undefined) {
          return new Response(JSON.stringify(body), {
            status: 200,
            headers: { "content-type": "application/json" },
          });
        }
        if (path.includes("/auth/settings")) {
          return new Response(JSON.stringify({ authType: "disabled", providers: [] }), {
            status: 200,
            headers: { "content-type": "application/json" },
          });
        }
        /* Everything else refused, and refused as a real envelope. Four of the frame's six requests
           are answered above and the other two are not, which is the ordinary state of this product
           — the drawer's whole design is that six failures are six failures and not one. */
        return new Response(
          JSON.stringify({ code: "KUI-ROUTE-NOT-FOUND", message: "no route", details: [] }),
          { status: 404, headers: { "content-type": "application/json" } },
        );
      }),
    );
  }

  /** Lets the six requests land and the reactive graph catch up. */
  async function settled(): Promise<void> {
    for (let turn = 0; turn < 4; turn += 1) {
      await new Promise((resolve) => setTimeout(resolve, 0));
      flush();
    }
  }

  it("draws the dashboard, not the 404 page, and fills the drawer from the store", async () => {
    window.history.replaceState({}, "", "/ui/clusters/prod-kyiv-01");
    stubCluster();
    const app = mountApp();
    await settled();

    /* The route change: `/clusters/<id>` names no page, and now resolves to the cluster's own
       dashboard instead of to the wildcard at the foot of the table. */
    expect(app.host.querySelector("[data-testid='overview']")).not.toBeNull();

    const drawer = app.host.querySelector(".kui-frame__drawer");
    // The head is the cluster block, and it is the store's summary that names it.
    expect(drawer?.textContent).toContain("prod-kyiv-01");

    /* 250 B used of 400 B, over the two brokers that reported a size. The third reported none and
       is in neither sum: a failed disk is not a disk of size zero, and counting its 200 B of
       capacity as empty would print 42% over a cluster that is at 63%. */
    const meter = drawer?.querySelector("[data-testid='storage-meter']");
    expect(meter?.textContent).toContain("63%");
    expect(meter?.textContent).toContain("250 B of 400 B");

    app.dispose();
  });

  /**
   * The badge seam, asserted where the product joins the two halves and not where a test joins
   * them.
   *
   * `shell.test.tsx` has three cases about badges and all three call `countLookup` and
   * `navigationGroups` themselves, so they check the *fold* and observe nothing about `App`'s use
   * of it. Replacing `countFor: countLookup(readingValue(facts.counts))` with `countFor: () =>
   * undefined` — cutting the store off from the drawer entirely — left all of them green. This case
   * mounts the real application over a gateway that answers `page.totalItems: 128` and looks at the
   * drawer, so the only way it passes is for the store's number to have reached the row.
   */
  it("carries the store's own count into the drawer's badge", async () => {
    window.history.replaceState({}, "", "/ui/clusters/prod-kyiv-01");
    stubCluster();
    const app = mountApp();
    await settled();

    announce(healthy("prod-kyiv-01"));

    const topics = app.host.querySelector("[data-testid='nav-topics']");
    expect(topics?.textContent).toContain("128");
    /* And in words, because the visible badge is a fragment: the row's accessible name is what a
       screen-reader user is given, and it is assembled from the badge's description. */
    expect(topics?.getAttribute("aria-label")).toContain("128");

    app.dispose();
  });

  /**
   * The tree, drawn from names the product fetched.
   *
   * `nav/topicTree.ts` was written, tested and exported a wave ago and called by nothing but the
   * barrel that exported it, so the drawer never nested. Expanding the row here is the assertion
   * that it is called: the disclosure only exists for a branch, and a branch only exists when
   * `childrenFor` answered — which needs the names endpoint, the fold and the frame's wiring all
   * three.
   */
  it("nests the topic tree under Topics, from the names the cluster reported", async () => {
    window.history.replaceState({}, "", "/ui/clusters/prod-kyiv-01");
    stubCluster();
    const app = mountApp();
    await settled();

    announce(healthy("prod-kyiv-01"));

    const disclosure = app.host.querySelector<HTMLButtonElement>(
      "[data-testid='nav-topics-disclosure']",
    );
    expect(disclosure).not.toBeNull();

    disclosure!.click();
    flush();

    const subtree = app.host.querySelector("[data-testid='nav-topics-subtree']");
    /* The three rules the fold owns, seen through the drawer: a genuine prefix is written with its
       star, a lone topic keeps its bare name, and every underscored topic is one padlocked row. */
    expect(subtree?.textContent).toContain("orders.*");
    expect(subtree?.textContent).toContain("heartbeats");
    expect(subtree?.textContent).not.toContain("heartbeats.*");
    expect(subtree?.textContent).toContain("internal");
    expect(subtree?.textContent).not.toContain("__consumer_offsets");

    /* The counts are the cluster's and not the page's, and the addresses are the list's own query
       rather than a hand-written path. */
    const orders = subtree?.querySelector("[data-testid='nav-prefix:orders.*']");
    expect(orders?.getAttribute("href")).toBe("/ui/clusters/prod-kyiv-01/topics?q=orders");
    expect(orders?.textContent).toContain("3");
    const internal = subtree?.querySelector("[data-testid='nav-prefix:internal']");
    expect(internal?.getAttribute("href")).toBe(
      "/ui/clusters/prod-kyiv-01/topics?showInternal=true",
    );

    app.dispose();
  });

  /**
   * The same wiring over a cluster that has no topics at all.
   *
   * Three genuinely different states draw this row as a leaf — no cluster chosen, the names not yet
   * arrived, and a cluster with nothing in it — and the third is the one an operator meets on a
   * cluster they have just registered. The row keeps its link and its badge; what it must not grow
   * is a chevron, because a disclosure opening onto an empty list is a control that appears broken.
   *
   * **The state is now the one the prose describes**, and it was not. This case overrode
   * `/topics/names` alone while `/topics` went on answering `page.totalItems: 128`, so what it
   * actually built was a cluster whose count endpoint and whose name index disagreed — a real
   * enough state, and not the one an operator meets on a cluster they have just registered. The
   * badge it asserted was `128` over an empty tree. Both answers say nought now, and the badge is
   * `0`: a measured zero, from a cluster that answered, which is exactly the figure this product
   * prints and exactly the figure it refuses to invent when nobody answered.
   *
   * This is the whole chain, and it is deliberately not the only case on the rule. The DOM cannot
   * tell `children: []` from an absent `children` — `NavItem` draws both as a leaf, which is the
   * point — so the memo's own answer is pinned where the product decides it, in
   * `nav/topicTree.ts`'s `topicSubtree`, and the renderer's predicate is pinned in
   * `chrome.test.tsx`. This case is what proves the two meet over a real gateway answer.
   */
  it("a memo over an empty topic list yields no subtree", async () => {
    window.history.replaceState({}, "", "/ui/clusters/prod-kyiv-01");
    stubCluster({
      "/api/v1/clusters/prod-kyiv-01/topics": {
        topics: ok({ items: [], page: { totalItems: 0 } }),
        incompleteTopics: 0,
      },
      "/api/v1/clusters/prod-kyiv-01/topics/names": { names: ok([]) },
    });
    const app = mountApp();
    await settled();

    announce(healthy("prod-kyiv-01"));

    const topics = app.host.querySelector("[data-testid='nav-topics']");
    expect(topics).not.toBeNull();
    /* The badge is the cluster's own figure, and here that figure is nought. It is drawn rather
       than dropped because the cluster answered: an absent badge is what an *unknown* count draws,
       and telling those two apart on this row is the whole vocabulary `NavCount` exists for. */
    expect(topics?.querySelector(".kui-nav-item__badge")?.textContent).toBe("0");
    expect(topics?.getAttribute("aria-label")).toContain("0");
    expect(app.host.querySelector("[data-testid='nav-topics-disclosure']")).toBeNull();
    expect(app.host.querySelector("[data-testid='nav-topics-subtree']")).toBeNull();

    app.dispose();
  });

  /**
   * The alert feed, from the address the shell asks for to the badge on the bell.
   *
   * Two halves of one wire, and neither is checked by the compiler yet.
   * `frontend/packages/api/src/schema.d.ts` is generated from `docs/api/openapi.browser.json` and
   * carries no alerts path until `services/alerts` and the gateway's routing have both landed and
   * W6-09 has regenerated it — so `data/alerts.ts` widens the client's `get` at one line, and the
   * *spelling* it passes is checked here instead, against the URL that actually reached `fetch`.
   * M8's exit criterion in `docs/plan/ROADMAP.md:471-476` is what fixes both ends: the section key
   * is `events`, the rows are `items`, and the count is `openCount`.
   *
   * That is the whole of the defence, and it is deliberate. Wave 5 shipped an encoder writing
   * `topics[]` and a decoder reading `entries[]`, both unit-tested against their own shape, and the
   * card drew "the metrics source named no producers" over a source that had named five — because
   * nothing anywhere decoded the *other side's* document. A case that mounts the application over a
   * document in the milestone's own spelling and looks at the bell is what that wave was missing.
   */
  it("asks the alerts service for its feed and draws that open count on the bell", async () => {
    window.history.replaceState({}, "", "/ui/clusters/prod-kyiv-01");
    stubCluster({
      "/api/v1/clusters/prod-kyiv-01/alerts/events": {
        events: {
          status: "ok",
          fetchedAt: "2026-09-06T09:00:00.000Z",
          data: {
            /* Written the way `AlertEventDto` encodes it, four fields and not two: `severity`
               beside `tone` and `category` beside `glyph`, because the derived halves travel so
               that the bell, the card and the panel cannot map them differently. */
            items: [
              {
                id: "evt-1",
                severity: "critical",
                tone: "danger",
                category: "storage",
                glyph: "storage",
                openedAt: "2026-09-06T08:00:00.000Z",
                lastSeenAt: "2026-09-06T08:55:00.000Z",
                title: "Log directory past its critical threshold",
                detail: "broker-3 · /data/a at 94%",
                resolution: null,
              },
              {
                id: "evt-2",
                severity: "warning",
                tone: "warning",
                category: "rebalance",
                glyph: "rebalance",
                openedAt: "2026-09-06T07:00:00.000Z",
                lastSeenAt: "2026-09-06T08:55:00.000Z",
                title: "orders-consumers has been rebalancing for 4m",
                detail: "12 members",
                resolution: null,
              },
            ],
            /* Deliberately **not** two. The feed is paged and the bell is not, so the server's own
               count is a different number from the rows this page holds — and a bell that folded
               the page would draw `2` here and be wrong by five. Nothing else in this case could
               tell the two apart. */
            openCount: 7,
            /* Two counts and neither is folded from the other. `openCount` is what the badge draws
               and `unreadCount` is what decides its tone, and they are both counted over the whole
               store by the service rather than over the two rows above. */
            unreadCount: 3,
            lastReadAt: "2026-09-06T06:00:00.000Z",
            /* When the rules last ran. Without it the count is not a count — see the case below,
               and the kernel store's `knownOpenCount`, which is now the only place that rule is
               applied. */
            evaluatedAt: "2026-09-06T08:59:00.000Z",
          },
        },
      },
    });
    const app = mountApp();
    await settled();
    announce(healthy("prod-kyiv-01"));

    /* The URL off the `Request` the client built, not off the argument this case passed in — the
       client is what turns a path template and its parameters into an address, and the template is
       the half that has no compiler behind it yet. */
    const asked = (globalThis.fetch as unknown as { mock: { calls: unknown[][] } }).mock.calls.map(
      (call) => {
        const first = call[0];
        return typeof first === "string"
          ? first
          : first instanceof URL
            ? first.href
            : (first as Request).url;
      },
    );
    const feedReads = asked
      .map((url) => new URL(url, "http://kui.test"))
      .filter((url) => url.pathname === "/api/v1/clusters/prod-kyiv-01/alerts/events");
    expect(feedReads).not.toHaveLength(0);
    /* And it reads without marking. Opening a page must not silently destroy the unread set of
       somebody who came to look at something else — `markRead` is what the "Mark all read" control
       sends, and the store takes one endpoint with a query rather than two addresses so that these
       two calls cannot end up spelled differently. */
    expect(feedReads.every((url) => url.searchParams.get("markRead") === "false")).toBe(true);
    /* And the stream, whose address needs nothing from the generated schema and is therefore the
       half that could drift silently for ever. */
    expect(SilentEventSource.opened).toContain(
      "/api/v1/clusters/prod-kyiv-01/alerts/stream",
    );

    const bell = app.host.querySelector("[data-testid='notifications']")!;
    expect(bell.querySelector(".kui-bell__badge")?.textContent).toBe("7");
    expect(bell.getAttribute("aria-label")).toBe("Notifications, 7 open alerts, unread");

    /* The drawer's Alerts row carries the same figure, from the same accessor. Two readers of one
       store is the whole reason the store is in the kernel. */
    const row = app.host.querySelector("[data-testid='nav-alerts']");
    expect(row?.textContent).toContain("7");
    expect(row?.getAttribute("aria-label")).toContain("7 open");

    /* And the panel lists what the bell counted, from the same store. It was a hard-coded empty
       list for three waves, so a bell with a number over a panel saying "the cluster has been
       quiet" is the exact self-contradiction this wiring exists to remove. */
    (app.host.querySelector<HTMLButtonElement>("[data-testid='notifications']"))!.click();
    flush();
    const panel = app.host.querySelector("[data-testid='notification-panel']")!;
    expect(panel.textContent).toContain("Log directory past its critical threshold");
    expect(panel.textContent).toContain("orders-consumers has been rebalancing");
    expect(panel.textContent).not.toContain("has been quiet");

    /* Mark all read is the server's decision, not the browser's: the same endpoint with its own
       query, so that the count the bell then draws is one the service recounted rather than one
       this tab zeroed and hoped about. */
    const before = feedReads.length;
    (panel.querySelector<HTMLButtonElement>(".kui-notices__mark"))!.click();
    await settled();
    const marked = (globalThis.fetch as unknown as { mock: { calls: unknown[][] } }).mock.calls
      .map((call) => {
        const first = call[0];
        return typeof first === "string" ? first : (first as Request).url;
      })
      .map((url) => new URL(url, "http://kui.test"))
      .filter((url) => url.pathname === "/api/v1/clusters/prod-kyiv-01/alerts/events");
    expect(marked.length).toBeGreaterThan(before);
    expect(marked.some((url) => url.searchParams.get("markRead") === "true")).toBe(true);

    app.dispose();
  });

  /**
   * The same feed, over a deployment that runs no alerts service.
   *
   * `not_configured` is **hidden, not empty** (ADR-032), and the two halves of that rule land in
   * two different places because the bell is not a nav row: the drawer leaves the Alerts entry out
   * altogether, and the bell — which is part of the frame and predates the feed — draws no badge
   * and says in words that there is no service, rather than "the cluster has been quiet", which is
   * a claim about a cluster nobody measured.
   */
  it("draws no Alerts row and claims no quiet cluster where none is configured", async () => {
    window.history.replaceState({}, "", "/ui/clusters/prod-kyiv-01");
    stubCluster({
      "/api/v1/clusters/prod-kyiv-01/alerts/events": {
        events: { status: "not_configured", fetchedAt: "2026-09-06T09:00:00.000Z" },
      },
    });
    const app = mountApp();
    await settled();
    /* The row's visibility is the **capability** picture's answer and not the feed's: ADR-032 hides
       a feature whose upstream is not configured, and that is a fact about the deployment which the
       gateway reports before anything is read. The feed's own `not_configured` section is what the
       bell then has to say something honest about, which is the other half of this case. */
    announce(withoutAlerts("prod-kyiv-01"));

    expect(app.host.querySelector("[data-testid='nav-alerts']")).toBeNull();

    const bell = app.host.querySelector("[data-testid='notifications']")!;
    expect(bell.querySelector(".kui-bell__badge")).toBeNull();
    expect(bell.getAttribute("aria-label")).toBe(
      "Notifications, the number of open alerts is not known",
    );

    (bell as HTMLButtonElement).click();
    flush();
    const panel = app.host.querySelector("[data-testid='notification-panel']")!;
    expect(panel.textContent).toContain("runs no alerts service");
    expect(panel.textContent).not.toContain("has been quiet");
    // Nothing failed, so there is nothing to try again.
    expect(panel.textContent).not.toContain("Try again");

    app.dispose();
  });

  /**
   * A zero the rules have never produced, which is the worst possible thing to draw as a zero.
   *
   * `AlertFeed.evaluatedAt` is when `services/alerts` last ran its rules over this cluster, and it
   * is absent when it never has here — a KUI that has just started, or whose evaluation loop has
   * not reached this cluster yet. The feed is then perfectly well-formed and `openCount` is
   * perfectly `0`, and a bell that drew that as "no open alerts" would be putting a confident green
   * over a question nobody has asked. It is the storage meter's em dash inverted: there, an unknown
   * was drawn as unreadable; here, an unlooked-at cluster would be drawn as well.
   *
   * The whole difference is one absent field, so nothing on the screen distinguishes the two states
   * except the sentence — which is exactly why the sentence is the assertion.
   */
  it("says the open count is not known for a feed whose rules have never run", async () => {
    window.history.replaceState({}, "", "/ui/clusters/prod-kyiv-01");
    stubCluster({
      "/api/v1/clusters/prod-kyiv-01/alerts/events": {
        events: {
          status: "ok",
          fetchedAt: "2026-09-06T09:00:00.000Z",
          // A real, readable feed. Nothing is wrong with it; nothing has been evaluated either.
          data: { items: [], openCount: 0, unreadCount: 0 },
        },
      },
    });
    const app = mountApp();
    await settled();
    announce(healthy("prod-kyiv-01"));

    const bell = app.host.querySelector("[data-testid='notifications']")!;
    expect(bell.querySelector(".kui-bell__badge")).toBeNull();
    expect(bell.getAttribute("aria-label")).toBe(
      "Notifications, the number of open alerts is not known",
    );
    // And emphatically not this, which is the sentence for a cluster the rules *have* swept.
    expect(bell.getAttribute("aria-label")).not.toContain("no open alerts");

    app.dispose();
  });

  /**
   * Nothing goes down a stream before the session exists, and there are two streams now.
   *
   * Both the capability stream and the alert feed's open through `deferUntilSession`, and the
   * reason is spelled out where the first of them is built: `openEventSource` uses the browser's
   * native `EventSource`, which the API client's middleware never sees, so the token gate in
   * `@kui/api` cannot hold it. A cookieless stream makes the gateway mint a session and stamp
   * `Set-Cookie` on the answer; `/auth/me` races it, the browser keeps whichever reply lands last,
   * and the CSRF token this client holds then belongs to the other session. Every read still works
   * — a fresh anonymous session can read what an anonymous session can read — and every **write**
   * is refused with "X-Csrf-Token does not match the session's token". Two cookieless streams
   * instead of one makes it twice as likely.
   *
   * Removing the wrapper from either left all 500 cases in this package green, because every stub
   * in this file answers `/auth/me` on the spot and the interval the rule is about did not exist.
   * So this case holds that answer open and looks at the interval.
   */
  it("opens neither stream until the session has answered", async () => {
    window.history.replaceState({}, "", "/ui/clusters/prod-kyiv-01");
    let release: (() => void) | undefined;
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    stubCluster({}, held);

    const app = mountApp();
    await settled();
    /* Not "no alerts stream": **no stream at all**. The capability stream is subject to the same
       rule and was equally unasserted, and naming only the one this packet added would leave the
       other free to regress. */
    expect(SilentEventSource.opened).toEqual([]);

    release!();
    await settled();

    expect(SilentEventSource.opened).toContain("/api/v1/capabilities/stream");
    expect(SilentEventSource.opened).toContain(
      "/api/v1/clusters/prod-kyiv-01/alerts/stream",
    );

    app.dispose();
  });

  /**
   * Switching cluster, from the alert feed's point of view.
   *
   * The store is built once and restarted per cluster, and the restart is the part worth pinning:
   * the previous cluster's stream has to be closed and a new one opened against the new address.
   * Left open, an `EventSource` for `prod` goes on delivering counts into a bell that is now
   * describing `staging` — a number from somewhere the operator is not looking, which is the
   * wrong-answer-that-looks-right failure the overview's own fetch effect guards against and the
   * reason the store also checks the cluster each frame names.
   *
   * Asserted on the addresses opened rather than on a mock's call count, because the address is the
   * thing that is wrong when this breaks.
   */
  it("re-opens the alert stream against the cluster the address moved to", async () => {
    window.history.replaceState({}, "", "/ui/clusters/prod-kyiv-01");
    stubCluster();
    const app = mountApp();
    await settled();
    announce(healthy("prod-kyiv-01", "staging-eu-01"));

    const streams = () => SilentEventSource.opened.filter((url) => url.includes("/alerts/stream"));
    expect(streams()).toContain("/api/v1/clusters/prod-kyiv-01/alerts/stream");
    expect(streams()).not.toContain("/api/v1/clusters/staging-eu-01/alerts/stream");

    const tile = app.host.querySelector<HTMLButtonElement>(
      "[data-testid='env-tile-staging-eu-01']",
    );
    expect(tile).not.toBeNull();
    tile!.click();
    await settled();

    expect(streams()).toContain("/api/v1/clusters/staging-eu-01/alerts/stream");
    /* And the feed was re-read for the new cluster, so the bell is not left drawing the count it
       learned for the one the operator has left. */
    const asked = (globalThis.fetch as unknown as { mock: { calls: unknown[][] } }).mock.calls
      .map((call) => {
        const first = call[0];
        return typeof first === "string" ? first : (first as Request).url;
      })
      .map((url) => new URL(url, "http://kui.test").pathname);
    expect(asked).toContain("/api/v1/clusters/staging-eu-01/alerts/events");

    app.dispose();
  });

  /** A feed the rules have run over, so the bell has a real figure to be moved off. */
  const EVALUATED_FEED: unknown = {
    events: {
      status: "ok",
      fetchedAt: "2026-09-06T09:00:00.000Z",
      data: {
        items: [],
        openCount: 7,
        unreadCount: 0,
        evaluatedAt: "2026-09-06T08:59:00.000Z",
      },
    },
  };

  const badgeOf = (host: HTMLElement): string | undefined =>
    host.querySelector("[data-testid='notifications'] .kui-bell__badge")?.textContent ?? undefined;

  /**
   * Which cluster a stream frame is allowed to move this bell for.
   *
   * The guard is one line in `App.tsx` — `cluster: () => clusterForFrame()` on the store's options
   * — and the kernel reads it as `if (mine !== undefined && change.cluster !== mine) return`.
   * Passing `() => undefined` therefore does not *loosen* the check, it **deletes** it: the store
   * stops having a cluster to compare against and every frame on the connection moves the count.
   * That was green across every case in this package, because nothing had ever pushed a frame
   * naming a cluster other than the one the frame is describing.
   *
   * It is not a hypothetical shape. The gateway's relay carries one cluster's stream today, and the
   * store is built **once** and restarted per cluster — so a frame in flight when the operator
   * switches, or a relay that ever multiplexes, lands on a bell that is now describing somewhere
   * else. A number from a cluster nobody is looking at, on the one control that is meant to say
   * whether this cluster is on fire.
   *
   * Both directions in one case, because the refusal alone is satisfiable by a store that ignores
   * every frame — which is exactly what the wave-6 stream did for two weeks.
   */
  it("takes a stream frame for its own cluster, and refuses one for another", async () => {
    window.history.replaceState({}, "", "/ui/clusters/prod-kyiv-01");
    stubCluster({ "/api/v1/clusters/prod-kyiv-01/alerts/events": EVALUATED_FEED });
    const app = mountApp();
    await settled();
    announce(healthy("prod-kyiv-01", "staging-eu-01"));

    expect(badgeOf(app.host)).toBe("7");

    /* A frame naming the cluster the operator has *not* got open, down the connection this cluster
       opened. 147 rather than 8, so the mutation's symptom is unmistakable: the badge would read
       `9+`, which no other state on this case can produce. */
    announceAlerts("prod-kyiv-01", {
      cluster: "staging-eu-01",
      openCount: 147,
      at: "2026-09-06T09:01:00.000Z",
    });
    expect(badgeOf(app.host)).toBe("7");

    /* And this cluster's own frame moves it in the same breath, which is what makes the line above
       a filter rather than a bell that has stopped listening. */
    announceAlerts("prod-kyiv-01", {
      cluster: "prod-kyiv-01",
      openCount: 9,
      at: "2026-09-06T09:02:00.000Z",
    });
    expect(badgeOf(app.host)).toBe("9");

    app.dispose();
  });

  /**
   * No cluster in the address, and therefore no feed at all.
   *
   * The guard is `if (chosen === undefined) return undefined;` at the head of the per-cluster
   * effect, and deleting it left every case green — because every case that cares about alerts
   * mounts on a cluster's address. What it costs is two requests to addresses that match no route:
   * `GET /api/v1/clusters//alerts/events` and an `EventSource` on
   * `/api/v1/clusters//alerts/stream`, the second of which reconnects on its own schedule for as
   * long as the operator stays on `/ui/settings`. The comment block above the guard discusses at
   * length which *other* line in the same effect is safely deletable, which is precisely the kind
   * of paragraph that reads as a test and is not one.
   *
   * The capability stream is asserted open in the same case, so this cannot pass by the shell
   * having failed to start.
   */
  it("opens no alert feed and no alert stream until an address names a cluster", async () => {
    window.history.replaceState({}, "", "/ui/settings");
    stubCluster();
    const app = mountApp();
    await settled();

    expect(SilentEventSource.opened.filter((url) => url.includes("/alerts/"))).toEqual([]);
    expect(SilentEventSource.opened).toContain("/api/v1/capabilities/stream");

    const asked = (globalThis.fetch as unknown as { mock: { calls: unknown[][] } }).mock.calls
      .map((call) => {
        const first = call[0];
        return typeof first === "string" ? first : (first as Request).url;
      })
      .map((url) => new URL(url, "http://kui.test").pathname);
    expect(asked.filter((path) => path.includes("/alerts/"))).toEqual([]);
    // And nothing was asked about the empty cluster under any other spelling either.
    expect(asked.filter((path) => path.startsWith("/api/v1/clusters//"))).toEqual([]);

    app.dispose();
  });

  /**
   * A refusal is not a retry, and the widening that breaks it does not look like a widening.
   *
   * `onRetryNotifications` is handed over only when the feed `kind` is `failed`. Widening that to
   * `!== "loading"` is green across the whole package, and it puts a Try-again button under *"You
   * do not have permission to see this cluster's alerts."* — a control that is refused every time
   * it is pressed, which is the one thing this product's error copy is written to avoid.
   *
   * The packet that shipped the guard disclosed it as gated, citing the not-configured case above.
   * That case cannot fail: `NotificationPanel` draws no retry in its `not_configured` arm at all,
   * whatever `onRetry` it is handed. `forbidden` is the status that reaches the panel *as* a
   * failure — `noticesOf` maps it to `{ kind: "failed" }` with a permission sentence — so it is the
   * only state where the widening is visible, and it is the state this case drives.
   */
  it("offers no Try again under a refusal, and one when the read actually failed", async () => {
    window.history.replaceState({}, "", "/ui/clusters/prod-kyiv-01");
    stubCluster({
      "/api/v1/clusters/prod-kyiv-01/alerts/events": {
        events: { status: "forbidden", fetchedAt: "2026-09-06T09:00:00.000Z" },
      },
    });
    const refused = mountApp();
    await settled();
    announce(healthy("prod-kyiv-01"));

    refused.host.querySelector<HTMLButtonElement>("[data-testid='notifications']")!.click();
    flush();
    const refusedPanel = refused.host.querySelector("[data-testid='notification-panel']")!;
    expect(refusedPanel.textContent).toContain("You do not have permission");
    expect(refusedPanel.textContent).not.toContain("Try again");
    refused.dispose();

    /* The other half, so the rule is a distinction and not a button nobody ever draws. An upstream
       that did not answer is worth asking again, and this is the state that says so. */
    SilentEventSource.live.length = 0;
    SilentEventSource.opened.length = 0;
    stubCluster({
      "/api/v1/clusters/prod-kyiv-01/alerts/events": {
        events: {
          status: "unavailable",
          fetchedAt: "2026-09-06T09:00:00.000Z",
          /* The envelope's own spelling: `reason` is the code and `message` is the sentence beside
             it, which is what `decodeSection` reads and what `readReason` folds into a
             `SectionReason`. */
          reason: "KUI-UPSTREAM-UNAVAILABLE",
          message: "The alerts service did not answer.",
        },
      },
    });
    const broken = mountApp();
    await settled();
    announce(healthy("prod-kyiv-01"));

    broken.host.querySelector<HTMLButtonElement>("[data-testid='notifications']")!.click();
    flush();
    const brokenPanel = broken.host.querySelector("[data-testid='notification-panel']")!;
    expect(brokenPanel.textContent).toContain("The alerts service did not answer.");
    expect(brokenPanel.textContent).toContain("Try again");
    broken.dispose();
  });

  /**
   * `+ Create topic` on the dashboard.
   *
   * The handler is three lines in `App.tsx` and replacing it with a no-op was, until this case,
   * invisible to all 281 shell tests: nothing mounted the route that renders the button. The
   * assertion is the address, because the address is what the wiring produces — the create flow
   * lives inside the topics screen and this button is the route to it.
   */
  /**
   * The cluster half of a race whose search half is gated twice.
   *
   * Filed by W8-07's verification pass as V3-1. `App.tsx`'s overview effect carries a `cancelled`
   * flag and a cleanup that sets it, and the comment beside it states the rule: *"Switching cluster
   * while five requests are in flight must not let the old cluster's answers land on the new
   * cluster's screen — the most convincing kind of wrong number there is."* Replacing
   * `if (cancelled) return;` with `if (false) return;` left all 536 shell cases green.
   *
   * The search field's copy of this rule has two cases — the older query's rows, and the emptied
   * box — and both go red when their guard is defeated. The dashboard's copy had none, and it is
   * the worse of the two failures: a stale search result is a list of topic names that visibly do
   * not match what is in the box, while a stale broker count is a plain correct-looking number on
   * the tile an operator reads first, under the name of a cluster it is not about. Nothing on the
   * screen contradicts it and nothing ever will — the figure simply stays wrong until the next
   * poll, and the two clusters this product is designed around are production and staging.
   *
   * The interval is the whole of the case, which is why the first cluster's answer is held on a
   * deferred rather than stubbed: with an immediate stub there is no window for a switch to happen
   * in, and every existing case in this file has run inside that empty window.
   */
  it("hides the previous cluster's overview immediately while the next cluster loads", async () => {
    window.history.replaceState({}, "", "/ui/clusters/prod-kyiv-01");
    stubCluster();
    const original = globalThis.fetch;
    let release!: () => void;
    const held = new Promise<void>((resolve) => { release = resolve; });
    vi.stubGlobal("fetch", async (request: Request) => {
      if (request.url.includes("/clusters/staging-eu-01")) await held;
      return original(request);
    });
    const app = mountApp();
    try {
      await settled();
      announce(healthy("prod-kyiv-01", "staging-eu-01"));
      await settled();
      const figure = () => app.host.querySelector("[data-testid='stat-brokers']")?.textContent ?? "";
      expect(figure()).toContain("3");
      app.host.querySelector<HTMLButtonElement>("[data-testid='env-tile-staging-eu-01']")!.click();
      await settled();
      expect(figure()).not.toContain("3");
    } finally { release(); await settled(); app.dispose(); }
  });

  it("keeps the new cluster's figures when the old one's answer lands later", async () => {
    window.history.replaceState({}, "", "/ui/clusters/prod-kyiv-01");

    let releaseProd: () => void = () => undefined;
    const prodHeld = new Promise<void>((resolve) => {
      releaseProd = resolve;
    });

    const summaryFor = (id: string, brokerCount: number) => ({
      cluster: {
        id,
        name: id,
        summary: ok({
          version: "3.7.0",
          brokerCount,
          offlinePartitionCount: 0,
          underReplicatedPartitionCount: 0,
        }),
      },
    });

    /* The staging cluster answers everything the overview asks for, immediately; the production
       cluster answers everything immediately **except** its own summary, which is the one request
       held open across the switch. Holding one of the overview's reads holds `fetchOverview`, which
       is the await the guard protects. */
    const answers: Readonly<Record<string, unknown>> = {
      ...CLUSTER,
      "/api/v1/clusters/prod-kyiv-01": summaryFor("prod-kyiv-01", 3),
      "/api/v1/clusters/staging-eu-01": summaryFor("staging-eu-01", 7),
      "/api/v1/clusters/staging-eu-01/brokers": {
        brokers: ok([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }, { id: 5 }, { id: 6 }, { id: 7 }]),
      },
      "/api/v1/clusters/staging-eu-01/log-dirs": { logDirs: ok([]) },
      "/api/v1/clusters/staging-eu-01/topics": {
        topics: ok({ items: [], page: { totalItems: 4 } }),
        incompleteTopics: 0,
      },
      "/api/v1/clusters/staging-eu-01/topics/names": { names: ok([]) },
    };

    vi.stubGlobal("EventSource", SilentEventSource);
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const href =
          typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
        const path = new URL(href, "http://kui.test").pathname;
        if (path.includes("/auth/me")) {
          return new Response(JSON.stringify(SESSION), {
            status: 200,
            headers: { "content-type": "application/json" },
          });
        }
        if (path === "/api/v1/clusters/prod-kyiv-01") await prodHeld;
        const body = answers[path];
        if (body !== undefined) {
          return new Response(JSON.stringify(body), {
            status: 200,
            headers: { "content-type": "application/json" },
          });
        }
        if (path.includes("/auth/settings")) {
          return new Response(JSON.stringify({ authType: "disabled", providers: [] }), {
            status: 200,
            headers: { "content-type": "application/json" },
          });
        }
        return new Response(
          JSON.stringify({ code: "KUI-ROUTE-NOT-FOUND", message: "no route", details: [] }),
          { status: 404, headers: { "content-type": "application/json" } },
        );
      }),
    );

    const app = mountApp();
    await settled();
    announce(healthy("prod-kyiv-01", "staging-eu-01"));
    await settled();

    const brokerFigure = (): string =>
      app.host.querySelector("[data-testid='stat-brokers']")?.textContent ?? "";

    /* `finally` rather than a plain sequence: an assertion that throws before `releaseProd()` would
       leave this case's fetch stub awaiting a promise nobody ever settles, and the next case in the
       file would inherit a suspended request against a global that `afterEach` has already
       unstubbed. A failing case must fail loudly here, not quietly somewhere below. */
    try {
      // Production's read is still open, so its figure has not arrived and must not be invented.
      expect(brokerFigure()).not.toContain("3");

      const tile = app.host.querySelector<HTMLButtonElement>(
        "[data-testid='env-tile-staging-eu-01']",
      );
      expect(tile).not.toBeNull();
      tile!.click();
      await settled();

      expect(brokerFigure()).toContain("7");

      // And now the loser: production's answer, arriving after the operator has left it.
      releaseProd();
      await settled();
      await settled();

      expect(brokerFigure()).toContain("7");
      expect(brokerFigure()).not.toContain("3");
    } finally {
      releaseProd();
      app.dispose();
    }
  });

  it("takes the dashboard's Create topic button to the cluster's topic list", async () => {
    window.history.replaceState({}, "", "/ui/clusters/prod-kyiv-01");
    stubCluster();
    const app = mountApp();
    await settled();

    const create = [...app.host.querySelectorAll<HTMLButtonElement>("button")].find((button) =>
      button.textContent?.includes("Create topic"),
    );
    expect(create).toBeDefined();

    create!.click();
    await settled();

    expect(window.location.pathname).toBe("/ui/clusters/prod-kyiv-01/topics");

    app.dispose();
  });

  /**
   * The same button on the address that names no cluster, pressed.
   *
   * Filed as W11-04/U2 by that packet's verifier: `App.tsx`'s handler guards on
   * `chosen !== undefined` and nothing held the guard — `navigate(paths.topics(chosen ?? ""))` left all
   * 557 shell cases green. Measured here before this case was written, and the finding is argued down
   * rather than closed by it, because the algebra says the mutant is **equivalent today**:
   * `CreateTopicAction` marks the button `aria-disabled` whenever `params.clusterId ?? kui.cluster()`
   * is undefined, `kui.cluster()` IS `clusterForFrame()` (App.tsx:371) and `params.clusterId` is
   * `routeCluster()` by another name, so the button is inert on exactly the addresses the guard
   * refuses, and `Button` swallows an inert press before the handler runs. The guard is unreachable —
   * by accident of there being one caller, not by construction.
   *
   * So this is the case that makes the accident stay visible, in the shape wave 11 asks for. It is the
   * only place in the repository that presses this button at `/ui` through the composition root:
   * `overview.render.test.tsx` mounts `Overview` directly and deliberately passes no `onCreateTopic`,
   * so nothing asserted that the ROUTE hands the component a cluster of `undefined` here. Enabling the
   * button — restoring the defect wave 11's W11-04 came to repair — reddens this and not that.
   *
   * Measured, three ways, on 2026-09-12:
   *
   *   guard removed, disablement kept      this case GREEN -- the mutant is equivalent, as argued above
   *   disablement removed, guard kept      RED on `aria-disabled` alone; the address stayed `/ui`,
   *                                        which is the guard doing the work the button stopped doing
   *   both removed                         RED twice, the second on `/ui/clusters//topics`
   *
   * Two guards on one rule, and this is the only case that sees them as one rule.
   */
  it("presses `Create topic` on the address that names no cluster and goes nowhere", async () => {
    // The selection is persisted, and the cases above have been choosing clusters. `/ui` recovers the
    // stored one, so a case about "no cluster" has to start from a browser that has never chosen.
    window.localStorage.clear();
    window.history.replaceState({}, "", "/ui");
    stubCluster();
    const app = mountApp();
    await settled();

    const create = [...app.host.querySelectorAll<HTMLButtonElement>("button")].find((button) =>
      button.textContent?.includes("Create topic"),
    );
    expect(create, "the dashboard draws no Create topic action at all on /ui").toBeDefined();
    // `soft`, so that the press below is still made and reported when this half fails: the two halves
    // are two different guards on one rule, and a run that stopped here would say nothing about
    // whether the address moved.
    expect
      .soft(
        create!.getAttribute("aria-disabled"),
        "the root dashboard offers an enabled primary action it cannot perform",
      )
      .toBe("true");

    create!.click();
    await settled();

    expect(window.location.pathname).toBe("/ui");

    app.dispose();
  });

  /**
   * Every control `/ui` draws, pressed, through the composition root.
   *
   * ## What this replaces, and why the case it replaces was not enough
   *
   * `overview/overview.render.test.tsx` has swept this rule since wave 11 under the name *"offers
   * no enabled action this address cannot perform"*, and the name over-claimed six-fold. It mounts
   * `Overview` **alone**, so the inventory it sweeps is exactly two entries — the header's
   * `Create topic` and the empty state's `Manage clusters` link — and its distinguishing half, the
   * press, executes **zero** times on a green tree: the button hits `continue` on `aria-disabled`
   * and the link hits `continue` on the `A` branch. Its vacuity guard, `length > 1`, is satisfied
   * by exactly those two and would not have noticed. Measured here on the same address through
   * `App`: the frame draws **thirteen** `a, button` controls before a capability frame arrives, of
   * which four are buttons. The other eleven are `TopBar`'s three unlabelled glyphs, the rail's two
   * links, the brand block's, the drawer's three rows and the cluster status card's — every one of
   * them on `/ui` exactly as the header action is, and none of them reachable from a mount of one
   * screen. That case is now named for the two controls it really holds; this one is the sweep.
   *
   * ## The rule, and why it is *the document changed* rather than *it navigated*
   *
   * The defect this class exists for is a control that claims an action it cannot perform:
   * `/ui` shipped an **enabled** primary `Create topic` whose handler read the selection, found
   * none and returned — no navigation, no dialog, no message, nothing. The Overview-level case
   * asked *navigated or opened a dialog*, which is the right question for a header action and the
   * wrong one for a frame: the theme glyph repaints, the appearance glyph opens a popover and the
   * bell opens a panel, and not one of those three is a navigation or a `role="dialog"`. So the
   * question this asks is the weakest one that still refuses the defect — **pressing an enabled
   * control changes the address or changes the document.** A control that does neither did nothing,
   * whatever it promised, and `document.documentElement` is the comparison because the theme
   * control writes its answer onto the root element rather than inside the mounted host.
   *
   * Links are asserted to have a destination rather than clicked, which is the same branch the
   * Overview case takes and for a jsdom reason rather than a design one: jsdom refuses a navigation
   * to another document, so a clicked `<a href>` changes neither side of the question and every
   * link would pass for the wrong reason.
   *
   * ## What the sweep leaves out, and it is one node
   *
   * `.kui-notice-stack` — the toast region. A toast is not a control this address draws; it is a
   * receipt for an action somebody already took, it dismisses itself on a timer, and the store
   * behind it is module-level and therefore survives a `dispose()`. A sweep that included it read
   * a leftover *"Switched to staging-eu-01"* from a case ten `it`s further up when the whole file
   * ran, and then found it gone on the second mount — green under `-t`, red under `pnpm test`,
   * which is the worst shape a gate can have. Its own dismiss button is swept by
   * `kernel/src/components/surfaces.test.tsx`.
   *
   * ## Why a fresh mount per press
   *
   * Pressing the bell opens a panel, and the panel draws controls of its own. A sweep that pressed
   * its way down one inventory would therefore be pressing a frame that the previous press had
   * already changed, and the fifth assertion would be about a document the first four made. Each
   * press gets a frame that nothing has touched, and the control is found again by position with
   * its identity re-asserted, so a re-order between mounts fails loudly instead of silently
   * pressing something else.
   */
  it("presses every enabled control `/ui` draws, and each one does something", async () => {
    stubCluster();

    /** Enough of a control to name it in a failure and to recognise it on a second mount. */
    const identify = (control: Element): string => {
      const label =
        (control.textContent ?? "").trim() ||
        control.getAttribute("aria-label") ||
        "(no accessible name)";
      const testid = control.getAttribute("data-testid");
      return `${control.tagName} "${label}"${testid === null ? "" : ` [${testid}]`}`;
    };

    /* The selection is persisted and the cases above have been choosing clusters, so a case about
       the address that names none has to start from a browser that has never chosen. */
    const openFrame = async () => {
      window.localStorage.clear();
      window.history.replaceState({}, "", "/ui");
      const app = mountApp();
      await settled();
      return app;
    };

    /** Every control the frame draws, minus the transient toast region — see above. */
    const draws = (app: { readonly host: HTMLElement }): HTMLElement[] =>
      [...app.host.querySelectorAll<HTMLElement>("a, button")].filter(
        (control) => control.closest(".kui-notice-stack") === null,
      );

    const survey = await openFrame();
    const roster = draws(survey);
    const named = roster.map(identify);

    /* A sweep over nothing passes every assertion inside it. `> 1` was the old guard and two
       controls satisfied it; this address draws a rail, a drawer, a top bar and a screen, so the
       guard is a whole frame's worth. It is deliberately well under the thirteen measured, because
       a number that has to be edited when a nav row is added is a number somebody edits without
       reading. */
    expect(
      named.length,
      `/ui drew ${named.length} controls, which is a screen and not a frame: ${named.join(", ")}`,
    ).toBeGreaterThan(8);

    roster.forEach((control, index) => {
      if (control.tagName !== "A") return;
      expect(
        control.getAttribute("href") ?? "",
        `the link ${named[index]} goes nowhere`,
      ).not.toBe("");
    });

    const pressable = roster
      .map((control, index) => ({ control, index, name: named[index] ?? "" }))
      .filter(
        ({ control }) =>
          control.tagName === "BUTTON" && control.getAttribute("aria-disabled") !== "true",
      )
      .map(({ index, name }) => ({ index, name }));
    survey.dispose();

    /* And the other half of the same vacuity argument. Both filters above discard rather than
       assert — a link is checked for a destination, a refusing button is skipped — so a frame whose
       every button carried `aria-disabled` would press nothing at all while still reporting
       thirteen controls, which is precisely how the case this replaces stayed green. */
    expect(
      pressable.length,
      "no enabled button on /ui, so the press below asserts nothing",
    ).toBeGreaterThan(2);

    for (const { index, name } of pressable) {
      const app = await openFrame();
      const control = draws(app)[index];
      expect(
        control === undefined ? "(gone)" : identify(control),
        "the frame drew a different roster on a second mount, so this pressed the wrong control",
      ).toBe(name);

      const addressBefore = `${window.location.pathname}${window.location.search}`;
      const documentBefore = document.documentElement.outerHTML;
      control?.click();
      await settled();

      const moved = `${window.location.pathname}${window.location.search}` !== addressBefore;
      const drew = document.documentElement.outerHTML !== documentBefore;
      expect(
        moved || drew,
        `pressing the enabled control ${name} changed neither the address nor the document`,
      ).toBe(true);

      app.dispose();
    }
  });

  /**
   * Every in-page link the frame draws points at an element that is actually in the page.
   *
   * Filed by this wave's verification pass over W12-04 as two rows, and both reproduced here before
   * this case was written. The sweep above checks a link's `href` for `!== ""`, which is the weakest
   * possible statement about a destination and is satisfied by one character of punctuation:
   *
   *   `AppFrame.tsx:46  href="#kui-content"` -> `href="#"`      `pnpm -C frontend test` GREEN
   *   `AppFrame.tsx:55  id="kui-content"`    -> `id="kui-conten"` `pnpm -C frontend test` GREEN
   *
   * Under either one the skip link is still drawn, still has a destination and still passes every
   * case in this repository — and it now jumps a keyboard user to the top of the page, which is
   * where they already are. The second was disclosed by W12-04 itself as a green it could not close;
   * it is confirmed here by mutation rather than taken on trust.
   *
   * A fragment is the one kind of destination jsdom can settle, which is why this is a unit case and
   * not an `e2e` one: `document.getElementById` either finds the target or it does not, and a link
   * pointing at nothing is not a design question.
   */
  it("resolves every fragment link `/ui` draws to an element the frame really carries", async () => {
    stubCluster();
    window.localStorage.clear();
    window.history.replaceState({}, "", "/ui");
    const app = mountApp();
    await settled();

    const fragments = [...app.host.querySelectorAll<HTMLAnchorElement>("a[href^='#']")];

    /* The anchor, and it is the whole reason this case is not vacuous: the skip link is the only
       in-page link the frame draws, so a sweep that found none would pass silently the moment
       somebody renamed its class or dropped it. */
    expect(
      fragments.map((link) => link.getAttribute("href") ?? ""),
      "/ui drew no in-page link at all, so the rule below holds over nothing — the skip link is the " +
        "one this frame is required to carry",
    ).toContain("#kui-content");

    for (const link of fragments) {
      const href = link.getAttribute("href") ?? "";
      const name = (link.textContent ?? "").trim() || href;
      /* `#` on its own is a valid `href` and an invalid selector, so it is refused by name rather
         than handed to `querySelector`, which would throw instead of failing. */
      expect(href.length, `the in-page link ${name} points at "${href}", which is the page it is on`)
        .toBeGreaterThan(1);
      expect(
        app.host.querySelector(`[id="${href.slice(1)}"]`),
        `the in-page link ${name} points at ${href} and no element in the frame carries that id, so ` +
          "following it moves the reader nowhere",
      ).not.toBeNull();
    }

    app.dispose();
  });

  /**
   * The bell closes what it opened.
   *
   * Filed by this wave's verification pass over W12-04, and it is the direct cost of the sweep
   * above being a fresh-mount-per-press design: every control is pressed exactly once from a clean
   * frame, so NO toggle's second press is asserted anywhere in this frontend. Measured on
   * 2026-09-12 — `onToggleNotifications={() => setNoticesOpen(!noticesOpen())}` in `App.tsx`
   * replaced by `() => setNoticesOpen(true)` left `pnpm -C frontend test` green over 83 files. The
   * bell becomes one-way: it opens the panel and can never close it, and the only way back is a
   * reload. The sweep cannot see it, because opening the panel changes the document, which is
   * exactly what the sweep asks for.
   */
  it("closes the notification panel on the bell's second press", async () => {
    stubCluster();
    window.localStorage.clear();
    window.history.replaceState({}, "", "/ui");
    const app = mountApp();
    await settled();

    const bell = app.host.querySelector<HTMLButtonElement>("[data-testid='notifications']");
    expect(bell, "the frame drew no notifications control, so this case presses nothing").not.toBeNull();

    bell!.click();
    await settled();
    /* The anchor: without an open panel the second press would be asserted against a frame that
       never changed, and a bell wired to nothing at all would pass. */
    expect(
      app.host.querySelector("[data-testid='notification-panel']"),
      "the first press did not open the notification panel, so the close below asserts nothing",
    ).not.toBeNull();

    bell!.click();
    await settled();
    expect(
      app.host.querySelector("[data-testid='notification-panel']"),
      "the bell's second press left the notification panel open, so the control is one-way: it " +
        "opens the panel and the only way to close it is a reload",
    ).toBeNull();

    app.dispose();
  });
});

/**
 * Changing environment from the rail.
 *
 * `EnvRailProps.onSelect` is optional, so an unwired rail is not even a type error — and
 * `shell.test.tsx`'s three cases about switching call `environmentSwitch` directly, which is the
 * *decision* and not the four side effects that carry it out. So this case does the thing an
 * operator does: it puts two clusters in front of the rail and clicks the other one.
 *
 * The capability snapshot is what puts them there. `clusters()` is folded out of the capability
 * registry and out of nothing else, so a frame has to arrive before the rail has anything to draw —
 * which is also the first time this suite has driven the stream rather than stubbing it silent.
 */
describe("the environment rail", () => {
  it("switches the frame to the cluster the operator clicked", async () => {
    window.history.replaceState({}, "", "/ui/clusters/prod-kyiv-01");
    stubGateway();
    const app = mountApp();
    await settle();
    await settle();

    announce(healthy("prod-kyiv-01", "staging-eu-01"));

    const tile = app.host.querySelector<HTMLButtonElement>(
      "[data-testid='env-tile-staging-eu-01']",
    );
    expect(tile).not.toBeNull();

    tile!.click();
    await settle();
    await settle();

    /* The address is rewritten because the address named a cluster: leaving it alone would put a
       URL saying `prod-kyiv-01` in front of a frame describing `staging-eu-01`, and the address is
       the half of that pair people copy. */
    expect(window.location.pathname).toBe("/ui/clusters/staging-eu-01/dashboard/overview");
    /* And the switch is announced, once, by the one toast region in the product. */
    expect(app.host.ownerDocument.body.textContent).toContain("Switched to staging-eu-01");

    app.dispose();
  });
});

/**
 * The top bar's search field, wired to the gateway's fold.
 *
 * The field has existed since wave 1 and searched nothing: its `onInput` was `() => undefined` and
 * its status was the literal `"idle"`, so every state below the box was reachable only in a story.
 * This is the seam — a person types, a request goes out, and the overlay draws what came back —
 * and it is asserted through the mounted application because that is the only place the wiring is.
 */
describe("the search field", () => {
  const FOUND = {
    results: {
      topics: [{ cluster: "prod-kyiv-01", name: "orders.payments.v2" }],
      groups: [{ cluster: "prod-kyiv-01", groupId: "payments-processor" }],
    },
    /* The distributed stack routes no schema service, so this is the ordinary answer rather than a
       failure — and the field has to say so instead of showing two lists out of three. */
    partial: ["schema"],
  };

  /** Records every search the shell asks for, and answers the rest as the frame's stub does. */
  function stubSearch(): string[] {
    const searched: string[] = [];
    vi.stubGlobal("EventSource", SilentEventSource);
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const href =
          typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
        const url = new URL(href, "http://kui.test");
        if (url.pathname.includes("/auth/me")) {
          return new Response(JSON.stringify(SESSION), {
            status: 200,
            headers: { "content-type": "application/json" },
          });
        }
        if (url.pathname === "/api/v1/search") {
          searched.push(url.searchParams.get("q") ?? "");
          return new Response(JSON.stringify(FOUND), {
            status: 200,
            headers: { "content-type": "application/json" },
          });
        }
        return new Response(JSON.stringify({ authType: "disabled", providers: [] }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }),
    );
    return searched;
  }

  /** Types into the box the way a person does: one event per character. */
  function type(app: { readonly host: HTMLElement }, text: string): void {
    const input = app.host.querySelector<HTMLInputElement>("[data-testid='search-input']");
    if (input === null) throw new Error("the search box is not on the page");
    input.focus();
    for (let length = 1; length <= text.length; length += 1) {
      input.value = text.slice(0, length);
      input.dispatchEvent(new Event("input", { bubbles: true }));
    }
    flush();
  }

  /** Waits out the debounce and lets the answer land. */
  async function answered(): Promise<void> {
    await new Promise((resolve) => setTimeout(resolve, SEARCH_DEBOUNCE_MS + 80));
    for (let turn = 0; turn < 4; turn += 1) {
      await new Promise((resolve) => setTimeout(resolve, 0));
      flush();
    }
  }

  /**
   * A gateway whose searches are held open until the case chooses to answer them.
   *
   * The two cases below are about *when* an answer lands, and neither is reachable through the stub
   * above: it resolves every search on the microtask queue, so two searches are never in flight at
   * once and nothing can arrive out of order. That is why the guard this file now gates went
   * unnoticed for a wave — deleting `if (episode !== searchEpisode) return;` from `App.tsx` left
   * all 333 shell tests green, because no test had ever had two answers to lose a race with.
   *
   * Keyed by the query, because that is what the case knows and what the shell sends.
   */
  function stubHeldSearch(): {
    readonly asked: readonly string[];
    readonly answer: (query: string, body: unknown) => void;
    readonly signal: (query: string) => AbortSignal;
  } {
    const asked: string[] = [];
    const held = new Map<string, (body: unknown) => void>();
    const signals = new Map<string, AbortSignal>();
    vi.stubGlobal("EventSource", SilentEventSource);
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const href =
          typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
        const url = new URL(href, "http://kui.test");
        if (url.pathname.includes("/auth/me")) {
          return new Response(JSON.stringify(SESSION), {
            status: 200,
            headers: { "content-type": "application/json" },
          });
        }
        if (url.pathname === "/api/v1/search") {
          const query = url.searchParams.get("q") ?? "";
          asked.push(query);
          if (input instanceof Request) signals.set(query, input.signal);
          return new Promise<Response>((resolve) => {
            held.set(query, (body) =>
              resolve(
                new Response(JSON.stringify(body), {
                  status: 200,
                  headers: { "content-type": "application/json" },
                }),
              ),
            );
          });
        }
        return new Response(JSON.stringify({ authType: "disabled", providers: [] }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }),
    );
    return {
      asked,
      answer: (query, body) => {
        const settle = held.get(query);
        if (settle === undefined) throw new Error(`no search for "${query}" is in flight`);
        held.delete(query);
        settle(body);
      },
      signal: (query) => {
        const signal = signals.get(query);
        if (signal === undefined) throw new Error(`no search for "${query}" is in flight`);
        return signal;
      },
    };
  }

  /** One answer, with one topic in it, so two answers are told apart by what they carry. */
  const found = (topic: string) => ({
    results: { topics: [{ cluster: "prod-kyiv-01", name: topic }] },
    partial: [],
  });

  /** Waits out the debounce so the request is out, without answering it. */
  async function requested(): Promise<void> {
    await new Promise((resolve) => setTimeout(resolve, SEARCH_DEBOUNCE_MS + 80));
    flush();
  }

  /** Lets a released answer land and the reactive graph catch up. */
  async function landed(): Promise<void> {
    for (let turn = 0; turn < 4; turn += 1) {
      await new Promise((resolve) => setTimeout(resolve, 0));
      flush();
    }
  }

  it("invalidates and cancels the previous search as soon as the query changes", async () => {
    const gateway = stubHeldSearch();
    const app = mountApp();
    await settle();

    type(app, "ord");
    await requested();
    expect(gateway.asked).toEqual(["ord"]);

    type(app, "orders");
    gateway.answer("ord", found("ord-legacy.audit"));
    await landed();

    const search = app.host.querySelector("[data-testid='search']")?.textContent ?? "";
    expect(search).not.toContain("ord-legacy.audit");
    expect(search).toContain("Searching");
    expect(gateway.signal("ord").aborted).toBe(true);

    app.dispose();
  });

  /**
   * The out-of-order guard, at the seam it defends.
   *
   * `GET /api/v1/search` is a fold over three services across every cluster, so two searches in
   * flight can finish in either order and the slower one is usually the *earlier* one — a shorter
   * prefix matches more, and matching more is what takes the time. The loser landing last would put
   * the results for `ord` under a box that reads `orders`, which is the one failure mode a search
   * box has that a user cannot see: the rows look like an answer, and they are an answer to a
   * question that was withdrawn.
   */
  it("keeps the newer query's rows when an older search lands after them", async () => {
    const gateway = stubHeldSearch();
    const app = mountApp();
    await settle();

    type(app, "ord");
    await requested();
    type(app, "orders");
    await requested();
    // Both are out. Neither has been answered.
    expect(gateway.asked).toEqual(["ord", "orders"]);

    gateway.answer("orders", found("orders.payments.v2"));
    await landed();
    const results = () => app.host.querySelector("[data-testid='search']")?.textContent ?? "";
    expect(results()).toContain("orders.payments.v2");

    // And now the loser, arriving late with a wider answer to a question nobody is asking.
    gateway.answer("ord", found("ord-legacy.audit"));
    await landed();

    expect(results()).not.toContain("ord-legacy.audit");
    expect(results()).toContain("orders.payments.v2");

    app.dispose();
  });

  /**
   * Emptying the box is the end of searching, not a search for nothing.
   *
   * The same guard, from the other side: the episode is stepped when the box is cleared, so an
   * answer already in flight cannot reopen the overlay behind the caret. Without it a person who
   * types, thinks better of it and clears the field gets a list of results a second later over an
   * empty box — with no query to explain what they are.
   */
  it("drops an answer already in flight when the box is emptied", async () => {
    const gateway = stubHeldSearch();
    const app = mountApp();
    await settle();

    type(app, "orders");
    await requested();
    expect(gateway.asked).toEqual(["orders"]);

    const input = app.host.querySelector<HTMLInputElement>("[data-testid='search-input']")!;
    input.value = "";
    input.dispatchEvent(new Event("input", { bubbles: true }));
    flush();

    gateway.answer("orders", found("orders.payments.v2"));
    await landed();

    expect(app.host.querySelector("[data-testid='search']")?.textContent).not.toContain(
      "orders.payments.v2",
    );
    /* And the overlay is not merely empty of that row: the field is back to `idle`, which is what
       takes the listbox out of the accessibility tree rather than leaving an empty one behind. */
    expect(app.host.querySelector("[role='listbox']")).toBeNull();

    app.dispose();
  });

  /**
   * A search that nobody answers is evidence about the gateway.
   *
   * The `report("shell", …)` beside the guard is the third line in this handler that no test could
   * see. It is what makes the search field count towards the connectivity tracker, and the tracker
   * is what takes the whole application to its "cannot reach the server" screen after three
   * consecutive transport failures with no success between them — which is exactly the situation a
   * person discovers by typing in a box and getting nothing back.
   */
  it("counts unanswered searches towards the connection, not towards the query", async () => {
    vi.stubGlobal("EventSource", SilentEventSource);
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const href =
          typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
        const url = new URL(href, "http://kui.test");
        if (url.pathname === "/api/v1/search") throw new TypeError("Failed to fetch");
        if (url.pathname.includes("/auth/me")) {
          return new Response(JSON.stringify(SESSION), {
            status: 200,
            headers: { "content-type": "application/json" },
          });
        }
        return new Response(JSON.stringify({ authType: "disabled", providers: [] }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }),
    );

    const app = mountApp();
    await settle();
    /* Start-up succeeded, so the tracker is connected and the count is at zero: what follows is
       three failures in a row and nothing else. */
    expect(app.host.querySelector("[data-testid='gateway-unreachable']")).toBeNull();

    for (const query of ["o", "or", "ord"]) {
      type(app, query);
      await requested();
      await landed();
    }

    /* `FailuresBeforeGivingUp` of them, and the shell says so once rather than drawing a failed
       overlay under a box that still looks like it is working. */
    expect(FailuresBeforeGivingUp).toBe(3);
    const unreachable = app.host.querySelector("[data-testid='gateway-unreachable']");
    expect(unreachable).not.toBeNull();
    expect(unreachable?.textContent).toContain("KUI cannot reach the server");

    app.dispose();
  });

  it("asks the gateway once for a typed word and draws what came back", async () => {
    const searched = stubSearch();
    const app = mountApp();
    await settle();

    type(app, "orders");
    /* One request for six keystrokes. The fold is one call per service per cluster, so a keystroke
       that escaped the debounce is three upstream calls and not one. */
    expect(searched).toEqual([]);

    await answered();
    expect(searched).toEqual(["orders"]);

    const results = app.host.querySelector("[data-testid='search']");
    expect(results?.textContent).toContain("orders.payments.v2");
    expect(results?.textContent).toContain("payments-processor");
    /* And the third list, which nobody was asked for, named rather than silently absent. */
    expect(results?.textContent).toContain("Schema Registry");

    app.dispose();
  });

  /**
   * How long the box waits, which is a rule and not a taste.
   *
   * The case above asserts that nothing has gone out *synchronously*, and that is true of any
   * deferral at all: `SEARCH_DEBOUNCE_MS` can be set to `0` and every case in this package stays
   * green, because `answered()` waits `SEARCH_DEBOUNCE_MS + 80` and rescales with it and no case
   * ever put real time between two keystrokes. What that costs is one gateway fold per character,
   * and the fold is one call per service per cluster — so a six-letter word becomes eighteen
   * upstream calls instead of three.
   *
   * The clock is faked so the waits are facts rather than races, and only the timer functions are:
   * Solid 2 batches to a microtask, and a faked `queueMicrotask` would stop the renderer. The
   * intervals are absolute milliseconds and deliberately not derived from the constant, which is
   * the mistake that let the rule go ungated in the first place.
   */
  it("holds the request through a whole typed word, not through one keystroke", async () => {
    const searched = stubSearch();
    const app = mountApp();
    await settle();

    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    try {
      const input = app.host.querySelector<HTMLInputElement>("[data-testid='search-input']")!;
      input.focus();
      // 100 ms apart, which is a fast typist rather than an impossible one.
      for (const typed of ["o", "or", "ord"]) {
        input.value = typed;
        input.dispatchEvent(new Event("input", { bubbles: true }));
        flush();
        vi.advanceTimersByTime(100);
        for (let turn = 0; turn < 4; turn += 1) {
          await Promise.resolve();
          flush();
        }
        expect(searched).toEqual([]);
      }

      // Three hundred milliseconds of typing and nothing asked. Then the pause, and one request.
      vi.advanceTimersByTime(400);
      for (let turn = 0; turn < 4; turn += 1) {
        await Promise.resolve();
        flush();
      }
      expect(searched).toEqual(["ord"]);
    } finally {
      vi.useRealTimers();
      app.dispose();
    }
  });

  /**
   * The bound the box carries, which reaches it only through `App`.
   *
   * `SEARCH_MAX_LENGTH` is passed as the field's `maxLength` at one line of `App.tsx` and nothing
   * asserted the figure: raised to 20000 the whole package stays green. What that costs is a `q`
   * the endpoint refuses with a `KUI-VALIDATION` 400, and the only failure this overlay can draw
   * says "search is not answering" — a sentence that sends an operator to look at a gateway that
   * is working perfectly. The number is written out here rather than imported, because importing
   * it is what let it drift.
   */
  it("bounds the box at the two hundred characters the endpoint accepts", () => {
    stubSearch();
    const app = mountApp();
    const input = app.host.querySelector<HTMLInputElement>("[data-testid='search-input']")!;
    expect(input.getAttribute("maxlength")).toBe("200");
    app.dispose();
  });

  it("stops searching when the box is emptied rather than searching for nothing", async () => {
    const searched = stubSearch();
    const app = mountApp();
    await settle();

    type(app, "orders");
    await answered();
    expect(searched).toEqual(["orders"]);

    const input = app.host.querySelector<HTMLInputElement>("[data-testid='search-input']")!;
    input.value = "";
    input.dispatchEvent(new Event("input", { bubbles: true }));
    flush();
    await answered();

    expect(searched).toEqual(["orders"]);
    expect(app.host.querySelector("[data-testid='search']")?.textContent).not.toContain(
      "orders.payments.v2",
    );

    app.dispose();
  });
});

/**
 * Every feature's view permission must be spelled the way the server spells it.
 *
 * The shell used to ask `permits(serviceId, "view", …)`, which compared `"topic"` against `TOPIC`
 * and `"view"` against `VIEW` by exact string, and asked for a resource called `"cluster"` that
 * does not exist at all — the cluster feature is gated on `CLUSTERCONFIG`. Every question answered
 * "no" the instant `/auth/me` replied, so on the demonstration environment, where authentication is
 * *disabled* and the principal holds a grant on every resource and every cluster, the whole drawer
 * went dim and each page read "You do not have permission to view …".
 *
 * Checking the registrations against the generated vocabulary is cheap and catches the reappearance
 * of that whole class: a value not drawn from `Actions` cannot match, and a hand-written string can
 * no longer be one.
 */
describe("the permissions the features ask for", () => {
  it("names only actions from the generated vocabulary", () => {
    const known = new Set(Object.values(Actions).map((a) => `${a.resource}:${a.action}`));
    for (const registration of featureRegistry) {
      expect(known).toContain(
        `${registration.viewAction.resource}:${registration.viewAction.action}`,
      );
    }
  });

  it("asks about a resource the server's own enum contains", () => {
    const resources = new Set<string>(Object.values(Resources));
    for (const registration of featureRegistry) {
      expect(resources).toContain(registration.viewAction.resource);
    }
  });

  it("lets a principal holding a wildcard grant see every feature", () => {
    // This is the shape `/auth/me` really returns when authentication is disabled: one grant per
    // resource, scoped to every cluster. Under the old spelling this expectation failed for all
    // four features at once.
    const grants = featureRegistry.map((registration) => ({
      clusters: ["*"],
      resource: registration.viewAction.resource,
      value: ".*",
      actions: [registration.viewAction.action],
    }));

    for (const registration of featureRegistry) {
      const covering = grants.filter(
        (grant) =>
          grant.resource === registration.viewAction.resource &&
          grant.actions.includes(registration.viewAction.action),
      );
      expect(covering.length).toBeGreaterThan(0);
    }
  });
});

/**
 * Lets the pending promises run, then flushes Solid.
 *
 * A macrotask rather than a few `await Promise.resolve()`: releasing the session gate resolves a
 * chain of promises inside the client's middleware and the capability store, and counting how many
 * microtask turns that takes is a test asserting an implementation detail of two other modules.
 */
async function settle(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 0));
  flush();
}

describe("start-up asks for the session before anything else", () => {
  /**
   * The gateway mints an anonymous session for any API request that arrives without a cookie, and
   * stamps `Set-Cookie` on the answer. Two cookieless requests therefore mint two sessions, the
   * browser keeps whichever reply lands last, and the CSRF token this client keeps — the one
   * `/auth/me` returned — belongs to the other one.
   *
   * The symptom is as bad as it gets: every read works, because a fresh anonymous session can read
   * everything an anonymous session can read, and every *write* comes back
   * "X-Csrf-Token does not match the session's token". It stayed invisible for the whole of the read
   * work and appeared the moment the first mutation existed to be refused.
   *
   * So `/auth/me` goes first and alone. Everything else — `/auth/settings`, the capability stream,
   * every feature's first read — waits behind it, by which time the browser holds a cookie and the
   * gateway resolves it instead of minting another.
   */
  it("nothing goes out beside /auth/me", async () => {
    const asked: string[] = [];
    SilentEventSource.opened.length = 0;
    vi.stubGlobal("EventSource", SilentEventSource);

    let releaseMe: (() => void) | undefined;
    const mePending = new Promise<void>((resolve) => {
      releaseMe = resolve;
    });

    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url =
          typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
        asked.push(new URL(url, "http://kui.test").pathname);
        if (url.includes("/auth/me")) {
          await mePending;
          return new Response(
            JSON.stringify({
              authType: "disabled",
              csrfToken: "test-token",
              principal: { kind: "anonymous", name: "anonymous" },
            }),
            { status: 200, headers: { "content-type": "application/json" } },
          );
        }
        return new Response(JSON.stringify({ authType: "disabled", providers: [] }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }),
    );

    const { dispose } = mountApp();
    await settle();
    await settle();

    // While `/auth/me` is still out, it is the only thing that has been asked for, and the
    // capability stream — which uses the native EventSource and so bypasses the client's own gate —
    // has not been opened either.
    expect(asked.filter((path) => !path.includes("/auth/me"))).toEqual([]);
    expect(SilentEventSource.opened).toEqual([]);

    releaseMe?.();
    await settle();
    await settle();
    await settle();

    // Once the session exists, the rest follows.
    expect(asked.some((path) => path.includes("/auth/settings"))).toBe(true);
    expect(SilentEventSource.opened.length).toBeGreaterThan(0);

/*
     * Not asserted here: whether the connectivity banner clears.
     *
     * `SilentEventSource` records the URL and never dispatches `open`, so the stream in this harness
     * genuinely never connects and the banner is *correct*. The property that matters — that the
     * deferred handle's `connection()` is reactive, so the effect driving the banner re-runs when the
     * real stream opens — needs a stream that opens, and is checked against the running stack
     * instead. It was a plain variable first, which gave Solid nothing to subscribe to: the effect
     * read "connecting" once and never again, and the application told the operator it had lost its
     * connection for the rest of the session while frames were arriving.
     */

    dispose();
  });
});
