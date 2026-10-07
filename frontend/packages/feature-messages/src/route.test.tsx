/**
 * The route, driven as the product drives it: a router, a client, and the screen it renders.
 *
 * ## Why these cases are here and not against a component
 *
 * Every rule this packet is about lives in the *joins*, and each of them can be broken with a
 * component suite still green:
 *
 *   - The partition count is a request the route makes and threads to three children. A test that
 *     passes `partitionCount={12}` to `ResendDialog` asserts that the dialog can print a number it
 *     was handed. It cannot see the route handing it a hard-coded zero, which is what the route did.
 *   - The typed predicates have no query parameter. They reach a browse only if the route compiles
 *     them, registers them with the service and quotes the id it minted — three steps, none of them
 *     inside a component, and a suite over `predicates.ts` would only assert that a string builder
 *     builds a string.
 *   - And the address writer only works if the *router* is told. `window.history.replaceState` does
 *     not fire `popstate`, so the previous version wrote a URL that `useLocation()` never saw: the
 *     bar changed the address and the browse kept reading the range it was opened with. Nothing
 *     short of a real router in the test can catch that, which is why there is one here.
 *
 * So the seam under test is the route with a router around it and a fake transport underneath, and
 * the assertions are on the two things that leave the browser: the URL the address becomes, and the
 * requests the client is asked to make.
 */

import { beforeEach, describe, expect, test } from "vitest";
import { flush } from "solid-js";
import { createRouter, memoryHistory } from "@solidjs/router";
import { KuiProvider, clearToasts, toasts, type KuiContextValue, type KuiPaths } from "@kui/kernel";
import { Actions, type KuiApiClient } from "@kui/api";

import { mount } from "./testing.js";
import { presetsKey } from "./presets.js";
import { FILTER_DEBOUNCE_MS } from "./MessageFilterBar.jsx";
import Messages from "./MessagesRoute.jsx";

const CLUSTER = "quickstart";
/** Where the product is mounted. Every address this route writes has to keep it, exactly once. */
const BASE = "/ui";
const TOPIC = "orders.payments.v2";

/** Solid batches writes to a microtask; a router navigation takes a couple of them to settle. */
async function settle(times = 6): Promise<void> {
  for (let i = 0; i < times; i += 1) await flush();
}

/** Real time, for the boxes `MessageFilterBar` debounces before it commits and writes the address. */
async function pastDebounce(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, FILTER_DEBOUNCE_MS + 20));
  await settle();
}

interface Call {
  readonly method: "get" | "post";
  readonly path: string;
  readonly body?: unknown;
}

/**
 * A client that answers from a table and records what was asked.
 *
 * Cast at one boundary rather than implemented against the generated signatures: `KuiApiClient`'s
 * methods are typed from the OpenAPI document — the path constrains the parameters, the body and
 * the answer — and a fake that satisfied all of that would be a second copy of the schema. The cast
 * is the same one every stub client in this workspace makes, and the calls it records are compared
 * against the literal paths the product uses, so a renamed endpoint still shows up as a failure.
 */
function fakeApi(options: {
  readonly topicAnswer?: unknown;
  readonly topicFails?: boolean;
  readonly filterId?: string;
  readonly filterFails?: string;
  readonly filterReply?: Promise<{ readonly id: string }>;
  /** What `POST …/messages` answers with. Absent means the endpoint is not part of the case. */
  readonly produced?: readonly { readonly partition: number; readonly offset: number }[];
  readonly produceFails?: string;
  /** What `POST …/messages/resend` answers with: the server's own two figures. */
  readonly copied?: { readonly toTopic: string; readonly read: number; readonly written: number };
}): { readonly api: KuiApiClient; readonly calls: Call[] } {
  const calls: Call[] = [];
  const api = {
    url: (path: string) => path,
    get: async (path: string) => {
      calls.push({ method: "get", path });
      if (path === "/api/v1/clusters/{clusterId}/topics/{topicName}") {
        if (options.topicFails === true) {
          const cause = "the gateway is not answering";
          return { ok: false, error: { kind: "unreachable", cause } };
        }
        return { ok: true, value: options.topicAnswer };
      }
      return { ok: false, error: { kind: "unreachable", cause: "nothing answers that here" } };
    },
    post: async (path: string, init?: { readonly body?: unknown }) => {
      calls.push({ method: "post", path, body: init?.body });
      if (path === "/api/v1/clusters/{clusterId}/messages/filters") {
        if (options.filterFails !== undefined) {
          return {
            ok: false,
            error: {
              kind: "envelope",
              code: "KUI-UNSUPPORTED",
              message: options.filterFails,
              details: [],
              correlationId: "test",
              retryable: false,
            },
          };
        }
        return {
          ok: true,
          value:
            options.filterReply === undefined
              ? { id: options.filterId ?? "0123456789abcdef" }
              : await options.filterReply,
        };
      }
      if (path === "/api/v1/clusters/{clusterId}/topics/{topicName}/messages") {
        if (options.produceFails !== undefined) {
          return {
            ok: false,
            error: {
              kind: "envelope",
              code: "KUI-READ-ONLY",
              message: options.produceFails,
              details: [],
              correlationId: "test",
              retryable: false,
            },
          };
        }
        return {
          ok: true,
          value: {
            records: (options.produced ?? [{ partition: 3, offset: 4_812 }]).map((record) => ({
              ...record,
              timestamp: "2026-09-05T10:00:08Z",
            })),
          },
        };
      }
      if (path === "/api/v1/clusters/{clusterId}/topics/{topicName}/messages/resend") {
        return {
          ok: true,
          value: options.copied ?? { toTopic: "orders.replay", read: 3, written: 3 },
        };
      }
      return { ok: false, error: { kind: "unreachable", cause: "nothing answers that here" } };
    },
    put: async () => ({ ok: false, error: { kind: "unreachable", cause: "no" } }),
    delete: async () => ({ ok: false, error: { kind: "unreachable", cause: "no" } }),
    patch: async () => ({ ok: false, error: { kind: "unreachable", cause: "no" } }),
    raw: undefined,
  } as unknown as KuiApiClient;
  return { api, calls };
}

/** A topic answer carrying a real partition count, in the envelope the gateway sends. */
function topicWith(partitionCount: number, name: string = TOPIC): unknown {
  return {
    partitionsTruncated: false,
    topic: {
      status: "ok",
      fetchedAt: "2026-09-05T10:00:00Z",
      data: {
        row: {
          name,
          internal: false,
          partitionCount,
          replicationFactor: 3,
          outOfSyncReplicas: 0,
          offlinePartitions: 0,
        },
      },
    },
  };
}

/**
 * A topic answer whose section is `ok` and whose partition count is `null`.
 *
 * The case `topic.ts`'s most argued-for line exists for, and the one the suite never had: the
 * section succeeded, so the refused-section path does not cover it, and the count is present and
 * unreadable, so the happy path does not either. `?? 0` would satisfy the type and turn the server
 * saying "I could not read this" into the claim that the topic has no partitions.
 */
const TOPIC_COUNT_UNREADABLE: unknown = {
  partitionsTruncated: false,
  topic: {
    status: "ok",
    fetchedAt: "2026-09-05T10:00:00Z",
    data: { row: { name: "orders.uncounted", internal: false, partitionCount: null } },
  },
};

/** A topic answer the gateway could not fill in: the section refuses and carries no data. */
const TOPIC_UNAVAILABLE: unknown = {
  partitionsTruncated: false,
  topic: {
    status: "unavailable",
    reason: "upstream_unavailable",
    message: "the topic service is down",
  },
};

const PATHS: KuiPaths = {
  home: () => "/ui",
  settings: () => "/ui/settings",
  clusters: () => "/ui/clusters",
  manageClusters: () => "/ui/clusters/manage",
  dashboard: (cluster, tab) => `/ui/clusters/${cluster}/dashboard/${tab ?? "overview"}`,
  brokers: (cluster) => `/ui/clusters/${cluster}/brokers`,
  broker: (cluster, brokerId) => `/ui/clusters/${cluster}/brokers/${String(brokerId)}`,
  topics: (cluster) => `/ui/clusters/${cluster}/topics`,
  topic: (cluster, name) => `/ui/clusters/${cluster}/topics/${encodeURIComponent(name)}`,
  topicMessages: (cluster, name) =>
    `/ui/clusters/${cluster}/topics/${encodeURIComponent(name)}/messages`,
  trackMessages: (cluster) => `/ui/clusters/${cluster}/messages/track`,
  consumerGroups: (cluster) => `/ui/clusters/${cluster}/consumer-groups`,
  consumerGroup: (cluster, groupId) =>
    `/ui/clusters/${cluster}/consumer-groups/${encodeURIComponent(groupId)}`,
};

/**
 * Mounts the route inside a real router with an in-memory history.
 *
 * `memoryHistory` and not the browser's, because the browser's would write into the test runner's
 * own address bar and leak between cases — but it is the same adapter contract, so a navigation that
 * fails to notify the router fails here exactly as it does in a browser.
 */
function routeAt(
  search: string,
  api: KuiApiClient,
  /* Each case browses its own topic. `useQuery`'s registry is module state shared by the whole
   * process — which is the point of it, one answer per key for the whole tab — so two cases naming
   * one topic would have the second read the first's answer and assert against a count it never
   * asked for. A distinct name per case is the key being a key. */
  topic: string = TOPIC,
  /* Yes to everything unless a case says otherwise. It is a parameter because the route is where a
     permission becomes a control: `MessagesTab` renders the two write buttons disabled when it is
     told to, and that has a case — what decides whether it is told is `mayProduce()` and
     `mayResend()` here, and a harness that can only mount permitted cannot observe either. */
  permits: KuiContextValue["permits"] = () => true,
  messageBrowser?: KuiContextValue["messageBrowser"],
  writeBlocked?: KuiContextValue["writeBlocked"],
): { readonly container: HTMLElement; readonly dispose: () => void; readonly url: () => string; readonly switchCluster: (cluster: string) => void } {
  /* Mounted at `/ui`, as the product is, and this is not decoration. `navigate` resolves a `to`
   * that begins with `/` against the base, and `useLocation().pathname` already carries the base —
   * so a writer that hands the pathname back produces `/ui/ui/clusters/…`. With no base a test
   * cannot see that at all, which is how the first version of this suite passed against a route
   * that broke its own address in a browser on the first keystroke. */
  const start = `${BASE}/clusters/${CLUSTER}/topics/${topic}/messages${search}`;
  const history = memoryHistory(start);
  const Router = createRouter({
    routes: [{ path: "/clusters/:clusterId/topics/:topicName/messages", component: Messages }],
    base: BASE,
    history,
    scrollRestoration: false,
  });

  const value: KuiContextValue = {
    api,
    cluster: () => CLUSTER,
    permits,
    paths: PATHS,
    report: () => undefined,
    ...(messageBrowser === undefined ? {} : { messageBrowser }),
    ...(writeBlocked === undefined ? {} : { writeBlocked }),
  };

  const mounted = mount(() => (
    <KuiProvider value={value}>
      <Router />
    </KuiProvider>
  ));
  return { ...mounted, url: () => history.get(), switchCluster: (cluster) => history.set({ value: `${BASE}/clusters/${cluster}/topics/${topic}/messages` }) };
}

test("a same-topic cluster switch disposes the old stream and uses the configured API URL", async () => {
  const fake = fakeApi({ topicAnswer: topicWith(1) });
  const api = { ...fake.api, url: (path: string) => `https://gateway.example/custom${path}` };
  const urls: string[] = [];
  await withFetch((url) => urls.push(url), async () => {
    const ui = routeAt("", api, "cluster-switch");
    await settle(); press(ui.container, "Read"); await settle();
    ui.switchCluster("other"); await settle();
    press(ui.container, "Read"); await settle();
    expect(urls).toHaveLength(2);
    expect(urls[0]).toContain("https://gateway.example/custom/api/v1/clusters/quickstart/");
    expect(urls[1]).toContain("https://gateway.example/custom/api/v1/clusters/other/");
    ui.dispose();
  });
});

/**
 * The route builds its own transport, so the fake has to be installed where it builds it.
 *
 * `createBrowseTransport` reaches the network through `fetch`, and a test that let it do so would be
 * a test that hangs. Replacing `fetch` is the honest seam: everything above it — the URL the session
 * composes, the parameters on it, the abort — is the product's own code.
 */
async function withFetch(spy: (url: string) => void, run: () => Promise<void>): Promise<void> {
  const original = globalThis.fetch;
  globalThis.fetch = ((input: RequestInfo | URL) => {
    spy(typeof input === "string" ? input : input instanceof URL ? input.href : input.url);
    // Never resolves: the browse's own transport handles a stream that never arrives, and this test
    // is about the request, not the answer.
    return new Promise<Response>(() => undefined);
  }) as typeof globalThis.fetch;
  try {
    /* Awaited *inside* the try. The first version returned the promise and restored `fetch` in the
     * `finally`, which runs at the caller's first `await` — so the replacement was gone before the
     * transport ever reached for it, and every assertion about a request read zero. */
    await run();
  } finally {
    globalThis.fetch = original;
  }
}

function press(container: HTMLElement, label: string): void {
  const button = [...container.querySelectorAll("button")].find(
    (candidate) => (candidate.textContent ?? "").trim() === label,
  );
  if (button === undefined) throw new Error(`no button reading ${label}`);
  button.click();
}

/**
 * The overlay this case opened.
 *
 * `Dialog` and `Drawer` both render through a `Portal` into `document.body`, and Solid tears a
 * portal down on its own schedule rather than synchronously in `dispose()` — so a query across the
 * body can find the previous case's overlay. The last match is the one this case mounted.
 */
function overlay(): HTMLElement {
  const all = document.body.querySelectorAll<HTMLElement>("[role='dialog']");
  const last = all[all.length - 1];
  if (last === undefined) throw new Error("no dialog or drawer is open");
  return last;
}

function dialogText(): string {
  return overlay().textContent ?? "";
}

/** A button inside one root, by the words on it. Scoped: two overlays can offer the same label. */
function pressIn(root: HTMLElement, label: RegExp): void {
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
 * Types into the field whose label begins with these words, through a real `input` event.
 *
 * `TextField` reads `event.currentTarget.value` rather than being a controlled value the test can
 * poke, which is the same path a keystroke takes — so setting the value and dispatching the event
 * is the browser's own sequence and not a shortcut round the component.
 */
function type(root: HTMLElement, label: string, value: string): void {
  const field = [...root.querySelectorAll("label")].find((candidate) =>
    (candidate.textContent ?? "").trim().startsWith(label),
  );
  if (field === undefined) {
    throw new Error(
      `no label starting with ${label}; this root offers: ` +
        [...root.querySelectorAll("label")]
          .map((one) => (one.textContent ?? "").trim())
          .join(" | "),
    );
  }
  const id = field.getAttribute("for") ?? "";
  const input = root.querySelector<HTMLInputElement>(`#${CSS.escape(id)}`);
  if (input === null) throw new Error(`no field labelled ${label}`);
  input.value = value;
  input.dispatchEvent(new Event("input", { bubbles: true }));
}

const toastTitles = (): readonly string[] => toasts().map((toast) => toast.title);

/**
 * Fills the copy dialog in: a destination, one range, and the destination typed back to confirm it.
 *
 * A flush between every field, and it is not decoration. The dialog holds one draft and each field
 * writes `{ ...draft(), one: change }`; Solid 2 commits signal writes on a microtask, so two fields
 * changed inside one tick both compose their update from the same committed draft and the second
 * wins. A person typing cannot do that — there are frames between their keystrokes — but a test
 * that sets four values in a row can, and silently loses three of them.
 */
async function fillCopy(dialog: HTMLElement, destination: string): Promise<void> {
  type(dialog, "Copy into topic", destination);
  await settle();
  type(dialog, "From offset", "0");
  await settle();
  type(dialog, "Until offset", "3");
  await settle();
  // The label carries the destination once there is one, which is why this is typed last.
  type(dialog, `Type ${destination} to confirm`, destination);
  await settle();
}

describe("the topic context around the message browser", () => {
  test("keeps the topic name, health, breadcrumb and section navigation on screen", async () => {
    const { api } = fakeApi({ topicAnswer: topicWith(12) });
    const { container, dispose } = routeAt("", api);
    await settle();

    expect(container.querySelector("h1")?.textContent).toBe(TOPIC);
    expect(container.textContent).toContain("in sync");
    expect(container.querySelector("[aria-label='Breadcrumb']")?.textContent).toContain("Topics");
    expect(container.querySelectorAll(`[aria-label='Messages in ${TOPIC}']`)).toHaveLength(1);

    const tabs = container.querySelector("[aria-label='Topic sections']");
    expect(tabs).not.toBeNull();
    const base = `/ui/clusters/${CLUSTER}/topics/${encodeURIComponent(TOPIC)}`;
    expect(tabs?.querySelector("[data-testid='tab-overview']")?.getAttribute("href")).toBe(base);
    expect(tabs?.querySelector("[data-testid='tab-partitions']")?.getAttribute("href")).toBe(
      `${base}?tab=partitions`,
    );
    expect(tabs?.querySelector("[data-testid='tab-messages']")?.getAttribute("href")).toBe(
      `${base}/messages`,
    );
    expect(tabs?.querySelector("[data-testid='tab-consumers']")?.getAttribute("href")).toBe(
      `${base}?tab=consumers`,
    );
    expect(tabs?.querySelector("[data-testid='tab-settings']")?.getAttribute("href")).toBe(
      `${base}?tab=settings`,
    );
    expect(tabs?.querySelector("[aria-current='page']")?.textContent).toContain("Messages");

    dispose();
  });
});

describe("the partition count the route fetches", () => {
  test("the copy dialog quotes the topic's real partition count", async () => {
    const { api } = fakeApi({ topicAnswer: topicWith(12) });
    const { container, dispose } = routeAt("", api);
    await settle();

    press(container, "Copy records out");
    await settle();

    /* The figure, in the dialog, in words the operator reads. Twelve is the topic's, and it can only
     * have arrived by the route asking for the topic — nothing else in this test knows it. */
    expect(dialogText()).toContain(`${TOPIC} has 12 partitions.`);
    dispose();
  });

  test("a topic whose count is not known renders no sentence claiming one", async () => {
    const { api } = fakeApi({ topicAnswer: TOPIC_UNAVAILABLE });
    const { container, dispose } = routeAt("", api, "orders.undescribed");
    await settle();

    press(container, "Copy records out");
    await settle();

    const text = dialogText();
    expect(text).toContain("KUI has not been told how many partitions");
    /* The rule, stated as the thing that must not be on screen: no count at all, and above all not
     * the zero this route supplied for the whole life of the dialog. */
    expect(text).not.toMatch(/has \d+ partitions/);
    dispose();
  });

  test("a section answering with a null count draws the sentence, never a zero", async () => {
    /*
     * Not the same case as `TOPIC_UNAVAILABLE` above, and that is the point. There the section
     * refused and carried no data at all; here it succeeded and the *figure* inside it is null —
     * the server saying it could not read the count for a topic it otherwise described. The three
     * children below all draw from one `undefined`, so the assertion is on what reaches the screen.
     */
    const { api } = fakeApi({ topicAnswer: TOPIC_COUNT_UNREADABLE });
    const { container, dispose } = routeAt("", api, "orders.uncounted");
    await settle();

    press(container, "Copy records out");
    await settle();

    const text = dialogText();
    expect(text).toContain("KUI has not been told how many partitions");
    // A topic cannot have zero partitions, so `has 0 partitions` is a claim that is both impossible
    // and reassuring — which is the pairing this whole screen exists to keep off the page.
    expect(text).not.toMatch(/has \d+ partitions?\./);

    dispose();
  });

  test("the partition picker says so in words rather than offering `all 0`", async () => {
    const { api } = fakeApi({ topicFails: true });
    const { container, dispose } = routeAt("", api, "orders.unreachable");
    await settle();

    const trigger = container.querySelector<HTMLButtonElement>(".kui-partition-picker__trigger");
    expect(trigger?.textContent).toContain("all partitions");
    expect(trigger?.textContent).not.toContain("all 0");
    expect(trigger?.disabled).toBe(true);
    dispose();
  });
});

describe("the typed predicates", () => {
  test("changing the controls while registration is pending does not start the stale browse", async () => {
    let finishRegistration: ((value: { readonly id: string }) => void) | undefined;
    const filterReply = new Promise<{ readonly id: string }>((resolve) => {
      finishRegistration = resolve;
    });
    const { api } = fakeApi({
      topicAnswer: topicWith(12, "orders.registration-race"),
      filterReply,
    });
    const opened: string[] = [];

    await withFetch(
      (url) => opened.push(url),
      async () => {
        const { container, dispose } = routeAt(
          "?value=before&valueMode=contains",
          api,
          "orders.registration-race",
        );
        await settle();

        press(container, "Read");
        await settle();

        const valueBox = container.querySelector<HTMLInputElement>(
          ".kui-browse-bar__predicate:nth-of-type(2) input",
        );
        expect(valueBox).not.toBeNull();
        if (valueBox !== null) {
          valueBox.value = "after";
          valueBox.dispatchEvent(new Event("input", { bubbles: true }));
        }
        await pastDebounce();

        finishRegistration?.({ id: "abc0123456789def" });
        await settle();

        expect(opened).toEqual([]);
        dispose();
      },
    );
  });

  test("a key predicate and a value predicate reach the request separately", async () => {
    const { api, calls } = fakeApi({
      topicAnswer: topicWith(12, "orders.predicates"),
      filterId: "abc0123456789def",
    });
    const opened: string[] = [];

    const search = "?key=ord_&keyMode=starts&value=UAH&valueMode=contains";
    await withFetch(
      (url) => opened.push(url),
      async () => {
        const { container, dispose } = routeAt(search, api, "orders.predicates");
        await settle();

        press(container, "Read");
        await settle();
        dispose();
      },
    );

    /* One registration, carrying both predicates as two terms over two variables. This is the
     * assertion the packet's brief names: the key's predicate is about the key and the value's is
     * about the value, and neither has been folded into the other or into the plain `q` substring,
     * which matches the whole record and could not tell them apart. */
    const registration = calls.find(
      (call) => call.path === "/api/v1/clusters/{clusterId}/messages/filters",
    );
    expect(registration).toBeDefined();
    const source = (registration?.body as { readonly source?: string } | undefined)?.source ?? "";
    expect(source).toContain('record.keyAsText.startsWith("ord_")');
    expect(source).toContain('record.valueAsText.contains("UAH")');
    expect(source).toBe('record.keyAsText.startsWith("ord_") && record.valueAsText.contains("UAH")');

    /* And it reached the browse. The id the service minted travels with the source it was minted
     * from — a browse carrying only the id is refused by a replica that never saw the registration,
     * and one carrying only the source is silently ignored by every replica. */
    expect(opened).toHaveLength(1);
    const url = new URL(opened[0] ?? "", "http://localhost");
    expect(url.searchParams.get("filterId")).toBe("abc0123456789def");
    expect(url.searchParams.get("filterSource")).toContain("record.keyAsText");
    expect(url.searchParams.get("filterSource")).toContain("record.valueAsText");
  });

  test("typing them writes an address a colleague can be sent", async () => {
    const { api } = fakeApi({ topicAnswer: topicWith(12, "orders.typed") });
    const { container, dispose, url } = routeAt("", api, "orders.typed");
    await settle();

    const boxes = [
      ...container.querySelectorAll<HTMLInputElement>(".kui-browse-bar__predicate input"),
    ];
    expect(boxes).toHaveLength(2);

    /* The address is the product's memory of what the controls hold, and it is written through the
     * router. The version that wrote it with `window.history.replaceState` passed a component test
     * and did nothing at all here: the URL changed and `useLocation()` never heard about it, so the
     * browse below went on reading whatever the page was opened with. */
    boxes[0]?.dispatchEvent(new Event("input", { bubbles: true }));
    if (boxes[0] !== undefined) boxes[0].value = "ord_";
    boxes[0]?.dispatchEvent(new Event("input", { bubbles: true }));
    await pastDebounce();
    if (boxes[1] !== undefined) boxes[1].value = "UAH";
    boxes[1]?.dispatchEvent(new Event("input", { bubbles: true }));
    await pastDebounce();

    const written = new URL(url(), "http://localhost");
    expect(written.searchParams.get("key")).toBe("ord_");
    expect(written.searchParams.get("value")).toBe("UAH");
    // The path is the one it started on. A writer that hands `location.pathname` back to `navigate`
    // doubles the mount point, and the address stops naming a route the next time it is opened.
    expect(written.pathname).toBe(`${BASE}/clusters/${CLUSTER}/topics/orders.typed/messages`);
    dispose();
  });

  test("a time-window chip sets the start and drops the window's end in one address", async () => {
    // Two halves of one change. They are written together because `query()` and `predicates()` read
    // the *location*, which the router updates on its own schedule — so a second write composed
    // from the location would be composed from the state before the first, and would land an
    // address holding a start with the stale upper bound still on it.
    const { api } = fakeApi({ topicAnswer: topicWith(12, "orders.window") });
    const { container, dispose, url } = routeAt(
      "?seekTo=timestamp%3A%3A1000&untilTime=2000",
      api,
      "orders.window",
    );
    await settle();

    const chip = [...container.querySelectorAll<HTMLButtonElement>(".kui-fchip")].find(
      (candidate) => (candidate.textContent ?? "").trim() === "15m",
    );
    expect(chip).toBeDefined();
    chip?.click();
    await settle();

    const written = new URL(url(), "http://localhost");
    expect(written.searchParams.get("untilTime")).toBeNull();
    const seek = written.searchParams.get("seekTo") ?? "";
    expect(seek.startsWith("timestamp::")).toBe(true);
    expect(seek).not.toBe("timestamp::1000");
    dispose();
  });

  test("an upper bound on the offset is a term about the offset, not a second start", async () => {
    const { api, calls } = fakeApi({ topicAnswer: topicWith(12, "orders.bounded") });
    await withFetch(
      () => undefined,
      async () => {
        const { container, dispose } = routeAt(
          "?seekTo=offset%3A%3A100&untilOffset=200",
          api,
          "orders.bounded",
        );
        await settle();
        press(container, "Read");
        await settle();
        dispose();
      },
    );

    const registration = calls.find(
      (call) => call.path === "/api/v1/clusters/{clusterId}/messages/filters",
    );
    const source = (registration?.body as { readonly source?: string } | undefined)?.source ?? "";
    /* The endpoint has a start and no stop, so the end of a range can only be a predicate. Sending
     * it as a second `seekTo` would be refused; sending it as nothing at all — which is what the bar
     * did before there was a box for it — is a range whose upper half is decoration. */
    expect(source).toBe("record.offset <= 200");
  });

  test("a cluster that will not compile the filter stops the browse and says why", async () => {
    const { api } = fakeApi({
      topicAnswer: topicWith(12, "orders.nofilter"),
      filterFails: "cluster 'quickstart' has no filter engine, so a smart filter cannot be run",
    });
    const opened: string[] = [];

    await withFetch(
      (url) => opened.push(url),
      async () => {
        const { container, dispose } = routeAt("?key=ord_", api, "orders.nofilter");
        await settle();
        press(container, "Read");
        await settle();

        /* Not a browse that quietly returns the whole topic. `filterSource` without a `filterId` is
         * dropped by the message service, so starting anyway would show every record on the topic
         * under a bar that says a filter is applied. */
        expect(opened).toHaveLength(0);
        expect(container.textContent).toContain("no filter engine");
        dispose();
      },
    );
  });

  test("a browse with nothing to compile registers nothing", async () => {
    const { api, calls } = fakeApi({ topicAnswer: topicWith(12, "orders.plain") });
    const opened: string[] = [];
    await withFetch(
      (url) => opened.push(url),
      async () => {
        const { container, dispose } = routeAt("", api, "orders.plain");
        await settle();
        press(container, "Read");
        await settle();
        dispose();
      },
    );

    // The common browse costs one request, as it did before any of this existed.
    expect(calls.some((call) => call.path.endsWith("/messages/filters"))).toBe(false);
    expect(opened).toHaveLength(1);
  });

  test("applies the configured page size when the URL does not name one", async () => {
    const { api } = fakeApi({ topicAnswer: topicWith(12, "orders.default-size") });
    const opened: string[] = [];
    await withFetch(
      (url) => opened.push(url),
      async () => {
        const { container, dispose } = routeAt(
          "",
          api,
          "orders.default-size",
          () => true,
          { pageSize: () => 250, mode: () => "pages" },
        );
        await settle();
        press(container, "Read");
        await settle();
        dispose();
      },
    );

    expect(new URL(opened[0] ?? "", "http://localhost").searchParams.get("limit")).toBe("250");
  });

  test("keeps an explicit URL page size and uses the configured loading mode", async () => {
    const { api } = fakeApi({ topicAnswer: topicWith(12, "orders.explicit-size") });
    const opened: string[] = [];
    await withFetch(
      (url) => opened.push(url),
      async () => {
        const { container, dispose } = routeAt(
          "?limit=25",
          api,
          "orders.explicit-size",
          () => true,
          { pageSize: () => 250, mode: () => "infinite" },
        );
        await settle();

        expect(
          container.querySelector<HTMLInputElement>('input[value="infinite"]')?.checked,
        ).toBe(true);
        press(container, "Read");
        await settle();
        dispose();
      },
    );

    expect(new URL(opened[0] ?? "", "http://localhost").searchParams.get("limit")).toBe("25");
  });
});

/**
 * The toasts, at the route, because the route is the only place a mutation's *answer* is seen.
 *
 * "A toast on every destructive success" is an M6 bullet and, until this block, it was a rule that
 * could be deleted from this package without anything noticing: suppressing the produce `notify`
 * left 142 tests green, and so did suppressing the resend one — including the deliberate
 * `tone: "warning"` for a copy that moved nothing, which is the most careful line in the file and
 * the one with the least behind it.
 *
 * The assertions are on `toasts()`, the kernel's store, which is what the shell's `ToastRegion`
 * draws from. This package renders no toast of its own, so asserting against markup here would
 * assert nothing; mounting the region beside the route would assert that the kernel can draw a
 * toast it was handed, which is the kernel's own suite's business.
 *
 * Every raise has its refusal beside it. A route that raised a toast from the click rather than
 * from the answer passes the first case of each pair and fails the second, and that is the whole
 * difference between a confirmation and a decoration.
 */
describe("the toasts a write raises", () => {
  beforeEach(() => {
    // Module-level state shared by the whole process: one case's confirmation is otherwise the next
    // case's evidence.
    clearToasts();
  });

  test("a produce that lands raises a toast quoting where it landed", async () => {
    const { api } = fakeApi({
      topicAnswer: topicWith(12, "orders.produced"),
      produced: [{ partition: 3, offset: 4_812 }],
    });
    const { container, dispose } = routeAt("", api, "orders.produced");
    await settle();

    press(container, "Produce message");
    await settle();
    pressIn(overlay(), /^Produce record$/);
    await settle();

    /*
     * A position, not the word "sent". The drawer shows the same receipt and the drawer is
     * dismissible; the toast is what an operator has afterwards to say the write happened, and a
     * position is the one thing in it they can go and look at.
     */
    expect(toastTitles()).toContain("Record published");
    const raised = toasts().find((toast) => toast.title === "Record published");
    expect(raised?.message).toContain("partition 3");
    expect(raised?.message).toContain("offset 4812");
    expect(raised?.tone).toBe("success");

    dispose();
  });

  test("a tombstone is sent as an absent value, not as an empty one", async () => {
    /*
     * Not a toast rule, and it is here because this is where the whole path is: the drawer's switch
     * writes `null` into the draft and `produce.ts` **omits** the field, and it is the omission the
     * server reads as a tombstone. `value: draft.value ?? ""` satisfies every type in that path and
     * turns "delete this key, permanently, on a compacted topic" into an ordinary record with no
     * characters in it. Nothing in this package could tell the two apart before this case.
     */
    const { api, calls } = fakeApi({ topicAnswer: topicWith(12, "orders.tombstoned") });
    const { container, dispose } = routeAt("", api, "orders.tombstoned");
    await settle();

    press(container, "Produce message");
    await settle();
    const drawer = overlay();
    const tombstone = drawer.querySelector<HTMLInputElement>("input[role='switch']");
    expect(tombstone).not.toBeNull();
    tombstone?.click();
    await settle();

    // The button's own words change with the draft, which is the operator's confirmation that this
    // is no longer a write. Pressed by that name, so the case cannot pass against an unflipped one.
    pressIn(overlay(), /^Produce tombstone$/);
    await settle();

    const write = calls.find(
      (call) => call.path === "/api/v1/clusters/{clusterId}/topics/{topicName}/messages",
    );
    expect(write).toBeDefined();
    const body = (write?.body ?? {}) as Record<string, unknown>;
    // Absent, not `null` and not `""`. The contract's own sentence: an absent value is a tombstone.
    expect(Object.keys(body)).not.toContain("value");

    dispose();
  });

  test("a produce the cluster refuses raises none", async () => {
    const { api } = fakeApi({
      topicAnswer: topicWith(12, "orders.readonly"),
      produceFails: "cluster 'quickstart' is read-only, so nothing may be published to it",
    });
    const { container, dispose } = routeAt("", api, "orders.readonly");
    await settle();

    press(container, "Produce message");
    await settle();
    pressIn(overlay(), /^Produce record$/);
    await settle();

    expect(toastTitles()).toEqual([]);
    // And the refusal is in the drawer, in the server's own words, where the operator is looking.
    expect(dialogText()).toContain("read-only");

    dispose();
  });

  test("a copy that moved records raises a success toast carrying both figures", async () => {
    const { api } = fakeApi({
      topicAnswer: topicWith(12, "orders.copied"),
      copied: { toTopic: "orders.replay", read: 3, written: 3 },
    });
    const { container, dispose } = routeAt("", api, "orders.copied");
    await settle();

    press(container, "Copy records out");
    await settle();
    const dialog = overlay();
    await fillCopy(dialog, "orders.replay");
    pressIn(dialog, /^Copy records$/);
    await settle();

    expect(toastTitles()).toContain("Records copied");
    const raised = toasts().find((toast) => toast.title === "Records copied");
    expect(raised?.tone).toBe("success");
    // `written` of `read`, both of them: the pair is the fact, and one number alone cannot say that
    // retention removed part of the source underneath the copy.
    expect(raised?.message).toContain("3 of 3");
    expect(raised?.message).toContain("orders.replay");

    dispose();
  });

  test("a copy whose source was partly eaten by retention reports both figures and they differ",
    async () => {
    /*
     * The case the rule exists for, and the one this suite did not have.
     *
     * `MessagesRoute` composes the toast as `written of read`, and its comment says why: the
     * request said how many records to try for, the answer says how many arrived, and on a range
     * retention has already eaten those are different numbers. Every other copy case in this
     * package — here and in `dialogs.test.tsx`'s route-free ones — uses a fixture where `read` and
     * `written` are equal, so the two figures are interchangeable and the template could report
     * either one. With `read: 12, written: 3` they are not: reporting `read` twice tells the
     * operator twelve records reached the destination when three did, which is the copy stating
     * the intention it was given rather than the outcome the server measured.
     */
    const { api } = fakeApi({
      topicAnswer: topicWith(12, "orders.eaten"),
      copied: { toTopic: "orders.replay", read: 12, written: 3 },
    });
    const { container, dispose } = routeAt("", api, "orders.eaten");
    await settle();

    press(container, "Copy records out");
    await settle();
    const dialog = overlay();
    await fillCopy(dialog, "orders.replay");
    pressIn(dialog, /^Copy records$/);
    await settle();

    const raised = toasts().find((toast) => toast.title === "Records copied");
    expect(raised).toBeDefined();
    // Written first, read second, and the two are not the same number.
    expect(raised?.message).toContain("3 of 12");
    // The two ways the pair collapses into a single figure. `12 of 12` is `read` in the written
    // slot — the mutation this case is written against — and `3 of 3` is the request's own figure
    // standing in for what the log still held.
    expect(raised?.message).not.toContain("12 of 12");
    expect(raised?.message).not.toContain("3 of 3");
    // Still a success and not a warning: three records did move, and the operator has a
    // destination to go and look at. The shortfall is in the figures, which is where they are.
    expect(raised?.tone).toBe("success");

    dispose();
  });

  test("a copy that moved nothing raises a warning toast, never a success one", async () => {
    /*
     * The state this whole screen is shaped around: a range whose offsets retention has already
     * removed answers **200** with `read: 0, written: 0` — no error, no warning, nothing. A green
     * tick over that sends an operator to look at a destination they believe now holds their
     * records. `tone` is the assertion, not the wording: the wording is what a reader skims and the
     * tone is what they see from across the room.
     */
    const { api } = fakeApi({
      topicAnswer: topicWith(12, "orders.emptied"),
      copied: { toTopic: "orders.replay", read: 0, written: 0 },
    });
    const { container, dispose } = routeAt("", api, "orders.emptied");
    await settle();

    press(container, "Copy records out");
    await settle();
    const dialog = overlay();
    await fillCopy(dialog, "orders.replay");
    pressIn(dialog, /^Copy records$/);
    await settle();

    expect(toastTitles()).toContain("Nothing was copied");
    const raised = toasts().find((toast) => toast.title === "Nothing was copied");
    expect(raised?.tone).toBe("warning");
    expect(toastTitles()).not.toContain("Records copied");
    // `written`, not `requested`: the request said how many to try for and the answer says how many
    // arrived. Reporting the first would be reporting the intention.
    expect(raised?.message).toContain("0 of 0");

    dispose();
  });
});

describe("saving what is on the filter bar", () => {
  beforeEach(() => {
    clearToasts();
    // A preset store is per browser and per cluster, and it survives between cases in one process.
    // Left in place, the second run of this file would find the first run's chip already there.
    try {
      window.localStorage.removeItem(presetsKey(CLUSTER));
    } catch {
      /* A store that refuses is one of the states `presets.ts` is written to survive. */
    }
  });

  test("naming a preset confirms it was saved, and says where it was saved", async () => {
    /*
     * The sixth `notify` in these two packages and the only one nothing asserted.
     *
     * A preset is written into this browser's `localStorage` and nowhere else: it does not reach
     * the cluster, it is not visible to a colleague, and it is gone with the browser profile. The
     * chip appearing on the bar says a preset exists; it does not say *that* — and the sentence
     * this raises is the only place the product tells anybody, which is why it is asserted here
     * rather than left as a toast that could be deleted with 150 cases still green.
     */
    const asked: string[] = [];
    const original = window.prompt;
    // jsdom implements `prompt` as a stub that returns `null` and logs "not implemented", so a
    // case that did not replace it would exercise the cancel path and never reach the notify.
    window.prompt = (message?: string) => {
      asked.push(message ?? "");
      return "Big tickets";
    };

    try {
      const { api } = fakeApi({ topicAnswer: topicWith(12, "orders.presets") });
      // A predicate on the address, because the control is offered only when there is something to
      // save: a "save as preset" over the empty arrangement would be a chip named after nothing.
      const { container, dispose } = routeAt("?key=ord_", api, "orders.presets");
      await settle();

      press(container, "Save as preset");
      await settle();

      expect(asked).toEqual(["Name this filter"]);
      expect(toastTitles()).toContain("Filter saved");
      const raised = toasts().find((toast) => toast.title === "Filter saved");
      expect(raised?.message).toContain("Big tickets");
      // The half a chip cannot say. A preset that is only in this browser and reads as though it
      // were saved on the cluster is a colleague being sent a link that shows them nothing.
      expect(raised?.message).toContain("this browser only");
      expect(raised?.tone).toBe("info");

      // And it is on the bar afterwards, so the toast is a confirmation of something that happened
      // rather than a sentence raised beside a write that did not.
      expect(container.textContent).toContain("Big tickets");

      dispose();
    } finally {
      window.prompt = original;
    }
  });

  test("cancelling the prompt saves nothing and says nothing", async () => {
    const original = window.prompt;
    // What an operator who changes their mind gets. An empty answer means "cancel", and a toast
    // reading "Filter saved" over a preset nobody named is worse than silence.
    window.prompt = () => null;

    try {
      const { api } = fakeApi({ topicAnswer: topicWith(12, "orders.unsaved") });
      const { container, dispose } = routeAt("?key=ord_", api, "orders.unsaved");
      await settle();

      press(container, "Save as preset");
      await settle();

      expect(toastTitles()).toEqual([]);
      dispose();
    } finally {
      window.prompt = original;
    }
  });
});

/**
 * The permission wiring, which is a different thing from the rendering it drives.
 *
 * `MessagesTab` draws a write control disabled with a reason when it is handed `mayProduce={false}`
 * or `mayResend={false}`, and `messages.test.tsx` covers that. What decides which of those it is
 * handed is two accessors in `MessagesRoute`, and until this block every case in this package
 * mounted the route with a `permits` that said yes to everything — so both accessors could be
 * replaced with the constant `true` and all 158 cases stayed green, while an account holding no
 * write permission at all was shown two live buttons over a topic it may not touch.
 *
 * Three arrangements, because the two controls are gated on different actions and a single boolean
 * would satisfy any two of them. `Copy records out` reads this topic and writes another, so it
 * needs both permissions; `Produce message` needs only the write. An account trusted to publish but
 * not to read is therefore offered produce and refused resend, and that is the arrangement no
 * single flag can produce.
 */
describe("the write controls the route offers", () => {
  test("copy reads the source and checks produce only on the selected destination", async () => {
    const { api, calls } = fakeApi({ topicAnswer: topicWith(1) });
    const ui = routeAt("", api, "source-only", (action, name) =>
      action === Actions.TopicMessagesProduce ? name === "allowed-destination" : true);
    await settle();
    expect(control(ui.container, "Copy records out").getAttribute("aria-disabled")).toBeNull();
    press(ui.container, "Copy records out"); await settle();
    await fillCopy(overlay(), "denied-destination");
    pressIn(overlay(), /^Copy records$/); await settle();
    expect(calls.filter((call) => call.path.endsWith("/resend"))).toHaveLength(0);
    await fillCopy(overlay(), "allowed-destination");
    pressIn(overlay(), /^Copy records$/); await settle();
    expect(calls.filter((call) => call.path.endsWith("/resend"))).toHaveLength(1);
    ui.dispose();
  });

  test("shared write policy blocks produce even when legacy permissions allow it", async () => {
    const { api } = fakeApi({ topicAnswer: topicWith(1) });
    const ui = routeAt("", api, "policy-test", () => true, undefined, () => "This cluster is read-only.");
    await settle();
    expect(control(ui.container, "Produce message").getAttribute("aria-disabled")).toBe("true");
    expect(await reasonUnder(control(ui.container, "Produce message"))).toContain("read-only");
    ui.dispose();
  });
  /**
   * Yes to everything except these, compared field by field rather than by object identity.
   *
   * ## `exceptOn`, and why this helper had to grow it
   *
   * For two waves this compared `{resource, action}` and **threw the `name` away**, so it answered
   * the same thing to `permits(action)` and to `permits(action, "orders.payments")`. That made the
   * subject in `MessagesRoute`'s `mayProduce` and `mayResend` unobservable: pointing either call at
   * a topic nobody is looking at left all 165 cases in this package green. It is the unfixed twin
   * of the helper `feature-consumers/src/groupRoute.test.tsx` repaired in wave 7, one directory
   * over, and it is repaired here the same way.
   *
   * A grant carries a *pattern*, so `exceptOn` is the principal one produces: denied the action
   * everywhere except on the topic it names, and **yes** when nobody names a topic at all, because
   * they do hold it on something. `kernel/src/state/session.ts` is explicit that the subjectless
   * form is *"the right answer for a list heading and the wrong one for a row's delete button"*.
   */
  function permitsAllBut(
    ...denied: readonly {
      readonly resource: string;
      readonly action: string;
      readonly exceptOn?: string;
    }[]
  ): KuiContextValue["permits"] {
    return (asked, name) => {
      const rule = denied.find(
        (one) => one.resource === asked.resource && one.action === asked.action,
      );
      if (rule === undefined) return true;
      if (rule.exceptOn === undefined) return false;
      // The weaker question, answered the way the server answers it: they hold this action on
      // something, and only a named subject can turn that into a refusal.
      return name === undefined || name === rule.exceptOn;
    };
  }

  /** Every message action, granted on one topic's pattern and on nothing else. */
  function grantedOnlyOn(topic: string): KuiContextValue["permits"] {
    return permitsAllBut(
      { ...Actions.TopicMessagesProduce, exceptOn: topic },
      { ...Actions.TopicMessagesRead, exceptOn: topic },
    );
  }

  function control(container: HTMLElement, label: string): HTMLButtonElement {
    const button = [...container.querySelectorAll("button")].find(
      (candidate) => (candidate.textContent ?? "").trim() === label,
    );
    if (button === undefined) throw new Error(`no button reading ${label}`);
    return button;
  }

  /**
   * The reason under one disabled control, read through that control's own `aria-describedby`.
   *
   * Not `document.body.querySelector('[role="tooltip"]')`: the bubble is a portal into `body` and
   * an earlier case's can still be there, so the first match is not necessarily this button's — a
   * reading that made two of these cases assert a sentence from the case before them.
   */
  async function reasonUnder(button: HTMLButtonElement): Promise<string> {
    button.dispatchEvent(new FocusEvent("focusin", { bubbles: true }));
    await settle();
    const id = button.getAttribute("aria-describedby") ?? "";
    const bubble = id === "" ? null : document.getElementById(id);
    if (bubble === null) throw new Error(`no tooltip is attached to ${button.textContent ?? ""}`);
    return bubble.textContent ?? "";
  }

  test("offers both write controls to a principal holding both permissions", async () => {
    // The capability working, beside the two refusals below. A screen that disabled everything for
    // everybody would satisfy them and be exactly as wrong.
    const { api } = fakeApi({ topicAnswer: topicWith(12, "orders.permitted") });
    const { container, dispose } = routeAt("", api, "orders.permitted");
    await settle();

    expect(control(container, "Produce message").getAttribute("aria-disabled")).toBeNull();
    expect(control(container, "Copy records out").getAttribute("aria-disabled")).toBeNull();

    dispose();
  });

  test("refuses produce but permits choosing a copy destination for a source reader", async () => {
    const { api } = fakeApi({ topicAnswer: topicWith(12, "orders.unwritable") });
    const { container, dispose } = routeAt(
      "",
      api,
      "orders.unwritable",
      permitsAllBut(Actions.TopicMessagesProduce),
    );
    await settle();

    /* Disabled and present, not absent: a control that vanishes teaches an operator that KUI cannot
       publish at all, when the truth is that this account may not. */
    const produce = control(container, "Produce message");
    expect(produce.getAttribute("aria-disabled")).toBe("true");
    expect(await reasonUnder(produce)).toContain(
      "You do not have permission to publish into this topic.",
    );

    // Destination permission is not knowable until the operator selects it.
    expect(control(container, "Copy records out").getAttribute("aria-disabled")).toBeNull();

    dispose();
  });

  /**
   * The track screen, which is the same feature at a different address and had the same hole.
   *
   * `/messages/track` names no topic — a track reads across them — so it is a different branch of
   * this route's root and no case in this package had ever mounted it. Its Search button is
   * disabled for two quite different reasons, and that is why the query has to be filled in first:
   * an empty form disables Search because the form is incomplete, which would make a permission
   * assertion pass over a gate that had been deleted.
   */
  function trackAt(api: KuiApiClient, permits: KuiContextValue["permits"] = () => true): {
    readonly container: HTMLElement;
    readonly dispose: () => void;
  } {
    const history = memoryHistory(`${BASE}/clusters/${CLUSTER}/messages/track`);
    const Router = createRouter({
      routes: [{ path: "/clusters/:clusterId/messages/track", component: Messages }],
      base: BASE,
      history,
      scrollRestoration: false,
    });
    const value: KuiContextValue = {
      api,
      cluster: () => CLUSTER,
      permits,
      paths: PATHS,
      report: () => undefined,
    };
    return mount(() => (
      <KuiProvider value={value}>
        <Router />
      </KuiProvider>
    ));
  }

  /** A complete track query, so that Search is disabled by permission and by nothing else. */
  async function fillTrack(container: HTMLElement): Promise<void> {
    /* Settled between the two, because `patch` spreads the query it was last **rendered** with:
       two keystrokes in one turn read the same stale object and the second discards the first. */
    type(container, "Topics", "orders.v1, orders.payments.v2");
    await settle();
    type(container, "Value", "order-4711");
    await settle();
  }

  test("offers the track search to a principal who may read messages", async () => {
    const { api } = fakeApi({ topicAnswer: topicWith(12, "orders.trackable") });
    const { container, dispose } = trackAt(api);
    await settle();
    await fillTrack(container);

    expect(control(container, "Search").getAttribute("aria-disabled")).toBeNull();

    dispose();
  });

  test("refuses the track search to a principal who may not read messages", async () => {
    const { api } = fakeApi({ topicAnswer: topicWith(12, "orders.untrackable") });
    const { container, dispose } = trackAt(api, permitsAllBut(Actions.TopicMessagesRead));
    await settle();
    await fillTrack(container);

    const search = control(container, "Search");
    expect(search.getAttribute("aria-disabled")).toBe("true");
    // The permission sentence, and not "This search is not complete." — the form is complete, and
    // telling an operator to finish a filled-in form sends them looking for a typo that is not
    // there while the real answer is that this account may not read.
    expect(await reasonUnder(search)).toContain(
      "You do not have permission to read messages on this cluster.",
    );

    dispose();
  });

  test("refuses only the copy to a principal who may produce but not read", async () => {
    /*
     * The arrangement that separates the two gates. A single boolean over both controls agrees with
     * the two cases above and disagrees here — which is the shape wave 5's topic-detail packet
     * named as its own worst finding, one screen over.
     */
    const { api } = fakeApi({ topicAnswer: topicWith(12, "orders.writeonly") });
    const { container, dispose } = routeAt(
      "",
      api,
      "orders.writeonly",
      permitsAllBut(Actions.TopicMessagesRead),
    );
    await settle();

    expect(control(container, "Produce message").getAttribute("aria-disabled")).toBeNull();

    const resend = control(container, "Copy records out");
    expect(resend.getAttribute("aria-disabled")).toBe("true");
    expect(await reasonUnder(resend)).toContain(
      "You do not have permission to read this topic and publish into another one.",
    );

    dispose();
  });

  /**
   * The subject, which is what every case above is blind to.
   *
   * Every case above hands `permitsAllBut` an entry with no `exceptOn`, which refuses the action
   * whatever subject is named — so a route asking the subjectless question and a route asking the
   * named one are indistinguishable to all of them, and both `mayProduce` and `mayResend` asked the
   * subjectless one until wave 8. The principal here is the one a pattern grant makes:
   * `TOPIC:MESSAGES_*` on some other topic, which answers **yes** to "do they hold this action on
   * anything" and **no** about this page's topic.
   *
   * Both halves, because one alone cannot separate a route that names the topic from a route that
   * answers `false` to everybody. The track form's positive half is the case four above it, which
   * already mounts a principal permitted everywhere.
   */
  test("the topic's write controls ask about this topic and not about the cluster", async () => {
    const { api } = fakeApi({ topicAnswer: topicWith(12, "orders.subject") });
    const { container, dispose } = routeAt(
      "",
      api,
      "orders.subject",
      grantedOnlyOn("analytics.pageviews"),
    );
    await settle();

    const produce = control(container, "Produce message");
    expect(produce.getAttribute("aria-disabled")).toBe("true");
    expect(await reasonUnder(produce)).toContain(
      "You do not have permission to publish into this topic.",
    );
    expect(control(container, "Copy records out").getAttribute("aria-disabled")).toBe("true");

    dispose();
  });

  test("and offers them when the grant is on this topic", async () => {
    const { api } = fakeApi({ topicAnswer: topicWith(12, "orders.granted") });
    const granted = grantedOnlyOn("orders.granted");
    const { container, dispose } = routeAt("", api, "orders.granted", granted);
    await settle();

    expect(control(container, "Produce message").getAttribute("aria-disabled")).toBeNull();
    expect(control(container, "Copy records out").getAttribute("aria-disabled")).toBeNull();

    dispose();
  });

  /**
   * The track form, where the subject is whatever the reader typed.
   *
   * A track is a full read of every topic it names, so a grant covering one of two is not enough —
   * and the subjectless question cannot see the shortfall at all. The form below names
   * `orders.v1` and `orders.payments.v2`; the grant covers the first, and Search must still close,
   * naming the topic it closed on so the reader is not sent to ask for a permission they hold.
   */
  test("the track search refuses a topic on the form the principal may not read", async () => {
    const { api } = fakeApi({ topicAnswer: topicWith(12, "orders.halftracked") });
    const { container, dispose } = trackAt(api, grantedOnlyOn("orders.v1"));
    await settle();
    await fillTrack(container);

    const search = control(container, "Search");
    expect(search.getAttribute("aria-disabled")).toBe("true");
    expect(await reasonUnder(search)).toContain(
      "You do not have permission to read messages on orders.payments.v2.",
    );

    dispose();
  });
});
