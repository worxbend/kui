/**
 * The topics feature mounted at an address, over a stubbed gateway.
 *
 * ## Why the tests go through this and not through the components
 *
 * Every interesting rule on these screens lives in the *wiring*, not in a component's props: which
 * document the statistics tiles are given, whether the Overview tab has a body at all, whether the
 * table and the cards are handed the same selection. A case that composes `TopicListPage` by hand
 * and passes it a `statistics` element has asserted the arrangement the test itself made — it
 * cannot fail when `TopicsRoute` starts computing those totals from the rows on screen, which is
 * precisely the defect worth a test.
 *
 * So this builds the real route over a real router and a real `KuiProvider`, and the only thing
 * that is not real is the gateway. `useParams`, `useLocation`, `useQuery`, the tab strip's hrefs
 * and every mapping in `data.ts` are the product's own.
 *
 * `memoryHistory` rather than jsdom's single URL: `?tab=consumers` and
 * `/topics/orders.v1` are the inputs to half of these cases, and a test that could not set them
 * would be a test of the default tab.
 *
 * ## The stub answers by path, and refuses what it was not given
 *
 * A stub that answered `{}` to an unknown path would let a screen appear to work while asking for
 * something nobody wrote down. An unstubbed path is an `unreachable` failure naming itself, which
 * shows up in the assertion rather than as an empty table.
 */
import { createRouter, memoryHistory, type RouteSectionProps } from "@solidjs/router";
import { flush } from "solid-js";
import type { JSX } from "@solidjs/web";
import type { ApiError, KuiApiClient } from "@kui/api";
import { KuiProvider, sharedQueries, type KuiContextValue, type KuiPaths } from "@kui/kernel";
import Topics from "./TopicsRoute.jsx";

/**
 * What a stub gateway was told to say, keyed by the templated path the client is called with.
 *
 * A value that is a **function** is called with the request instead of being sent as the body. That
 * form exists for the bulk paths and only for them: `deleteTopics` plans and confirms one topic at
 * a
 * time through the same two templated paths, so "a set of which one topic refused" — the state the
 * bulk toast's tone is decided by — cannot be arranged by path alone.
 */
export type StubbedAnswers = Readonly<Record<string, unknown>>;

/** The request a function-valued answer is given. `params.path` is what varies within a bulk run. */
export interface StubRequest {
  readonly path: string;
  readonly params: {
    readonly path?: Readonly<Record<string, string>>;
    readonly query?: Readonly<Record<string, unknown>>;
  };
  readonly body?: unknown;
}

export interface StubApi {
  readonly api: KuiApiClient;
  /** Every path asked for, in order. What a "one request per open" assertion counts. */
  readonly calls: readonly string[];
  /**
   * The same calls with their parameters.
   *
   * A path alone cannot say whether a control reached the server: the topic list is one path and
   * its
   * whole state is in the query string, so "did the address filter the list" is a question only
   * this
   * can answer. `calls` stays because a count of opens is a different question.
   */
  readonly requests: readonly StubRequest[];
}

export function stubApi(answers: StubbedAnswers): StubApi {
  const calls: string[] = [];
  const requests: StubRequest[] = [];
  const answer = async (path: string, init?: unknown): Promise<unknown> => {
    calls.push(path);
    const sent = (init ?? {}) as { readonly params?: StubRequest["params"]; readonly body?: unknown };
    const request: StubRequest = { path, params: sent.params ?? {}, body: sent.body };
    requests.push(request);
    if (Object.hasOwn(answers, path)) {
      const told = answers[path];
      if (typeof told !== "function") return { ok: true, value: told };
      return { ok: true, value: (told as (request: StubRequest) => unknown)(request) };
    }
    const error: ApiError = {
      kind: "unreachable",
      cause: `this test stubbed no answer for ${path}`,
    };
    return { ok: false, error };
  };
  const client = {
    get: answer,
    post: answer,
    put: answer,
    delete: answer,
    patch: answer,
    raw: {},
  } as unknown as KuiApiClient;
  return { api: client, calls, requests };
}

/**
 * The addresses this feature reaches.
 *
 * Written out rather than taken from the shell, because a feature may not import the shell — that
 * edge is the one the microfrontend split exists to prevent. They are the same shapes
 * `shellPaths` produces, and a test that cared about the exact spelling would be testing the shell.
 */
const paths: KuiPaths = {
  home: () => "/ui",
  settings: () => "/ui/settings",
  clusters: () => "/ui/clusters",
  manageClusters: () => "/ui/clusters/manage",
  dashboard: (cluster, tab = "overview") => `/ui/clusters/${cluster}/dashboard/${tab}`,
  brokers: (cluster) => `/ui/clusters/${cluster}/brokers`,
  broker: (cluster, brokerId) => `/ui/clusters/${cluster}/brokers/${brokerId}`,
  topics: (cluster) => `/ui/clusters/${cluster}/topics`,
  topic: (cluster, name) => `/ui/clusters/${cluster}/topics/${encodeURIComponent(name)}`,
  topicMessages: (cluster, name) =>
    `/ui/clusters/${cluster}/topics/${encodeURIComponent(name)}/messages`,
  trackMessages: (cluster) => `/ui/clusters/${cluster}/messages/track`,
  consumerGroups: (cluster) => `/ui/clusters/${cluster}/consumer-groups`,
  consumerGroup: (cluster, groupId) =>
    `/ui/clusters/${cluster}/consumer-groups/${encodeURIComponent(groupId)}`,
};

export interface HostOptions {
  /** The address, as the router sees it: `/clusters/quickstart/topics?tab=consumers`. */
  readonly at: string;
  readonly answers: StubbedAnswers;
  /**
   * What `useKui().permits` answers. Defaults to "everything", so a case about a control's presence
   * is not silently a case about permissions.
   *
   * A predicate as well as a flag, because this screen gates four controls on four different
   * actions and a single `false` cannot tell them apart: with one answer for everything, a control
   * wired to the wrong action is disabled at exactly the moments the right one would be.
   *
   * Typed off the context rather than off `KnownAction`, which the kernel's barrel does not export.
   */
  readonly permits?: boolean | KuiContextValue["permits"] | undefined;
  readonly writeBlocked?: KuiContextValue["writeBlocked"] | undefined;
}

/**
 * A mountable topics feature, plus the stub it is talking to.
 *
 * `sharedQueries` is module state by design — two features asking for one cluster should ask once —
 * so two cases that use the same cluster id share an answer, and the second would assert against
 * the first one's stub. {@link forgetQueries} in an `afterEach` is what keeps cases independent;
 * giving each case its own cluster id keeps them independent even when somebody forgets.
 */
export function topicsHost(options: HostOptions): {
  readonly view: () => JSX.Element;
  readonly stub: StubApi;
  readonly goTo: (address: string) => void;
} {
  const stub = stubApi(options.answers);

  /*
   * Held rather than inlined, because a case needs to move the address of a route that is *already
   * mounted*.
   *
   * Every other case in this file mounts at its address, so the screen's seed answers it and the
   * effect that keeps reading the address is never exercised. Pushing onto this history changes the
   * search string under a live route — the drawer's second prefix row, exactly — and the router
   * keeps the same component instance because the matched path did not change.
   */
  const history = memoryHistory(options.at);

  const Router = createRouter({
    routes: [
      { path: "/clusters/:clusterId/topics", component: () => <Topics /> },
      { path: "/clusters/:clusterId/topics/:topicName", component: () => <Topics /> },
      { path: "*", component: () => <p data-testid="no-route">no route matched</p> },
    ],
    history,
  });

  const context: KuiContextValue = {
    api: stub.api,
    cluster: () => undefined,
    permits: (action, name) =>
      typeof options.permits === "function"
        ? options.permits(action, name)
        : (options.permits ?? true),
    paths,
    report: () => {},
    ...(options.writeBlocked === undefined ? {} : { writeBlocked: options.writeBlocked }),
  };

  return {
    stub,
    view: () => (
      <KuiProvider value={context}>
        <Router>{(route: RouteSectionProps) => route.children}</Router>
      </KuiProvider>
    ),
    /**
     * Navigates the mounted view, the way a link in the drawer does.
     *
     * A push and not a replace: `back()` is a real thing an operator does after following two
     * prefix rows, and a history whose entries are overwritten cannot be walked backwards.
     */
    goTo: (address: string) => history.set({ value: address }),
  };
}

/**
 * Empties the shared query cache between cases.
 *
 * The registry is a browser tab's view of one server, so it outlives a component and, in a test
 * file, a case. Without this a second case asking for the same cluster reads the first case's
 * answer and passes for the wrong reason, which is the failure mode a shared cache always has.
 */
export function forgetQueries(): void {
  sharedQueries.invalidateWhere(() => true);
}

/**
 * Lets every pending request land, then lets the DOM catch up.
 *
 * `flush()` alone drains Solid's own queue; it does not resolve a promise. These screens fetch on
 * mount, map the answer, and then draw — three microtask hops before anything is on screen — and a
 * case that flushed once would assert against a skeleton and read as a component drawing nothing.
 * The loop is bounded rather than a fixed count of hops so a chain one link longer (a tab whose
 * query opens after the overview lands) does not need every case edited.
 */
export async function settle(rounds = 8): Promise<void> {
  for (let round = 0; round < rounds; round += 1) {
    await new Promise((resolve) => setTimeout(resolve, 0));
    await flush();
  }
}

/**
 * Gives every element a height for the length of one case, and puts it back.
 *
 * `VirtualizedTable` windows on the height its own scroller reports, and jsdom has no layout engine:
 * every element is zero pixels tall, the window is empty, and the route's table draws no rows at
 * all. A case about a *component* sidesteps that with the `viewportHeight` override — a case about
 * the route cannot, because `TopicsRoute` does not pass one and must not start passing one to suit a
 * test. So the environment is given the one measurement it is missing, rather than the product being
 * given a prop it has no use for.
 *
 * The undo is {@link restoreMeasuredRows} and belongs in an `afterEach` rather than at the end of a
 * case: a case that fails before its own restore would leave every element in the file two hundred
 * pixels tall, and the next failure would be about that instead of about itself.
 */
let realClientHeight: PropertyDescriptor | undefined;
let heightStubbed = false;

export function withMeasuredRows(height = 480): void {
  if (!heightStubbed) {
    realClientHeight = Object.getOwnPropertyDescriptor(HTMLElement.prototype, "clientHeight");
    heightStubbed = true;
  }
  Object.defineProperty(HTMLElement.prototype, "clientHeight", {
    configurable: true,
    get: () => height,
  });
}

/** Puts the real `clientHeight` back. Safe to call when nothing was stubbed. */
export function restoreMeasuredRows(): void {
  if (!heightStubbed) return;
  if (realClientHeight === undefined) Reflect.deleteProperty(HTMLElement.prototype, "clientHeight");
  else Object.defineProperty(HTMLElement.prototype, "clientHeight", realClientHeight);
  heightStubbed = false;
}
