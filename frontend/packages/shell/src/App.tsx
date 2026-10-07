/**
 * The application, assembled.
 *
 * ## The order of start-up matters
 *
 * The session is fetched before anything mutating can work, because the only place a CSRF token ever
 * comes from is the body of `GET /api/v1/auth/me` and nothing else calls it. When start-up did not,
 * the token stayed absent for the life of the page and every non-`GET` was refused — including the
 * "Retry now" button on the degraded-feature panel, which therefore never worked.
 *
 * The capability stream is opened before the first paint, and nothing waits for it: an empty picture
 * renders as degraded-with-STARTING, which is the honest state — the features are usable and their
 * health has not been established yet.
 *
 * The router is built from the static route table, which is complete before a byte of any feature has
 * been downloaded, so a deep link resolves on the very first pass.
 *
 * ## The two full-screen states, and why their order is the whole rule
 *
 * A gateway that is not answering wins over the sign-in screen, because a sign-in form that cannot
 * reach a server is a form that can only fail, and the unreachable screen is the one that says why
 * and retries. Only when KUI *is* reachable is the sign-in question asked at all — and the default
 * answer to it is no, because authentication is disabled in every deployment until somebody
 * configures an identity provider.
 */
import {
  Show,
  createEffect,
  createMemo,
  createSignal,
  createStore,
  onCleanup,
  onSettled,
} from "solid-js";
import {
  createApiClient,
  createCsrfTokens,
  type CsrfTokens,
  readBootstrap,
  userMessage,
  type ApiError,
  type KuiApiClient,
} from "@kui/api";
import {
  Banner,
  ToastRegion,
  AlertsProvider,
  AuthDisabled,
  createAlerts,
  createCapabilities,
  createCurrentCluster,
  createMutation,
  createSession,
  notify,
  openEventSource,
  type SseHandle,
  soleClusterChoice,
  type FeatureId,
  type FeatureRegistration,
  type FeatureState,
  KuiProvider,
  type KuiContextValue,
  accentPreference,
  densityPreference,
  themePreference,
} from "@kui/kernel";
import { AccountMenu } from "./chrome/AccountMenu.jsx";
import { AppFrame } from "./chrome/AppFrame.jsx";
import { EnvRail, type RailDestination } from "./chrome/EnvRail.jsx";
import { NavDrawer } from "./chrome/NavDrawer.jsx";
import { installSearchShortcut } from "./chrome/searchShortcut.js";
import { Overview } from "./overview/Overview.jsx";
import { fetchOverview, loadingData, toOverviewModel, type OverviewData } from "./overview/load.js";
import { readingValue } from "./overview/reading.js";
import { TopBar } from "./chrome/TopBar.jsx";
import type {
  ClusterSummary,
  Crumb,
  NavCount,
  NavCounts,
  NavDestination,
} from "./chrome/types.js";
import {
  alertsBadge,
  alertsStreamUrl,
  loadAlertFeed,
  noticesOf,
} from "./data/alerts.js";
import { createAlertFeedCache } from "./data/alertCache.js";
import { createAppearanceSync } from "./data/appearance.js";
import { createMessageBrowserSync } from "./data/messageBrowser.js";
import { createClusterWritePolicy } from "./data/writePolicy.js";
import { brokerStorageOf, createClusterStore } from "./data/clusterStore.js";
import {
  SEARCH_DEBOUNCE_MS,
  SEARCH_LIMIT,
  SEARCH_MAX_LENGTH,
  fetchSearch,
  searchGroups,
  searchStatus,
  unavailableServices,
  type SearchAnswer,
  type SearchLinks,
  type SearchState,
} from "./data/search.js";
import { featureRegistry } from "./features/registry.js";
import { FeatureGate } from "./features/FeatureGate.jsx";
import { createHealth, type CallScope } from "./health.js";
import { degradedBanner, StaleBanner } from "./messages.js";
import {
  navigationGroups,
  stillWorking,
  degradedLabels,
  type FeatureStatus,
} from "./nav/navigation.js";
import { topicGroupHref, topicSubtree } from "./nav/topicTree.js";
import { ForbiddenPage, GatewayUnreachablePage, NotFoundPage } from "./pages/errorPages.jsx";
import { SignIn } from "./pages/SignIn.jsx";
import { SettingsPage, asPreference } from "./pages/SettingsPage.jsx";
import {
  clusterInUrl,
  createShellRouter,
  landingFor,
  UiPath,
  type ShellRouter,
} from "./routing/routes.jsx";
import { shellPaths } from "./routing/paths.js";
import { useNavigate, type RouteSectionProps } from "@solidjs/router";

export function App() {
  const bootstrap = readBootstrap();
  const origin = window.location.origin;
  /** Where the frontend is mounted, deployment prefix included. */
  const uiPrefix = `${bootstrap.basePath.replace(/\/$/, "")}${UiPath}`;

  const csrf = createCsrfTokens();
  const session = createSession({
    settleCsrf: (token) => csrf.settle(token),
    invalidateCsrf: () => csrf.invalidate(),
  });
  let clearPrivateBrowserState = (): Promise<void> => Promise.resolve();
  let reauthenticationRequested = false;

  const api: KuiApiClient = createApiClient({
    bootstrap,
    origin,
    csrf,
    // The gateway said the session lapsed. Emptying it here rather than at the call site means no
    // write control survives that moment even for the length of a reload.
    onUnauthorized: () => {
      // An anonymous identity is a genuine session, including the CSRF token needed to log in.
      // Protected reads beneath the overlay can be refused without expiring that session again.
      if (session.identity()?.principal.kind === "anonymous" || reauthenticationRequested) return false;
      reauthenticationRequested = true;
      session.markExpired();
      void clearPrivateBrowserState();
      // If expiry races start-up's settings read, joining that old flight cannot refresh /me.
      // Queue exactly one fresh session after it; concurrent 401s share this episode.
      void (sessionFlight ?? Promise.resolve()).then(() => startUp());
      return true;
    },
  });

  const health = createHealth({
    now: () => new Date(),
    schedule: (delayMs, run) => void setTimeout(run, delayMs),
    onRetry: () => void startUp(),
  });

  const report = (scope: CallScope, error: ApiError | undefined): void => {
    if (error === undefined) health.report(scope, "ok");
    // A 403 or a 404 is the gateway *answering*, and answering is the opposite of being unreachable.
    else
      health.report(
        scope,
        error.kind === "unreachable" || error.kind === "timeout" ? "transport-failure" : "answered",
      );
  };

  const capabilities = createCapabilities({
    /*
     * The capability stream waits for the session, like every other call — but it has to do it here
     * rather than in the client, because it is the one request that does not go through the client
     * at all. `openEventSource` uses the browser's native `EventSource`, which the client's
     * middleware never sees, so the gate in `@kui/api` cannot hold it.
     *
     * Left ungated it is usually the *first* request out, since it opens during construction while
     * `startUp` is still queued behind `onSettled`. Cookieless, it makes the gateway mint a session,
     * which `/auth/me` then races — and the loser's CSRF token is the one this client keeps. See
     * `isSessionCall` in `@kui/api` for what that costs.
     */
    openStream: (subscriber) =>
      deferUntilSession(
        () =>
          openEventSource(
            `${bootstrap.apiBase.replace(/\/$/, "")}/capabilities/stream`,
            subscriber,
          ),
        csrf,
      ),
    poll: () => api.get("/api/v1/capabilities", {}),
    notify: (notice) => notify(notice.title, { tone: notice.tone, message: notice.message }),
    schedule: (delayMs, action) => void setTimeout(action, delayMs),
  });

  const cluster = createCurrentCluster({ storage: safeLocalStorage() });
  const writePolicy = createClusterWritePolicy(api, session);
  createEffect(() => session.identity(), () => writePolicy.refresh());
  onSettled(() => {
    const timer = setInterval(() => writePolicy.refresh(), 30_000);
    return () => clearInterval(timer);
  });
  onCleanup(() => writePolicy.dispose());

  /**
   * The cluster the *address* names — the first paint's answer, and then every navigation's.
   *
   * A URL naming a cluster wins over the stored selection, because a link is usually pasted by a
   * colleague and the recipient has to see what the sender saw. The initial value comes from
   * `window.location` because it has to exist before anything renders, and {@link Frame} keeps it
   * current from the router's own location afterwards: every navigation after the first is a
   * client-side one and `window.location` is never read again, so a link from one cluster's topic
   * list to another cluster's dashboard would otherwise leave the whole frame — head, badges,
   * meter — describing the cluster the user left.
   *
   * A signal here, and deliberately **not** `cluster.select(...)` called on this line. Writing to a
   * store during a component's own construction is `REACTIVE_WRITE_IN_OWNED_SCOPE`, which Solid 2's
   * development build raises and its production build compiles away — the worse way round, because
   * the shape that is wrong is the shape that ships, and the one address that reached this line
   * (`/ui/clusters/<id>/…`, a deep link) is the one nothing had ever mounted. The stored selection
   * is brought into step in `Frame`'s effect instead, where a write is what effects are for.
   */
  const [routeCluster, setRouteCluster] = createSignal<string | undefined>(
    clusterInUrl(window.location.pathname, uiPrefix),
  );

  /**
   * Which cluster everything below is about.
   *
   * The address wins, and the stored selection is the fallback for the addresses that name no
   * cluster — `/ui`, `/ui/settings`. Both are needed and neither is derivable from the other: the
   * address is the source of truth for a pasted link, and the stored selection is what somebody
   * arriving at the root with no cluster in the address gets back. `Frame` keeps the second in step
   * with the first, so the two only ever disagree while no address names a cluster at all.
   */
  const clusterForFrame = (): string | undefined => routeCluster() ?? cluster.selected();

  const alertCacheScope = () => {
    const selected = clusterForFrame();
    const identity = session.identity();
    const principal = identity?.principal;
    if (selected === undefined || identity === undefined || principal === undefined)
      return undefined;
    return {
      cluster: selected,
      principalKind: principal.kind,
      principalName: principal.name,
      authorization: JSON.stringify({ roles: principal.roles ?? [], grants: identity.permissions }),
    };
  };
  const alertCache = createAlertFeedCache({ scope: alertCacheScope });
  clearPrivateBrowserState = () => alertCache.clear();

  const appearance = createAppearanceSync({
    api,
    preferences: {
      theme: themePreference,
      accent: accentPreference,
      density: densityPreference,
    },
    storage: safeLocalStorage(),
  });
  const messageBrowser = createMessageBrowserSync({ api, storage: safeLocalStorage() });

  createEffect(
    () => {
      const selected = clusterForFrame();
      const principal = session.identity()?.principal;
      return selected === undefined || principal === undefined
        ? undefined
        : {
            cluster: selected,
            principalKind: principal.kind,
            principalName: principal.name,
          };
    },
    (scope) => {
      appearance.selectScope(scope);
      messageBrowser.selectScope(scope);
    },
  );
  onCleanup(() => {
    appearance.dispose();
    messageBrowser.dispose();
  });

  /**
   * The search input, once it exists, and the `⌘K` that focuses it.
   *
   * A plain binding rather than a signal: nothing renders from it, it is only read inside a key
   * handler, and making it reactive would add a dependency edge no computation wants. The listener
   * goes on `document` rather than on the frame, because the shortcut has to work with focus
   * anywhere — including inside a dialog rendered through a portal.
   */
  let searchInput: HTMLInputElement | undefined;
  onSettled(() =>
    installSearchShortcut(() => {
      // `select()` as well as `focus()`: pressing the shortcut with a query already in the box means
      // "search for something else", and leaving the old text with the caret at its end makes the
      // user delete it first. Every command palette and address bar selects.
      searchInput?.focus();
      searchInput?.select();
    }),
  );

  /**
   * What is in the search box, and what the last search answered.
   *
   * Two signals rather than one, because the text and the answer are out of step for most of the
   * time the box is in use: a debounce is precisely the interval during which the field shows what
   * was typed and the overlay shows the previous answer, and collapsing them would either delay the
   * characters appearing or throw the results away on every keystroke.
   */
  const [searchText, setSearchText] = createSignal("");
  const [searchState, setSearchState] = createSignal<SearchState>({ kind: "idle" });

  let searchTimer: ReturnType<typeof setTimeout> | undefined;
  let searchRequest: AbortController | undefined;
  /**
   * Which search is current.
   *
   * `GET /api/v1/search` is a fold over three services, so two searches in flight can finish in
   * either order — and the loser landing last would put the results for `ord` under a box that says
   * `orders`. Every answer carries the number of the search that asked for it and is dropped when
   * that number has moved on, which is the same rule the overview's fetch effect applies to a
   * cluster switch and for the same reason: a wrong answer that looks right is the worst kind.
   */
  let searchEpisode = 0;

  const invalidateSearch = (): number => {
    const episode = (searchEpisode += 1);
    searchRequest?.abort();
    searchRequest = undefined;
    return episode;
  };

  const runSearch = (query: string, episode: number): void => {
    const request = new AbortController();
    searchRequest = request;
    setSearchState({ kind: "searching" });
    void fetchSearch(api, query, SEARCH_LIMIT, request.signal).then((answer) => {
      if (searchRequest === request) searchRequest = undefined;
      if (episode !== searchEpisode) return;
      /* The search endpoint is the gateway's own, so a failure here is evidence about the gateway
         rather than about one upstream — the same reasoning the capability probe applies. A service
         that could not be asked is *not* a failure and does not reach this branch: it arrives
         inside a successful answer, in `partial`. */
      report("shell", answer.ok ? undefined : answer.error);
      setSearchState(
        answer.ok
          ? { kind: "ready", answer: answer.value }
          : { kind: "failed", reason: userMessage(answer.error) },
      );
    });
  };

  const onSearchInput = (next: string): void => {
    setSearchText(next);
    if (searchTimer !== undefined) clearTimeout(searchTimer);
    searchTimer = undefined;

    /* Invalidate at the keystroke, not when its debounce expires. Otherwise an older request can
       land during this 200 ms window and replace "Searching" with rows for text the field no
       longer contains. Cancelling also stops a gateway fan-out whose answer cannot be used. */
    const episode = invalidateSearch();

    const query = next.trim();
    if (query.length === 0) {
      // Emptying the box is not a search for nothing; it is the end of searching. The episode is
      // stepped so that an answer already in flight cannot reopen the overlay behind the caret.
      setSearchState({ kind: "idle" });
      return;
    }

    /* `searching` immediately, and the request after the debounce. Waiting to say anything until
       the request goes out leaves the overlay showing the *previous* query's results for a fifth of
       a second under new text, which reads as a search that answered wrongly. */
    setSearchState({ kind: "searching" });
    searchTimer = setTimeout(() => {
      searchTimer = undefined;
      runSearch(query, episode);
    }, SEARCH_DEBOUNCE_MS);
  };

  onCleanup(() => {
    if (searchTimer !== undefined) clearTimeout(searchTimer);
    searchEpisode += 1;
    searchRequest?.abort();
  });

  const searchAnswer = createMemo<SearchAnswer | undefined>(() => {
    const state = searchState();
    return state.kind === "ready" ? state.answer : undefined;
  });

  /**
   * The cluster overview's data.
   *
   * It is fetched here rather than in the screen so that `Overview` takes a finished view model and
   * fetches nothing — which is what lets every state of that screen, including the ones that only
   * happen when a service is down, be rendered in a story and a test with no server.
   */
  const [overview, setOverview] = createStore<{ cluster: string | undefined; data: OverviewData }>({
    cluster: undefined, data: loadingData(),
  });
  const overviewData = () => overview.cluster === clusterForFrame() ? overview.data : loadingData();

  createEffect(
    () => clusterForFrame(),
    (selected) => {
      if (selected === undefined) return undefined;
      let cancelled = false;
      void (async () => {
        const next = await fetchOverview(api, selected);
        if (cancelled) return;
        setOverview((draft) => {
          draft.cluster = selected;
          draft.data = next;
        });
      })();
      // Switching cluster while five requests are in flight must not let the old cluster's answers
      // land on the new cluster's screen — the most convincing kind of wrong number there is.
      return () => {
        cancelled = true;
      };
    },
  );

  /**
   * The alert feed, opened once for the whole frame.
   *
   * Three things read it and they are in three packages that may not see each other: the bell a few
   * hundred lines below, the drawer's Alerts badge beside it, and the alerts card in
   * `@kui/feature-alerts`. A feature may not import the shell and the shell must not *statically*
   * import a feature — `frontend/scripts/bundle-shape.mjs` fails the build on the second — so the
   * kernel is the one place all three can read from, and this is the one place it is constructed.
   * A second store would be a second open count, and the count is the whole point.
   *
   * `load` and `openStream` are functions rather than values because the cluster changes underneath
   * them: both read `clusterForFrame()` at the moment they are called, so the effect below can
   * restart the store without rebuilding it. The stream goes through `deferUntilSession` for the
   * reason the capability stream does — a cookieless `EventSource` makes the gateway mint a second
   * session, and the loser's CSRF token is the one this client keeps.
   */
  const alerts = createAlerts({
    openStream: (subscriber) =>
      deferUntilSession(
        () =>
          openEventSource(
            alertsStreamUrl(bootstrap.apiBase, clusterForFrame() ?? ""),
            subscriber,
          ),
        csrf,
      ),
    load: (markRead) => loadAlertFeed(api, clusterForFrame() ?? "", markRead),
    /* Which cluster the frames on this stream should be about. The store checks it, because a frame
       naming another cluster moving this bell's count would be a number from somewhere the
       operator is not looking — and the accessor rather than the value, so a switch is followed. */
    cluster: () => clusterForFrame(),
    cache: alertCache,
  });

  /*
   * Started per cluster.
   *
   * Not started at all until a cluster is chosen: both addresses name one, and a feed opened
   * against `/clusters//alerts/events` would ask for a path that matches no route. What the restart
   * buys is asserted in `app.render.test.tsx`'s "re-opens the alert stream against the cluster the
   * address moved to" — a stream left pointing at the cluster the operator has left goes on
   * delivering counts into a bell that is now describing a different one.
   *
   * **The cleanup is not what makes the switch safe, and saying so is the point of this sentence.**
   * `Alerts.start()` already releases the previous handle and steps its own episode, so the old
   * stream is closed and an in-flight read for the old cluster is discarded whether or not this
   * returns anything. The cleanup covers the one case `start()` cannot: a frame that stops naming a
   * cluster at all, where there is no new start to do the releasing. Dropping it therefore leaves
   * every case here green, which is a fact about what it is for rather than a hole — the teardown
   * on unmount is `onCleanup` below, and that one is load-bearing.
   */
  createEffect(
    () => {
      const scope = alertCacheScope();
      return scope === undefined
        ? undefined
        : JSON.stringify([
            scope.cluster,
            scope.principalKind,
            scope.principalName,
            scope.authorization,
          ]);
    },
    (scope) => {
      if (scope === undefined) return undefined;
      alerts.start();
      return () => alerts.stop();
    },
  );
  onCleanup(() => alerts.stop());

  /**
   * Whether the notifications panel is showing.
   *
   * Owned here rather than by the top bar, so that Escape and a click elsewhere can close it — a
   * panel whose open state lives inside the control that opens it is a panel nothing else can
   * dismiss.
   */
  const [noticesOpen, setNoticesOpen] = createSignal(false);

  /**
   * Whether the account panel at the foot of the rail is showing. Owned here for the same reason the
   * notifications panel's openness is.
   */
  const [accountOpen, setAccountOpen] = createSignal(false);

  /**
   * Ending the session.
   *
   * `POST /api/v1/auth/logout` clears the cookie server-side; the reload afterwards is what clears
   * everything this page built while the departing principal was signed in. See `AccountMenu` for
   * why the reload is not a list of stores to refresh, and why a *failed* sign-out must not reload.
   */
  const signOut = createMutation(() => api.post("/api/v1/auth/logout", {}));

  /**
   * Whether there is a session for the rail to offer to end.
   *
   * The mirror of `mustSignIn`'s rule, and it needs both halves for the same reasons: a deployment
   * with `authType: "disabled"` has an anonymous principal that logging out cannot dispose of, and a
   * principal who is anonymous under a configured provider is somebody who has not signed in yet.
   */
  const canSignOut = (): boolean =>
    session.signedIn() && (session.settings()?.authType ?? AuthDisabled) !== AuthDisabled;

  const [probing, setProbing] = createSignal<ReadonlySet<string>>(new Set<string>());
  const [probeErrors, setProbeErrors] = createSignal<ReadonlyMap<string, string>>(new Map());

  /**
   * The "Retry now" button's other half: asking the gateway to re-check one service.
   *
   * A second press while the first is outstanding does nothing. Without that, a user watching a slow
   * service can queue up a dozen probes, each of which makes the gateway call an upstream that is
   * already struggling. The recomputed state reaches the navigation through the capability stream
   * like every other transition — nothing here writes into the store, because two writers to one
   * picture is how a picture ends up disagreeing with itself.
   */
  const probe = (service: string): void => {
    if (probing().has(service)) return;
    setProbing(new Set([...probing(), service]));
    setProbeErrors(new Map([...probeErrors()].filter(([key]) => key !== service)));

    void api
      .post("/api/v1/capabilities/{service}/probe", { params: { path: { service } } })
      .then((answer) => {
        setProbing(new Set([...probing()].filter((id) => id !== service)));
        // The capability endpoints are the shell's own, so a failure here is evidence about the
        // gateway itself and is reported as such.
        report("shell", answer.ok ? undefined : answer.error);
        if (!answer.ok) {
          setProbeErrors(new Map([...probeErrors(), [service, userMessage(answer.error)]]));
        }
      });
  };

  /**
   * The start-up calls, re-run by the connectivity tracker's retry.
   *
   * `/auth/me` goes first and **alone**, and everything else follows it. These two used to run in
   * one `Promise.all`, which was wrong twice over.
   *
   * The gateway mints an anonymous session for any API request arriving without a cookie and stamps
   * `Set-Cookie` on the answer, so two cookieless requests mint two sessions and the browser keeps
   * whichever reply lands last. The CSRF token this client keeps is the one `/auth/me` returned, and
   * it belongs to that session — the same session only by luck. Every read then works, because a
   * fresh anonymous session can read what an anonymous session can read, and every *write* is
   * refused with "X-Csrf-Token does not match the session's token".
   *
   * The client now holds every request other than `/auth/me` behind the token gate for exactly this
   * reason (see `isSessionCall` in `@kui/api`), which makes the second problem with `Promise.all`
   * fatal rather than merely subtle: `/auth/settings` would wait for the gate, the gate opens on
   * `session.accept` below, and `session.accept` was waiting for `/auth/settings`. A deadlock, ended
   * only by the gate's ten-second deadline — long enough that the first person to see it reads it as
   * the gateway being slow.
   */
  let sessionFlight: Promise<void> | undefined;
  const sessionRequest = new AbortController();
  onCleanup(() => sessionRequest.abort());
  const startUp = (): Promise<void> => {
    if (sessionFlight !== undefined) return sessionFlight;
    sessionFlight = Promise.resolve().then(async () => {
      const me = await api.get("/api/v1/auth/me", { signal: sessionRequest.signal });
      if (sessionRequest.signal.aborted) return;

      report("shell", me.ok ? undefined : me.error);
      if (me.ok) {
        session.accept(me.value);
        if (me.value.principal.kind !== "anonymous") reauthenticationRequested = false;
      } else {
        session.markExpired();
        // Release settings/sign-in reads, but never invent a successfully established identity.
        csrf.settle(undefined);
      }

      // Only now, with the session established and the gate open, does anything else go out.
      const settings = await api.get("/api/v1/auth/settings", { signal: sessionRequest.signal });
      if (sessionRequest.signal.aborted) return;
      session.acceptSettings(settings.ok ? settings.value : undefined);
    }).finally(() => { sessionFlight = undefined; });
    return sessionFlight;
  };

  /*
   * The capability stream is opened during construction, not in `onSettled`.
   *
   * `capabilities.start()` builds a reactive root to watch the connection, and Solid 2 forbids
   * creating reactive primitives inside an effect or an owner-backed `onSettled`: it raises
   * `PRIMITIVE_IN_FORBIDDEN_SCOPE`. The development build enforces that and the production build
   * compiles the check away, so the violation ran unnoticed in the browser and failed the moment
   * anything mounted `App` under the development renderer — the worst way round, because the shape
   * that is wrong is the shape that ships.
   *
   * Opening it here is also what the start-up order already asked for: nothing waits for the
   * picture, so there is nothing to gain by waiting for the first paint before asking for it.
   */
  capabilities.start();
  onCleanup(() => capabilities.stop());

  onSettled(() => {
    void startUp();
  });

  /**
   * What the shell currently knows about one feature.
   *
   * Declared *before* the memo that calls it, and that order is load-bearing rather than tidiness.
   * A `const` arrow function is in its temporal dead zone until the line that assigns it runs, and
   * `createMemo` in Solid 2 computes eagerly when it is created — so a memo written above this
   * binding calls it while it is still uninitialised. That throws `ReferenceError: Cannot access
   * 'stateOf' before initialization` inside the reactive graph, which Solid reports as
   * `REACTIVITY_HALTED`: the whole tree stops and the application renders a blank page with no
   * failed request and no visibly broken component to point at.
   */
  const stateOf = (registration: FeatureRegistration): FeatureState =>
    capabilities.featureState(
      registration.serviceId,
      registration.requiresCluster ? clusterForFrame() : undefined,
      // Unknown and expired identities are never evidence of a grant.
      session.permits(
          registration.viewAction.resource,
          registration.viewAction.action,
          clusterForFrame(),
        ),
    );

  /** Every feature's registration paired with what the shell currently knows about it. */
  const statuses = createMemo<readonly FeatureStatus[]>(() =>
    featureRegistry.map((registration) => ({
      registration,
      state: stateOf(registration),
    })),
  );

  const statusOf = (id: FeatureId): FeatureStatus | undefined =>
    statuses().find((status) => status.registration.id === id);

  const Router: ShellRouter = createShellRouter(bootstrap.basePath, {
    home: () => {
      /*
       * `+ Create topic` on the dashboard, which did nothing at all until now.
       *
       * There is no address that opens the creation dialog — it lives inside the topics screen and
       * is opened from that screen's own button — so the honest wiring is to take the operator to
       * the place the flow starts rather than to invent a second entry point that would then be a
       * second thing to keep working. The link is built through `KuiPaths` like every other, so a
       * renamed segment is a compile error here instead of a button that navigates to a 404.
       *
       * `useNavigate` is called here rather than passed down because this *is* a route component:
       * it is rendered under the router, which is the only place the hook can be read.
       */
      const navigate = useNavigate();
      return (
        <Overview
          model={toOverviewModel(overviewData())}
          onCreateTopic={() => {
            // No cluster, no topic list to send anybody to. The dashboard's own empty state is
            // where that case is explained; a button that navigates nowhere is not.
            //
            // This guard is no longer the *only* thing keeping that rule. It used to be, and it
            // was not enough: the button stayed enabled and drew a primary affordance whose press
            // reached here and returned, so the screen offered an action it could not perform.
            // `CreateTopicAction` now disables it with the reason. The guard stays because it is
            // the honest behaviour for a handler that can be called with no cluster — a press
            // swallowed by `aria-disabled` is a browser promise, not a program invariant — and
            // because a second caller would otherwise inherit the old defect.
            const chosen = clusterForFrame();
            if (chosen !== undefined) navigate(paths.topics(chosen), AlreadyPrefixed);
          }}
        />
      );
    },
    settings: () => (
      <SettingsPage
        /* The kernel's singletons, handed in rather than reached for. The page takes them as props
           so a test can drive it without sharing `localStorage` with the next suite. */
        theme={asPreference(appearance.preferences.theme)}
        accent={asPreference(appearance.preferences.accent)}
        density={asPreference(appearance.preferences.density)}
        persistence={appearance.status()}
        messagePageSize={messageBrowser.preferences.pageSize}
        messageViewMode={messageBrowser.preferences.mode}
        messagePersistence={messageBrowser.status()}
        version={bootstrap.buildVersion}
        apiBase={bootstrap.apiBase}
      />
    ),
    forbidden: () => <ForbiddenPage subject="this page" homeHref={Router.paths()} />,
    notFound: () => <NotFoundPage attempted={window.location.pathname} homeHref={Router.paths()} />,
    feature: (id) => () => {
      const status = statusOf(id);
      if (status === undefined)
        return <NotFoundPage attempted={window.location.pathname} homeHref={Router.paths()} />;
      return (
        <FeatureGate
          registration={status.registration}
          state={() => statusOf(id)?.state ?? { kind: "ready" }}
          onProbe={() => probe(status.registration.serviceId)}
          probing={() => probing().has(status.registration.serviceId)}
          probeError={() => probeErrors().get(status.registration.serviceId)}
          stillWorking={() => stillWorking(statuses(), id)}
        />
      );
    },
  });

  /**
   * Every link the shell and its features build.
   *
   * Built once, from the router, and shared by the route views above and by the feature context
   * below. Two calls to `shellPaths` would be two implementations of one interface, which is how a
   * product ends up spelling one page two ways — and the route views read it from inside a closure
   * that runs long after this line, so declaring it after the router is not a hazard.
   */
  const paths = shellPaths(Router);

  /**
   * Where a search result goes, one function per kind of hit.
   *
   * All three through the router's typed proxy, like every other address the shell builds. The
   * subject row is the one that needs `landingFor` rather than `paths`: `KuiPaths` carries no
   * registry address — it gains no member this wave — and the registry has no per-subject address
   * anyway, so a subject opens its cluster's registry, which is a page that exists and shows it.
   */
  const searchLinks: SearchLinks = {
    topic: (cluster, name) => paths.topic(cluster, name),
    group: (cluster, groupId) => paths.consumerGroup(cluster, groupId),
    subjects: (cluster) => landingFor(Router, "schemas", cluster) ?? paths.dashboard(cluster),
  };

  /* The two things the field draws from an answer, folded once each rather than at the call site.
     Declared *after* `searchLinks` and not beside the state above, because `createMemo` computes
     eagerly in Solid 2 and a memo written above that binding would read it inside its temporal dead
     zone — the `REACTIVITY_HALTED` blank page this file's header describes at length. */
  const searchResults = createMemo(() => {
    const answer = searchAnswer();
    return answer === undefined ? undefined : searchGroups(answer, searchLinks);
  });

  const searchUnavailable = createMemo(() => {
    const answer = searchAnswer();
    return answer === undefined ? undefined : unavailableServices(answer);
  });

  /** The clusters the capability registry knows, folded to one row each. */
  const clusters = createMemo<readonly ClusterSummary[]>(() =>
    clusterSummaries(capabilities.states()),
  );

  // A deployment with one cluster is not asking the user to choose. The selection is filled in and
  // the user is *not* navigated anywhere: that would move somebody who had deliberately opened
  // another page, and the only thing missing was the cluster-scoped navigation entries, which appear
  // the moment the selection exists.
  createEffect(
    () => soleClusterChoice(clusters(), cluster.selected()),
    (only) => {
      if (only !== undefined) cluster.select(only);
    },
  );

  /**
   * What every feature is handed.
   *
   * Built once and shared, but every field that can change is a *function* — `cluster()` and
   * `permits()` are called at the moment a feature needs them, so switching cluster from the rail
   * or having the session settle reaches a mounted screen. Handing over values would freeze both at
   * first render, and the symptom is a screen still showing the previous cluster's topics.
   *
   * `report` narrows the shell's three-way health signal to the one bit a feature can honestly
   * supply: the call failed, or it did not. Deciding whether a failure means "the gateway is down"
   * or "this upstream is down" needs `ApiError.kind`, which is the shell's job — a feature that
   * guessed would put the whole product behind the gateway-unreachable screen because one topic
   * list timed out.
   */
  const featureContext: KuiContextValue = {
    api,
    /* `clusterForFrame` rather than the stored selection, so that a feature and the frame around it
       cannot describe two different clusters. A deep link is the case that separates them: the
       address names one cluster from the first paint, and the stored selection is whatever the last
       visit left behind until `Frame`'s effect has run. */
    cluster: () => clusterForFrame(),
    permits: (action, name) =>
      session.permits(action.resource, action.action, clusterForFrame(), name),
    writeBlocked: writePolicy.writeBlocked,
    paths,
    report: (scope, failed) => health.report(scope, failed ? "answered" : "ok"),
    messageBrowser: {
      pageSize: messageBrowser.preferences.pageSize.choice,
      mode: messageBrowser.preferences.mode.choice,
    },
  };

  const banner = createMemo<string | undefined>(() =>
    capabilities.stale() ? StaleBanner : degradedBanner(degradedLabels(statuses())),
  );

  /**
   * Everything under the provider, so that the frame's own store can be built inside it.
   *
   * `createClusterStore` reads the API client out of `useKui`, and the provider is inside the
   * router's render prop — so there is nowhere in `App`'s body to build it. A component is the
   * seam, and it is the right one: the router calls its render prop once, untracked, so this mounts
   * once and the store's six caches are created once. One store and not one per consumer is the
   * whole point of the query cache underneath it — the head, the badges and the meter between them
   * ask about the same brokers, and they ask once.
   */
  const Frame = (props: { readonly route: RouteSectionProps }) => {
    const navigate = useNavigate();
    const facts = createClusterStore(clusterForFrame);

    /*
     * The address, followed.
     *
     * Two things happen here and they are not the same thing. `setRouteCluster` is what makes the
     * frame describe the cluster the address names, immediately and without a stored value getting
     * a say. `cluster.select` is what keeps the *stored* selection from drifting away from it, so
     * that the next visit to `/ui` returns to the cluster this person was last actually looking at
     * rather than to the one they last picked off the rail.
     */
    createEffect(
      () => clusterInUrl(props.route.location.pathname, uiPrefix),
      (named) => {
        setRouteCluster(named);
        if (named !== undefined && named !== cluster.selected()) cluster.select(named);
      },
    );

    /**
     * Changing environment: the four things that happen, from the one decision above them.
     *
     * The rail asks and this raises the toast, because there is exactly one `ToastRegion` in the
     * product and it is mounted a few lines below. A rail that announced its own switch would need
     * a region of its own, and two regions is how one confirmation gets announced twice.
     *
     * `setRouteCluster(undefined)` hands the frame back to the stored selection for the instant
     * between the write and the address catching up. Without it the accessor would go on preferring
     * the *old* cluster's name in the address, and a switch made from a cluster-scoped page would
     * appear to do nothing at all.
     */
    const switchEnvironment = (id: string): void => {
      const change = environmentSwitch(
        id,
        clusterForFrame(),
        clusters().find((entry) => entry.id === id)?.name,
        clusterInUrl(props.route.location.pathname, uiPrefix) !== undefined,
      );
      if (change === undefined) return;

      cluster.select(id);
      setRouteCluster(undefined);
      notify(change.title, { tone: "info", message: change.message });
      if (change.rewriteAddress) navigate(paths.dashboard(id), AlreadyPrefixed);
    };

    /**
     * The rows nested under Topics: `SCREENS-V4.md` §2.2's tree.
     *
     * The fold is `nav/topicTree.ts`'s and the cap is `nav/prefixes.ts`'s; nothing is re-folded
     * here and no name is grouped in a component. What this supplies is the two things only the
     * frame knows — which cluster it is, and how to spell an address — and it reads the names
     * through the same store the badges and the meter read, so expanding the tree costs one request
     * and not one per row.
     *
     * `undefined` in all three of the cases below, and the three are genuinely different states
     * that happen to draw the same row: no cluster is chosen, the names have not arrived, and the
     * cluster has no topics. The first two are decided here because only the frame knows them; the
     * third is `topicSubtree`'s, beside the fold, where a case can call it — it used to be a
     * `names.length === 0` clause on this line defended by a comment naming `NavItem` as the real
     * guard, and `NavItem`'s predicate was deletable at the same time with every case
     * `pnpm -C frontend test packages/shell` runs still green — the count is whatever that command
     * prints today, which is the point: a number frozen into this sentence would stop being true
     * the next time anybody added a case.
     */
    const topicChildren = createMemo<readonly NavDestination[] | undefined>(() => {
      const chosen = clusterForFrame();
      if (chosen === undefined) return undefined;
      const names = readingValue(facts.topicNames);
      if (names === undefined) return undefined;
      return topicSubtree({
        names,
        /* The list, asked for the topics this row stands for. `topicGroupHref` owns the three cases
           — a prefix, `internal`, and the `other` residue that is not describable as a search. */
        groupHref: (group) => topicGroupHref(paths.topics(chosen), group),
      });
    });

    const groups = createMemo(() =>
      navigationGroups({
        features: statuses(),
        landingFor: (registration, chosen) => landingFor(Router, registration.id, chosen),
        cluster: clusterForFrame(),
        shellDestinations: shellDestinations(Router),
        /* The badges the drawer draws, from the one store above. `navigationGroups` already owns
           every rule about what happens to them — the capability badge wins over a count, an
           unknown count is no badge rather than a `0`, and the tone follows the meaning — so all
           that is supplied here is the lookup. */
        countFor: countLookup(readingValue(facts.counts), alertsBadge(alerts.openCount())),
        /* Only Topics nests. Brokers and Consumers have no tree in the design and no fold behind
           one, and a lookup that answered for every feature would be a promise this shell cannot
           keep. */
        childrenFor: (registration) =>
          registration.id === "topics" ? topicChildren() : undefined,
      }),
    );

    /**
     * The cluster the head describes.
     *
     * Two sources, and the order matters. The store's summary is the cluster's own report — the
     * version, the broker count and the under-replicated count the caption is made of — and the
     * capability row knows only what the gateway thinks of the services in front of it. So the
     * scrape wins when there is one, and the capability row is what remains when there is not:
     * a cluster whose cluster service is unreachable has no scrape at all, and the row still
     * carries its name and an `unreachable` dot, which is exactly what the head should draw.
     */
    const clusterBlock = createMemo<ClusterSummary | undefined>(() => {
      const row = clusters().find((entry) => entry.id === clusterForFrame());
      return readingValue(facts.summary) ?? row;
    });

    return (
      <>
        <AppFrame
          rail={
            <EnvRail
              environments={clusters()}
              currentId={clusterForFrame()}
              onSelect={switchEnvironment}
              destinations={railDestinations(Router)}
              homeHref={Router.paths()}
              accountName={session.signedIn() ? session.identity()?.principal.name : undefined}
              /* No handler where there is no session: the avatar stays a picture rather than
                 becoming a button that opens a panel offering to end nothing. */
              onOpenAccount={canSignOut() ? () => setAccountOpen(!accountOpen()) : undefined}
              accountOpen={accountOpen()}
              accountPanel={
                canSignOut() ? (
                  <AccountMenu
                    name={session.identity()?.principal.name ?? ""}
                    authType={session.settings()?.authType}
                    busy={signOut.busy()}
                    failure={
                      signOut.state().kind === "failed" || signOut.state().kind === "forbidden"
                        ? "Signing out did not work. You are still signed in."
                        : undefined
                    }
                    onSignOut={() => {
                      void signOut.run().then((outcome) => {
                        // Only on success. Reloading after a refusal would redraw a signed-in
                        // shell, which is indistinguishable from a sign-out that worked.
                        if (outcome.kind === "done")
                          void alertCache.clear().finally(() => window.location.reload());
                      });
                    }}
                  />
                ) : undefined
              }
            />
          }
          drawer={
            <NavDrawer
              groups={groups()}
              currentId={currentFeatureId(props.route.location.pathname, uiPrefix)}
              cluster={clusterBlock()}
              /* `brokerStorageOf` and not `facts.storage`: the meter takes an array, and the
                 reading's `kind` is what tells "still loading" from "could not be read". It is
                 flattened here, where that distinction has already been used, and the meter goes on
                 drawing its own "not known" rendering for an empty array — the right picture
                 for all three of the ways this reading can carry no value, and emphatically not a
                 row of zeros, which draws an empty bar and reads as "your disks are empty". */
              storage={brokerStorageOf(facts.storage)}
              /* The `+` at the head, and the status card's own button when there is no cluster for
                 the head to describe. They are the only routes to cluster registration in
                 twenty-three screens, and both take their address from `KuiPaths` rather than
                 from a literal written here — one page, one spelling. */
              manageHref={paths.manageClusters()}
              configureHref={paths.manageClusters()}
            />
          }
          topbar={
            <TopBar
              crumbs={topCrumbs(
                clusters(),
                clusterForFrame(),
                props.route.location.pathname,
                uiPrefix,
                Router,
              )}
              /* The field, wired to the gateway's cross-entity search. Everything about *what* it
               shows is decided in `data/search.ts` and handed over as plain data, so the four
               states the overlay can be in — nothing asked for, asking, an answer, a failure — are
               each reachable in a story with no server. `inputRef` is how the `⌘K` bound above
               reaches the element; a hint for a key that does nothing teaches the reader that
               shortcuts do not work. */
              search={{
                value: searchText(),
                onInput: onSearchInput,
                status: searchStatus(searchState()),
                maxLength: SEARCH_MAX_LENGTH,
                results: searchResults(),
                /* The services the fold could not ask, in words. Reported rather than dropped: the
                   distributed stack routes no schema service, so a search that returned two lists
                   out of three would tell an operator their subject does not exist. */
                unavailable: searchUnavailable(),
                onRetry: () => {
                  const query = searchText().trim();
                  if (query.length > 0) runSearch(query, invalidateSearch());
                },
                inputRef: (el) => {
                  searchInput = el;
                },
              }}
              /* The kernel's own preference singletons, and not a copy of what they currently
                 say. `SettingsPage` writes the same three, and the popover in the top bar has to
                 be the *same* control rather than a second one: two spellings of one preference is
                 how a product ends up with a theme switch that the settings page disagrees with.
                 Passing the preferences rather than a mode is also what removes the shell's own
                 read of `data-theme` — that attribute is written *by* the preference, so reading it
                 back was the frame asking the stylesheet what it had just been told. */
              appearance={{ ...appearance.preferences, persistence: appearance.status }}
              notificationsOpen={noticesOpen()}
              onToggleNotifications={() => setNoticesOpen(!noticesOpen())}
              /* The bell's two facts, and neither is folded here. The figure is the alerts
                 service's own **cluster-wide** open count, straight off the kernel store's one
                 derivation — `null` while nothing has said, and `null` too for a `0` the service's
                 rules have never actually produced — and the mark is that service's
                 **per-principal** unread count, a different number counted over the same store.
                 Both are the server's; the browser recomputes neither, because the feed is paged
                 and the bell is not. The drawer's Alerts badge, this bell and the dashboard's
                 alerts card call the same accessor, which is what makes the three unable to
                 disagree — a second derivation in `data/alerts.ts` was deleted this wave for
                 exactly that reason. */
              alertsOpen={alerts.openCount()}
              alertsUnread={alerts.unread()}
              /* The panel is the same feed the bell counts, which is what §4.16 means by putting it
                 in the frame rather than on a page. It was a hard-coded empty list for three waves
                 because there was no service behind it; there is one now, and a bell counting one
                 feed over a panel listing another would be two answers to one question. */
              notifications={noticesOf(
                alerts.feed(),
                landingFor(Router, "alerts", clusterForFrame()),
              )}
              onMarkAllRead={() => alerts.markAllRead()}
              /* Only where trying again could help. `forbidden` and `not_configured` arrive as
                 sentences with no control beside them: a Try-again under a refusal teaches
                 operators to press a button that cannot work. */
              onRetryNotifications={
                alerts.feed().kind === "failed" ? () => alerts.refresh() : undefined
              }
            />
          }
        >
          <Show when={banner()}>
            {(message) => (
              <Banner
                tone="warning"
                message={message()}
                testId="capability-banner"
                /* A cluster that is not answering must not be dismissible: dismissing it makes
                 every stale number on the page look current. */
              />
            )}
          </Show>

          {props.route.children}
        </AppFrame>

        <ToastRegion />

        {/* The full-screen states, in the one order that is correct. */}
        <Show when={health.connectivity().kind === "lost"}>
          <GatewayUnreachablePage state={health.connectivity()} onRetry={health.retryNow} />
        </Show>
        <Show when={health.connectivity().kind === "connected" && session.mustSignIn()}>
          <SignIn
            authType={session.authType() ?? "form"}
            providerLabel={session.settings()?.providerLabel}
            api={api}
            onSignedIn={() => {
              /*
               * A reload, deliberately, and not a re-fetch.
               *
               * Every store in this shell — the permissions, the capability fold, the cluster
               * list, each feature's own data — was populated as the *anonymous* principal while
               * the sign-in screen was on top of it. Re-fetching a few of them by hand is a list
               * somebody will one day fail to keep up to date, and the failure mode is the worst
               * kind: a signed-in operator looking at what anonymous was allowed to see, with no
               * indication that anything is missing. A reload cannot get that list wrong.
               */
              window.location.reload();
            }}
          />
        </Show>
      </>
    );
  };

  return (
    <AlertsProvider value={alerts}>
      <Router>
        {(route: RouteSectionProps) => (
          <KuiProvider value={featureContext}>
            <Frame route={route} />
          </KuiProvider>
        )}
      </Router>
    </AlertsProvider>
  );
}

/**
 * Navigating to an address {@link shellPaths} built.
 *
 * `useNavigate`'s default is to treat a leading-`/` string as base-*relative* and prefix the
 * router's base onto it, which is right for a hand-written `"/settings"` and wrong for everything
 * this shell has: every address it holds comes from the router's typed proxy, which has already
 * applied the base. So the default turned `/ui/clusters/x/topics` into `/ui/ui/clusters/x/topics`,
 * which matches no route and drew the 404 page.
 *
 * Both of the shell's navigations had it — `+ Create topic` and the environment rail — and neither
 * was ever exercised by a test, so the two wirings that wave 2 recorded as "done" produced a 404
 * every time anybody used them. `resolve: false` says the string is already the final path, which
 * is exactly what a `KuiPaths` address is.
 */
const AlreadyPrefixed = { resolve: false } as const;

/**
 * The rail's shortcut glyphs.
 *
 * Only destinations the shell itself owns, because a shortcut whose service is not configured must
 * not be drawn at all — a rail is a set of shortcuts, and a dead shortcut costs the operator the
 * attention it takes to discover it does nothing. The ecosystem glyphs the design shows there join
 * this list when their features exist and their capabilities say so.
 */
function railDestinations(Router: ShellRouter): readonly RailDestination[] {
  return [
    {
      id: "settings",
      label: "Settings",
      icon: "settings",
      href: Router.paths.settings(),
      atFoot: true,
    },
  ];
}

/**
 * A section the top band can name: every feature this build can contain, plus the shell's own two.
 *
 * A closed union rather than `string`, and that is the whole of {@link topCrumbs}' correctness.
 * `currentFeatureId` returns one of these, `LABELS` is a total map over them, and the ninth feature
 * cannot be added to `FeatureId` without the crumb table refusing to compile — which is what
 * `landingFor`'s exhaustive switch already does for the landing route and is why `ksql` was
 * impossible to forget there and easy to forget here.
 */
export type CrumbSection = FeatureId | "settings" | "overview";

/**
 * The top band's trail: the cluster, then the section.
 *
 * It always begins with the cluster, because this is the *installation* trail — its job is to say
 * which deployment and which cluster you are looking at, which is the question the environment rail
 * answers by colour and this answers in words. An object page adds its own, shorter breadcrumb in
 * the content column; the two are not redundant.
 *
 * ## Why the label table is total over {@link CrumbSection} and not a `Record<string, string>`
 *
 * It was the looser type, and the failure that type permits is not a blank crumb: a lookup that
 * misses yields `undefined`, the `if` below declines to push, and what the band then draws is the
 * cluster name alone — **which is exactly the trail the cluster's dashboard draws.** A
 * correct-looking answer for the wrong screen, on every page of a section somebody forgot to name.
 * W8-07 measured it on the one row that had no case: deleting `settings: "Settings"` left all 536
 * shell cases green.
 *
 * Typed this way, a forgotten section is a type error at the table, before any test runs. The
 * values are `string | undefined` rather than `string` so that "this section deliberately has no
 * crumb" is a thing the table can *say*; a `Record` property is required whether or not its type
 * admits `undefined`, so omitting a key is still the compile error — the option is to write the
 * silence down, not to leave it out.
 */
export function topCrumbs(
  clusters: readonly ClusterSummary[],
  selected: string | undefined,
  pathname: string,
  uiPrefix: string,
  Router: ShellRouter,
): readonly Crumb[] {
  if (selected === undefined) return [];
  const name = clusters.find((entry) => entry.id === selected)?.name ?? selected;
  const trail: Crumb[] = [{ label: name, href: Router.paths() }];

  const section = currentFeatureId(pathname, uiPrefix);
  const LABELS: Record<CrumbSection, string | undefined> = {
    clusters: "Brokers",
    topics: "Topics",
    /* The record browser, which hangs off a topic and is reached from a topic's page — so under
       `/topics/<name>/messages` the section is `topics` and the trail says Topics, the page you
       navigated from. This label is for `/messages/track`, the cross-topic search, which is the one
       address this feature owns on its own. */
    messages: "Messages",
    consumers: "Consumers",
    schemas: "Schema Registry",
    alerts: "Alerts",
    connect: "Connect",
    /* The product's word, not the service's. `SCREENS-V4.md` §4.15 draws the row, the heading and
       the breadcrumb as `ksqlDB` — one capital in the middle — and the feature id is `ksql` because
       an id is a path segment. Spelling the crumb from the id would put `Ksql` in the top band. */
    ksql: "ksqlDB",
    settings: "Settings",
    /* `undefined` on purpose, and written down rather than omitted: "overview" adds nothing,
       because the cluster crumb already links there and a trail that repeats itself is a trail
       nobody reads. Leaving the key out would say the same thing on screen and a different thing to
       the next reader — that somebody forgot — which is the confusion this table exists to end. */
    overview: undefined,
  };
  const label = section === undefined ? undefined : LABELS[section];
  if (label !== undefined) trail.push({ label });
  return trail;
}

/**
 * The shell's own destinations.
 *
 * They have no service behind them — they are the frame, not a feature — so they are always
 * reachable, and their hrefs come from the same typed proxy every other link uses.
 */
function shellDestinations(Router: ShellRouter): readonly NavDestination[] {
  return [
    { id: "overview", label: "Overview", icon: "dashboard", href: Router.paths(), state: "ready" },
    {
      id: "settings",
      label: "Settings",
      icon: "settings",
      href: Router.paths.settings(),
      state: "ready",
    },
  ];
}

/**
 * One row per cluster, folded to the *worst* of its services.
 *
 * A cluster whose topic service is fine and whose cluster service is unreachable is not a healthy
 * cluster, and a dot reporting the best of its services would be reassuring and wrong. The name is
 * the one the gateway reported; the id is the fallback, which degrades rather than showing a blank
 * row.
 */
export function clusterSummaries(
  entries: ReadonlyMap<
    string,
    {
      readonly key: { readonly cluster?: string | undefined };
      readonly state: { readonly status: string };
      readonly name?: string | undefined;
    }
  >,
): readonly ClusterSummary[] {
  const worst = new Map<string, { health: ClusterSummary["health"]; name: string }>();

  for (const entry of entries.values()) {
    const id = entry.key.cluster;
    if (id === undefined) continue;
    const health = healthOf(entry.state.status);
    const existing = worst.get(id);
    worst.set(id, {
      health: existing === undefined ? health : worseOf(existing.health, health),
      name: entry.name ?? existing?.name ?? id,
    });
  }

  return [...worst]
    .map(([id, value]) => ({ id, name: value.name, health: value.health }))
    .sort((a, b) => a.name.localeCompare(b.name) || a.id.localeCompare(b.id));
}

const SEVERITY: Record<ClusterSummary["health"], number> = {
  unreachable: 0,
  degraded: 1,
  unknown: 2,
  healthy: 3,
};

function worseOf(
  a: ClusterSummary["health"],
  b: ClusterSummary["health"],
): ClusterSummary["health"] {
  return SEVERITY[a] <= SEVERITY[b] ? a : b;
}

function healthOf(status: string): ClusterSummary["health"] {
  switch (status) {
    case "available":
      return "healthy";
    case "degraded":
      return "degraded";
    case "unavailable":
      return "unreachable";
    default:
      // "not configured" is not a health claim, and neither is a status this build does not know.
      return "unknown";
  }
}

/** What changing environment amounts to, before anything has been changed. */
export type EnvironmentSwitch = {
  readonly title: string;
  readonly message: string;
  /**
   * Whether the address has to be rewritten to the new cluster's dashboard.
   *
   * True exactly when the current address names a cluster. Leaving it alone then would put a URL
   * saying `prod` in front of a frame describing `staging`, and the address is the half of that
   * pair people copy and paste. When the address names no cluster — `/ui/settings`, say — there is
   * no contradiction to resolve, and moving somebody off the page they deliberately opened is the
   * rudeness that `soleClusterChoice` above is careful to avoid for the same reason.
   */
  readonly rewriteAddress: boolean;
};

/**
 * The decision behind the environment rail, separated from the four side effects that carry it out.
 *
 * Selecting a cluster writes a store, raises a toast, resets a signal and may navigate, and none of
 * those can be asserted without a mounted shell and a capability stream. What is worth asserting is
 * none of them: it is that switching to the cluster you are already on does nothing at all, that
 * the sentence names the cluster the way the operator named it, and that the address is rewritten
 * only when it disagrees. So the decision is a function of plain data and the effects are the
 * caller's.
 *
 * `undefined` for a switch to the current cluster. Not a toast saying nothing changed: the rail
 * marks the current environment, so this is a misclick, and a confirmation for a misclick teaches
 * the operator that the toasts are noise.
 */
export function environmentSwitch(
  to: string,
  from: string | undefined,
  name: string | undefined,
  addressNamesCluster: boolean,
): EnvironmentSwitch | undefined {
  if (to === from) return undefined;
  return {
    /* The operator's own name for the cluster when the gateway reported one, and the identifier
       when it did not — the same degradation `clusterSummaries` makes, and for the same reason: a
       blank where a name goes reads as a bug in the toast rather than as a cluster nobody named. */
    title: `Switched to ${name ?? to}`,
    message: "Every panel in the frame now describes this cluster.",
    rewriteAddress: addressNamesCluster,
  };
}

/**
 * The drawer's badge lookup, over whatever the store has learned so far.
 *
 * A function of the registration rather than the table itself, because that is what
 * `navigationGroups` takes and its reason for taking one is worth keeping: the numbers come from a
 * store that fetches, and the fold over capability states has to stay a pure function of plain
 * data. The table is keyed by `FeatureId`, so a renamed feature loses its badge as a type error
 * rather than silently — and a silently missing badge is indistinguishable on screen from a count
 * that could not be fetched.
 *
 * An absent count comes back `undefined`, which the fold draws as no badge at all. It is never
 * turned into a `0`: `Topics 0` beside a cluster whose topic service did not answer is a statement
 * about the cluster, and a false one.
 */
export function countLookup(
  counts: NavCounts | undefined,
  alerts?: NavCount | undefined,
): (feature: FeatureRegistration) => NavCount | undefined {
  /* Alerts comes from a different store and is therefore a different argument, not a sixth member
     of the table. The four in `counts` are the cluster store's — one query cache, four endpoints,
     one `Reading` — while this one is the kernel's alert feed, held open by a stream for the whole
     life of the page. Folding it into `NavCounts` would mean the cluster store either fetching a
     feed it does not own or being handed one, and the drawer's badge would then be a *copy* of the
     bell's number rather than the same number. */
  return (feature) => (feature.id === "alerts" ? alerts : counts?.[feature.id]);
}

/**
 * Which navigation entry to mark as current, from the address the browser is on.
 *
 * The fall-through is `clusters`, and that is right for `/clusters` and for a broker's page and
 * wrong for the two addresses that were reaching it by accident. A cluster's **dashboard** —
 * `/clusters/<id>` and `/clusters/<id>/dashboard/<tab>`, which is the address the product opens on
 * — marked *Brokers* as the current entry and put "Brokers" in the top band's trail, over a page
 * headed "Cluster overview". Three signals, two of them wrong, on the first screen anybody sees.
 * The registry's own screen had the same defect one row down.
 *
 * `manage` is excluded from the dashboard test for the reason `clusterInUrl` excludes it: it is a
 * page of the cluster list, not the id of a cluster.
 *
 * The return type is {@link CrumbSection} rather than `string`, so that this function and
 * {@link topCrumbs}' label table cannot part company: a section this returns that the table does
 * not name is a compile error at the table, and a word returned here that is no feature id — a
 * typo, a service name where a feature id belongs — is a compile error at the `return`.
 */
export function currentFeatureId(pathname: string, uiPrefix: string): CrumbSection | undefined {
  const relative = pathname.startsWith(uiPrefix) ? pathname.slice(uiPrefix.length) : pathname;
  const segments = relative.split("/").filter((segment) => segment.length > 0);
  if (segments.length === 0) return "overview";
  if (segments[0] === "settings") return "settings";
  if (segments[0] !== "clusters") return undefined;
  /* The dashboard is decided **before** the sections below, and that ordering is the rule rather
     than a tidy-up. Every test under it is a `segments.includes`, which cannot tell a section from
     a tab of the same name — and `alerts` is exactly that: a route of its own at
     `/clusters/<id>/alerts`, and a tab the design draws at `SCREENS-V4.md` §4.4. An unrecognised
     tab segment falls back to the overview (`overview/tabs.ts`), so without this line
     `/dashboard/alerts` drew the overview while the drawer highlighted Alerts and the trail said
     "Alerts" — three signals, two of them wrong, which is the defect the dashboard and the registry
     each had until wave 5 and the reason the case below is written. */
  if (segments[1] !== undefined && segments[2] === "dashboard") return "overview";
  if (segments.includes("topics")) return "topics";
  if (segments.includes("consumer-groups")) return "consumers";
  if (segments.includes("schemas")) return "schemas";
  if (segments.includes("alerts")) return "alerts";
  if (segments.includes("connect")) return "connect";
  if (segments.includes("ksql")) return "ksql";
  /* After `topics`, and that ordering is the rule rather than an accident of where it was typed.
     The record browser lives at `/topics/<name>/messages` and belongs to Topics — it is opened from
     a topic's page and the trail should lead back there. `/messages/track` is the other address the
     same feature owns, it names no topic, and every test above it misses it: before this line it
     fell through to the `/clusters/<id>` arm below and drew the cluster **dashboard's** trail over
     the cross-topic search, which is the failure `topCrumbs`' header describes and the reason that
     table is now total. The drawer highlights nothing for it, which is correct: `messages` is the
     one registration with `sidebar: false`, and highlighting Overview — which is what happened
     until this line existed — pointed at a page the operator was not on. */
  if (segments.includes("messages")) return "messages";
  // `/clusters/<id>`, with or without `/dashboard/<tab>` after it. Both are the same page.
  if (segments[1] !== undefined && segments[1] !== "manage" && !segments.includes("brokers")) {
    return "overview";
  }
  return "clusters";
}

/**
 * `localStorage`, when there is one.
 *
 * Reaching for it can itself throw — a browser configured to block site data raises on the accessor,
 * not on the call — so even asking the question is wrapped.
 */
function safeLocalStorage(): Storage | undefined {
  try {
    return window.localStorage;
  } catch {
    return undefined;
  }
}

/* A placeholder for the one shell-owned screen that is still to come. It renders something honest
 * rather than nothing, because a route that renders nothing is a blank content area and users read a
 * blank page as a broken page. (The overview is no longer among these: it is the real screen now.) */

/**
 * Opens a stream once the session has been established, and stays closable in the meantime.
 *
 * The handle is returned immediately because the caller needs one — `createCapabilities` stores it
 * and calls `close()` on teardown, and a component that unmounts during start-up must not leave a
 * stream to open behind it. So this stands in for the real handle: `close()` before the session
 * settles cancels the opening rather than closing something that does not exist yet.
 *
 * `connection` reports `connecting` while waiting, which is what it is. Reporting `open` would put a
 * green indicator on a stream with no socket, and reporting an error would send somebody to look at
 * a network that is fine.
 */
function deferUntilSession(open: () => SseHandle, csrf: CsrfTokens): SseHandle {
  /*
   * A *signal*, not a plain variable.
   *
   * `connection()` is read inside an effect — that is how the frame's connectivity banner follows
   * the stream — and an accessor that closes over an ordinary variable gives Solid nothing to
   * subscribe to. The effect therefore ran once, read `connecting`, and never ran again: the stream
   * opened, the server sent frames, and the application went on saying "KUI has lost its live
   * connection to the gateway" for the rest of the session. A banner that is wrong in the reassuring
   * direction would be bad; this one was wrong in the alarming direction, which teaches operators to
   * ignore it.
   */
  const [opened, setOpened] = createSignal<SseHandle | undefined>(undefined, { ownedWrite: true });
  let cancelled = false;

  void csrf.waitForToken().then(() => {
    if (!cancelled) setOpened(() => open());
  });

  return {
    connection: () => opened()?.connection() ?? { phase: "connecting" },
    close: () => {
      cancelled = true;
      opened()?.close();
    },
    endMarker: () => opened()?.endMarker(),
  };
}
