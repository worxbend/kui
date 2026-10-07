/**
 * The clusters feature's route entry: the screen the shell renders for every `/clusters…` address.
 *
 * ## One component for three addresses
 *
 * The shell hands a feature the same root for every route it owns, so this component decides which
 * of its screens to draw from the address bar. That is the shape ADR-012 chose deliberately — the
 * alternative, a component per route, means the shell has to know the feature's internal structure
 * before it has downloaded it.
 *
 * The addresses this owns:
 *
 *   /clusters                          the cluster list
 *   /clusters/:id/brokers              that cluster's brokers
 *   /clusters/:id/brokers/:brokerId    one broker: its disks and its settings
 *
 * ## What it does when nothing has arrived
 *
 * Never a spinner over the whole page. The screens each take a finished view model plus a state,
 * and their loading rendering is a skeleton in the shape of the answer, so the page does not jump
 * when the data lands. A failure is the screen's own failure panel with the reason and a retry that
 * works — not a thrown error, and never an empty list, which reads as "this cluster has no brokers"
 * when it means "nobody answered".
 *
 * ## How it reads
 *
 * Through `useQuery`, and not through the twenty-line `useFetch` this file used to carry. The
 * kernel's version is the one with the behaviour the copy did not have: a refetch that fails keeps
 * the last good value and marks it stale rather than blanking a panel that was showing real
 * figures, and two components asking for the same key share one request — which is what makes the
 * broker page below draw a broker's identity immediately when it is opened from the list, off the
 * document the list already has.
 */
import { Show, createEffect, createSignal } from "solid-js";
import type { JSX } from "@solidjs/web";
import {
  Button,
  Card,
  EmptyState,
  PageHeader,
  createMutation,
  notify,
  sharedQueries,
  useKui,
  useQuery,
  valueOf,
  type Fetched,
} from "@kui/kernel";
import { Actions } from "@kui/api";
import { useLocation, useNavigate, useParams } from "@solidjs/router";
import { ClusterList } from "./ClusterList.jsx";
import { BrokersScreen, failureOf } from "./BrokersScreen.jsx";
import { BrokerDetail, type BrokerTabKey, type Loaded } from "./BrokerDetail.jsx";
import { ClusterAdmin, type Connectivity } from "./ClusterAdmin.jsx";
import { EMPTY_CLUSTER_FORM, formFor, toRequest, type ClusterForm } from "./clusterForm.js";
import {
  deleteCluster,
  fetchBrokerConfigs,
  fetchBrokerLogDirs,
  fetchBrokers,
  fetchClusters,
  fetchManagedClusters,
  saveCluster,
  testConnection,
  type ManagedClusterRow,
} from "./data.js";
import type { Broker, ClusterSummary, ConfigEntry, LogDir } from "./model.js";

/**
 * The keys this route's queries are held under.
 *
 * Spelled out in one place because a key is a promise — everything that changes the request is in
 * the string — and because two of these ask the same endpoint for different mappings. `clusters`
 * and `managed-clusters` are both `GET /clusters`; one produces the list screen's rows and the
 * other the administration screen's registrations, and sharing a key would hand one screen the
 * other's answer. The manage key is not `clusters/manage` for the same reason the path check below
 * is not route ordering: a cluster may perfectly well be called `manage`.
 */
const CLUSTERS_KEY = "clusters";
const MANAGED_KEY = "managed-clusters";

/** Everything about clusters is out of date after a registration changes. */
function forgetClusters(): void {
  sharedQueries.invalidateWhere(
    (key) => key === CLUSTERS_KEY || key === MANAGED_KEY || key.startsWith("clusters/"),
  );
}

export default function Clusters(): JSX.Element {
  const params = useParams<{ readonly clusterId?: string; readonly brokerId?: string }>();
  const location = useLocation();

  /*
   * `/clusters/manage` is a sibling of `/clusters/:clusterId`, and the router matches the parameter
   * route for it — `manage` is a perfectly good cluster id as far as the pattern is concerned. So
   * the path is checked here rather than relying on route ordering, which would make a cluster
   * genuinely named `manage` unreachable instead.
   */
  const managing = () => location.pathname.replace(/\/+$/, "").endsWith("/clusters/manage");

  return (
    <Show when={!managing()} fallback={<ManageScreen />}>
      <Show when={params.clusterId} fallback={<ClustersScreen />}>
        {(clusterId) => (
          <Show when={params.brokerId} fallback={<BrokersRoute clusterId={clusterId()} />}>
            {(brokerId) => <BrokerScreen clusterId={clusterId()} brokerId={brokerId()} />}
          </Show>
        )}
      </Show>
    </Show>
  );
}

/**
 * Adding, changing and removing the clusters KUI knows about.
 *
 * This screen is about KUI's *own* configuration rather than about a Kafka cluster's settings, which
 * is why it asks for `ApplicationConfig` rather than `ClusterConfig`: the difference is the list of
 * which clusters exist and how to reach them, versus a broker's own configuration.
 *
 * Exported because it is where the write path ends: the mutation resolves here, and so does the
 * toast that is the only thing on screen saying a removal happened. A test that reaches it through
 * the route would need a router it has nothing to say about; a test that reaches it directly is
 * still driving the product's own component and its own handlers.
 */
export function ManageScreen(): JSX.Element {
  const kui = useKui();
  const { state, reload } = useQuery<readonly ManagedClusterRow[]>({
    key: () => MANAGED_KEY,
    load: () => fetchManagedClusters(kui.api),
  });

  const [editing, setEditing] = createSignal<
    { readonly id: string | undefined; readonly form: ClusterForm } | undefined
  >(undefined);
  const [connectivity, setConnectivity] = createSignal<Connectivity | undefined>(undefined);
  const [tested, setTested] = createSignal<ReturnType<typeof editing>>(undefined);

  /** The version of the record being replaced, or `undefined` for a create. */
  const version = (): number | undefined => rows().find((row) => row.id === editing()?.id)?.version;

  const rows = () => valueOf(state(), []);

  /*
   * Both mutations take the built request rather than reading the form themselves.
   *
   * The alternative needed a failure value for "there is no valid form", and the only ones available
   * describe the *network* — so a form with an empty name would have been reported as the gateway
   * being unreachable. Building at the call site, where the answer is already known, means the
   * unrepresentable state stays unrepresentable.
   */
  const save = createMutation(
    (clusterId: string, request: Record<string, unknown>, at: number | undefined) =>
      saveCluster(kui.api, clusterId, request, at),
  );

  const test = createMutation((request: Record<string, unknown>) =>
    testConnection(kui.api, request),
  );

  const remove = createMutation((cluster: ManagedClusterRow) =>
    deleteCluster(kui.api, cluster.id, cluster.version),
  );

  // Registration changes application configuration, not Kafka data. Do not use the cluster
  // write policy here: new clusters have no policy yet, and read-only must remain editable.
  const mayEdit = () => kui.permits(Actions.ApplicationConfigEdit);

  /**
   * Clears everything the last form left behind.
   *
   * The connectivity result especially: a green "KUI reached this cluster" left over from the
   * cluster somebody was looking at a moment ago, sitting under a different cluster's settings, is
   * the most misleading thing this screen could show.
   */
  const forget = (): void => {
    setConnectivity(undefined);
    setTested(undefined);
    save.reset();
    test.reset();
  };

  return (
    <ClusterAdmin
      clusters={rows()}
      loading={state().kind === "loading"}
      failure={failureOf(state(), reload)}
      editing={editing()}
      onAdd={() => {
        forget();
        setEditing({ id: undefined, form: EMPTY_CLUSTER_FORM });
      }}
      onEdit={(cluster) => {
        forget();
        setEditing({ id: cluster.id, form: formFor(cluster) });
      }}
      onCancel={() => {
        forget();
        setEditing(undefined);
      }}
      onFormChange={(form) => {
        const current = editing();
        if (current !== undefined) setEditing({ id: current.id, form });
        // The result described the settings as they were, not as they now are.
        setConnectivity(undefined);
        test.reset();
      }}
      onSave={() => {
        if (!mayEdit()) return;
        const current = editing();
        if (current === undefined) return;
        const built = toRequest(current.form);
        // The button is disabled while this is false; the check is here as well because a form can
        // also be submitted with Enter.
        if (!built.ok) return;
        const adding = current.id === undefined;
        void save
          .run(current.id ?? current.form.id.trim(), built.request, version())
          .then((outcome) => {
            if (outcome.kind !== "done") return;
            setEditing(undefined);
            /* The form closing is the only thing on screen that changes, and that is also what
               cancelling looks like. The toast distinguishes "saved" from "gave up". */
            notify(adding ? "Cluster added" : "Cluster saved", {
              message: `KUI now reaches ${current.form.name} at ${current.form.bootstrapServers}.`,
            });
            forgetClusters();
            reload();
          });
      }}
      onTest={() => {
        if (!mayEdit()) return;
        const current = editing();
        if (current === undefined || test.busy()) return;
        const built = toRequest(current.form);
        if (!built.ok) return;
        setTested(current);
        void test.run(built.request).then((outcome) => {
          if (outcome.kind !== "done") return;
          // `editing()` is replaced wholesale on every keystroke and on cancel/switch, so identity
          // here means "still the same form this result was tested against"; a slower response for
          // a form the user has since changed or left is discarded rather than painted underneath
          // whatever is on screen now.
          if (editing() !== current) return;
          setConnectivity(outcome.value as Connectivity);
        });
      }}
      onDelete={async (cluster): Promise<boolean> => {
        if (!mayEdit()) return false;
        return remove.run(cluster).then((outcome) => {
          if (outcome.kind !== "done") return false;
          /* A destructive success is the one case with nothing left on screen to confirm it: the
             row is gone, and a row that is gone looks exactly like one that was never there. */
          notify("Cluster removed", {
            message:
              `${cluster.name} is no longer registered. Its Kafka cluster and its data are untouched.`,
          });
          forgetClusters();
          reload();
          return true;
        });
      }}
      connectivity={connectivity()}
      saveState={save.state()}
      testState={test.busy() || tested() === editing() ? test.state() : { kind: "idle" }}
      deleteState={remove.state()}
      disabledReason={
        mayEdit()
          ? undefined
          : "You do not have permission to change which clusters KUI knows about."
      }
    />
  );
}

function ClustersScreen(): JSX.Element {
  const kui = useKui();
  const { state, reload } = useQuery<readonly ClusterSummary[]>({
    key: () => CLUSTERS_KEY,
    load: () => fetchClusters(kui.api),
  });

  createEffect(
    () => state(),
    (current) => {
      // The shell decides what a failure means for connectivity; the feature only says whether the
      // call came back.
      if (current.kind !== "loading") kui.report("feature", current.kind === "failed");
    },
  );

  return (
    <ClusterList
      clusters={valueOf(state(), [])}
      loading={state().kind === "loading"}
      failure={failureOf(state(), reload)}
      hrefFor={(id) => kui.paths.brokers(id)}
    />
  );
}

/**
 * The brokers screen.
 *
 * Two lines, because everything the screen does — three requests, the settings fetched on
 * expansion, the disks folded into the brokers — lives in `BrokersScreen.tsx`, where a test can
 * mount it with nothing but an API client. What is left here is what only the router knows: which
 * cluster, and where its links go.
 */
function BrokersRoute(props: { readonly clusterId: string }): JSX.Element {
  const kui = useKui();
  return (
    <BrokersScreen
      clusterId={props.clusterId}
      clustersHref={kui.paths.clusters()}
      hrefFor={(brokerId) => kui.paths.broker(props.clusterId, brokerId)}
    />
  );
}

/**
 * One broker: the page the broker list has been linking to since it was written.
 *
 * ## Three requests, three fates
 *
 * The broker's identity comes from the cluster's broker *list* — there is no per-broker endpoint,
 * and the row in that list is where the host, the port, the rack and the controller flag live. Its
 * disks and its settings come from two more endpoints, each of which fails on its own and neither of
 * which may blank the other; `BrokerDetail` already draws that, so all this does is keep the three
 * states apart on the way in.
 *
 * ## The settings are not fetched until the tab is opened
 *
 * `describeConfigs` on an ordinary broker is three hundred and forty rows and sixty kilobytes.
 * Somebody who came to see which disk is filling up should not pay for it, so the request is made
 * when the configuration tab is selected and not before. Coming back to it later fetches again
 * rather than holding the last answer, which is the right way round for a page whose whole purpose
 * is to show what a broker is configured with *now*.
 *
 * ## The tab is in the address, not in a signal
 *
 * `?tab=configuration`, so that the tab somebody is looking at is the tab in the link they send, and
 * so that Back leaves the tab where Back should leave it. The same shape the topic page uses.
 */
function BrokerScreen(props: {
  readonly clusterId: string;
  readonly brokerId: string;
}): JSX.Element {
  const kui = useKui();
  const navigate = useNavigate();
  const location = useLocation();

  /*
   * Kafka node ids are integers and the endpoints take them as integers. A path segment that is not
   * one — a typed URL, an old bookmark — becomes `NaN`, which matches no broker in the list, and the
   * page below says so instead of asking the gateway about a broker that cannot exist.
   */
  const brokerId = (): number => Number(props.brokerId);

  const tab = (): BrokerTabKey =>
    new URLSearchParams(location.search).get("tab") === "configuration"
      ? "configuration"
      : "logdirs";

  /* The same key the brokers screen holds this cluster's list under, so arriving from that screen
     draws the identity at once rather than asking again for a document already in hand. */
  const brokers = useQuery<readonly Broker[]>({
    key: () => `clusters/${props.clusterId}/brokers`,
    load: () => fetchBrokers(kui.api, props.clusterId),
  });

  const logDirs = useQuery<readonly LogDir[]>({
    key: () => `clusters/${props.clusterId}/brokers/${props.brokerId}/log-dirs`,
    load: () => fetchBrokerLogDirs(kui.api, props.clusterId, brokerId()),
  });

  const configs = useQuery<readonly ConfigEntry[]>({
    /* `undefined` until the tab is selected, which is `useQuery`'s way of saying "not yet": nothing
       is bound, nothing is fetched, and the state stays `loading`. The panel this feeds is only
       built once the tab is on screen, and `describeConfigs` is sixty kilobytes. */
    key: () =>
      tab() === "configuration"
        ? `clusters/${props.clusterId}/brokers/${props.brokerId}/configs`
        : undefined,
    load: () => fetchBrokerConfigs(kui.api, props.clusterId, brokerId()),
  });

  createEffect(
    () => brokers.state(),
    (current) => {
      if (current.kind !== "loading") kui.report("feature", current.kind === "failed");
    },
  );

  const broker = () => valueOf(brokers.state(), []).find((row) => row.id === brokerId());

  return (
    <Show
      when={broker()}
      fallback={
        <BrokerNotShown
          clusterId={props.clusterId}
          brokerId={props.brokerId}
          state={brokers.state()}
          onRetry={brokers.reload}
          brokersHref={kui.paths.brokers(props.clusterId)}
        />
      }
    >
      {(found) => (
        <BrokerDetail
          broker={found()}
          clusterName={props.clusterId}
          clustersHref={kui.paths.clusters()}
          brokersHref={kui.paths.brokers(props.clusterId)}
          logDirs={loadedOf(logDirs.state(), logDirs.reload)}
          configuration={loadedOf(configs.state(), configs.reload)}
          tab={tab()}
          onTabChange={(next) => {
            const here = kui.paths.broker(props.clusterId, found().id);
            // The default tab carries no query at all, so the canonical address of this page is the
            // bare one and two links to the same view cannot be spelled two ways.
            navigate(next === "logdirs" ? here : `${here}?tab=${next}`, { resolve: false });
          }}
        />
      )}
    </Show>
  );
}

/**
 * What stands in for the page when there is no broker to draw.
 *
 * Three different situations, and they must not look alike: the list has not answered yet, the list
 * could not be read, and the list was read and this broker is not in it. The third is the one worth
 * the care — a broker id that no longer exists is what a bookmark from before a decommission looks
 * like, and "the cluster service is not answering" would send somebody to investigate an outage that
 * is not happening.
 */
function BrokerNotShown(props: {
  readonly clusterId: string;
  readonly brokerId: string;
  readonly state: Fetched<readonly Broker[]>;
  readonly onRetry: () => void;
  readonly brokersHref: string;
}): JSX.Element {
  const failure = () => failureOf(props.state, props.onRetry);

  return (
    <section class="kui-brk-page" data-testid="broker-not-shown">
      <PageHeader
        title={`Broker ${props.brokerId}`}
        crumbs={[
          { label: props.clusterId },
          { label: "Brokers", href: props.brokersHref },
          { label: `Broker ${props.brokerId}` },
        ]}
        testId="broker-not-shown-head"
      />
      <Card
        title="Broker"
        state={props.state.kind === "loading" ? "loading" : failure() === undefined ? "ready" : "unavailable"}
        message={failure()?.message}
        description={
          failure() === undefined
            ? undefined
            : `KUI reads a broker's identity from ${props.clusterId}'s broker list, and that list could not be read.`
        }
        code={failure()?.code}
        stateAction={
          failure() === undefined ? undefined : (
            <Button variant="secondary" icon="refresh" onClick={props.onRetry}>
              Retry
            </Button>
          )
        }
        bodyMinHeight="12rem"
        testId="broker-not-shown-card"
      >
        <EmptyState
          kind="empty"
          title={`${props.clusterId} has no broker ${props.brokerId}.`}
          description="The cluster answered, and no broker in it reports that node id. It may have been decommissioned since this link was made."
          /* An anchor rather than a `Button`, because it goes somewhere: a real link is what makes
             middle-click, "open in new tab" and the status bar's preview work, and `Button` takes no
             href precisely so that this decision has to be made deliberately. */
          action={
            <a class="kui-btn kui-btn--secondary kui-btn--md" href={props.brokersHref}>
              <span class="kui-btn__label">All brokers</span>
            </a>
          }
        />
      </Card>
    </section>
  );
}

/**
 * A screen state as one tab of {@link BrokerDetail} takes it.
 *
 * `stale` keeps its value and is shown: data that is real and out of date is still the best answer
 * anybody has, and hiding it would replace a figure that was true a minute ago with a dash meaning
 * "not known". `not-configured` is drawn as unavailable with its own sentence rather than being
 * merged into a failure, for the same reason the cluster screens keep them apart.
 */
function loadedOf<T>(state: Fetched<T>, onRetry: () => void): Loaded<T> {
  switch (state.kind) {
    case "ready":
    case "stale":
      return { kind: "ready", value: state.value };
    case "loading":
      return { kind: "loading" };
    case "forbidden":
      return { kind: "forbidden", message: "You may not read this.", code: "FORBIDDEN" };
    case "not-configured":
      return {
        kind: "unavailable",
        message: "This deployment has not configured it.",
        code: "NOT_CONFIGURED",
        onRetry,
      };
    case "failed":
      return { kind: "unavailable", message: state.message, code: state.code, onRetry };
  }
}
