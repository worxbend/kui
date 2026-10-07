/**
 * The topics feature's route entry: the screen the shell renders for every `/topics…` address.
 *
 * ## The feature had none, and the shell said nothing
 *
 * `registry.ts` loads this package with `import("@kui/feature-topics").then(featureModule)`, and
 * `featureModule` reads a `default` export. There wasn't one. The kernel tolerates that — it renders
 * a panel saying the feature arrived without a screen — which is the right behaviour for a feature
 * that is half-built and the reason nobody noticed for as long as they did: the route resolved, the
 * navigation highlighted, and the page said something reasonable.
 *
 * ## The addresses this owns
 *
 *   /clusters/:clusterId/topics              the list
 *   /clusters/:clusterId/topics/:topicName   one topic
 *
 * The message browser hangs off a topic but belongs to `feature-messages`, which is why
 * `/topics/:topicName/messages` is not here.
 */
import { Match, Show, Switch, createEffect, createMemo, createSignal, onCleanup } from "solid-js";
import type { JSX } from "@solidjs/web";
import { Actions, userMessage, type ApiResult } from "@kui/api";
import { useLocation, useNavigate, useParams } from "@solidjs/router";
import {
  Breadcrumbs,
  Button,
  ConfirmDialog,
  EmptyState,
  TabStrip,
  createMutation,
  createQueryRegistry,
  formatCount,
  notify,
  sharedQueries,
  useKui,
  useQuery,
  valueOf,
  writeBlockedReason,
  type BulkAction,
  type Fetched,
  type QueryRegistry,
} from "@kui/kernel";
import { TopicListPage, queryFromAddress, type TopicListQuery } from "./TopicListPage.jsx";
import { TopicStatisticsRegion } from "./TopicStatisticsRegion.jsx";
import { TopicOverviewTab } from "./TopicOverviewTab.jsx";
import { sortFieldFor, topicsCsv, topicsVoice } from "./topicList.js";
import { TopicPage } from "./TopicPage.jsx";
import { CreateTopicDialog } from "./CreateTopicDialog.jsx";
import { PlannedActionDialog } from "./PlannedActionDialog.jsx";
import { TopicSettings, type ConfigChange } from "./TopicSettings.jsx";
import { TopicPartitions, type PartitionsFailure } from "./TopicPartitions.jsx";
import { TopicConsumers, type ConsumersFailure } from "./TopicConsumers.jsx";
import { AddPartitionsDialog } from "./AddPartitionsDialog.jsx";
import { fetchTopicConfig, type TopicConfig } from "./config.js";
import {
  fetchClusterWriteState,
  fetchPartitions,
  fetchTopicConsumers,
  fetchTopicOverview,
  fetchTopicStatistics,
  fetchTopics,
  type ClusterWriteState,
  type PartitionRow,
  type TopicConsumerRow,
  type TopicListResult,
  type TopicOverview,
  type TopicQuery,
  type TopicStatistics,
} from "./data.js";
import type { TopicRow } from "./types.js";
import type { NewTopic } from "./write.js";
import {
  confirmPurge,
  createTopic,
  deleteTopic,
  deleteTopics,
  increasePartitions,
  planDeletion,
  planPartitionIncrease,
  planPurge,
  purgeTopics,
  updateTopicConfig,
  type BulkOutcome,
  type DeletionPlan,
  type PartitionPlan,
  type PurgePlan,
} from "./write.js";

export default function Topics(): JSX.Element {
  const params = useParams<{
    readonly clusterId?: string;
    readonly topicName?: string;
  }>();

  return (
    <Show keyed when={params.clusterId} fallback={<NoCluster />}>
      {(clusterId) => (
        <Show keyed when={params.topicName} fallback={<TopicsScreen clusterId={clusterId} />}>
          {(topicName) => <TopicScreen clusterId={clusterId} topicName={topicName} />}
        </Show>
      )}
    </Show>
  );
}

/**
 * Reached only by a hand-typed or stale address: the navigation cannot produce a topics link
 * without a cluster (`landingFor` returns `undefined`), so this is the bookmark somebody kept from
 * a deployment that has since been reconfigured. It says so rather than rendering an empty list,
 * which would read as a cluster with no topics.
 */
function NoCluster(): JSX.Element {
  const kui = useKui();
  return (
    <section class="kui-topic-list" aria-label="Topics">
      <p class="kui-topic-list__incomplete" role="status">
        No cluster is selected, so there are no topics to list.{" "}
        <a href={kui.paths.clusters()}>Choose a cluster</a> and try again.
      </p>
    </section>
  );
}

/**
 * The tabs' own answers, held apart from every other query in the product.
 *
 * A registry rather than the shared one, and `staleAfterMs: 0` rather than the default thirty
 * seconds, because these particular tabs have a rule the general cache does not: **an answer is
 * re-read every time the tab is opened**. Consumer lag and partition offsets move while somebody is
 * looking at the page, and a figure carried over from four minutes ago is wrong in the direction
 * that matters — it says a group has caught up when it has not.
 *
 * An entry that is always stale is refetched when a *new* watcher acquires it, which is exactly the
 * moment a tab opens; it is not refetched on every read, so this is one request per open and not a
 * loop. Two components on one key still share one request, which is the thing a hand-rolled
 * a hand-rolled fetch hook never gave, and the reason this file no longer has one.
 */
const TAB_QUERIES: QueryRegistry = createQueryRegistry({ staleAfterMs: 0 });

/**
 * A tab's data, fetched the first time the tab is opened and re-fetched whenever it is opened again.
 *
 * The topic page is one document with several sections, and fetching all of them on arrival would
 * ask the cluster for thirty-three configuration keys, the whole partition table and every consumer
 * group for a visitor who came to look at the overview. So each tab pays for itself: a closed tab's
 * key is `undefined`, `useQuery` binds nothing, and nothing is requested.
 */
function useTabQuery<T>(
  isOpen: () => boolean,
  key: () => string,
  load: () => Promise<Fetched<T>>,
): { readonly state: () => Fetched<T>; readonly reload: () => void } {
  const query = useQuery<T>({
    key: () => (isOpen() ? key() : undefined),
    load,
    registry: TAB_QUERIES,
  });
  return { state: query.state, reload: query.reload };
}

/**
 * Whether this cluster is registered read-only, for every write gate on these two screens.
 *
 * ADR-047's flag is a property of the **deployment**, not of the principal, and
 * `writeBlockedReason` prints a different sentence for each because the two need different actions
 * from the reader. All seven of this file's calls passed a hard-coded `false` until wave 7, so a
 * cluster somebody had deliberately registered read-only still offered `Create topic`, the bulk
 * `Delete` and `Empty topic` as live controls, and the refusal arrived from the server *after* the
 * operator had typed a confirmation. The permission half of that gate was closed in wave 5; this
 * is its other half.
 *
 * One query, keyed by the cluster and by nothing else, on the shared registry: the flag does not
 * move when a search box does, and the list screen and the topic page ask once between them.
 *
 * Answers `false` while the question is still out, and `false` when it could not be asked at all —
 * the same choice `useKui().permits` documents for the same reason. Disabling every write control
 * on a fact KUI does not have would put a screenful of refusals in front of every reader for the
 * length of one request, and the server is the authority either way: this decides only whether a
 * control explains itself in advance instead of failing afterwards.
 */
function useClusterReadOnly(clusterId: () => string): () => boolean {
  const kui = useKui();
  const query = useQuery<ClusterWriteState>({
    key: () => `cluster-write-state|${clusterId()}`,
    load: () => fetchClusterWriteState(kui.api, clusterId()),
  });
  return () => {
    const current = query.state();
    return (current.kind === "ready" || current.kind === "stale") && current.value.readOnly;
  };
}

/**
 * A tab's fetch state, as the table's `failure` prop wants it.
 *
 * The four not-happy states of ADR-039 are never interchangeable and this is the one place they are
 * translated. `loading`, `ready` and `stale` all produce `undefined`: a stale table draws its rows,
 * because real data that is out of date is worth more than an error message, and the staleness is
 * reported beside it rather than instead of it.
 */
function tabFailure<T>(
  state: Fetched<T>,
  onRetry: () => void,
):
  | {
      readonly kind: "unavailable";
      readonly message: string;
      readonly code: string;
      readonly onRetry: () => void;
    }
  | { readonly kind: "forbidden"; readonly message: string; readonly code: string }
  | { readonly kind: "not-configured"; readonly message: string }
  | undefined {
  switch (state.kind) {
    case "failed":
      return { kind: "unavailable", message: state.message, code: state.code, onRetry };
    case "forbidden":
      return {
        kind: "forbidden",
        // No code: a refusal is not an incident and there is nothing for the operator to quote at
        // anybody except the administrator who grants the role.
        message: "Ask an administrator for a role that includes this cluster's consumer and topic read permissions.",
        code: "FORBIDDEN",
      };
    case "not-configured":
      return {
        kind: "not-configured",
        message: "This deployment has not configured the service that answers for it, so there is nothing to retry.",
      };
    default:
      return undefined;
  }
}

/**
 * What the topic list itself shows in place of the table when the request that fills it did not
 * come back — never the generic "No topics yet" empty state, which reads a fetch failure as a
 * cluster with zero topics.
 */
function TopicsListFailure(props: {
  readonly failure: NonNullable<ReturnType<typeof tabFailure>>;
}): JSX.Element {
  return (
    <section class="kui-topic-list" aria-label="Topics">
      <Switch>
        <Match when={props.failure.kind === "unavailable" ? props.failure : undefined}>
          {(reason) => (
            <EmptyState
              kind="unavailable"
              title="The topic list did not come back."
              description={reason().message}
              code={(reason() as { readonly code: string }).code}
              action={
                <Button
                  variant="secondary"
                  icon="refresh"
                  onClick={() => (reason() as { readonly onRetry: () => void }).onRetry()}
                >
                  Try again
                </Button>
              }
            />
          )}
        </Match>

        <Match when={props.failure.kind === "forbidden" ? props.failure : undefined}>
          {(reason) => (
            <EmptyState
              kind="forbidden"
              title="You may not list topics on this cluster."
              description={reason().message}
              code={(reason() as { readonly code: string }).code}
            />
          )}
        </Match>

        <Match when={props.failure.kind === "not-configured" ? props.failure : undefined}>
          {(reason) => (
            <EmptyState
              kind="empty"
              title="Topics are not reported on this deployment."
              description={reason().message}
            />
          )}
        </Match>
      </Switch>
    </section>
  );
}

/**
 * The key `TopicsScreen`'s cluster-wide statistics query is bound under.
 *
 * Named apart so `TopicScreen` — the single-topic page, which holds no `useQuery` for this document
 * itself — can invalidate the same entry on `sharedQueries` after a mutation that changes it, without
 * the two screens' key strings drifting apart.
 */
function topicStatisticsKey(clusterId: string): string {
  return `topic-statistics|${clusterId}`;
}

/**
 * The screen's query, as the topics endpoint takes it.
 *
 * `showInternal` comes out of the facet chip rather than out of a checkbox of its own, and that is
 * the whole of the mapping between the design's four-chip bar and the one parameter the wire has.
 * `Internal` asks the server for Kafka's bookkeeping topics; the other three do not, and the page
 * says which of them it applies itself — see `isServerFacet`.
 *
 * The order goes through `sortFieldFor`, which is the same list the `Sort ·` menu's options are
 * built from. It used to be a second hand-written map in this file, and the pair could disagree in
 * one direction without anything noticing: an option offered here with no field sent a request with
 * no `sort` and drew the arrow anyway.
 */
export function toTopicQuery(query: TopicListQuery): TopicQuery {
  const field = query.sort === null ? undefined : sortFieldFor(query.sort.columnId);
  return {
    showInternal: query.facet === "internal",
    ...(query.search === "" ? {} : { q: query.search }),
    ...(field === undefined ? {} : { sort: `${field}:${query.sort?.order ?? "asc"}` }),
    page: query.page,
    pageSize: query.pageSize,
  };
}

/**
 * The sentence a bulk action's outcome deserves.
 *
 * Both halves, always, because a set can fail in the middle: "3 topics deleted" over a selection of
 * five is a sentence that leaves the operator to discover the other two. `SCREENS-V4.md` §6
 * recommends the flat register here — the design's own `2 topics deleted (in spirit)` hedges about
 * whether a destructive action happened, and a confirmation is the one place this product's voice
 * is not funny.
 */
export function bulkSentence(verb: string, outcome: BulkOutcome): string {
  const done = `${formatCount(outcome.done.length)} ${outcome.done.length === 1 ? "topic" : "topics"} ${verb}`;
  if (outcome.failed.length === 0) return done;
  return `${done}. ${formatCount(outcome.failed.length)} refused: ${outcome.failed
    .map((failure) => `${failure.topic} — ${failure.reason}`)
    .join("; ")}`;
}

/**
 * Hands the browser a file.
 *
 * An object URL and a synthetic click, revoked immediately afterwards: the alternative is a data
 * URI, which several browsers cap at a couple of megabytes and which a cluster of four thousand
 * topics would exceed. The anchor is attached before it is clicked because a detached one is
 * ignored in some browsers, and removed straight after because it is not part of the page.
 */
function download(filename: string, text: string, type: string): void {
  const url = URL.createObjectURL(new Blob([text], { type }));
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

/**
 * How many times the list is re-read after a create before the screen stops asking.
 *
 * Six at half a second is three seconds, which is far longer than the controller takes. The bound
 * exists so the poll **stops rather than spinning**: a topic still absent after three seconds is a
 * fact about the cluster and not a race, and a screen that kept asking would issue a request every
 * half second for as long as the tab stayed open.
 */
export const CREATE_POLL_ATTEMPTS = 6;

/**
 * Long enough for the fetch a reload just started to have landed, short enough that the list is on
 * screen well before anybody wonders.
 *
 * Module-private, and it was exported. Nothing in the product or in a case ever named it from
 * outside this file, and an export with no caller is a promise this module is not being asked for.
 * What holds the figure is `pollUntilListed`'s own case, driven on a fake clock so the rule can be
 * asserted without a case that sleeps for it.
 */
const CREATE_POLL_INTERVAL_MS = 500;

/**
 * Re-reads the list until the new topic is in it, or until it is time to stop asking.
 *
 * Kafka's `createTopics` returns when the *controller has accepted* the create, not when every
 * broker will list the topic — so a single re-fetch straight afterwards is a race, and losing it
 * means an operator creates a topic and does not see it. They then create it again, and the second
 * attempt fails with "topic already exists", which reads as the product being broken twice.
 *
 * The row is not spliced in locally instead, because the row this screen would invent is a guess at
 * what the broker decided about the defaults it was not given — and a guessed partition count on a
 * topic somebody is about to produce to is worse than a short wait.
 *
 * Lifted out of `TopicsScreen` so that all three halves of the rule can be gated, none of which the
 * screen can show. That it polls at all, rather than re-reading once, is observable through the
 * screen and has a case. The **bound** is not: 6 and 100 draw the same page and differ only in how
 * long the browser keeps asking a cluster that is never going to answer. Neither is the
 * **short-circuit** — deleting `if (listed()) return;` draws the same page too, and every
 * successful create then fires all six reloads over three seconds instead of stopping at the first
 * that can see the topic. Nor is the **interval**, whose only effect is wall-clock. So the loop is
 * a function with one caller, one line below, and its three rules are asserted on the function.
 */
export async function pollUntilListed(reload: () => void, listed: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < CREATE_POLL_ATTEMPTS; attempt += 1) {
    reload();
    await new Promise((resolve) => setTimeout(resolve, CREATE_POLL_INTERVAL_MS));
    if (listed()) return;
  }
}

/** Recheck at submission as well as at entry: policy can change while a form is open. */
function allowWrite(reason: string | undefined): boolean {
  if (reason === undefined) return true;
  notify("Write blocked", { message: reason, tone: "warning" });
  return false;
}

function TopicsScreen(props: { readonly clusterId: string }): JSX.Element {
  const kui = useKui();
  const navigate = useNavigate();

  /*
   * Seeded from the address, not from the default.
   *
   * The shell's drawer links its topic-prefix rows at `…/topics?q=<prefix>`. Seeded from
   * `DEFAULT_TOPIC_QUERY` this screen ignored that and listed the whole cluster, so the link was
   * honest and the destination was not — a defect neither side's tests could see, because the link
   * is one package and the reading is another. `queryFromAddress` is where the two meet.
   */
  const listLocation = useLocation();
  const [query, setQuery] = createSignal<TopicListQuery>(queryFromAddress(listLocation.search));

  const list = useQuery<TopicListResult>({
    /* Every control on this page is applied by the server. It used to ask for the largest page the
       endpoint allows and then filter, search and sort what came back, which is honest for one page
       and wrong for a cluster with four thousand topics: a search that only looks at the rows it was
       handed is a search that lies, and it lies by finding nothing and saying so.

       The whole request is in the key, which is what `useQuery` means by one: two screens asking
       for the same page share one call, and a change to any control is a different key and
       therefore a different answer. */
    key: () => `topics|${props.clusterId}|${JSON.stringify(toTopicQuery(query()))}`,
    load: () => fetchTopics(kui.api, props.clusterId, toTopicQuery(query())),
  });
  const state = list.state;
  const reload = list.reload;

  /**
   * The cluster-wide statistics, on a key of their own.
   *
   * Deliberately **not** keyed by the query. This document is about the whole cluster and does not
   * move when the search box does, so typing `orders.` re-fetches the page and leaves these totals
   * exactly where they were — which is the design's load-bearing fact for this screen rather than an
   * optimisation. A key that carried the filter would issue a request per keystroke for figures that
   * cannot change because of it.
   */
  const statistics = useQuery<TopicStatistics>({
    key: () => topicStatisticsKey(props.clusterId),
    load: () => fetchTopicStatistics(kui.api, props.clusterId),
  });

  /* The deployment's own answer to "may anything here write at all", which the three gates below
     ask before they ask about the principal. See `useClusterReadOnly`. */
  const readOnly = useClusterReadOnly(() => props.clusterId);

  createEffect(
    () => state(),
    (current) => {
      if (current.kind !== "loading") kui.report("feature", current.kind === "failed");
    },
  );

  const result = () =>
    valueOf(state(), {
      topics: [],
      incomplete: 0,
      page: { page: 1, pageSize: 0, totalItems: undefined },
    });

  /**
   * Why the list is empty, when the reason is that the request itself failed rather than that the
   * cluster genuinely has no topics. `undefined` for `loading`, `ready` and `stale` — a stale table
   * still draws its rows, per `tabFailure`'s own doc.
   */
  const listFailure = () => tabFailure(state(), reload);

  /** The statistics document, or nothing. `loading`, `failed` and every refusal are all "nothing". */
  const totals = (): TopicStatistics | undefined => {
    const current = statistics.state();
    return current.kind === "ready" || current.kind === "stale" ? current.value : undefined;
  };

  const [creating, setCreating] = createSignal(false);

  /**
   * The selected topic names — one set, shared by the table and the cards (`SCREENS-V4.md` §3.7).
   *
   * Cleared whenever the query changes. A bar reading "2 topics selected" over a page holding
   * neither of them is a control whose subject the operator cannot see, and the first thing they
   * would do is click `Delete` to find out what it meant. The ticks are on rows; they go with the
   * rows.
   */
  const [selected, setSelected] = createSignal<ReadonlySet<string>>(new Set<string>());

  const changeQuery = (next: TopicListQuery): void => {
    setSelected(new Set<string>());
    setQuery(next);
  };

  /*
   * The address keeps being read, not read once.
   *
   * The drawer's prefix rows are ordinary links, and clicking a second one while this screen is
   * already on does not remount the route — it changes the search string underneath it. Guarded on
   * the two fields the address carries so that typing in the search box, which moves the query and
   * not the address, is not undone on the next unrelated navigation.
   *
   * The claim above went three waves with nothing behind it: every address case mounted fresh, so
   * the seed answered them and this could be deleted whole with the package green. "A second
   * address change on a mounted route reaches the server" is the case that fails without it, and it
   * pushes onto the harness's history rather than mounting again, which is the only arrangement in
   * which a *second* read is distinguishable from the first.
   */
  createEffect(
    () => listLocation.search,
    (search: string) => {
      const asked = queryFromAddress(search);
      if (asked.search !== query().search || asked.facet !== query().facet) changeQuery(asked);
    },
  );

  /** The rows behind the ticks. A name with no row on this page contributes nothing to the file. */
  const selectedRows = createMemo(() => result().topics.filter((topic) => selected().has(topic.name)));

  const [bulk, setBulk] = createSignal<"purge" | "delete" | undefined>(undefined);

  const runBulk = createMutation(async (kind: "purge" | "delete") => {
    const names = [...selected()];
    const outcome =
      kind === "delete"
        ? await deleteTopics(kui.api, props.clusterId, names)
        : await purgeTopics(kui.api, props.clusterId, names);
    // Always `ok`: the outcome *is* the answer, failures included, because a set can fail in the
    // middle and one error envelope cannot say which half did. See `bulkSentence`.
    return { ok: true as const, value: outcome };
  });

  const create = createMutation((topic: NewTopic) => createTopic(kui.api, props.clusterId, topic));

  const settleAfterCreate = (name: string): Promise<void> =>
    // `result()` is the screen's own view of the answer, with the same fallback the table uses, so
    // this asks exactly the question the operator is about to ask: is it on the list?
    pollUntilListed(reload, () => result().topics.some((one) => one.name === name));

  /*
   * Two separate reasons, and the button says which one applies. A read-only cluster is ADR-047's
   * own state rather than a permission problem, and telling an operator to ask an administrator for
   * a permission they already hold wastes their afternoon.
   */
  const createBlocked = (): string | undefined =>
    kui.writeBlocked !== undefined ? kui.writeBlocked(props.clusterId, Actions.TopicCreate) : writeBlockedReason({
      permitted: kui.permits(Actions.TopicCreate),
      readOnly: readOnly(),
      action: "create a topic on this cluster",
    });

  const purgeBlocked = (): string | undefined =>
    kui.writeBlocked !== undefined ? kui.writeBlocked(props.clusterId, Actions.TopicMessagesDelete) : writeBlockedReason({
      permitted: kui.permits(Actions.TopicMessagesDelete),
      readOnly: readOnly(),
      action: "empty topics on this cluster",
    });

  const deleteBlocked = (): string | undefined =>
    kui.writeBlocked !== undefined ? kui.writeBlocked(props.clusterId, Actions.TopicDelete) : writeBlockedReason({
      permitted: kui.permits(Actions.TopicDelete),
      readOnly: readOnly(),
      action: "delete topics on this cluster",
    });

  /**
   * The rows on screen, as a file. Named for the cluster, because two of these in one folder would
   * otherwise be two files called `topics.csv`.
   */
  const exportRows = (rows: readonly TopicRow[]): void => {
    download(`${props.clusterId}-topics.csv`, topicsCsv(rows), "text/csv;charset=utf-8");
  };

  /**
   * What the bulk bar offers.
   *
   * An action the principal may not take is **disabled with its reason**, never hidden: if `Delete`
   * disappeared for one operator then `Purge` would move into its place, and the same gesture would
   * do two different irreversible things to two different people (`SCREENS-V4.md` §3.7).
   */
  const bulkActions = (): readonly BulkAction[] => [
    {
      id: "export",
      label: "Export",
      icon: "download",
      onSelect: () => exportRows(selectedRows()),
    },
    {
      id: "purge",
      label: "Empty",
      icon: "minus",
      destructive: true,
      ...(purgeBlocked() === undefined ? {} : { disabledReason: purgeBlocked() }),
      onSelect: () => {
        runBulk.reset();
        setBulk("purge");
      },
    },
    {
      id: "delete",
      label: "Delete",
      icon: "trash",
      destructive: true,
      ...(deleteBlocked() === undefined ? {} : { disabledReason: deleteBlocked() }),
      onSelect: () => {
        runBulk.reset();
        setBulk("delete");
      },
    },
  ];

  return (
    <>
      <Show
        when={listFailure()}
        fallback={
          <TopicListPage
            topics={result().topics}
            loading={state().kind === "loading"}
            incomplete={result().incomplete}
            query={query()}
            onQueryChange={changeQuery}
            totalItems={result().page.totalItems}
            /* Two documents, one sentence: the match count is the server's answer to the *current*
               search and the two totals are the cluster's, from a document that does not move when
               the search box does. Neither component below holds both, which is why the line is
               composed here — see `topicsVoice`. */
            voice={topicsVoice(result().page.totalItems, totals()?.topics, totals()?.partitions)}
            statistics={
              <TopicStatisticsRegion
                statistics={totals()}
                loading={statistics.state().kind === "loading"}
                {...(statistics.state().kind === "failed"
                  ? {
                      unavailableReason:
                        "KUI could not read this cluster's topic totals. The list below is still this cluster's.",
                    }
                  : {})}
              />
            }
            selected={selected()}
            onSelectionChange={setSelected}
            bulkActions={bulkActions()}
            onExport={() => exportRows(result().topics)}
            onOpen={(topic) => {
              navigate(kui.paths.topic(props.clusterId, topic.name), { resolve: false });
            }}
            onCreate={() => {
              // Any failure from a previous attempt goes with the dialog that showed it. Reopening
              // to find last time's error still on screen reads as this attempt having already
              // failed.
              create.reset();
              setCreating(true);
            }}
            createDisabledReason={createBlocked()}
          />
        }
      >
        {(failure) => <TopicsListFailure failure={failure()} />}
      </Show>

      <ConfirmDialog
        open={bulk() !== undefined}
        onClose={() => setBulk(undefined)}
        title={
          bulk() === "delete"
            ? `Delete ${formatCount(selected().size)} ${selected().size === 1 ? "topic" : "topics"}?`
            : `Empty ${formatCount(selected().size)} ${selected().size === 1 ? "topic" : "topics"}?`
        }
        /* Named rather than counted. A confirmation that says "5 topics" is a confirmation the
           operator cannot check, and the whole reason for a plan→confirm flow is that they can. */
        consequence={
          bulk() === "delete"
            ? `Removes ${[...selected()].join(", ")}, with every record and every configuration override each of them holds. Each topic is planned and confirmed on its own, so a refusal stops that topic and not the rest.`
            : `Deletes every record currently in ${[...selected()].join(", ")}. The topics, their configuration and their partition counts are left as they are.`
        }
        confirmLabel={bulk() === "delete" ? "Delete topics" : "Empty topics"}
        confirmIcon={bulk() === "delete" ? "trash" : "minus"}
        /* Typed, because neither can be undone and both act on more than one thing at once. The
           word rather than a name: there is no single name to type. */
        typeToConfirm={bulk() === "delete" ? "delete" : "empty"}
        busy={runBulk.busy()}
        onConfirm={() => {
          const kind = bulk();
          if (kind === undefined) return;
          if (!allowWrite(kind === "delete" ? deleteBlocked() : purgeBlocked())) return;
          if (kui.writeBlocked !== undefined && [...selected()].some((name) =>
            !allowWrite(kui.writeBlocked!(props.clusterId,
              kind === "delete" ? Actions.TopicDelete : Actions.TopicMessagesDelete, name)))) return;
          void runBulk.run(kind).then((outcome) => {
            if (outcome.kind !== "done") return;
            setBulk(undefined);
            setSelected(new Set<string>());
            notify(bulkSentence(kind === "delete" ? "deleted" : "emptied", outcome.value), {
              // Not `success` when part of the set refused: a green toast over three failures is
              // the reassuring rendering of the state that needs attention.
              tone: outcome.value.failed.length === 0 ? "success" : "warning",
            });
            reload();
            statistics.reload();
          });
        }}
        testId="topic-bulk-confirm"
      />
      <CreateTopicDialog
        open={creating()}
        onClose={() => setCreating(false)}
        state={create.state()}
        /* The names the browser holds, so an obvious clash is caught without a round trip. It is
           not the authority — the server's rejection is — and on a cluster whose list was truncated
           or is stale this misses some. Missing one costs a round trip and an accurate error;
           inventing one would refuse a name that is genuinely free. */
        existingNames={result().topics.map((topic) => topic.name)}
        onCreate={(topic) => {
          if (!allowWrite(kui.writeBlocked !== undefined
            ? kui.writeBlocked(props.clusterId, Actions.TopicCreate, topic.name) : createBlocked())) return;
          void create.run(topic).then((outcome) => {
            if (outcome.kind !== "done") return;
            setCreating(false);
            notify(`${topic.name} created`, {
              message: "It may take a moment to appear in the list below.",
            });
            void settleAfterCreate(topic.name);
            statistics.reload();
          });
        }}
      />
    </>
  );
}

function TopicScreen(props: {
  readonly clusterId: string;
  readonly topicName: string;
}): JSX.Element {
  const kui = useKui();
  const navigate = useNavigate();
  const overviewQuery = useQuery<TopicOverview>({
    key: () => `topic-overview|${props.clusterId}|${props.topicName}`,
    load: () => fetchTopicOverview(kui.api, props.clusterId, props.topicName),
  });
  const state = overviewQuery.state;
  const reload = overviewQuery.reload;
  // The keyed route owns both target strings and all confirmations. Mutation receipts may
  // still arrive after navigation, but must not close dialogs or navigate the new workspace.
  let active = true;
  onCleanup(() => { active = false; });

  /* The same one question the list screen asks, on the same key, so the two share one request. */
  const readOnly = useClusterReadOnly(() => props.clusterId);

  createEffect(
    () => state(),
    (current) => {
      if (current.kind !== "loading") kui.report("feature", current.kind === "failed");
    },
  );

  const overview = () =>
    state().kind === "ready" || state().kind === "stale" ? valueOf(state(), undefined) : undefined;

  const [purging, setPurging] = createSignal(false);
  const [deleting, setDeleting] = createSignal(false);

  /*
   * Growing the topic is two dialogs and therefore two signals.
   *
   * `growing` is the form that asks for a number; `growTarget` is the number it produced, and its
   * presence is what opens the confirmation. They are separate rather than one three-state value
   * because the confirmation's `plan()` closes over the target, and `PlannedActionDialog` fetches
   * the plan the moment it opens — a target that arrived after the dialog did would plan for the
   * previous one.
   */
  const [growing, setGrowing] = createSignal(false);
  const [growTarget, setGrowTarget] = createSignal<number | undefined>(undefined);

  const purge = createMutation((token: string) =>
    confirmPurge(kui.api, props.clusterId, props.topicName, token),
  );
  const remove = createMutation((token: string) =>
    deleteTopic(kui.api, props.clusterId, props.topicName, token),
  );
  const grow = createMutation((token: string) =>
    increasePartitions(kui.api, props.clusterId, props.topicName, token),
  );

  /** A plan request, as the dialog wants it: the plan, or one sentence saying why there is none. */
  const planning = async <P,>(
    request: Promise<ApiResult<P>>,
  ): Promise<P | { readonly failure: string }> => {
    const answer = await request;
    return answer.ok ? answer.value : { failure: userMessage(answer.error) };
  };

  /**
   * Which section of the topic page is on screen.
   *
   * Read from the address rather than held in a signal, so that a link to a topic's settings is a
   * link somebody can send. `?tab=settings` rather than a path segment because the tabs are one
   * page's sections, not separate resources — the overview and the settings describe the same topic.
   */
  const routerLocation = useLocation();
  const tab = () => new URLSearchParams(routerLocation.search).get("tab") ?? "overview";

  /** What identifies the subject of every tab's request. Changing topic re-fetches whatever is open. */
  const subject = (): string => `${props.clusterId}/${props.topicName}`;

  const config = useTabQuery<TopicConfig>(
    () => tab() === "settings",
    () => `topic-config|${subject()}`,
    () => fetchTopicConfig(kui.api, props.clusterId, props.topicName),
  );

  const partitions = useTabQuery<readonly PartitionRow[]>(
    () => tab() === "partitions",
    () => `topic-partitions|${subject()}`,
    /* The uncapped endpoint, not the overview's list. The overview stops at 500 partitions, so on a
       large topic its table is short and the totals above it are not — see `fetchPartitions`. */
    () => fetchPartitions(kui.api, props.clusterId, props.topicName),
  );

  const consumers = useTabQuery<readonly TopicConsumerRow[]>(
    () => tab() === "consumers",
    () => `topic-consumers|${subject()}`,
    () => fetchTopicConsumers(kui.api, props.clusterId, props.topicName),
  );

  const editConfig = createMutation((change: ConfigChange) =>
    updateTopicConfig(kui.api, props.clusterId, props.topicName, change),
  );

  /**
   * Every gate on this page names the topic, and the list screen's three deliberately do not.
   *
   * A grant carries a *pattern*, so somebody granted `TOPIC:DELETE` on `analytics\..*` holds the
   * action on something and holds it on nothing here. `kernel/src/state/session.ts` says which
   * question is which: the subjectless form is *"the right answer for a list heading and the wrong
   * one for a row's delete button"* — and this whole page is one row's delete button. Until wave 8
   * all four of these asked the weaker question, so an account trusted with `analytics.*` was
   * offered a live `Delete this topic` on `orders.payments` and found out from the gateway.
   */
  const purgeBlocked = (): string | undefined =>
    kui.writeBlocked !== undefined ? kui.writeBlocked(props.clusterId, Actions.TopicMessagesDelete, props.topicName) : writeBlockedReason({
      permitted: kui.permits(Actions.TopicMessagesDelete, props.topicName),
      readOnly: readOnly(),
      action: "empty this topic",
    });

  const deleteBlocked = (): string | undefined =>
    kui.writeBlocked !== undefined ? kui.writeBlocked(props.clusterId, Actions.TopicDelete, props.topicName) : writeBlockedReason({
      permitted: kui.permits(Actions.TopicDelete, props.topicName),
      readOnly: readOnly(),
      action: "delete this topic",
    });

  /**
   * Growing the topic is an edit of the topic itself, so it is gated on `TopicEdit`.
   *
   * There is no separate "add partitions" action in the server's vocabulary, and inventing one here
   * — a hand-written `{resource, action}` pair — is exactly the mistake `useKui().permits` was
   * narrowed to make impossible: it would name an action the server has never heard of, answer
   * `false` for everyone for ever, and disable a control with a message blaming the operator.
   */
  const growBlocked = (): string | undefined =>
    kui.writeBlocked !== undefined ? kui.writeBlocked(props.clusterId, Actions.TopicEdit, props.topicName) : writeBlockedReason({
      permitted: kui.permits(Actions.TopicEdit, props.topicName),
      readOnly: readOnly(),
      action: "add partitions to this topic",
    });

  const editBlocked = (): string | undefined =>
    kui.writeBlocked !== undefined ? kui.writeBlocked(props.clusterId, Actions.TopicEdit, props.topicName) : writeBlockedReason({
      permitted: kui.permits(Actions.TopicEdit, props.topicName),
      readOnly: readOnly(),
      action: "change this topic's settings",
    });

  return (
    <>
      <TopicPage
        name={props.topicName}
        /* `unknown` until the description arrives, and `unknown` is not `offline`: one says the topic
         is broken, the other says we have not been told. */
        health={overview()?.topic.health ?? "unknown"}
        /* The trail the design draws inside the content (`SCREENS-V4.md` §4.9). The shell's own
           breadcrumb names the cluster and the section; this one names the object and the list it
           came from, which is the link an operator uses to get back to where they were. */
        breadcrumb={
          <Breadcrumbs
            crumbs={[
              { label: "Topics", href: kui.paths.topics(props.clusterId) },
              { label: props.topicName },
            ]}
          />
        }
        /* Not a produce form on this page: the browser owns producing, and a second one here would
           be a second implementation of the same drawer. The button goes where the drawer is, which
           is what the design's `➤ Produce message` does. */
        onProduce={{
          label: "Produce message",
          onClick: () => {
            navigate(kui.paths.topicMessages(props.clusterId, props.topicName), {
              resolve: false,
            });
          },
        }}
        onPurge={{
          label: "Empty topic",
          onClick: () => {
            purge.reset();
            setPurging(true);
          },
          disabledReason: purgeBlocked(),
          busy: purge.busy(),
        }}
        onDelete={{
          label: "Delete topic",
          onClick: () => {
            remove.reset();
            setDeleting(true);
          },
          disabledReason: deleteBlocked(),
          busy: remove.busy(),
        }}
        tabs={
          <TabStrip
            label="Topic sections"
            currentId={tab()}
            tabs={[
              {
                id: "overview",
                label: "Overview",
                icon: "info",
                href: kui.paths.topic(props.clusterId, props.topicName),
              },
              {
                id: "partitions",
                label: "Partitions",
                icon: "partitions",
                /* The count comes from the overview's row rather than from the partition tab's own
                   fetch, so the strip can say how many there are before anybody opens the tab. It
                   is `undefined` — no badge at all — while the topic is undescribed, because a `0`
                   beside "Partitions" would claim something no Kafka topic is. */
                count: overview()?.topic.partitions,
                href: `${kui.paths.topic(props.clusterId, props.topicName)}?tab=partitions`,
              },
              {
                id: "messages",
                label: "Messages",
                icon: "messages",
                href: kui.paths.topicMessages(props.clusterId, props.topicName),
              },
              {
                id: "consumers",
                label: "Consumers",
                icon: "consumers",
                href: `${kui.paths.topic(props.clusterId, props.topicName)}?tab=consumers`,
              },
              {
                id: "settings",
                label: "Settings",
                icon: "sliders",
                href: `${kui.paths.topic(props.clusterId, props.topicName)}?tab=settings`,
              },
            ]}
          />
        }
      >
        <Show when={tab() === "overview"}>
          <TopicOverviewTab
            {...(overview() === undefined ? {} : { overview: overview() })}
            loading={state().kind === "loading"}
            partitionsHref={`${kui.paths.topic(props.clusterId, props.topicName)}?tab=partitions`}
          />
        </Show>

        <Show when={tab() === "partitions"}>
          <TopicPartitions
            partitions={valueOf(partitions.state(), [])}
            loading={partitions.state().kind === "loading"}
            failure={
              tabFailure(partitions.state(), () => partitions.reload()) as
                | PartitionsFailure
                | undefined
            }
            onAdd={() => {
              grow.reset();
              setGrowing(true);
            }}
            addDisabledReason={growBlocked()}
            addBusy={grow.busy()}
          />
        </Show>

        <Show when={tab() === "consumers"}>
          <TopicConsumers
            rows={valueOf(consumers.state(), [])}
            loading={consumers.state().kind === "loading"}
            /* `not-configured` cannot reach here: this endpoint is not sectioned, so a consumer
               service that is not configured arrives as an error envelope. The cast is narrowing
               the shared union to the two members this table draws. */
            failure={
              tabFailure(consumers.state(), () => consumers.reload()) as
                | ConsumersFailure
                | undefined
            }
            hrefFor={(groupId) => kui.paths.consumerGroup(props.clusterId, groupId)}
          />
        </Show>

        <Show when={tab() === "settings"}>
          <TopicSettings
            config={valueOf(config.state(), { entries: [], overridden: 0 })}
            loading={config.state().kind === "loading"}
            state={editConfig.state()}
            onChange={
              editBlocked() === undefined
                ? (change) => {
                    if (!allowWrite(editBlocked())) return;
                    void editConfig.run(change).then((outcome) => {
                      if (!active || outcome.kind !== "done") return;
                      /* Re-read rather than patching the row locally. The response is the
                         configuration as the broker holds it *afterwards*, so a value the broker
                         normalised — "3600000" for "1h" — is the value the operator sees, and the
                         `source` of the key they just changed flips to "set on this topic" without
                         this screen having to work out that it would. */
                      config.reload();
                    });
                  }
                : undefined
            }
            changeDisabledReason={editBlocked()}
          />
        </Show>
      </TopicPage>

      <PlannedActionDialog<PurgePlan>
        open={purging()}
        onClose={() => setPurging(false)}
        title={`Empty ${props.topicName}?`}
        confirmLabel="Empty topic"
        confirmIcon="minus"
        /* Typing the name is asked for here because the action destroys data and cannot be undone.
         It is deliberately *not* asked for on things that can be undone — a product that demands it
         everywhere teaches operators to type names without reading. */
        typeToConfirm={props.topicName}
        plan={() => planning(planPurge(kui.api, props.clusterId, props.topicName))}
        describe={describePurge}
        state={purge.state()}
        onConfirm={(token) => {
          if (!allowWrite(purgeBlocked())) return;
          void purge.run(token).then((outcome) => {
            if (!active || outcome.kind !== "done") return;
            setPurging(false);
            /* The count is the server's own, from the answer rather than from the plan: a partition
               the broker refused is not a partition that was emptied, and the toast is the only
               place the operator is told the difference. */
            notify(`${props.topicName} emptied`, {
              message:
                outcome.value.refused.length === 0
                  ? `${formatCount(outcome.value.purgedPartitions)} ${outcome.value.purgedPartitions === 1 ? "partition" : "partitions"} emptied.`
                  : `${formatCount(outcome.value.purgedPartitions)} emptied; ${formatCount(outcome.value.refused.length)} refused.`,
              tone: outcome.value.refused.length === 0 ? "success" : "warning",
            });
            // The record counts and sizes on this page are now wrong by exactly what was deleted.
            reload();
            // The cluster-wide totals the list screen shows are wrong by the same amount, on a
            // query this page holds no handle to — see `topicStatisticsKey`.
            sharedQueries.invalidate(topicStatisticsKey(props.clusterId));
          });
        }}
      />

      <AddPartitionsDialog
        open={growing()}
        onClose={() => setGrowing(false)}
        topicName={props.topicName}
        /* The count the page already read. `undefined` when the topic could not be described, which
           the form reports rather than filling in a plausible number — a pre-filled `1` on a topic
           with twelve partitions is a wrong answer wearing a right answer's shape. */
        current={overview()?.topic.partitions}
        onContinue={(target) => {
          if (!allowWrite(growBlocked())) return;
          setGrowing(false);
          setGrowTarget(target);
        }}
      />

      <PlannedActionDialog<PartitionPlan>
        /* The confirmation exists only once a target has been chosen, which is what guarantees the
           plan it fetches on open is a plan for that target. */
        open={growTarget() !== undefined}
        onClose={() => setGrowTarget(undefined)}
        title={`Add partitions to ${props.topicName}?`}
        confirmLabel="Add partitions"
        confirmIcon="plus"
        /* Not destructive — no record is deleted — and still typed, because Kafka cannot remove a
           partition afterwards. The kernel's test for asking somebody to type a name is whether the
           action can be undone, and this one cannot. */
        destructive={false}
        typeToConfirm={props.topicName}
        planningMessage="Asking the cluster what this would change…"
        plan={() =>
          planning(
            planPartitionIncrease(
              kui.api,
              props.clusterId,
              props.topicName,
              growTarget() as number,
            ),
          )
        }
        describe={describePartitionIncrease}
        state={grow.state()}
        onConfirm={(token) => {
          if (!allowWrite(growBlocked())) return;
          void grow.run(token).then((outcome) => {
            if (!active || outcome.kind !== "done") return;
            setGrowTarget(undefined);
            notify(`${props.topicName} now has ${formatCount(outcome.value.target)} partitions`, {
              message: "Kafka cannot remove a partition, so this cannot be undone.",
            });
            /* Both the partition table and the topic's own row are now wrong: the table is short by
               the new partitions and the header's count is the old one. Re-read both rather than
               splicing empty rows in — the broker decides the new partitions' replica assignment,
               and this screen would be guessing at it. */
            partitions.reload();
            reload();
          });
        }}
      />

      <PlannedActionDialog<DeletionPlan>
        open={deleting()}
        onClose={() => setDeleting(false)}
        title={`Delete ${props.topicName}?`}
        confirmLabel="Delete topic"
        confirmIcon="trash"
        typeToConfirm={props.topicName}
        plan={() => planning(planDeletion(kui.api, props.clusterId, props.topicName))}
        describe={describeDeletion}
        state={remove.state()}
        onConfirm={(token) => {
          if (!allowWrite(deleteBlocked())) return;
          void remove.run(token).then((outcome) => {
            if (outcome.kind !== "done") return;
            const cluster = props.clusterId;
            const topic = props.topicName;
            sharedQueries.invalidateWhere((key) =>
              key.startsWith(`topics|${cluster}|`) ||
              key === topicStatisticsKey(cluster) ||
              key === `topic-overview|${cluster}|${topic}` ||
              ["topic-config", "topic-partitions", "topic-consumers"].some((kind) =>
                key === `${kind}|${cluster}/${topic}`) ||
              key === `messages:topic:${cluster}:${topic}`,
            );
            if (!active) return;
            setDeleting(false);
            /* Raised before the navigation, and it survives it: the toast region lives in the shell,
               above the route, so a confirmation for a page that no longer exists is still read on
               the page the operator lands on. */
            notify(`${props.topicName} deleted`, {
              message:
                outcome.value.autoCreateEnabled === true
                  ? "This cluster creates topics automatically, so anything still producing to this name will recreate it."
                  : undefined,
            });
            /* Back to the list, because this page is now about a topic that does not exist. Kafka's
             delete is asynchronous — the controller accepts it and the topic can still appear in a
             listing for a moment — so the list may still show it. That is not a failure and the
             screen says nothing about it; complaining would be this product reporting Kafka's
             ordinary behaviour as a fault. */
            navigate(kui.paths.topics(props.clusterId), { resolve: false });
          });
        }}
      />
    </>
  );
}

/**
 * What emptying this topic destroys, in figures.
 *
 * The count is `null` when any partition could not be read, and the sentence says so rather than
 * quoting a total: a sum over the partitions that *could* be read is a smaller number wearing the
 * confidence of a complete one, and the operator would agree to lose more than they were shown.
 */
export function describePurge(plan: PurgePlan): string {
  const where = `${plan.partitions} ${plan.partitions === 1 ? "partition" : "partitions"}`;
  if (plan.records === null) {
    return `Deletes every record currently in ${where}. At least one partition could not be counted, so the number of records is not known.`;
  }
  return `Deletes ${plan.records.toLocaleString()} ${plan.records === 1 ? "record" : "records"} across ${where}. The topic, its configuration and its partition count are left as they are.`;
}

/** What deleting this topic destroys. */
export function describeDeletion(plan: DeletionPlan): string {
  const where = `${plan.partitions} ${plan.partitions === 1 ? "partition" : "partitions"}`;
  const records =
    plan.records === null
      ? "At least one partition could not be counted, so the number of records is not known."
      : `That is ${plan.records.toLocaleString()} ${plan.records === 1 ? "record" : "records"}.`;
  /*
   * The most useful sentence on this dialog, and the one an operator is least likely to have
   * thought of. With `auto.create.topics.enable` on, deleting a topic something is still producing
   * to does not remove it — it removes the configuration and the data and leaves a fresh topic with
   * the broker's defaults, which is usually the opposite of the intent.
   */
  const recreated =
    plan.autoCreateEnabled === true
      ? " This cluster creates topics automatically, so anything still producing to this name will recreate it immediately — with the broker's default settings, not these."
      : "";
  return `Removes the topic and its ${where}, along with its configuration. ${records}${recreated}`;
}

/**
 * What adding partitions would do, when the server's plan carries no warning of its own.
 *
 * The server always sends `KEY_ROUTING_CHANGES`, so in practice `consequenceOf` uses that sentence
 * and this one never appears — which is the right way round: the server knows the counts, writes
 * the better sentence, and this is here so the dialog is never blank if a future server has nothing
 * to say. It states the count change and nothing else, because the one thing worth warning about is
 * precisely the thing the server's own warning covers.
 */
export function describePartitionIncrease(plan: PartitionPlan): string {
  return `Raises this topic from ${plan.current} to ${plan.target} partitions, adding ${plan.added} ${plan.added === 1 ? "partition" : "partitions"}. Kafka cannot remove a partition, so this cannot be undone.`;
}
