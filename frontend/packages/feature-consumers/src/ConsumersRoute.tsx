/**
 * The consumer feature's route entry: the screen the shell renders for every `/consumer-groups…`
 * address.
 *
 *   /clusters/:clusterId/consumer-groups            the list
 *   /clusters/:clusterId/consumer-groups/:groupId   one group
 *
 * It used to render `SAMPLE_GROUPS` — a screen that looked like it worked and showed invented
 * groups with invented lag. That is the most dangerous state this product can be in: an operator
 * checking whether a consumer is behind would have got a confident answer about a group that does
 * not exist.
 *
 * ## Paging, and the count that is not the array's length
 *
 * The list is one page of the cluster's groups. The page number is this screen's state and it is
 * part of the query key, so asking for page 2 is a different request rather than the same request
 * re-read; `GroupList` gets the server's `totalItems` and prints that over the table. Nothing here
 * derives a total from `rows`.
 *
 * ## `useQuery` rather than the fourth hand-rolled fetch hook
 *
 * The copy that used to live here started every attempt at `loading`, so a poll that failed blanked
 * a table of real figures. `useQuery` keeps the last good answer and marks it stale, which is
 * ADR-032's rule, and it shares one request per key with anything else asking for the same page.
 * (The name of the hook it replaced is deliberately not written here: M6's exit criterion greps
 * this tree for it, and a comment naming it would keep a closed migration looking open.)
 */
import { Show, createEffect, createMemo, createSignal } from "solid-js";
import type { JSX } from "@solidjs/web";
import { useParams } from "@solidjs/router";
import { useKui, useQuery, valueOf } from "@kui/kernel";
import { GroupList } from "./GroupList.jsx";
import { GroupRoute } from "./GroupRoute.jsx";
import { DEFAULT_PAGE_SIZE, fetchGroups, type GroupListResult, type GroupQuery } from "./data.js";
import { pollLag, type ListingFigures } from "./lag.js";
import type { GroupSummary } from "./model.js";

export default function Consumers(): JSX.Element {
  const params = useParams<{ readonly clusterId?: string; readonly groupId?: string }>();
  /*
   * Keyed by cluster, as the topic, message and schema routes are: the router keeps this route
   * mounted when only `clusterId` changes, and an unkeyed screen carried one cluster's page number
   * into the next cluster's request and drew its rows under the next cluster while that loaded.
   * The group page keys itself on both of its parameters.
   */
  return (
    <Show keyed when={params.clusterId} fallback={<NoCluster />}>
      {(clusterId) => (
        <Show when={params.groupId} fallback={<GroupsScreen clusterId={clusterId} />}>
          {/* Every row in the list links here, and until now every one of those links landed back
              on the list: the detail page and the offset-reset wizard were both built and neither
              had a route to reach them from. */}
          <GroupRoute />
        </Show>
      )}
    </Show>
  );
}

function NoCluster(): JSX.Element {
  const kui = useKui();
  return (
    <section aria-label="Consumer groups">
      <p role="status">
        No cluster is selected, so there are no consumer groups to list.{" "}
        <a href={kui.paths.clusters()}>Choose a cluster</a> and try again.
      </p>
    </section>
  );
}

/**
 * The list screen.
 *
 * Exported so that a test can mount the thing the product mounts. The route wrapper above reads the
 * address and this draws the page; asserting against `GroupList` alone would leave the join between
 * the data layer and the screen — which is where the page number and the server's total are
 * threaded — with nothing watching it.
 */
export function GroupsScreen(props: { readonly clusterId: string }): JSX.Element {
  const kui = useKui();
  const [page, setPage] = createSignal(1);
  const [pageSize, setPageSize] = createSignal(DEFAULT_PAGE_SIZE);

  const query = createMemo<GroupQuery>(() => ({ page: page(), pageSize: pageSize() }));

  /*
   * The key carries everything that changes the request, which here is the cluster and the page.
   * That is `useQuery`'s whole contract: a filter left out of the key is a screen whose controls
   * silently drive somebody else's data, and a page left out of it is page 2 answered with page 1's
   * rows out of the cache.
   */
  const groups = useQuery<GroupListResult>({
    key: () => `consumer-groups:${props.clusterId}:${query().page}:${query().pageSize}`,
    load: () => fetchGroups(kui.api, props.clusterId, query()),
  });

  /*
   * The rows the screen draws, held apart from the request that first produced them.
   *
   * They have to be a signal of their own rather than a derivation of `groups.state()`, because
   * after the first answer they are edited in place by the lag poll — and the query's state must
   * stay what the list request said, so that `failure()` below still describes the right thing.
   */
  const [rows, setRows] = createSignal<readonly GroupSummary[]>([]);
  const [coordinatorsMissing, setCoordinatorsMissing] = createSignal(0);
  /** The server's account of the page. `null` until an answer has arrived; never a row count. */
  const [total, setTotal] = createSignal<number | null>(null);
  /** Whether the background lag poll's last attempt reached the cluster. See `pollLag`'s `onHealth`. */
  const [lagHealthy, setLagHealthy] = createSignal(true);

  createEffect(
    () => groups.state(),
    (current) => {
      if (current.kind !== "loading") kui.report("feature", current.kind === "failed");
      const listing = valueOf(current, undefined);
      if (listing === undefined) return;
      setRows(() => listing.groups);
      setCoordinatorsMissing(listing.coordinatorsMissing);
      setTotal(listing.page.totalItems);
    },
  );

  // Polling starts only once there is a list to merge into, and stops when this screen goes away —
  // a timer left running after unmount holds the whole component graph alive and keeps asking a
  // cluster the operator has navigated away from. The page is in the dependency list because a
  // full-list refetch has to ask for the page on screen, not for the first one.
  createEffect(
    () => [props.clusterId, query(), groups.state().kind === "ready"] as const,
    ([clusterId, current, ready]) => {
      if (!ready) return undefined;
      // The returned function is the effect's cleanup: Solid runs it before the next run and again
      // when the screen is disposed.
      return pollLag(
        kui.api,
        clusterId,
        rows,
        // Annotated rather than inferred: `null` and a figure block are the two halves of the
        // contract this callback is holding up, and spelling the type out is what makes the
        // `null` branch below read as a case rather than as a defensive check.
        (next: readonly GroupSummary[], listing: ListingFigures | null, removedCount: number) => {
          setRows(() => next);
          // `null` is a merge: the delta named no coordinator count and no total, so the screen
          // keeps the ones the list request gave it rather than resetting them to zero — except
          // for the rows the delta itself said were gone, which the server's own total can no
          // longer be counting either.
          if (listing === null) {
            if (removedCount > 0) setTotal((current) => (current === null ? current : current - removedCount));
            return;
          }
          setCoordinatorsMissing(listing.coordinatorsMissing);
          setTotal(listing.totalItems);
        },
        current,
        setLagHealthy,
      );
    },
  );

  /**
   * The failure, in the screen's own vocabulary.
   *
   * `GroupList` distinguishes `unavailable` from `forbidden` because only one of them has a retry
   * that could work — and neither is the same as "your filter matched nothing", which is the third
   * case its type carries and which the screen raises itself.
   */
  const failure = () => {
    const current = groups.state();
    switch (current.kind) {
      case "failed":
        return {
          kind: "unavailable" as const,
          message: current.message,
          code: current.code,
          onRetry: groups.reload,
        };
      case "forbidden":
        return {
          kind: "forbidden" as const,
          message: "You do not have permission to see this cluster's consumer groups.",
          code: "FORBIDDEN",
        };
      case "not-configured":
        return {
          kind: "unavailable" as const,
          message: "This deployment has no consumer group service configured.",
          code: "NOT_CONFIGURED",
          onRetry: groups.reload,
        };
      default:
        return undefined;
    }
  };

  return (
    <GroupList
      rows={rows()}
      coordinatorsMissing={coordinatorsMissing()}
      totalItems={total()}
      page={page()}
      pageSize={pageSize()}
      onPage={setPage}
      onPageSize={(size) => {
        setPageSize(size);
        // Page 9 of a list at 8 a page is page 3 at 32, and there is no honest way to keep the
        // operator's place across the change. Going back to the first page is the one answer that
        // is never a page that does not exist.
        setPage(1);
      }}
      loading={groups.state().kind === "loading"}
      failure={failure()}
      lagUnavailable={!lagHealthy()}
      hrefFor={(groupId) => kui.paths.consumerGroup(props.clusterId, groupId)}
    />
  );
}
