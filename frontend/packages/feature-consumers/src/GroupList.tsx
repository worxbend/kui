/**
 * The consumer-group list — screenshot `04`.
 *
 * Six columns, and six is the whole list: GROUP ID, STATE, MEMBERS, TOPICS, COORDINATOR, LAG.
 *
 * ## What is deliberately not here, and why that is the point
 *
 * The Laminar screen this replaces drew eight columns. Two of them — PARTITIONS and PACE — are gone.
 * PACE is gone because the design does not draw it; PARTITIONS is gone because it was one of several
 * columns that showed an em dash on every row of every cluster the team ever looked at. The brief
 * for this work names the rule: a column that can never be filled teaches people to stop reading
 * columns, so either fill it or leave it out.
 *
 * That is a real loss and it is worth naming. Lag on its own does not answer the operator's actual
 * question, which is "is this getting better?" — a lag of two million falling at forty thousand a
 * second needs nothing, and a lag of nine hundred that is not moving needs attention now. The
 * answer is not to smuggle PACE back into a table the design does not draw it in. The lag poll
 * carries a server-measured rate for a client that asks for one; this table does not, and the
 * detail endpoint does not either, so the detail page's PACE row stays unavailable.
 *
 * ## The count over the table is the cluster's, and the rows are one page of it
 *
 * The voice line reads `totalItems` from the server and the table draws whatever page came back.
 * They are allowed to disagree, and on any cluster with more groups than fit a page they must:
 * screenshot `04` says `14 groups` over six rows. Before this, `healthOf` counted the array, so the
 * sentence was a restatement of the table — true, useless, and wrong about the cluster.
 *
 * ## The row is a link, and it is also a row
 *
 * Every row carries a real `<a href>` in its first cell, because copy-link, bookmark and
 * open-in-new-tab are three gestures a table of names has to support, and none of them survives a
 * `div` with an `onClick`. The row itself is *also* activatable, so the hit target is the whole
 * width — `DataTable` handles the keyboard for that. A modified click (⌘, Ctrl, Shift, Alt) is left
 * alone: it is the user asking their browser for a new tab, and swallowing it breaks the one gesture
 * that makes a table of links worth having.
 *
 * ## The narrow window drops columns rather than hiding them
 *
 * SPEC §7.5: below 900px, COORDINATOR goes first and TOPICS second, and GROUP ID, STATE and LAG
 * never go. They are dropped from the array, not set to `display: none`, because a hidden column is
 * still in the accessibility tree and still counted in the row — a screen-reader user would hear a
 * cell nobody can see.
 */

import { Show, createMemo } from "solid-js";
import type { JSX } from "@solidjs/web";
import {
  Banner,
  Button,
  Card,
  DataTable,
  EmptyState,
  MISSING,
  NARROW_QUERY,
  PageHeader,
  Pagination,
  StatusPill,
  ThresholdValue,
  createMediaQuery,
  formatCount,
  type Column,
  type Sort,
} from "@kui/kernel";
import {
  UNREADABLE_STATE_CHIP,
  groupsVoice,
  healthOf,
  lagAnnouncement,
  lagLevel,
  stateChip,
  type GroupSummary,
} from "./model.js";

export interface GroupListProps {
  readonly rows: readonly GroupSummary[];
  /** How many coordinators did not answer. Drives the voice line and the incomplete chips. */
  readonly coordinatorsMissing?: number | undefined;
  /**
   * How many groups the **cluster** has, as the server counted them.
   *
   * `null` or absent when the server did not say, which the voice line then states in words. It is
   * never replaced by `rows.length`: this table draws one page, and the sentence above it is about
   * the cluster. Screenshot `04` prints `14 groups` over six rows for exactly this reason.
   */
  readonly totalItems?: number | null | undefined;
  /** One-based, and the server's, not this array's index. */
  readonly page?: number | undefined;
  /** How many rows a page holds, as asked of the API. Not `rows.length`, which is what came back. */
  readonly pageSize?: number | undefined;
  /** Asks the server for another page. The control is drawn only when this is supplied. */
  readonly onPage?: ((page: number) => void) | undefined;
  readonly onPageSize?: ((size: number) => void) | undefined;
  readonly loading?: boolean | undefined;
  /**
   * Why the table has no rows, when it has none. `null` means "there is genuinely nothing yet".
   *
   * The four not-happy renderings of SPEC §7.1 are never interchangeable, and this is where the
   * caller says which one applies. In particular "no groups on this cluster" and "nothing matched
   * your filter" are different sentences and one must never be substituted for the other.
   */
  readonly failure?:
    | { readonly kind: "unavailable"; readonly message: string; readonly code: string; readonly onRetry: () => void }
    | { readonly kind: "forbidden"; readonly message: string; readonly code: string }
    | { readonly kind: "filtered"; readonly term: string; readonly onClear: () => void }
    | undefined;
  readonly sort?: Sort | null | undefined;
  readonly onSortChange?: ((next: Sort | null) => void) | undefined;
  /** Where a group's name points. A real URL, so the browser's own gestures work. */
  readonly hrefFor: (groupId: string) => string;
  readonly onOpen?: ((groupId: string) => void) | undefined;
  /**
   * The background lag poll has stopped reaching the cluster. The rows on screen are the last
   * figures it read, not a claim that lag has stopped moving — see `pollLag`'s `onHealth`.
   */
  readonly lagUnavailable?: boolean | undefined;
}

export function GroupList(props: GroupListProps): JSX.Element {
  const narrow = createMediaQuery(NARROW_QUERY);
  const health = createMemo(() =>
    healthOf(
      props.rows,
      props.coordinatorsMissing ?? 0,
      props.totalItems ?? null,
      props.loading === true,
    ),
  );

  /**
   * The voice line, chosen from the health of the rows on screen and never assembled from a
   * template. See `groupsVoice`: SPEC §6.3 rule 3 is that the aside disappears when the state is
   * not healthy, and only a union of whole sentences can guarantee that.
   */
  const voice = createMemo(() => {
    if (props.failure?.kind === "unavailable" || props.failure?.kind === "forbidden") {
      return "Consumer group data is unavailable.";
    }
    // A filtered list's count is the filter's, not the cluster's, and this screen has no figure for
    // either — so it names the filter instead of publishing a group count that would be read as the
    // cluster's. The empty state below carries the way out.
    if (props.failure?.kind === "filtered") {
      return `No consumer group on this cluster is named like ${props.failure.term}.`;
    }
    return groupsVoice(health());
  });

  /*
   * The widths are percentages taken off screenshot `04`, not rem values.
   *
   * The column edges there sit at 235, 710, 975, 1170, 1390 and 1723 in a 1990px frame, which is
   * roughly 28 / 15 / 11 / 11 / 19 / 16. Fixed rem widths were tried first and were wrong at every
   * window size but one: they leave the name column with all the slack, so on a wide screen the
   * group id gets an eighth of the page of empty space and STATE ends up three quarters of the way
   * across. Percentages keep the design's proportions at any width, and the name column — the only
   * one with no width — takes what is left.
   */
  const columns = createMemo<readonly Column<GroupSummary>[]>(() => {
    const all: readonly Column<GroupSummary>[] = [
      {
        id: "id",
        header: "Group id",
        sortable: true,
        render: (row) => <GroupCell row={row} href={props.hrefFor(row.groupId)} onOpen={props.onOpen} />,
      },
      { id: "state", header: "State", sortable: true, width: "15%", render: (row) => <StateCell row={row} /> },
      {
        id: "members",
        header: "Members",
        sortable: true,
        align: "numeric",
        width: "11%",
        // `null` and not `0`: a member count KUI could not read is not a group with no consumers,
        // and printing a zero would say exactly that about a healthy group.
        render: (row) => (row.members === null ? <Unreadable what="member count" /> : formatCount(row.members)),
      },
      { id: "topics", header: "Topics", sortable: true, align: "numeric", width: "11%", render: (row) => formatCount(row.topics) },
      {
        id: "coordinator",
        header: "Coordinator",
        width: "19%",
        render: (row) =>
          row.coordinator === null ? (
            <Unreadable what="coordinator" />
          ) : (
            <span class="kui-cg-mono">{row.coordinator}</span>
          ),
      },
      { id: "lag", header: "Lag", sortable: true, align: "numeric", width: "16%", render: (row) => <LagCell row={row} /> },
    ];
    if (!narrow()) return all;
    return all.filter((column) => column.id !== "coordinator" && column.id !== "topics");
  });

  const failed = (): boolean => props.failure?.kind === "unavailable" || props.failure?.kind === "forbidden";

  /**
   * Whether there may be another page, for a server that carried no total.
   *
   * One weak signal, and it is the only one available: a page that came back full may have a
   * successor. It enables `next` and nothing more — `Pagination` still draws no numbered buttons
   * and no `last` step without a total, because both would be arithmetic over a figure nobody gave.
   * Disabling `next` instead would strand the operator on page 1 of a list the server can page.
   */
  const mayHaveMore = (): boolean =>
    props.pageSize !== undefined && props.rows.length >= props.pageSize;

  /**
   * The paginator, when there is one — and it needs **both** halves, not just a handler.
   *
   * `pageSize` used to fall back to `props.rows.length`, which is the array's own length: the
   * quantity the doc on that prop four lines up forbids, and the one this screen was rewritten to
   * stop publishing. On the last page of a list it is smaller than the page, so the control's
   * "showing X to Y" arithmetic and its page count were both computed off how many rows happened to
   * come back. A caller that can answer `onPage` knows what it asked for, so it supplies both;
   * a story or a test holding a fixed array supplies neither and gets no control.
   */
  const paging = createMemo<
    { readonly onPage: (page: number) => void; readonly pageSize: number } | undefined
  >(() => {
    const onPage = props.onPage;
    const pageSize = props.pageSize;
    if (onPage === undefined || pageSize === undefined) return undefined;
    return { onPage, pageSize };
  });

  return (
    <section class="kui-cg-page" data-testid="consumer-groups">
      <PageHeader title="Consumer groups" voice={voice()} testId="consumer-groups-head" />

      <Show when={props.lagUnavailable === true}>
        <Banner
          tone="warning"
          message="Lag updates are paused — KUI cannot reach the cluster right now. The figures below are the last it read."
          testId="consumer-groups-lag-paused"
        />
      </Show>

      {/*
        A failure gets a titled card and the happy path does not, and that asymmetry is deliberate.
        Screenshot `04` draws the table as the panel — no heading above it, because the page title
        four lines up already says what these rows are, and a second "Groups" heading is a word a
        screen reader reads twice. But SPEC §7.1's rule is that *the frame never disappears*: when
        the request fails there must still be a named box on the page holding the explanation, the
        error code and the way out, so that a page can show three healthy panels and one that failed
        without either lying. So the frame appears exactly when there is nothing else to hold it up.
      */}
      <Show
        when={!failed()}
        fallback={
          <Card
            title="Consumer groups"
            state={props.failure?.kind === "forbidden" ? "forbidden" : "unavailable"}
            message={props.failure?.kind === "unavailable" || props.failure?.kind === "forbidden" ? props.failure.message : undefined}
            description={
              props.failure?.kind === "forbidden"
                ? "Ask an administrator for read access to consumer groups on this cluster."
                : "The consumer service is not responding."
            }
            code={props.failure?.kind === "unavailable" || props.failure?.kind === "forbidden" ? props.failure.code : undefined}
            stateAction={
              props.failure?.kind === "unavailable" ? (
                <Button variant="secondary" icon="refresh" onClick={props.failure.onRetry}>
                  Retry
                </Button>
              ) : undefined
            }
            testId="consumer-groups-card"
          />
        }
      >
        <DataTable<GroupSummary>
          caption="Consumer groups on this cluster"
          columns={columns()}
          rows={props.rows}
          rowKey={(row) => row.groupId}
          sort={props.sort ?? null}
          onSortChange={props.onSortChange}
          onRowClick={props.onOpen === undefined ? undefined : (row) => props.onOpen?.(row.groupId)}
          loading={props.loading}
          testId="consumer-groups-table"
          empty={
            props.failure?.kind === "filtered" ? (
              <EmptyState
                kind="filtered"
                title={`Nothing matched ${props.failure.term}.`}
                description="No consumer group on this cluster has a name like that."
                action={
                  <Button variant="secondary" onClick={props.failure.onClear}>
                    Clear filter
                  </Button>
                }
              />
            ) : (
              <EmptyState
                kind="empty"
                title="No consumer groups yet."
                description="A group appears here the first time something consumes from this cluster."
              />
            )
          }
        />

        {/*
          The paging control, drawn only when a caller can actually answer it.

          `SCREENS-V4.md` §4.12 records that the capture draws none — `14 groups` over six rows and
          no way to the other eight. This packet's brief asks for one, and the two go together: the
          sentence is only allowed to name a figure larger than the table once the table can be
          moved through. A story or a test that hands this a fixed array supplies no `onPage` and
          gets no control, rather than one that does nothing.
        */}
        <Show when={paging()}>
          {(control) => (
            <Pagination
              page={props.page ?? 1}
              pageSize={control().pageSize}
              // The server's figure, straight through. `undefined` is `Pagination`'s own word for
              // "no total was given", and it then hides the numbered buttons rather than guessing
              // a last page — which is the same refusal the voice line makes in words.
              total={props.totalItems ?? undefined}
              shown={props.rows.length}
              hasNext={mayHaveMore()}
              onPage={control().onPage}
              onPageSize={props.onPageSize}
              label="Consumer group pages"
              testId="consumer-groups-pagination"
            />
          )}
        </Show>
      </Show>
    </section>
  );
}

function GroupCell(props: {
  readonly row: GroupSummary;
  readonly href: string;
  readonly onOpen?: ((groupId: string) => void) | undefined;
}): JSX.Element {
  return (
    <span class="kui-cg-name">
      <a
        class="kui-cg-name__link"
        href={props.href}
        onClick={(event: MouseEvent) => {
          // Only a plain left click is ours. A modified click is the user asking their browser for
          // a new tab, and intercepting it breaks the gesture a table of links exists for.
          if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
          if (props.onOpen === undefined) return;
          event.preventDefault();
          // The row is also clickable; without this the row handler fires too and the navigation
          // happens twice.
          event.stopPropagation();
          props.onOpen(props.row.groupId);
        }}
      >
        {props.row.groupId}
      </a>
      <Show when={props.row.incomplete}>
        {(incomplete) => (
          <StatusPill tone="warning" title={incomplete().note}>
            partial
          </StatusPill>
        )}
      </Show>
    </span>
  );
}

function StateCell(props: { readonly row: GroupSummary }): JSX.Element {
  const chip = () => (props.row.state === null ? UNREADABLE_STATE_CHIP : stateChip(props.row.state));
  return (
    <StatusPill tone={chip().tone} title={chip().title}>
      {chip().label}
    </StatusPill>
  );
}

/**
 * The lag figure, and the em dash that is not a zero.
 *
 * **No bar.** The screen this replaces drew a magnitude bar in every lag cell, scaled to the
 * largest lag on the page. Screenshot `04` does not: the LAG column is a right-aligned tabular
 * figure and nothing else, and the bars live in the dashboard's "Top consumer lag" panel where
 * there is room for them. Drawing one in a cell a hundred and fifty pixels wide puts a four-pixel
 * smudge beside every number, which says nothing and costs the column its scannability.
 *
 * **No change highlight either, and that is a decision rather than an omission.** `ConsumersRoute`
 * now refreshes this column incrementally (see `lag.ts`), so the screen knows exactly which rows the
 * server said had moved and could flash them. It does not, for three reasons:
 *
 * - On a cluster that is actually doing work, nearly every group's lag changes on nearly every poll.
 *   A marker that fires on almost every row marks nothing — and the one row it fails to fire on is
 *   then read as the odd one out, which is backwards.
 * - The interesting quantity is not *that* the figure moved but which way and how fast, and this
 *   column cannot show that. A group falling from two million to one million and a group climbing
 *   from nine hundred to a thousand would get the same flash. The rate is not drawn here, and the
 *   detail page cannot show it either: that endpoint does not carry a measured rate.
 * - The row is a link people click. A cell that repaints under a moving pointer every thirty seconds
 *   makes a table feel untrustworthy while telling nobody anything.
 *
 * What the incremental refresh must never do is make an *unchanged* row look stale or absent. It
 * does not: `applyLagDelta` passes an unmentioned row through by identity, so this cell keeps drawing
 * the last figure the server stood behind — including a `null` that stays an em dash.
 */
function LagCell(props: { readonly row: GroupSummary }): JSX.Element {
  return (
    <Show when={props.row.totalLag !== null} fallback={<Unreadable what="lag" />}>
      <span class="kui-cg-lag" title={excludedNote(props.row)}>
        <ThresholdValue
          value={formatCount(props.row.totalLag ?? 0)}
          level={lagLevel(props.row.totalLag ?? 0)}
          announcement={lagAnnouncement}
          class={props.row.totalLag === 0 ? "kui-cg-lag__zero" : undefined}
          data-testid={`group-${props.row.groupId}-lag`}
        />
      </span>
    </Show>
  );
}

/**
 * A total computed over fewer partitions than the group holds is smaller than the truth, and
 * nothing else on the row would say so.
 */
function excludedNote(row: GroupSummary): string | undefined {
  if (row.excludedPartitions === 0) return undefined;
  return `${formatCount(row.excludedPartitions)} partition${row.excludedPartitions === 1 ? "" : "s"} could not be read, so this total is lower than the real lag.`;
}

/** An em dash with the reason attached. Never an empty cell, and never a zero. */
function Unreadable(props: { readonly what: string }): JSX.Element {
  return (
    <span class="kui-cg-missing" title={`KUI could not read this group's ${props.what}.`}>
      <span aria-hidden="true">{MISSING}</span>
      <span class="kui-visually-hidden">{`${props.what} unavailable`}</span>
    </span>
  );
}

/** Exported for the stories and the tests, which need to draw the pieces without a page around them. */
export { GroupCell, LagCell, StateCell, excludedNote };
