/**
 * The message browser: the screen the product is used on more than any other.
 *
 * ## Nothing is read until somebody asks
 *
 * Opening this tab does not start a browse. Every other screen in KUI fetches on mount because its
 * answer is a fixed, small document; a browse is a Kafka consumer reading an unbounded log, and
 * starting one for anybody who so much as clicked the wrong tab is how a cluster ends up with
 * consumers nobody asked for. So the controls are drawn, the empty state says what to do, and Read
 * starts it.
 *
 * ## Where it starts is a choice, and the choice is in the URL
 *
 * Beginning, end, an offset, or a time — the four ways anybody actually describes where to look.
 * All of them live in the query string under the *service's own* parameter names, which means a
 * browse is a link: an operator who has found the record that matters sends the URL and their
 * colleague sees the same records. It also means the Back button undoes a filter, which is what a
 * browser's Back button is for.
 *
 * The per-partition seek the contract also supports has no control of its own — it would be a table
 * of sixty inputs — but it survives a URL round trip, so a link carrying one keeps working.
 *
 * ## Following live
 *
 * LIVE starts at the end and keeps the stream open. It is exclusive with a start position and the
 * server refuses the combination, so turning it on *sets* the seek to `latest` rather than sending
 * both. The pause is separate and is not a stop: it holds new records back while the stream keeps
 * reading, so the row somebody is looking at stays where it is and nothing is lost. See
 * `session.ts`.
 *
 * ## Stopping is real
 *
 * Stop aborts the request, and that abort travels: the gateway's stream is cancelled, the service's
 * fiber is cancelled and its Kafka consumer is closed (ADR-035). Unmounting does the same thing, so
 * navigating away cannot leave a consumer running.
 *
 * ## The bar is outside everything that re-renders
 *
 * The filter bar is a sibling of the list, not a child of anything whose condition flips when
 * records land. That is the whole fix for "the drawer rebuilt while I was typing"; see the header of
 * `MessageFilterBar.tsx`.
 */

import type { JSX } from "@solidjs/web";
import { For, Show, createEffect, createMemo, createSignal, onCleanup, untrack } from "solid-js";
import {
  Button,
  EmptyState,
  Icon,
  SegmentedControl,
  Spinner,
  RecordList,
  RecordRow,
  recordKey,
  type KafkaRecord,
  type MessageViewMode,
} from "@kui/kernel";
import { userMessage } from "@kui/api";
import {
  MessageFilterBar,
  type LiveAvailability,
  type SmartFilterSlot,
} from "./MessageFilterBar.jsx";
import type { BrowseQuery, SeekMode } from "./browse.js";
import { isEmpty, type Predicates } from "./predicates.js";
import type { FilterPreset } from "./presets.js";
import { MAX_ROWS, type BrowseEndReason, type BrowseSession } from "./session.js";

export interface MessagesTabProps {
  readonly topic: string;
  /**
   * How many partitions the topic has, or `undefined` when KUI has not been told.
   *
   * Passed straight to the bar, which draws the difference. This screen never substitutes a number
   * for the absence: a count is a measurement, and the one thing it must not become is a zero.
   */
  readonly partitionCount?: number | undefined;
  /** The browse the URL describes. This component never writes it; it asks. */
  readonly query: BrowseQuery;
  /**
   * Asks for the URL to change. The screen above owns the address bar, because a browse is a link
   * and only one thing may write it.
   */
  readonly onQueryChange: (query: BrowseQuery) => void;
  readonly session: BrowseSession;
  /** Saved presentation used until this screen's operator chooses another one. */
  readonly defaultView?: MessageViewMode | undefined;
  /** Whether this principal may publish into this topic. */
  readonly mayProduce?: boolean | undefined;
  /** Why not, when they may not. A disabled control without a reason is a control that looks broken. */
  readonly produceDisabledReason?: string | undefined;
  readonly onProduce?: (() => void) | undefined;
  readonly liveAvailability?: LiveAvailability | undefined;

  /**
   * The smart-filter control, when the screen above offers one.
   *
   * Passed straight through to the bar. `MessagesTab` deliberately does not own it: registering a
   * filter is a request, and this component has no client — the same reason producing is a callback
   * and not a form here.
   */
  readonly smartFilter?: SmartFilterSlot | undefined;

  /**
   * The typed predicates and the upper bounds, and the way to change them.
   *
   * Required rather than optional, unlike everything else on this component, because they are not a
   * decoration: the bar draws two controls for them and a screen that rendered those controls with
   * nowhere for their changes to go would be a screen whose filters silently do nothing.
   */
  readonly predicates: Predicates;
  readonly onPredicatesChange: (predicates: Predicates) => void;

  /**
   * What Read does.
   *
   * The default is `session.start(query)` — which is right for a browse with nothing to compile, and
   * wrong the moment there is: a predicate has to be registered with the service before a browse can
   * quote it, and that is a request, which this component has no client for. The route supplies this
   * and does the registration first.
   */
  readonly onRead?: (() => void) | undefined;

  /**
   * Why the last Read did not start a browse at all.
   *
   * Distinct from the session's own failure, which is a browse that started and went wrong. This one
   * is shown *instead* of a browse — a filter the cluster's engine refused to compile, most often —
   * and it must be said out loud, because the alternative is a Read button that appears to do
   * nothing.
   */
  readonly refusal?: string | undefined;

  /**
   * Whether {@link MessagesTabProps.onRead} is still getting ready.
   *
   * A Read that has to register a filter first is a round trip before a single record can arrive,
   * and a button that looked idle through it is a button somebody presses twice. The second press
   * is refused by the mutation's own guard, so what they see is a control that does nothing.
   */
  readonly readBusy?: boolean | undefined;

  /** The saved arrangements this browser holds, and what may be done with them. */
  readonly presets?: readonly FilterPreset[] | undefined;
  readonly onApplyPreset?: ((preset: FilterPreset) => void) | undefined;
  readonly onRemovePreset?: ((preset: FilterPreset) => void) | undefined;
  readonly onSavePreset?: (() => void) | undefined;

  /** Copy a range of these records into another topic. */
  readonly onResend?: (() => void) | undefined;
  readonly mayResend?: boolean | undefined;
  readonly resendDisabledReason?: string | undefined;

  /** The clock, passed in so relative times are testable. */
  readonly now?: number | undefined;
}

export function MessagesTab(props: MessagesTabProps): JSX.Element {
  const session = props.session;
  const [viewOverride, setViewOverride] = createSignal<MessageViewMode>();
  const effectiveView = (): MessageViewMode =>
    props.query.live ? "infinite" : (viewOverride() ?? props.defaultView ?? "pages");

  /* The filter box's own text, which is not the same thing as the browse's `contains`. The box
   * holds what has been typed; the query holds what has been asked for. Conflating them is what
   * makes a filter field jump back to the last committed value mid-word. The effect below re-seeds
   * it only when `contains` changed for a reason other than the box's own commit — Back/Forward
   * chief among them — mirroring `FieldPredicateControl` in MessageFilterBar.tsx. */
  const [filterText, setFilterText] = createSignal(untrack(() => props.query.contains ?? ""));
  let emittedContains = untrack(() => props.query.contains ?? "");

  createEffect(
    () => props.query.contains ?? "",
    (incoming) => {
      if (incoming !== emittedContains) {
        emittedContains = incoming;
        setFilterText(incoming);
      }
    },
  );

  /* The clock, ticking, so "2s ago" becomes "3s ago" without every record row owning a timer. One
   * interval for the screen; five hundred rows read it. */
  const [now, setNow] = createSignal(props.now ?? Date.now());
  const ticking = setInterval(() => setNow(props.now ?? Date.now()), 1000);
  onCleanup(() => clearInterval(ticking));

  const rows = (): readonly KafkaRecord[] =>
    effectiveView() === "pages" ? session.pageRows() : session.rows();
  const failure = createMemo(() => session.progress().failure);

  const [preloadSentinel, setPreloadSentinel] = createSignal<HTMLDivElement>();
  createEffect(
    () => ({
      view: effectiveView(),
      canLoadMore: session.canLoadMore() && session.rows().length < MAX_ROWS,
      sentinel: preloadSentinel(),
    }),
    ({ view: currentView, canLoadMore, sentinel }) => {
      if (
        currentView !== "infinite" ||
        !canLoadMore ||
        sentinel === undefined ||
        typeof IntersectionObserver === "undefined"
      ) return;

      const observer = new IntersectionObserver(
        (entries) => {
          if (entries.some((entry) => entry.isIntersecting) && session.canLoadMore()) {
            session.loadMore();
          }
        },
        /* Start the next cursor while the user is still roughly one screen away from the end. The
         * visible fallback button below remains usable when observers are unavailable or disabled. */
        { rootMargin: "720px 0px" },
      );
      observer.observe(sentinel);
      /* Returned, not registered with `onCleanup`: Solid 2 runs an effect's callback with no owner,
       * so an `onCleanup` here is never run. Every completed page re-runs this effect, and an
       * observer the re-run did not disconnect keeps loading after the newest run has stopped at
       * the display limit — and after the screen is gone. Same rule as `kernel/src/media.ts`. */
      return () => observer.disconnect();
    },
  );

  /* Commits whatever the bar's debounced boxes still hold. See `flushRef` on the bar. */
  let flushBar: (() => void) | undefined;

  function read(): void {
    /* Text typed inside the debounce is part of what Read asks for. Committing it first writes it
       to the address the route's Read composes from, and leaves no timer behind to stop the
       browse this starts. */
    flushBar?.();
    /* The route's Read, when there is one: it compiles and registers the typed predicates before a
       browse quotes them. Falling back to starting the session directly keeps this component
       usable on its own, which every story and every test in this package relies on. */
    if (props.onRead !== undefined) {
      props.onRead();
      return;
    }
    session.start(props.query);
  }

  /**
   * Changing anything about *where* to read stops whatever is running.
   *
   * A browse in flight is reading a different range from the one the controls now describe, and
   * letting it keep delivering into the list would mix two ranges with nothing on screen to say
   * why.
   */
  function change(next: BrowseQuery): void {
    session.stop();
    props.onQueryChange(next);
  }

  function setSeek(seek: SeekMode): void {
    /* Following and a start position are exclusive and the server refuses the pair. Choosing a
       start therefore turns following off, rather than sending a request that will be refused. */
    change({ ...props.query, seek, live: seek.kind === "latest" ? props.query.live : false });
  }

  function setLive(live: boolean): void {
    // Following *is* "start at the end", so turning it on says so rather than sending both.
    change({ ...props.query, live, seek: live ? { kind: "latest" } : props.query.seek });
  }

  return (
    <section class="kui-browse" aria-label={`Messages in ${props.topic}`}>
      <MessageFilterBar
        seek={props.query.seek}
        onSeekChange={setSeek}
        partitionCount={props.partitionCount}
        partitions={props.query.partitions}
        onPartitionsChange={(partitions) => change({ ...props.query, partitions })}
        filter={filterText()}
        onFilterChange={setFilterText}
        flushRef={(flush) => (flushBar = flush)}
        onFilterCommit={(text) => {
          emittedContains = text;
          change({ ...props.query, ...(text === "" ? { contains: undefined } : { contains: text }) });
        }}
        live={props.query.live}
        onLiveChange={setLive}
        predicates={props.predicates}
        /* Changing a predicate stops a running browse for exactly the reason changing the seek
           does: the records still arriving were selected by the expression the browse was started
           with, and mixing them with the ones the new predicate would keep is two answers in one
           list with nothing on screen to say so. */
        onPredicatesChange={(predicates) => {
          session.stop();
          props.onPredicatesChange(predicates);
        }}
        {...(props.now === undefined ? {} : { now: props.now })}
        {...(props.presets === undefined ? {} : { presets: props.presets })}
        {...(props.onApplyPreset === undefined ? {} : { onApplyPreset: props.onApplyPreset })}
        {...(props.onRemovePreset === undefined ? {} : { onRemovePreset: props.onRemovePreset })}
        {...(props.onSavePreset === undefined ? {} : { onSavePreset: props.onSavePreset })}
        {...(props.liveAvailability === undefined ? {} : { liveAvailability: props.liveAvailability })}
        {...(props.smartFilter === undefined ? {} : { smartFilter: props.smartFilter })}
      >
        <Show
          when={session.running()}
          fallback={
            <Button
              variant="primary"
              size="sm"
              icon="refresh"
              busy={props.readBusy === true}
              onClick={read}
            >
              Read
            </Button>
          }
        >
          {/* Two controls while a browse runs, and they are different verbs. Pause holds records
              back with the consumer still reading; Stop closes it. Offering only one of them is
              what made people stop a tail to read one row and lose everything after it. */}
          <Button
            variant="secondary"
            size="sm"
            icon={session.paused() ? "chevron-down" : "minus"}
            onClick={() => session.setPaused(!session.paused())}
          >
            {/* The count appears only when there is one. `Resume (0)` was on screen for a second
                in the story that pauses a stream nothing has arrived on since — a zero drawn as a
                quantity, which is the rule this product has broken before with a bar that drew a
                full-width track for it. */}
            {pauseLabel(session.paused(), session.held())}
          </Button>
          <Button variant="secondary" size="sm" icon="close" onClick={() => session.stop()}>
            Stop
          </Button>
        </Show>
      </MessageFilterBar>

      <BrowseStatus session={session} />

      <div class="kui-browse__view-bar">
        <SegmentedControl<"pages" | "infinite">
          label="Message loading mode"
          size="sm"
          value={effectiveView()}
          segments={[
            { value: "pages", label: "Pages", icon: "table", disabled: props.query.live },
            { value: "infinite", label: "Infinite scroll", icon: "chevron-down" },
          ]}
          onChange={setViewOverride}
        />
        <Show when={props.query.live}>
          <span class="kui-browse__view-note">Live browsing follows new offsets continuously.</span>
        </Show>
      </div>

      {/* A browse that never started. It sits above the failure, because it is about the request
          the operator just made rather than about the one that is still running. */}
      <Show when={props.refusal}>
        {(refused) => (
          <p class="kui-browse__failure" role="alert">
            <span class="kui-browse__failure-text">{refused()}</span>
          </p>
        )}
      </Show>

      {/* The failure sits beside the records rather than replacing them. The records that did
          arrive are still what the user asked for, and clearing the list to show an error would
          throw away the evidence they were reading. */}
      <Show when={failure()}>
        {(problem) => (
          <p class="kui-browse__failure" role="alert">
            <span class="kui-browse__failure-text">{describe(problem())}</span>
          </p>
        )}
      </Show>

      <Show
        when={rows().length > 0}
        fallback={
          <BrowseEmpty
            running={session.running()}
            everRan={session.progress().connection.phase !== "idle"}
            filtered={
              props.query.contains !== undefined ||
              props.query.filterId !== undefined ||
              /* An expression-only preset writes the source and no id; Read registers it. */
              props.query.filterSource !== undefined ||
              !isEmpty(props.predicates)
            }
            byExpression={props.query.filterSource}
            filterErrors={session.progress().consumed?.filterErrors ?? 0}
            endReason={session.progress().endReason}
            canContinue={session.canNextPage()}
            onRead={read}
          />
        }
      >
        <RecordList label={`Records in ${props.topic}`}>
          <For each={rows()} keyed={(record) => recordKey(record)}>
            {(record) => (
              <RecordRow
                record={record()}
                now={now()}
                arrived={session.arrived().has(recordKey(record()))}
              />
            )}
          </For>
        </RecordList>
      </Show>

      <Show
        when={
          effectiveView() === "pages" &&
          (rows().length > 0 ||
            session.pageNumber() > 1 ||
            session.canPreviousPage() ||
            session.canNextPage())
        }
      >
        <OffsetPagination session={session} rows={rows()} />
      </Show>

      <Show when={effectiveView() === "infinite" && (rows().length > 0 || session.canLoadMore())}>
        <div
          class="kui-browse__infinite-sentinel"
          ref={setPreloadSentinel}
          aria-live="polite"
        >
          <span class="kui-browse__offset-range">{offsetRangeLabel(rows())}</span>
          <Show when={session.canLoadMore() && rows().length < MAX_ROWS}>
            <Button variant="secondary" size="sm" onClick={() => session.loadMore()}>
              Load next offsets
            </Button>
          </Show>
          <Show when={session.canLoadMore() && rows().length >= MAX_ROWS}>
            <span class="kui-browse__display-limit" role="status">
              {MAX_ROWS.toLocaleString()}-record display limit reached. Switch to Pages to continue.
            </span>
          </Show>
          <Show when={session.running() && rows().length > 0}>
            <span class="kui-browse__preloading">
              <Spinner size="16px" /> Preloading next offsets…
            </span>
          </Show>
        </div>
      </Show>

      <Show when={props.onProduce !== undefined}>
        <div class="kui-browse__produce">
          <Button
            variant="secondary"
            icon="send"
            {...(props.mayProduce === false
              ? {
                  disabled: true as const,
                  disabledReason:
                    props.produceDisabledReason ??
                    "You do not hold a role that permits producing to this topic.",
                }
              : {})}
            onClick={() => props.onProduce?.()}
          >
            Produce message
          </Button>

          {/* Beside produce rather than in the bar: both of these *write*, and grouping them keeps
              the row of controls that change a cluster in one place instead of scattered among the
              ones that only read it. */}
          <Show when={props.onResend !== undefined}>
            <Button
              variant="secondary"
              icon="refresh"
              {...(props.mayResend === false
                ? {
                    disabled: true as const,
                    disabledReason:
                      props.resendDisabledReason ??
                      "You do not hold a role that permits copying records out of this topic.",
                  }
                : {})}
              onClick={() => props.onResend?.()}
            >
              Copy records out
            </Button>
          </Show>
        </div>
      </Show>
    </section>
  );
}

function OffsetPagination(props: {
  readonly session: BrowseSession;
  readonly rows: readonly KafkaRecord[];
}): JSX.Element {
  return (
    <nav class="kui-browse__pagination" aria-label="Message offset pages">
      <p class="kui-browse__offset-range" aria-live="polite">
        Page {props.session.pageNumber().toLocaleString()} · {offsetRangeLabel(props.rows)} · {plural(props.rows.length, "record")}
      </p>
      <div class="kui-browse__page-actions">
        <button
          type="button"
          class="kui-browse__page-button kui-focusable"
          aria-label="Previous offset page"
          disabled={!props.session.canPreviousPage()}
          onClick={() => props.session.previousPage()}
        >
          <Icon name="chevron-left" />
          <span>Previous</span>
        </button>
        <button
          type="button"
          class="kui-browse__page-button kui-focusable"
          aria-label="Next offset page"
          disabled={!props.session.canNextPage()}
          onClick={() => props.session.nextPage()}
        >
          <span>Next</span>
          <Icon name="chevron-right" />
        </button>
      </div>
    </nav>
  );
}

/** A Kafka page has no stable global row number. Name the bounds the broker does have instead. */
export function offsetRangeLabel(rows: readonly KafkaRecord[]): string {
  const byPartition = new Map<number, string[]>();
  for (const row of rows) {
    const offsets = byPartition.get(row.partition) ?? [];
    offsets.push(row.offset);
    byPartition.set(row.partition, offsets);
  }
  if (byPartition.size === 0) return "No offsets loaded";

  const ranges = [...byPartition.entries()]
    .sort(([left], [right]) => left - right)
    .map(([partition, offsets]) => {
      const ordered = offsets.slice().sort(compareOffsets);
      return `p${String(partition)} offsets ${ordered[0]}–${ordered[ordered.length - 1]}`;
    });
  const shown = ranges.slice(0, 3).join(" · ");
  const remainder = ranges.length - 3;
  return remainder > 0 ? `${shown} · +${String(remainder)} partitions` : shown;
}

function compareOffsets(left: string, right: string): number {
  const a = left.replace(/^0+(?=\d)/, "");
  const b = right.replace(/^0+(?=\d)/, "");
  if (/^\d+$/.test(a) && /^\d+$/.test(b)) {
    if (a.length !== b.length) return a.length - b.length;
    return a < b ? -1 : a > b ? 1 : 0;
  }
  return left.localeCompare(right);
}

/**
 * What the stream is doing, how many records arrived, and how much Kafka was read to find them.
 *
 * The scanned figure is the one that makes a filtered browse interpretable: a scan over a large
 * topic routinely reads a million records and matches none, and without this line that screen is
 * identical to a topic that is empty.
 */
function BrowseStatus(props: { readonly session: BrowseSession }): JSX.Element {
  const progress = (): ReturnType<BrowseSession["progress"]> => props.session.progress();
  return (
    <p class="kui-browse__status" aria-live="polite">
      <span class="kui-browse__phase">{phaseSentence(props.session)}</span>
      <Show when={progress().delivered > 0}>
        <span class="kui-browse__figure">{plural(progress().delivered, "record")}</span>
      </Show>
      <Show when={progress().consumed}>
        {(consumed) => (
          <span class="kui-browse__figure">
            {/* "Scanned", not "read": the number is how much of the log was examined, which on a
                filtered browse is a much larger number than the one beside it, and the two being
                different is the point. */}
            scanned {plural(consumed().records, "record")}
          </span>
        )}
      </Show>
      <Show when={(progress().consumed?.filterErrors ?? 0) > 0}>
        <span class="kui-browse__figure kui-browse__figure--warning">
          {plural(progress().consumed?.filterErrors ?? 0, "filter evaluation")} failed
        </span>
      </Show>
      <Show when={props.session.paused() && props.session.held() > 0}>
        <span class="kui-browse__figure kui-browse__figure--held">
          {plural(props.session.held(), "record")} held
        </span>
      </Show>
    </p>
  );
}

function phaseSentence(session: BrowseSession): string {
  const progress = session.progress();
  if (session.paused()) return "Paused — the stream is still reading.";
  switch (progress.connection.phase) {
    case "idle":
      return "Not started.";
    case "connecting":
      return "Connecting…";
    case "open":
      return progress.phase === undefined ? "Reading…" : `${capitalise(progress.phase)}…`;
    case "closed":
      if (progress.endReason === "cancelled") return "Stopped.";
      if (progress.endReason === "budget") {
        return session.canNextPage()
          ? "Scan paused at its safety budget — continue from the next offsets."
          : "Stopped at the scan safety budget.";
      }
      if (progress.delivered > 0) return "Finished.";
      return (progress.consumed?.filterErrors ?? 0) > 0
        ? "Finished — some records could not be evaluated."
        : "Finished — nothing matched.";
  }
}

/**
 * The five things "no records" can mean, drawn as five different screens.
 *
 * They are genuinely different facts and this project has drawn them all as the same blank space:
 * a browse nobody has started yet, one that is running and has not delivered, one that finished
 * with nothing, one whose substring excluded everything, and one whose *expression* did. Only the
 * last three are about the topic, and the last two are about different things the operator typed.
 *
 * The expression case is separate because its remedy is different. A substring that matched nothing
 * is usually the wrong substring; an expression that matched nothing may equally be an expression
 * that **threw on every record**, which the server counts separately and which looks identical from
 * here. So that panel points at the preview rather than telling the operator to clear the filter,
 * because trying it against one record is the thing that tells the two apart.
 */
function BrowseEmpty(props: {
  readonly running: boolean;
  readonly everRan: boolean;
  readonly filtered: boolean;
  /** The smart-filter expression that was running, when one was. */
  readonly byExpression?: string | undefined;
  readonly filterErrors: number;
  readonly endReason?: BrowseEndReason | undefined;
  readonly canContinue: boolean;
  readonly onRead: () => void;
}): JSX.Element {
  return (
    <Show
      when={props.everRan}
      fallback={
        <EmptyState
          kind="empty"
          title="Nothing has been read yet."
          description="Opening a topic does not start reading it — a browse is a real Kafka consumer. Choose where to start and press Read."
          action={
            <Button variant="primary" icon="refresh" onClick={props.onRead}>
              Read
            </Button>
          }
        />
      }
    >
      <Show
        when={!props.running}
        fallback={
          <p class="kui-browse__reading">
            <Spinner size="16px" />
            {/* Not an EmptyState: this is not an empty screen, it is a screen that is about to
                have records on it, and drawing the same panel for both makes a browse that is
                working look like one that found nothing. */}
            Reading… records appear as they arrive.
          </p>
        }
      >
        <Show
          when={props.endReason !== "budget"}
          fallback={
            <Show
              when={props.filtered}
              fallback={
                <EmptyState
                  kind="empty"
                  title="No records found before the scan paused."
                  description={
                    "The server reached its scan safety budget before finding a record in the selected range. " +
                    continuationSentence(props.canContinue)
                  }
                />
              }
            >
              <Show
                when={props.byExpression}
                fallback={
                  <EmptyState
                    kind="filtered"
                    title="No record matched that filter yet."
                    description={
                      "The server reached its scan safety budget before finding a match. " +
                      continuationSentence(props.canContinue)
                    }
                  />
                }
              >
                {(expression) => (
                  <EmptyState
                    kind="filtered"
                    title="No record matched that expression yet."
                    description={
                      `The server reached its scan safety budget before finding a match for ${expression()}. ` +
                      continuationSentence(props.canContinue)
                    }
                  />
                )}
              </Show>
            </Show>
          }
        >
          <Show
            when={props.filtered}
            fallback={
              <EmptyState
                kind="empty"
                title="No records in that range."
                description="The partitions you chose hold nothing between where you started and where the read stopped."
              />
            }
          >
            <Show
              when={props.byExpression}
              fallback={
                <EmptyState
                  kind="filtered"
                  title="No record matched that filter."
                  description="Every record in the range was read; none contained that text. Clearing the filter shows them."
                />
              }
            >
              {(expression) => (
                <EmptyState
                  kind="filtered"
                  title="No record matched that expression."
                  description={
                    props.filterErrors > 0
                      ? `Some records could not be evaluated (${String(props.filterErrors)} failures), and no ` +
                        `record that evaluated successfully satisfied ${expression()}. Try the expression ` +
                        "against one record to see the server's exact error."
                      : `Every record in the range was read and none of them satisfied ${expression()}. ` +
                        "Try it against a single record before assuming the topic has nothing in it."
                  }
                />
              )}
            </Show>
          </Show>
        </Show>
      </Show>
    </Show>
  );
}

function continuationSentence(canContinue: boolean): string {
  return canContinue
    ? "Continue to scan later offsets."
    : "The scan cannot be continued from this response.";
}

function describe(failure: NonNullable<ReturnType<BrowseSession["progress"]>["failure"]>): string {
  switch (failure.kind) {
    case "server":
      return userMessage(failure.error);
    case "transport":
      return `The stream stopped: ${failure.cause}. The records above are what arrived before it did.`;
    case "decode":
      // Informational: the stream is still running, and saying so is the difference between "one
      // record was odd" and "the browse died".
      return `One ${failure.event} event could not be read (${failure.cause}). The browse is still running.`;
  }
}

/** What the pause control says. Exported-shaped as a function so the zero case is testable. */
export function pauseLabel(paused: boolean, held: number): string {
  if (!paused) return "Pause";
  return held === 0 ? "Resume" : `Resume (${held.toLocaleString()})`;
}

function plural(count: number, noun: string): string {
  return `${count.toLocaleString()} ${noun}${count === 1 ? "" : "s"}`;
}

function capitalise(text: string): string {
  return text.length === 0 ? text : text[0]!.toUpperCase() + text.slice(1);
}
