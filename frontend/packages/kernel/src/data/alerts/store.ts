/**
 * The alert feed the bell and the card both read (M8, `SCREENS-V4.md` §3.8 and §3.9).
 *
 * ## Why there is one store and why it is here
 *
 * The bell is in the shell's chrome and the card is in `@kui/feature-alerts`. A feature may not
 * import the shell, nothing may import a feature statically, and the two must never be able to
 * disagree about how many alerts are open — a bell reading `2` beside a card reading `3` is worse
 * than either number alone, because the operator then has to decide which of KUI's own screens to
 * believe. So the feed is fetched once, held once, and read twice, and the kernel is the only
 * package both consumers can see. It is the capability store's argument exactly.
 *
 * ## How the two halves fit together, which is the service's design and not this file's
 *
 * `AlertChangeDto`'s scaladoc in
 * `services/alerts/contract/src/kui/alerts/contract/dto/AlertDtos.scala` settles it: the stream
 * frame carries **the open count and no events**, so that one principal's unread count never
 * reaches another subscriber's socket and a dropped frame costs a fetch rather than a stale screen.
 * So this store takes the count off the frame *first* — it is the number the bell and the danger
 * pill both draw, and it is the same for every principal — and then re-reads for the rows.
 *
 * Every citation into that file names a **symbol**, never a line. The four line numbers this file
 * and `./events.ts` used to carry were each eleven lines above the type they named, because the
 * Scala moved and nothing in this repository compares a comment to a line number. A symbol survives
 * an edit above it, and the thing that actually holds the two sides together is
 * `./wire.golden.test.ts`, which decodes the service's own committed documents through
 * `./events.ts` and fails when a field is renamed.
 *
 * ## Why every frame costs a read, and what the count on the frame is therefore for
 *
 * The DTO's scaladoc argues that "a subscriber that sees a number it does not hold re-reads the
 * feed". This store deliberately does **not** implement that comparison, and the sentence is not a
 * description of the code: {@link Alerts} re-reads on every frame it accepts. The reason is that
 * the count is not a summary of the page. One event can close while another opens in the same
 * evaluation pass — the net `openCount` is unchanged and both the rows and this principal's unread
 * state have moved — so a store that skipped the read on an equal count would leave the panel
 * showing a resolved event and the bell showing the wrong unread mark. `store.test.ts`'s
 * *"re-reads after a same-count change because the rows may have changed"* is that case.
 *
 * What the count on the frame buys is not a saved fetch, it is a **saved round trip**: the bell and
 * the danger pill move at the instant the frame lands rather than when the read it triggered comes
 * back. That is the whole reason the service puts it there, and `openCount()` reads it — see the
 * note on `streamed` below.
 *
 * The cost is one feed read per accepted frame per open tab, and it is bounded by how often the
 * service publishes rather than by how much is happening: `AlertsRoutes.changes` emits when
 * `AlertStore.changes` says this cluster's feed moved, and a frame naming another cluster is
 * dropped here before any read. If that ever becomes the wrong trade the fix is a smaller feed
 * page, not a comparison that drops real changes on the floor.
 *
 * ## The rules, each of which is a defect this product has already paid for
 *
 * - **Every count is the server's own figure.** `openCount` and `unreadCount` are counted over the
 *   whole store and `items` is one page, so a fold over the rows this browser holds is a different
 *   number by construction. There is no such fold in this file. A document that carries no count
 *   answers `null` — "KUI does not know" — and never `0`.
 * - **A document this build cannot read is a refusal, not an empty feed.** Wave 5's producers wire
 *   decoded successfully into nothing and the card announced it. {@link Alerts.feed} goes to
 *   `failed` with the cause in it instead.
 * - **Nothing about a severity, a tone or a glyph is decided here.** The server sends both halves of
 *   each pair for that reason; see `./events.ts`.
 * - **The bell goes quiet because the server said so.** `markAllRead()` re-reads with the
 *   endpoint's own `markRead` query — `AlertsEndpoints.MarkReadParam` — and takes the post-write
 *   `unreadCount` and `lastReadAt` that come back. Zeroing a local number and hoping is how a bell
 *   ends up disagreeing with the panel it opens.
 * - **An answer that arrives after the store moved on is dropped.** Reads cannot be recalled, and
 *   applying a stale one repaints a card the shell has torn down or overwrites a newer feed with an
 *   older one. Every read carries the episode that asked for it.
 * - **The stream is closed before another is opened, and `stop()` really stops.** The same rule, and
 *   the same shipped defect, as `../capabilities/store.ts`: an abandoned `EventSource` is
 *   unreachable through the store and retries for the life of the tab.
 * - **A completed read supersedes the frame's hint, on every outcome.** The hint and the read are
 *   the same number from the same source, one of them is older, and leaving both alive is how two
 *   screens come to draw two figures. `applyRead()` drops it first thing; `store.test.ts`'s *"a
 *   completed read supersedes the count the frame carried"* and *"a read that fails at the transport
 *   keeps the rows and drops the count the frame carried"* are the two cases that fail when it does
 *   not — one per side of the failure branch, because either one alone leaves the line movable.
 * - **A read that never reached the gateway is not a refusal.** It is the stream dropping, one
 *   transport down: the rows stay, badged `stale`, with the reason beside them. A server that
 *   *answered* — an error envelope — and an answer this build could not decode are refusals, and
 *   they blank the feed.
 *
 * ## Which of these accessors the product reads, measured rather than assumed
 *
 * Re-measured at wave 8 over the shell's and every feature package's `src`, excluding test, story
 * and fixture files, by running this from `frontend/packages`:
 *
 * ```
 * # the eight this file says have callers
 * grep -rnE "\.(feed|events|openCount|unread|markAllRead|refresh|start|stop)\(\)" \
 *   shell/src feature-*\/src | grep -v "\.test\.\|\.stories\."
 * # and the three it says have none
 * grep -rnE "\.(unreadCount|lastReadAt|connection)\(\)" \
 *   shell/src feature-*\/src | grep -v "\.test\.\|\.stories\."
 * ```
 *
 * `feed`, `events`, `openCount`, `unread`, `markAllRead`, `refresh`, `start` and `stop` have
 * production callers. {@link Alerts.unreadCount}, {@link Alerts.lastReadAt} and
 * {@link Alerts.connection} still have none. **Read the second grep's hits rather than counting
 * them**: `connection()` is a member of `SseHandle` as well, and `App.tsx`'s gateway stream and
 * `feature-messages/src/transport.ts` both call *that* one — which is why this census is done by
 * eye and why a mechanical version of it would report the opposite of the truth.
 *
 * The commands are written out because the sentence is prose: **nothing in this repository compares
 * it to the tree**, so it is true on the day it is written and nobody is told the day it stops
 * being. Re-run them rather than trusting this paragraph.
 *
 * Wave 8's plan records this paragraph as a false hand-off — that `unreadCount()` gained a caller in
 * `App.tsx`'s drawer badge during wave 7. The grep above says otherwise, in the direction nobody
 * expected: the badge and the bell read `openCount()` and `unread()` (`App.tsx`'s `alertsOpen` and
 * `alertsUnread` props), and `unread()` is a different member answering a boolean over the same
 * count. It is one letter's difference in a report and it is exactly why the command is here.
 *
 * `openCount()` is **the** open count, and that was settled in wave 7 rather than assumed: the
 * shell held a second derivation (`shell/src/data/alerts.ts`'s `openCountOf`) which read the same
 * feed under its own `evaluatedAt` rule and had never heard of `streamed`, so the two agreed until
 * a frame arrived and disagreed from then on. It is gone. The drawer's badge, the bell and the
 * dashboard's alerts card all read this accessor, which is what §3.8 means by one number in three
 * places that cannot disagree.
 *
 * ## Wave 9 re-ran both greps and the answer did not move — including the part that was promised
 *
 * Re-measured on 2026-09-11, over the same scope, and **all three still have zero production
 * callers.** The second grep returns exactly two hits and neither is this interface:
 * `App.tsx:1346`'s gateway stream and `feature-messages/src/transport.ts:88` both call
 * `SseHandle.connection()`, which is the trap the paragraph above warns about.
 *
 * Two consecutive waves' plans have now written down that `unreadCount()` gained a caller in the
 * drawer's Alerts badge, and the tree has disagreed both times. The badge is
 * `alertsBadge(alerts.openCount())` (`App.tsx:805`) — the cluster-wide **open** count, not the
 * per-principal unread one — and the bell takes `alerts.openCount()` and `alerts.unread()`
 * (`App.tsx:944-945`), where `unread()` is a *boolean over* the count rather than the count.
 * `TopBar` does declare an `unreadCount` prop, which is a third thing wearing the same word, and
 * **the shell passes it nothing**: it defaults to `0` and that zero is what `Notifications` is
 * handed. The plan is not wrong about where the number should come from; it is wrong about whether
 * the wiring happened, and it has been wrong about it twice — which is what a sentence nothing in
 * the repository compares to the tree does over time.
 *
 * **The decision, taken here and recorded rather than assumed: all three stay, for three different
 * reasons, and only one of them is weak.**
 *
 * - **`unreadCount()` stays.** It is the per-principal figure the server sends and the only
 *   accessor that carries it; `unread()` answers a different question — *is there anything* — and
 *   cannot be widened back into a count. Deleting the count in a wave whose own plan is trying to
 *   draw it would be the two halves of one seam disagreeing inside one wave, and the badge is one
 *   prop away from reading it.
 * - **`connection()` stays.** It is the only observable this store has of the stream's lifecycle
 *   and is what the case pinning *a released stream moves nothing* reads — deleting it would take
 *   that assertion with it, which is a gate lost to tidiness.
 * - **`lastReadAt()` stays, and it is the weakest of the three by a long way.** Nothing reads it,
 *   nothing is about to, and the shell already gets the same value off the decoded feed
 *   (`shell/src/data/alerts.ts:209`, which compares it to each event's `openedAt`) rather than
 *   through this store. It survives on ownership alone: {@link Alerts} is implemented by two
 *   doubles outside this package — `shell/src/overview/harness.tsx:157`'s `staticAlerts` and
 *   `feature-alerts/src/fixtures.tsx:62`'s `storeFor` — so removing the member is a three-file edit
 *   across three packages, and the packet that owns this file owns neither of the other two. It is
 *   filed as debt with that edit named rather than left as an implication. **This is the one to
 *   delete the moment a packet owns the kernel and the shell together**, and it is now the only
 *   member of this interface with no argument for its existence beyond who may touch which file.
 *
 * ## Why there is no poller behind the stream
 *
 * The capability store has one because the navigation is drawn from it and a frozen navigation is
 * unusable. The feed is not that: when its stream drops, what is held is real and simply not
 * current, which is what `stale` says. A second fetch path would be a second decoder against the
 * same wire, which is the pair of half-contracts this file exists to avoid.
 *
 * The stream itself is opened again, though, and that is a different thing. The browser's
 * `EventSource` retries a dropped connection on its own, and it stops for good on a non-200 answer,
 * which is what a proxy sends while the gateway restarts. The heartbeat watchdog also ends a
 * stream that has been silent too long. Without a reopen, any of these left the bell on the
 * pre-restart count, badged `stale`, for the rest of the session. So a stream that closes without
 * this store asking is reopened on `backoff()`'s schedule, and the first `open` after a reopen
 * re-reads the feed, because whatever changed during the outage never arrived as a frame.
 */
import type { ApiResult } from "@kui/api";
import { decodeSection, isTransportFailure, userMessage } from "@kui/api";
import { createEffect, createRoot, createSignal, type Accessor } from "solid-js";

import { apiFailure, type Fetched } from "../fetched.js";
import { backoff, type SseConnection, type SseHandle, type SseSubscriber } from "../sse/stream.js";
import {
  decodeAlertChange,
  decodeAlertFeed,
  type AlertChange,
  type AlertEvent,
  type AlertFeed,
} from "./events.js";

/**
 * The event name the change stream publishes under.
 *
 * `AlertChangeDto.EventName` on the server. A literal rather than a member of `@kui/api`'s
 * `SseEventNames`, because that file is generated from the Scala vocabulary and compared byte for
 * byte by `./mill frontend.apiConstants --check`, so it will carry this name when the generator is
 * next run and not before. It is an option as well, so the shell can override it without this file
 * moving.
 */
export const ALERTS_EVENT_NAME = "alerts";

/**
 * The key the feed's section hangs off in the read's body.
 *
 * `AlertFeedResponse` is `events: Section[AlertFeedDto]`, and M8's exit criterion in
 * `docs/plan/ROADMAP.md` greps `.events.status` and `.events.data.items` for it. Cited by document
 * and not by line: the line range this used to name moved during wave 7 and the sentence stayed.
 */
export const ALERTS_SECTION_KEY = "events";

/**
 * An optional, best-effort warm cache for the feed.
 *
 * The shell owns the browser technology and the privacy boundary; the kernel only accepts bytes,
 * validates them through the same decoder as the network, and always lets the backend replace
 * them. Cache failures never turn an otherwise usable feed into a failure.
 */
export interface AlertFeedCache {
  readonly read: () => Promise<unknown | undefined>;
  readonly write: (feed: AlertFeed) => Promise<void>;
  /** Removes the current scope after the backend refuses or cannot validate it. */
  readonly remove: () => Promise<void>;
}

export interface AlertsOptions {
  /** Subscribes to the change stream. A function, because a new one is opened after a close. */
  readonly openStream: (subscriber: SseSubscriber<AlertChange>) => SseHandle;
  /**
   * Runs an action after a delay: how a stream that closed on its own is reopened later. Defaults
   * to `setTimeout`; a test passes a clock it drives by hand.
   */
  readonly schedule?: ((delayMs: number, action: () => void) => void) | undefined;
  /**
   * `GET …/alerts/events`, with the endpoint's `markRead` query.
   *
   * A parameter rather than two functions so that "read the feed" and "read the feed and mark it
   * read" cannot end up spelling the address differently — it is one endpoint and one caller.
   */
  readonly load: (markRead: boolean) => Promise<ApiResult<unknown>>;
  /**
   * Which cluster this store is for, when the caller knows.
   *
   * The stream is opened per cluster, so every frame on it should be this one's — but the frame
   * names its cluster, and a frame for another one moving this bell's count would be a number from
   * somewhere the operator is not looking. Checked when it is offered.
   */
  readonly cluster?: (() => string | undefined) | undefined;
  readonly eventName?: string | undefined;
  readonly sectionKey?: string | undefined;
  /** A cluster-and-principal-scoped browser cache used only while the first read is in flight. */
  readonly cache?: AlertFeedCache | undefined;
  /** Where an unreadable frame is reported. Defaults to the console. */
  readonly warn?: ((message: string) => void) | undefined;
}

export interface Alerts {
  /** The feed and an honest account of its state, for whatever is drawing it. */
  readonly feed: Accessor<Fetched<AlertFeed>>;
  /** The rows of the page the server sent, in its own order. Empty while loading and after a refusal. */
  readonly events: Accessor<readonly AlertEvent[]>;
  /** The server's cluster-wide open count, or `null` when nothing has said. Never a fold over rows. */
  readonly openCount: Accessor<number | null>;
  /** The server's per-principal unread count, or `null`. Never a fold over rows. */
  readonly unreadCount: Accessor<number | null>;
  /** Whether this principal has anything unread. `null` — nobody has said — is not "unread". */
  readonly unread: Accessor<boolean>;
  /** How far this principal has read, as the server recorded it. */
  readonly lastReadAt: Accessor<string | undefined>;
  /** What the stream is doing, for the connection indicator. */
  readonly connection: Accessor<SseConnection>;
  /** Re-reads the feed and asks the server to mark it read for this principal. */
  markAllRead(): void;
  /** Re-reads the feed without marking anything. */
  refresh(): void;
  /** Reads the feed and opens the stream. Called once, by the shell. */
  start(): void;
  /** Closes the stream. Idempotent. */
  stop(): void;
}

export function createAlerts(options: AlertsOptions): Alerts {
  const eventName = options.eventName ?? ALERTS_EVENT_NAME;
  const sectionKey = options.sectionKey ?? ALERTS_SECTION_KEY;
  const warn =
    options.warn ??
    ((message: string) => {
      console.warn(message);
    });
  const schedule =
    options.schedule ??
    ((delayMs: number, action: () => void) => {
      setTimeout(action, delayMs);
    });

  // Written from network callbacks, which can be reached while an owned scope is current — the
  // same one-writer-many-readers shape the capability store has.
  const [feed, setFeed] = createSignal<Fetched<AlertFeed>>({ kind: "loading" }, { ownedWrite: true });
  const [connection, setConnection] = createSignal<SseConnection>(
    { phase: "connecting" },
    { ownedWrite: true },
  );
  /**
   * The count the newest frame carried, held beside the feed rather than folded into it.
   *
   * The frame arrives before the re-read it triggers, and the whole reason the service puts the
   * count on the frame is so that the bell and the pill move together at that moment rather than a
   * round trip later. It is dropped the instant a read lands, because the read's count is the same
   * number from the same source and one of them has to win.
   *
   * Three places clear it and one deliberately does not. `applyRead()` clears it on every outcome
   * and `stop()` clears it as part of ending this life of the store. `markStale()` leaves it —
   * reached from the subscriber, that is a terminal stream failure with no read behind it, and
   * reached from `applyRead()` the clearing has already happened one line above the branch. That is
   * correct in both directions: the count it holds is still the last thing the server said about
   * this cluster, it is exactly as old as the rows being shown beside it, and the feed is badged
   * `stale` while both are on screen. There is no state in which this hint outlives a refusal,
   * because {@link knownOpenCount} answers `null` for every feed state that holds no document.
   *
   * *"On every outcome"* is a claim about one line, and until wave 8 only one of the outcomes could
   * see it: every failing read blanked the feed to `failed`, `knownOpenCount()` answers `null` for
   * that state whatever this signal holds, and a suite therefore stayed green with the clearing
   * moved below the failure branch. It is observable now because a read that fails at the transport
   * over a feed that is still held keeps the rows and badges them `stale` — so the hint and a
   * document are on screen together, and a hint from a frame whose read never landed would win.
   */
  const [streamed, setStreamed] = createSignal<number | null>(null, { ownedWrite: true });

  let handle: SseHandle | undefined;
  /** Disposes the computation following the current handle's connection. */
  let disposeWatch: (() => void) | undefined;
  let stopped = false;
  /**
   * The reopen waiting on the clock, if any, and how many have been tried since a stream was last
   * `open`. A scheduled reopen cannot be recalled, so it runs only while it is still the one held
   * here: `start()` and `stop()` both drop it, which is what stops a reopen from an earlier life
   * opening a second stream beside the current one, or resurrecting a stream the shell released.
   */
  let pendingReopen: object | undefined;
  let reopens = 0;

  /**
   * Which life of this store the read in flight belongs to.
   *
   * A read cannot be recalled once it is out, and its answer arrives whenever it arrives — after a
   * `stop()`, after a second `start()`, or after a newer read has already landed. Applying it then
   * paints a feed nobody is waiting for over the one that is current, and on a stopped store it
   * repopulates a card the shell has torn down. Raised by every read, which is also how `start()`
   * raises it, and the answer is dropped when it no longer matches. The capability store's episode,
   * one file over, is the same rule for the same reason.
   *
   * `stop()` used to raise it as well, and that line is deleted rather than left. It could not fire:
   * `stopped` refuses every read across a teardown on its own, and the only thing that lowers
   * `stopped` is `start()`, which raises the episode on the very next line by reading — so no
   * ordering exists in which this raise is the refuser. Measured rather than argued: deleting it
   * left `pnpm -C frontend test packages/kernel` at 23 files / 455 cases green, which is what a line
   * nothing can reach looks like from the outside. It is gone for the reason `connect()`'s
   * unreachable guard went in wave 7 — a guard that cannot fire reads as a considered defence of a
   * state that does not exist — and its going makes *"a read answered after the store was stopped is
   * not applied"* into the case that actually holds `stopped`, which it did not while both were
   * there.
   */
  let episode = 0;

  const subscriber: SseSubscriber<AlertChange> = {
    events: [eventName],
    decode: (_event, data) => decodeAlertChange(data),
    onEvent: (change) => {
      const mine = options.cluster?.();
      if (mine !== undefined && change.cluster !== mine) {
        // Not this store's cluster. Taking the count would put a number on this bell from a
        // cluster the operator is not looking at.
        return;
      }
      // The count first — see the note on `streamed` — and the rows after.
      setStreamed(change.openCount);
      read(false);
    },
    onError: (error) => {
      if (error.kind === "decode") {
        // A frame we cannot read is reported and skipped; what is on screen is still what the
        // server last said, which is more use than a blank card.
        warn(`kui: ignoring an unreadable alert frame: ${JSON.stringify(error)}`);
        return;
      }
      // `server` and `transport` are terminal. What is held is now as fresh as the last read and
      // no fresher, which is what `stale` means — it is not a claim that anything is broken.
      markStale(error.kind === "server" ? userMessage(error.error) : error.cause);
    },
  };

  /**
   * Detaches the current stream: forget it, stop following it, then close it.
   *
   * The close is itself an event, which is what all three lines are about. `SseHandle.close()`
   * moves the handle's own connection to `closed / "closed by the client"`, and a watcher still
   * following it would write that into {@link Alerts.connection} — a deliberate teardown read back
   * by the frame's connectivity banner as an outage the operator is expected to do something about.
   *
   * `handle = undefined` and `disposeWatch?.()` are **two independent guards on the same write**,
   * and wave 7 measured which of them carries it by deleting each. Neither alone: the watcher
   * checks `handle === opened` before writing, so clearing the field is sufficient on its own, and
   * disposing the computation is sufficient on its own. `store.test.ts`'s *"says the teardown was
   * its own, and stops following the stream it released"* goes red when **both** are removed and
   * stays green for either one, which is what redundant depth looks like when it is measured
   * instead of assumed. Reordering `open?.close()` above the other two reddens nothing for the same
   * reason. That is disclosed here rather than defended: the property the case pins is *a released
   * stream moves nothing*, and the two lines are two ways of holding it, not two rules.
   */
  function releaseHandle(): void {
    const open = handle;
    handle = undefined;
    disposeWatch?.();
    disposeWatch = undefined;
    open?.close();
  }

  /**
   * Opens a stream and follows *its* connection rather than keeping a second opinion here.
   *
   * The obvious alternative is to set the phase from the events this store already sees —
   * `connecting` when the stream is opened, `open` when a frame arrives — and it is wrong twice.
   * A healthy stream with nothing happening on it then reads as "connecting" for as long as the
   * cluster stays quiet, which on a well-run cluster is for ever; and `reconnecting`, the one phase
   * an operator actually needs to see, cannot be reported at all, because the retry happens inside
   * the transport and produces no frame. The transport already knows all three. This says what it
   * knows.
   *
   * A detached root, because the store has no component around it and Solid needs an owner for a
   * computation to be disposable — the capability store, one directory over, carries the same note
   * for the same reason.
   *
   * There is no `if (stopped) return;` here. Its two callers are `start()`, which assigns
   * `stopped = false` on the line above the call, and {@link reopenLater}, whose action runs only
   * while its reopen is still the pending one, which `stop()` clears. `stop()` does not route
   * through here — it calls {@link releaseHandle} directly — and the case *"a second stop changes
   * nothing, and a stopped store opens nothing"* holds it to that.
   *
   * A `closed` read from the handle this store still holds is one it did not ask for: a release
   * clears `handle` before it closes, so the client's own close never reaches the branch.
   */
  function connect(): void {
    releaseHandle();
    const opened = options.openStream(subscriber);
    handle = opened;
    disposeWatch = createRoot((dispose) => {
      createEffect(
        () => opened.connection(),
        (current) => {
          if (handle !== opened) return;
          setConnection(current);
          if (current.phase === "closed") {
            reopenLater();
          } else if (current.phase === "open" && reopens > 0) {
            reopens = 0;
            read(false);
          }
        },
      );
      return dispose;
    });
  }

  /** Opens the stream again after {@link backoff}'s wait, unless the store moved on meanwhile. */
  function reopenLater(): void {
    const token = {};
    pendingReopen = token;
    reopens += 1;
    schedule(backoff(reopens), () => {
      if (pendingReopen !== token) return;
      pendingReopen = undefined;
      connect();
    });
  }

  /** What is held right now, whether it arrived fresh or is being shown under a staleness badge. */
  function current(): AlertFeed | undefined {
    const state = feed();
    return state.kind === "ready" || state.kind === "stale" ? state.value : undefined;
  }

  /**
   * The one open-count derivation used by every consumer of this store.
   *
   * Two guards, and each answers a different question.
   *
   * `evaluatedAt === undefined` is the honest-refusal rule: a feed the rules have never run against
   * carries `openCount: 0`, and drawing that is the most reassuring possible rendering of the one
   * thing nobody measured. It also covers every state that holds no feed at all — `forbidden`,
   * `not-configured`, `failed`, `loading` — because `current()` answers `undefined` for all four,
   * which is why a live hint can never outlive a refusal even for the instant before the read that
   * caused the refusal lands.
   *
   * `streamed() ?? held.openCount` is the round trip the frame saves: between a frame arriving and
   * the read it triggered coming back, the frame's count is the newer of the two.
   */
  function knownOpenCount(): number | null {
    const held = current();
    if (held?.evaluatedAt === undefined) return null;
    return streamed() ?? held.openCount;
  }

  function markStale(reason: string): void {
    const held = current();
    if (held === undefined) {
      setFeed({ kind: "failed", message: reason, code: "STREAM_CLOSED" });
      return;
    }
    setFeed({ kind: "stale", value: held, reason });
  }

  /**
   * Reads the feed, and drops the answer if the store has moved on by the time it arrives.
   *
   * Both halves of the guard are reachable and neither implies the other.
   *
   * `asked !== episode` is the ordinary race: two reads in flight, the older one answering last.
   * `stop()` raises the episode too, so it also covers a read issued before a teardown.
   *
   * `stopped` covers the read issued *after* one — `stop()` followed by `refresh()`, which is a
   * Retry click landing in the same tick as the frame unmounting, and is reachable from this
   * store's own public surface with nothing else involved. Without it that read repopulates a feed
   * and a bell the shell has already torn down, on a session the operator may have signed out of.
   * `store.test.ts`'s *"a refresh issued after stop() is not applied"* is that case, and it fails
   * when this clause is weakened rather than when the whole guard is deleted.
   */
  function read(markRead: boolean, warmFromCache = false): void {
    episode += 1;
    const asked = episode;
    let serverSettled = false;

    if (warmFromCache && options.cache !== undefined) {
      void options.cache
        .read()
        .then((cached) => {
          if (
            cached === undefined ||
            serverSettled ||
            asked !== episode ||
            stopped ||
            feed().kind !== "loading"
          )
            return;
          const decoded = decodeAlertFeed(cached);
          if (!decoded.ok) return;
          setFeed({
            kind: "stale",
            value: decoded.value,
            reason: "Showing saved alerts while KUI refreshes.",
          });
        })
        .catch(() => undefined);
    }

    void options.load(markRead).then((answer) => {
      serverSettled = true;
      if (asked !== episode || stopped) return;
      applyRead(answer);
    });
  }

  function cache(feed: AlertFeed): void {
    void options.cache?.write(feed).catch(() => undefined);
  }

  function discardCache(): void {
    void options.cache?.remove().catch(() => undefined);
  }

  function applyRead(result: ApiResult<unknown>): void {
    // A completed read supersedes the stream hint on every outcome — including the ordinary one,
    // where the read simply carries a newer count than the frame that triggered it. The two are the
    // same number from the same source and one of them is older; leaving both alive is how two
    // screens come to draw two figures, and on a failed read it is how a bell shows a live count
    // beside a feed badged as the last answer KUI received. Deleting this line reddens *"a completed
    // read supersedes the count the frame carried"*; moving it under the failure branch reddens
    // *"a read that fails at the transport keeps the rows and drops the count the frame carried"*.
    // Both are in `store.test.ts`, and the second exists because until wave 8 the first outcome was
    // the only one any case could see — see the note on the branch below.
    setStreamed(null);
    if (!result.ok) {
      // Nothing answered, so nothing was disproved. A read that never reached the gateway is the
      // same event as the stream dropping — what is held is real and simply not current, which is
      // what `stale` says and what {@link markStale} already does for the stream. Blanking a feed
      // the operator is reading because one poll timed out takes the last known figures away at
      // exactly the moment they are wanted, which is the defect `Fetched.stale` exists to prevent.
      //
      // `envelope` and `decoding` are deliberately not in here and it is not a detail. An envelope
      // is the server having an opinion — it may be the refusal that revokes this principal's sight
      // of the cluster — and a `decoding` failure means this build and the gateway disagree about
      // the contract, which must be loud. Neither may leave rows on screen under a staleness badge.
      if (isTransportFailure(result.error) && current() !== undefined) {
        markStale(userMessage(result.error));
        return;
      }
      discardCache();
      setFeed(apiFailure(result.error));
      return;
    }
    const body = result.value;
    const envelope =
      typeof body === "object" && body !== null ? (body as Record<string, unknown>) : undefined;
    const section = decodeSection<unknown>(envelope?.[sectionKey]);
    switch (section.status) {
      case "forbidden":
        discardCache();
        setFeed({ kind: "forbidden" });
        return;
      case "not_configured":
        discardCache();
        setFeed({ kind: "not-configured" });
        return;
      case "unavailable":
      case "unreadable":
        discardCache();
        setFeed({
          kind: "failed",
          message: section.reason.message ?? "The alerts service did not answer.",
          code: section.reason.code,
        });
        return;
      case "ok":
      case "stale": {
        const decoded = decodeAlertFeed(section.data);
        if (!decoded.ok) {
          // The refusal, spelled out. This is the sentence that would have caught wave 5's
          // producers wire on the day it shipped rather than two waves later.
          discardCache();
          setFeed({
            kind: "failed",
            message: `KUI could not read the alert feed: ${decoded.cause}`,
            code: "UNREADABLE_FEED",
          });
          return;
        }
        if (section.status === "stale") {
          setFeed({
            kind: "stale",
            value: decoded.value,
            reason: section.reason.message ?? "This is the last answer KUI received.",
          });
          cache(decoded.value);
          return;
        }
        setFeed({ kind: "ready", value: decoded.value });
        cache(decoded.value);
        return;
      }
    }
  }

  return {
    feed,
    events: () => current()?.items ?? [],
    connection,
    lastReadAt: () => current()?.lastReadAt,

    openCount: knownOpenCount,

    unreadCount: () => current()?.unreadCount ?? null,

    // `null` is not "unread". Nobody has said how many are unread, and lighting the bell on that
    // would be a mark meaning "KUI does not know", which is not what a reader takes it for.
    unread: () => (current()?.unreadCount ?? 0) > 0,

    markAllRead(): void {
      // A pre-write snapshot may still carry the unread count this request is about to retire.
      // Remove it before asking so an immediate reload cannot briefly relight the bell from an old
      // browser record. A successful answer writes the server's post-read feed back afterwards.
      discardCache();
      read(true);
    },

    refresh(): void {
      read(false);
    },

    start(): void {
      stopped = false;
      pendingReopen = undefined;
      reopens = 0;
      setStreamed(null);
      setFeed({ kind: "loading" });
      read(false, true);
      connect();
    },

    stop(): void {
      stopped = true;
      pendingReopen = undefined;
      reopens = 0;
      setStreamed(null);
      releaseHandle();
      setConnection({ phase: "closed", reason: "closed by the client" });
    },
  };
}
