/**
 * Incremental lag: refreshing the list's most volatile column without describing every group again.
 *
 * ## Why this exists
 *
 * `fetchGroups` is the expensive call on this screen. Describing a consumer group means asking its
 * coordinator, so a cluster with two hundred groups spread over six brokers costs a round trip per
 * coordinator every time the list refreshes — and the only figures that actually move between one
 * refresh and the next are lag, state and member count. `GET …/consumer-groups/lag` answers exactly
 * those three, for only the groups whose numbers changed since a token the server itself issued.
 *
 * ## The token
 *
 * Opaque, and treated as opaque here. Against the quickstart it decodes to `quickstart:7` — the
 * cluster and the snapshot version it was cut from — but nothing below parses it, compares two of
 * them, or infers anything from one. The endpoint's own description says why the server issues it
 * rather than accepting a timestamp: a browser clock is not a version, and sending one back drops
 * or replays updates whenever the two clocks disagree.
 *
 * ## The four answers this endpoint gives, three of which look alike and are not
 *
 * Recorded from a running gateway, all four in `src/recorded/`:
 *
 * - `lag-full.json` — the answer to a request with no token at all. `full: true`, every group, and
 *   `"pace": null` on each because the server has one observation and no interval to divide by.
 * - `lag-delta.json` — `changed` names one group, `full: false`. **Two groups are not mentioned.**
 *   That is the server saying their lag is unchanged as of this token. It is not saying it is zero
 *   and it is not saying it is unknown, and `applyLagDelta` below leaves those rows alone.
 * - `lag-quiet.json` — `changed: []`, `gone: []`, `full: false`, and the same token back. Nothing on
 *   the cluster moved. A cluster with **no groups at all** answers `changed: []` with `full: true`,
 *   which is a different sentence; `full` is the field that separates them, and it is the reason
 *   this module never reads "an empty `changed`" as "an empty cluster".
 * - `lag-expired.json` — `full: true`. The token was not honoured (unrecognised, or cut from a
 *   snapshot the server has since discarded) and the answer restates every group. It is a lag
 *   answer, not a group list: it carries no topic count, no coordinator and no partial-read note, so
 *   it cannot rebuild a row. The only correct response is to fetch the whole list again, which is
 *   what `LagMerge`'s `needs-full-list` asks the caller to do.
 *
 * ## The drift this file is written against, rather than against the schema
 *
 * `docs/api/openapi.browser.json` marks `state` and `members` required on `LagUpdateDto` and says
 * nothing about nullability of `pace`; the running server sends `"pace": null` on the first answer
 * of a session — it has only one observation and will not divide by a made-up interval — and `0.0`
 * afterwards. Every field is therefore read defensively, and every figure goes through the kernel's
 * `figure`, which is `null` for anything that is not a number. Never `0`.
 *
 * ## Why every request names the groups it is about
 *
 * The endpoint answers cluster-wide when `group` is absent, and the list above it draws **one
 * page**. Asked without scoping, a cluster with more groups than a page holds answers about groups
 * that are not on screen — and `applyLagDelta` has no row for those, so it takes its
 * `needs-full-list` branch and the caller pays for a whole group list on *every* poll. No wrong
 * figure is produced; the entire saving is, silently, and the screen looks exactly as it does when
 * the protocol is working.
 *
 * So `fetchLagDelta` takes the group ids it wants and sends them as repeated `group` parameters,
 * which is what the parameter is for ("Only these groups; repeat the parameter"). Two consequences
 * worth stating, because they are what make the scoping safe rather than merely cheaper:
 *
 * - `gone` is computed by the server over the groups it was asked about, so a group that vanished
 *   from the page is still reported gone; it is not silently dropped by the filter.
 * - a `changed` entry for a group with no row is then a genuine surprise rather than the ordinary
 *   case, which is what lets `applyLagDelta` keep refusing it instead of guessing a row.
 */
import type { KuiApiClient } from "@kui/api";
import { apiFailure, figure, type Fetched } from "@kui/kernel";
import { FIRST_PAGE, fetchGroups, stateOf, type GroupQuery } from "./data.js";
import type { GroupSummary } from "./model.js";

/** One group's changed figures. Everything is nullable here because everything can be unreadable. */
export interface LagUpdate {
  readonly groupId: string;
  /** `null` when the server could not compute it. Never `0` by default — see the header. */
  readonly totalLag: number | null;
  readonly state: GroupSummary["state"];
  readonly members: number | null;
  /**
   * Records per second, when the server has two observations to derive it from.
   *
   * Carried through the mapping and deliberately not put on the list's rows: `GroupList` draws six
   * columns and PACE is not one of them, and its header explains why. The group detail endpoint
   * does not carry this figure, so the detail page cannot show it; it is on the delta so a client
   * can use the server's rate rather than one computed from two page loads.
   */
  readonly pace: number | null;
}

export interface LagDelta {
  readonly changed: readonly LagUpdate[];
  /** Groups that no longer exist. Removed from the list, not blanked. */
  readonly gone: readonly string[];
  /** Send this back as `since` next time. Opaque. */
  readonly token: string | null;
  /** How long the server would like us to wait. Advice, clamped below. */
  readonly nextPollMs: number;
  /** `true` when the answer restates everything because the token was not honoured. */
  readonly full: boolean;
}

interface LagUpdatePayload {
  readonly groupId: string;
  readonly totalLag?: number | null;
  readonly pace?: number | null;
  readonly state?: string | null;
  readonly members?: number | null;
}

interface LagDeltaPayload {
  readonly changed?: readonly LagUpdatePayload[];
  readonly gone?: readonly string[];
  readonly token?: string | null;
  readonly nextPollMs?: number | null;
  readonly full?: boolean;
}

/**
 * The floor and the fallback for the poll interval.
 *
 * The quickstart's server asks for 30 seconds. It is advice and it arrives over the network, so a
 * `0` — from a bug, a truncated body, or a future server that means something else by the field —
 * must not turn this into a request loop that describes groups as fast as the browser can ask.
 */
export const MIN_POLL_MS = 2_000;
export const DEFAULT_POLL_MS = 30_000;

function toUpdate(payload: LagUpdatePayload): LagUpdate {
  return {
    groupId: payload.groupId,
    // The most expensive `null` in this file. A group whose lag the server could not compute must
    // arrive here as unknown, so the row keeps drawing an em dash rather than claiming it caught up.
    totalLag: figure(payload.totalLag),
    state: stateOf(payload.state),
    members: figure(payload.members),
    pace: figure(payload.pace),
  };
}

export async function fetchLagDelta(
  api: KuiApiClient,
  clusterId: string,
  since?: string | undefined,
  /**
   * The groups this answer should be about — the page on screen.
   *
   * Omitted or empty means the whole cluster, which is the server's own reading of an absent
   * `group`. See the header: asking cluster-wide from a screen that draws one page is what turns
   * the incremental protocol off above one page without anything saying so.
   */
  groups?: readonly string[] | undefined,
): Promise<Fetched<LagDelta>> {
  const answer = await api.get("/api/v1/clusters/{clusterId}/consumer-groups/lag", {
    params: {
      path: { clusterId },
      query: {
        // An absent `since` is the documented way to ask for everything. Sending an empty string
        // instead would be an *unrecognised* token, which the server also answers in full — but
        // only by accident of it not matching, and relying on that is relying on an implementation
        // detail. The same reasoning applies to `group`: an empty list is not "no groups".
        ...(since === undefined ? {} : { since }),
        ...(groups === undefined || groups.length === 0 ? {} : { group: [...groups] }),
      },
    },
  });
  if (!answer.ok) return apiFailure(answer.error);

  // Not an ADR-039 section: this response is the delta itself, like the group detail endpoint and
  // unlike the group list. Recorded documents in `src/recorded/lag-*.json` are the evidence.
  const payload = answer.value as unknown as LagDeltaPayload;
  const advised = figure(payload.nextPollMs);

  return {
    kind: "ready",
    value: {
      changed: (payload.changed ?? []).map(toUpdate),
      gone: payload.gone ?? [],
      token: typeof payload.token === "string" && payload.token !== "" ? payload.token : null,
      nextPollMs: advised === null ? DEFAULT_POLL_MS : Math.max(advised, MIN_POLL_MS),
      // Absent means absent, and the safe reading of "the server did not tell us whether this is a
      // complete answer" is that it is not incremental — fall back to the full list rather than
      // merging figures we cannot place.
      full: payload.full !== false,
    },
  };
}

/**
 * The two figures only a whole list carries.
 *
 * A lag answer names neither: it does not say how many coordinators failed and it does not count
 * the cluster's groups. So a merge hands back `null` here and the screen keeps what it had, rather
 * than resetting a total to zero every thirty seconds.
 */
export interface ListingFigures {
  readonly coordinatorsMissing: number;
  readonly totalItems: number | null;
}

/**
 * What a delta does to the rows the screen already holds.
 *
 * A union rather than a `GroupSummary[]` with a side flag, because the two outcomes are not
 * variations on each other: one updates three fields in place, and the other says the incremental
 * protocol cannot continue and the caller must pay for a whole list.
 */
export type LagMerge =
  | {
      readonly kind: "merged";
      readonly rows: readonly GroupSummary[];
      /** How many rows `gone` removed, so a caller holding a server-given total can adjust it. */
      readonly removed: number;
    }
  | { readonly kind: "needs-full-list"; readonly reason: string };

/**
 * Merges a delta into the rows already on screen.
 *
 * The whole risk of an incremental protocol is in one sentence: **a group the answer does not
 * mention is unchanged.** It is not zero, and it is not unknown. So this function only ever writes
 * to rows named in `changed`; every other row is passed through by identity. A merge written as
 * "rebuild each row from the update, defaulting what is missing" would turn every quiet group's lag
 * into `0` on the first poll — a screen full of confident, wrong good news, which is the exact
 * failure the never-zero rule exists to prevent.
 *
 * Two conditions end the incremental run rather than being papered over:
 *
 * - `full` — the server did not honour the token. See the header: a lag answer cannot rebuild a row.
 * - a `changed` entry for a group with no row here. Because every request names the page's own
 *   group ids (see the header), that can only be a group that appeared since the list was
 *   fetched — not the ordinary case of a cluster with more groups than a page holds — and there is
 *   no honest way to invent its topic count, coordinator or partial-read note. Adding a row with
 *   `topics: 0` and `coordinator: null` would print two facts nobody reported.
 */
export function applyLagDelta(rows: readonly GroupSummary[], delta: LagDelta): LagMerge {
  if (delta.full) {
    return {
      kind: "needs-full-list",
      reason: "The server answered in full, so the token it was given no longer identifies a snapshot.",
    };
  }

  const updates = new Map(delta.changed.map((update) => [update.groupId, update]));
  const known = new Set(rows.map((row) => row.groupId));
  const unknown = delta.changed.find((update) => !known.has(update.groupId));
  if (unknown !== undefined) {
    return {
      kind: "needs-full-list",
      reason: `${unknown.groupId} is new since the list was fetched, and a lag answer does not carry enough to draw its row.`,
    };
  }

  const gone = new Set(delta.gone);
  let removed = 0;
  const merged = rows
    .filter((row) => {
      if (!gone.has(row.groupId)) return true;
      removed += 1;
      return false;
    })
    .map((row) => {
      const update = updates.get(row.groupId);
      // Identity, not a copy. An untouched row keeps every field it had, including a `totalLag` of
      // `null` that must not be rewritten, and Solid's keyed rendering skips it entirely.
      if (update === undefined) return row;
      return { ...row, totalLag: update.totalLag, state: update.state, members: update.members };
    });

  return { kind: "merged", rows: merged, removed };
}

/**
 * Keeps the lag column current without describing every group again.
 *
 * The shape of the run, and the reason for each step:
 *
 * 1. The caller has already fetched the whole list. This starts by asking the lag endpoint once
 *    with **no** `since`, and throws the figures away — it wants only the token. The list is at
 *    least as fresh as that answer and carries five more columns, so adopting the lag answer's
 *    numbers over it would gain nothing; what the browser does not have is a place in the server's
 *    snapshot sequence, and a token is the only way to get one.
 * 2. From then on every poll carries the token forward and merges what comes back.
 *
 * Every one of those requests — the seeding call included — names the group ids currently on
 * screen, read from `rows()` at the moment the request is made rather than captured once. That is
 * the whole of the page-scoping described in the header, and reading it late is what keeps it
 * correct after a `full` answer has replaced the rows with a fresh page.
 * 3. When the server answers in full — the token expired, or was never recognised — the merge
 *    refuses and this fetches the whole list again. That is the expensive call, and it happens when
 *    the cheap protocol has told us it cannot continue, which is the only time it is warranted.
 *
 * A poll that fails does **not** blank the list or report the feature as broken. The rows on screen
 * were real when they were fetched and are still the best answer available; an operator watching a
 * lag climb is worse served by an empty table than by figures that stopped moving. The next tick
 * retries, and the initial fetch's own failure rendering is what tells them the service is down.
 */
export function pollLag(
  api: KuiApiClient,
  clusterId: string,
  rows: () => readonly GroupSummary[],
  onRows: (next: readonly GroupSummary[], listing: ListingFigures | null, removedCount: number) => void,
  query: GroupQuery = FIRST_PAGE,
  /** Reports whether the last poll reached the server, so a caller can surface and clear a banner. */
  onHealth?: (ok: boolean) => void,
): () => void {
  let stopped = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let since: string | undefined;

  const later = (ms: number): void => {
    if (stopped) return;
    timer = setTimeout(() => void tick(), ms);
  };

  const fullList = async (): Promise<void> => {
    // The page on screen, not the first one. A refetch that silently went back to page 1 would
    // replace the rows the operator is reading with different groups and leave the paginator
    // saying they are still on page 4.
    const answer = await fetchGroups(api, clusterId, query);
    if (stopped || answer.kind !== "ready") return;
    onRows(
      answer.value.groups,
      {
        coordinatorsMissing: answer.value.coordinatorsMissing,
        totalItems: answer.value.page.totalItems,
      },
      0,
    );
  };

  /** The page, as the request names it. Read per request: the rows are edited in place by polls. */
  const onScreen = (): readonly string[] => rows().map((row) => row.groupId);

  const tick = async (): Promise<void> => {
    const answer = await fetchLagDelta(api, clusterId, since, onScreen());
    if (stopped) return;
    if (answer.kind !== "ready") {
      onHealth?.(false);
      later(DEFAULT_POLL_MS);
      return;
    }
    onHealth?.(true);

    const delta = answer.value;
    since = delta.token ?? undefined;
    const merge = applyLagDelta(rows(), delta);
    if (merge.kind === "merged") onRows(merge.rows, null, merge.removed);
    else await fullList();
    later(delta.nextPollMs);
  };

  // The seeding call, step 1 above. It is a normal poll with no token, and the merge it produces is
  // discarded — `applyLagDelta` would answer `needs-full-list` for it, since a first answer is
  // always `full`, and refetching a list the caller fetched a moment ago would be waste.
  const seed = async (): Promise<void> => {
    const answer = await fetchLagDelta(api, clusterId, undefined, onScreen());
    if (stopped) return;
    if (answer.kind === "ready") {
      onHealth?.(true);
      since = answer.value.token ?? undefined;
      later(answer.value.nextPollMs);
    } else {
      onHealth?.(false);
      later(DEFAULT_POLL_MS);
    }
  };

  /*
   * Scheduled rather than issued, and the zero is doing real work.
   *
   * `rows()` is a signal the caller writes from a *sibling* effect — the one that reads the list
   * answer — and Solid 2 commits a write on a microtask. `pollLag` is started from the effect that
   * runs straight after it, in the same tick, so a request composed here and now names the page as
   * it stood *before* the answer that started this poll: an empty one, and therefore an unscoped
   * cluster-wide request. A zero-delay timer runs after those microtasks have settled.
   *
   * This was found in a browser and not here. A unit test hands `pollLag` a plain variable that is
   * already assigned, so the ordering it exercises is not the ordering the route has — which is
   * exactly the shape of hole a test written beside its own subject leaves.
   */
  timer = setTimeout(() => void seed(), 0);

  return () => {
    stopped = true;
    if (timer !== undefined) clearTimeout(timer);
  };
}
