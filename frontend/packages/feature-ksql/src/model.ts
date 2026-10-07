/**
 * The result region's state machine and the words this screen uses.
 *
 * ADR-056 is the decision; this file is the decision made checkable. `SCREENS-V4.md` §3.16 draws
 * the workspace with **no result region at all** — every ksqlDB capture is of an unrun query — and
 * §7.9 carries that as an open finding: *"Columns, streaming rows, the row cap and the error
 * rendering are all unspecified while `Run` is drawn as an enabled control."* Everything here is
 * that finding being closed, and it is here rather than inside a component so that the rules can be
 * driven directly by a case instead of through markup.
 *
 * The four questions ADR-056 had to answer, and where each one lives:
 *
 * - **while it is still arriving** — {@link ResultRegion} `streaming`, which carries the rows so
 *    far and never claims to be a total;
 *  - **when it stops** — `ended`, carrying the same rows and the reason the stream gave;
 *  - **when it stops because the server went away** — `interrupted`, which keeps every row already
 *    received. They arrived and they were true; clearing them would throw away the only evidence
 *    the operator has of what the query was producing when it died;
 * - **the row cap** — {@link MAX_RESULT_ROWS}, with the number dropped counted and said out loud. A
 *    push query is unbounded; a browser tab is not. Silently keeping the first N is worse than
 *    keeping the most recent N, because the rows on screen then stop moving and look like a
 *    finished answer.
 *
 * ## Nothing here counts anything the server counted
 *
 * The rows this region holds are the rows this browser received, and every sentence says so. There
 * is no "1,204 rows produced" anywhere, because nobody measured that: ksqlDB does not report a push
 * query's production rate on this wire, and a figure this product cannot measure is stated in words
 * rather than as a number that looks measured.
 */
import type { KsqlCell, KsqlObjects, KsqlRow, StatementPlan, StatementResult } from "./wire.js";

/**
 * How many rows of one result this browser keeps.
 *
 * A push query over a busy topic produces rows faster than anybody reads them and never stops. The
 * cap is on the browser's side because it is the browser that runs out of memory, and it is a
 * *window on the most recent* rows rather than the first ones: a region that froze after 500 rows
 * would look exactly like a query that had finished, which is the most expensive wrong impression
 * this screen can give.
 *
 * 500 is a screenful of scrolling and not a measurement of anything; ADR-056 records it as a chosen
 * number and names what would change it.
 */
export const MAX_RESULT_ROWS = 500;

/** What the reader is looking at under the editor. */
export type ResultRegion =
  /** Nothing has been run in this tab yet. The region is absent rather than empty. */
  | { readonly kind: "idle" }
  /** A statement has been sent and nothing has come back. */
  | { readonly kind: "running" }
  /** A destructive statement is waiting for the reader to confirm it (ADR-045). */
  | { readonly kind: "confirming"; readonly plan: StatementPlan }
  /** Accepted upstream, but execution has not reached a known terminal state. */
  | {
      readonly kind: "pending";
      readonly message: string | undefined;
      readonly commandId: string | undefined;
    }
  /** A statement that changed something and returned no rows: the server's own sentence. */
  | {
      readonly kind: "status";
      readonly message: string;
      /** What it acted on, when the server named it. */
      readonly entity: string | undefined;
    }
  /** A pull query that finished. `rows` is the whole answer. */
  | {
      readonly kind: "rows";
      readonly columns: readonly string[];
      readonly rows: readonly KsqlRow[];
    }
  /** A push query with its stream open. `rows` is what has arrived, never a total. */
  | {
      readonly kind: "streaming";
      readonly columns: readonly string[];
      readonly rows: readonly KsqlRow[];
      readonly dropped: number;
    }
  /** A push query that ended, by itself or because the reader stopped it. */
  | {
      readonly kind: "ended";
      readonly columns: readonly string[];
      readonly rows: readonly KsqlRow[];
      readonly dropped: number;
      /** Why it ended, in the words the stream used. */
      readonly reason: string;
    }
  /** The stream stopped because something broke. The rows already received are kept. */
  | {
      readonly kind: "interrupted";
      readonly columns: readonly string[];
      readonly rows: readonly KsqlRow[];
      readonly dropped: number;
      readonly message: string;
    }
  /** The statement did not run at all. */
  | { readonly kind: "failed"; readonly message: string; readonly code: string | undefined };

/** The empty region, before anything has been run. */
export const IDLE: ResultRegion = { kind: "idle" };

/** A push query with its stream open and nothing in it yet. */
export const OPENING: ResultRegion = { kind: "streaming", columns: [], rows: [], dropped: 0 };

/** Whether a statement is in flight or a stream is open — what the Run/Cancel control reads. */
export function isLive(region: ResultRegion): boolean {
  return region.kind === "running" || region.kind === "streaming";
}

/**
 * The region a statement receipt produces.
 *
 * `pending` confirms acceptance, not completion. `status` and `rows` are terminal outcomes,
 * kept apart here for the reason
 * `StatementResultDto`'s scaladoc gives: `rows` with an empty list is a query that matched nothing,
 * which is a measured emptiness worth saying out loud, and a statement that returns no rows at all
 * has no rows field to misread.
 */
export function regionFor(result: StatementResult): ResultRegion {
  if (result.outcome === "pending") {
    return { kind: "pending", message: result.message, commandId: result.entity };
  }
  if (result.outcome === "status") {
    return {
      kind: "status",
      // The decoder refuses a status with no sentence, so this fallback is unreachable from a
      // decoded result; it is here because the type says the field is optional and a silent `""`
      // would render as a blank panel that says the product knows and will not tell.
      message:
        result.message ?? "The ksqlDB server accepted the statement and said nothing about it.",
      entity: result.entity,
    };
  }
  return { kind: "rows", columns: result.columns, rows: result.rows };
}

/**
 * The column list a push query's `phase` frame declared.
 *
 * The **first** one wins: a server that re-sent a different column list mid-query would otherwise
 * silently re-align every row already on screen against headings they were never produced under.
 */
export function withColumns(region: ResultRegion, columns: readonly string[]): ResultRegion {
  if (region.kind !== "streaming" || region.columns.length > 0) return region;
  return { ...region, columns };
}

/**
 * One row appended to a live region.
 *
 * A row that arrives after the stream ended is dropped rather than appended — the region has
 * already said how many rows the query delivered, and a late row would make that sentence false.
 */
export function appendRow(
  region: ResultRegion,
  row: KsqlRow,
  cap: number = MAX_RESULT_ROWS,
): ResultRegion {
  if (region.kind !== "streaming") return region;

  const rows = [...region.rows, row];
  const overflow = Math.max(0, rows.length - cap);
  return {
    kind: "streaming",
    columns: region.columns,
    // The window is on the most recent rows. See {@link MAX_RESULT_ROWS}.
    rows: overflow === 0 ? rows : rows.slice(overflow),
    dropped: region.dropped + overflow,
  };
}

/** The region a live stream becomes when it finishes without a failure. */
export function endLive(region: ResultRegion, reason: string): ResultRegion {
  if (region.kind !== "streaming") return region;
  return {
    kind: "ended",
    columns: region.columns,
    rows: region.rows,
    dropped: region.dropped,
    reason,
  };
}

/**
 * The region a live stream becomes when it stops because something broke.
 *
 * Distinct from {@link endLive} and from `failed`, and the distinction is the whole of ADR-056's
 * third question. `failed` means the statement never ran; `interrupted` means it ran, produced
 * these rows, and then the connection died — so the rows stay on screen with a banner over them
 * rather than being replaced by an error panel. Replacing them would delete the only record the
 * operator has of what their query was doing.
 */
export function interrupt(
  region: ResultRegion,
  message: string,
  code?: string | undefined,
): ResultRegion {
  if (region.kind === "streaming") {
    return {
      kind: "interrupted",
      columns: region.columns,
      rows: region.rows,
      dropped: region.dropped,
      message,
    };
  }
  return { kind: "failed", message, code };
}

/* --- The words ---------------------------------------------------------------------------- */

/** A push query whose stream is open and which has produced nothing yet. */
export const NO_ROWS_YET =
  "No row has arrived yet. A push query shows rows as they are produced, so nothing appears here " +
  "until something is written to the stream it reads.";

/** A pull query that ran and matched nothing. A fact about the data, and never “0 rows”. */
export const NO_ROWS = "This query ran and matched no rows.";

/** What a cell with no value at that position says. Not a SQL null — see `wire.ts`. */
export const CELL_ABSENT = "no value sent";

/** What a SQL `null` says. The word, because the query selected the column and it is nothing. */
export const CELL_NULL = "null";

/** A ksqlDB server this deployment has not configured. */
export const NOT_CONFIGURED =
  "No ksqlDB server is configured for this cluster. Set kui.clusters.<n>.ksql.url and " +
  "restart KUI to run statements against one.";

/** A ksqlDB that answered and is running nothing. */
export const NO_OBJECTS =
  "The ksqlDB server answered and has no streams or tables yet. One appears here as soon as a " +
  "CREATE STREAM or CREATE TABLE runs.";

/**
 * Why the `auto.offset.reset` control on the workspace footer is inert.
 *
 * `KsqlWorkspace` draws it because `SCREENS-V4.md` §3.16 draws it, and it decides whether a push
 * query shows the last hour of a topic or only what arrives from now on — the difference between a
 * query that answers and one that appears to hang. But the wire carries nowhere to put it:
 * `StatementRequestDto` is `{statement, token}` and `KsqlStreamEndpoint` takes one `statement`
 * query parameter. So the control is **disabled and this sentence is beside it**, rather than
 * enabled and silently doing nothing — which is the failure mode `MESSAGES_PURGE` already cost this
 * project once, a control that looks live and has never worked. ADR-056 §5 names the wire change.
 */
export const OFFSET_RESET_NOT_SETTABLE =
  "KUI cannot set auto.offset.reset: the ksqlDB statement wire carries no query properties, so a " +
  "push query starts wherever this ksqlDB server's own configuration puts it.";

/** Completes “You do not have permission to …” for the statement editor. */
export const EXECUTE_ACTION = "run a ksqlDB statement";

/**
 * How many rows are on screen, and whether that is all of them.
 *
 * Four sentences rather than one template, because the facts are not interchangeable: a live
 * query's count is a floor, a finished query's is a total, and a capped one has to say what is
 * missing or the reader believes the window is the answer.
 */
export function rowSentence(region: ResultRegion): string {
  switch (region.kind) {
    case "rows":
      return region.rows.length === 0 ? NO_ROWS : `${plural(region.rows.length)}.`;
    case "streaming":
      return region.rows.length === 0
        ? NO_ROWS_YET
        : `${plural(region.rows.length)} so far, and the query is still running.` +
          dropped(region.dropped);
    case "ended":
      return region.rows.length === 0
        ? "The query ended without producing a row."
        : `${plural(region.rows.length)} before the query ended.${dropped(region.dropped)}`;
    case "interrupted":
      return region.rows.length === 0
        ? "The query ended before it produced a row."
        : `${plural(region.rows.length)} before the stream ended.${dropped(region.dropped)}`;
    default:
      return "";
  }
}

function plural(count: number): string {
  return count === 1 ? "1 row" : `${count} rows`;
}

/**
 * What the window dropped, said out loud.
 *
 * Empty when nothing was dropped: a sentence about a cap that has not been reached is noise, and
 * noise is what stops the sentence being read on the run where it matters.
 */
function dropped(count: number): string {
  if (count === 0) return "";
  return (
    ` ${plural(count)} arrived earlier and are no longer held: ` +
    `this screen keeps the most recent ${MAX_RESULT_ROWS}.`
  );
}

/**
 * The sentence under the page title.
 *
 * Counted from what the server named, and absent until something has answered. A voice line written
 * over a document nobody has read yet is the product asserting what it does not know.
 */
export function ksqlVoice(objects: KsqlObjects): string {
  const streams = countOf(objects, "stream");
  const tables = countOf(objects, "table");
  const queries = countOf(objects, "query");
  if (objects.items.length === 0) {
    return `This ksqlDB server answered and named nothing.${extras(objects)}`;
  }
  const parts = [phrase(streams, "stream"), phrase(tables, "table")].filter((part) => part !== "");
  const running =
    queries === 0 ? "no query is running" : `${phrase(queries, "query", "queries")} running`;
  const head = parts.length === 0 ? "" : `${parts.join(" and ")}, `;
  return `${head}${running}.${extras(objects)}`;
}

function countOf(objects: KsqlObjects, kind: string): number {
  return objects.items.filter((one) => one.kind === kind).length;
}

/**
 * The two things the listing says that no count of streams can.
 *
 * `truncated` is a **measured** zero when it is zero — the service counts what it cut — so it is
 * said only when there is something to say, and said as a number when there is. `unreadable` names
 * rows the server returned and KUI could not describe; a screen that omitted them would be
 * indistinguishable from a ksqlDB that does not have them.
 */
function extras(objects: KsqlObjects): string {
  const parts: string[] = [];
  if (objects.unreadable.length > 0) {
    parts.push(
      ` ${phrase(objects.unreadable.length, "row")} the server returned could not be ` +
        "described by this build.",
    );
  }
  if (objects.truncated > 0) {
    parts.push(
      ` ${phrase(objects.truncated, "object")} were left out to keep the answer bounded, so this ` +
        "list is not the whole of what this ksqlDB knows.",
    );
  }
  return parts.join("");
}

function phrase(value: number, one: string, many?: string): string {
  if (value === 0) return "";
  return value === 1 ? `1 ${one}` : `${value} ${many ?? `${one}s`}`;
}

/**
 * How many undescribable rows the banner names before it starts counting them.
 *
 * The service does not bound this list. `KsqlObjects.of` caps `items` at 500 and passes
 * `unreadable` through untouched, so a ksqlDB this build disagrees with about 10,000 rows puts
 * 10,000 names on the wire — and the banner joined every one of them into one sentence. Ten is a
 * **chosen** number and not a measurement: enough names to recognise a pattern in, few enough to
 * read at the top of a screen. The service half is filed rather than fixed here, because a browser
 * bound cannot keep a 10,000-entry list out of one JSON document.
 *
 * Bounded with the cost stated, like every other cap in this package: the rest are counted out
 * loud, because a sentence that quietly stopped at ten would be this screen doing the thing the
 * `unreadable` list exists to refuse.
 */
export const MAX_NAMED_UNREADABLE = 10;

/**
 * The banner over a listing that carries rows this build could not describe.
 *
 * A row missing from a list is indistinguishable from a row that is not there, which is why the
 * service puts these on the wire at all — so they are named here rather than dropped, up to the
 * bound above, and counted after it.
 */
export function unreadableSentence(names: readonly string[]): string {
  const named = names.slice(0, MAX_NAMED_UNREADABLE);
  const rest = names.length - named.length;
  const more = rest === 0 ? "" : `, and ${phrase(rest, "other row", "other rows")}`;
  return (
    `KUI could not describe ${named.join(", ")}${more}. ` +
    "This build and the ksqlDB server disagree about what those rows are, so they are named here " +
    "rather than left out of the list."
  );
}

/**
 * What one cell renders as.
 *
 * Three cases, because the wire has three. A SQL `null` is a value the query produced and is
 * printed as the word; a cell the server did not send at all — a row shorter than the column list
 * the same answer declared — says so in words. Neither is drawn as a blank, because a blank cell
 * reads as a value and one of these is a disagreement inside one document.
 */
export function cellText(cell: KsqlCell): string {
  if (cell === undefined) return CELL_ABSENT;
  if (cell === null) return CELL_NULL;
  return cell;
}
