/**
 * The ksqlDB wire, as this browser reads it.
 *
 * ## Every shape here is transcribed from `services/ksql/contract`'s DTOs, and checked against them
 *
 * The first draft of this file was written from the wave plan's prose contract — *`Section`-wrapped
 * reads and one write under `/api/v1/clusters/{clusterId}/ksql/…`* — and it was wrong in five ways
 * at once: it read `data.queries` where the service flattens queries into `items` with a `kind`, it
 * expected the statement's answer inside a `Section` where the service returns the DTO bare, it had
 * no plan phase at all where the service implements ADR-045 over free-form text, it typed a cell as
 * a JSON value where the service renders every cell as text, and it put the push query's columns on
 * the row frame where the service sends them once on `phase`. Not one of those would have failed a
 * unit case: the decoders were self-consistent and the stories were drawn from the same invention.
 *
 * What caught it was reading `KsqlDtos.scala`, and what *keeps* it caught is `wire.golden.test.ts`,
 * which reads `services/ksql/contract/test/resources/golden/*.json` off disk, decodes every one of
 * them through the same functions the screen calls, and **throws a named failure when that
 * directory does not exist**. House rule 12: the path is the contract, and a suite that skipped
 * would report green over a browser and a service that have never met.
 *
 * What this side reads, field by field:
 *
 *   GET  /api/v1/clusters/{clusterId}/ksql/objects
 *     { "objects": Section<{ items: [ { kind, name, topic, format, windowed, sinks,
 *                                       statement, partitions, replication } ],
 *                            unreadable: [ … ], truncated: <int> }> }
 *
 *   POST /api/v1/clusters/{clusterId}/ksql/statements/plan   { statement, token }
 *     { statement, shape, destructive, deletesTopic, warnings, token, expiresAt, computedAt }
 *
 *   POST /api/v1/clusters/{clusterId}/ksql/statements        { statement, token }
 *     { statement, shape, outcome, columns, rows, message, entity, executedAt }
 *
 *   GET  /api/v1/clusters/{clusterId}/ksql/stream?statement=…            (ADR-035 SSE)
 *     event: phase   data: { "columns": [ … ] }      — once, when the server accepts the query
 *     event: row     data: { "values":  [ … ] }      — one per record
 *
 * The plan and the result are **not** section-wrapped and nothing here pretends otherwise: they are
 * `KuiEndpoint.mutation` outputs, so a failure arrives as an ADR-034 envelope with a status code,
 * which `@kui/api`'s client already turns into an `ApiError`.
 *
 * ## Every decoder refuses rather than answering emptiness
 *
 * `data.items ?? []` is shorter and is the wave-5 defect verbatim: it turns *"this build read the
 * wrong field name"* into *"this ksqlDB cluster has no streams"*, and the second sentence is one an
 * operator acts on. So a payload this build does not recognise answers {@link Unreadable}, and the
 * screen says KUI could not read what the server said.
 *
 * ## A cell is text, and `null` and *absent* are two more things it can be
 *
 * `StatementResultDto.rows` renders every value as text rather than as typed JSON — ksqlDB's row
 * schema is whatever the statement selected (ADR-055 §4) — so a cell is a string. It can also be
 * two other things, and they are not the same thing as each other:
 *
 * - **`null`**, which `statement-rows.json` and `ksql-stream-frame.json` both carry. That is a SQL
 *     null the query produced: the column was selected and its value is nothing.
 *   - **missing**, when a row is shorter than the column list the same answer declared. That is a
 *     disagreement inside one document rather than a value.
 *
 * {@link KsqlCell} keeps all three apart, because padding either of the last two into a blank cell
 * lets it read as data — and one of them is the shape a decoding fault takes.
 */
import { decodeSection, type Section } from "@kui/api";

/** The key the object listing wraps its outer `Section` in — `KsqlObjectsResponse`'s one field. */
export const KSQL_OBJECTS_SECTION_KEY = "objects";

/**
 * The SSE event a push query's rows arrive under.
 *
 * `SseEventName.Row`, which `services/ksql` takes from `libs/contracts-core` rather than spelling
 * itself — the whole argument in that file's scaladoc is about the five copies of `alerts` and
 * `message` that nothing compares. This is the browser's copy and `wire.golden.test.ts` compares it
 * to the service's committed frame, so there are two spellings and one of them is checked against
 * the other. It is deliberately not one of ADR-035's four shared names: `@kui/kernel`'s stream
 * client handles those itself and throws for a subscriber that lists one.
 */
export const KSQL_ROW_EVENT_NAME = "row";

/** What {@link decodeObjects} and the statement decoders answer for a payload they cannot read. */
export const Unreadable = Symbol("ksqlDB document this build cannot read");
export type Unreadable = typeof Unreadable;

/**
 * What one row of the object listing is.
 *
 * The service's four kinds, plus `other` for a word this build has never seen. `other` is not a
 * fallback that hides something: ksqlDB's object vocabulary has grown across releases, and an
 * unknown kind is counted and named on screen rather than drawn as a stream. Guessing `stream`
 * would be KUI making a claim about somebody's ksqlDB on no evidence.
 */
export type KsqlObjectKind = "stream" | "table" | "query" | "topic" | "other";

/**
 * One object, with the fields its own kind carries.
 *
 * Everything but `kind` and `name` is optional **by kind, not by luck**, and `KsqlDtos.scala`
 * states which: a stream and a table have a topic, a table has `windowed`, a query has `statement`
 * and `sinks`, a topic has `partitions` and `replication`. Nothing here defaults one of them — a
 * `null` `format` means the server did not report one, which older ksqlDB releases genuinely do
 * not, and writing `JSON` there would tell an operator their Avro stream is JSON.
 */
export interface KsqlObjectRow {
  readonly kind: KsqlObjectKind;
  /** The word the server used, kept verbatim so an `other` row can say what it actually was. */
  readonly rawKind: string;
  readonly name: string;
  readonly topic: string | undefined;
  readonly format: string | undefined;
  readonly windowed: boolean | undefined;
  /** What a query writes into. Empty for a transient query, which writes nowhere. */
  readonly sinks: readonly string[];
  /** A query's own SQL. The only description of what a persistent query does. */
  readonly statement: string | undefined;
  readonly partitions: number | undefined;
  readonly replication: number | undefined;
}

export interface KsqlObjects {
  /** In the server's order: it orders by kind then name, and re-ordering hides that it stopped. */
  readonly items: readonly KsqlObjectRow[];
  /**
   * The rows the server returned and KUI could not describe, in the order it returned them.
   *
   * Drawn rather than dropped, because a row missing from a list is indistinguishable from a row
   * that is not there — `KsqlObjectsDto.unreadable`'s own argument, and the live case is a
   * `SHOW STREAMS` answer with a field this build has never seen.
   */
  readonly unreadable: readonly string[];
  /**
   * How many objects the service dropped to keep the answer bounded.
   *
   * **Zero is a measured zero here** and the DTO says so: it is the count of what was cut, not an
   * unknown. A screen that said nothing would leave an operator concluding a stream does not exist.
   */
  readonly truncated: number;
}

/**
 * What a statement is.
 *
 * `unknown` is this build's, not the service's: a shape word from a newer service must not be
 * folded into `statement`, because the three shapes go to three different endpoints and guessing
 * wrong sends a push query to the address that refuses it.
 */
export type StatementShape = "pull_query" | "push_query" | "statement" | "unknown";

/** What running a statement would do, and the token that confirms it (ADR-045). */
export interface StatementPlan {
  /** The statement the service canonicalised, which is what the token is bound to. */
  readonly statement: string;
  readonly shape: StatementShape;
  readonly destructive: boolean;
  /** The one thing ksqlDB's language can do that destroys records. */
  readonly deletesTopic: boolean;
  /** Sentences for the person about to confirm, in the order they should be read. */
  readonly warnings: readonly string[];
  /** `undefined` when nothing needs confirming, which is most statements. */
  readonly token: string | undefined;
  readonly expiresAt: string | undefined;
}

/** One cell: text, a SQL `null`, or *no cell at that position*. See the header. */
export type KsqlCell = string | null | undefined;

/** A result row, positioned against {@link StatementResult.columns}. */
export type KsqlRow = readonly KsqlCell[];

/**
 * What a statement that finished produced.
 *
 * One document for both answers, discriminated by `outcome`, because the client's next action is
 * the same either way — draw what came back. What is not shared is emptiness: `rows` with an empty
 * list is a query that matched nothing, and a statement that returns no rows at all is `status` and
 * has no rows field to misread.
 */
export interface StatementResult {
  readonly statement: string;
  readonly shape: StatementShape;
  readonly outcome: "rows" | "status" | "pending";
  readonly columns: readonly string[];
  readonly rows: readonly KsqlRow[];
  /** The server's own sentence for a status answer. Absent on a rows answer. */
  readonly message: string | undefined;
  /** What the statement acted on, when the server named it. */
  readonly entity: string | undefined;
  readonly executedAt: string;
}

/* --- The listing ------------------------------------------------------------------------------ */

/**
 * The object listing's section payload, or {@link Unreadable}.
 *
 * `items` must be an array. The service's own decoder defaults it, because a Scala decoder is read
 * by a service reading another service; this one does not, because a browser reading a field name
 * that has moved is the defect this package exists to refuse. `unreadable` and `truncated` do
 * default: an older service that omitted them still sent a readable list, and their absence says
 * nothing was dropped.
 */
export function decodeObjects(payload: unknown): KsqlObjects | Unreadable {
  const root = asRecord(payload);
  if (root === undefined) return Unreadable;

  const items = asArray(root["items"]);
  if (items === undefined) return Unreadable;

  return {
    items: items.map(decodeObject).filter((one): one is KsqlObjectRow => one !== undefined),
    unreadable: strings(root["unreadable"]),
    truncated: asNumber(root["truncated"]) ?? 0,
  };
}

/**
 * One object, or `undefined` when the entry has no name.
 *
 * A nameless object is dropped rather than drawn: the name is what makes a row mean anything —
 * every sentence this screen writes about one names it — and it is also the shape a decoding fault
 * takes, so dropping it keeps the count the golden suite compares honest.
 */
function decodeObject(entry: unknown): KsqlObjectRow | undefined {
  const record = asRecord(entry);
  if (record === undefined) return undefined;
  const name = asString(record["name"]);
  if (name === undefined) return undefined;

  const rawKind = asString(record["kind"]) ?? "";
  return {
    kind: objectKind(rawKind),
    rawKind,
    name,
    topic: nonBlank(asString(record["topic"])),
    format: nonBlank(asString(record["format"])),
    windowed: typeof record["windowed"] === "boolean" ? record["windowed"] : undefined,
    sinks: strings(record["sinks"]),
    statement: nonBlank(asString(record["statement"])),
    partitions: asNumber(record["partitions"]),
    replication: asNumber(record["replication"]),
  };
}

/** A kind word, folded to one this build draws. Anything else is `other`; see the type. */
export function objectKind(value: unknown): KsqlObjectKind {
  switch (asString(value)?.toLowerCase()) {
    case "stream":
      return "stream";
    case "table":
      return "table";
    case "query":
      return "query";
    case "topic":
      return "topic";
    default:
      return "other";
  }
}

/* --- The two phases of a statement ------------------------------------------------------------ */

/**
 * A plan, or {@link Unreadable}.
 *
 * `destructive` and `deletesTopic` are read strictly — a missing boolean is not `false`. Defaulting
 * either one would turn a document this build could not read into a confirmation KUI decided nobody
 * needed, which is the single most expensive default available on this screen.
 */
export function decodePlan(payload: unknown): StatementPlan | Unreadable {
  const root = asRecord(payload);
  if (root === undefined) return Unreadable;

  const statement = asString(root["statement"]);
  const destructive = root["destructive"];
  const deletesTopic = root["deletesTopic"];
  const flagsMissing = typeof destructive !== "boolean" || typeof deletesTopic !== "boolean";
  if (statement === undefined || flagsMissing) {
    return Unreadable;
  }

  return {
    statement,
    shape: statementShape(root["shape"]),
    destructive,
    deletesTopic,
    warnings: strings(root["warnings"]),
    token: nonBlank(asString(root["token"])),
    expiresAt: nonBlank(asString(root["expiresAt"])),
  };
}

/**
 * A statement receipt, including accepted-but-pending execution, or {@link Unreadable}.
 *
 * An `outcome` this build has never seen is refused rather than folded into `status`: a screen that
 * drew an unknown answer as "the server said something" would be putting KUI's words over the
 * server's silence, which is the one thing this product's voice rules forbid everywhere else. And a
 * `status` with no sentence is refused, because the whole of that answer *is* the sentence.
 */
export function decodeResult(payload: unknown): StatementResult | Unreadable {
  const root = asRecord(payload);
  if (root === undefined) return Unreadable;

  const statement = asString(root["statement"]);
  const executedAt = asString(root["executedAt"]);
  if (statement === undefined || executedAt === undefined) return Unreadable;

  const outcome = asString(root["outcome"]);
  const message = nonBlank(asString(root["message"]));
  if (outcome !== "rows" && outcome !== "status" && outcome !== "pending") return Unreadable;
  if (outcome === "status" && message === undefined) return Unreadable;

  const rows = asArray(root["rows"]);
  if (outcome === "rows" && rows === undefined) return Unreadable;

  return {
    statement,
    shape: statementShape(root["shape"]),
    outcome,
    columns: strings(root["columns"]),
    rows: (rows ?? []).map(decodeRow).filter((row): row is KsqlRow => row !== undefined),
    message,
    entity: nonBlank(asString(root["entity"])),
    executedAt,
  };
}

/** A shape word, folded. See {@link StatementShape} for why an unknown one is not `statement`. */
export function statementShape(value: unknown): StatementShape {
  switch (asString(value)) {
    case "pull_query":
      return "pull_query";
    case "push_query":
      return "push_query";
    case "statement":
      return "statement";
    default:
      return "unknown";
  }
}

/**
 * One row, or `undefined` when the entry is not an array at all.
 *
 * `null` survives as `null` and anything that is neither a string nor `null` becomes `undefined` —
 * the same value an absent cell has, because a cell of a shape this build has never seen is exactly
 * as much of a disagreement as a missing one and says so in the same words.
 */
function decodeRow(entry: unknown): KsqlRow | undefined {
  const cells = asArray(entry);
  return cells === undefined
    ? undefined
    : cells.map((cell) => (cell === null ? null : asString(cell)));
}

/* --- The push query's two frames -------------------------------------------------------------- */

/**
 * The `phase` frame's columns, or {@link Unreadable}.
 *
 * `phase` is one of ADR-035's shared names and the kernel delivers it through its own callback, so
 * this decoder is called from `onPhase` rather than from the event switch. It arrives as soon as
 * ksqlDB accepts the query rather than when the first record does, which is what makes an idle push
 * query a stream that has visibly started rather than a socket that has said nothing.
 */
export function decodeQueryHeader(payload: unknown): readonly string[] | Unreadable {
  const root = asRecord(payload);
  if (root === undefined) return Unreadable;
  const columns = asArray(root["columns"]);
  return columns === undefined ? Unreadable : columns.map((column) => asString(column) ?? "");
}

/**
 * One `row` frame's values, or {@link Unreadable}.
 *
 * The column names are not on it: they were sent once on `phase`, and a stream that repeated them
 * would send a schema a thousand times to describe a thousand rows.
 */
export function decodeQueryRow(payload: unknown): KsqlRow | Unreadable {
  const root = asRecord(payload);
  if (root === undefined) return Unreadable;
  const values = decodeRow(root["values"]);
  return values === undefined ? Unreadable : values;
}

/* --- The readers everything above is built from ----------------------------------------------- */

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function asArray(value: unknown): readonly unknown[] | undefined {
  return Array.isArray(value) ? (value as readonly unknown[]) : undefined;
}

function asString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

/** A number, and never `NaN` or an infinity — both of which format as words on a screen. */
function asNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

/** A list of strings, dropping anything that is not one. An absent list is an empty one. */
function strings(value: unknown): readonly string[] {
  return (asArray(value) ?? [])
    .map((entry) => asString(entry))
    .filter((entry): entry is string => entry !== undefined);
}

function nonBlank(value: string | undefined): string | undefined {
  return value === undefined || value.trim() === "" ? undefined : value;
}

/** The outer section, decoded the same way every other section in this product is. */
export function sectionOf(body: unknown, key: string): Section<unknown> | undefined {
  const root = asRecord(body);
  if (root === undefined || !(key in root)) return undefined;
  return decodeSection<unknown>(root[key]);
}
