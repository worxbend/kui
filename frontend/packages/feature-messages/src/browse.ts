/**
 * Everything one browse asks for, and the URL grammar it is written in.
 *
 * No DOM, no reactivity, no `fetch`. Every rule below is a function a test can call, which is the
 * only reason the grammar is testable at all: a browse URL is the one thing on this screen that is
 * wrong silently — the server answers 400, or worse answers 200 for a range nobody asked for, and
 * nothing on either side notices.
 *
 * ## Ported from behaviour, not from code
 *
 * This is the TypeScript of `frontend/ui-messages/.../browse/BrowseQuery.scala`. The Scala version
 * could read the parameter *names* out of `kui.message.contract.BrowseAddress`, because that module
 * cross-compiled to Scala.js and both halves saw one copy of the strings. This package cannot: the
 * server's contract module does not compile to TypeScript, and the OpenAPI document the rest of
 * `@kui/api` is generated from describes the stream endpoint's *path* but not its query grammar (a
 * repeated `seekTo` whose values are a two-colon mini-language is not something a generated client
 * can express). So the names and the value grammar are spelled once here, in `BROWSE_PARAM` and
 * `encodeSeek`, and nowhere else in the frontend.
 *
 * That is a real risk and it is written down rather than hidden: renaming `seekTo` on the server
 * would leave this compiling and 404 — sorry, 400 — at run time. The mitigation is that there is
 * exactly one copy, it is named, and `browse.test.ts` asserts the exact query strings the server's
 * own `BrowseParamsSuite` asserts. If the seam ever earns a stronger check, the check is a
 * generated constants file like `constants.generated.ts`, not a second copy of these strings in a
 * component.
 */

/**
 * Where a browse starts.
 *
 * Four shapes, because those are the four ways a person says where to look, plus a fifth
 * (`atOffsets`) that no control produces and that must nonetheless survive: it is how a colleague
 * hands over *exactly* where they were, one offset per partition, and it is the shape the server's
 * own continuation cursors have internally. A control for it would be a table of sixty inputs; a
 * link carrying one is free.
 */
export type SeekMode =
  | { readonly kind: "beginning" }
  | { readonly kind: "latest" }
  /** The same offset on every selected partition. */
  | { readonly kind: "offset"; readonly offset: string }
  /** The first record at or after this epoch millisecond. */
  | { readonly kind: "timestamp"; readonly epochMillis: number }
  /** One offset per partition. Never built by a control; round-trips through a URL. */
  | { readonly kind: "atOffsets"; readonly offsets: ReadonlyMap<number, string> };

/**
 * A serde override, or the empty string for "let the service choose".
 *
 * The eleven names are the spellings the service resolves against — they are what an operator
 * already has in their configuration file. A twelfth typed here would be a menu entry that sends a
 * name nothing answers to.
 */
export type SerdeName =
  | ""
  | "STRING"
  | "JSON"
  | "INT32"
  | "INT64"
  | "UINT32"
  | "UINT64"
  | "UUID"
  | "BASE64"
  | "HEX"
  | "SCHEMA_REGISTRY"
  | "FALLBACK";

/**
 * The serdes a browse may name, as `{value, label}`, with "Automatic" first.
 *
 * ## Where an override comes from today
 *
 * Only from the address. A link carrying `keySerde` or `valueSerde` decodes the browse with that
 * serde ({@link fromParams} reads it through {@link offeredSerde}), and the browse's own URL keeps
 * it, so the link survives being shared. Neither the filter bar nor the produce drawer draws a
 * serde picker: the bar has no control for the decode side, and the drawer always produces with
 * Automatic, since `EMPTY_RECORD_DRAFT` holds no serde and nothing in the drawer sets one. A
 * picker added on either side should offer this list rather than a second one: publishing with a
 * serde the reader cannot read back is how this screen would leave a topic worse than it found it.
 *
 * ## Why "Automatic" is the empty string and the default
 *
 * The service already resolves a serde per topic and per half of the record, says which it used on
 * every record it returns, and is right for almost every topic. Sending no name at all is what
 * makes a produced record agree with the browse by default: the record is written with the serde
 * it would have been read with. The override exists for the topic where that choice is wrong — a
 * key written as a big-endian long that autodetection reads as four characters of nonsense — where
 * without one the only way to work with the topic is to edit the deployment's configuration, which
 * somebody in the middle of an incident cannot do.
 */
export const SERDE_CHOICES: readonly { readonly value: SerdeName; readonly label: string }[] = [
  { value: "", label: "Automatic" },
  { value: "STRING", label: "STRING" },
  { value: "JSON", label: "JSON" },
  { value: "INT32", label: "INT32" },
  { value: "INT64", label: "INT64" },
  { value: "UINT32", label: "UINT32" },
  { value: "UINT64", label: "UINT64" },
  { value: "UUID", label: "UUID" },
  { value: "BASE64", label: "BASE64" },
  { value: "HEX", label: "HEX" },
  { value: "SCHEMA_REGISTRY", label: "SCHEMA_REGISTRY" },
  { value: "FALLBACK", label: "FALLBACK" },
];

/**
 * A serde {@link SERDE_CHOICES} lists, or `""` (Automatic) for anything else.
 *
 * Used when a link's `keySerde` or `valueSerde` parameter is read. A serde the list does not name —
 * one a deployment configured itself, or one from a newer KUI — falls back to Automatic and is
 * dropped from the browse rather than sent on, so the browse never asks for a serde this build
 * cannot name.
 */
export function offeredSerde(raw: string): SerdeName {
  const found = SERDE_CHOICES.find((choice) => choice.value !== "" && choice.value === raw);
  return found?.value ?? "";
}

/** Everything one browse asks for. */
export interface BrowseQuery {
  readonly seek: SeekMode;
  /** Empty means every partition, which is what the server means by the parameter being absent. */
  readonly partitions: readonly number[];
  readonly limit?: number | undefined;
  /** A plain substring the decoded record must contain. */
  readonly contains?: string | undefined;
  readonly keySerde?: SerdeName | undefined;
  readonly valueSerde?: SerdeName | undefined;
  /**
   * Tail mode. Exclusive with a start position, and the **server** refuses the combination — so
   * this screen never sends both: turning LIVE on *sets* the seek to `latest`, which is what
   * following means.
   */
  readonly live: boolean;
  /**
   * The registered smart filter this browse runs, and the expression it was minted from.
   *
   * Both, always together. The id is the short handle a URL can carry; the source travels with it
   * so that whichever replica answers can compile it rather than telling the user their filter has
   * expired — and so that a link somebody was sent shows the *expression* in the editor rather than
   * sixteen hexadecimal characters nobody can read.
   */
  readonly filterId?: string | undefined;
  readonly filterSource?: string | undefined;
  /**
   * The signed continuation of a finished browse.
   *
   * Never read out of a URL and never written into one: a cursor is a five-minute-old statement
   * about offsets, and a link carrying one would take the recipient to a page that no longer
   * exists or, worse, quietly to a different one. It is set for exactly one request — the "load
   * more" — and discarded.
   */
  readonly cursor?: string | undefined;
}

/**
 * What the screen starts on: the newest records.
 *
 * The end and not the beginning, because the overwhelmingly common question about a topic is "what
 * is happening now", and a topic with a million records answers "start at the beginning" with a
 * million records nobody wanted. It is also what every reference product does, and an operator's
 * fingers already expect it.
 */
export const DEFAULT_BROWSE: BrowseQuery = {
  seek: { kind: "latest" },
  partitions: [],
  live: false,
};

/** The query-parameter names. One copy; see the note at the top of this file. */
export const BROWSE_PARAM = {
  seek: "seekTo",
  partition: "partition",
  limit: "limit",
  /** `q`, the same name every other list screen in KUI uses. */
  contains: "q",
  live: "live",
  keySerde: "keySerde",
  valueSerde: "valueSerde",
  filterId: "filterId",
  filterSource: "filterSource",
  cursor: "cursor",
} as const;

/**
 * The separator between a seek's two halves.
 *
 * Two colons and not one, so that a future timestamp form written as an ISO instant — which
 * contains single colons — needs no escaping and no change here. The server's `BrowseParams` says
 * the same thing for the same reason.
 */
const SEPARATOR = "::";

/** The `seekTo` values one seek becomes. A per-partition seek becomes several. */
export function encodeSeek(seek: SeekMode): readonly string[] {
  switch (seek.kind) {
    case "beginning":
      return ["beginning"];
    case "latest":
      return ["latest"];
    case "offset":
      return [`offset${SEPARATOR}${seek.offset}`];
    case "timestamp":
      return [`timestamp${SEPARATOR}${String(seek.epochMillis)}`];
    case "atOffsets":
      return [...seek.offsets.entries()]
        .sort((a, b) => a[0] - b[0])
        .map(([partition, offset]) => `${String(partition)}${SEPARATOR}${offset}`);
  }
}

/**
 * One or more `seekTo` values back into a seek, or `undefined` if they do not parse.
 *
 * `undefined` rather than a thrown error, and the caller falls back to the default: these values
 * come from a link somebody was sent, and a parameter that no longer parses should cost the
 * recipient that one setting rather than the whole screen.
 */
export function decodeSeek(values: readonly string[]): SeekMode | undefined {
  const entries = values.map((value) => value.trim()).filter((value) => value.length > 0);
  if (entries.length === 0) return undefined;

  if (entries.length === 1) {
    const only = entries[0] as string;
    if (only === "beginning") return { kind: "beginning" };
    if (only === "latest") return { kind: "latest" };
    const [head, tail] = splitOnce(only);
    if (tail === undefined) return undefined;
    if (head === "offset") return isDigits(tail) ? { kind: "offset", offset: tail } : undefined;
    if (head === "timestamp") {
      const millis = Number(tail);
      return Number.isSafeInteger(millis) ? { kind: "timestamp", epochMillis: millis } : undefined;
    }
  }

  // The per-partition form. Every entry must be one, and the whole thing is refused if any is not:
  // the server refuses a mixture rather than resolving it by a precedence rule, and a browser that
  // quietly picked one half of a mixed link would show a range nobody asked for.
  const offsets = new Map<number, string>();
  for (const entry of entries) {
    const [head, tail] = splitOnce(entry);
    if (tail === undefined || !isDigits(head) || !isDigits(tail)) return undefined;
    const partition = Number(head);
    if (!Number.isSafeInteger(partition) || offsets.has(partition)) return undefined;
    offsets.set(partition, tail);
  }
  return { kind: "atOffsets", offsets };
}

/**
 * The query string this browse becomes, without the leading `?`.
 *
 * A cursor already says where to start, and the server refuses the two together rather than picking
 * one — which is right, and means the screen must send one or the other. Following is dropped for
 * the same reason: a continuation is not a tail.
 */
export function queryString(query: BrowseQuery): string {
  const pairs: [string, string][] = [];

  if (query.cursor === undefined) {
    for (const value of encodeSeek(query.seek)) pairs.push([BROWSE_PARAM.seek, value]);
  }

  for (const partition of [...query.partitions].sort((a, b) => a - b)) {
    pairs.push([BROWSE_PARAM.partition, String(partition)]);
  }

  if (query.limit !== undefined) pairs.push([BROWSE_PARAM.limit, String(query.limit)]);
  if (query.contains !== undefined && query.contains !== "") {
    pairs.push([BROWSE_PARAM.contains, query.contains]);
  }
  // Sent only when the user overrode it. Absent means "let the service choose", which is what it
  // does anyway, and a URL that spells out every default is one a person cannot read.
  if (query.keySerde !== undefined && query.keySerde !== "") {
    pairs.push([BROWSE_PARAM.keySerde, query.keySerde]);
  }
  if (query.valueSerde !== undefined && query.valueSerde !== "") {
    pairs.push([BROWSE_PARAM.valueSerde, query.valueSerde]);
  }
  // `live=false` and no `live` at all mean the same thing to the server, and the shorter URL is
  // the one a person can read.
  if (query.live && query.cursor === undefined) pairs.push([BROWSE_PARAM.live, "true"]);
  // The smart filter travels alongside a cursor, unlike the seek: a cursor names a position, and
  // which records at that position are worth delivering is still the filter's decision.
  if (query.filterId !== undefined) pairs.push([BROWSE_PARAM.filterId, query.filterId]);
  if (query.filterSource !== undefined) pairs.push([BROWSE_PARAM.filterSource, query.filterSource]);
  if (query.cursor !== undefined) pairs.push([BROWSE_PARAM.cursor, query.cursor]);

  return pairs.map(([name, value]) => `${encode(name)}=${encode(value)}`).join("&");
}

/**
 * Reads a browse back out of a URL's parameters.
 *
 * Lenient throughout, and deliberately: a value that no longer parses costs the recipient that one
 * setting rather than the whole screen. An unreadable seek falls back to the default, which is a
 * screen that works.
 */
export function fromParams(params: URLSearchParams): BrowseQuery {
  const partitions = [
    ...new Set(
      params
        .getAll(BROWSE_PARAM.partition)
        // The comma-separated spelling as well as the repeated one, because both round-trip
        // through the server's own codec and a link written either way must read back the same.
        .flatMap((raw) => raw.split(","))
        .map((raw) => Number(raw.trim()))
        .filter((value) => Number.isSafeInteger(value) && value >= 0),
    ),
  ].sort((a, b) => a - b);

  const limit = Number(params.get(BROWSE_PARAM.limit) ?? "");
  const contains = (params.get(BROWSE_PARAM.contains) ?? "").trim();

  return {
    seek: decodeSeek(params.getAll(BROWSE_PARAM.seek)) ?? DEFAULT_BROWSE.seek,
    partitions,
    ...(Number.isSafeInteger(limit) && limit > 0 ? { limit } : {}),
    ...(contains === "" ? {} : { contains }),
    ...serdeIn(params, BROWSE_PARAM.keySerde, "keySerde"),
    ...serdeIn(params, BROWSE_PARAM.valueSerde, "valueSerde"),
    live: params.get(BROWSE_PARAM.live) === "true",
    ...text(params, BROWSE_PARAM.filterId, "filterId"),
    ...text(params, BROWSE_PARAM.filterSource, "filterSource"),
  };
}

/**
 * The four starts the control offers, as the *kind* the seek select holds.
 *
 * `atOffsets` reports as `offset`, which is what makes a link carrying one show the offset box with
 * the offset in it when every partition agrees — and show it empty when they do not, rather than
 * showing one of them and silently discarding the rest the moment somebody presses Read.
 */
export type SeekKind = "latest" | "beginning" | "offset" | "timestamp";

export function seekKind(seek: SeekMode): SeekKind {
  return seek.kind === "atOffsets" ? "offset" : seek.kind;
}

/** The offset an offset-shaped seek names, for the input box to show. */
export function offsetOf(seek: SeekMode): string | undefined {
  if (seek.kind === "offset") return seek.offset;
  if (seek.kind !== "atOffsets") return undefined;
  // Every partition at the same offset is expressible in one box; a mixture is not.
  const distinct = new Set(seek.offsets.values());
  return distinct.size === 1 ? [...distinct][0] : undefined;
}

/** The instant a timestamp-shaped seek names, as the value `<input type="datetime-local">` wants. */
export function timestampOf(seek: SeekMode): number | undefined {
  return seek.kind === "timestamp" ? seek.epochMillis : undefined;
}

/** Builds a seek from what the controls hold. */
export function seekFor(
  kind: SeekKind,
  offset: string | undefined,
  epochMillis: number | undefined,
): SeekMode {
  switch (kind) {
    case "beginning":
      return { kind: "beginning" };
    case "offset":
      // An empty box means the beginning of the range, not "no seek": the control is showing an
      // offset field, so the user has said they want to start at an offset.
      return { kind: "offset", offset: offset !== undefined && isDigits(offset) ? offset : "0" };
    case "timestamp":
      return { kind: "timestamp", epochMillis: epochMillis ?? 0 };
    case "latest":
      return { kind: "latest" };
  }
}

/**
 * How the partition selector reads: `all 12`, `p 3`, `3 of 12`.
 *
 * Never `0 selected`. An empty selection means every partition — that is what the server means by
 * the parameter being absent — and a control that read "0" for "all" would be a control that says
 * the opposite of what it does.
 *
 * `total` is `undefined` while KUI has not been told how many partitions the topic has, and then the
 * summary says `all partitions` — words, and no figure. It read `all 0` for the whole life of the
 * route that hard-coded a zero: a count nobody had measured, drawn as a quantity, on a control whose
 * menu was empty for the same reason.
 */
export function partitionSummary(selected: readonly number[], total: number | undefined): string {
  if (total === undefined) {
    return selected.length === 0 ? "all partitions" : `${String(selected.length)} chosen`;
  }
  if (selected.length === 0 || selected.length === total) return `all ${String(total)}`;
  if (selected.length === 1) return `p ${String(selected[0])}`;
  return `${String(selected.length)} of ${String(total)}`;
}

// --- The small pieces ----------------------------------------------------------------------------

function splitOnce(value: string): [string, string | undefined] {
  const at = value.indexOf(SEPARATOR);
  if (at < 0) return [value, undefined];
  return [value.slice(0, at), value.slice(at + SEPARATOR.length)];
}

/**
 * Digits and nothing else.
 *
 * Offsets are carried as strings throughout — see `KafkaRecord.offset` in the kernel — because a
 * Kafka offset is a signed 64-bit integer and a JavaScript number holds integers exactly only up to
 * 2^53. Validating with a regular expression rather than by parsing keeps that property: `Number`
 * would accept `1e3`, `0x10` and `9007199254740993`, and hand back something other than what the
 * user typed.
 */
function isDigits(value: string): boolean {
  return /^\d+$/.test(value);
}

function text<K extends string>(
  params: URLSearchParams,
  name: string,
  key: K,
): Record<K, string> | Record<string, never> {
  const raw = (params.get(name) ?? "").trim();
  return raw === "" ? {} : ({ [key]: raw } as Record<K, string>);
}

function serdeIn<K extends string>(
  params: URLSearchParams,
  name: string,
  key: K,
): Record<K, SerdeName> | Record<string, never> {
  const raw = (params.get(name) ?? "").trim();
  const serde = offeredSerde(raw);
  return serde === "" ? {} : ({ [key]: serde } as Record<K, SerdeName>);
}

/**
 * `encodeURIComponent`, spelled out.
 *
 * The built-in leaves `!'()*` unescaped, which is legal in a query string and is not what the
 * server's own encoder produces — and the suite here asserts against the strings the server's
 * suite asserts. Spelling it out makes the two agree character for character.
 */
function encode(raw: string): string {
  return [...raw]
    .map((character) =>
      /[A-Za-z0-9\-_.~]/.test(character) ? character : escapeCharacter(character),
    )
    .join("");
}

function escapeCharacter(character: string): string {
  return [...new TextEncoder().encode(character)]
    .map((byte) => `%${byte.toString(16).toUpperCase().padStart(2, "0")}`)
    .join("");
}
