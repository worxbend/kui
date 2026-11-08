/**
 * The Connect screen's arithmetic and its sentences, with no JSX anywhere near them.
 *
 * Wave 5's adversary measured this split and wave 6's confirmed it: **every rule in a package's
 * pure-data module was gated, and sixteen of seventeen ungated rules were in a `.tsx`.** So every
 * decision this screen makes that could be wrong — which control is offered, what the card says
 * when a figure is missing, whose words a failure reason is in — is a function here, and the
 * components do nothing but render what these answer.
 *
 * One thing this module deliberately does **not** do is count tasks. `runningTasks`, `taskCount`
 * and `failed` are computed by `services/connect` and travel on the wire so that the card, a drawer
 * row and the page's voice line show one number rather than three derivations of it; recomputing
 * them here would be the third opinion `ConnectorDto`'s scaladoc exists to prevent.
 */
import type { ConnectorState, TaskState } from "@kui/kernel";

import type { ConnectorCommand } from "./data.js";
import type { Connector, ConnectorListing, ConnectorRunState, WorkerRow } from "./wire.js";

/**
 * The name a permission question about this connector has to carry.
 *
 * **The Connect cluster's name**, not `<connect>/<connector>`, because that is the question the
 * server asks: `ConnectEndpoints.operating` declares
 * `ResourceRequirement.named(Resource.Connect, connectName, Action.ConnectOperate)`, so the grant
 * that decides a pause is `CONNECT:OPERATE` on the Connect cluster in the path. Asking a different
 * question in the browser gives a control that is enabled and then refused, or disabled for
 * somebody who holds the permission — the two failures a client-side gate exists to prevent.
 *
 * `ConnectorFallbackActions` and the `<connect>/<connector>` spelling that goes with them are for a
 * `Resource.Connector` requirement, which this endpoint deliberately does not declare:
 * `EndpointAuthorization.access` has no `NameSource` that can build the connector-with-parent-
 * fallback access, so a `Resource.Connector` requirement would ask for a grant on `orders-sink`
 * while every grant in the model is spelled `payments/orders-sink`. ADR-054 §3 carries that gap and
 * the seam that would close it; until it closes, **this is the question the server asks and so it
 * is the question the screen asks.**
 */
export function operateSubject(connector: { readonly connect: string }): string {
  return connector.connect;
}

/** How a connector is named in a sentence to a person: the cluster and the connector. */
export function connectorLabel(connector: {
  readonly connect: string;
  readonly name: string;
}): string {
  return `${connector.connect}/${connector.name}`;
}

/** Which of pause and resume this connector's control offers. Restart is always the other one. */
export function toggleOf(state: ConnectorRunState): "pause" | "resume" {
  return state === "PAUSED" ? "resume" : "pause";
}

/**
 * What a connector's card is handed for its two controls: **handlers, or a refusal, never both.**
 *
 * `ConnectorCard` branches on `actionsDisabledReason` and draws a disabled pair when it is there,
 * so a panel that passed `onPause` *alongside* a refusal would look right on every screen and in
 * every case — and would arm itself the day that branch is reworked. The defence against an
 * unpermitted principal pausing a connector would then be living entirely in `Button`, which
 * swallows the click on `aria-disabled` because it chose `aria-disabled` over the attribute, in a
 * package this one does not own.
 *
 * It is a function rather than a ternary inside the panel because **the difference is invisible in
 * the DOM**: nothing a case can select on separates a card that was given a handler from a card
 * that was not, so the only place the rule can be asserted is on the value itself. W7's
 * verification filed exactly that — the case named *"hands an unpermitted principal no working
 * control anywhere on the page"* stayed green with the handlers wired unconditionally, because it
 * reads `aria-disabled` and nothing else.
 */
export type CardActions =
  | { readonly onPause: () => void; readonly onRestart: () => void }
  | { readonly actionsDisabledReason: string };

export function cardActions(options: {
  /** Absent means permitted. Present is the sentence the disabled controls carry. */
  readonly refusal: string | undefined;
  /** Which of pause and resume this connector's state offers. See {@link toggleOf}. */
  readonly toggle: "pause" | "resume";
  readonly onCommand: ((which: ConnectorCommand) => void) | undefined;
}): CardActions {
  const refusal = options.refusal;
  if (refusal !== undefined) return { actionsDisabledReason: refusal };
  return {
    onPause: () => options.onCommand?.(options.toggle),
    onRestart: () => options.onCommand?.("restart"),
  };
}

/**
 * The sentence that completes "You do not have permission to …".
 *
 * Names the **Connect cluster**, because that is what the missing grant is on: telling somebody
 * they need a permission on `payments/orders-sink` when the grant they must ask for is on
 * `payments` sends them to ask for something that does not exist.
 */
export function operateAction(connector: { readonly connect: string }): string {
  return `pause, resume or restart connectors on the Connect cluster '${connector.connect}'`;
}

/* --- What the card is handed ------------------------------------------------------------------ */

/**
 * The state the kernel's `ConnectorCard` draws, from the state the worker reported.
 *
 * `RESTARTING` becomes `UNKNOWN` rather than `RUNNING`: it is not a connector doing work, and the
 * card's own header says drawing an unreported state as `RUNNING` is the worst thing a monitoring
 * screen can do. The same argument covers a state word Connect added after this build shipped.
 */
export function pillState(state: ConnectorRunState): ConnectorState {
  switch (state) {
    case "RUNNING":
    case "FAILED":
    case "PAUSED":
    case "UNASSIGNED":
      return state;
    default:
      return "UNKNOWN";
  }
}

/**
 * One task bar segment, from the state the worker reported for that task.
 *
 * `RESTARTING` and `UNASSIGNED` both become `unknown` rather than `running`, for the same reason
 * {@link pillState} gives: neither is a task doing work, and a green segment mid-restart tells an
 * operator everything is fine at the one moment they are watching to see whether it is. `unknown`
 * draws as an outline, which is the shape that claims nothing.
 */
export function taskSegment(state: ConnectorRunState): TaskState {
  switch (state) {
    case "RUNNING":
      return "running";
    case "FAILED":
      return "failed";
    case "PAUSED":
      return "paused";
    default:
      return "unknown";
  }
}

/**
 * The task bar's segments, one per task the worker described.
 *
 * The **length** is `tasks.length` and not `taskCount`: the bar is a picture of the tasks KUI was
 * told about, and padding it out to a count would draw segments for tasks whose state nobody knows
 * as though the absence were a state. Where the two disagree, `ConnectorPanel` says so in words.
 */
export function segmentsOf(connector: Connector): readonly TaskState[] {
  return connector.tasks.map((task) => taskSegment(task.state));
}

/*
 * There was a `taskCaption()` here — `3/3 tasks`, from the service's own two figures — and it had
 * no product caller. `ConnectorPanel`'s own `taskSentence` says the same two numbers in words and
 * says the gap between `tasks.length` and `taskCount` as well, which a caption cannot; the kernel's
 * `ConnectorCard` draws its own summary from the segments. A third phrasing of one pair of figures,
 * reachable only from a case, is a fourth chance to disagree with the domain about what `running`
 * means. Deleted in wave 8 rather than given a caller.
 */

/* --- The failure reason, which is the whole of `SCREENS-V4.md` §7.7 --------------------------- */

export interface FailureReason {
  /** The worker's own line, as the service sliced it. Never a sentence KUI composed. */
  readonly line: string;
  /** Which task reported it, when a task did. `undefined` when the connector itself carried it. */
  readonly task: number | undefined;
  /** Whether there is more of the trace than the line above. */
  readonly hasTrace: boolean;
}

/**
 * Why this connector is failing, in the worker's words, or `undefined` when it is not failing.
 *
 * §7.7: *"`M19`'s failed card carries a state and a task count and nothing else, while the
 * notification for the same event carries `Task 0: connection refused to es-01:9200`. Either the
 * card links to a connector detail page that does not exist in any capture, or it carries the first
 * line of the trace."* This is the second.
 *
 * The connector's own `reason` wins, because the service already picked between the connector's
 * line and its first failed task's; the task is looked up only to say **which** one reported it,
 * which is how an operator finds the rest of the trace in the worker's log.
 */
export function failureReason(connector: Connector): FailureReason | undefined {
  if (!connector.failed) return undefined;
  if (connector.reason === undefined) return undefined;

  const owner = connector.tasks.find((task) => task.reason === connector.reason);
  return {
    line: connector.reason,
    task: owner?.id,
    hasTrace: (owner?.trace ?? "").trim() !== "",
  };
}

/**
 * What a failed connector says when the worker sent no reason at all.
 *
 * Stated rather than left blank. A failed card with an empty reason area reads as "KUI knows and
 * will not say", and the true fact — a failure reported with no reason — is itself something an
 * operator needs, because it points at the worker's log rather than at KUI.
 */
export const NO_REASON_REPORTED =
  "The worker reported this connector as failed and sent no reason with it. The connector's own " +
  "task log on the Connect worker is where the trace will be.";

/**
 * What is said about a connector the worker named and refused to describe.
 *
 * `ConnectorsDto.unreadable` exists so this row can be drawn at all: a connector missing from a
 * list is indistinguishable from a connector that was deleted, and `GET /connectors?expand=status`
 * is answered per connector by the worker that owns it — so one wedged worker loses the status of
 * its share while the rest answer normally. The row names it and claims nothing else.
 */
export const NOT_DESCRIBED =
  "The Connect cluster named this connector and would not describe it, so KUI cannot say what " +
  "state " +
  "it is in or how many tasks it has. It has not been deleted.";

/* --- The empty and unmeasured states, none of which is a zero --------------------------------- */

/** A worker answered and named no connectors. A fact about the worker, never about this decoder. */
export const NO_CONNECTORS =
  "The Connect workers answered and named no connectors. Nothing is deployed on them yet.";

/** No Connect worker is configured for this cluster. Nothing is broken; ADR-032's ordinary case. */
export const NOT_CONFIGURED =
  "No Kafka Connect worker is configured for this cluster, so there is nothing to list. Add one " +
  "under kui.clusters.<n>.connect in KUI's configuration.";

/**
 * What the throughput slot says, on every card, always.
 *
 * The Connect REST API publishes no per-connector record rate at all (ADR-054 §6), so this is not a
 * fallback for a missing figure — it is the only thing there is to say. §3.14 draws `1,204 msg/s`
 * and its own *Absent* paragraph is the rule being kept: a literal `0 msg/s` on a paused connector
 * is a measured zero, and an unmeasured one must never look like it.
 *
 * **It is the second copy of a sentence the kernel owns.** `ConnectorCard.tsx` renders the literal
 * itself, because this package hands the card no `throughput` prop and the card decides what to say
 * about that; nothing here can pass the sentence in. So this constant has no product caller and
 * cannot have one — what it has instead is `connect.test.tsx`, which asserts the **card's rendered
 * text against this value** rather than against a literal of its own. Two copies compared beat two
 * copies drifting, and this is the only join available across a package boundary a feature may not
 * cross.
 */
export const THROUGHPUT_NOT_MEASURED = "throughput not measured";

/* --- The voice line --------------------------------------------------------------------------- */

/** Every connector across every worker that answered, in configuration order. */
export function allConnectors(listing: ConnectorListing): readonly Connector[] {
  return listing.workers.flatMap((worker) => pageOf(worker)?.items ?? []);
}

/** Every connector a worker named and would not describe, with the worker it is on. */
export function allNotDescribed(
  listing: ConnectorListing,
): readonly { readonly connect: string; readonly name: string }[] {
  return listing.workers.flatMap((worker) =>
    (pageOf(worker)?.unreadable ?? []).map((name) => ({ connect: worker.connect, name })),
  );
}

/** Whether any worker failed to answer, so the list on screen may be short. */
export function workersThatDidNotAnswer(listing: ConnectorListing): readonly WorkerRow[] {
  return listing.workers.filter(
    (worker) => worker.page.kind !== "ok" && worker.page.kind !== "stale",
  );
}

function pageOf(worker: WorkerRow) {
  return worker.page.kind === "ok" || worker.page.kind === "stale" ? worker.page.page : undefined;
}

/**
 * The sentence under the page title.
 *
 * `SCREENS-V4.md` §4.14 draws `4 connectors · 1 failed and sulking`. Both halves are counted from
 * the rows this browser is holding, which is the only figure on this screen the browser is allowed
 * to compute: it is a count of rows, not an answer to a question the API was asked. The failure
 * count reads each connector's `failed` flag — **not** its state word — because the golden document
 * has a connector whose own state is `RUNNING` and whose task 1 has failed, and a voice line that
 * counted state words would call that cluster healthy.
 *
 * "All running" is the other direction of that lie. It is used only when every described connector's
 * state is `RUNNING`. `PAUSED`, `UNASSIGNED`, `RESTARTING` and `UNKNOWN` are not a connector doing
 * work, and {@link pillState} already refuses to draw them as running. They are counted by the state
 * word only after the `failed` flag has been applied, so a running connector with a failed task is
 * still "failed and sulking" and not "1 paused".
 *
 * A worker that named connectors and would not describe them did deploy something. The zero-item
 * branch says those names rather than "nothing is deployed", which is what the empty grid says when
 * `unreadable` is empty. Where a worker did not answer, the line says the count is partial rather
 * than stating it flat: a confident "3 connectors" over a cluster whose second worker is down is a
 * smaller number in the reassuring direction.
 */
export function connectVoice(listing: ConnectorListing): string {
  const connectors = allConnectors(listing);
  const silent = workersThatDidNotAnswer(listing).length;
  const undescribed = allNotDescribed(listing);

  if (connectors.length === 0) {
    if (undescribed.length > 0) return nothingDescribed(undescribed, silent);
    return silent > 0
      ? "No connectors, from the workers that answered — and some did not."
      : "No connectors are deployed. A quiet pipeline is a happy pipeline.";
  }

  const noun = connectors.length === 1 ? "1 connector" : `${connectors.length} connectors`;
  const partial = silent > 0 ? ", from the workers that answered" : "";
  const failed = connectors.filter((one) => one.failed).length;
  if (failed > 0) return `${noun}${partial} · ${failed} failed and sulking`;
  if (undescribed.length > 0) {
    const word =
      undescribed.length === 1
        ? "one more it would not describe"
        : `${undescribed.length} more it would not describe`;
    return `${noun}${partial} · and ${word}`;
  }
  const resting = nonRunningPhrase(connectors);
  if (resting !== undefined) return `${noun}${partial} · ${resting}`;
  return `${noun}${partial} · all running, which is how it should look.`;
}

/**
 * What to say when every worker that answered named connectors and described none of them.
 *
 * The names are the fact the cards below repeat. Leaving them out of this sentence is how the
 * header used to say nothing was deployed above a row of "would not describe" cards.
 */
function nothingDescribed(
  undescribed: readonly { readonly connect: string; readonly name: string }[],
  silent: number,
): string {
  const list = quotedList(undescribed.map((one) => one.name));
  const who =
    new Set(undescribed.map((one) => one.connect)).size === 1 ? "The worker" : "The workers";
  const named =
    undescribed.length === 1
      ? `${who} named ${list} and would not describe it`
      : `${who} named ${list} and would not describe them`;
  const base = `No connectors were described. ${named}.`;
  return silent > 0 ? `${base} Some workers did not answer.` : base;
}

/** `'a'`, `'a' and 'b'`, or `'a', 'b', and 'c'`. */
function quotedList(names: readonly string[]): string {
  const quoted = names.map((name) => `'${name}'`);
  const [first, second] = quoted;
  if (quoted.length === 1) return first ?? "";
  if (quoted.length === 2) return `${first} and ${second}`;
  const last = quoted[quoted.length - 1];
  return `${quoted.slice(0, -1).join(", ")}, and ${last}`;
}

/**
 * The non-`RUNNING` states on this page, or `undefined` when every connector is running.
 *
 * Order is fixed so two clusters in the same mix read the same sentence. A state this build does
 * not name still counts: dropping it would fall through to "all running".
 */
function nonRunningPhrase(connectors: readonly Connector[]): string | undefined {
  const counts = new Map<string, number>();
  for (const connector of connectors) {
    const word = healthWord(connector.state);
    if (word === undefined) continue;
    counts.set(word, (counts.get(word) ?? 0) + 1);
  }
  if (counts.size === 0) return undefined;
  const order = ["paused", "unassigned", "restarting", "failed", "unknown"];
  const known = order.filter((word) => counts.has(word));
  const extra = [...counts.keys()].filter((word) => !order.includes(word));
  return [...known, ...extra].map((word) => `${counts.get(word)} ${word}`).join(", ");
}

/** The voice-line word for a state, or `undefined` when that state is `RUNNING`. */
function healthWord(state: ConnectorRunState): string | undefined {
  switch (state) {
    case "RUNNING":
      return undefined;
    case "PAUSED":
      return "paused";
    case "UNASSIGNED":
      return "unassigned";
    case "RESTARTING":
      return "restarting";
    case "FAILED":
      return "failed";
    case "UNKNOWN":
      return "unknown";
    default: {
      const unexpected: never = state;
      return String(unexpected).toLowerCase();
    }
  }
}
