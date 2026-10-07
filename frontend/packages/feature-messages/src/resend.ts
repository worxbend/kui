/**
 * Copying a range of records into another topic.
 *
 * ## What a resend is, in the words the contract uses
 *
 * The records are re-written **byte for byte, headers included, and are never deserialized**. There
 * is no transform and no option to add or drop a header. That is the property the operation exists
 * for — a topic KUI cannot decode can still be replayed, which is the case a resend is most often
 * needed for — and it is also the property that makes the destination indistinguishable from the
 * original producer's output. Anyone reading the destination sees records the original producer
 * wrote, with no marking of any kind to say KUI put them there. An operator confirming a resend has
 * to be told that, because it is the part they cannot infer from a form with two offsets in it.
 *
 * ## It appends. It is not atomic. It has no undo.
 *
 * The source is untouched, so the contract marks it `destructive = false` and it genuinely is not
 * one — nothing is taken away. But `destructive = false` is not the same as harmless: what it *can*
 * do is append a great deal to a topic other people are consuming, and cancelled or failed halfway
 * it leaves what it already wrote. There is no plan endpoint and no token, because the operator can
 * see the whole of what the operation will do from the range and the destination they typed. What
 * they cannot see is the three sentences above, so {@link RESEND_WARNINGS} carries them onto the
 * confirmation.
 *
 * ## `read` and `written` are two numbers because they differ
 *
 * The answer is a receipt, not an echo. `read` is how many records came out of the source and
 * `written` is how many went into the destination, and they come apart whenever retention removed
 * part of the source under the copy. Reporting only `written` would make a resend that copied
 * nothing — because the offsets named had aged out of the log — look exactly like one that had
 * nothing to copy and succeeded.
 *
 * Against a real cluster, a range naming offsets that are no longer in the log answers **200**:
 *
 * ```
 * {"toTopic":"scratch.jm-test","read":0,"written":0}
 * ```
 *
 * No error, no warning, nothing. That is the single most important state on this screen: a silent,
 * successful, empty copy. {@link resendOutcome} compares what was asked for against what happened so
 * that the dialog can say "nothing was copied" in those words, and the never-zero rule applies with
 * full force — 0 records copied is a fact and must be drawn as the figure `0`, never as a blank or
 * an em dash, and never as a green tick.
 *
 * ## The cap the schema does not mention
 *
 * `docs/api/openapi.browser.json` says nothing about a limit. The service refuses anything larger
 * than {@link MAX_RESEND_RECORDS} and names the number in the refusal:
 *
 * ```
 * a resend may copy at most 10000 records at a time, and this range holds 900000000;
 * copy it in several ranges
 * ```
 *
 * It is checked here too, before the request, so the operator finds out while they are still holding
 * the field they need to change. The server's refusal remains the authority; this is an editor
 * courtesy and never a substitute for it.
 */

import type { ApiResult, KuiApiClient, components } from "@kui/api";

/**
 * A half-open window `[from, until)` of one partition.
 *
 * Half-open and named so, because every off-by-one here is a disagreement about whether the last
 * offset is included. It is not: `until` is the offset of the first record **not** copied, the same
 * convention Kafka's own end offsets use. The dialog says this in words next to the fields, because
 * "0 to 3" reads as four records to most people and copies three.
 *
 * The offsets are text and not numbers. A field that reparsed on every keystroke would erase a
 * half-typed `1` the moment it became `1_`, and this is the same decision the seek control in
 * `MessageFilterBar` makes for the same reason.
 */
export interface ResendRange {
  readonly partition: number;
  readonly from: string;
  readonly until: string;
}

export interface ResendDraft {
  /** The destination. It may be the source topic; replaying a topic into itself is a real operation. */
  readonly toTopic: string;
  readonly ranges: readonly ResendRange[];
}

/** The service's per-request ceiling. See the header: it is in no schema, only in the refusal. */
export const MAX_RESEND_RECORDS = 10_000;

/**
 * What the confirmation must say before an operator agrees to this.
 *
 * These are the contract's own statements about the operation, not sentences composed to sound
 * cautious. Each one is something the operator cannot work out from the form in front of them, and
 * each one has a consequence they would otherwise discover afterwards.
 */
export const RESEND_WARNINGS: readonly { readonly code: string; readonly message: string }[] = [
  {
    code: "RESEND-ORIGINAL-BYTES",
    message:
      "The destination gets the producer's original bytes, headers included. Nothing marks these " +
      "records as copies, so consumers of the destination cannot tell them from records the " +
      "original producer wrote.",
  },
  {
    code: "RESEND-APPENDS",
    message:
      "This appends to the destination and changes nothing in the source. Records already in the " +
      "destination stay where they are; these are added after them.",
  },
  {
    code: "RESEND-NOT-ATOMIC",
    message:
      "It is not atomic. If it fails or is cancelled halfway, the records it had already written " +
      "stay written, and there is no undo.",
  },
  {
    code: "RESEND-RETENTION",
    message:
      "Retention may have removed part of the range since you chose it. The answer reports how " +
      "many records were read and how many were written, and those numbers differ when it has.",
  },
];

/** How many records a range names, or `undefined` when its offsets are not both readable yet. */
export function rangeSize(range: ResendRange): number | undefined {
  const from = Number(range.from);
  const until = Number(range.until);
  if (range.from.trim() === "" || range.until.trim() === "") return undefined;
  if (!Number.isSafeInteger(from) || !Number.isSafeInteger(until)) return undefined;
  if (from < 0 || until < from) return undefined;
  return until - from;
}

/**
 * How many records the whole draft names, or `undefined` if any range is not yet readable.
 *
 * `undefined` and `0` are different answers and the dialog draws them differently: nought records
 * is a range the operator has typed backwards or emptied, which is a fact worth stating, and
 * "not known" is a half-typed offset, which is not an error yet.
 */
export function draftSize(draft: ResendDraft): number | undefined {
  let total = 0;
  for (const range of draft.ranges) {
    const size = rangeSize(range);
    if (size === undefined) return undefined;
    total += size;
  }
  return total;
}

/**
 * Why this draft cannot be sent, or `undefined`.
 *
 * Every one of these is a refusal the server would issue anyway, moved to where the operator can
 * still fix it. The wordings deliberately echo the server's, so that meeting the real one later does
 * not read as a second, unrelated problem.
 */
export function resendDraftProblem(draft: ResendDraft): string | undefined {
  if (draft.toTopic.trim() === "") return "Name the topic to copy these records into.";
  // The server's own words: "a resend names no offsets, so there is nothing to copy".
  if (draft.ranges.length === 0) return "Add at least one partition range; there is nothing to copy.";

  for (const range of draft.ranges) {
    if (range.from.trim() === "" || range.until.trim() === "") {
      return `Partition ${String(range.partition)} needs both a first and a last offset.`;
    }
    const size = rangeSize(range);
    if (size === undefined) {
      return `Partition ${String(range.partition)}'s range runs backwards: the second offset must not be below the first.`;
    }
  }

  const total = draftSize(draft);
  if (total === 0) {
    return "This range holds no records: `until` is the first offset that is not copied, so it must be above `from`.";
  }
  if (total !== undefined && total > MAX_RESEND_RECORDS) {
    return (
      `A resend may copy at most ${MAX_RESEND_RECORDS.toLocaleString()} records at a time, and ` +
      `this names ${total.toLocaleString()}. Copy it in several ranges.`
    );
  }
  return undefined;
}

/**
 * A finished resend: the server's tally, and what it means against what was asked for.
 *
 * `requested` is not on the wire. It is the client's own arithmetic over the ranges it sent, and it
 * is kept because `read` alone cannot distinguish "the log held fewer records than you named" from
 * "you named exactly this many and got them all". The dialog needs that distinction to choose
 * between a receipt and a warning.
 */
export interface ResendOutcome {
  readonly toTopic: string;
  readonly read: number;
  readonly written: number;
  readonly failures?: components["schemas"]["ResendResultDto"]["failures"];
  readonly rangeFailures?: components["schemas"]["ResendResultDto"]["rangeFailures"];
  /** How many the ranges named, when that was knowable. */
  readonly requested?: number | undefined;
}

/** How a finished resend should be read. */
export type ResendReading =
  /** Explicit record or range failures outrank counts, including equal or zero counts. */
  | { readonly kind: "incomplete" }
  /** Everything named was read and written. */
  | { readonly kind: "complete" }
  /** Nothing at all was copied — a 200 that copied no record. Never drawn as success. */
  | { readonly kind: "nothing"; readonly requested?: number | undefined }
  /** Fewer records existed than were named: retention had removed part of the source. */
  | { readonly kind: "short"; readonly missing: number }
  /** Records were read but not all of them landed: the copy failed part-way. */
  | { readonly kind: "partial"; readonly lost: number };

/**
 * Which reading this receipt supports.
 *
 * Explicit failures outrank all counts: a failed range can coexist with equal read/write counts,
 * or with zeroes, neither of which proves retention or success. Without reported failures, keep
 * empty copies, partial writes, short reads, and complete copies distinct.
 */
export function readingOf(outcome: ResendOutcome): ResendReading {
  if ((outcome.failures?.length ?? 0) > 0 || (outcome.rangeFailures?.length ?? 0) > 0) {
    return { kind: "incomplete" };
  }
  if (outcome.read === 0 && outcome.written === 0) {
    return {
      kind: "nothing",
      ...(outcome.requested === undefined ? {} : { requested: outcome.requested }),
    };
  }
  if (outcome.written < outcome.read) return { kind: "partial", lost: outcome.read - outcome.written };
  if (outcome.requested !== undefined && outcome.read < outcome.requested) {
    return { kind: "short", missing: outcome.requested - outcome.read };
  }
  return { kind: "complete" };
}

export async function resend(
  api: KuiApiClient,
  clusterId: string,
  topicName: string,
  draft: ResendDraft,
): Promise<ApiResult<ResendOutcome>> {
  const answer = await api.post(
    "/api/v1/clusters/{clusterId}/topics/{topicName}/messages/resend",
    {
      params: { path: { clusterId, topicName } },
      body: {
        toTopic: draft.toTopic,
        /* `ranges` is sent always, even though the generated type marks it optional. It is not:
         * the server's decoder requires the field and answers KUI-VALIDATION without it. This is
         * the schema and the decoder disagreeing, and the decoder is what runs. */
        ranges: draft.ranges.map((range) => ({
          partition: range.partition,
          /* Offsets cross this boundary as JSON numbers because the contract says `int64`, which
           * has no JavaScript representation. The loss is real above 2^53 and it is the same seam
           * `wire.ts` documents on the way in; converting here at least keeps it to one place. */
          from: Number(range.from),
          until: Number(range.until),
        })),
      },
    },
  );
  if (!answer.ok) return answer;
  const requested = draftSize(draft);
  return {
    ok: true,
    value: {
      toTopic: answer.value.toTopic,
      read: answer.value.read,
      written: answer.value.written,
      failures: Array.from(answer.value.failures ?? []),
      rangeFailures: Array.from(answer.value.rangeFailures ?? []),
      ...(requested === undefined ? {} : { requested }),
    },
  };
}
