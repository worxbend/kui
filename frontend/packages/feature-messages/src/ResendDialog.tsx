/**
 * Confirming a copy of a range of records into another topic.
 *
 * ## Why the warnings are the server's semantics and not a scare sentence
 *
 * A resend takes nothing away, so it carries no plan and no token: an operator can see the whole of
 * what it will do from the destination and the range in front of them. What they cannot see is what
 * the copy *is*, and the contract is specific about three things that all have consequences after
 * the fact:
 *
 * - the destination gets the producer's original bytes, headers included, unmarked — nobody reading
 *   the destination can tell these records from ones the original producer wrote;
 * - it is not atomic, so a failure halfway leaves what it already wrote, with no undo;
 * - retention may have removed part of the source since the range was chosen, and `read` and
 *   `written` come apart exactly when it has.
 *
 * Those are in {@link RESEND_WARNINGS}, written from the contract, and they are listed rather than
 * summarised. "This cannot be undone" is a sentence every operator has clicked past; "consumers of
 * the destination cannot tell these from records the original producer wrote" is one they read,
 * because it is about something they will have to explain to somebody.
 *
 * ## The receipt is the point, and a copy of nothing is not a success
 *
 * A resend that names offsets retention has already removed answers **200 with `read: 0,
 * written: 0`** — no error, no warning. Reporting that as "Copied" with a tick would be the single
 * most misleading thing this screen could do, because the operator's next action is to go and look
 * at a destination they believe now holds their records.
 *
 * So the answer is read through {@link readingOf} into distinct panels, and the figures
 * are always drawn as figures. **0 is a fact.** It renders as `0`, never as a blank and never as an
 * em dash — the never-zero rule in the direction it is usually forgotten: a zero that is known is
 * not the same as a number nobody could read, and here the zero is the whole message.
 *
 * ## Typing the destination
 *
 * Asked for, like every irreversible action in this product. The test is undo-ability rather than
 * destruction: a resend destroys nothing and still cannot be taken back, and — unlike a purge — it
 * can be aimed at the *wrong topic*, which is a mistake nothing on the confirmation would otherwise
 * catch. Typing the destination's name is the one mechanism muscle memory cannot get past.
 */
import { For, Show, createEffect, createMemo, createSignal, untrack } from "solid-js";
import type { JSX } from "@solidjs/web";
import { Banner, Button, Dialog, Icon, TextField, type Mutation } from "@kui/kernel";

import {
  MAX_RESEND_RECORDS,
  RESEND_WARNINGS,
  draftSize,
  rangeSize,
  readingOf,
  resendDraftProblem,
  type ResendDraft,
  type ResendOutcome,
  type ResendRange,
} from "./resend.js";

export interface ResendDialogProps {
  readonly open: boolean;
  readonly onClose: () => void;
  /** The topic the records are read from. */
  readonly topic: string;
  /**
   * How many partitions the source has, or `undefined` when KUI has not been told.
   *
   * Optional, and this is the field that made it worth being careful. It was a required `number`,
   * the route above supplied a hard-coded `0`, and the sentence below therefore read
   * "`orders.payments.v2` has 0 partitions, and there is a range for each of them" on every copy
   * dialog this product has drawn — a false statement about somebody's cluster, sitting beside a
   * control that copies records between topics, and disabling that control as a side effect.
   *
   * `undefined` is not a smaller number. It means the dialog may not bound the range list and may
   * not say anything about how many partitions there are; it says *that* instead.
   */
  readonly partitionCount?: number | undefined;
  /** The range to open with — the selection on screen, when the screen has one. */
  readonly initial?: ResendDraft | undefined;
  readonly onSend: (draft: ResendDraft) => void;
  readonly state: Mutation<ResendOutcome>;
  readonly destinationBlocked?: ((topic: string) => string | undefined) | undefined;
}

/** One empty range on partition 0: a form with no rows at all reads as a form that is still loading. */
const EMPTY_DRAFT: ResendDraft = { toTopic: "", ranges: [{ partition: 0, from: "", until: "" }] };

export function ResendDialog(props: ResendDialogProps): JSX.Element {
  /* Seeded on *open*, not on mount, for the reason `SmartFilterDialog` gives: this dialog is
   * mounted with the screen and opened much later, so a draft taken at mount would be the selection
   * as it stood when the page loaded. */
  const [draft, setDraft] = createSignal<ResendDraft>(untrack(() => props.initial ?? EMPTY_DRAFT));
  const [typed, setTyped] = createSignal("");

  createEffect(
    () => props.open,
    (open) => {
      if (!open) return;
      setDraft(untrack(() => props.initial ?? EMPTY_DRAFT));
      /* The confirmation is retyped every time. Carrying it over would mean an operator who opened
       * this dialog, changed the destination and pressed the button confirmed a topic name they
       * typed for a different one. */
      setTyped("");
    },
  );

  /**
   * The partition count as a *value* the sentence below is drawn from, or nothing.
   *
   * Wrapped in an object rather than handed over bare so that the narrowing is structural: `Show`
   * renders the child only when this answers, and the child is given the number, so the sentence
   * cannot be composed at all without one. Read straight off `props.partitionCount` inside a
   * `<Show when={props.partitionCount !== undefined}>`, the compiler still saw `number |
   * undefined` there, and the `?? 0` that satisfied it was a pre-written "has 0 partitions" —
   * unreachable only for as long as the sibling condition and the fallback stayed in step. That
   * sentence is the exact falsehood this dialog's own header, this file's tests and an end-to-end
   * assertion all exist to prevent, so it is not left one edit away from being said.
   *
   * Bare `when={props.partitionCount}` would not do it either: a count of `0` is falsy, so a
   * server that did say zero would be redrawn as a server that said nothing.
   */
  const measured = createMemo<{ readonly count: number } | undefined>(() =>
    props.partitionCount === undefined ? undefined : { count: props.partitionCount },
  );

  const problem = createMemo(() => resendDraftProblem(draft()));
  const busy = () => props.state.kind === "running";
  const total = createMemo(() => draftSize(draft()));

  const outcome = (): ResendOutcome | undefined =>
    props.state.kind === "done" ? props.state.value : undefined;

  const failure = (): { readonly message: string; readonly code?: string } | undefined => {
    if (props.state.kind === "failed") return { message: props.state.message, code: props.state.code };
    if (props.state.kind === "forbidden") return { message: props.state.message };
    return undefined;
  };

  /** Why the copy cannot be started. Every branch is a sentence, because the button demands one. */
  const blockedReason = (): string | undefined => {
    if (busy()) return "The copy is running.";
    const denied = props.destinationBlocked?.(draft().toTopic);
    if (denied !== undefined) return denied;
    const stated = problem();
    if (stated !== undefined) return stated;
    if (typed() !== draft().toTopic) {
      return `Type ${draft().toTopic} to confirm the destination.`;
    }
    return undefined;
  };

  const patchRange = (index: number, change: Partial<ResendRange>): void => {
    setDraft({
      ...draft(),
      ranges: draft().ranges.map((range, at) => (at === index ? { ...range, ...change } : range)),
    });
  };

  return (
    <Dialog
      open={props.open}
      onClose={props.onClose}
      title={`Copy records out of ${props.topic}`}
      description="The records are copied into another topic. Nothing in this topic is changed."
      size="lg"
      closeOnScrimClick={false}
      testId="resend-dialog"
      actions={
        <>
          <Button variant="secondary" onClick={props.onClose}>
            {outcome() === undefined ? "Cancel" : "Close"}
          </Button>
          {/* Once the copy has happened the button is gone rather than disabled: it has run, and a
              second press would copy the same records a second time. There is nothing to retry. */}
          <Show when={outcome() === undefined}>
            <Button
              variant="primary"
              icon="refresh"
              busy={busy()}
              {...disabledProps(blockedReason())}
              onClick={() => props.onSend(draft())}
            >
              Copy records
            </Button>
          </Show>
        </>
      }
    >
      <div class="kui-resend">
        <Show
          when={outcome()}
          fallback={
            <>
              <TextField
                label="Copy into topic"
                value={draft().toTopic}
                placeholder="orders.v1.replay"
                help="It may be this same topic; replaying a topic into itself is a real operation."
                onInput={(value) => setDraft({ ...draft(), toTopic: value })}
              />

              <section class="kui-resend__ranges" aria-label="Which records to copy">
                <h3 class="kui-resend__heading">Which records</h3>
                {/* Said in words rather than left to the field names. "0 to 3" reads as four
                    records to most people and copies three, and that off-by-one is the one this
                    form would otherwise ship. */}
                <p class="kui-resend__note">
                  Each range is <strong>from</strong> the first offset copied,{" "}
                  <strong>until</strong> the first offset that is <em>not</em> copied — so 0 until 3
                  copies three records: 0, 1 and 2.
                </p>

                <For each={draft().ranges}>
                  {(range, index) => (
                    <div class="kui-resend__range">
                      <TextField
                        label="Partition"
                        size="sm"
                        mono
                        value={String(range.partition)}
                        onInput={(value) => {
                          const digits = value.replace(/\D/g, "");
                          patchRange(index(), { partition: digits === "" ? 0 : Number(digits) });
                        }}
                      />
                      <TextField
                        label="From offset"
                        size="sm"
                        mono
                        placeholder="0"
                        value={range.from}
                        /* Digits only, filtered rather than rejected — an offset is a 64-bit
                           integer carried as text everywhere in this frontend, and a box that
                           accepted `1e6` would send the server something it has to refuse. */
                        onInput={(value) => patchRange(index(), { from: value.replace(/\D/g, "") })}
                      />
                      <TextField
                        label="Until offset"
                        size="sm"
                        mono
                        placeholder="0"
                        value={range.until}
                        onInput={(value) => patchRange(index(), { until: value.replace(/\D/g, "") })}
                      />
                      <span class="kui-resend__range-size">{describeSize(rangeSize(range))}</span>
                      <Show when={draft().ranges.length > 1}>
                        <Button
                          variant="ghost"
                          size="sm"
                          icon="close"
                          iconOnly
                          onClick={() =>
                            setDraft({
                              ...draft(),
                              ranges: draft().ranges.filter((_, at) => at !== index()),
                            })
                          }
                        >
                          {`Remove the range on partition ${String(range.partition)}`}
                        </Button>
                      </Show>
                    </div>
                  )}
                </For>

                <div class="kui-resend__range-actions">
                  <Button
                    variant="secondary"
                    size="sm"
                    icon="plus"
                    {...disabledProps(
                      props.partitionCount !== undefined &&
                        draft().ranges.length >= props.partitionCount
                        ? `${props.topic} ${describePartitions(props.partitionCount)}, ` +
                          "and there is a range for each of them."
                        : undefined,
                    )}
                    onClick={() =>
                      setDraft({
                        ...draft(),
                        ranges: [
                          ...draft().ranges,
                          { partition: draft().ranges.length, from: "", until: "" },
                        ],
                      })
                    }
                  >
                    Add a partition
                  </Button>
                  <span class="kui-resend__total">{describeTotal(total())}</span>
                </div>

                {/* How many partitions there are to write ranges for, or a sentence saying nobody
                    has measured it. Both are said out loud, because an operator writing ranges
                    needs to know whether they have covered the topic — and the thing this dialog
                    shipped instead was worse than silence: a hard-coded zero, stated as a fact,
                    with the control that adds a range disabled to match it. */}
                <Show
                  when={measured()}
                  fallback={
                    <p class="kui-resend__unknown">
                      KUI has not been told how many partitions {props.topic} has, so it cannot say
                      when the ranges cover all of them. The server checks every range and refuses
                      one that names a partition the topic does not have.
                    </p>
                  }
                >
                  {/* `__known`, not `__unknown`. The two sentences are opposite statements and they
                      were sharing a class, so the styling that says "this is a gap" was applied to
                      the one that states a measured figure. */}
                  {(known) => (
                    <p class="kui-resend__known">
                      {props.topic} {describePartitions(known().count)}.
                    </p>
                  )}
                </Show>
              </section>

              <section class="kui-resend__warnings" aria-label="What a copy does">
                <h3 class="kui-resend__heading">Before you copy</h3>
                <ul class="kui-resend__warning-list">
                  <For each={RESEND_WARNINGS}>
                    {(warning) => (
                      <li class="kui-resend__warning">
                        <Icon name="warning" size="14px" class="kui-resend__warning-glyph" />
                        <span>{warning.message}</span>
                      </li>
                    )}
                  </For>
                </ul>
              </section>

              <Show when={problem()}>
                {(stated) => <p class="kui-resend__problem">{stated()}</p>}
              </Show>

              {/* `help` carries the reason this box is dead, because `TextField` has no
                  `disabledReason` of its own the way `Button` does — and a disabled control still
                  owes the operator an explanation. */}
              <TextField
                label={`Type ${draft().toTopic === "" ? "the destination topic's name" : draft().toTopic} to confirm`}
                value={typed()}
                mono
                disabled={draft().toTopic === ""}
                help={
                  draft().toTopic === ""
                    ? "Name the destination topic first; then type it here to confirm."
                    : "A resend cannot be undone, and it can be aimed at the wrong topic. Typing the name is what stops that."
                }
                onInput={(value) => setTyped(value)}
              />
            </>
          }
        >
          {(done) => <Receipt outcome={done()} topic={props.topic} />}
        </Show>

        <Show when={failure()}>
          {(problem) => (
            <Banner
              tone="danger"
              message={`The copy did not run: ${problem().message}`}
              {...(problem().code === undefined ? {} : { code: problem().code })}
            />
          )}
        </Show>
      </div>
    </Dialog>
  );
}

/**
 * What actually happened, in the two figures the server sent.
 *
 * Both are always shown, side by side, whatever the reading — because the *pair* is the fact. A
 * receipt that showed only "written" would be unable to express a copy whose source had shrunk
 * underneath it, which is the case the two numbers exist for.
 */
function Receipt(props: {
  readonly outcome: ResendOutcome;
  /** The source topic, named so the two columns are "out of X" and "into Y" rather than two counts. */
  readonly topic: string;
}): JSX.Element {
  const reading = createMemo(() => readingOf(props.outcome));

  return (
    <div class={["kui-resend__receipt", `kui-resend__receipt--${reading().kind}`]} role="status">
      <p class="kui-resend__receipt-headline">
        <Icon
          name={reading().kind === "complete" ? "check" : "warning"}
          size="16px"
          class="kui-resend__receipt-glyph"
        />
        {headlineOf(reading().kind)}
      </p>

      <dl class="kui-resend__tally">
        <div>
          <dt>Read from {props.topic}</dt>
          {/* `.toLocaleString()` on the number itself: a zero here is a measurement and prints as
              `0`. It must never be blanked or dashed — an absent figure and a figure of nought are
              different facts and this screen turns on telling them apart. */}
          <dd class="kui-resend__figure">{props.outcome.read.toLocaleString()}</dd>
        </div>
        <div>
          <dt>Written to {props.outcome.toTopic}</dt>
          <dd class="kui-resend__figure">{props.outcome.written.toLocaleString()}</dd>
        </div>
        <Show when={props.outcome.requested}>
          {(asked) => (
            <div>
              <dt>Named by your ranges</dt>
              <dd class="kui-resend__figure">{asked().toLocaleString()}</dd>
            </div>
          )}
        </Show>
      </dl>

      <p class="kui-resend__receipt-detail">{detailOf(reading(), props.outcome)}</p>
      <Show when={(props.outcome.failures?.length ?? 0) > 0}>
        <section aria-label="Failed source records">
          <h3 class="kui-resend__heading">Failed source records</h3>
          <ul>
            <For each={props.outcome.failures}>
              {(failure) => <li>
                Partition {failure.partition}, offset {failure.offset}: {failure.error} <code>{failure.code}</code>
              </li>}
            </For>
          </ul>
        </section>
      </Show>
      <Show when={(props.outcome.rangeFailures?.length ?? 0) > 0}>
        <section aria-label="Incomplete source ranges">
          <h3 class="kui-resend__heading">Incomplete source ranges</h3>
          <ul>
            <For each={props.outcome.rangeFailures}>
              {(failure) => <li>
                Partition {failure.range.partition}, from {failure.range.from} until {failure.range.until} (exclusive): {failure.error} <code>{failure.code}</code>
              </li>}
            </For>
          </ul>
        </section>
      </Show>
    </div>
  );
}

function headlineOf(kind: ReturnType<typeof readingOf>["kind"]): string {
  switch (kind) {
    case "incomplete":
      return "Copy incomplete";
    case "complete":
      return "Copied";
    case "nothing":
      return "Nothing was copied";
    case "short":
      return "Copied, but the source held fewer records than you named";
    case "partial":
      return "Copied part of the way";
  }
}

function detailOf(reading: ReturnType<typeof readingOf>, outcome: ResendOutcome): string {
  switch (reading.kind) {
    case "incomplete":
      return (
        `The server reported failed records or incomplete ranges. ${outcome.written.toLocaleString()} records were written to ${outcome.toTopic} and remain there. ` +
        "Do not repeat the whole batch: that would duplicate successful writes. Check the destination, resolve the reasons below, " +
        "then select only the failed source offsets or incomplete ranges for a new copy."
      );
    case "complete":
      return `Every record your ranges named is now in ${outcome.toTopic}, with the original producer's bytes and headers.`;
    case "nothing":
      /* The state a bare success message would hide entirely. It says what almost certainly
       * happened and what to do about it, because "0 and 0" alone leaves an operator staring at a
       * destination they believe holds their records. */
      return (
        "The server read no records and wrote none, and it reported no error. The offsets you " +
        "named are no longer in the source topic — retention or a purge removed them since you " +
        `chose the range. Nothing was added to ${outcome.toTopic}. Read the topic to find the ` +
        "offsets it still holds, then copy those."
      );
    case "short":
      return (
        `${reading.missing.toLocaleString()} of the records you named were not in the log any more — ` +
        `retention removed them before the copy ran. Everything that was still there was copied.`
      );
    case "partial":
      return (
        `${reading.lost.toLocaleString()} records were read but never written: the copy failed ` +
        `part-way. A resend is not atomic, so what had already been written stayed written — ` +
        `check ${outcome.toTopic} before running it again, or you will copy those records twice.`
      );
  }
}

/**
 * How many partitions the topic has, as a clause.
 *
 * Written out because the dialog said "has 1 partitions" on every single-partition topic, in two
 * places. A single partition is the ordinary shape of a compacted config topic and of most of what
 * a scratch cluster holds, so this was not a rare case — and a sentence that cannot count is a
 * sentence an operator stops reading, on the one screen whose whole job is to be believed about
 * numbers.
 */
function describePartitions(count: number): string {
  return count === 1 ? "has 1 partition" : `has ${String(count)} partitions`;
}

/** One range's size, or the honest absence of one while it is still being typed. */
function describeSize(size: number | undefined): string {
  if (size === undefined) return "";
  return size === 1 ? "1 record" : `${size.toLocaleString()} records`;
}

function describeTotal(total: number | undefined): string {
  if (total === undefined) return "";
  if (total > MAX_RESEND_RECORDS) {
    return `${total.toLocaleString()} records — over the ${MAX_RESEND_RECORDS.toLocaleString()} a copy may carry`;
  }
  return total === 1 ? "1 record in total" : `${total.toLocaleString()} records in total`;
}

/** See `feature-topics/src/TopicPage.tsx`: `disabled` and its reason are one pair in `Button`'s type. */
function disabledProps(
  reason: string | undefined,
): { readonly disabled: true; readonly disabledReason: string } | Record<string, never> {
  return reason === undefined ? {} : { disabled: true, disabledReason: reason };
}
