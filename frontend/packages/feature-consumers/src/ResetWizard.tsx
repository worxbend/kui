/**
 * The offset-reset wizard: compose, preview, apply, receipt (ADR-045).
 *
 * ## Why a preview is not a nicety
 *
 * The operator asks to "reset to 09:00". What that means in offsets depends on what exists at 09:00
 * on each of twelve partitions, on whether retention has already moved past that point, and on
 * KIP-122's rule that a timestamp with no matching record resolves to the *end* of the partition —
 * the opposite of what the person expects, and how a reset meant to replay a morning's traffic
 * instead skips it. Only the broker knows those numbers. So the first request resolves them and
 * changes nothing, the screen shows what each partition would move from and to, and the second
 * request writes exactly that and nothing else.
 *
 * ## The defect this rewrite exists to fix
 *
 * In the screen this replaces, pressing **Preview** produced no feedback whatsoever. The button did
 * not change, no plan appeared, and nothing said why — a click that neither advances nor explains is
 * indistinguishable from a broken button, and operators stopped trusting the flow and reset offsets
 * from a shell instead. There were two causes and both are closed here:
 *
 * 1. **A refusal with nowhere to go.** A form that could not be turned into a request simply
 *    returned, silently. Validation is now a *value* — `resetRequestOf` returns either a request or
 *    a sentence — and the only path that does not advance the step is the one that writes the
 *    sentence into `problem`, which is rendered in a live region above the form. There is no branch
 *    that does neither.
 * 2. **A plan step that drew nothing.** `Planning` re-rendered the composer with its button
 *    disabled, so on a fast server the screen flickered and on a slow one it looked frozen. The
 *    plan step is now its own rendering with its own heading, its own skeleton table at the height
 *    the real plan will occupy, and a busy button that says `Planning…` — so pressing Preview always
 *    changes the screen within one frame, whatever the server does next.
 *
 * ## The plan is the confirmation, and there is no second one
 *
 * The apply endpoint takes a token and nothing else, so **Apply** cannot exist before a plan does —
 * not because a rule disables it, but because there is nothing to send. There is deliberately no
 * type-the-group-name step either: that guards against acting on the wrong row, and the failure this
 * flow is about is the one where the operator is confident and the arithmetic is the surprise.
 *
 * A token expires after five minutes. When the server refuses an expired one, the wizard goes back
 * to the plan it is showing rather than quietly re-planning: a silent re-plan would compute
 * different offsets from the ones on screen, which is the exact thing being prevented.
 *
 * ## A plan that changes nothing offers nothing to confirm
 *
 * `noOp` means every partition is already where the reset would put it. The wizard says so and shows
 * no Apply button, because a confirmation for an operation that changes nothing is how operators
 * learn to click through confirmations.
 */

import { For, Show, createMemo, createSignal, onCleanup } from "solid-js";
import type { JSX } from "@solidjs/web";
import {
  Button,
  Card,
  DataTable,
  EmptyState,
  MISSING,
  Select,
  Skeleton,
  TextField,
  formatCount,
  formatDelta,
  type Column,
} from "@kui/kernel";
import {
  EMPTY_RESET_FORM,
  RESET_TARGETS,
  recordsMoved,
  resetRequestOf,
  targetOption,
  type PlannedPartition,
  type ResetForm,
  type ResetPlan,
  type ResetRequest,
  type ResetTarget,
} from "./detail.js";

/**
 * Where the wizard is. One value, so the screen cannot be in two of these at once.
 *
 * Not three booleans on the side: "a plan is being fetched", "a plan is on screen" and "the plan is
 * being applied" disable the same controls and draw different things, and independent flags is how
 * a screen ends up offering Apply while a plan is still being computed.
 */
export type ResetStep =
  | { readonly kind: "composing" }
  | { readonly kind: "planning" }
  | { readonly kind: "planned"; readonly plan: ResetPlan }
  | { readonly kind: "applying"; readonly plan: ResetPlan }
  | { readonly kind: "applied"; readonly receipt: ResetPlan };

export interface ResetWizardProps {
  /** The topics this group holds offsets on. Resetting a group on a topic it does not consume
   *  writes offsets for a subscription that does not exist. */
  readonly topics: readonly { readonly topic: string; readonly partitions: readonly number[] }[];
  /** Resolves the request against the group's live offsets. Changes nothing. */
  readonly plan: (request: ResetRequest) => Promise<{ readonly ok: true; readonly plan: ResetPlan } | { readonly ok: false; readonly problem: string }>;
  /** Sends the token and nothing else. Answers with what was actually written. */
  readonly apply: (token: string) => Promise<{ readonly ok: true; readonly receipt: ResetPlan } | { readonly ok: false; readonly problem: string }>;
  /**
   * Whether this user may reset this group's offsets. Gates the way *in* rather than the final
   * Apply: a wizard somebody may open, fill in and preview only to be refused at the last step is
   * three screens of work thrown away, and it reads as a fault in KUI rather than as a permission.
   */
  readonly permitted?: boolean | undefined;
  /** Why they may not, when they may not. A dead control with no reason is worse than no control. */
  readonly refusal?: string | undefined;
  /** For formatting the token's expiry. Injected so a test is not at the mercy of the clock. */
  readonly formatTime?: ((at: Date) => string) | undefined;
}

export function ResetWizard(props: ResetWizardProps): JSX.Element {
  const [open, setOpen] = createSignal(false);
  const [step, setStep] = createSignal<ResetStep>({ kind: "composing" });
  const [form, setForm] = createSignal<ResetForm>(EMPTY_RESET_FORM);
  /**
   * Bumped every time `preview`/`applyPlan` starts a request and every time it is abandoned, so a
   * response that finally arrives after the operator has cancelled or started a new one can tell it
   * is stale and skip writing over whatever the screen shows now.
   */
  let requestToken = 0;
  onCleanup(() => { requestToken++; });
  /**
   * The last thing that went wrong, whether the form refused it or the server did.
   *
   * One place, because from the operator's side "I filled this in wrongly" and "the cluster refused"
   * are the same question — what do I do now — and two places to look for the answer is one too
   * many.
   */
  const [problem, setProblem] = createSignal<string | null>(null);

  /** The chosen topic's partitions, which is what the reset's scope sentence counts. */
  const partitions = createMemo<readonly number[]>(
    () => props.topics.find((one) => one.topic === chosenTopic())?.partitions ?? [],
  );

  /**
   * The topic the form is on: the operator's choice if they have made one, otherwise the first
   * topic the group holds offsets on.
   *
   * Derived rather than seeded by an effect. An effect that wrote the first topic into the form
   * would fire again every time a fresh snapshot of the group arrived — and moving somebody off the
   * topic they had chosen, mid-form, because the server answered again is the drawer-rebuild defect
   * in a different costume.
   */
  const chosenTopic = createMemo(() => {
    const explicit = form().topic;
    if (explicit !== "") return explicit;
    return props.topics[0]?.topic ?? "";
  });

  const parameter = createMemo(() => targetOption(form().target).parameter);
  const busy = createMemo(() => step().kind === "planning" || step().kind === "applying");
  /** The receipt, when there is one. A value rather than a `kind` check inside JSX, so that the
   *  narrowing survives into the block that draws it. */
  const receipt = createMemo<ResetPlan | undefined>(() => {
    const current = step();
    return current.kind === "applied" ? current.receipt : undefined;
  });

  function edit<K extends keyof ResetForm>(field: K, value: ResetForm[K]): void {
    setForm({ ...form(), [field]: value });
  }

  function close(): void {
    requestToken++;
    setOpen(false);
    setStep({ kind: "composing" });
    setProblem(null);
  }

  /**
   * The Preview button's whole job. Every path through it changes something the operator can see:
   * either the step advances, or a sentence appears. There is no third path.
   */
  async function preview(): Promise<void> {
    if (props.permitted === false) {
      setProblem(props.refusal ?? "You do not have permission to reset offsets.");
      return;
    }
    const attempt = resetRequestOf({ ...form(), topic: chosenTopic() }, partitions());
    if (!attempt.ok) {
      setProblem(attempt.problem);
      return;
    }
    setProblem(null);
    const token = ++requestToken;
    setStep({ kind: "planning" });
    const answer = await props.plan(attempt.request);
    // A cancelled or superseded request: the operator has already been moved off this step, and
    // writing over whatever they are looking at now would be the stuck-forever bug in reverse.
    if (token !== requestToken) return;
    if (answer.ok) {
      setStep({ kind: "planned", plan: answer.plan });
      return;
    }
    // Back to the form, with the reason. Staying on a "planning" rendering that will never finish
    // is the one outcome an operator cannot act on.
    setStep({ kind: "composing" });
    setProblem(answer.problem);
  }

  async function applyPlan(plan: ResetPlan): Promise<void> {
    if (props.permitted === false) {
      setProblem(props.refusal ?? "You do not have permission to reset offsets.");
      return;
    }
    setProblem(null);
    const token = ++requestToken;
    setStep({ kind: "applying", plan });
    const answer = await props.apply(plan.token);
    if (token !== requestToken) return;
    if (answer.ok) {
      setStep({ kind: "applied", receipt: answer.receipt });
      return;
    }
    // Back to the *plan*, not to the form: the plan is still what the operator read, and if the
    // token has expired the honest next step is to ask for a new one, not to re-plan silently.
    setProblem(answer.problem);
    setStep({ kind: "planned", plan });
  }

  /**
   * The escape hatch this file's audit found missing: if `plan`/`apply` never settles because the
   * coordinator is unreachable, `busy()` had disabled every field and the only way out was closing
   * the wizard. Cancelling abandons the in-flight call in the browser — `requestToken` makes its
   * eventual answer, if one ever arrives, a no-op — and says plainly that the server side of it is
   * unknown, because a write that was cancelled client-side may already have happened.
   */
  function cancelPlanning(): void {
    requestToken++;
    setStep({ kind: "composing" });
    setProblem("The plan request was abandoned. It may still be running on the server; try again in a moment.");
  }

  function cancelApplying(plan: ResetPlan): void {
    requestToken++;
    setStep({ kind: "planned", plan });
    setProblem(
      "The apply request was abandoned in the browser. It may have already written to the cluster — check the group's offsets before retrying.",
    );
  }

  return (
    <section class="kui-cg-reset" data-testid="group-reset">
      <div class="kui-cg-reset__head">
        <h2 class="kui-cg-section__title">Reset offsets</h2>
        <Show
          when={open()}
          fallback={
            <Show
              when={props.permitted !== false}
              fallback={
                <Button
                  variant="secondary"
                  disabled
                  /* Cluster-scoped, where the caller's sentence names the group — see the note
                     on `GroupDetail`'s forget fallback. Two identical sentences make the caller's
                     gate unobservable. */
                  disabledReason={
                    props.refusal ?? "You do not have permission to reset offsets on this cluster."
                  }
                >
                  Reset offsets
                </Button>
              }
            >
              <Button variant="secondary" onClick={() => setOpen(true)}>
                Reset offsets
              </Button>
            </Show>
          }
        >
          <Button variant="ghost" onClick={close}>
            Close
          </Button>
        </Show>
      </div>

      <Show when={open()}>
        {/*
          The problem line lives above everything and is a live region, so a refusal is announced
          without moving focus. It is `role="alert"` rather than `aria-live="polite"` because the
          operator has just pressed a button and is waiting to find out what it did.
        */}
        <Show when={problem()}>
          {(message) => (
            <p class="kui-cg-reset__problem" role="alert" data-testid="group-reset-problem">
              {message()}
            </p>
          )}
        </Show>

        <Show when={step().kind === "composing" || step().kind === "planning"}>
          <div class="kui-cg-reset__form" data-testid="group-reset-form">
            <p class="kui-cg-reset__note">
              Nothing is written until you have read the plan. The preview asks the broker where each partition would
              move to and changes nothing.
            </p>
            <div class="kui-cg-reset__fields">
              <Select
                label="Topic"
                options={props.topics.map((one) => ({ value: one.topic, label: one.topic }))}
                value={chosenTopic()}
                disabled={busy()}
                disabledReason={busy() ? "A plan is being computed." : undefined}
                emptyMessage="This group holds no offsets, so there is nothing to reset."
                onChange={(value) => edit("topic", value)}
              />
              <Select<ResetTarget>
                label="Move to"
                options={RESET_TARGETS.map((one) => ({ value: one.value, label: one.label }))}
                value={form().target}
                disabled={busy()}
                disabledReason={busy() ? "A plan is being computed." : undefined}
                onChange={(value) => edit("target", value)}
              />
              <Show when={parameter() === "offset"}>
                <TextField
                  label="Offset"
                  value={form().offset}
                  help={targetOption(form().target).hint}
                  disabled={busy()}
                  mono
                  onInput={(value) => edit("offset", value)}
                />
              </Show>
              <Show when={parameter() === "timestamp"}>
                <TextField
                  label="Date and time"
                  value={form().timestamp}
                  help={targetOption(form().target).hint}
                  disabled={busy()}
                  onInput={(value) => edit("timestamp", value)}
                />
              </Show>
              <Show when={parameter() === "shiftBy"}>
                <TextField
                  label="Records to move by"
                  value={form().shiftBy}
                  help={targetOption(form().target).hint}
                  disabled={busy()}
                  mono
                  onInput={(value) => edit("shiftBy", value)}
                />
              </Show>
              <Show when={parameter() === "durationMinutes"}>
                <TextField
                  label="Minutes to go back"
                  value={form().durationMinutes}
                  help={targetOption(form().target).hint}
                  disabled={busy()}
                  mono
                  onInput={(value) => edit("durationMinutes", value)}
                />
              </Show>
            </div>
            <p class="kui-cg-reset__note" data-testid="group-reset-scope">
              {scopeSentence(partitions().length)}
            </p>
            <Show when={props.permitted !== false} fallback={
              <Button disabled disabledReason={props.refusal ?? "You do not have permission to reset offsets."}>Preview the plan</Button>
            }>
              <Button busy={step().kind === "planning"} onClick={() => void preview()}>
                {step().kind === "planning" ? "Planning…" : "Preview the plan"}
              </Button>
            </Show>
          </div>
        </Show>

        {/*
          The planning rendering. It is a step of its own rather than the form with a disabled
          button, because the point of pressing Preview is to find out what will happen and the
          screen has to start answering immediately. The skeleton is the size the plan table will
          be, so nothing jumps when the numbers land.
        */}
        <Show when={step().kind === "planning"}>
          <Card title="Plan" state="ready" testId="group-reset-planning">
            <p class="kui-cg-reset__note">Asking the broker where each partition would move to…</p>
            <div class="kui-cg-reset__skeleton" aria-hidden="true">
              <For each={[0, 1, 2, 3]}>{() => <Skeleton width="100%" height="1.75rem" />}</For>
            </div>
            <div class="kui-cg-reset__actions">
              <Button variant="ghost" onClick={cancelPlanning}>
                Cancel
              </Button>
            </div>
          </Card>
        </Show>

        <Show when={planOf(step())}>
          {(plan) => (
            <PlanView
              plan={plan()}
              applying={step().kind === "applying"}
              permitted={props.permitted}
              refusal={props.refusal}
              formatTime={props.formatTime}
              onApply={() => void applyPlan(plan())}
              onCancelApply={() => cancelApplying(plan())}
              onBack={() => {
                setStep({ kind: "composing" });
                setProblem(null);
              }}
            />
          )}
        </Show>

        {/*
          The receipt: what the broker says it wrote, not what the browser asked for. It is a
          separate rendering from the plan even though the two draw the same table, because the
          operator's one record of a destructive action is the sentence above it.
        */}
        <Show when={receipt()}>
          {(written) => (
            <Card title="What was written" testId="group-reset-receipt">
              <p class="kui-cg-reset__note">
                These offsets are what the broker reported after the write, not what KUI asked for.
              </p>
              <PartitionTable partitions={written().partitions} caption="Offsets written" testId="group-reset-receipt-table" />
              <div class="kui-cg-reset__actions">
                <Button variant="secondary" onClick={() => setStep({ kind: "composing" })}>
                  Done
                </Button>
              </div>
            </Card>
          )}
        </Show>
      </Show>
    </section>
  );
}

/** The plan on screen, whether it is being read or being applied. */
function planOf(step: ResetStep): ResetPlan | undefined {
  if (step.kind === "planned") return step.plan;
  if (step.kind === "applying") return step.plan;
  return undefined;
}

export function scopeSentence(count: number): string {
  if (count === 0) return "Nothing will be moved: this group holds no offsets on that topic.";
  if (count === 1) return "1 partition will be moved.";
  return `${formatCount(count)} partitions will be moved.`;
}

/**
 * The plan step: the warnings, the numbers, the expiry, and the one button that writes them.
 *
 * The warnings sit above the table because they change how every number under them should be read.
 * Clamping is the case they exist for: an operator who asked for offset 9,000,000 on a partition
 * holding four hundred records has to see what will actually be written.
 */
function PlanView(props: {
  readonly plan: ResetPlan;
  readonly applying: boolean;
  readonly permitted?: boolean | undefined;
  readonly refusal?: string | undefined;
  readonly formatTime?: ((at: Date) => string) | undefined;
  readonly onApply: () => void;
  readonly onCancelApply: () => void;
  readonly onBack: () => void;
}): JSX.Element {
  const moved = createMemo(() => recordsMoved(props.plan));
  return (
    <Card title="Plan" testId="group-reset-plan">
      <p class="kui-cg-reset__summary" data-testid="group-reset-summary">
        {props.plan.noOp
          ? "Every partition is already where this reset would put it."
          : `${formatCount(props.plan.partitions.length)} ${props.plan.partitions.length === 1 ? "partition moves" : "partitions move"}, ${formatCount(moved())} ${moved() === 1 ? "record" : "records"} in total.`}
      </p>

      <Show when={props.plan.warnings.length > 0}>
        <ul class="kui-cg-reset__warnings" data-testid="group-reset-warnings">
          <For each={props.plan.warnings}>
            {(warning) => (
              <li class="kui-cg-reset__warning" data-kind={warning.kind}>
                {warning.message}
              </li>
            )}
          </For>
        </ul>
      </Show>

      <PartitionTable partitions={props.plan.partitions} caption="Offsets this reset would write" testId="group-reset-plan-table" />

      <p class="kui-cg-reset__note" data-testid="group-reset-expiry">
        This plan expires at {(props.formatTime ?? defaultTime)(props.plan.expiresAt)}. After that it has to be computed
        again, because the offsets it names may no longer be the right ones.
      </p>

      <div class="kui-cg-reset__actions">
        <Show
          when={!props.plan.noOp}
          fallback={
            <p class="kui-cg-reset__note" data-testid="group-reset-noop">
              There is nothing to apply.
            </p>
          }
        >
          <Show when={props.permitted !== false} fallback={
            <Button variant="danger" icon="warning" disabled disabledReason={props.refusal ?? "You do not have permission to reset offsets."}>Apply this plan</Button>
          }>
            <Button variant="danger" icon="warning" busy={props.applying} onClick={props.onApply}>
              {props.applying ? "Writing offsets…" : "Apply this plan"}
            </Button>
          </Show>
        </Show>
        <Show
          when={!props.applying}
          fallback={
            <Button variant="ghost" onClick={props.onCancelApply}>
              Cancel
            </Button>
          }
        >
          <Button variant="ghost" onClick={props.onBack}>
            Start again
          </Button>
        </Show>
      </div>
    </Card>
  );
}

function defaultTime(at: Date): string {
  return at.toLocaleTimeString();
}

/**
 * The four columns that make a reset readable: which partition, where it is, where it goes, and by
 * how much.
 *
 * One table draws both the plan and the receipt, because they are the same document — the apply
 * endpoint answers with what it wrote — and two tables would be two chances for "what we said we
 * would do" and "what we did" to be drawn differently.
 */
function PartitionTable(props: {
  readonly partitions: readonly PlannedPartition[];
  readonly caption: string;
  readonly testId: string;
}): JSX.Element {
  const columns: readonly Column<PlannedPartition>[] = [
    { id: "partition", header: "Partition", align: "numeric", width: "8rem", render: (row) => formatCount(row.partition) },
    {
      id: "from",
      header: "From",
      align: "numeric",
      // An em dash and never a zero. A partition this group has never committed on is not a
      // partition whose consumer sits at the beginning of the log, and `0` would say exactly that.
      render: (row) =>
        row.current === null ? (
          <span title="This group has never committed an offset on this partition.">{MISSING}</span>
        ) : (
          formatCount(row.current)
        ),
    },
    { id: "to", header: "To", align: "numeric", render: (row) => formatCount(row.proposed) },
    {
      id: "delta",
      header: "Change",
      align: "numeric",
      // Signed, because "this rewinds 4,200 records" and "this skips 4,200 records" are the two
      // things the operator is choosing between and the sign is the whole difference.
      render: (row) => (row.delta === null ? MISSING : formatDelta(row.delta)),
    },
  ];

  return (
    <DataTable<PlannedPartition>
      caption={props.caption}
      columns={columns}
      rows={[...props.partitions].sort((a, b) => a.partition - b.partition)}
      rowKey={(row) => String(row.partition)}
      testId={props.testId}
      empty={<EmptyState kind="empty" title="No partitions in this plan." description="Nothing would be written." />}
    />
  );
}
