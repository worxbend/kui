/**
 * The confirmation for an irreversible action that the server plans first.
 *
 * Three callers: purge, delete, and raising a topic's partition count. The first two destroy data;
 * the third destroys nothing and still cannot be undone, because Kafka has no way to remove a
 * partition. That is why the dialog is about *irreversibility* rather than about destruction — the
 * `destructive` and `planningMessage` props are how the third caller says so, and a dialog that
 * announced it was working out what a partition increase "would destroy" would be the product
 * inventing a consequence.
 *
 * ## Why the plan is not a formality
 *
 * Purge and delete both take a token from `POST …/plan` and nothing else. It would be possible to
 * fetch a plan, ignore it, and send the token when the operator clicks — and that would throw away
 * the two things the design buys:
 *
 * **The consequence is measured, not adjectived.** "This will permanently delete data" is a sentence
 * every operator has clicked past. "Deletes 1,284,003 records across 12 partitions" is one they
 * read, because it is about *their* topic. The plan is where that number comes from; nothing on the
 * client can compute it.
 *
 * **The agreement is fixed at plan time.** A purge deletes up to the offsets the plan resolved, not
 * to the topic's end as it stands when the button is clicked. Records produced while the operator
 * was reading this dialog survive, because they are not what the operator agreed to lose. That
 * property lives entirely in the token, which is why an expired one is reported as a failure and
 * never silently replaced with a fresh plan — re-planning would quietly widen what gets destroyed,
 * at the exact moment the operator believes they are confirming what they just read.
 *
 * ## The three states before the button is live
 *
 * Opening the dialog starts the plan, so there is a moment with nothing to show. It says it is
 * working it out rather than rendering a confirmation with blanks in it — a destructive dialog whose
 * numbers arrive after the button does is a dialog somebody can confirm without having read
 * anything. A plan that *fails* offers no confirm button at all: without a token there is nothing to
 * send, and a button that cannot work is worse than an absent one.
 */
import { Show, createEffect, createSignal, onCleanup } from "solid-js";
import type { JSX } from "@solidjs/web";
import { Banner, ConfirmDialog, Dialog, Spinner, type Mutation } from "@kui/kernel";
import type { IconName } from "@kui/kernel";

/** The part of a plan this dialog needs. Purge and delete plans both satisfy it. */
export interface TokenPlan {
  readonly token: string | null;
  readonly warnings: readonly {
    readonly code: string;
    readonly message: string;
  }[];
}

export interface PlannedActionDialogProps<P extends TokenPlan> {
  readonly open: boolean;
  readonly onClose: () => void;

  /** `Purge orders.payments.v2?` — the object named, never "this topic". */
  readonly title: string;
  /** `Purge`, `Delete topic`. A verb. */
  readonly confirmLabel: string;
  readonly confirmIcon: IconName;
  /**
   * Typed before the button becomes usable. The topic's name, for anything that cannot be undone.
   *
   * Optional, and the test is undo-ability rather than destruction. Purge and delete ask for it
   * because the records do not come back; adding partitions asks for it too, because Kafka has no
   * way to remove one afterwards. An action that can be reversed must not ask, or the mechanism
   * becomes a reflex and stops being read.
   */
  readonly typeToConfirm?: string | undefined;

  /**
   * `false` for an action that is irreversible but destroys nothing.
   *
   * Adding partitions is the case this exists for. It cannot be undone, so it is confirmed and
   * typed; it deletes no record, so it must not wear delete's silhouette. Two danger buttons that
   * mean different amounts of harm is how the wrong one gets clicked.
   */
  readonly destructive?: boolean | undefined;

  /**
   * What the "working it out" dialog says while the plan is out.
   *
   * The default names destruction, which is right for purge and delete and false for a partition
   * increase — telling an operator KUI is working out what an operation "would destroy" when it
   * destroys nothing is the product inventing a consequence.
   */
  readonly planningMessage?: string | undefined;

  /** Asks the server what would happen. Started when the dialog opens. */
  readonly plan: () => Promise<P | { readonly failure: string }>;
  /** The measured consequence, from the plan. One sentence, in figures. */
  readonly describe: (plan: P) => string;

  /** Runs it, with the plan's token. */
  readonly onConfirm: (token: string) => void;
  readonly state: Mutation<unknown>;
}

export function PlannedActionDialog<P extends TokenPlan>(
  props: PlannedActionDialogProps<P>,
): JSX.Element {
  const [plan, setPlan] = createSignal<P | undefined>(undefined, {
    ownedWrite: true,
  });
  const [planFailure, setPlanFailure] = createSignal<string | undefined>(undefined, {
    ownedWrite: true,
  });

  /*
   * The plan is fetched on *open*, not on mount, and thrown away on close.
   *
   * Both halves matter. Fetching on mount would plan a purge for every topic row the page rendered,
   * which is a burst of offset reads against the cluster for an action nobody has asked for.
   * Discarding on close is what stops a token from an abandoned confirmation being reused later
   * against a topic that has moved on since — the plan would then describe a state the cluster is no
   * longer in, and the confirmation would be an agreement to something the operator never saw.
   */
  let generation = 0;
  onCleanup(() => { generation += 1; });
  const close = (): void => {
    generation += 1;
    setPlan(undefined);
    setPlanFailure(undefined);
    props.onClose();
  };
  createEffect(
    () => [props.open, props.title, props.plan] as const,
    ([open]) => {
      const current = ++generation;
      if (!open) {
        setPlan(undefined);
        setPlanFailure(undefined);
        return;
      }
      setPlan(undefined);
      setPlanFailure(undefined);
      void props.plan().then((answer) => {
        if (current !== generation || !props.open) return;
        if ("failure" in answer) setPlanFailure(answer.failure);
        else setPlan(() => answer);
      }).catch((error: unknown) => {
        if (current !== generation || !props.open) return;
        setPlanFailure(error instanceof Error ? error.message : "The plan could not be loaded.");
      });
    },
  );

  const busy = () => props.state.kind === "running";
  const failure = () =>
    props.state.kind === "failed" || props.state.kind === "forbidden" ? props.state : undefined;

  return (
    <>
      {/* Working it out. A separate dialog rather than a spinner inside the confirmation, because a
          confirmation whose figures arrive after its button does can be confirmed by somebody who
          has read nothing. */}
      <Dialog
        open={props.open && plan() === undefined && planFailure() === undefined}
        onClose={close}
        title={props.title}
        size="sm"
        testId="planned-action-planning"
      >
        <p role="status">
          <Spinner />{" "}
          {props.planningMessage ?? "Asking the cluster what this would destroy…"}
        </p>
      </Dialog>

      {/* The plan could not be made. No token, so no confirm button: there is nothing to send, and
          a button that cannot work is worse than one that is not there. */}
      <Dialog
        open={props.open && planFailure() !== undefined}
        onClose={close}
        title={props.title}
        size="sm"
        testId="planned-action-unplannable"
      >
        <Banner tone="danger" message={planFailure() ?? ""} />
      </Dialog>

      <Show when={plan()}>
        {(ready) => (
          <ConfirmDialog
            open={props.open}
            onClose={close}
            title={props.title}
            consequence={consequenceOf(ready(), props.describe)}
            confirmLabel={props.confirmLabel}
            confirmIcon={props.confirmIcon}
            {...(props.typeToConfirm === undefined ? {} : { typeToConfirm: props.typeToConfirm })}
            {...(props.destructive === undefined ? {} : { destructive: props.destructive })}
            busy={busy()}
            error={
              failure() === undefined
                ? undefined
                : {
                    message: failure()!.message,
                    code:
                      failure()!.kind === "failed"
                        ? (failure() as { code: string }).code
                        : undefined,
                  }
            }
            onConfirm={() => {
              const token = ready().token;
              /*
               * A plan with no token is one the server computed but will not let this caller apply —
               * a read-only cluster answers exactly that way, so the operator can see what *would*
               * happen. There is nothing to send, and inventing a token would produce a validation
               * error that reads like a bug.
               */
              if (token === null) return;
              props.onConfirm(token);
            }}
            testId="planned-action-confirm"
          />
        )}
      </Show>
    </>
  );
}

/**
 * The consequence sentence.
 *
 * ## The server's warnings are the prose, and `describe` is the fallback
 *
 * This was originally the other way round — a sentence composed here, with the plan's warnings
 * appended — and running it against a real cluster showed why that is wrong. The server already
 * writes the measured sentence, and writes it better, because it knows things the browser does not:
 *
 *     16 records across 4 partitions are deleted and cannot be recovered. The topic, its
 *     configuration and its partitions stay exactly as they are; only the records go.
 *
 * Note "4 partitions" where the topic has six — the two holding nothing are not mentioned, which is
 * a distinction the client cannot draw from a total. Composing our own sentence alongside that gave
 * the operator the same fact twice, in two phrasings with two different numbers, on a dialog whose
 * entire purpose is that its numbers get read.
 *
 * So the warnings are used as written when there are any, and `describe` covers the case where there
 * are none — a plan for a topic with nothing in it, which is exactly when the server has nothing to
 * warn about and the dialog still owes the operator a sentence.
 */
export function consequenceOf<P extends TokenPlan>(plan: P, describe: (plan: P) => string): string {
  const warnings = plan.warnings.map((warning) => warning.message);
  const blocked =
    plan.token === null
      ? ["This cluster is read-only in KUI, so this cannot be applied — only previewed."]
      : [];
  const body = warnings.length > 0 ? warnings : [describe(plan)];
  return [...body, ...blocked].join(" ");
}
