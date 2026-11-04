/**
 * The dialog, and the confirmation that every destructive action in this product opens.
 *
 * ## A dialog interrupts to ask a question
 *
 * That is the whole of its purpose, and it is why a dialog is small, centred, and has exactly one
 * question in it. Anything that is a *place to work* — composing a message, editing a config — is
 * a drawer instead (see `Drawer.tsx`). The two share all of their behaviour and none of their
 * shape.
 *
 * ## Absent from the document while closed
 *
 * Not hidden: absent. A `display: none` dialog is still in the accessibility tree in some
 * combinations, its inputs are still form-submittable, its `autofocus` still fires, and its
 * content is still found by the browser's find-in-page. `<Show>` around the whole surface means
 * "closed" and "not there" are the same fact, which also gives the focus trap a clean lifecycle:
 * it is installed when the surface mounts and torn down when it unmounts, and there is no third
 * state where it exists but should not be running.
 *
 * ## The confirmation, and what it must say
 *
 * A confirmation that says "Are you sure?" is a keystroke, not a decision — people learn the
 * shape of it and clear it without reading. So `ConfirmDialog` requires two things a generic
 * "are you sure" cannot have:
 *
 *   1. **It names the object.** `Purge orders.payments.v2?`, never `Purge this topic?`.
 *   2. **It states the consequence in measurements, not adjectives.** *"This deletes 1,536
 *      partitions' worth of records — about 128 GB. It cannot be undone."* is a sentence an
 *      operator can check against what they believe they are looking at. *"This is permanent and
 *      cannot be undone"* is true of every dialog they have ever dismissed.
 *
 * And for the worst of them — deleting a topic, emptying a topic — it requires the operator to
 * type the object's name. That is not friction for its own sake: it is the one mechanism that
 * makes it impossible to destroy the *wrong* topic by muscle memory, because the muscle memory
 * does not know the name.
 *
 * ## Focus starts on Cancel
 *
 * Every destructive confirmation opens with focus on the safe action. The keystroke that opened
 * the dialog is often still going — an `Enter` held a fraction too long on the row behind — and
 * the confirm button must not be under it.
 *
 * ## Failure keeps the dialog open
 *
 * If the mutation fails, the dialog stays, the typed name stays, and the error appears above the
 * actions with its code. Closing on failure means the operator has to reconstruct what they were
 * doing in order to find out that it did not happen.
 */
import { createEffect, createSignal, Show } from "solid-js";
import type { JSX } from "@solidjs/web";
import { Portal } from "@solidjs/web";
import { Button } from "./Button.jsx";
import { Icon, type IconName } from "./Icon.jsx";
import { modalBehaviour, scrimClickHandler } from "./overlay.js";

export type DialogSize = "sm" | "md" | "lg";

export interface DialogProps {
  readonly open: boolean;
  readonly onClose: () => void;
  readonly title: string;
  /** One sentence under the title. Becomes the dialog's accessible description. */
  readonly description?: string | undefined;
  readonly size?: DialogSize | undefined;
  /** The footer. Order is [secondary…, primary] left to right; the primary action sits at the end. */
  readonly actions?: JSX.Element | undefined;
  /** Set false while a form holds unsaved input, so a stray click on the veil cannot discard it. */
  readonly closeOnScrimClick?: boolean | undefined;
  /** Where focus lands. Defaults to the first focusable control. */
  readonly initialFocus?: (() => HTMLElement | null | undefined) | undefined;
  readonly testId?: string | undefined;
  readonly children?: JSX.Element;
}

/** Unique enough for `aria-labelledby` without pulling in an id generator per element. */
let sequence = 0;
function nextId(prefix: string): string {
  sequence += 1;
  return `${prefix}-${sequence}`;
}

export function Dialog(props: DialogProps): JSX.Element {
  const titleId = nextId("kui-modal-title");
  const descriptionId = nextId("kui-modal-desc");


  // Held so the veil handler can tell "clicked the veil" from "clicked the surface".
  let scrim: HTMLElement | undefined;
  return (
    <Show when={props.open}>
      {/* Rendered at the end of `<body>` so that no ancestor's `overflow`, `transform` or
          `z-index` can clip a surface that is meant to be over the entire page. A `transform` on
          an ancestor creates a containing block for `position: fixed`, and the symptom is a
          dialog pinned inside a scrolling panel with no clue as to why. */}
      <Portal mount={document.body}>
        <div
          class="kui-modal-scrim"
          ref={(element: HTMLElement) => (scrim = element)}
          onClick={scrimClickHandler(() => scrim, {
            onClose: () => props.onClose(),
            ...(props.closeOnScrimClick !== undefined ? { closeOnScrimClick: props.closeOnScrimClick } : {}),
          })}
        >
          <div
            class={["kui-modal", `kui-modal--${props.size ?? "md"}`]}
            role="dialog"
            aria-modal="true"
            aria-labelledby={titleId}
            aria-describedby={props.description === undefined ? undefined : descriptionId}
            data-testid={props.testId}
            ref={modalBehaviour({
              onClose: () => props.onClose(),
              ...(props.initialFocus !== undefined ? { initialFocus: props.initialFocus } : {}),
            })}
          >
            {/* A `div`, not a `<header>`. A `<header>` that is not inside an article, aside, main, nav
                or section is a *banner* landmark, and a page with two banner landmarks — the
                application's top bar and this one — is a page whose landmark navigation is
                broken. The same argument applies to the actions row below. */}
            <div class="kui-modal__header">
              <div class="kui-modal__heading">
                <h2 class="kui-modal__title" id={titleId}>
                  {props.title}
                </h2>
                <Show when={props.description}>
                  {(description) => (
                    <p class="kui-modal__description" id={descriptionId}>
                      {description()}
                    </p>
                  )}
                </Show>
              </div>
              {/* The close button is last in the DOM but drawn first-to-the-right, so `Tab` from
                  the title reaches the content before it reaches the way out. */}
              <button
                type="button"
                class="kui-modal__close kui-focusable"
                aria-label={`Close ${props.title}`}
                onClick={() => props.onClose()}
              >
                <Icon name="close" />
              </button>
            </div>

            <div class="kui-modal__body">{props.children}</div>

            <Show when={props.actions}>
              {(actions) => <div class="kui-modal__actions">{actions()}</div>}
            </Show>
          </div>
        </div>
      </Portal>
    </Show>
  );
}

export interface ConfirmDialogProps {
  readonly open: boolean;
  readonly onClose: () => void;
  /** Runs the mutation. Keep the dialog open until it resolves; pass `busy` while it does. */
  readonly onConfirm: () => void;

  /**
   * The question, with the object named in it: `Purge orders.payments.v2?`.
   * A title with "this topic" in it is a title that has failed at its only job.
   */
  readonly title: string;
  /**
   * The consequence, in measurements. See the note at the top of this file for why adjectives are
   * not acceptable here.
   */
  readonly consequence: string;

  /** The confirm button's label: `Purge`, `Delete topic`, `Add partitions`. A verb, not "OK". */
  readonly confirmLabel: string;
  /** Destructive by default. `false` for an irreversible-but-not-destructive action. */
  readonly destructive?: boolean | undefined;
  /**
   * The glyph on the confirm button, and it is **required**.
   *
   * The destructive variant is distinguished from every other button by an outline and a colour,
   * and a colour is not a distinction to somebody who cannot see it. The glyph is the part that
   * survives — so the type refuses a confirmation that has none rather than trusting each call
   * site to remember.
   */
  readonly confirmIcon: IconName;

  /**
   * Require the operator to type this exact string before the confirm button becomes usable. Set
   * it to the object's name for delete and empty. Leave it unset for everything else — asking
   * somebody to type a name before an action they can undo teaches them to ignore the mechanism.
   */
  readonly typeToConfirm?: string | undefined;

  readonly busy?: boolean | undefined;
  /** The mutation failed. Shown above the actions; the dialog stays open. */
  readonly error?: { readonly message: string; readonly code?: string | undefined } | undefined;
  readonly testId?: string | undefined;
}

export function ConfirmDialog(props: ConfirmDialogProps): JSX.Element {
  // `ownedWrite` because the reset below happens inside an effect, which is an owned scope and
  // therefore refuses signal writes by default in Solid 2. This is the narrow case the option
  // exists for: a component resetting its own internal state, with no other writer.
  const [typed, setTyped] = createSignal("", { ownedWrite: true });

  // Reopening a confirmation must not find the previous attempt's text still in the box. The
  // dialog's *surface* is unmounted while closed, but this component is not, so the reset is
  // explicit rather than falling out of the lifecycle.
  createEffect(
    () => props.open,
    (open) => {
      if (open) setTyped("");
    },
  );
  const destructive = () => props.destructive !== false;
  const confirmationSatisfied = () =>
    props.typeToConfirm === undefined || typed() === props.typeToConfirm;
  const canConfirm = () => confirmationSatisfied() && props.busy !== true;
  /** Why the confirm button is not available. Never empty: a disabled control owes an explanation. */
  const blockedReason = (): string =>
    props.busy === true
      ? "This action is already running."
      : `Type ${props.typeToConfirm ?? ""} in the box above to enable this action.`;
  /**
   * The confirm button's disablement, as props for the one button rather than as a second button.
   * `disabled: false` is spelled out so that TypeScript keeps the two shapes `Button` accepts apart
   * instead of merging them into one with an optional `disabled`, which `ButtonProps` refuses.
   */
  const blocked = () =>
    canConfirm()
      ? { disabled: false as const }
      : { disabled: true as const, disabledReason: blockedReason() };

  let cancelButton: HTMLButtonElement | undefined;
  const inputId = nextId("kui-confirm-input");

  return (
    <Dialog
      open={props.open}
      onClose={props.onClose}
      title={props.title}
      size="sm"
      testId={props.testId}
      // A confirmation is a question, and the veil is not an answer. Cancelling has to be a thing
      // the operator did on purpose, so a stray click outside does nothing at all.
      closeOnScrimClick={false}
      initialFocus={() => cancelButton}
      actions={
        <>
          <Button variant="ghost" ref={(element: HTMLButtonElement) => (cancelButton = element)} onClick={() => props.onClose()}>
            Cancel
          </Button>
          {/*
           * Two spellings of one button, because `ButtonProps` makes `danger` a type rather than a
           * flag: it requires a glyph. Whether the button is usable is *not* a third branch. It is
           * the same element with its `disabled` and `busy` props changed, because swapping
           * elements here moved focus: pressing Confirm sets `busy`, and a `<Show>` that swapped in
           * a busy twin removed the focused button and dropped focus to `<body>`, outside the
           * dialog, where it stayed after the server refused and the dialog asked again.
           *
           * `disabled` on `Button` is the soft kind: it marks the control `aria-disabled` and
           * swallows the click rather than removing it from the tab order, so a keyboard user can
           * still reach it and hear why it is not available. That matters most in this exact case,
           * where the reason is "you have not typed the name yet". `Button` also swallows a click
           * while `busy`, so passing `onConfirm` in every state confirms nothing twice.
           */}
          <Show
            when={destructive()}
            fallback={
              <Button
                variant="primary"
                icon={props.confirmIcon}
                busy={props.busy === true}
                {...blocked()}
                onClick={() => props.onConfirm()}
              >
                {props.confirmLabel}
              </Button>
            }
          >
            <Button
              variant="danger"
              icon={props.confirmIcon}
              busy={props.busy === true}
              {...blocked()}
              onClick={() => props.onConfirm()}
            >
              {props.confirmLabel}
            </Button>
          </Show>
        </>
      }
    >
      <p class="kui-confirm__consequence">{props.consequence}</p>

      <Show when={props.typeToConfirm}>
        {(expected) => (
          <div class="kui-confirm__gate">
            <label class="kui-confirm__label" for={inputId}>
              Type <code class="kui-confirm__expected">{expected()}</code> to confirm
            </label>
            <input
              id={inputId}
              class="kui-confirm__input kui-focusable"
              type="text"
              value={typed()}
              autocomplete="off"
              spellcheck={false}
              // `aria-invalid` only once something has been typed: an empty field is not wrong,
              // it is unstarted, and announcing it as invalid on open is noise.
              aria-invalid={typed().length > 0 && typed() !== expected() ? "true" : undefined}
              onInput={(event) => setTyped(event.currentTarget.value)}
            />
          </div>
        )}
      </Show>

      <Show when={props.error}>
        {(error) => (
          // `alert` and not `status`: the operator asked for something and it did not happen, and
          // they are looking at the dialog, so interrupting is correct.
          <p class="kui-confirm__error" role="alert">
            <Icon name="error" class="kui-confirm__error-glyph" />
            <span>{error().message}</span>
            <Show when={error().code}>{(code) => <code class="kui-confirm__error-code">{code()}</code>}</Show>
          </p>
        )}
      </Show>
    </Dialog>
  );
}
