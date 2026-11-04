import { onCleanup } from "solid-js";

/**
 * Closing a panel that hangs off the control that opens it: on Escape, and on a press anywhere
 * outside the two.
 *
 * For the panels whose openness the caller holds (the notifications panel and the account panel).
 * The component draws the panel, so it is the one that can tell "inside" from "elsewhere", and it
 * asks the caller to close through `close`.
 *
 * Both listeners are on `document`. Escape has to work while focus is on `<body>`, which is where a
 * click on a button leaves it in a browser that does not focus buttons on click. A press elsewhere
 * is, by definition, not on anything this component draws.
 *
 * While a modal surface is open, its Escape is its own. The kernel's dialogs and drawers listen on
 * `document` too, and they register when they open, after this listener, so their `preventDefault`
 * arrives too late to be seen here. Without the check, one Escape would close the dialog and the
 * panel under it together. An Escape an earlier handler has already consumed is left alone as well.
 *
 * `mousedown` and not `click`, for the reason the appearance popover gives in `TopBar`: a press that
 * begins inside the panel and ends outside it is not a press elsewhere. The anchor holds the opening
 * control as well as the panel, so that control's own press stays a toggle and does not close the
 * panel only for the click to reopen it.
 */
export function dismissOnEscapeOrPressElsewhere(options: {
  readonly anchor: () => HTMLElement | undefined;
  readonly open: () => boolean;
  readonly close: () => void;
  /** Where focus goes after Escape, when it was inside and would otherwise be lost with the panel. */
  readonly returnFocus: () => HTMLElement | null | undefined;
}): void {
  const onKeyDown = (event: KeyboardEvent): void => {
    if (event.key !== "Escape" || event.defaultPrevented || !options.open()) return;
    if (document.querySelector('[aria-modal="true"]') !== null) return;
    const hadFocus = options.anchor()?.contains(document.activeElement) === true;
    options.close();
    if (hadFocus) options.returnFocus()?.focus();
  };
  const onMouseDown = (event: MouseEvent): void => {
    if (!options.open()) return;
    const target = event.target;
    if (target instanceof Node && options.anchor()?.contains(target) === true) return;
    options.close();
  };
  document.addEventListener("keydown", onKeyDown);
  document.addEventListener("mousedown", onMouseDown);
  onCleanup(() => {
    document.removeEventListener("keydown", onKeyDown);
    document.removeEventListener("mousedown", onMouseDown);
  });
}
