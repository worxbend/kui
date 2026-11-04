/**
 * Rendering, interaction and accessibility for the surfaces: the card, the stat card, the stale
 * badge, the dialog, the confirmation, the drawer, the toast, the banner and the bulk action bar.
 *
 * The windowed table's *selection* is tested here rather than beside the rest of `VirtualizedTable`
 * in `lists.test.tsx`, and deliberately: the table's checkboxes and the bar that floats over them
 * are one feature with one contract — the caller holds the set, both sides only read it — and a
 * change to that contract should break one file rather than two.
 *
 * Every case below is attached either to a statement in `.agent/design/SPEC.md` or to a defect this
 * project has already paid for. Nothing here asserts a colour, a size or a position: jsdom has no
 * layout engine, so a test that did would be asserting numbers jsdom invented. Those are judged by
 * looking at the stories against the design screenshots.
 */

import { createSignal, flush } from "solid-js";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { Banner } from "./Banner.jsx";
import { Card } from "./Card.jsx";
import { ConfirmDialog, Dialog } from "./Dialog.jsx";
import { Drawer } from "./Drawer.jsx";
import { StaleBadge, relativeAge } from "./StaleBadge.jsx";
import { BulkActionBar, type BulkAction } from "./BulkActionBar.jsx";
import { DataTable, type Column } from "./DataTable.jsx";
import { StatCard } from "./StatCard.jsx";
import { ToastRegion, clearToasts, dismissToast, notify, toasts, MAX_VISIBLE_TOASTS } from "./Toast.jsx";
import { VirtualizedTable } from "./VirtualizedTable.jsx";
import { focusableWithin } from "./overlay.js";
import { describeViolations, findViolations, mount } from "./testing.js";

/** The longest strings the product can be asked to draw. Every surface gets one of these. */
const LONG_TOPIC =
  "orders.payments.reconciliation.v2.eu-central-1.high-throughput.retry.dead-letter.compacted";
const LONG_SENTENCE =
  "The consumer service did not answer within the gateway's upstream timeout, so the figures on " +
  "this panel are the last ones it returned, and the count of groups needing attention in the " +
  "navigation drawer has been withheld rather than shown as zero.";

/**
 * A real click on a real element. See the note at the `Dialog` veil test.
 *
 * `init` carries the modifiers, because that is the only way to express them: `userEvent.click`
 * has no ⌘ and the modifier keys are what separate "open this row" from "open its link somewhere
 * else". A `MouseEvent` is what the browser delivers either way.
 */
function clickOn(element: HTMLElement, init: MouseEventInit = {}): void {
  element.dispatchEvent(new MouseEvent("click", { bubbles: true, ...init }));
}

afterEach(() => {
  clearToasts();
  vi.useRealTimers();
  // A case that mounts a modal surface and asserts on the lock has to hand the page back unlocked,
  // or the next case inherits a `<body>` nothing in it opened.
  document.body.style.overflow = "";
});

/* ------------------------------------------------------------------------------------------- */

describe("Card", () => {
  it("keeps its title and its frame in every failing state", () => {
    for (const state of ["loading", "empty", "filtered", "unavailable", "forbidden"] as const) {
      const { container, dispose } = mount(() => (
        <Card title="Consumer groups" state={state} message="Consumer group data is unavailable.">
          <p>content</p>
        </Card>
      ));
      // The frame never disappears: a page shows three healthy panels and one that failed at the
      // same time, and the failed one still has to say what it is.
      expect(container.querySelector(".kui-panel__title")!.textContent).toContain("Consumer groups");
      expect(container.querySelector(".kui-panel")).not.toBeNull();
      dispose();
    }
  });

  it("does not render its children while it is showing a state instead of content", () => {
    const { container, dispose } = mount(() => (
      <Card title="Throughput" state="unavailable" message="Metrics are unavailable.">
        <p data-testid="content">the real content</p>
      </Card>
    ));
    expect(container.querySelector('[data-testid="content"]')).toBeNull();
    dispose();
  });

  it("says something different for 'nothing yet' and for 'your filter matched nothing'", () => {
    const empty = mount(() => (
      <Card title="Topics" state="empty" message="No topics yet." description="Create one." />
    ));
    const filtered = mount(() => (
      <Card title="Topics" state="filtered" message="Nothing matched `payments`." />
    ));
    // Substituting one for the other sends somebody looking for a problem that is not there.
    expect(empty.container.textContent).toContain("No topics yet.");
    expect(filtered.container.textContent).toContain("Nothing matched");
    expect(filtered.container.textContent).not.toContain("No topics yet.");
    empty.dispose();
    filtered.dispose();
  });

  it("keeps the failure code verbatim so it can be quoted", () => {
    const { container, dispose } = mount(() => (
      <Card
        title="Consumer groups"
        state="unavailable"
        message="Consumer group data is unavailable."
        description="The consumer service is not responding."
        code="UPSTREAM_UNAVAILABLE"
      />
    ));
    // The sentence is what the operator can act on; the code is what whoever they escalate to
    // searches for. Neither is allowed to replace the other.
    expect(container.textContent).toContain("The consumer service is not responding.");
    expect(container.querySelector(".kui-empty-state__code")!.textContent).toBe("UPSTREAM_UNAVAILABLE");
    dispose();
  });

  it("marks itself busy while loading, so the skeletons are not read as the content", () => {
    const { container, dispose } = mount(() => <Card title="Broker health" state="loading" />);
    expect(container.querySelector(".kui-panel")!.getAttribute("aria-busy")).toBe("true");
    expect(container.querySelectorAll(".kui-skeleton").length).toBeGreaterThan(0);
    dispose();
  });

  it("keeps stale content on screen under a badge rather than blanking it", () => {
    const { container, dispose } = mount(() => (
      <Card
        title="Consumer lag"
        stale={{ asOf: new Date(Date.now() - 4 * 60_000), detail: "the metrics service is not answering", code: "UPSTREAM_UNAVAILABLE" }}
      >
        <p data-testid="content">4,212</p>
      </Card>
    ));
    // The last known value is more useful than nothing, provided the interface says it is the last
    // known value.
    expect(container.querySelector('[data-testid="content"]')!.textContent).toBe("4,212");
    expect(container.querySelector(".kui-stale-badge")).not.toBeNull();
    expect(container.querySelector(".kui-panel__body")!.className).toContain("kui-panel__body--stale");
    dispose();
  });

  it("has no axe violations in any state, including with a very long title", async () => {
    for (const state of ["ready", "loading", "empty", "unavailable", "forbidden"] as const) {
      const { container, dispose } = mount(() => (
        <Card title={LONG_TOPIC} state={state} message={LONG_SENTENCE} code="UPSTREAM_UNAVAILABLE">
          <p>content</p>
        </Card>
      ));
      const violations = await findViolations(container);
      expect(describeViolations(violations)).toBe("");
      dispose();
    }
  });
});

/* ------------------------------------------------------------------------------------------- */

describe("StatCard", () => {
  it("draws a zero as a zero", () => {
    const { container, dispose } = mount(() => (
      <StatCard
        label="CONSUMER LAG"
        icon="lag"
        tone="warning"
        figure={{ kind: "value", text: "0" }}
        pill={{ text: "all caught up", tone: "success" }}
      />
    ));
    // A cluster with no lag is good news, and the good news is a digit.
    expect(container.querySelector(".kui-stat__value")!.textContent).toBe("0");
    expect(container.querySelector(".kui-stat__unknown")).toBeNull();
    dispose();
  });

  it("draws an unknown as a dash and never as a zero", () => {
    const { container, dispose } = mount(() => (
      <StatCard
        label="CONSUMER LAG"
        icon="lag"
        tone="warning"
        figure={{ kind: "unknown" }}
        pill={{ text: "metrics unavailable", tone: "neutral" }}
      />
    ));
    // Printing 0 for an unknown is the most reassuring rendering of the least reassuring state.
    expect(container.querySelector(".kui-stat__unknown")!.textContent).toBe("—");
    expect(container.querySelector(".kui-stat__value")).toBeNull();
    expect(container.textContent).not.toContain("0");
    dispose();
  });

  it("draws a skeleton and marks itself busy while the figure is pending", () => {
    const { container, dispose } = mount(() => (
      <StatCard label="TOPICS" icon="topics" tone="primary" figure={{ kind: "pending" }} />
    ));
    expect(container.querySelector(".kui-stat")!.getAttribute("aria-busy")).toBe("true");
    expect(container.querySelector(".kui-skeleton")).not.toBeNull();
    // A pending value must not look like an absent one.
    expect(container.querySelector(".kui-stat__unknown")).toBeNull();
    dispose();
  });

  it("keeps the unit beside the figure rather than inside it", () => {
    const { container, dispose } = mount(() => (
      <StatCard label="PRODUCTION" icon="lag" tone="accent" figure={{ kind: "value", text: "86.4", unit: "MB/s" }} />
    ));
    expect(container.querySelector(".kui-stat__value")!.textContent).toBe("86.4");
    expect(container.querySelector(".kui-stat__unit")!.textContent).toBe("MB/s");
    dispose();
  });

  /**
   * The visual slot, with the two claims it makes.
   *
   * It renders, it sits beside the figure rather than in place of the pill, and — the part that
   * matters — the card stays `aria-busy` while the figure is pending. A card that dropped
   * `aria-busy` because there was something to look at would be telling a screen reader that the
   * number had landed.
   */
  it("renders a visual beside the figure and stays busy while the figure is pending", () => {
    const { container, dispose } = mount(() => (
      <StatCard
        label="CONSUME RATE"
        icon="lag"
        tone="primary"
        figure={{ kind: "pending" }}
        pill={{ text: "last hour", tone: "neutral" }}
        visual={<svg data-testid="spark" />}
      />
    ));
    expect(container.querySelector(".kui-stat")!.getAttribute("aria-busy")).toBe("true");
    expect(container.querySelector('[data-testid="spark"]')).not.toBeNull();
    // Beside the figure, in the same row, and the pill is still on its own line below it.
    expect(container.querySelector(".kui-stat__row .kui-stat__figure")).not.toBeNull();
    expect(container.querySelector(".kui-stat__row .kui-stat__visual")).not.toBeNull();
    expect(container.querySelector(".kui-stat__pill-slot")).not.toBeNull();
    // Whatever is in the slot is a second drawing of the figure the card already printed, and
    // announcing both reads as "128, 128".
    expect(container.querySelector(".kui-stat__visual")!.getAttribute("aria-hidden")).toBe("true");
    dispose();
  });

  /**
   * A card with no series draws no visual at all — not an empty box, and above all not a flat line
   * at zero, which is a measured claim about a quantity nobody measured.
   */
  it("draws no visual, and reserves no room for one, when there is no series", () => {
    const { container, dispose } = mount(() => (
      <StatCard
        label="PRODUCTION"
        icon="arrow-up-right"
        tone="accent"
        figure={{ kind: "unknown" }}
        pill={{ text: "metrics unavailable", tone: "neutral" }}
      />
    ));
    expect(container.querySelector(".kui-stat__visual")).toBeNull();
    // The row holds the figure and nothing else — no empty box standing where a sparkline would
    // have gone, so the absence of a series is visible rather than reserved for.
    expect(container.querySelector(".kui-stat__row")!.children).toHaveLength(1);
    dispose();
  });

  /**
   * The shape the absence actually arrives in.
   *
   * `visual={hasSeries && <Sparkline .../>}` is what a call site writes, and it evaluates to
   * `false`, not `undefined` — `JSX.Element` admits both. A guard that tested for presence would
   * reserve the empty box the component's own header forbids, and would do it only for the callers
   * who wrote the conditional the natural way. The absent-prop case above is the one that was
   * already covered, which is why this survived.
   */
  it("draws no visual for a slot that is falsy rather than absent", () => {
    for (const empty of [false, null, undefined] as const) {
      const { container, dispose } = mount(() => (
        <StatCard
          label="CONSUME RATE"
          icon="lag"
          tone="primary"
          figure={{ kind: "value", text: "71.2", unit: "MB/s" }}
          visual={empty}
        />
      ));
      expect(container.querySelector(".kui-stat__visual")).toBeNull();
      expect(container.querySelector(".kui-stat__row")!.children).toHaveLength(1);
      dispose();
    }
  });

  it("has no axe violations at the extremes", async () => {
    const { container, dispose } = mount(() => (
      <StatCard
        label="PARTITIONS UNDER MINIMUM IN-SYNC REPLICAS"
        icon="warning"
        tone="danger"
        figure={{ kind: "value", text: "18,446,744,073,709,551,615" }}
        pill={{ text: LONG_SENTENCE, tone: "danger", icon: "warning" }}
        href="/brokers"
        visual={<svg width="72" height="24" />}
      />
    ));
    const violations = await findViolations(container);
    expect(describeViolations(violations)).toBe("");
    dispose();
  });
});

/* ------------------------------------------------------------------------------------------- */

describe("StaleBadge", () => {
  it("rounds to whole units so it does not change every second", () => {
    const now = new Date("2026-09-05T12:00:00Z");
    expect(relativeAge(new Date("2026-09-05T11:59:58Z"), now)).toBe("just now");
    expect(relativeAge(new Date("2026-09-05T11:59:30Z"), now)).toBe("30s ago");
    expect(relativeAge(new Date("2026-09-05T11:56:00Z"), now)).toBe("4m ago");
    expect(relativeAge(new Date("2026-09-05T09:00:00Z"), now)).toBe("3h ago");
    expect(relativeAge(new Date("2026-09-02T12:00:00Z"), now)).toBe("3d ago");
  });

  it("carries the sentence and the code, and puts the absolute time in a title", () => {
    const asOf = new Date("2026-09-05T11:56:00Z");
    const { container, dispose } = mount(() => (
      <StaleBadge
        asOf={asOf}
        now={new Date("2026-09-05T12:00:00Z")}
        detail="the metrics service is not answering"
        code="UPSTREAM_UNAVAILABLE"
      />
    ));
    expect(container.textContent).toContain("the metrics service is not answering");
    expect(container.querySelector(".kui-stale-badge__code")!.textContent).toBe("UPSTREAM_UNAVAILABLE");
    const time = container.querySelector("time")!;
    expect(time.getAttribute("datetime")).toBe(asOf.toISOString());
    expect(time.getAttribute("title")).toBe(asOf.toLocaleString());
    dispose();
  });
});

/* ------------------------------------------------------------------------------------------- */

describe("Dialog", () => {
  it("is absent from the document while closed, not merely hidden", () => {
    const { dispose } = mount(() => (
      <Dialog open={false} onClose={() => {}} title="Create topic">
        <input data-testid="field" />
      </Dialog>
    ));
    // Hidden is not enough: a hidden form is still submittable and still found by find-in-page.
    expect(document.querySelector('[data-testid="field"]')).toBeNull();
    dispose();
  });

  it("moves focus inside on open and gives it back on close", async () => {
    const [open, setOpen] = createSignal(true, { ownedWrite: true });
    const opener = document.createElement("button");
    document.body.appendChild(opener);
    opener.focus();

    const { dispose } = mount(() => (
      <Dialog open={open()} onClose={() => setOpen(false)} title="Create topic">
        <input data-testid="field" />
      </Dialog>
    ));
    await Promise.resolve();
    // The surface itself, not its first control: a screen reader then reads the dialog's name and
    // description before anything else, instead of announcing "Close button" and leaving the
    // reader to go looking for the question.
    expect(document.activeElement).toBe(document.querySelector('[role="dialog"]'));

    setOpen(false);
    flush();
    await Promise.resolve();
    // Losing focus on close drops the user at the top of the document, which after a confirmation
    // on row 400 of a table is a small catastrophe.
    expect(document.activeElement).toBe(opener);

    opener.remove();
    dispose();
  });

  it("wraps Tab at both ends so focus cannot walk out onto the page behind", async () => {
    const user = userEvent.setup();
    const { dispose } = mount(() => (
      <Dialog open onClose={() => {}} title="Create topic" actions={<button type="button">Create</button>}>
        <input data-testid="field" />
      </Dialog>
    ));
    await Promise.resolve();

    const stops = Array.from(
      (document.querySelector('[role="dialog"]') as HTMLElement).querySelectorAll("button, input"),
    ) as HTMLElement[];
    const first = stops[0] as HTMLElement;
    const last = stops[stops.length - 1] as HTMLElement;

    last.focus();
    await user.tab();
    // Past the last control, back to the first — never out into a page the user cannot see and
    // cannot get back from.
    expect(document.activeElement).toBe(first);

    await user.tab({ shift: true });
    expect(document.activeElement).toBe(last);
    dispose();
  });

  it("pulls Tab back inside when focus has fallen out of the surface", async () => {
    // Focus lands on `<body>` when a focused control inside the surface is removed. The wrap above
    // only acts from the surface's own first or last stop, so from `<body>` the browser's default
    // Tab ran and walked out into the page behind the aria-modal surface.
    const user = userEvent.setup();
    const behind = document.createElement("button");
    behind.textContent = "Behind the dialog";
    document.body.prepend(behind);
    const { dispose } = mount(() => (
      <Dialog open onClose={() => {}} title="Create topic" actions={<button type="button">Create</button>}>
        <input data-testid="field" />
      </Dialog>
    ));
    try {
      await Promise.resolve();

      const dialog = document.querySelector('[role="dialog"]') as HTMLElement;
      const stops = focusableWithin(dialog);
      const first = stops[0] as HTMLElement;
      const last = stops[stops.length - 1] as HTMLElement;

      (document.activeElement as HTMLElement).blur();
      expect(document.activeElement).toBe(document.body);
      await user.tab();
      expect(document.activeElement).toBe(first);

      (document.activeElement as HTMLElement).blur();
      await user.tab({ shift: true });
      expect(document.activeElement).toBe(last);
    } finally {
      // In `finally`: the surface is portalled onto `<body>`, and one left behind by a failing case
      // is what the next case's `querySelector` would find.
      behind.remove();
      dispose();
    }
  });

  it("puts focus where the caller asks", async () => {
    let field: HTMLInputElement | undefined;
    const { dispose } = mount(() => (
      <Dialog open onClose={() => {}} title="Create topic" initialFocus={() => field ?? null}>
        <input data-testid="field" ref={(element: HTMLInputElement) => (field = element)} />
      </Dialog>
    ));
    await Promise.resolve();
    expect(document.activeElement).toBe(document.querySelector('[data-testid="field"]'));
    dispose();
  });

  it("closes on Escape", async () => {
    const user = userEvent.setup();
    let closed = 0;
    const { dispose } = mount(() => (
      <Dialog open onClose={() => (closed += 1)} title="Create topic">
        <input data-testid="field" />
      </Dialog>
    ));
    await Promise.resolve();
    await user.keyboard("{Escape}");
    expect(closed).toBe(1);
    dispose();
  });

  it("closes when the veil itself is clicked, and not when the surface is", async () => {
    const user = userEvent.setup();
    let closed = 0;
    const { dispose } = mount(() => (
      <Dialog open onClose={() => (closed += 1)} title="Create topic">
        <p data-testid="body">body</p>
      </Dialog>
    ));
    await Promise.resolve();
    await user.click(document.querySelector('[data-testid="body"]') as HTMLElement);
    expect(closed).toBe(0);
    // Dispatched natively rather than through `userEvent`, which decides where a click lands from
    // layout — and jsdom has none, so it never reaches an element that covers the window.
    clickOn(document.querySelector(".kui-modal-scrim") as HTMLElement);
    expect(closed).toBe(1);
    dispose();
  });

  it("names itself from its title and describes itself from its description", async () => {
    const { dispose } = mount(() => (
      <Dialog open onClose={() => {}} title="Create topic" description="Topics cannot be renamed.">
        <input />
      </Dialog>
    ));
    await Promise.resolve();
    const dialog = document.querySelector('[role="dialog"]') as HTMLElement;
    expect(dialog.getAttribute("aria-modal")).toBe("true");
    const labelled = document.getElementById(dialog.getAttribute("aria-labelledby") as string);
    const described = document.getElementById(dialog.getAttribute("aria-describedby") as string);
    expect(labelled!.textContent).toBe("Create topic");
    expect(described!.textContent).toBe("Topics cannot be renamed.");
    dispose();
  });

  it("has no axe violations", async () => {
    const { dispose } = mount(() => (
      <Dialog open onClose={() => {}} title={LONG_TOPIC} description={LONG_SENTENCE} actions={<button type="button">OK</button>}>
        <p>{LONG_SENTENCE}</p>
      </Dialog>
    ));
    await Promise.resolve();
    const violations = await findViolations(document.querySelector(".kui-modal-scrim") as HTMLElement);
    expect(describeViolations(violations)).toBe("");
    dispose();
  });
});

/* ------------------------------------------------------------------------------------------- */

describe("ConfirmDialog", () => {
  const props = {
    title: `Purge ${LONG_TOPIC}?`,
    consequence: "This deletes 1,536 partitions' worth of records — about 128 GB. It cannot be undone.",
    confirmLabel: "Purge",
    confirmIcon: "trash",
  } as const;

  it("opens with focus on Cancel, not on the button that destroys something", async () => {
    const { dispose } = mount(() => (
      <ConfirmDialog open onClose={() => {}} onConfirm={() => {}} {...props} />
    ));
    await Promise.resolve();
    // The keystroke that opened the dialog is often still going.
    expect((document.activeElement as HTMLElement).textContent).toContain("Cancel");
    dispose();
  });

  it("refuses to confirm until the object's name is typed exactly", async () => {
    const user = userEvent.setup();
    let confirmed = 0;
    const { dispose } = mount(() => (
      <ConfirmDialog open onClose={() => {}} onConfirm={() => (confirmed += 1)} {...props} typeToConfirm="orders.payments.v2" />
    ));
    await Promise.resolve();

    const confirm = () =>
      Array.from(document.querySelectorAll("button")).find((b) => b.textContent?.includes("Purge")) as HTMLButtonElement;

    expect(confirm().getAttribute("aria-disabled")).toBe("true");
    await user.click(confirm());
    expect(confirmed).toBe(0);

    const input = document.querySelector(".kui-confirm__input") as HTMLInputElement;
    await user.type(input, "orders.payments.v1");
    flush();
    expect(confirm().getAttribute("aria-disabled")).toBe("true");

    await user.clear(input);
    await user.type(input, "orders.payments.v2");
    flush();
    await Promise.resolve();
    expect(confirm().getAttribute("aria-disabled")).toBeNull();
    await user.click(confirm());
    expect(confirmed).toBe(1);
    dispose();
  });

  it("keeps the blocked confirm button reachable and explained", async () => {
    const { dispose } = mount(() => (
      <ConfirmDialog open onClose={() => {}} onConfirm={() => {}} {...props} typeToConfirm="orders.payments.v2" />
    ));
    await Promise.resolve();
    const confirm = Array.from(document.querySelectorAll("button")).find((b) =>
      b.textContent?.includes("Purge"),
    ) as HTMLButtonElement;
    // `aria-disabled` rather than `disabled`, so a keyboard user can reach it and hear the reason.
    expect(confirm.hasAttribute("disabled")).toBe(false);
    expect(confirm.getAttribute("aria-disabled")).toBe("true");
    dispose();
  });

  it("keeps focus on the confirm button while the mutation runs and after it is refused", async () => {
    // The defect: the enabled and the busy confirm buttons were two different elements behind a
    // `<Show>`, so pressing Confirm unmounted the focused button and dropped focus to `<body>`,
    // outside the aria-modal dialog, with no focus ring and nothing for Enter to press. It stayed
    // there after the server refused and the dialog came back with the error.
    const [busy, setBusy] = createSignal(false, { ownedWrite: true });
    const [error, setError] = createSignal<{ readonly message: string } | undefined>(undefined, {
      ownedWrite: true,
    });
    const { dispose } = mount(() => (
      <ConfirmDialog
        open
        onClose={() => {}}
        onConfirm={() => setBusy(true)}
        {...props}
        busy={busy()}
        error={error()}
      />
    ));
    try {
      await Promise.resolve();
      const dialog = document.querySelector('[role="dialog"]') as HTMLElement;
      const confirm = (): HTMLButtonElement =>
        Array.from(dialog.querySelectorAll("button")).find((b) =>
          b.textContent?.includes("Purge"),
        ) as HTMLButtonElement;

      const pressed = confirm();
      pressed.focus();
      clickOn(pressed);
      flush();
      expect(confirm().getAttribute("aria-busy")).toBe("true");
      expect(document.activeElement).toBe(pressed);

      setBusy(false);
      setError({ message: "The topic service refused the purge." });
      flush();
      expect(confirm().getAttribute("aria-busy")).toBeNull();
      expect(document.activeElement).toBe(pressed);
      expect(dialog.contains(document.activeElement)).toBe(true);
    } finally {
      dispose();
    }
  });

  it("draws the destructive action with the danger silhouette and a glyph", async () => {
    const { dispose } = mount(() => <ConfirmDialog open onClose={() => {}} onConfirm={() => {}} {...props} />);
    await Promise.resolve();
    const confirm = Array.from(document.querySelectorAll("button")).find((b) =>
      b.textContent?.includes("Purge"),
    ) as HTMLButtonElement;
    // The outline alone would be a colour-only distinction, so the glyph is required as well.
    expect(confirm.className).toContain("kui-btn--danger");
    expect(confirm.querySelector("svg")).not.toBeNull();
    dispose();
  });

  it("stays open when the mutation fails, and announces the failure with its code", async () => {
    const { dispose } = mount(() => (
      <ConfirmDialog
        open
        onClose={() => {}}
        onConfirm={() => {}}
        {...props}
        error={{ message: "The topic service refused the purge.", code: "MUTATION_REJECTED" }}
      />
    ));
    await Promise.resolve();
    const alert = document.querySelector('[role="alert"]') as HTMLElement;
    expect(alert.textContent).toContain("The topic service refused the purge.");
    expect(alert.textContent).toContain("MUTATION_REJECTED");
    // Throwing the dialog away on failure makes the operator reconstruct what they were doing in
    // order to find out that it did not happen.
    expect(document.querySelector('[role="dialog"]')).not.toBeNull();
    dispose();
  });

  it("has no axe violations while blocked", async () => {
    const { dispose } = mount(() => (
      <ConfirmDialog open onClose={() => {}} onConfirm={() => {}} {...props} typeToConfirm="orders.payments.v2" />
    ));
    await Promise.resolve();
    const violations = await findViolations(document.querySelector(".kui-modal-scrim") as HTMLElement);
    expect(describeViolations(violations)).toBe("");
    dispose();
  });
});

/* ------------------------------------------------------------------------------------------- */

describe("Drawer", () => {
  it("ignores a click on the veil, because it usually holds something somebody typed", async () => {
    const user = userEvent.setup();
    let closed = 0;
    const { dispose } = mount(() => (
      <Drawer open onClose={() => (closed += 1)} title="Produce to orders.payments.v2">
        <textarea data-testid="editor" />
      </Drawer>
    ));
    await Promise.resolve();
    clickOn(document.querySelector(".kui-sheet-scrim") as HTMLElement);
    expect(closed).toBe(0);
    // Escape is still a deliberate act and still closes it.
    await user.keyboard("{Escape}");
    expect(closed).toBe(1);
    dispose();
  });

  it("does not rebuild the form when something else in the drawer changes", async () => {
    const user = userEvent.setup();
    const [serdes, setSerdes] = createSignal<readonly string[]>([], { ownedWrite: true });

    const { dispose } = mount(() => (
      <Drawer
        open
        onClose={() => {}}
        title="Produce to orders.payments.v2"
        footer={<span data-testid="serde-count">{serdes().length} serdes</span>}
      >
        {/* The async region is around the select only, never around the editor. */}
        <select data-testid="serde">
          {serdes().map((serde) => (
            <option>{serde}</option>
          ))}
        </select>
        <textarea data-testid="editor" />
      </Drawer>
    ));
    await Promise.resolve();

    const editor = document.querySelector('[data-testid="editor"]') as HTMLTextAreaElement;
    // The braces are doubled because userEvent reads `{` as the start of a key descriptor.
    await user.type(editor, '{{"id":42}');

    // The serdes arrive after the drawer opened. This is the exact moment the shipped defect threw
    // the payload away.
    setSerdes(["String", "JSON", "Avro"]);
    flush();

    const editorAfter = document.querySelector('[data-testid="editor"]') as HTMLTextAreaElement;
    expect(editorAfter).toBe(editor);
    expect(editorAfter.value).toBe('{"id":42}');
    expect(document.querySelector('[data-testid="serde-count"]')!.textContent).toBe("3 serdes");
    dispose();
  });

  it("a nested overlay closing does not unlock the page beneath it", async () => {
    const [dialogOpen, setDialogOpen] = createSignal(true, { ownedWrite: true });

    // A drawer with a confirmation over it — produce a record, then confirm the topic it goes to.
    // Both surfaces lock the page, and the inner one is closed first, which is the sequence no
    // test drove and the one the restore was written for.
    const { dispose } = mount(() => (
      <Drawer open onClose={() => {}} title="Produce to orders.payments.v2">
        <textarea data-testid="editor" />
        <Dialog open={dialogOpen()} onClose={() => setDialogOpen(false)} title="Are you sure?">
          <p>This writes to a production topic.</p>
        </Dialog>
      </Drawer>
    ));
    await Promise.resolve();
    expect(document.body.style.overflow).toBe("hidden");

    setDialogOpen(false);
    flush();
    await Promise.resolve();

    // Restored to what it was when the dialog opened, not to "". The drawer is still on screen
    // over the page, and unlocking here leaves it floating over a document that scrolls underneath
    // it — the whole page moving behind a surface the user has not finished with.
    expect(document.querySelector(".kui-modal")).toBeNull();
    expect(document.querySelector(".kui-sheet")).not.toBeNull();
    expect(document.body.style.overflow).toBe("hidden");

    dispose();
    await Promise.resolve();
    // And the last one out really does give the page back.
    expect(document.body.style.overflow).toBe("");
  });

  it("a nested dialog answers Escape and the drawer under it does not", async () => {
    const user = userEvent.setup();
    const [dialogOpen, setDialogOpen] = createSignal(true, { ownedWrite: true });
    let drawerClosed = 0;
    let dialogClosed = 0;

    const { dispose } = mount(() => (
      <Drawer open onClose={() => (drawerClosed += 1)} title="Produce to orders.payments.v2">
        <textarea data-testid="editor" />
        <Dialog
          open={dialogOpen()}
          onClose={() => {
            dialogClosed += 1;
            setDialogOpen(false);
          }}
          title="Are you sure?"
        >
          <p>This writes to a production topic.</p>
        </Dialog>
      </Drawer>
    ));
    await Promise.resolve();

    await user.keyboard("{Escape}");
    flush();

    // Both traps listen on `document`, so `stopPropagation` cannot keep one key away from the
    // other — measured before this case existed: one Escape closed both, and the operator lost the
    // record they had typed along with the confirmation they were answering.
    expect([dialogClosed, drawerClosed]).toEqual([1, 0]);

    // And the drawer takes it back once it is the only thing trapping the page.
    await user.keyboard("{Escape}");
    flush();
    expect(drawerClosed).toBe(1);
    dispose();
  });

  it("a nested overlay torn down out of order leaves the stack correct", async () => {
    const user = userEvent.setup();
    let drawerClosed = 0;
    let dialogClosed = 0;

    // Two roots rather than one tree, because the sequence being asserted is the one where the
    // *outer* surface's cleanup runs first: a route change tears the drawer down while the
    // confirmation opened from inside it is still on screen, and nothing orders the two cleanups.
    const drawer = mount(() => (
      <Drawer open onClose={() => (drawerClosed += 1)} title="Produce to orders.payments.v2">
        <textarea data-testid="editor" />
      </Drawer>
    ));
    await Promise.resolve();
    const dialog = mount(() => (
      <Dialog open onClose={() => (dialogClosed += 1)} title="Are you sure?">
        <p>This writes to a production topic.</p>
      </Dialog>
    ));
    await Promise.resolve();

    drawer.dispose();
    await Promise.resolve();

    await user.keyboard("{Escape}");
    flush();

    // The dialog is the only surface left trapping the page, so it answers. Removing the *last*
    // entry instead of this element's takes the dialog's own entry off the stack and leaves the
    // dead drawer on top of it: the dialog then ignores Escape for the rest of its life, and so
    // does everything opened over it afterwards, with nothing on screen to say why.
    expect([dialogClosed, drawerClosed]).toEqual([1, 0]);
    dialog.dispose();
    await Promise.resolve();
    expect(document.body.style.overflow).toBe("");
  });

  it("only the innermost surface answers Tab", async () => {
    const drawer = mount(() => (
      <Drawer open onClose={() => {}} title="Produce to orders.payments.v2">
        <textarea data-testid="editor" />
        <button type="button">Produce</button>
      </Drawer>
    ));
    await Promise.resolve();
    const dialog = mount(() => (
      <Dialog open onClose={() => {}} title="Are you sure?">
        <button type="button">Confirm</button>
      </Dialog>
    ));
    await Promise.resolve();

    // The drawer's *last* stop, which is where its own wrap fires: focus lands back there whenever
    // a control inside it is removed while focused, or after a stray click on the surface.
    const sheet = document.querySelector(".kui-sheet") as HTMLElement;
    const stops = focusableWithin(sheet);
    const lastInDrawer = stops[stops.length - 1] as HTMLElement;
    let drawerWrapped = 0;
    stops[0]?.addEventListener("focus", () => (drawerWrapped += 1));
    lastInDrawer.focus();
    expect(document.activeElement).toBe(lastInDrawer);

    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Tab", bubbles: true }));
    flush();

    // The drawer is not the surface trapping the page, so it does not wrap. Left unscoped it runs
    // its own trap on every Tab and pulls focus round to its own first control — a surface the
    // operator is not looking at, while the dialog over it owns the keyboard. Counted as focus
    // events rather than read off `activeElement`, because the dialog moves focus afterwards.
    expect(drawerWrapped).toBe(0);
    // The dialog does answer it. Focus outside the innermost surface is brought into that surface:
    // this used to assert that focus stayed in the drawer, which is the trap letting focus sit
    // behind an aria-modal dialog for the browser's own Tab to carry further out.
    const inner = document.querySelector(".kui-modal") as HTMLElement;
    expect(document.activeElement).toBe(focusableWithin(inner)[0]);

    dialog.dispose();
    await Promise.resolve();
    drawer.dispose();
    await Promise.resolve();
  });

  it("has no axe violations", async () => {
    const { dispose } = mount(() => (
      <Drawer
        open
        onClose={() => {}}
        title={`Produce to ${LONG_TOPIC}`}
        description={LONG_SENTENCE}
        error={{ message: "The broker rejected the record.", code: "RECORD_TOO_LARGE" }}
        footer={<button type="button">Produce</button>}
      >
        <label for="v">Value</label>
        <textarea id="v" />
      </Drawer>
    ));
    await Promise.resolve();
    const violations = await findViolations(document.querySelector(".kui-sheet-scrim") as HTMLElement);
    expect(describeViolations(violations)).toBe("");
    dispose();
  });
});

/* ------------------------------------------------------------------------------------------- */

describe("Toast", () => {
  it("never lets a failure disappear on a timer", () => {
    notify("Could not create topic", { tone: "danger", durationMs: 1000, code: "UPSTREAM_UNAVAILABLE" });
    // Solid 2 batches writes onto a microtask, so the store is not readable until it is drained.
    flush();
    // The caller asked for a second. A failure carries a code the reader may need to write down,
    // so the request is overruled here rather than trusted at every call site.
    expect(toasts()[0]!.durationMs).toBeNull();
  });

  it("dismisses a confirmation once its time is up, and not before", () => {
    vi.useFakeTimers();
    const { dispose } = mount(() => <ToastRegion />);
    notify("Topic created", { durationMs: 1000 });
    flush();
    expect(toasts()).toHaveLength(1);

    vi.advanceTimersByTime(500);
    expect(toasts()).toHaveLength(1);
    vi.advanceTimersByTime(600);
    flush();
    expect(toasts()).toHaveLength(0);
    dispose();
  });

  it("pauses the timer while the pointer is on the stack", () => {
    vi.useFakeTimers();
    const { dispose } = mount(() => <ToastRegion />);
    notify("Topic created", { durationMs: 1000 });
    flush();

    const stack = document.querySelector(".kui-notice-stack") as HTMLElement;
    stack.dispatchEvent(new MouseEvent("mouseenter", { bubbles: false }));
    flush();
    vi.advanceTimersByTime(5000);
    flush();
    // Somebody reaching for the dismiss button must not have the toast vanish out from under them.
    expect(toasts()).toHaveLength(1);

    stack.dispatchEvent(new MouseEvent("mouseleave", { bubbles: false }));
    flush();
    vi.advanceTimersByTime(1100);
    flush();
    expect(toasts()).toHaveLength(0);
    dispose();
  });

  it("keeps the live region in the document when there is nothing to say", () => {
    const { container, dispose } = mount(() => <ToastRegion />);
    // A live region created at the moment it receives its first message is, in several
    // screen-reader and browser combinations, not announced at all.
    const region = container.querySelector(".kui-notice-stack") as HTMLElement;
    expect(region).not.toBeNull();
    expect(region.getAttribute("aria-live")).toBe("polite");
    dispose();
  });

  it("shows a bounded number and says how many more are waiting", () => {
    const { container, dispose } = mount(() => <ToastRegion />);
    for (let i = 0; i < MAX_VISIBLE_TOASTS + 2; i += 1) notify(`Topic ${i} created`, { durationMs: null });
    flush();
    expect(container.querySelectorAll(".kui-notice")).toHaveLength(MAX_VISIBLE_TOASTS);
    expect(container.querySelector(".kui-notice-stack__queued")!.textContent).toBe("2 more");
    dispose();
  });

  it("names what each dismiss button dismisses", () => {
    const { container, dispose } = mount(() => <ToastRegion />);
    notify("Topic created", { durationMs: null });
    flush();
    const dismiss = container.querySelector(".kui-notice__dismiss") as HTMLElement;
    // Three identical "Close" buttons in a stack are three buttons nobody can tell apart.
    expect(dismiss.getAttribute("aria-label")).toBe("Dismiss: Topic created");
    dispose();
  });

  it("dismisses the toast the button belongs to when it is pressed", () => {
    // Filed by this wave's verification pass over W12-04 and reproduced here before it was closed:
    // `onClick={() => dismissToast(props.toast.id)}` -> `onClick={() => {}}` left `pnpm -C frontend test`
    // green over 83 files and 1,932 cases. The dismiss button's LABEL had a case and its BEHAVIOUR had
    // none — the case above reads `aria-label` off the same element and never presses it, and
    // `dismissToast` is exercised only by calling it directly, which asserts the function and not the
    // control. A close button that does nothing is the one defect a toast cannot survive: it is the only
    // way out of a notification raised with `durationMs: null`.
    const { container, dispose } = mount(() => <ToastRegion />);
    notify("Topic created", { durationMs: null });
    notify("Offsets reset", { durationMs: null });
    flush();
    const buttons = container.querySelectorAll(".kui-notice__dismiss");
    // The anchor: with no button found, every expectation below would hold over nothing.
    expect(buttons).toHaveLength(2);

    clickOn(buttons[0] as HTMLElement);
    flush();

    // And it dismisses ITS OWN toast, not merely one of them: a handler closing over the wrong id
    // empties the stack in the wrong order and is invisible to a length check alone.
    expect(toasts().map((t) => t.title)).toEqual(["Offsets reset"]);
    expect(container.querySelectorAll(".kui-notice")).toHaveLength(1);
    dispose();
  });

  it("keeps both when two are raised in the same tick", () => {
    // Solid 2 batches writes onto a microtask and applies an updater to the last *committed*
    // value, so `setToasts(prev => [...prev, mine])` twice in one tick would have both computed
    // from the empty list and the second would have won. One confirmation would vanish, silently,
    // and only when two things happened at once — which is when it matters most.
    notify("Topic created", { durationMs: null });
    notify("Offsets reset", { durationMs: null });
    flush();
    expect(toasts().map((t) => t.title)).toEqual(["Topic created", "Offsets reset"]);
  });

  it("removes the one asked for and leaves the rest", () => {
    const first = notify("A", { durationMs: null });
    notify("B", { durationMs: null });
    dismissToast(first);
    flush();
    expect(toasts().map((t) => t.title)).toEqual(["B"]);
  });

  it("has no axe violations", async () => {
    const { container, dispose } = mount(() => <ToastRegion />);
    notify("Could not produce the record", {
      tone: "danger",
      message: LONG_SENTENCE,
      code: "RECORD_TOO_LARGE",
      action: { label: "Retry", onClick: () => {} },
    });
    flush();
    const violations = await findViolations(container);
    expect(describeViolations(violations)).toBe("");
    dispose();
  });
});

/* ------------------------------------------------------------------------------------------- */

describe("Banner", () => {
  it("interrupts for a failure and waits its turn for anything else", () => {
    const danger = mount(() => <Banner tone="danger" message="The cluster is not answering." />);
    const info = mount(() => <Banner tone="info" message="KUI is in read-only mode." />);
    expect(danger.container.querySelector(".kui-banner")!.getAttribute("role")).toBe("alert");
    // Making everything an alert means the first one is the only one anybody hears.
    expect(info.container.querySelector(".kui-banner")!.getAttribute("role")).toBe("status");
    danger.dispose();
    info.dispose();
  });

  it("keeps the code beside the sentence", () => {
    const { container, dispose } = mount(() => (
      <Banner tone="danger" message="The cluster is not answering." code="CLUSTER_UNREACHABLE" />
    ));
    expect(container.querySelector(".kui-banner__code")!.textContent).toBe("CLUSTER_UNREACHABLE");
    dispose();
  });

  it("is only dismissible when it is given a way to be", async () => {
    const user = userEvent.setup();
    let dismissed = 0;
    const fixed = mount(() => <Banner tone="danger" message="The cluster is not answering." />);
    expect(fixed.container.querySelector(".kui-banner__dismiss")).toBeNull();
    fixed.dispose();

    const closable = mount(() => (
      <Banner tone="info" message="KUI is in read-only mode." onDismiss={() => (dismissed += 1)} />
    ));
    await user.click(closable.container.querySelector(".kui-banner__dismiss") as HTMLElement);
    expect(dismissed).toBe(1);
    closable.dispose();
  });

  it("has no axe violations", async () => {
    const { container, dispose } = mount(() => (
      <Banner
        tone="danger"
        message={LONG_SENTENCE}
        code="CLUSTER_UNREACHABLE"
        action={<button type="button">Retry</button>}
        onDismiss={() => {}}
      />
    ));
    const violations = await findViolations(container);
    expect(describeViolations(violations)).toBe("");
    dispose();
  });
});

/* ------------------------------------------------------------------------------------------- */

describe("BulkActionBar", () => {
  const actions: readonly BulkAction[] = [
    { id: "config", label: "Edit config", icon: "settings", onSelect: () => {} },
    { id: "delete", label: "Delete", icon: "trash", destructive: true, onSelect: () => {} },
  ];

  /**
   * Absent means absent. A bar that is always in the document is a strip of the window nobody can
   * use, and one that is merely invisible still swallows the clicks meant for the row underneath.
   */
  it("renders nothing at all at zero selection", () => {
    const { container, dispose } = mount(() => (
      <BulkActionBar count={0} noun="topic" actions={actions} onDismiss={() => {}} />
    ));
    expect(container.querySelector(".kui-bulkbar")).toBeNull();
    expect(container.textContent).toBe("");
    dispose();
  });

  it("says what was selected as well as how many, and agrees with itself about number", () => {
    const [count, setCount] = createSignal(1);
    const { container, dispose } = mount(() => (
      <BulkActionBar count={count()} noun="topic" actions={actions} onDismiss={() => {}} />
    ));
    // "1 topics selected" is the kind of detail that is read as carelessness about everything else
    // on the screen.
    expect(container.querySelector(".kui-bulkbar__count")!.textContent).toBe("1 topic selected");
    setCount(4);
    flush();
    expect(container.querySelector(".kui-bulkbar__count")!.textContent).toBe("4 topics selected");
    dispose();
  });

  /**
   * The rule the component exists to enforce. If Delete disappeared for a principal without the
   * permission, Purge would slide into its place, and one gesture would purge for one operator and
   * delete for another.
   */
  it("disables an action nobody may take, with its reason, and does not hide it", async () => {
    const onSelect = vi.fn();
    const { container, dispose } = mount(() => (
      <BulkActionBar
        count={2}
        noun="topic"
        onDismiss={() => {}}
        actions={[
          actions[0] as BulkAction,
          {
            id: "delete",
            label: "Delete",
            icon: "trash",
            destructive: true,
            disabledReason: "You do not have permission to delete topics on this cluster.",
            onSelect,
          },
        ]}
      />
    ));

    const buttons = Array.from(container.querySelectorAll("button"));
    const remove = buttons.find((button) => button.textContent?.includes("Delete"));
    expect(remove).toBeDefined();
    // Second of the two actions, exactly where it is for everybody else.
    expect(buttons.indexOf(remove!)).toBe(1);
    expect(remove!.getAttribute("aria-disabled")).toBe("true");
    // Focusable, or the explanation is unreachable by the people who need it.
    expect(remove!.disabled).toBe(false);

    await userEvent.click(remove!);
    flush();
    expect(onSelect).not.toHaveBeenCalled();

    const described = document.getElementById(remove!.getAttribute("aria-describedby") ?? "");
    expect(described?.textContent).toContain("permission to delete topics");
    dispose();
  });

  it("runs an action the principal may take, and clears on dismiss", async () => {
    const onSelect = vi.fn();
    const onDismiss = vi.fn();
    const { container, dispose } = mount(() => (
      <BulkActionBar
        count={2}
        noun="topic"
        onDismiss={onDismiss}
        actions={[{ id: "purge", label: "Purge", icon: "trash", destructive: true, onSelect }]}
      />
    ));
    const buttons = Array.from(container.querySelectorAll("button"));
    await userEvent.click(buttons[0]!);
    flush();
    expect(onSelect).toHaveBeenCalledTimes(1);

    // The dismiss is named by the action, not by the glyph: "Clear selection", never "close".
    const dismiss = buttons.find((button) => button.textContent?.includes("Clear selection"));
    expect(dismiss).toBeDefined();
    await userEvent.click(dismiss!);
    flush();
    expect(onDismiss).toHaveBeenCalledTimes(1);
    dispose();
  });

  it("has no axe violations, including with an action nobody may take", async () => {
    const { container, dispose } = mount(() => (
      <BulkActionBar
        count={128}
        noun="topic"
        onDismiss={() => {}}
        actions={[
          actions[0] as BulkAction,
          { id: "purge", label: "Purge", icon: "trash", destructive: true, onSelect: () => {} },
          {
            id: "delete",
            label: "Delete",
            icon: "trash",
            destructive: true,
            disabledReason: LONG_SENTENCE,
            onSelect: () => {},
          },
        ]}
      />
    ));
    const violations = await findViolations(container);
    expect(describeViolations(violations)).toBe("");
    dispose();
  });
});

/* ------------------------------------------------------------------------------------------- */

/**
 * Selection on the windowed table.
 *
 * These cases live beside `BulkActionBar` rather than beside the rest of `VirtualizedTable`
 * because they are the other half of one contract: the caller holds the set, the table reads it
 * and the bar reads it, and neither of them prunes it. See the note at the top of this file.
 *
 * The window is moved with the keyboard rather than by scrolling. jsdom has no layout engine, so a
 * scroll event has nothing to report; `End` and `Home` go through the same `scrollToPx` the
 * scrollbar does, which is the code path a test can actually drive.
 */
describe("VirtualizedTable selection", () => {
  interface Topic {
    readonly name: string;
  }
  const rows: readonly Topic[] = Array.from({ length: 400 }, (_, index) => ({
    name: `topic-${index}`,
  }));
  const columns: readonly Column<Topic>[] = [
    { id: "name", header: "Topic", render: (topic) => topic.name },
  ];
  const base = {
    columns,
    rows,
    rowKey: (topic: Topic) => topic.name,
    caption: "Topics",
    viewportHeight: 240,
    // Pinned, so the arithmetic below does not change with the document's density attribute.
    compact: false,
  };

  it("keeps a selected key that scrolls out of the window", async () => {
    const [selected, setSelected] = createSignal<ReadonlySet<string>>(new Set(["topic-0"]));
    const { container, dispose } = mount(() => (
      <VirtualizedTable
        {...base}
        selection={{ selectedKeys: selected(), onChange: setSelected }}
      />
    ));
    expect(container.querySelectorAll(".kui-table__row--selected")).toHaveLength(1);

    container.querySelector<HTMLElement>(".kui-vtable__row")!.focus();
    await userEvent.keyboard("{End}");
    flush();

    // The row has genuinely left the document — which is what makes the next assertion mean
    // something rather than being a test of a row that never moved.
    expect(container.textContent).not.toContain("topic-0");
    expect(selected().has("topic-0")).toBe(true);

    await userEvent.keyboard("{Home}");
    flush();
    expect(container.textContent).toContain("topic-0");
    expect(container.querySelectorAll(".kui-table__row--selected")).toHaveLength(1);
    dispose();
  });

  /**
   * Select-all walks the rows the table was handed, never the rows it can see. The obvious
   * implementation walks the window, and it silently discards every selection made further up the
   * list — including, here, one made on a page this table has never rendered.
   */
  it("selects the whole page without disturbing a key from another page", async () => {
    const [selected, setSelected] = createSignal<ReadonlySet<string>>(
      new Set(["a-topic-from-page-two"]),
    );
    const { container, dispose } = mount(() => (
      <VirtualizedTable
        {...base}
        selection={{ selectedKeys: selected(), onChange: setSelected }}
      />
    ));
    await userEvent.click(container.querySelector<HTMLInputElement>('[data-testid="select-all"]')!);
    flush();

    expect(selected().size).toBe(rows.length + 1);
    expect(selected().has("a-topic-from-page-two")).toBe(true);
    // Including rows that were never in the document.
    expect(selected().has("topic-399")).toBe(true);
    dispose();
  });

  /**
   * Mixed against the page, and only ever against the page. A header that went checked while five
   * hundred of ten thousand topics were ticked would be a claim that the next Delete acts on all
   * ten thousand.
   */
  it("shows the header checkbox mixed for a partial page selection", async () => {
    const [selected, setSelected] = createSignal<ReadonlySet<string>>(new Set(["topic-1"]));
    const { container, dispose } = mount(() => (
      <VirtualizedTable
        {...base}
        selection={{ selectedKeys: selected(), onChange: setSelected }}
      />
    ));
    const all = container.querySelector<HTMLInputElement>('[data-testid="select-all"]')!;
    expect(all.indeterminate).toBe(true);
    expect(all.checked).toBe(false);

    await userEvent.click(all);
    flush();
    expect(all.indeterminate).toBe(false);
    expect(all.checked).toBe(true);

    await userEvent.click(all);
    flush();
    expect(selected().size).toBe(0);
    expect(all.indeterminate).toBe(false);
    expect(all.checked).toBe(false);
    dispose();
  });

  /** A row checkbox names the row it selects, so a screen reader in a column of forty of them is
   * not reading "checkbox, checkbox, checkbox". */
  it("names each row's checkbox by the row", () => {
    const { container, dispose } = mount(() => (
      <VirtualizedTable
        {...base}
        selection={{
          selectedKeys: new Set<string>(),
          onChange: () => {},
          rowLabel: (key) => `topic ${key}`,
        }}
      />
    ));
    const label = container.querySelector(".kui-vtable__row .kui-checkbox")!;
    expect(label.textContent).toContain("Select topic topic-0");
    dispose();
  });

  /** Ticking a row must not also open it: the list being selected from would be gone, and the
   * ticks with it. */
  it("does not activate a clickable row when its checkbox is ticked", async () => {
    const onRowClick = vi.fn();
    const [selected, setSelected] = createSignal<ReadonlySet<string>>(new Set());
    const { container, dispose } = mount(() => (
      <VirtualizedTable
        {...base}
        onRowClick={onRowClick}
        selection={{ selectedKeys: selected(), onChange: setSelected }}
      />
    ));
    await userEvent.click(
      container.querySelector<HTMLInputElement>(".kui-vtable__row .kui-checkbox__input")!,
    );
    flush();
    expect(selected().has("topic-0")).toBe(true);
    expect(onRowClick).not.toHaveBeenCalled();
    dispose();
  });

  /** Absent selection costs nothing: no column, no empty cells, no change to the row count. */
  it("adds no column at all when it is given no selection", () => {
    const { container, dispose } = mount(() => <VirtualizedTable {...base} />);
    expect(container.querySelector(".kui-table__cell--select")).toBeNull();
    expect(container.querySelector('[data-testid="select-all"]')).toBeNull();
    dispose();
  });

  it("has no axe violations with a partial selection", async () => {
    const [selected, setSelected] = createSignal<ReadonlySet<string>>(new Set(["topic-2"]));
    const { container, dispose } = mount(() => (
      <VirtualizedTable
        {...base}
        selection={{ selectedKeys: selected(), onChange: setSelected }}
      />
    ));
    const violations = await findViolations(container);
    expect(describeViolations(violations)).toBe("");
    dispose();
  });
});

/* ------------------------------------------------------------------------------------------- */

/**
 * Selection on the plain table, where it meets a clickable row.
 *
 * One case, and it is here rather than in `lists.test.tsx` for the reason the file header gives:
 * the select cell and the row it sits in are one contract, and `VirtualizedTable`'s half of it is
 * asserted a few lines above. The two tables draw the same `<td class="kui-table__cell--select">`
 * and must behave alike; for a while only one of them did, and nothing said so.
 */
describe("DataTable selection", () => {
  interface Topic {
    readonly name: string;
  }
  const rows: readonly Topic[] = [{ name: "orders" }, { name: "payments" }];
  const columns: readonly Column<Topic>[] = [
    { id: "name", header: "Topic", render: (topic) => topic.name },
  ];

  /** Ticking a row must not also open it: the list being selected from would be gone, and the
   * ticks with it. `VirtualizedTable` has the same case, and the two now agree. */
  it("does not activate a clickable row when its checkbox is ticked", async () => {
    const onRowClick = vi.fn();
    const [selected, setSelected] = createSignal<ReadonlySet<string>>(new Set());
    const { container, dispose } = mount(() => (
      <DataTable
        caption="Topics"
        columns={columns}
        rows={rows}
        rowKey={(topic) => topic.name}
        onRowClick={onRowClick}
        selection={{ selectedKeys: selected(), onChange: setSelected }}
      />
    ));
    await userEvent.click(
      container.querySelector<HTMLInputElement>(".kui-table__cell--select .kui-checkbox__input")!,
    );
    flush();
    expect(selected().has("orders")).toBe(true);
    expect(onRowClick).not.toHaveBeenCalled();

    // And the row itself still opens, so the guard stopped one click rather than every click.
    await userEvent.click(container.querySelectorAll<HTMLElement>(".kui-table__row")[1]!);
    flush();
    expect(onRowClick).toHaveBeenCalledTimes(1);
    dispose();
  });
});

/* ------------------------------------------------------------------------------------------- */

/**
 * A modified click on a clickable row, in both tables, because it is one rule and two call sites.
 *
 * Filed by wave 6's third adversarial pass and measured on the shipped build: a ⌘/ctrl/shift-click
 * on a link inside a clickable row opened a background tab **and** navigated the tab the operator
 * was reading, because the row's handler took no event and therefore could not tell the two
 * gestures apart. There is no "open this row elsewhere" for `onRowClick` to do instead — it
 * navigates the current document — so declining is the whole behaviour.
 *
 * Both tables are asserted here rather than one, for the reason the file header already gives about
 * the select cell: the two draw the same row and a guard fixed in one of them and not the other is
 * this project's most repeated defect.
 */
describe("row activation and the modifier keys", () => {
  interface Topic {
    readonly name: string;
  }
  const rows: readonly Topic[] = [{ name: "orders" }, { name: "payments" }];
  const columns: readonly Column<Topic>[] = [
    { id: "name", header: "Topic", render: (topic) => topic.name },
  ];

  /** Every gesture that means "somewhere else", and the one that means "here". */
  const elsewhere: readonly (readonly [string, MouseEventInit])[] = [
    ["⌘-click, a background tab on macOS", { metaKey: true }],
    ["ctrl-click, the same gesture on Linux and Windows", { ctrlKey: true }],
    ["shift-click, a new window", { shiftKey: true }],
    ["alt-click, which downloads rather than opens", { altKey: true }],
    ["a button that is not the primary one", { button: 1 }],
  ];

  it("does not navigate the current tab when DataTable's row is click-modified", () => {
    const onRowClick = vi.fn();
    const { container, dispose } = mount(() => (
      <DataTable
        caption="Topics"
        columns={columns}
        rows={rows}
        rowKey={(topic) => topic.name}
        onRowClick={onRowClick}
      />
    ));
    const row = container.querySelector<HTMLElement>(".kui-table__row")!;

    for (const [gesture, init] of elsewhere) {
      clickOn(row, init);
      flush();
      expect(onRowClick, gesture).not.toHaveBeenCalled();
    }

    // And the plain click still opens the row, so the guard declined five gestures rather than all
    // of them — which is the shape of the fix that breaks the feature instead of the defect.
    clickOn(row);
    flush();
    expect(onRowClick).toHaveBeenCalledTimes(1);
    dispose();
  });

  it("does not navigate the current tab when VirtualizedTable's row is click-modified", () => {
    const onRowClick = vi.fn();
    const { container, dispose } = mount(() => (
      <VirtualizedTable
        caption="Topics"
        columns={columns}
        rows={rows}
        rowKey={(topic: Topic) => topic.name}
        viewportHeight={240}
        compact={false}
        onRowClick={onRowClick}
      />
    ));
    const row = container.querySelector<HTMLElement>(".kui-vtable__row")!;

    for (const [gesture, init] of elsewhere) {
      clickOn(row, init);
      flush();
      expect(onRowClick, gesture).not.toHaveBeenCalled();
    }

    clickOn(row);
    flush();
    expect(onRowClick).toHaveBeenCalledTimes(1);
    dispose();
  });
});
