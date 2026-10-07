/**
 * The two dialogs, as they are actually rendered.
 *
 * The plain-function suites cover what is computed; these cover the things only a mounted component
 * can be wrong about — that a copy of nothing is not drawn as a success, that a zero is drawn as a
 * zero, that the server's own words reach the screen, and that reopening a dialog shows the state
 * the browse is in rather than the state it was in when the page loaded.
 */
import { describe, expect, it, vi } from "vitest";
import { createSignal } from "solid-js";
import type { KafkaRecord } from "@kui/kernel";

import { ResendDialog } from "./ResendDialog.jsx";
import { SmartFilterDialog } from "./SmartFilterDialog.jsx";
import { RESEND_WARNINGS } from "./resend.js";
import { mount } from "./testing.js";

/** Solid batches writes to a microtask, so nothing may be asserted before the flush. */
const flush = (): Promise<void> => new Promise((resolve) => queueMicrotask(resolve));

const RECORD: KafkaRecord = {
  offset: "18442901",
  partition: 3,
  key: "ord_9f21ac",
  timestamp: "2026-09-05T10:00:08Z",
  headers: [],
  value: { kind: "json", text: '{"status":"CAPTURED"}' },
};

/**
 * The dialog that was mounted most recently.
 *
 * `Dialog` renders through a `Portal` into `document.body`, so its content is not inside the
 * container `mount` created — and Solid tears a portal down on its own schedule rather than
 * synchronously inside `dispose()`. A query across the whole body can therefore find a dialog from
 * the previous case and assert against it, which fails in a way that reads like a broken component.
 * Taking the *last* match is unambiguous: the newest node in the body is the one this case mounted.
 */
function panel(): HTMLElement {
  const all = document.body.querySelectorAll<HTMLElement>("[role='dialog']");
  const last = all[all.length - 1];
  if (last === undefined) throw new Error("no dialog was rendered");
  return last;
}

const text = (): string => panel().textContent ?? "";

const buttons = (): readonly HTMLButtonElement[] => [...panel().querySelectorAll("button")];

const find = <T extends Element>(selector: string): T | null => panel().querySelector<T>(selector);

/**
 * Whether the kernel's `Button` considers this control unusable.
 *
 * It sets `aria-disabled` and swallows the click rather than setting the DOM's `disabled`, so that
 * the button stays focusable and the reason in its tooltip is reachable by somebody who cannot
 * hover. Asserting `.disabled` here would therefore be asserting a property this design deliberately
 * does not use, and would pass for a button that is fully live.
 */
const isDisabled = (button: HTMLButtonElement | undefined): boolean =>
  button?.getAttribute("aria-disabled") === "true";

describe("the resend receipt", () => {
  const base = {
    open: true,
    onClose: () => undefined,
    onSend: () => undefined,
    topic: "orders.payments.v2",
    partitionCount: 12,
  };

  it("draws a copy that moved nothing as a warning, never as a success", async () => {
    const { dispose } = mount(() => (
      <ResendDialog
        {...base}
        state={{
          kind: "done",
          value: { toTopic: "orders.replay", read: 0, written: 0, requested: 10 },
        }}
      />
    ));
    await flush();

    const receipt = find(".kui-resend__receipt");
    expect(receipt).not.toBeNull();
    /* The class is the whole visual difference between "it worked" and "read this". A 200 with two
     * zeroes must never land in the success branch. */
    expect(receipt?.className).toContain("kui-resend__receipt--nothing");
    expect(receipt?.className).not.toContain("kui-resend__receipt--complete");
    expect(text()).toContain("Nothing was copied");

    dispose();
  });

  it("draws a zero as the figure 0 and not as a blank or a dash", async () => {
    const { dispose } = mount(() => (
      <ResendDialog
        {...base}
        state={{ kind: "done", value: { toTopic: "orders.replay", read: 0, written: 0 } }}
      />
    ));
    await flush();

    const figures = [...panel().querySelectorAll(".kui-resend__figure")].map(
      (node) => node.textContent ?? "",
    );
    /* The never-zero rule in the direction this screen can get wrong. "We copied none of them" and
     * "we could not tell how many" are different facts, and an em dash here would say the second
     * when the truth is the first. */
    expect(figures).toContain("0");
    expect(figures.some((figure) => figure.includes("—"))).toBe(false);
    expect(figures.some((figure) => figure.trim() === "")).toBe(false);

    dispose();
  });

  it("marks a complete copy complete", async () => {
    const { dispose } = mount(() => (
      <ResendDialog
        {...base}
        state={{
          kind: "done",
          value: { toTopic: "orders.replay", read: 100, written: 100, requested: 100 },
        }}
      />
    ));
    await flush();

    expect(find(".kui-resend__receipt")?.className).toContain("kui-resend__receipt--complete");
    dispose();
  });

  it("warns that records were written when the copy stopped part-way", async () => {
    const { dispose } = mount(() => (
      <ResendDialog
        {...base}
        state={{
          kind: "done",
          value: { toTopic: "orders.replay", read: 100, written: 41, requested: 100 },
        }}
      />
    ));
    await flush();

    // The consequence, not just the numbers: running it again duplicates what already landed.
    expect(text()).toContain("copy those records twice");
    dispose();
  });
});

describe("the resend confirmation", () => {
  const base = {
    open: true,
    onClose: () => undefined,
    topic: "orders.payments.v2",
    partitionCount: 12,
    state: { kind: "idle" } as const,
  };

  it("shows the contract's own warnings, all of them", async () => {
    const { dispose } = mount(() => <ResendDialog {...base} onSend={() => undefined} />);
    await flush();

    /* Each one is something the operator cannot infer from a form with two offsets in it, and each
     * has a consequence they would otherwise meet afterwards. Summarising them into one sentence is
     * what makes a confirmation skippable. */
    for (const warning of RESEND_WARNINGS) {
      expect(text()).toContain(warning.message);
    }
    dispose();
  });

  it("counts one partition as one, in both places it says so", async () => {
    /*
     * A single-partition topic is the ordinary shape of a compacted config topic and of most of
     * what a scratch cluster holds, and this dialog said "has 1 partitions" about all of them — in
     * the sentence under the ranges and again in the reason the `Add a partition` button gives for
     * being unavailable. On the one screen whose job is to be believed about numbers.
     */
    const { dispose } = mount(() => (
      <ResendDialog
        {...base}
        partitionCount={1}
        initial={{ toTopic: "orders.replay", ranges: [{ partition: 0, from: "0", until: "3" }] }}
        onSend={() => undefined}
      />
    ));
    await flush();

    expect(text()).toContain("has 1 partition.");
    expect(text()).not.toContain("has 1 partitions");
    /* The second place, which is the reason `Add a partition` gives for being unavailable. It lives
       in a `Tooltip` that renders nothing until the control is pointed at or focused, so the case
       focuses it — which is also the path a keyboard user takes to that sentence. */
    const add = buttons().find((button) => (button.textContent ?? "").includes("Add a partition"));
    expect(add?.getAttribute("aria-disabled")).toBe("true");
    add?.dispatchEvent(new FocusEvent("focusin", { bubbles: true }));
    await flush();
    // The bubble portals out of the dialog, so this reads the document rather than the panel.
    expect(document.body.textContent).toContain("has 1 partition,");
    expect(document.body.textContent).not.toContain("has 1 partitions,");

    dispose();
  });

  it("marks the sentence that states a count as a statement, not as a gap", async () => {
    /*
     * The known and the unknown sentences shared `kui-resend__unknown` — the class whose entire job
     * is to mark an absence — so the one line here that carries a measured figure was drawn as the
     * absence of one. Opposite statements, and they must not look alike.
     */
    const known = mount(() => (
      <ResendDialog {...base} partitionCount={12} onSend={() => undefined} />
    ));
    await flush();
    expect(find(".kui-resend__known")?.textContent).toContain("has 12 partitions.");
    expect(find(".kui-resend__unknown")).toBeNull();
    known.dispose();

    const unknown = mount(() => (
      <ResendDialog {...base} partitionCount={undefined} onSend={() => undefined} />
    ));
    await flush();
    expect(find(".kui-resend__unknown")?.textContent).toContain("KUI has not been told");
    expect(find(".kui-resend__known")).toBeNull();
    unknown.dispose();
  });

  it("explains the half-open range rather than leaving it to the field names", async () => {
    const { dispose } = mount(() => <ResendDialog {...base} onSend={() => undefined} />);
    await flush();
    // "0 to 3" reads as four records to most people and copies three.
    expect(text()).toContain("copies three records");
    dispose();
  });

  it("will not send until the destination has been typed back", async () => {
    const onSend = vi.fn();
    const { dispose } = mount(() => (
      <ResendDialog
        {...base}
        initial={{ toTopic: "orders.replay", ranges: [{ partition: 0, from: "0", until: "3" }] }}
        onSend={onSend}
      />
    ));
    await flush();

    const confirm = buttons().find((button) => (button.textContent ?? "").includes("Copy records"));
    expect(confirm).toBeDefined();
    expect(isDisabled(confirm)).toBe(true);

    /* Clicked anyway, because `aria-disabled` does not stop the browser dispatching. The button
     * swallows it, and that is what makes the statement true — a confirmation that fired on a
     * control the operator was told was unusable would be the worst kind of surprise here. */
    confirm?.click();
    await flush();
    expect(onSend).not.toHaveBeenCalled();

    dispose();
  });
});

describe("the smart filter editor", () => {
  it("invalidates an in-flight verdict when the expression or sample changes", async () => {
    const [state, setState] = createSignal<import("@kui/kernel").Mutation<import("./filters.js").FilterVerdict>>({ kind: "idle" });
    const ui = mount(() => <SmartFilterDialog open onClose={() => undefined} topic="orders"
      source="true" samples={[RECORD]} testState={state()} applyState={{ kind: "idle" }}
      onApply={() => undefined} onTest={() => setState({ kind: "running" })} />);
    await flush();
    buttons().find((b) => b.textContent?.includes("Try it on one record"))?.click(); await flush();
    const editor = find<HTMLTextAreaElement>("textarea")!;
    editor.value = "false"; editor.dispatchEvent(new Event("input", { bubbles: true })); await flush();
    setState({ kind: "done", value: { kind: "matched" } }); await flush();
    expect(find(".kui-verdict")).toBeNull();
    editor.value = "true"; editor.dispatchEvent(new Event("input", { bubbles: true })); await flush();
    expect(find(".kui-verdict")).toBeNull();
    ui.dispose();
  });
  const base = {
    open: true,
    onClose: () => undefined,
    onTest: () => undefined,
    onApply: () => undefined,
    topic: "orders.payments.v2",
    samples: [RECORD],
    testState: { kind: "idle" } as const,
    applyState: { kind: "idle" } as const,
  };

  it("builds a safe field filter before exposing advanced CEL", async () => {
    const onApply = vi.fn();
    const { dispose } = mount(() => <SmartFilterDialog {...base} onApply={onApply} />);
    await flush();

    expect(text()).toContain("Field path");
    expect(find(".kui-smart-filter__editor")).toBeNull();

    const value = find<HTMLInputElement>('input[name="json-path-filter-value"]');
    expect(value).not.toBeNull();
    if (value !== null) {
      value.value = "CAPTURED";
      value.dispatchEvent(new Event("input", { bubbles: true }));
    }
    await flush();

    buttons().find((button) => (button.textContent ?? "").includes("Use this filter"))?.click();
    expect(onApply).toHaveBeenCalledWith('record.value.status == "CAPTURED"');

    dispose();
  });

  it("draws an expression that threw differently from one that did not match", async () => {
    const failed = mount(() => (
      <SmartFilterDialog
        {...base}
        testState={{
          kind: "done",
          value: { kind: "failed", reason: "key 'customer' is not present in map." },
        }}
      />
    ));
    await flush();
    expect(find(".kui-verdict")?.className).toContain("kui-verdict--failed");
    // The server's own words, because they name the field the operator has to fix.
    expect(text()).toContain("key 'customer' is not present in map.");
    failed.dispose();

    const missed = mount(() => (
      <SmartFilterDialog {...base} testState={{ kind: "done", value: { kind: "no-match" } }} />
    ));
    await flush();
    /* The distinction the whole endpoint is shaped around. Both arrive as `matched: false`; one is
     * a broken filter and one is a working one, and they must not share a panel. */
    expect(find(".kui-verdict")?.className).toContain("kui-verdict--no-match");
    expect(find(".kui-verdict")?.className).not.toContain("kui-verdict--failed");
    missed.dispose();
  });

  it("disables the preview with a reason rather than hiding it when nothing has been read", async () => {
    const { dispose } = mount(() => (
      <SmartFilterDialog {...base} samples={[]} source="record.partition == 0" />
    ));
    await flush();

    const tryIt = buttons().find((button) =>
      (button.textContent ?? "").includes("Try it on one record"),
    );
    /* Present, disabled, and explained. A control that vanishes teaches the operator the product
     * cannot preview a filter; this one tells them to read some records first. */
    expect(tryIt).toBeDefined();
    expect(isDisabled(tryIt)).toBe(true);
    /* And the reason is on the screen, not only in a tooltip: the operator has to be told what to
     * do first, and a hover-only explanation is no explanation on a touch device. */
    expect(text()).toContain("No records are on screen yet");
    expect(text()).toContain("Read some first");

    dispose();
  });

  it("keeps the expression on screen when the server refuses to compile it", async () => {
    const { dispose } = mount(() => (
      <SmartFilterDialog
        {...base}
        source="record.value.status =="
        applyState={{
          kind: "failed",
          code: "KUI-FILTER-COMPILE",
          message: "line 1, column 22: mismatched input '<EOF>'",
        }}
      />
    ));
    await flush();

    const editor = find<HTMLTextAreaElement>(".kui-smart-filter__editor");
    // Losing the text is losing the work at the one moment it is needed — and the line and column
    // in the message mean nothing without the text they point into.
    expect(editor?.value).toBe("record.value.status ==");
    expect(text()).toContain("line 1, column 22");

    dispose();
  });

  it("shows the filter that is running when it is reopened, not the one from page load", async () => {
    /* The dialog is mounted with the screen and opened much later. Seeding the box at mount would
     * leave it empty for an operator who applied a filter and came back to edit it. */
    const [open, setOpen] = createSignal(false);
    const [source, setSource] = createSignal<string | undefined>(undefined);

    const { dispose } = mount(() => (
      <SmartFilterDialog {...base} open={open()} source={source()} />
    ));
    await flush();

    // A filter is applied while the dialog is shut, then the operator reopens it.
    setSource("record.value.status == \"CAPTURED\"");
    setOpen(true);
    await flush();

    const editor = find<HTMLTextAreaElement>(".kui-smart-filter__editor");
    expect(editor?.value).toBe('record.value.status == "CAPTURED"');

    dispose();
  });
});
