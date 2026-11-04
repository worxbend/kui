import { describe, expect, it, vi } from "vitest";

import { ProduceDrawer } from "./ProduceDrawer.jsx";
import { mount } from "./testing.js";

const flush = (): Promise<void> => new Promise((resolve) => queueMicrotask(resolve));

function drawer(): HTMLElement {
  const all = document.body.querySelectorAll<HTMLElement>("[role='dialog']");
  const latest = all[all.length - 1];
  if (latest === undefined) throw new Error("no produce drawer was rendered");
  return latest;
}

function setValue(element: HTMLTextAreaElement, value: string): void {
  const setter = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, "value")?.set;
  setter?.call(element, value);
  element.dispatchEvent(new InputEvent("input", { bubbles: true, inputType: "insertText" }));
}

function button(name: RegExp): HTMLButtonElement | undefined {
  return [...drawer().querySelectorAll("button")].find((candidate) =>
    name.test(candidate.textContent ?? ""),
  );
}

describe("the produce value editor", () => {
  const base = {
    open: true,
    onClose: () => undefined,
    topic: "orders.v1",
    state: { kind: "idle" } as const,
  };

  it("formats and highlights valid JSON in a multiline textarea", async () => {
    const mounted = mount(() => <ProduceDrawer {...base} onSend={() => undefined} />);
    await flush();

    const editor = drawer().querySelector<HTMLTextAreaElement>("textarea[aria-label='Value']");
    expect(editor).not.toBeNull();
    setValue(editor!, '{"order":{"id":42,"paid":true}}');
    await flush();

    expect(drawer().textContent).toContain("Valid JSON");
    expect(drawer().querySelector(".kui-json-editor__token--key")?.textContent).toBe('"order"');
    expect(drawer().querySelector(".kui-json-editor__token--number")?.textContent).toBe("42");

    button(/^Format JSON$/)?.click();
    await flush();
    expect(editor?.value).toBe('{\n  "order": {\n    "id": 42,\n    "paid": true\n  }\n}');

    mounted.dispose();
  });

  it("checks JSON grammar before enabling produce", async () => {
    const mounted = mount(() => <ProduceDrawer {...base} onSend={() => undefined} />);
    await flush();

    const editor = drawer().querySelector<HTMLTextAreaElement>("textarea[aria-label='Value']")!;
    setValue(editor, '{\n  "order": 42,\n  "paid": tru\n}');
    await flush();

    expect(editor.getAttribute("aria-invalid")).toBe("true");
    expect(drawer().textContent).toMatch(/line 3, column \d+/i);
    expect(button(/^Produce record$/)?.getAttribute("aria-disabled")).toBe("true");

    mounted.dispose();
  });

  it("lets text that only starts like JSON be sent as plain text", async () => {
    /* String topics carry log lines such as `[INFO] started`. The grammar check stays the default,
     * but the operator must be able to say the value is not JSON and send it as typed. */
    const onSend = vi.fn();
    const mounted = mount(() => <ProduceDrawer {...base} onSend={onSend} />);
    await flush();

    const editor = drawer().querySelector<HTMLTextAreaElement>("textarea[aria-label='Value']")!;
    setValue(editor, "[INFO] started");
    await flush();
    expect(button(/^Produce record$/)?.getAttribute("aria-disabled")).toBe("true");

    const asText = [...drawer().querySelectorAll<HTMLLabelElement>("label.kui-checkbox")].find(
      (label) => label.textContent?.includes("Send as plain text"),
    );
    expect(asText).toBeDefined();
    asText?.querySelector<HTMLInputElement>("input[type='checkbox']")?.click();
    await flush();

    expect(button(/^Produce record$/)?.getAttribute("aria-disabled")).not.toBe("true");
    button(/^Produce record$/)?.click();
    expect(onSend).toHaveBeenCalledWith(expect.objectContaining({ value: "[INFO] started" }));

    mounted.dispose();
  });

  it("minifies valid JSON only when the checkbox is selected", async () => {
    const onSend = vi.fn();
    const mounted = mount(() => <ProduceDrawer {...base} onSend={onSend} />);
    await flush();

    const editor = drawer().querySelector<HTMLTextAreaElement>("textarea[aria-label='Value']")!;
    setValue(editor, '{\n  "id": 9007199254740993,\n  "paid": true\n}');
    await flush();

    const minify = drawer().querySelector<HTMLInputElement>(
      "input[type='checkbox']:not([role='switch'])",
    );
    expect(minify).not.toBeNull();
    minify?.click();
    await flush();
    button(/^Produce record$/)?.click();

    expect(onSend).toHaveBeenCalledWith(
      expect.objectContaining({ value: '{"id":9007199254740993,"paid":true}' }),
    );

    mounted.dispose();
  });

  it("bounds the syntax layer for very large values while preserving validation", async () => {
    const mounted = mount(() => <ProduceDrawer {...base} onSend={() => undefined} />);
    await flush();

    const editor = drawer().querySelector<HTMLTextAreaElement>("textarea[aria-label='Value']")!;
    setValue(editor, JSON.stringify({ payload: "x".repeat(200_000) }));
    await flush();

    expect(drawer().textContent).toContain("Valid JSON · highlighting paused for size");
    expect(drawer().querySelectorAll(".kui-json-editor__token")).toHaveLength(1);
    expect(editor.value).toHaveLength(200_014);

    mounted.dispose();
  });
});
