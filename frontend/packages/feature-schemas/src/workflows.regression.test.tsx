import { expect, it } from "vitest";
import { createSignal, flush } from "solid-js";
import { CompatibilityCheck } from "./CompatibilityCheck.jsx";
import { mount } from "./testing.js";
import type { Mutation } from "@kui/kernel";
import type { CompatibilityVerdict } from "./data.js";

it("72: changing the subject or comparison schema permanently discards its verdict", async () => {
  const [subject, setSubject] = createSignal("first");
  const [initial, setInitial] = createSignal('"string"');
  const view = mount(() => <CompatibilityCheck subject={subject()} level="BACKWARD"
    initialDefinition={initial()} state={{ kind: "done", value: { compatible: true, messages: [] } }} onCheck={() => {}} />);
  try {
    await flush(); expect(view.container.querySelector('[data-testid="compatibility-verdict"]')).not.toBeNull();
    setSubject("second"); await flush(); setSubject("first"); await flush();
    expect(view.container.querySelector('[data-testid="compatibility-verdict"]')).toBeNull();
    setInitial('"int"'); await flush();
    expect(view.container.querySelector('[data-testid="compatibility-verdict"]')).toBeNull();
  } finally { view.dispose(); }
});

it("71: initializes from a late schema while pristine and resets for a different subject", async () => {
  const [initial, setInitial] = createSignal<{ type: string; definition: string }>();
  const [subject, setSubject] = createSignal("first");
  const view = mount(() => <CompatibilityCheck subject={subject()} level="BACKWARD" state={{ kind: "idle" }}
    initialSchemaType={initial()?.type} initialDefinition={initial()?.definition} onCheck={() => {}} />);
  try {
    await flush(); setInitial({ type: "PROTOBUF", definition: "message First {}" }); await flush();
    const editor = view.container.querySelector("textarea")!;
    expect(editor.value).toBe("message First {}");
    expect(view.container.querySelector('[role="combobox"]')?.textContent).toContain("PROTOBUF");
    editor.value = "message Mine {}"; editor.dispatchEvent(new Event("input", { bubbles: true })); await flush();
    setInitial({ type: "PROTOBUF", definition: "message Refreshed {}" }); await flush();
    expect(editor.value).toBe("message Mine {}");
    setSubject("second"); setInitial({ type: "JSON", definition: "{}" }); await flush();
    expect(editor.value).toBe("{}"); expect(view.container.querySelector('[role="combobox"]')?.textContent).toContain("JSON");
  } finally { view.dispose(); }
});

it("72: an edited proposal never displays an earlier or late verdict", async () => {
  const [state, setState] = createSignal<Mutation<CompatibilityVerdict>>({ kind: "idle" });
  const view = mount(() => <CompatibilityCheck subject="orders" level="BACKWARD" state={state()}
    initialDefinition='"string"' onCheck={() => setState({ kind: "running" })} />);
  try {
    await flush();
    const check = () => [...view.container.querySelectorAll("button")].find(b => b.textContent?.trim() === "Check compatibility")!.click();
    check(); await flush();
    setState({ kind: "done", value: { compatible: true, messages: [] } }); await flush();
    expect(view.container.querySelector('[data-testid="compatibility-verdict"]')).not.toBeNull();
    const editor = view.container.querySelector("textarea")!;
    editor.value = '"int"'; editor.dispatchEvent(new Event("input", { bubbles: true })); await flush();
    expect(view.container.querySelector('[data-testid="compatibility-verdict"]')).toBeNull();
    check(); await flush();
    editor.value = '"long"'; editor.dispatchEvent(new Event("input", { bubbles: true })); await flush();
    setState({ kind: "done", value: { compatible: true, messages: [] } }); await flush();
    expect(view.container.querySelector('[data-testid="compatibility-verdict"]')).toBeNull();
  } finally { view.dispose(); }
});
