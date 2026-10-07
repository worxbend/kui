import { expect, it, vi } from "vitest";
import { createSignal, flush } from "solid-js";
import { KuiProvider, createQueryRegistry } from "@kui/kernel";
import { Actions, type KuiApiClient } from "@kui/api";
import { mount, testContext } from "./testing.js";
import { ConnectScreen } from "./ConnectRoute.jsx";
import document from "./documents/connectors-response.json";

it("77: shared cluster policy reactively blocks connector writes and keeps the Connect subject", async () => {
  const [reason, setReason] = createSignal<string | undefined>("Cluster is read-only");
  const writeBlocked = vi.fn(() => reason());
  const post = vi.fn(async () => ({ ok: true, value: {} }));
  const api = { get: async () => ({ ok: true, value: document }), post, put: post } as unknown as KuiApiClient;
  const context = { ...testContext(api), writeBlocked };
  const queries = createQueryRegistry();
  const view = mount(() => <KuiProvider value={context}><ConnectScreen clusterId="quickstart" queries={queries} /></KuiProvider>);
  try {
    for (let i = 0; i < 10; i++) await flush();
    const commands = () => [...view.container.querySelectorAll("button")].filter(b => /^(Pause|Resume|Restart)$/.test(b.textContent?.trim() ?? ""));
    expect(commands().length).toBeGreaterThan(0);
    expect(commands().every(b => b.getAttribute("aria-disabled") === "true")).toBe(true);
    expect(writeBlocked).toHaveBeenCalledWith("quickstart", Actions.ConnectOperate, expect.any(String));
    commands().forEach(b => b.click()); await flush(); expect(post).not.toHaveBeenCalled();
    setReason(undefined); await flush();
    expect(commands().some(b => b.getAttribute("aria-disabled") !== "true")).toBe(true);
  } finally { view.dispose(); }
});
