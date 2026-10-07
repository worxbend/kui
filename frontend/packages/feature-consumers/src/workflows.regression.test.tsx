import { expect, it, vi } from "vitest";
import { createSignal, flush } from "solid-js";
import { mount, testContext } from "./testing.js";
import { KuiProvider } from "@kui/kernel";
import { Actions, type KuiApiClient } from "@kui/api";
import { createRouter, memoryHistory } from "@solidjs/router";
import { GroupRoute } from "./GroupRoute.jsx";
import { DEFAULT_POLL_MS } from "./lag.js";
import groupDocument from "./recorded/group.json";
import { ResetWizard } from "./ResetWizard.jsx";
import { SAMPLE_PLAN } from "./fixtures.js";

it("77: group write controls use the reactive cluster policy and named group", async () => {
  const [reason, setReason] = createSignal<string | undefined>();
  const writeBlocked = vi.fn(() => reason());
  const api = { get: async () => ({ ok: true, value: { ...groupDocument, members: [] } }) } as unknown as KuiApiClient;
  const Router = createRouter({ routes: [{ path: "/clusters/:clusterId/consumer-groups/:groupId", component: GroupRoute }],
    history: memoryHistory("/clusters/quickstart/consumer-groups/analytics-indexer"), scrollRestoration: false });
  const view = mount(() => <KuiProvider value={{ ...testContext(api), writeBlocked }}><Router /></KuiProvider>);
  try {
    for (let i = 0; i < 8; i++) await flush();
    setReason("Cluster is read-only"); await flush();
    const buttons = [...view.container.querySelectorAll("button")].filter(b => /^(Reset offsets|Delete group|Forget offsets)$/.test(b.textContent?.trim() ?? ""));
    expect(buttons.length).toBe(3);
    expect(buttons.every(b => b.getAttribute("aria-disabled") === "true")).toBe(true);
    expect(writeBlocked).toHaveBeenCalledWith("quickstart", Actions.ConsumerGroupResetOffsets, "analytics-indexer");
    expect(writeBlocked).toHaveBeenCalledWith("quickstart", Actions.ConsumerGroupDelete, "analytics-indexer");
  } finally { view.dispose(); }
});

it("77: a wizard opened before policy revocation cannot preview or apply", async () => {
  const [permitted, setPermitted] = createSignal(true);
  const plan = vi.fn(async () => ({ ok: true as const, plan: SAMPLE_PLAN }));
  const apply = vi.fn(async () => ({ ok: true as const, receipt: SAMPLE_PLAN }));
  const view = mount(() => <ResetWizard topics={[{ topic: "orders", partitions: [0] }]}
    permitted={permitted()} refusal="Cluster is read-only" plan={plan} apply={apply} />);
  const press = async (label: string) => { [...view.container.querySelectorAll("button")].find(b => b.textContent?.trim() === label)!.click(); await flush(); };
  try {
    await flush(); await press("Reset offsets");
    setPermitted(false); await flush(); await press("Preview the plan");
    expect(plan).not.toHaveBeenCalled();
    setPermitted(true); await flush(); await press("Preview the plan"); await flush();
    setPermitted(false); await flush(); await press("Apply this plan");
    expect(apply).not.toHaveBeenCalled();
  } finally { view.dispose(); }
});

it.each(["KUI-UPSTREAM", "KUI-FORBIDDEN"])("69: a background %s is not shown as fresh data", async (code) => {
  vi.useFakeTimers();
  let calls = 0;
  const api = { get: async () => ++calls === 1 ? { ok: true, value: groupDocument }
    : { ok: false, error: { kind: "envelope", code, message: "Coordinator unavailable", details: [], correlationId: "test", retryable: true } } } as unknown as KuiApiClient;
  const Router = createRouter({ routes: [{ path: "/clusters/:clusterId/consumer-groups/:groupId", component: GroupRoute }],
    history: memoryHistory("/clusters/quickstart/consumer-groups/analytics-indexer"), scrollRestoration: false });
  const view = mount(() => <KuiProvider value={testContext(api)}><Router /></KuiProvider>);
  try {
    for (let i = 0; i < 8; i++) await flush();
    expect(view.container.textContent).toContain("analytics-indexer");
    await vi.advanceTimersByTimeAsync(DEFAULT_POLL_MS);
    for (let i = 0; i < 8; i++) await flush();
    if (code === "KUI-FORBIDDEN") {
      expect(view.container.textContent).toContain("do not have permission to see");
      expect(view.container.textContent).not.toContain("analytics-indexer");
    } else {
      expect(view.container.textContent).toContain("Coordinator unavailable");
      expect(view.container.textContent).toContain("analytics-indexer");
    }
  } finally { view.dispose(); vi.useRealTimers(); }
});

it("66: closing and reopening abandons the old preview", async () => {
  let finish!: (answer: { ok: true; plan: typeof SAMPLE_PLAN }) => void;
  const view = mount(() => <ResetWizard topics={[{ topic: "orders", partitions: [0] }]}
    plan={() => new Promise(resolve => { finish = resolve; })}
    apply={async () => ({ ok: true, receipt: SAMPLE_PLAN })} />);
  const press = async (label: string) => {
    const button = [...view.container.querySelectorAll("button")].find(b => b.textContent?.trim() === label);
    expect(button).toBeDefined(); button!.click(); await flush();
  };
  try {
    await flush();
    await press("Reset offsets"); await press("Preview the plan");
    await press("Close"); await press("Reset offsets");
    finish({ ok: true, plan: SAMPLE_PLAN }); await flush(); await flush();
    expect(view.container.querySelector('[data-testid="group-reset-plan"]')).toBeNull();
    expect(view.container.querySelector('[data-testid="group-reset-form"]')).not.toBeNull();
  } finally { view.dispose(); }
});
