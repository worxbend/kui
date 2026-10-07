import { expect, it, vi } from "vitest";
import { createSignal, flush } from "solid-js";
import { KuiProvider, sharedQueries, type Mutation } from "@kui/kernel";
import type { KuiApiClient } from "@kui/api";
import { createRouter, memoryHistory } from "@solidjs/router";
import { mount, testContext } from "./testing.js";
import Clusters, { ManageScreen } from "./ClustersRoute.jsx";
import brokers from "./recorded/brokers.json";
import { ClusterAdmin, type ManagedCluster } from "./ClusterAdmin.jsx";
import { EMPTY_CLUSTER_FORM } from "./clusterForm.js";

const cluster: ManagedCluster = { id: "spare", name: "spare", bootstrapServers: "spare:9092", readOnly: false,
  origin: "stored", version: 1, security: { protocol: "PLAINTEXT", mechanism: null } };
const noop = () => {};

it("77: application configuration permission is rechecked in an already open form", async () => {
  const [reason, setReason] = createSignal<string | undefined>();
  const save = vi.fn(); const test = vi.fn();
  const form = { ...EMPTY_CLUSTER_FORM, id: "spare", name: "spare", bootstrapServers: "spare:9092" };
  const view = mount(() => <ClusterAdmin clusters={[cluster]} editing={{ id: "spare", form }}
    onAdd={noop} onEdit={noop} onCancel={noop} onFormChange={noop} onSave={save} onTest={test} onDelete={noop}
    disabledReason={reason()} saveState={{ kind: "idle" }} testState={{ kind: "idle" }} deleteState={{ kind: "idle" }} />);
  try {
    await flush(); setReason("Configuration permission revoked"); await flush();
    await press(view.container, "Save"); await press(view.container, "Test the connection");
    view.container.querySelector("form")!.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true })); await flush();
    expect(save).not.toHaveBeenCalled(); expect(test).not.toHaveBeenCalled();
  } finally { view.dispose(); }
});

it("68: a late failed connection test is not attached to edited settings", async () => {
  sharedQueries.invalidateWhere(() => true);
  let finish!: (answer: unknown) => void;
  const api = { get: async () => ({ ok: true, value: { clusters: { status: "ok", data: [{ cluster }] } } }),
    post: () => new Promise(resolve => { finish = resolve; }) } as unknown as KuiApiClient;
  const view = mount(() => <KuiProvider value={testContext(api)}><ManageScreen /></KuiProvider>);
  try {
    for (let i = 0; i < 10; i++) await flush();
    await press(view.container, "Edit"); await press(view.container, "Test the connection");
    const input = view.container.querySelector("input")!;
    input.value = "Edited name"; input.dispatchEvent(new Event("input", { bubbles: true })); await flush();
    finish({ ok: false, error: { kind: "envelope", code: "KUI-UPSTREAM", message: "Old test failed", details: [], correlationId: "test", retryable: false } });
    for (let i = 0; i < 10; i++) await flush();
    expect(view.container.textContent).not.toContain("Old test failed");
  } finally { view.dispose(); sharedQueries.invalidateWhere(() => true); }
});

it("70: broker tab navigation applies the deployment prefix once", async () => {
  sharedQueries.invalidateWhere(() => true);
  const api = { get: async (path: string) => ({ ok: true, value: path.endsWith("/brokers") ? brokers : {} }) } as unknown as KuiApiClient;
  const history = memoryHistory("/ui/clusters/quickstart/brokers/1");
  const Router = createRouter({ routes: [{ path: "/clusters/:clusterId/brokers/:brokerId", component: Clusters }],
    base: "/ui", history, scrollRestoration: false });
  const view = mount(() => <KuiProvider value={testContext(api)}><Router /></KuiProvider>);
  try {
    for (let i = 0; i < 10; i++) await flush();
    await press(view.container, "Configuration");
    for (let i = 0; i < 10; i++) await flush();
    expect(history.get()).toBe("/ui/clusters/quickstart/brokers/1?tab=configuration");
  } finally { view.dispose(); sharedQueries.invalidateWhere(() => true); }
});

it.each(["failed", "forbidden"] as const)("68: connection test %s is visible beside the form", async (kind) => {
  const view = mount(() => <ClusterAdmin clusters={[cluster]}
    editing={{ id: cluster.id, form: EMPTY_CLUSTER_FORM }} onAdd={noop} onEdit={noop}
    onCancel={noop} onFormChange={noop} onSave={noop} onTest={noop} onDelete={noop}
    saveState={{ kind: "idle" }} testState={kind === "failed"
      ? { kind, message: "Connection refused", code: "KUI-UPSTREAM" }
      : { kind, message: "Test not permitted" }} deleteState={{ kind: "idle" }} />);
  try { await flush(); expect(view.container.textContent).toContain(kind === "failed" ? "Connection refused" : "Test not permitted"); }
  finally { view.dispose(); }
});
async function press(root: ParentNode, label: string) {
  const button = [...root.querySelectorAll("button")].find(b => b.textContent?.trim() === label);
  expect(button).toBeDefined(); button!.click(); await flush();
}

it("67: failed removal keeps its confirmation and retry visible until success", async () => {
  const [state, setState] = createSignal<Mutation<unknown>>({ kind: "idle" });
  let complete!: (done: boolean) => void;
  const view = mount(() => <ClusterAdmin clusters={[cluster]} editing={undefined} onAdd={noop} onEdit={noop}
    onCancel={noop} onFormChange={noop} onSave={noop} onTest={noop}
    onDelete={() => { setState({ kind: "running" }); return new Promise<boolean>(resolve => { complete = resolve; }); }}
    saveState={{ kind: "idle" }} testState={{ kind: "idle" }} deleteState={state()} />);
  try {
    await flush(); await press(view.container, "Remove");
    const dialog = document.querySelector('[role="dialog"]')!;
    const input = dialog.querySelector("input")!; input.value = "spare";
    input.dispatchEvent(new Event("input", { bubbles: true })); await flush();
    await press(dialog, "Remove cluster");
    setState({ kind: "failed", message: "Registry unavailable", code: "KUI-UPSTREAM" }); complete(false); await flush(); await flush();
    expect(document.querySelector('[role="dialog"]')?.textContent).toContain("Registry unavailable");
    await press(document.querySelector('[role="dialog"]')!, "Remove cluster");
    setState({ kind: "done", value: {} }); complete(true); await flush(); await flush();
    expect(document.querySelector('[role="dialog"]')).toBeNull();
  } finally { view.dispose(); }
});
