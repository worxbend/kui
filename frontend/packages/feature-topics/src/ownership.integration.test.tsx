import { afterEach, expect, it, vi } from "vitest";
import { flush } from "solid-js";
import { mount } from "./testing.js";
import { forgetQueries, topicsHost, settle, type StubRequest } from "./harness.jsx";

const planPath = "/api/v1/clusters/{clusterId}/topics/{topicName}/deletion/plan";
const deletePath = "/api/v1/clusters/{clusterId}/topics/{topicName}";
function fixture() {
  return topicsHost({ at: "/clusters/old-plan-cluster/topics/orders", answers: {
    "/api/v1/clusters/{clusterId}/topics/{topicName}/overview": { topic: { status: "ok", data: {
      row: { name: "orders", internal: false, partitionCount: 1, replicationFactor: 1, outOfSyncReplicas: 0, offlinePartitions: 0 }, partitions: [],
    } } },
    [planPath]: (request: StubRequest) => ({ topic: "orders", partitions: 1, records: 23,
      autoCreateEnabled: false, warnings: [], token: `${request.params.path?.clusterId}-token`, expiresAt: "2026-09-06T00:05:00Z" }),
    [deletePath]: { topic: "orders", autoCreateEnabled: false },
  } });
}
function clickDelete(root: ParentNode) {
  [...root.querySelectorAll("button")].find((button) => button.textContent?.trim() === "Delete topic")!.click();
}
async function confirm() {
  const dialog = document.querySelector('[data-testid="planned-action-confirm"]')!;
  const input = dialog.querySelector<HTMLInputElement>('input[type="text"]')!;
  input.value = "orders";
  input.dispatchEvent(new Event("input", { bubbles: true }));
  await flush();
  clickDelete(dialog);
  await settle();
}
afterEach(() => { forgetQueries(); vi.restoreAllMocks(); });

it.each([false, true])("abandons a same-topic confirmation across clusters (plan pending: %s)", async (pending) => {
  const host = fixture();
  let release!: () => void;
  const held = new Promise<void>((resolve) => { release = resolve; });
  const post = host.stub.api.post as (path: string, init?: { params?: StubRequest["params"] }) => Promise<unknown>;
  vi.spyOn(host.stub.api, "post").mockImplementation((async (path: string, init?: { params?: StubRequest["params"] }) => {
    const answer = await post(path, init);
    if (pending && path === planPath && init?.params?.path?.clusterId === "old-plan-cluster") await held;
    return answer;
  }) as typeof host.stub.api.post);
  const ui = mount(host.view);
  try {
    await settle();
    clickDelete(ui.container);
    await settle();
    expect(document.querySelector(pending ? '[data-testid="planned-action-planning"]' : '[data-testid="planned-action-confirm"]')).not.toBeNull();
    host.goTo("/clusters/new-plan-cluster/topics/orders");
    await settle();
    release();
    await settle();
    expect(document.querySelector('[data-testid="planned-action-confirm"]')).toBeNull();
    expect(document.querySelector('[data-testid="planned-action-planning"]')).toBeNull();
    clickDelete(ui.container);
    await settle();
    await confirm();
    const plans = host.stub.requests.filter((r) => r.path === planPath);
    expect(plans.map((r) => r.params.path?.clusterId)).toEqual(["old-plan-cluster", "new-plan-cluster"]);
    const applied = host.stub.requests.filter((r) => r.path === deletePath);
    expect(applied).toHaveLength(1);
    expect(applied[0]?.params).toMatchObject({ path: { clusterId: "new-plan-cluster", topicName: "orders" }, query: { token: "new-plan-cluster-token" } });
  } finally { release(); ui.dispose(); }
});

it("does not navigate the new cluster when an old deletion finishes", async () => {
  const host = fixture();
  let release!: () => void;
  const held = new Promise<void>((resolve) => { release = resolve; });
  const remove = host.stub.api.delete as (path: string, init?: unknown) => Promise<unknown>;
  vi.spyOn(host.stub.api, "delete").mockImplementation((async (path: string, init?: unknown) => {
    const answer = await remove(path, init);
    await held;
    return answer;
  }) as typeof host.stub.api.delete);
  const ui = mount(host.view);
  try {
    await settle(); clickDelete(ui.container); await settle(); await confirm();
    host.goTo("/clusters/new-plan-cluster/topics/orders"); await settle();
    release(); await settle();
    expect(ui.container.querySelector('[data-testid="no-route"]')).toBeNull();
    clickDelete(ui.container); await settle();
    expect(host.stub.requests.filter((r) => r.path === planPath).map((r) => r.params.path?.clusterId)).toEqual(["old-plan-cluster", "new-plan-cluster"]);
  } finally { release(); ui.dispose(); }
});
