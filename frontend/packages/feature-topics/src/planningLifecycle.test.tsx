import { expect, test } from "vitest";
import { createSignal, flush } from "solid-js";
import { mount } from "./testing.js";
import { PlannedActionDialog, type TokenPlan } from "./PlannedActionDialog.jsx";
import { topicsHost, settle } from "./harness.jsx";

test("topic creation adopts the shared cluster write policy", async () => {
  const host = topicsHost({ at: "/clusters/policy-cluster/topics", answers: {
    "/api/v1/clusters/{clusterId}/topics": { topics: { status: "ok", data: { items: [], page: { page: 1, pageSize: 32, totalItems: 0 } } } },
  }, writeBlocked: () => "Read-only policy" });
  const ui = mount(host.view); await settle();
  const create = [...ui.container.querySelectorAll("button")].find((b) => b.textContent?.includes("Create topic"));
  expect(create).toBeDefined();
  expect(create?.getAttribute("aria-disabled")).toBe("true");
  ui.dispose();
});

test("an abandoned plan cannot overwrite a reopened confirmation", async () => {
  const replies: Array<(p: TokenPlan) => void> = [];
  const [open, setOpen] = createSignal(true);
  const ui = mount(() => <PlannedActionDialog open={open()} onClose={() => setOpen(false)}
    title="Delete topic?" confirmLabel="Delete topic" confirmIcon="trash" state={{ kind: "idle" }}
    plan={() => new Promise<TokenPlan>((resolve) => replies.push(resolve))}
    describe={(plan) => plan.token ?? "none"} onConfirm={() => undefined} />);
  await flush();
  setOpen(false); await flush();
  setOpen(true); await flush();
  replies[1]?.({ token: "new plan", warnings: [] }); await flush(); await flush();
  replies[0]?.({ token: "old plan", warnings: [] }); await flush(); await flush();
  expect(document.body.textContent).toContain("new plan");
  expect(document.body.textContent).not.toContain("old plan");
  ui.dispose();
});

test("a topic confirmation rechecks a policy changed after planning", async () => {
  const [blocked, setBlocked] = createSignal<string | undefined>(undefined);
  const plan = { topic: "orders", partitions: 1, records: 1, autoCreateEnabled: false,
    warnings: [], token: "test-token", expiresAt: "2026-09-06T00:05:00Z" };
  const host = topicsHost({ at: "/clusters/policy-changing/topics/orders", writeBlocked: () => blocked(), answers: {
    "/api/v1/clusters/{clusterId}/topics/{topicName}/overview": { topic: { status: "ok", data: {
      row: { name: "orders", internal: false, partitionCount: 1, replicationFactor: 1, outOfSyncReplicas: 0, offlinePartitions: 0 }, partitions: [],
    } } },
    "/api/v1/clusters/{clusterId}/topics/{topicName}/deletion/plan": plan,
    "/api/v1/clusters/{clusterId}/topics/{topicName}": plan,
  } });
  const ui = mount(host.view); await settle();
  [...ui.container.querySelectorAll("button")].find((b) => b.textContent?.includes("Delete topic"))?.click();
  await settle();
  const dialog = document.querySelector('[data-testid="planned-action-confirm"]');
  expect(dialog).not.toBeNull();
  const input = dialog!.querySelector<HTMLInputElement>('input[type="text"]')!;
  input.value = "orders"; input.dispatchEvent(new Event("input", { bubbles: true })); await flush();
  setBlocked("Read-only policy changed"); await flush();
  [...dialog!.querySelectorAll("button")].find((b) => b.textContent?.trim() === "Delete topic")?.click();
  await settle();
  expect(host.stub.calls).not.toContain("/api/v1/clusters/{clusterId}/topics/{topicName}");
  ui.dispose();
});
