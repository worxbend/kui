import { afterEach, expect, it } from "vitest";
import { createRouter, memoryHistory } from "@solidjs/router";
import { flush } from "solid-js";
import { KuiProvider, sharedQueries } from "@kui/kernel";
import type { KuiApiClient } from "@kui/api";
import Schemas from "./SchemasRoute.jsx";
import { mount, testContext } from "./testing.js";
import schemaDocument from "./recorded/schema.json";
import subjectDocument from "./recorded/subjects.json";

const settle = async () => {
  for (let i = 0; i < 30; i++) { await Promise.resolve(); await flush(); }
};
afterEach(() => sharedQueries.invalidateWhere(() => true));

it("keeps the no-selection instruction when the keyed subject owner is absent", async () => {
  const api = { get: async (path: string) => ({ ok: true, value: path.endsWith("/subjects") ? subjectDocument : { level: "BACKWARD" } }) } as unknown as KuiApiClient;
  const Router = createRouter({ routes: [{ path: "/clusters/:clusterId/schemas", component: Schemas }], history: memoryHistory("/clusters/no-selection/schemas"), scrollRestoration: false });
  const view = mount(() => <KuiProvider value={testContext(api)}><Router /></KuiProvider>);
  try {
    await settle();
    expect(view.container.textContent).toContain("No subject selected.");
  } finally { view.dispose(); }
});

it.each([
  ["cluster", false], ["cluster", true], ["subject", false], ["subject", true],
] as const)("owns compatibility results by %s (pending: %s)", async (changed, pending) => {
  const posts: Array<{ params: { path: { clusterId: string; subject: string } } }> = [];
  const replies: Array<(answer: unknown) => void> = [];
  const api = {
    get: async (path: string) => ({ ok: true, value: path.endsWith("/subjects") ? subjectDocument :
      path.endsWith("/compatibility") ? { level: "BACKWARD", inheritedFromGlobal: true } :
      path.endsWith("/versions") ? { versions: [1] } : schemaDocument }),
    post: (_path: string, init: typeof posts[number]) => {
      posts.push(init);
      return new Promise((resolve) => { replies.push(resolve); });
    },
  } as unknown as KuiApiClient;
  const oldRoute = "/clusters/old-schema-cluster/schemas/orders.avro-value?version=1";
  const nextCluster = changed === "cluster" ? "new-schema-cluster" : "old-schema-cluster";
  const nextSubject = changed === "subject" ? "orders.copy-value" : "orders.avro-value";
  const history = memoryHistory(oldRoute);
  const Router = createRouter({ routes: [{ path: "/clusters/:clusterId/schemas/:subject", component: Schemas }], history, scrollRestoration: false });
  const view = mount(() => <KuiProvider value={testContext(api)}><Router /></KuiProvider>);
  const check = () => [...view.container.querySelectorAll("button")].find((button) => button.textContent?.trim() === "Check compatibility")!.click();
  const verdict = () => view.container.querySelector('[data-testid="compatibility-verdict"]');
  const answer = { ok: true, value: { compatible: true, messages: [] } };
  try {
    await settle(); check(); await settle();
    expect(posts).toHaveLength(1);
    if (!pending) { replies[0]!(answer); await settle(); expect(verdict()).not.toBeNull(); }
    history.set({ value: `/clusters/${nextCluster}/schemas/${nextSubject}?version=1` });
    await settle();
    expect(verdict()).toBeNull();
    expect(posts).toHaveLength(1);
    // The new workspace is usable immediately, and only its own answer may render a verdict.
    check(); await settle();
    expect(posts).toHaveLength(2);
    expect(posts[1]?.params.path).toMatchObject({ clusterId: nextCluster, subject: nextSubject });
    replies[1]!({ ok: true, value: { compatible: false, messages: ["new registry refused"] } }); await settle();
    if (pending) { replies[0]!(answer); await settle(); }
    expect(verdict()?.textContent).toContain("Would be refused");
    expect(verdict()?.textContent).not.toContain("Would be accepted");
    history.set({ value: oldRoute }); await settle();
    expect(verdict()).toBeNull();
  } finally { view.dispose(); }
});
