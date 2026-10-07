import { expect, it, vi } from "vitest";
import { createSignal, flush } from "solid-js";
import { createRouter, memoryHistory } from "@solidjs/router";
import { KuiProvider, sharedQueries, type KnownAction } from "@kui/kernel";
import { Actions, type KuiApiClient } from "@kui/api";
import { mount, testContext } from "./testing.js";
import Schemas from "./SchemasRoute.jsx";
import schema from "./recorded/schema.json";
import subjects from "./recorded/subjects.json";

it("77: schema writes follow shared policy at entry and inside an open registration form", async () => {
  sharedQueries.invalidateWhere(() => true);
  const [blocked, setBlocked] = createSignal(false);
  const writeBlocked = vi.fn((_cluster: string, _action: KnownAction, _name?: string) => blocked() ? "Cluster is read-only" : undefined);
  const post = vi.fn(async () => ({ ok: true, value: {} }));
  const api = { get: async (path: string) => ({ ok: true, value: path.endsWith("/subjects") ? subjects
    : path.endsWith("/compatibility") ? { level: "BACKWARD", inheritedFromGlobal: true }
    : path.endsWith("/versions") ? { versions: [1] } : schema }), post, put: post } as unknown as KuiApiClient;
  const Router = createRouter({ routes: [{ path: "/clusters/:clusterId/schemas/:subject", component: Schemas }],
    history: memoryHistory("/clusters/quickstart/schemas/orders.avro-value"), scrollRestoration: false });
  const view = mount(() => <KuiProvider value={{ ...testContext(api), writeBlocked }}><Router /></KuiProvider>);
  try {
    for (let i = 0; i < 10; i++) await flush();
    [...view.container.querySelectorAll("button")].find(b => b.textContent?.trim() === "Register schema")!.click(); await flush();
    const dialog = document.querySelector('[data-testid="register-schema-dialog"]')!;
    const subject = dialog.querySelector("input")!; subject.value = "orders.avro-value"; subject.dispatchEvent(new Event("input", { bubbles: true }));
    const editor = dialog.querySelector("textarea")!; editor.value = '"string"'; editor.dispatchEvent(new Event("input", { bubbles: true })); await flush();
    setBlocked(true); await flush();
    const register = [...dialog.querySelectorAll("button")].find(b => b.textContent?.trim() === "Register")!;
    expect(register.getAttribute("aria-disabled")).toBe("true"); register.click(); await flush();
    expect(post).not.toHaveBeenCalled();
    expect(writeBlocked).toHaveBeenCalledWith("quickstart", Actions.SchemaCreate, "orders.avro-value");
    expect(writeBlocked).toHaveBeenCalledWith("quickstart", Actions.SchemaEdit, "orders.avro-value");
    expect(writeBlocked).toHaveBeenCalledWith("quickstart", Actions.SchemaModifyGlobalCompatibility);
    expect(view.container.textContent).toContain("Cluster is read-only");
  } finally { view.dispose(); sharedQueries.invalidateWhere(() => true); }
});
