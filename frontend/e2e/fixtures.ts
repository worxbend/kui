/**
 * What every spec needs: a way to talk to the API directly, and a cluster in a known state.
 *
 * ## Why the API fixtures do not go through the interface
 *
 * A test that seeds through the screen it is about to assert on cannot tell a seeding failure from
 * the failure it is looking for. So arranging happens over HTTP against the gateway, and the browser
 * is only used for the thing under test.
 */
import { test as base, expect, type APIRequestContext } from "@playwright/test";
import { API } from "../playwright.config";
import { API_SESSION_STATE } from "./session";

export const CLUSTER = "quickstart";

/** A client that carries a session and the CSRF header every mutation needs (ADR-019). */
export interface KuiApi {
  readonly get: (path: string) => Promise<unknown>;
  readonly post: (path: string, body?: unknown) => Promise<unknown>;
  readonly del: (path: string) => Promise<unknown>;
  /** The raw request context, for a test that wants the status rather than the body. */
  readonly raw: APIRequestContext;
}

async function signedIn(request: APIRequestContext): Promise<string> {
  const me = await request.get(`${API}/api/v1/auth/me`);
  const body = (await me.json()) as { csrfToken?: string };
  return body.csrfToken ?? "";
}

export const test = base.extend<{ api: KuiApi; browserHealth: void }>({
  browserHealth: [
    async ({ page }, use, testInfo) => {
      const failures: string[] = [];
      page.on("pageerror", (error) => failures.push(`pageerror: ${error.message}`));
      page.on("console", (message) => {
        if (message.type() !== "error") return;

        const text = message.text();
        // Chromium reports every non-2xx fetch as a generic console error, including responses the
        // product deliberately consumes and renders (for example Kafka's 409 for a live consumer
        // group and 501 for an unconfigured optional service). Those outcomes belong to the UI
        // assertions in each scenario; the message contains neither the request URL nor enough
        // context to distinguish them from an unexpected response. Keep this fixture focused on
        // actual browser/runtime failures. Broken assets and unhandled API failures still make the
        // relevant screen assertion fail, while page errors and application console errors remain
        // fatal here.
        if (text.startsWith("Failed to load resource: the server responded with a status of")) return;

        failures.push(`console.error: ${text}`);
      });

      await use();

      if (failures.length > 0) {
        await testInfo.attach("browser-errors.json", {
          body: Buffer.from(JSON.stringify(failures, null, 2)),
          contentType: "application/json",
        });
        throw new Error(`The browser reported errors:\n${failures.join("\n")}`);
      }
    },
    { auto: true },
  ],
  api: async ({ playwright }, use) => {
    // Its own context, with its own cookie jar: the browser's session and this one are different
    // principals as far as the gateway is concerned, and sharing them would make a permissions test
    // depend on whichever ran first. Its jar is still seeded with a session minted once for the
    // whole run (`session.ts` says why), and that session is not the browser's.
    const request = await playwright.request.newContext({ baseURL: API, storageState: API_SESSION_STATE });
    const token = await signedIn(request);

    const json = async (response: Awaited<ReturnType<APIRequestContext["get"]>>) => {
      const text = await response.text();
      if (!response.ok()) {
        throw new Error(`${response.status()} from ${response.url()}: ${text.slice(0, 300)}`);
      }
      return text === "" ? undefined : JSON.parse(text);
    };

    await use({
      raw: request,
      get: async (path) => json(await request.get(`${API}${path}`)),
      post: async (path, body) =>
        json(
          await request.post(`${API}${path}`, {
            headers: { "X-Csrf-Token": token, "Content-Type": "application/json" },
            data: body ?? {},
          }),
        ),
      del: async (path) =>
        json(await request.delete(`${API}${path}`, { headers: { "X-Csrf-Token": token } })),
    });

    await request.dispose();
  },
});

export { expect };

/** A topic name nothing else will collide with, so a failed run leaves no landmine for the next. */
export function scratchTopic(what: string): string {
  return `kui-e2e-${what}-${Date.now()}`;
}

/**
 * Removes a topic, and does not care whether it was there.
 *
 * Cleanup that fails when the thing is already gone turns one failing test into two, and the second
 * one is about the cleanup.
 */
export async function removeTopic(api: KuiApi, name: string): Promise<void> {
  try {
    const plan = (await api.post(
      `/api/v1/clusters/${CLUSTER}/topics/${encodeURIComponent(name)}/deletion/plan`,
    )) as { token?: string };
    if (plan?.token !== undefined) {
      await api.del(
        `/api/v1/clusters/${CLUSTER}/topics/${encodeURIComponent(name)}?token=${encodeURIComponent(plan.token)}`,
      );
    }
  } catch {
    /* Already gone, or never created. Either way there is nothing to do. */
  }
}
