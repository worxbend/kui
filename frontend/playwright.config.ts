/**
 * The end-to-end suite: a real browser, against the product as it is deployed.
 *
 * ## Why this replaced a Scala suite rather than repairing it
 *
 * There was a Playwright suite in `e2e/`, written in Scala, and it went red when the Laminar
 * frontend was deleted. Repairing it looked cheaper than it was:
 *
 * - Almost none of the browser half survived. Twelve of its twenty-six selectors named `data-testid`
 *   attributes the deleted components carried, including the one every test waited on first.
 * - The larger job — the fixtures — was identical in either language, because the shape changed
 *   underneath both: the interface and the API are no longer one server, so a suite has to point a
 *   browser at one origin and assert against another. In TypeScript that is `baseURL` and a few
 *   lines here; in the Scala suite it was several hundred hand-rolled lines.
 * - And it would have put Node and pnpm back into the Mill build, which the split was for. The
 *   suite needs the interface built; if Mill runs the suite, Mill needs the interface.
 *
 * So the browser suite lives with the thing it drives. Mill still owns the *backend's* integration
 * tests — the compose fault-isolation script is not a browser test and stays where it is.
 *
 * ## It drives a running stack rather than starting one
 *
 * `webServer` can start a process; it cannot sensibly start a Kafka cluster, seed it, wait for a
 * schema registry and build two images. `deployment/quickstart/quickstart.sh` does all of that and
 * is the same command a person runs, so the suite checks the stack is up and says how to start it
 * if not. A suite that silently started its own stack would also be a suite that passed against a
 * deployment nobody else can reproduce.
 */
import { defineConfig, devices } from "@playwright/test";
import { SESSION_STATE } from "./e2e/session";

/** Where the interface is. The frontend image publishes 8090; `KUI_FRONTEND_PORT` overrides it. */
const UI = process.env.KUI_E2E_UI ?? "http://localhost:8090";

/**
 * Where the API is, for the handful of assertions that are about the *server's* answer rather than
 * the screen — and for the fixtures that put a cluster into a known state before a test looks at it.
 *
 * Deliberately a second base rather than going through the interface's proxy: a test that seeds
 * through the proxy cannot tell a seeding failure from a proxy failure.
 */
const API = process.env.KUI_E2E_API ?? "http://localhost:8080";

export default defineConfig({
  testDir: "./e2e",
  // The suite drives one shared cluster, so tests that write must not run beside each other. They
  // are marked serial in their own files; this keeps the default honest for the ones that read.
  fullyParallel: false,
  workers: 1,
  // A retry hides a flake and this suite is meant to find them. In CI one retry distinguishes "the
  // product is broken" from "the container had not finished starting", which is worth the noise.
  retries: process.env.CI ? 1 : 0,
  timeout: 60_000,
  expect: { timeout: 15_000 },
  reporter: process.env.CI ? [["github"], ["html", { open: "never" }]] : [["list"]],
  globalSetup: "./e2e/globalSetup.ts",
  use: {
    baseURL: UI,
    // The four things that explain a failure without anybody reproducing it. On failure only: on
    // success they are hundreds of megabytes of noise.
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
    // The one gateway session `globalSetup` minted; `e2e/session.ts` says why every case shares it.
    storageState: SESSION_STATE,
  },
  projects: [
    { name: "chromium", use: { ...devices["Desktop Chrome"] } },
  ],
  metadata: { api: API },
});

export { API, UI };
