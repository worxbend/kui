/**
 * Refuses to run against a stack that is not there, and says how to start one.
 *
 * The failure this prevents is a suite that reports twenty failing tests when the real problem is
 * that nothing is listening — twenty screenshots of a connection error, and a reader who has to
 * open one to find out. One message at the top is worth more than all of them.
 *
 * It also refuses to run against a stack that is *up but has not looked at Kafka yet*, which is a
 * different failure with the same shape. See `awaitFirstScrape` below.
 */
import { API, UI } from "../playwright.config";
import { API_SESSION_STATE, mintSharedSession, SESSION_STATE } from "./session";

/**
 * The shared session's `Cookie` header once `mintSharedSession` has run, so that this file's own
 * polling does not mint a session per request and spend the budget the suite is about to need.
 */
let sessionCookie = "";
const withSession = (): Record<string, string> =>
  sessionCookie === "" ? {} : { cookie: sessionCookie };

async function reachable(url: string): Promise<boolean> {
  try {
    const response = await fetch(url, { signal: AbortSignal.timeout(3_000) });
    return response.ok;
  } catch {
    return false;
  }
}

/** The build the process at each address reports. Two different answers mean two processes. */
async function sameGateway(): Promise<boolean> {
  const commit = async (base: string): Promise<string | undefined> => {
    try {
      const response = await fetch(`${base}/api/v1/info`, {
        headers: withSession(),
        signal: AbortSignal.timeout(3_000),
      });
      const body = (await response.json()) as { build?: { gitCommit?: string } };
      return body.build?.gitCommit;
    } catch {
      return undefined;
    }
  };
  const [direct, throughProxy] = await Promise.all([commit(API), commit(UI)]);
  return direct !== undefined && direct === throughProxy;
}

/** One ADR-034 section: a status, and the rows the answer carries when there are any. */
interface Section<T> {
  readonly status?: string;
  readonly data?: T;
}

/** How long the first scrape may take before this is a broken deployment rather than a slow one. */
const SCRAPE_TIMEOUT_MS = 120_000;
const SCRAPE_POLL_MS = 2_000;

async function sectionRows(url: string, key: string): Promise<number | undefined> {
  try {
    const response = await fetch(url, { headers: withSession(), signal: AbortSignal.timeout(5_000) });
    if (!response.ok) return undefined;
    const body = (await response.json()) as Record<string, Section<readonly unknown[]> | undefined>;
    const section = body[key];
    if (section?.status !== "ok") return undefined;
    return (section.data ?? []).length;
  } catch {
    return undefined;
  }
}

/**
 * Waits until the cluster service has finished its first scrape of every registered cluster.
 *
 * ## The failure this exists for, and why "up" was not the right question
 *
 * The three checks above establish that the stack is *listening*. They say nothing about whether it
 * has *asked Kafka anything yet*, and those are different states that a suite cannot tell apart
 * from a screen. Wave 9 lost `brokers.spec.ts:157` on a stack ninety seconds old: that case branches
 * on whether the deployment reports a disk capacity, `/log-dirs` answered an empty list, the case
 * took the "no capacity" arm and then spent fifteen seconds waiting for a sentence the warm product
 * does not draw. A perfectly healthy stack, a correct product, and a red case.
 *
 * ## What is actually polled, measured rather than guessed
 *
 * Measured on the quickstart on 2026-09-11, restarting `kui-quickstart-kui` and asking every two
 * seconds. The three column headings are shortened to fit, and the whole paths are written out here
 * because until wave 11 they were not and the shortened ones do not exist: readiness is
 * `${API}/api/v1/health/ready` — plain `/health/ready` answers **404** on the quickstart — and the
 * other two are `${API}/api/v1/clusters/{id}/brokers` and `.../log-dirs`, which is what the poll
 * below actually asks for. A reader copying a heading into `curl` got a 404 and no way to tell it
 * from the outage this table is about. (Filed by W11-03, whose `smoke.sh` asks the real one.)
 *
 * | t     | ready           | `/brokers`               | `/log-dirs`              |
 * | ----- | --------------- | ------------------------ | ------------------------ |
 * | 0–2s  | connection refused | —                     | —                        |
 * | 4–31s | 200             | `ok`, **1 broker**       | `ok`, **0 directories**  |
 * | 33s   | 200             | `ok`, 1 broker, new `fetchedAt` | `ok`, **1 directory**, `totalBytes` 203 GB |
 *
 * So readiness is true twenty-nine seconds before the first scrape lands, `/brokers` is `ok` with a
 * broker in it for that whole window, and the one thing that changes at the moment of the scrape is
 * that `/log-dirs` stops being empty. `brokers.fetchedAt` moves on the same tick — thirty seconds
 * after the first, which is the scrape interval.
 *
 * `status === "ok"` with an **empty** list is therefore the state to wait out, and it is worth
 * saying why that is honest rather than a workaround: an empty list is a true answer. The service
 * really has no log directories to report, because it has not asked for any yet. It is not an
 * error and it should not be reported as one — which is exactly why nothing in the product could
 * have caught this, and why it belongs to the harness.
 *
 * Both sections are polled, and not only the one that moves. `/brokers` costs nothing here and a
 * deployment where it is the slow one would otherwise trade this failure for the same failure on a
 * different case.
 *
 * ## Why it can time out rather than wait for ever
 *
 * A deployment that genuinely reports no log directories — an old broker that does not send the
 * filesystem size, a cluster the service cannot reach — never satisfies this, and a harness that
 * waited for ever would turn that into a hang with no message. Two minutes is four times the
 * thirty-three seconds measured, and the message says which cluster and which section was still
 * empty so that a reader can tell "slow" from "will never happen".
 */
async function awaitFirstScrape(): Promise<void> {
  const clusters = await registeredClusters();
  if (clusters.length === 0) {
    throw new Error(
      [
        `${API} registers no cluster at all, so there is nothing for this suite to drive.`,
        "",
        "Every case here names a cluster id. Start the stack again:",
        "",
        "  deployment/quickstart/quickstart.sh",
      ].join("\n"),
    );
  }

  const started = Date.now();
  const deadline = started + SCRAPE_TIMEOUT_MS;
  let waiting: string[] = [];
  let waited = false;

  for (;;) {
    const pending: string[] = [];
    for (const cluster of clusters) {
      const [brokers, dirs] = await Promise.all([
        sectionRows(`${API}/api/v1/clusters/${cluster}/brokers`, "brokers"),
        sectionRows(`${API}/api/v1/clusters/${cluster}/log-dirs`, "logDirs"),
      ]);
      if (brokers === undefined || brokers === 0) pending.push(`${cluster}: /brokers`);
      if (dirs === undefined || dirs === 0) pending.push(`${cluster}: /log-dirs`);
    }

    if (pending.length === 0) {
      // Said out loud only when there was something to wait for. A line on every run would be
      // noise; a silent thirty-second pause before the first test is the thing that makes a person
      // reach for Ctrl-C, and then wonder why the suite is slow.
      if (waited) {
        const seconds = ((Date.now() - started) / 1000).toFixed(0);
        console.log(
          `[globalSetup] waited ${seconds}s for the first scrape of ` +
            `${clusters.length} cluster(s) to land.`,
        );
      }
      return;
    }
    waiting = pending;
    waited = true;
    if (Date.now() >= deadline) break;
    await new Promise((wake) => setTimeout(wake, SCRAPE_POLL_MS));
  }

  throw new Error(
    [
      `The stack is up but has not finished its first scrape after ${SCRAPE_TIMEOUT_MS / 1000}s.`,
      "",
      "Still empty or unreadable:",
      ...waiting.map((one) => `  ${one}`),
      "",
      "Measured on a healthy quickstart this takes about 33 seconds from container start. A section",
      "that stays empty past two minutes is not slow — it is a deployment that cannot answer the",
      "question, and cases that branch on what it reports would take the wrong arm and then time out",
      "one by one. Check the broker is reachable from the KUI container:",
      "",
      "  deployment/quickstart/quickstart.sh logs",
    ].join("\n"),
  );
}

/** The cluster ids this deployment registers, read off the gateway rather than written down. */
async function registeredClusters(): Promise<readonly string[]> {
  try {
    const response = await fetch(`${API}/api/v1/clusters`, {
      headers: withSession(),
      signal: AbortSignal.timeout(5_000),
    });
    if (!response.ok) return [];
    const body = (await response.json()) as {
      clusters?: Section<readonly { cluster?: { id?: string } }[]>;
    };
    return (body.clusters?.data ?? [])
      .map((entry) => entry.cluster?.id)
      .filter((id): id is string => typeof id === "string");
  } catch {
    return [];
  }
}

export default async function globalSetup(): Promise<void> {
  /*
   * Three checks, and the third is the one that matters.
   *
   * The interface answers `/healthz` from nginx without touching anything behind it, and the API
   * answers on its own published port. Both can be true while the *browser's* path is broken: nginx
   * resolves the gateway by service name on the compose network, so a gateway that is down — or a
   * stray development server holding the published port while the container is not running — gives
   * a healthy interface, a healthy-looking API, and a 502 on every request the browser makes.
   *
   * That happened, and it cost twenty minutes of reading screenshots. So the last check goes through
   * the proxy, which is the only path the tests use.
   */
  const [ui, api, proxied] = await Promise.all([
    reachable(`${UI}/healthz`),
    reachable(`${API}/api/v1/health/ready`),
    reachable(`${UI}/api/v1/clusters`),
  ]);

  /*
   * One session for the browser and one for the `api` fixture, minted here once for the whole run
   * rather than once per case; `session.ts` says why — the gateway budgets new sessions per
   * address. This file's own polling below carries the API one.
   */
  if (ui && api && proxied) {
    await mintSharedSession(UI, SESSION_STATE);
    sessionCookie = await mintSharedSession(API, API_SESSION_STATE);
  }

  /*
   * And that the two are the *same* gateway.
   *
   * This check exists because the alternative wasted an afternoon. A development server left running
   * on the API's published port answered every health check cheerfully while the container behind
   * nginx was a different process entirely — with a different configuration and no clusters in it.
   * The tests then seeded through one gateway and asserted against another, and failed with
   * "cluster 'quickstart' does not exist" on a stack where it plainly did.
   *
   * The build's commit is the cheapest thing that distinguishes two processes, and `/api/v1/info`
   * carries it on both paths.
   */
  if (ui && api && proxied && !(await sameGateway())) {
    throw new Error(
      [
        `${API} and ${UI}/api are not the same gateway.`,
        "",
        "Something else is listening on the API's port — most often a `./mill dev` left running",
        "while the container was restarted. The tests would seed through one and assert against the",
        "other. Stop it, and start the stack again:",
        "",
        "  deployment/quickstart/quickstart.sh",
      ].join("\n"),
    );
  }

  /*
   * And last, that it has looked at Kafka. Up is not the same as scraped, and the difference costs
   * a case rather than a suite — see `awaitFirstScrape`.
   */
  if (ui && api && proxied) {
    await awaitFirstScrape();
    return;
  }

  const missing = [
    !ui ? `the interface at ${UI}` : "",
    !api ? `the API at ${API}` : "",
    ui && !proxied ? `the API *through* the interface at ${UI}/api/v1 (nginx is up; what it proxies to is not)` : "",
  ]
    .filter((one) => one !== "")
    .join(", and ");

  throw new Error(
    [
      `Cannot reach ${missing}.`,
      "",
      "This suite drives a running stack rather than starting one — starting a Kafka cluster,",
      "seeding it and building two images is what the quickstart script is for, and it is the same",
      "command a person runs:",
      "",
      "  deployment/quickstart/quickstart.sh",
      "",
      "Then: pnpm e2e",
    ].join("\n"),
  );
}
