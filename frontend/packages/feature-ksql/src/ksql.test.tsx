/**
 * The ksqlDB screen, mounted — which is where this packet's owned rule lives.
 *
 * ## The owned rule, and the direction it actually runs in
 *
 * **The statement editor's Run control is gated on `kui.permits(Actions.KsqlExecute)` — the
 * question with no subject in it — on the cluster the screen is showing.** `KsqlRoute.tsx` is where
 * the product asks it, so that is what these cases mount. Nothing here composes the rule by hand:
 * the harness holds grants in the wire's own shape and evaluates them through `@kui/kernel`'s
 * `grantsAllow` and `grantsAllowAny`, the same two functions `state/session.ts` calls in the
 * product.
 *
 * W8-05's plan asked for the *subject-aware* form and named the subjectless one as its mutation. On
 * `Resource.Ksql` that is the wrong way round and the tree says so in four places:
 * `KsqlEndpoints.executing` declares `ResourceRequirement.unnamed(Resource.Ksql,
 * Action.KsqlExecute)`, `Vocabulary.scala:84` puts `Ksql` among the unnamed resources,
 * `RbacConfigSection.scala:281` refuses a `KSQL` permission that carries a value, and `grantsAllow`
 * answers a named question `false` against a pattern-less grant. So the rule is closed in the
 * direction the vocabulary supports, and **both** directions are gated here: adding a subject
 * reddens *"offers a working Run to a principal who holds KSQL:EXECUTE"*, and dropping or weakening
 * the gate reddens the two refusal cases.
 *
 * ## The refusal cases assert the click, not only the attribute
 *
 * `Button` marks a refusal with `aria-disabled` and swallows the click itself, so a case that only
 * read the attribute would stay green if the handler were wired up unconditionally — the exact hole
 * `feature-connect` shipped, where the case named *"hands an unpermitted principal no working
 * control anywhere on the page"* measured `aria-disabled` only and was defused solely by the
 * kernel. These cases press Run and then assert that **nothing reached the server**.
 *
 * ## The documents are the service's own
 *
 * `src/documents/*.json` are **byte-for-byte copies** of
 * `services/ksql/contract/test/resources/golden/`, held that way by `wire.golden.test.ts`, so a
 * fixture here cannot become a third opinion about the wire. Two are hand-made because the service
 * commits no golden for them — `objects-forbidden.json` and `objects-empty.json` — and they are the
 * states a working ksqlDB cannot be put into on purpose, which is where this project's defects have
 * always lived. There were three: `statement-plan-push-query.json` was a plan no encoder had
 * produced, and W10-06 replaced it with `testing.ts`'s `pushQueryPlan`, which is derived from a
 * golden and says which two fields it changed.
 */
import { describe, expect, it, vi } from "vitest";
import { createSignal, flush } from "solid-js";
import {
  KuiProvider,
  createQueryRegistry,
  type PermissionGrant,
  type StreamTransport,
} from "@kui/kernel";

import { KsqlScreen } from "./KsqlRoute.jsx";
import {
  openPushQuery,
  pushQueryAddress,
  CLUSTER_PATH,
  KSQL_OBJECTS_PATH,
  KSQL_PLAN_PATH,
  KSQL_STATEMENTS_PATH,
  KSQL_STREAM_PATH,
  type PushQueryOpener,
  type PushQuerySubscriber,
} from "./data.js";
import {
  CELL_NULL,
  NOT_CONFIGURED,
  NO_OBJECTS,
  NO_ROWS,
  OFFSET_RESET_NOT_SETTABLE,
} from "./model.js";
import { KSQL_ROW_EVENT_NAME } from "./wire.js";
import {
  describeViolations,
  findViolations,
  grant,
  mount,
  pushQueryPlan,
  serving,
  testContext,
  TEST_CLUSTER,
} from "./testing.js";
import objectsResponse from "./documents/objects-response.json" with { type: "json" };
import objectsPartial from "./documents/objects-partial.json" with { type: "json" };
import objectsEmpty from "./documents/objects-empty.json" with { type: "json" };
import objectsNotConfigured from "./documents/objects-not-configured.json" with { type: "json" };
import objectsForbidden from "./documents/objects-forbidden.json" with { type: "json" };
import objectsUnavailable from "./documents/objects-unavailable.json" with { type: "json" };
import planHarmless from "./documents/statement-plan-harmless.json" with { type: "json" };
import planDrop from "./documents/statement-plan.json" with { type: "json" };
import resultRows from "./documents/statement-rows.json" with { type: "json" };
import resultStatus from "./documents/statement-status.json" with { type: "json" };

/** Solid batches writes onto a microtask; a query settles a couple of turns after it is read. */
async function settle(times = 10): Promise<void> {
  for (let index = 0; index < times; index += 1) await flush();
}

/** The cluster document, whose one interesting field is the deployment's read-only flag. */
function cluster(readOnly: boolean): unknown {
  return { cluster: { id: TEST_CLUSTER, name: TEST_CLUSTER, readOnly } };
}

interface Open {
  readonly container: HTMLElement;
  readonly dispose: () => void;
  readonly stub: ReturnType<typeof serving>;
  /** The registry this mount reads, so a case can make one key re-read while the screen is up. */
  readonly queries: ReturnType<typeof createQueryRegistry>;
}

function open(
  objects: unknown,
  options: {
    readonly grants?: readonly PermissionGrant[];
    readonly readOnly?: boolean;
    readonly plan?: unknown;
    readonly result?: unknown;
    readonly openStream?: PushQueryOpener | undefined;
    /** Runs before a write is answered. See `serving`: `undefined` back means "answer normally". */
    readonly write?: (path: string) => Promise<unknown>;
    readonly writeBlocked?: import("@kui/kernel").KuiContextValue["writeBlocked"];
    readonly clusterId?: () => string;
  } = {},
): Open {
  const stub = serving(
    {
      [KSQL_OBJECTS_PATH]: objects,
      [CLUSTER_PATH]: cluster(options.readOnly === true),
      ...(options.plan === undefined ? {} : { [KSQL_PLAN_PATH]: options.plan }),
      ...(options.result === undefined ? {} : { [KSQL_STATEMENTS_PATH]: options.result }),
    },
    options.write,
  );
  const queries = createQueryRegistry();
  const context = { ...testContext(stub.api, options.grants), ...(options.writeBlocked === undefined ? {} : { writeBlocked: options.writeBlocked }) };
  const mounted = mount(() => (
    <KuiProvider value={context}>
      <KsqlScreen
        clusterId={options.clusterId?.() ?? TEST_CLUSTER}
        queries={queries}
        {...(options.openStream === undefined ? {} : { openStream: options.openStream })}
      />
    </KuiProvider>
  ));
  return { ...mounted, stub, queries };
}

/** A button anywhere on the page, including inside the portalled confirmation dialogue. */
function button(container: HTMLElement, label: string): HTMLButtonElement | undefined {
  const search = [
    ...container.querySelectorAll("button"),
    ...document.body.querySelectorAll("button"),
  ];
  return search.find((one) => (one.textContent ?? "").includes(label));
}

/** Whether a control can actually be used. `Button` marks refusal with `aria-disabled`. */
function usable(control: HTMLButtonElement | undefined): boolean {
  return control !== undefined && control.getAttribute("aria-disabled") !== "true";
}

/** Types a statement into the editor, the way a reader does. */
function type(container: HTMLElement, sql: string): void {
  const editor = container.querySelector<HTMLTextAreaElement>("#kui-ksql-editor");
  if (editor === null) throw new Error("the screen drew no statement editor");
  editor.value = sql;
  editor.dispatchEvent(new Event("input", { bubbles: true }));
}

/** How many calls reached one address. The refusal cases are about this being zero. */
function calls(stub: ReturnType<typeof serving>, path: string): number {
  return stub.calls.filter((call) => call.path === path).length;
}

describe("the addresses this feature asks for", () => {
  it("spells each one out, so a wrong path is a red case and not a silent 404", () => {
    /*
     * Literals, and that is the whole point of the case. Every other assertion in this file reaches
     * the stub through the same constant the product built its request from, which asserts that a
     * constant equals itself: rename `KSQL_OBJECTS_PATH` to something the gateway does not serve
     * and all 72 cases stay green while the screen 404s. `data.ts` casts the client precisely
     * because the generated `paths` type cannot check these yet (W8-09 regenerates it after
     * `services/ksql` lands), so the path check the cast erases is rebuilt here and in
     * `e2e/ksql.spec.ts`.
     *
     * They are `KsqlEndpoints`' own segments under the gateway's public `/api/v1` prefix:
     * `clusters / {clusterId} / ksql / objects`, `… / statements / plan`, `… / statements`, and
     * `KsqlStreamEndpoint`'s `… / stream`.
     */
    expect(KSQL_OBJECTS_PATH).toBe("/api/v1/clusters/{clusterId}/ksql/objects");
    expect(KSQL_PLAN_PATH).toBe("/api/v1/clusters/{clusterId}/ksql/statements/plan");
    expect(KSQL_STATEMENTS_PATH).toBe("/api/v1/clusters/{clusterId}/ksql/statements");
    expect(KSQL_STREAM_PATH).toBe("/api/v1/clusters/{clusterId}/ksql/stream");
  });

  it("escapes the statement it puts in the push query's address", () => {
    /*
     * A `SELECT` carries `*`, `=`, `'`, `;` and spaces, and a query string that did not escape them
     * would either be truncated at the first `&` or rejected by the gateway — and neither failure
     * says "the URL was built wrong". `KsqlStreamEndpoint.StatementParam` is `statement`.
     *
     * The expectation is `URLSearchParams`' own output and not a tidier spelling of it: it encodes
     * a space as `+` and leaves `*` alone, both of which are correct in a query string and neither
     * of which is what `encodeURIComponent` would have produced. Writing the prettier string here
     * would be asserting a different encoder.
     */
    const address = pushQueryAddress("quickstart", "SELECT * FROM ORDERS EMIT CHANGES;");
    expect(address).toBe(
      "/api/v1/clusters/quickstart/ksql/stream" +
        "?statement=SELECT+*+FROM+ORDERS+EMIT+CHANGES%3B",
    );
  });
});

/* ------------------------------------------------------------------------------------------------
 * The stream client itself
 * ---------------------------------------------------------------------------------------------- */

/**
 * A stream whose bytes this case writes.
 *
 * `openPushQuery` is otherwise reachable from nothing: `KsqlScreen` takes a `PushQueryOpener`, so
 * every screen case above replaces the whole function — and with it the event name it registers,
 * the JSON it parses and the failures it forwards. Measured: renaming the event to `"ksql-row"`
 * left 74/74 green before this case existed. `openFetchStreamWith` is the kernel's own seam and
 * `feature-messages` uses the same pair.
 */
function textTransport(body: string): StreamTransport {
  let stopped = false;
  return {
    send: async () => ({
      status: 200,
      text: async () => body,
      readChunks: (handlers) => {
        // One chunk and then the end. The kernel's parser is fed the same way a real body feeds it;
        // splitting the string would test the parser, which `@kui/kernel` already does.
        handlers.onChunk(body);
        handlers.onDone();
      },
    }),
    abort: () => {
      stopped = true;
    },
    aborted: () => stopped,
  };
}

describe("the push query's stream client", () => {
  it("reports EOF without done as an interrupted query and retains received rows", async () => {
    const ui = open(objectsResponse, {
      grants: MAY_EXECUTE, plan: pushQueryPlan,
      openStream: (cluster, sql, subscriber) => openPushQuery(cluster, sql, subscriber,
        textTransport('event: row\ndata: {"values":["kept"]}\n\n')),
    });
    await settle(); type(ui.container, "SELECT * FROM ORDERS EMIT CHANGES;"); await settle();
    button(ui.container, "Run query")?.click(); await settle();
    expect(ui.container.querySelector('[data-testid="ksql-result"]')?.getAttribute("data-kind")).toBe("interrupted");
    expect(ui.container.textContent).toContain("kept");
    ui.dispose();
  });

  it("resolves a push stream through the configured API URL exactly once", async () => {
    const fetch = vi.fn(async (_input: RequestInfo | URL) => new Response('event: done\ndata: {"reason":"exhausted"}\n\n', {
      headers: { "Content-Type": "text/event-stream" },
    }));
    vi.stubGlobal("fetch", fetch);
    const resolve = vi.fn((path: string) => `https://gateway.example/custom${path}`);
    try {
      openPushQuery(TEST_CLUSTER, "SELECT * FROM ORDERS EMIT CHANGES;", {
        onColumns: () => undefined, onRow: () => undefined, onError: () => undefined,
      }, undefined, resolve);
      await settle();
      expect(resolve).toHaveBeenCalledTimes(1);
      expect(String(fetch.mock.calls[0]?.[0])).toContain("https://gateway.example/custom/api/v1/clusters/");
    } finally { vi.unstubAllGlobals(); }
  });

  it("ends the result region only after a done frame", async () => {
    const ui = open(objectsResponse, {
      grants: MAY_EXECUTE, plan: pushQueryPlan,
      openStream: (cluster, sql, subscriber) => openPushQuery(cluster, sql, subscriber,
        textTransport('event: row\ndata: {"values":["kept"]}\n\nevent: done\ndata: {"reason":"exhausted"}\n\n')),
    });
    await settle();
    type(ui.container, "SELECT * FROM ORDERS EMIT CHANGES;"); await settle();
    button(ui.container, "Run query")?.click(); await settle();
    expect(ui.container.querySelector('[data-testid="ksql-result"]')?.getAttribute("data-kind")).toBe("ended");
    expect(ui.container.textContent).toContain("kept");
    ui.dispose();
  });
  it("delivers the columns and the rows the service's own frames carry", async () => {
    /*
     * The two committed frames, written down the wire exactly as ADR-035 frames them. This is the
     * only case that exercises the event name `openPushQuery` registers — `wire.golden.test.ts`
     * pins the *name* against the golden, and this pins that the client actually listens on it.
     */
    const body =
      "event: phase\ndata: {\"columns\":[\"ID\",\"TOTAL\",\"NOTE\"]}\n\n" +
      "event: row\ndata: {\"values\":[\"17\",\"42.50\",null]}\n\n" +
      "event: row\ndata: not json\n\n" +
      "event: row\ndata: {\"values\":[\"18\",\"11.00\",\"gift wrap\"]}\n\n" +
      'event: done\ndata: {"reason":"exhausted"}\n\n';

    const columns: (readonly string[])[] = [];
    const rows: unknown[] = [];
    const failures: string[] = [];

    openPushQuery(
      TEST_CLUSTER,
      "SELECT * FROM ORDERS EMIT CHANGES;",
      {
        onColumns: (next) => columns.push(next),
        onRow: (row) => rows.push(row),
        onError: (error) => failures.push(error.kind),
      },
      textTransport(body),
    );
    await settle();

    expect(columns).toEqual([["ID", "TOTAL", "NOTE"]]);
    // The unreadable frame between them is a `decode` failure and **not** terminal: one malformed
    // row must not end a query that is otherwise delivering good ones.
    expect(rows).toEqual([
      ["17", "42.50", null],
      ["18", "11.00", "gift wrap"],
    ]);
    expect(failures).toEqual(["decode"]);
  });
});

/* ------------------------------------------------------------------------------------------------
 * The owned rule
 * ---------------------------------------------------------------------------------------------- */

/**
 * A grant over `KSQL` as the server can actually load one: no pattern.
 *
 * `RbacConfigSection.scala:281` refuses the configuration file for a `KSQL` permission carrying a
 * value — *"KSQL has no name to match against"* — so a grant written with one here would be a grant
 * no deployment could have, and a case built on it would prove nothing about the product.
 */
const MAY_EXECUTE: readonly PermissionGrant[] = [grant("KSQL", ["VIEW", "EXECUTE"], undefined)];
const MAY_ONLY_VIEW: readonly PermissionGrant[] = [grant("KSQL", ["VIEW"], undefined)];
const MAY_EXECUTE_ELSEWHERE: readonly PermissionGrant[] = [
  grant("KSQL", ["VIEW", "EXECUTE"], undefined, ["some-other-cluster"]),
];

describe("pending statement receipts", () => {
  it.each(["QUEUED", "PARSING", "EXECUTING", undefined])("keeps %s pending rather than claiming completion or non-execution", async (message) => {
    const { container, stub, dispose } = open(objectsResponse, {
      grants: MAY_EXECUTE, plan: planHarmless,
      result: { ...resultStatus, outcome: "pending", message, entity: "stream/ORDERS/create" },
    });
    try {
      await settle();
      type(container, planHarmless.statement);
      await settle();
      button(container, "Run query")?.click();
      await settle();
      const pending = container.querySelector("[data-testid='ksql-result-pending']");
      expect(pending).not.toBeNull();
      expect(pending?.textContent).toContain("Pending");
      expect(pending?.textContent).toContain("completion is unknown");
      expect(pending?.textContent).toContain("Do not resubmit");
      expect(pending?.textContent).toContain("stream/ORDERS/create");
      if (message !== undefined) expect(pending?.textContent).toContain(message);
      expect(container.querySelector("[data-testid='ksql-result-status']")).toBeNull();
      expect(container.querySelector("[data-testid='ksql-result-failed']")).toBeNull();
      expect(container.querySelector("[data-testid='ksql-result-count']")).toBeNull();
      expect(container.textContent).not.toContain("It acted on");
      expect(calls(stub, KSQL_STATEMENTS_PATH)).toBe(1);
    } finally { dispose(); }
  });
});

describe("the statement editor's permission question", () => {
  it("adopts the shared cluster write policy", async () => {
    const ui = open(objectsResponse, { grants: MAY_EXECUTE, writeBlocked: () => "Read-only policy" });
    await settle(); type(ui.container, "SHOW STREAMS;"); await settle();
    expect(usable(button(ui.container, "Run query"))).toBe(false);
    button(ui.container, "Run query")?.click(); await settle();
    expect(calls(ui.stub, KSQL_PLAN_PATH)).toBe(0);
    ui.dispose();
  });
  it("offers a working Run to a principal who holds KSQL:EXECUTE on this cluster", async () => {
    /*
     * This is the case a subject reddens. `grantsAllowAny` ignores a grant's pattern, so the
     * subjectless question answers `true` here; `grantsAllow(…, "quickstart")` calls `covers`,
     * which answers `false` for a grant with no pattern — so naming the cluster, the object, or
     * anything else as a subject disables this button for a principal who genuinely holds the
     * permission, which is the permanently-dead-control defect `permits` documents at length.
     */
    const { container, stub, dispose } = open(objectsResponse, {
      grants: MAY_EXECUTE,
      plan: planHarmless,
      result: resultStatus,
    });
    await settle();

    type(container, "CREATE STREAM ENRICHED_ORDERS AS SELECT * FROM ORDERS EMIT CHANGES;");
    await settle();

    const run = button(container, "Run query");
    expect(usable(run), "a principal holding KSQL:EXECUTE was refused the Run control").toBe(true);

    run?.click();
    await settle();
    // The click, not the attribute: a handler wired up unconditionally passes the line above.
    expect(calls(stub, KSQL_PLAN_PATH)).toBe(1);
    dispose();
  });

  it("refuses a principal who holds only KSQL:VIEW, and sends nothing", async () => {
    const { container, stub, dispose } = open(objectsResponse, { grants: MAY_ONLY_VIEW });
    await settle();

    type(container, "CREATE STREAM ENRICHED_ORDERS AS SELECT * FROM ORDERS EMIT CHANGES;");
    await settle();

    const run = button(container, "Run query");
    expect(usable(run), "a KSQL:VIEW principal was offered a live Run control").toBe(false);

    run?.click();
    await settle();
    expect(calls(stub, KSQL_PLAN_PATH), "a refused Run still reached the server").toBe(0);
    expect(calls(stub, KSQL_STATEMENTS_PATH)).toBe(0);
    dispose();
  });

  it("refuses a principal whose KSQL:EXECUTE is on a different cluster", async () => {
    /*
     * The cluster scope is not lost by asking the question without a subject: `useKui().permits`
     * passes the frame's cluster id separately, and a grant's `clusters` list is what scopes it.
     */
    const { container, stub, dispose } = open(objectsResponse, { grants: MAY_EXECUTE_ELSEWHERE });
    await settle();

    type(container, "SELECT * FROM ORDERS;");
    await settle();

    expect(usable(button(container, "Run query"))).toBe(false);
    button(container, "Run query")?.click();
    await settle();
    expect(calls(stub, KSQL_PLAN_PATH)).toBe(0);
    dispose();
  });

  it("refuses a statement on a read-only cluster, however permitted the principal", async () => {
    /*
     * The server agrees and `RbacLawsSuite:315` says so: a read-only cluster still lists ksqlDB
     * objects and still refuses a ksqlDB statement, because `KsqlView` does not alter and
     * `KsqlExecute` does. `feature-alerts` passes `readOnly: false` as a hard-coded constant and a
     * permitted principal there gets a live Acknowledge on a read-only cluster; this screen reads
     * the cluster document instead.
     */
    const { container, stub, dispose } = open(objectsResponse, {
      grants: MAY_EXECUTE,
      readOnly: true,
      plan: planHarmless,
    });
    await settle();

    type(container, "INSERT INTO ORDERS VALUES ('x');");
    await settle();

    expect(usable(button(container, "Run query"))).toBe(false);
    button(container, "Run query")?.click();
    await settle();
    expect(calls(stub, KSQL_PLAN_PATH)).toBe(0);
    dispose();
  });

  it("goes on refusing when the cluster document is stale rather than fresh", async () => {
    /*
     * W13-A1: `readOnly()` reads the flag out of `ready` *and* `stale`, and every case above leaves
     * the query `ready`. Narrowing it to `state.kind === "ready"` left all 1,935 cases green while
     * re-enabling Run on a read-only cluster the moment its document went stale — which is not an
     * exotic state but the ordinary one after the server has been unreachable for a moment: the
     * cached answer still says `readOnly: true` and it is still true. A refusal that lapses because
     * a re-read failed is worse than one that never appeared, because the reader watched it work.
     */
    const { container, stub, queries, dispose } = open(objectsResponse, {
      grants: MAY_EXECUTE,
      readOnly: true,
      plan: planHarmless,
    });
    await settle();
    expect(usable(button(container, "Run query"))).toBe(false);

    stub.refuse(CLUSTER_PATH);
    queries.invalidate(`cluster-write-state|${TEST_CLUSTER}`);
    await settle();

    // The re-read happened and it failed, which is what puts the query into `stale` over the last
    // good answer; the flag in that answer has not changed and neither has the refusal.
    expect(calls(stub, CLUSTER_PATH)).toBeGreaterThan(1);
    type(container, "INSERT INTO ORDERS VALUES ('x');");
    await settle();

    expect(usable(button(container, "Run query"))).toBe(false);
    button(container, "Run query")?.click();
    await settle();
    expect(calls(stub, KSQL_PLAN_PATH)).toBe(0);
    dispose();
  });
});

/* ------------------------------------------------------------------------------------------------
 * ADR-045: every statement is planned, and a destructive one is confirmed
 * ---------------------------------------------------------------------------------------------- */

describe("the plan phase", () => {
  it("plans a harmless statement and applies it without asking", async () => {
    /*
     * Every statement is planned, not only the ones the browser suspects — which statements are
     * destructive is the decision `services/ksql` exists to make, and a browser that only planned
     * the ones it recognised would be a second, worse classifier.
     */
    const { container, stub, dispose } = open(objectsResponse, {
      grants: MAY_EXECUTE,
      plan: planHarmless,
      result: resultStatus,
    });
    await settle();

    type(container, "CREATE STREAM ENRICHED_ORDERS AS SELECT * FROM ORDERS EMIT CHANGES;");
    await settle();
    button(container, "Run query")?.click();
    await settle();

    expect(calls(stub, KSQL_PLAN_PATH)).toBe(1);
    expect(calls(stub, KSQL_STATEMENTS_PATH)).toBe(1);
    expect(document.querySelector('[data-testid="ksql-confirm"]')).toBeNull();
    dispose();
  });

  it("asks before a DROP ... DELETE TOPIC, in the server's own words", async () => {
    const { container, stub, dispose } = open(objectsResponse, {
      grants: MAY_EXECUTE,
      plan: planDrop,
      result: resultStatus,
    });
    await settle();

    type(container, "DROP STREAM ORDERS DELETE TOPIC;");
    await settle();
    button(container, "Run query")?.click();
    await settle();

    // Planned, and not applied: the confirmation is what stands between the two.
    expect(calls(stub, KSQL_PLAN_PATH)).toBe(1);
    expect(calls(stub, KSQL_STATEMENTS_PATH)).toBe(0);

    const dialog = document.querySelector('[data-testid="ksql-confirm"]');
    expect(dialog, "a destructive statement was not confirmed").not.toBeNull();
    expect(dialog?.textContent).toContain("deletes the Kafka topic");
    // The service's sentence, not ours.
    expect(dialog?.textContent).toContain(
      "This deletes the Kafka topic 'orders' and every record in it.",
    );
    dispose();
  });

  it("draws the topic-deletion danger from deletesTopic and not from destructive", async () => {
    /*
     * W9-A1: the `<Show when={plan().deletesTopic}>` banner was held by nothing. Replacing it with
     * `false` — so the dialogue draws no danger banner at all — left all 75 cases in this package
     * green, because the case above reads the dialogue's whole `textContent` and the service's own
     * warning sentence also contains the words "deletes the Kafka topic".
     *
     * The two flags are separate on the wire on purpose (`StatementPlanDto`, ADR-055 §7): every
     * `DROP` is destructive and only `DROP … DELETE TOPIC` destroys records, and they coincide
     * today only because ksqlDB has one statement that does the second. So both directions are
     * asserted against the flag rather than against the words.
     */
    const deletes = open(objectsResponse, {
      grants: MAY_EXECUTE,
      plan: planDrop,
      result: resultStatus,
    });
    await settle();
    type(deletes.container, "DROP STREAM ORDERS DELETE TOPIC;");
    await settle();
    button(deletes.container, "Run query")?.click();
    await settle();

    const danger = document.querySelector('[data-testid="ksql-confirm-deletes-topic"]');
    expect(danger, "a statement that deletes a topic drew no danger banner").not.toBeNull();
    expect(danger?.textContent).toContain("every record in it");
    deletes.dispose();
    await settle();
    expect(document.querySelectorAll('[data-testid="ksql-confirm"]')).toHaveLength(0);

    // A confirmation that is needed and a topic deletion are not the same fact.
    const drops = open(objectsResponse, {
      grants: MAY_EXECUTE,
      plan: { ...planDrop, deletesTopic: false },
      result: resultStatus,
    });
    await settle();
    type(drops.container, "DROP STREAM ORDERS;");
    await settle();
    button(drops.container, "Run query")?.click();
    await settle();


    expect(document.querySelector('[data-testid="ksql-confirm"]')).not.toBeNull();
    expect(
      document.querySelector('[data-testid="ksql-confirm-deletes-topic"]'),
      "a drop that destroys no records was given the records-destroyed banner",
    ).toBeNull();
    drops.dispose();
  });

  it("applies the plan's own statement and token when the reader confirms", async () => {
    const { container, stub, dispose } = open(objectsResponse, {
      grants: MAY_EXECUTE,
      plan: planDrop,
      result: resultStatus,
    });
    await settle();

    type(container, "DROP STREAM ORDERS DELETE TOPIC;");
    await settle();
    button(container, "Run query")?.click();
    await settle();
    button(container, "Run it")?.click();
    await settle();

    const applied = stub.calls.find((call) => call.path === KSQL_STATEMENTS_PATH);
    /*
     * The *plan's* canonicalised text, not the editor's. `KsqlPlanToken.verify` refuses a token
     * whose statement is not character-for-character the one being applied, which is how ADR-045's
     * guarantee survives an endpoint that carries the statement as well as the token — and sending
     * the editor's current text would break exactly that guarantee the moment somebody typed while
     * the dialogue was open.
     */
    expect(applied?.body).toEqual({
      statement: "DROP STREAM ORDERS DELETE TOPIC;",
      token: "eyJ2IjoxfQ.c2ln",
    });
    dispose();
  });

  it("applies nothing when the reader cancels", async () => {
    const { container, stub, dispose } = open(objectsResponse, {
      grants: MAY_EXECUTE,
      plan: planDrop,
      result: resultStatus,
    });
    await settle();

    type(container, "DROP STREAM ORDERS DELETE TOPIC;");
    await settle();
    button(container, "Run query")?.click();
    await settle();
    button(container, "Cancel")?.click();
    await settle();

    expect(calls(stub, KSQL_STATEMENTS_PATH)).toBe(0);
    expect(document.querySelector('[data-testid="ksql-confirm"]')).toBeNull();
    dispose();
  });

  it("keeps the dialogue open when the veil is clicked, and applies nothing", async () => {
    /*
     * W9-A1, and the seam is one word: `ConfirmStatement` passes `closeOnScrimClick={false}`, and
     * flipping it to `true` left the whole workspace green — 1,904 cases when W9-A1 measured it,
     * and this case is what that flip now reddens. The veil is not an answer to the question this
     * dialogue is asking, and the reader is looking at a statement in order to decide, so a stray
     * click beside the box must leave both the dialogue and the statement exactly where they were.
     *
     * The click is dispatched rather than `userEvent.click`ed: `userEvent` decides where a click
     * lands from layout and jsdom has none, so it never reaches an element that covers the window.
     * The property itself is held in `kernel/src/components/dialog.test.tsx`, which drives the veil
     * at the level the prop is applied; this case is the caller asking for it.
     */
    const { container, stub, dispose } = open(objectsResponse, {
      grants: MAY_EXECUTE,
      plan: planDrop,
      result: resultStatus,
    });
    await settle();

    type(container, "DROP STREAM ORDERS DELETE TOPIC;");
    await settle();
    button(container, "Run query")?.click();
    await settle();

    const veil = document.querySelector(".kui-modal-scrim");
    expect(veil, "the confirmation drew no veil").not.toBeNull();
    veil?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    await settle();

    expect(
      document.querySelector('[data-testid="ksql-confirm"]'),
      "a stray click on the veil dismissed the dialogue between a typed statement and a " +
        "deleted Kafka topic",
    ).not.toBeNull();
    expect(calls(stub, KSQL_STATEMENTS_PATH)).toBe(0);
    // And the statement is still the one the service canonicalised, not a re-planned one.
    expect(document.querySelector('[data-testid="ksql-confirm-statement"]')?.textContent).toBe(
      "DROP STREAM ORDERS DELETE TOPIC;",
    );
    dispose();
  });

  it("refuses a second press while the confirmed statement is still in flight", async () => {
    /*
     * W9-A1's row, and the cost is not the one the row predicted. Measured here rather than
     * assumed: `busy={apply.busy()}` on `ConfirmStatement` replaced with `busy={false}` does
     * **not** send the statement twice — `createMutation.run` holds a plain `running` flag for
     * exactly this, set in the same synchronous turn, so the second request never leaves. The
     * network is defended whichever way this prop goes.
     *
     * What the prop defends is what the reader sees, and losing it is worse than a silent no-op:
     * with `busy={false}` the button looks idle and pressable while a `DROP … DELETE TOPIC` is in
     * flight, the press reaches `applyNow`, and `run` answers `{kind:"running"}` — which is not
     * `done`, so this screen closes the confirmation and paints *"The ksqlDB server did not accept
     * that, and did not say why"* over a statement that is running perfectly well. An operator is
     * then looking at a failure panel for a destructive statement that is about to succeed.
     *
     * So the assertions run in that order: the busy state first (the mutation kills it), then the
     * two consequences, then the send count — which is stated as a **regression guard and not a
     * closed mutation**, because `createMutation` holds it today and this case would stay green if
     * this prop were the only thing removed.
     *
     * The apply is held in flight rather than simulated: the stub's write hook does not answer the
     * statements address until this case releases it, so the second press lands in the same state a
     * reader's second press lands in — the first request sent, no answer yet.
     */
    let release: (() => void) | undefined;
    const inFlight = new Promise<void>((resolve) => {
      release = resolve;
    });

    const { container, stub, dispose } = open(objectsResponse, {
      grants: MAY_EXECUTE,
      plan: planDrop,
      result: resultStatus,
      write: async (path) => {
        // Only the apply is held. The plan is the other write on this screen and it must answer
        // normally, or the dialogue this case is about never opens.
        if (path === KSQL_STATEMENTS_PATH) await inFlight;
        return undefined;
      },
    });
    await settle();

    type(container, "DROP STREAM ORDERS DELETE TOPIC;");
    await settle();
    button(container, "Run query")?.click();
    await settle();

    button(container, "Run it")?.click();
    await settle();
    expect(calls(stub, KSQL_STATEMENTS_PATH), "the confirmed statement was not sent").toBe(1);

    const confirm = button(container, "Run it");
    expect(
      confirm?.getAttribute("aria-busy"),
      "the confirm button said nothing while a destructive statement was in flight",
    ).toBe("true");

    confirm?.click();
    await settle();
    expect(
      document.querySelector('[data-testid="ksql-confirm"]'),
      "a second press closed the confirmation while the statement it confirmed was still running",
    ).not.toBeNull();
    expect(
      document.querySelector('[data-testid="ksql-result"]')?.getAttribute("data-kind"),
      "a second press reported the running statement as one the server would not accept",
    ).not.toBe("failed");
    expect(
      calls(stub, KSQL_STATEMENTS_PATH),
      "a second press sent the destructive statement again while the first was still running",
    ).toBe(1);

    release?.();
    await settle();
    // Released, the one request finishes and the dialogue goes away: the refusal above is a
    // refusal to send it *twice*, not a refusal to send it at all.
    expect(calls(stub, KSQL_STATEMENTS_PATH)).toBe(1);
    expect(document.querySelector('[data-testid="ksql-confirm"]')).toBeNull();
    dispose();
  });

  it("never posts a push query to the address that refuses one", async () => {
    /*
     * A `SELECT ... EMIT CHANGES` does not finish, so `KsqlEndpoints.execute` refuses it and names
     * the stream address. The *plan* says which shape it is, and the browser routes on that rather
     * than on its own reading of the SQL: two definitions of "push query" is two chances to
     * disagree, and the browser's copy would be a SQL parser in a screen.
     */
    const { container, stub, dispose } = open(objectsResponse, {
      grants: MAY_EXECUTE,
      plan: pushQueryPlan,
      result: resultStatus,
      openStream: fakeStream().opener,
    });
    await settle();

    type(container, "SELECT * FROM ORDERS EMIT CHANGES;");
    await settle();
    button(container, "Run query")?.click();
    await settle();

    expect(calls(stub, KSQL_PLAN_PATH)).toBe(1);
    expect(calls(stub, KSQL_STATEMENTS_PATH)).toBe(0);
    // The region is live and the control has become Cancel, because a push query has no other way
    // out: closing the tab would leave it running on the server.
    expect(container.querySelector('[data-testid="ksql-result"]')?.getAttribute("data-kind")).toBe(
      "streaming",
    );
    expect(button(container, "Cancel query")).toBeDefined();
    dispose();
  });
});

/* ------------------------------------------------------------------------------------------------
 * The screen
 * ---------------------------------------------------------------------------------------------- */

describe("the ksqlDB screen", () => {
  it("does not open a stream from a plan belonging to the previous cluster", async () => {
    const [clusterId, setClusterId] = createSignal(TEST_CLUSTER);
    let release!: () => void;
    const pending = new Promise<void>((resolve) => { release = resolve; });
    const stream = fakeStream();
    const ui = open(objectsResponse, { plan: pushQueryPlan, clusterId, openStream: stream.opener,
      write: async (path) => { if (path === KSQL_PLAN_PATH) await pending; } });
    await settle(); type(ui.container, "SELECT * FROM ORDERS EMIT CHANGES;"); await settle();
    button(ui.container, "Run query")?.click(); await settle();
    setClusterId("other"); await settle();
    release(); await settle();
    expect(stream.statements).toHaveLength(0);
    ui.dispose();
  });
  it("does not execute a plan after disposal", async () => {
    let release!: () => void;
    const pending = new Promise<void>((resolve) => { release = resolve; });
    const ui = open(objectsResponse, {
      grants: MAY_EXECUTE, plan: planHarmless, result: resultStatus,
      write: async (path) => { if (path === KSQL_PLAN_PATH) await pending; },
    });
    await settle();
    type(ui.container, "SHOW STREAMS;"); await settle();
    button(ui.container, "Run query")?.click(); await settle();
    ui.dispose(); await settle();
    release(); await settle();
    expect(calls(ui.stub, KSQL_STATEMENTS_PATH)).toBe(0);
  });
  it("draws the streams and tables in the pane and the queries in their own table", async () => {
    const { container, dispose } = open(objectsResponse);
    await settle();

    const names = [...container.querySelectorAll(".kui-ksql__object-name")].map(
      (one) => one.textContent,
    );
    // Streams and tables only, in the server's order. Queries have their own table below and the
    // topic row is `Topics`' business, not this screen's.
    expect(names).toEqual(["ORDERS", "PAYMENTS", "USERS"]);

    const queries = container.querySelector('[data-testid="ksql-queries"]');
    expect(queries?.textContent).toContain("CSAS_ENRICHED_ORDERS_5");
    expect(queries?.textContent).toContain("ENRICHED_ORDERS");
    dispose();
  });

  it("names the rows the server returned and this build could not describe", async () => {
    /* A row missing from a list is indistinguishable from a row that is not there, which is why the
       service puts them on the wire at all. A browser that dropped them would undo that. */
    const { container, dispose } = open(objectsPartial);
    await settle();
    expect(container.querySelector('[data-testid="ksql-unreadable"]')?.textContent).toContain(
      "(a table the ksqlDB cluster did not name)",
    );
    // And what the service cut, which is the other half of what this listing says about itself.
    expect(container.textContent).toContain("1811 objects were left out");
    dispose();
  });

  it("says the auto.offset.reset control is inert instead of letting it look live", async () => {
    /*
     * `KsqlWorkspace` draws the control because §3.16 draws it, and the wire has nowhere to carry
     * it: `StatementRequestDto` is `{statement, token}` and the stream takes one `statement` query
     * parameter. A control that looks live and has never worked is what `MESSAGES_PURGE` already
     * cost this project once.
     */
    const { container, dispose } = open(objectsResponse);
    await settle();
    const select = container.querySelector<HTMLSelectElement>(".kui-ksql__offset select");
    expect(select?.disabled).toBe(true);
    expect(container.querySelector('[data-testid="ksql-offset-note"]')?.textContent).toBe(
      OFFSET_RESET_NOT_SETTABLE,
    );
    dispose();
  });

  it("says a ksqlDB that answered and is running nothing, without drawing a zero", async () => {
    const { container, dispose } = open(objectsEmpty);
    await settle();
    expect(container.textContent).toContain(NO_OBJECTS);
    expect(container.textContent).not.toContain("0 streams");
    dispose();
  });

  it("tells a deployment with no ksqlDB what to configure, offering no retry", async () => {
    const { container, dispose } = open(objectsNotConfigured);
    await settle();
    expect(container.querySelector('[data-testid="ksql-not-configured"]')).not.toBeNull();
    expect(container.textContent).toContain(NOT_CONFIGURED);
    // Nothing is broken, so there is nothing to try again — and no editor over a server that does
    // not exist.
    expect(button(container, "Retry")).toBeUndefined();
    expect(container.querySelector("#kui-ksql-editor")).toBeNull();
    dispose();
  });

  it("tells a principal without KSQL:VIEW it is a permission, not an empty server", async () => {
    const { container, dispose } = open(objectsForbidden);
    await settle();
    expect(container.querySelector('[data-testid="ksql-forbidden"]')).not.toBeNull();
    expect(container.textContent).toContain("Ask for KSQL:VIEW");
    // No retry: a permission decision does not change because somebody pressed a button.
    expect(button(container, "Retry")).toBeUndefined();
    dispose();
  });

  it("reports a ksqlDB that did not answer in the server's own words, with its code", async () => {
    const { container, dispose } = open(objectsUnavailable);
    await settle();
    const failed = container.querySelector('[data-testid="ksql-failed"]');
    expect(failed?.textContent).toContain("ksqldb could not be reached");
    expect(failed?.textContent).toContain("UPSTREAM_UNAVAILABLE");
    dispose();
  });

  it("refuses a document this build cannot read rather than drawing an empty server", async () => {
    /* The wave-5 defect, asked of the screen rather than of the decoder: a payload whose object
       list is spelled something else must not become "this ksqlDB has no streams". */
    const { container, dispose } = open({ objects: { status: "ok", data: { streams: [] } } });
    await settle();
    expect(container.querySelector('[data-testid="ksql-failed"]')).not.toBeNull();
    expect(container.textContent).not.toContain(NO_OBJECTS);
    dispose();
  });
});

/* ------------------------------------------------------------------------------------------------
 * The result region
 * ---------------------------------------------------------------------------------------------- */

describe("the result region", () => {
  it("draws a pull query's rows as a table, and an absent cell in words", async () => {
    const { container, dispose } = open(objectsResponse, {
      grants: MAY_EXECUTE,
      plan: planHarmless,
      result: resultRows,
    });
    await settle();

    type(container, "SELECT ID, TOTAL, NOTE FROM ORDERS WHERE ID = '17';");
    await settle();
    button(container, "Run query")?.click();
    await settle();

    const region = container.querySelector('[data-testid="ksql-result"]');
    expect(region?.getAttribute("data-kind")).toBe("rows");
    expect(region?.textContent).toContain("2 rows.");
    expect(region?.textContent).toContain("gift wrap");
    // The second row's `NOTE` is a SQL null — a value the query produced — and is drawn as the word
    // rather than as a blank cell that would read as an empty string.
    expect(region?.textContent).toContain(CELL_NULL);
    dispose();
  });

  it("draws a DDL answer as the server's own sentence and what it acted on", async () => {
    const { container, dispose } = open(objectsResponse, {
      grants: MAY_EXECUTE,
      plan: planHarmless,
      result: resultStatus,
    });
    await settle();

    type(container, "CREATE STREAM ENRICHED_ORDERS AS SELECT * FROM ORDERS EMIT CHANGES;");
    await settle();
    button(container, "Run query")?.click();
    await settle();

    const region = container.querySelector('[data-testid="ksql-result"]');
    expect(region?.getAttribute("data-kind")).toBe("status");
    expect(region?.textContent).toContain("Stream created and running");
    expect(region?.textContent).toContain("stream/ENRICHED_ORDERS/create");
    dispose();
  });

  it("says a query matched nothing rather than drawing an empty table", async () => {
    const empty = {
      statement: "SELECT * FROM ORDERS WHERE 1 = 2;",
      shape: "pull_query",
      outcome: "rows",
      columns: ["ID"],
      rows: [],
      message: null,
      entity: null,
      executedAt: "2026-09-03T10:11:12Z",
    };
    const { container, dispose } = open(objectsResponse, {
      grants: MAY_EXECUTE,
      plan: planHarmless,
      result: empty,
    });
    await settle();

    type(container, "SELECT * FROM ORDERS WHERE 1 = 2;");
    await settle();
    button(container, "Run query")?.click();
    await settle();

    expect(container.textContent).toContain(NO_ROWS);
    dispose();
  });

  it("reports a statement this build could not read without clearing the editor", async () => {
    const { container, dispose } = open(objectsResponse, {
      grants: MAY_EXECUTE,
      plan: planHarmless,
      result: { outcome: "table" },
    });
    await settle();

    type(container, "SELECT * FROM NOTHING;");
    await settle();
    button(container, "Run query")?.click();
    await settle();

    expect(container.querySelector('[data-testid="ksql-result-failed"]')).not.toBeNull();
    /* The editor keeps the text. A form that empties itself on a refusal makes the operator retype
       the statement in order to read the reason it was refused. */
    expect(container.querySelector<HTMLTextAreaElement>("#kui-ksql-editor")?.value).toBe(
      "SELECT * FROM NOTHING;",
    );
    dispose();
  });
});


/* ------------------------------------------------------------------------------------------------
 * ADR-056: what the result region does while a push query is arriving, and when it stops
 * ---------------------------------------------------------------------------------------------- */

/**
 * A push query this case drives itself.
 *
 * The seam exists because `streaming`, `ended` and `interrupted` are otherwise reachable only from
 * a real network, and they are ADR-056's three hardest decisions. The handle is the kernel's shape,
 * so the screen cannot tell this from the real one — including `close()`, which is what these cases
 * watch to prove the query is stopped rather than merely hidden.
 */
interface FakeStream {
  readonly opener: PushQueryOpener;
  readonly statements: string[];
  closes: number;
  emitColumns: (columns: readonly string[]) => void;
  emitRow: (values: readonly string[]) => void;
  fail: (cause: string) => void;
  /** A frame this build could not read, which the kernel reports and which must not be terminal. */
  garble: (cause: string) => void;
}

function fakeStream(): FakeStream {
  const stream: FakeStream = {
    statements: [],
    closes: 0,
    emitColumns: () => {},
    emitRow: () => {},
    fail: () => {},
    garble: () => {},
    opener: (_cluster: string, statement: string, subscriber: PushQuerySubscriber) => {
      stream.statements.push(statement);
      stream.emitColumns = (columns) => subscriber.onColumns(columns);
      stream.emitRow = (values) => subscriber.onRow(values);
      stream.fail = (cause) => subscriber.onError({ kind: "transport", cause });
      stream.garble = (cause) =>
        subscriber.onError({ kind: "decode", event: KSQL_ROW_EVENT_NAME, cause });
      return {
        connection: () => ({ phase: "open" as const }),
        close: () => {
          stream.closes += 1;
        },
        endMarker: () => undefined,
      };
    },
  };
  return stream;
}

describe("a push query's result region", () => {
  async function running(): Promise<Open & { readonly stream: FakeStream }> {
    const stream = fakeStream();
    const opened = open(objectsResponse, {
      grants: MAY_EXECUTE,
      plan: pushQueryPlan,
      openStream: stream.opener,
    });
    await settle();
    type(opened.container, "SELECT * FROM ORDERS EMIT CHANGES;");
    await settle();
    button(opened.container, "Run query")?.click();
    await settle();
    return { ...opened, stream };
  }

  it("opens the stream on the plan's own statement and says nothing has arrived yet", async () => {
    const { container, stream, dispose } = await running();
    // The plan's canonicalised text, for the same reason the apply sends it: the editor's is what
    // the reader can still be typing into.
    expect(stream.statements).toEqual(["SELECT * FROM ORDERS EMIT CHANGES;"]);
    expect(container.querySelector('[data-testid="ksql-result-count"]')?.textContent).toContain(
      "No row has arrived yet",
    );
    dispose();
  });

  it("draws the rows as they arrive, under the columns the phase frame declared", async () => {
    const { container, stream, dispose } = await running();
    stream.emitColumns(["REGION", "ORDERS"]);
    stream.emitRow(["eu-west", "41"]);
    stream.emitRow(["us-east", "7"]);
    await settle();

    const region = container.querySelector('[data-testid="ksql-result"]');
    expect(region?.getAttribute("data-kind")).toBe("streaming");
    expect(region?.textContent).toContain("2 rows so far, and the query is still running.");
    expect(region?.textContent).toContain("REGION");
    expect(region?.textContent).toContain("eu-west");
    dispose();
  });

  it("stops the query on Cancel and keeps what it produced", async () => {
    const { container, stream, dispose } = await running();
    stream.emitColumns(["REGION"]);
    stream.emitRow(["eu-west"]);
    await settle();
    button(container, "Cancel query")?.click();
    await settle();

    /* `close()` is the whole point: the kernel aborts the request, which cancels the gateway's
       relay, which stops the query on the ksqlDB server. A screen that only hid the rows would
       leave it running. */
    expect(stream.closes).toBe(1);
    const region = container.querySelector('[data-testid="ksql-result"]');
    expect(region?.getAttribute("data-kind")).toBe("ended");
    expect(region?.textContent).toContain("1 row before the query ended.");
    expect(region?.textContent).toContain("eu-west");
    dispose();
  });

  it("keeps the rows on screen when the stream dies, with the reason over them", async () => {
    /*
     * ADR-056's third decision. The rows arrived and were true, and they are the only record the
     * operator has of what the query was producing when the connection died — an error panel that
     * replaced them would delete evidence in order to show an error message.
     */
    const { container, stream, dispose } = await running();
    stream.emitColumns(["REGION"]);
    stream.emitRow(["eu-west"]);
    await settle();
    stream.fail("the stream ended unexpectedly");
    await settle();

    const region = container.querySelector('[data-testid="ksql-result"]');
    expect(region?.getAttribute("data-kind")).toBe("interrupted");
    expect(region?.textContent).toContain("the stream ended unexpectedly");
    expect(region?.textContent).toContain("eu-west");
    expect(region?.textContent).toContain("1 row before the stream ended.");
    dispose();
  });

  it("keeps delivering rows after a frame this build could not read", async () => {
    /*
     * W9-A1: `if (error.kind === "decode") return;` in `KsqlRoute`'s `onError` was held by nothing.
     * Deleting it — so that one unreadable frame ends the query — left all 75 cases in this package
     * and all 1,904 in the frontend green.
     *
     * It is the kernel's rule and `feature-messages` follows it too: a decode failure is
     * informational, because one malformed frame among thousands must not tear down a push query
     * that is otherwise delivering good rows. The two terminal kinds are asserted beside it, so the
     * case fails just as loudly if the guard is widened to swallow those instead.
     */
    const { container, stream, dispose } = await running();
    stream.emitColumns(["REGION"]);
    stream.emitRow(["eu-west"]);
    stream.garble("the frame carried no values this build could read");
    await settle();

    const region = container.querySelector('[data-testid="ksql-result"]');
    expect(region?.getAttribute("data-kind"), "an unreadable frame ended the query").toBe(
      "streaming",
    );

    // And the query really is still live: the next row still arrives and still draws.
    stream.emitRow(["us-east"]);
    await settle();
    expect(container.querySelector('[data-testid="ksql-result"]')?.textContent).toContain(
      "us-east",
    );

    // The other direction, in the same case: a transport failure *is* terminal.
    stream.fail("the stream ended unexpectedly");
    await settle();
    expect(container.querySelector('[data-testid="ksql-result"]')?.getAttribute("data-kind")).toBe(
      "interrupted",
    );
    dispose();
  });

  it("closes the push query already open before it runs the next statement", async () => {
    /*
     * W9-A1: the `stopStream()` at the top of `onRun` was held by nothing — deleting it left the
     * whole frontend suite green. A push query never ends by itself, so a reader who runs a second
     * statement without it leaves the first one executing on the operator's ksqlDB with nothing
     * on screen referring to it and no handle left in the browser to close it. `onCleanup` cannot
     * help: the component is still mounted, and the only reference to the old handle is gone.
     */
    const { container, stream, dispose } = await running();
    stream.emitRow(["eu-west"]);
    /*
     * The stream the reader is left holding: `onError` draws `interrupted` and deliberately keeps
     * the rows, but it does not close the handle — the connection is what failed, not the query,
     * and the query is still running on the ksqlDB server. `onCancel` closes; this path does not,
     * which is why the close has to happen on the way in to the next run.
     */
    stream.fail("the stream ended unexpectedly");
    await settle();
    expect(stream.closes, "the interrupted path closed the handle by itself").toBe(0);

    type(container, "SELECT * FROM PAYMENTS EMIT CHANGES;");
    await settle();
    button(container, "Run query")?.click();
    await settle();

    expect(stream.statements.length, "the second statement did not open a query").toBe(2);
    expect(stream.closes, "the first push query was left running on the server").toBe(1);
    dispose();
  });

  it("closes the query when the screen goes away", async () => {
    // A push query never ends by itself, so navigating away would otherwise leave one running on
    // somebody's cluster until the server's own stream budget expired.
    const { stream, dispose } = await running();
    dispose();
    expect(stream.closes).toBe(1);
  });
});

describe("accessibility", () => {
  it("has no axe violations with a result on screen", async () => {
    const { container, dispose } = open(objectsResponse, {
      grants: MAY_EXECUTE,
      plan: planHarmless,
      result: resultRows,
    });
    await settle();
    type(container, "SELECT ID, TOTAL, NOTE FROM ORDERS WHERE ID = '17';");
    await settle();
    button(container, "Run query")?.click();
    await settle();

    const violations = await findViolations(container);
    expect(violations, describeViolations(violations)).toEqual([]);
    dispose();
  });

  it("has no axe violations on a deployment with no ksqlDB", async () => {
    const { container, dispose } = open(objectsNotConfigured);
    await settle();
    const violations = await findViolations(container);
    expect(violations, describeViolations(violations)).toEqual([]);
    dispose();
  });
});
