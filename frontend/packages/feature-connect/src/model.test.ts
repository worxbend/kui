/**
 * The screen's arithmetic and its sentences, away from anything that renders.
 *
 * Every one of these is a decision the screen makes that could be wrong in a way an operator would
 * act on: which name a permission question carries, whose words a failure reason is in, and which
 * of several identical-looking empty states a cluster is in.
 */
import { describe, expect, it } from "vitest";

import {
  cardActions,
  connectVoice,
  connectorLabel,
  failureReason,
  operateAction,
  operateSubject,
  pillState,
  segmentsOf,
  taskSegment,
  toggleOf,
  workersThatDidNotAnswer,
} from "./model.js";
import {
  decodeConnectorListing,
  Unreadable,
  type Connector,
  type ConnectorListing,
} from "./wire.js";
import responseDocument from "./documents/connectors-response.json" with { type: "json" };
import partialDocument from "./documents/connectors-partial.json" with { type: "json" };
import emptyDocument from "./documents/connectors-empty.json" with { type: "json" };

function listing(document: unknown): ConnectorListing {
  const decoded = decodeConnectorListing(
    (document as { connectors: { data: unknown } }).connectors.data,
  );
  if (decoded === Unreadable) throw new Error("this document should decode");
  return decoded;
}

/** One worker that answered, so a voice-line case can name states without a fixture file. */
function voiceListing(
  items: readonly Connector[],
  unreadable: readonly string[] = [],
): ConnectorListing {
  return {
    workers: [
      {
        connect: "payments",
        page: { kind: "ok", page: { items, unreadable } },
      },
    ],
  };
}

function connector(overrides: Partial<Connector> = {}): Connector {
  return {
    connect: "payments",
    name: "orders-source",
    kind: "source",
    state: "RUNNING",
    workerId: "10.0.0.1:8083",
    reason: undefined,
    failed: false,
    runningTasks: 1,
    taskCount: 1,
    tasks: [
      { id: 0, state: "RUNNING", workerId: "10.0.0.1:8083", reason: undefined, trace: undefined },
    ],
    ...overrides,
  };
}

describe("the subject a permission question carries", () => {
  it("is the Connect cluster, because that is what the server's requirement names", () => {
    /*
     * `ConnectEndpoints.operating` declares
     * `ResourceRequirement.named(Resource.Connect, connectName, Action.ConnectOperate)`, so the
     * grant that decides a pause is `CONNECT:OPERATE` on the Connect cluster in the path. A browser
     * asking a different question gives a control that is enabled and then refused, or one that is
     * disabled for somebody who holds the permission.
     */
    expect(operateSubject({ connect: "payments" })).toBe("payments");
  });

  it("names the Connect cluster in the refusal, because that is the grant to ask for", () => {
    // Telling somebody they need a permission on `payments/orders-sink` when the grant they must
    // ask for is on `payments` sends them to ask for something that does not exist.
    const sentence = operateAction({ connect: "payments" });
    expect(sentence).toContain("'payments'");
    expect(sentence).not.toContain("/");
  });

  it("still labels a connector by both names, because a page can hold two workers'", () => {
    expect(connectorLabel({ connect: "analytics", name: "es-sink" })).toBe("analytics/es-sink");
  });
});

describe("the control a connector offers", () => {
  it("offers Resume to a paused connector and Pause to every other state", () => {
    expect(toggleOf("PAUSED")).toBe("resume");
    expect(toggleOf("RUNNING")).toBe("pause");
    expect(toggleOf("FAILED")).toBe("pause");
    expect(toggleOf("UNKNOWN")).toBe("pause");
  });
});

describe("what the card is handed", () => {
  it("draws a restarting connector as unknown and never as running", () => {
    // Not a connector doing work. A green pill mid-restart tells an operator everything is fine at
    // the one moment they are watching to see whether it is.
    expect(pillState("RESTARTING")).toBe("UNKNOWN");
    expect(pillState("UNKNOWN")).toBe("UNKNOWN");
    expect(pillState("RUNNING")).toBe("RUNNING");
    expect(pillState("PAUSED")).toBe("PAUSED");
  });

  it("draws a restarting task's segment as unknown and never as running", () => {
    // Same rule one level down. `TaskBar` draws `unknown` as an outline, which claims nothing.
    expect(taskSegment("RESTARTING")).toBe("unknown");
    expect(taskSegment("UNASSIGNED")).toBe("unknown");
    expect(taskSegment("RUNNING")).toBe("running");
    expect(taskSegment("FAILED")).toBe("failed");
  });

  it("draws one segment per task the worker described, and not per task it counted", () => {
    /* A bar padded out to `taskCount` would draw segments for tasks whose state nobody knows as
       though the absence were a state. Where the two disagree the panel says so in words. */
    const held = connector({
      taskCount: 4,
      runningTasks: 1,
      tasks: [
        { id: 0, state: "RUNNING", workerId: undefined, reason: undefined, trace: undefined },
      ],
    });
    expect(segmentsOf(held)).toEqual(["running"]);
  });

});

describe("what a connector's card is handed for its two controls", () => {
  /*
   * The rule is *never both*, and it is asserted on the value because it cannot be asserted on the
   * page. `ConnectorCard` branches on `actionsDisabledReason` and ignores the handlers whenever it
   * is present, so a card handed a live `onPause` **and** a refusal renders byte-identically to one
   * handed only the refusal. W7's verification filed exactly that: the wiring was made
   * unconditional and all 58 cases stayed green, including the one named for it, because every one
   * of them reads `aria-disabled`.
   */
  it("hands a refused connector the sentence and no handler of any kind", () => {
    const actions = cardActions({
      refusal: "You do not have permission to pause connectors on 'analytics'.",
      toggle: "pause",
      onCommand: () => {
        throw new Error("a refused card must not be able to reach a command at all");
      },
    });

    expect(actions).toEqual({
      actionsDisabledReason: "You do not have permission to pause connectors on 'analytics'.",
    });
    // Spelled out as well, because `toEqual` on an object with extra function properties is the
    // assertion that would quietly stop being about this rule if the shape grew a field.
    expect("onPause" in actions).toBe(false);
    expect("onRestart" in actions).toBe(false);
  });

  it("hands a permitted connector both handlers and no disabled reason", () => {
    const asked: string[] = [];
    const actions = cardActions({
      refusal: undefined,
      toggle: "pause",
      onCommand: (which) => asked.push(which),
    });

    expect("actionsDisabledReason" in actions).toBe(false);
    if (!("onPause" in actions)) throw new Error("a permitted card must carry its handlers");
    actions.onPause();
    actions.onRestart();
    expect(asked).toEqual(["pause", "restart"]);
  });

  it("sends the toggle the connector's own state offers, so a paused one resumes", () => {
    const asked: string[] = [];
    const actions = cardActions({
      refusal: undefined,
      toggle: toggleOf("PAUSED"),
      onCommand: (which) => asked.push(which),
    });
    if (!("onPause" in actions)) throw new Error("a permitted card must carry its handlers");

    actions.onPause();
    // Never a literal "pause": the control on a paused connector reads Resume and must send resume.
    expect(asked).toEqual(["resume"]);
  });
});

describe("the failure reason (SCREENS-V4 §7.7)", () => {
  it("is the worker's own line, as the service sliced it, with the task that reported it", () => {
    const elastic = listing(responseDocument).workers[0];
    const held =
      elastic?.page.kind === "ok"
        ? elastic.page.page.items.find((one) => one.name === "elastic-sink")
        : undefined;
    expect(held).toBeDefined();
    if (held === undefined) return;

    const reason = failureReason(held);
    expect(reason?.line).toBe(
      "org.apache.kafka.connect.errors.ConnectException: connection refused to es-01:9200",
    );
    expect(reason?.task).toBe(1);
    expect(reason?.hasTrace).toBe(true);
  });

  it("has a reason for a connector whose own state is RUNNING but whose task failed", () => {
    // The connector the whole `failed` flag exists for. Keying the reason off the state word hides
    // it on exactly the connector somebody has to go and fix.
    const elastic = listing(responseDocument).workers[0];
    const held =
      elastic?.page.kind === "ok"
        ? elastic.page.page.items.find((one) => one.name === "elastic-sink")
        : undefined;
    expect(held?.state).toBe("RUNNING");
    expect(failureReason(held ?? connector())).toBeDefined();
  });

  it("has none for a connector that is not failing, even when a reason survived on it", () => {
    /* Filed by W8-04's verification pass as F3. The case that stood here asserted only
       `failureReason(connector())`, whose fixture carries `reason: undefined` — so it came back
       undefined for the wrong reason, and deleting `if (!connector.failed) return undefined;`
       left all 75 cases green. A guard whose every fixture also satisfies the line below it is
       not measured by that fixture.

       The second connector here is the document that separates them, and the service does send
       it: `reason` is the worker's own line and it survives a recovery, so a connector that
       failed, was restarted and is now RUNNING carries a stale reason with `failed: false`.
       Today the only thing keeping that line off the card is `ConnectorPanel`'s
       `<Show when={props.connector.failed}>`, one level above — but `failureReason` is exported
       from `index.tsx` as public API promising *"or `undefined` when it is not failing"*, and a
       second caller, or that `Show` widening by one word, draws a resolved fault under a green
       pill. The promise is held here, in the function that makes it. */
    expect(failureReason(connector())).toBeUndefined();
    expect(
      failureReason(
        connector({
          failed: false,
          state: "RUNNING",
          reason: "org.apache.kafka.connect.errors.ConnectException: connection refused to es-01",
          tasks: [
            {
              id: 0,
              state: "RUNNING",
              workerId: "10.0.0.1:8083",
              reason:
                "org.apache.kafka.connect.errors.ConnectException: connection refused to es-01",
              trace: "  at org.apache.kafka.connect.runtime.WorkerSinkTask.poll\n",
            },
          ],
        }),
      ),
    ).toBeUndefined();
  });

  it("does not promise a trace for a task whose trace is whitespace", () => {
    /* Filed by W7-A3. `hasTrace` decides whether the panel prints "The rest of the trace is in
       that task's log on the Connect worker", which is a sentence that sends somebody to a log.
       A worker that answered with a blank `trace` — a newline, an indent from a template — has
       nothing there for them to find, and the trim is the only thing between the two. Dropping
       `.trim()` left every case green. */
    const blank = connector({
      failed: true,
      reason: "org.apache.kafka.connect.errors.ConnectException: connection refused",
      tasks: [
        {
          id: 0,
          state: "FAILED",
          workerId: "10.0.0.1:8083",
          reason: "org.apache.kafka.connect.errors.ConnectException: connection refused",
          trace: "\n   \n",
        },
      ],
    });
    expect(failureReason(blank)?.hasTrace).toBe(false);

    // And a trace with something in it is still a trace, so the guard declines blankness rather
    // than the feature.
    const real = connector({
      failed: true,
      reason: "boom",
      tasks: [
        { id: 0, state: "FAILED", workerId: "10.0.0.1:8083", reason: "boom", trace: "  at x\n" },
      ],
    });
    expect(failureReason(real)?.hasTrace).toBe(true);
  });

  it("has none for a failed connector whose worker recorded no reason", () => {
    /* Which is not the same as no problem: `ConnectorPanel` draws `NO_REASON_REPORTED` in that
       case, because an empty reason area reads as "KUI knows and will not say". */
    expect(failureReason(connector({ failed: true, reason: undefined }))).toBeUndefined();
  });
});

describe("which workers did not answer", () => {
  it("names the ones with no page, and not the ones with an empty page", () => {
    expect(workersThatDidNotAnswer(listing(responseDocument)).map((one) => one.connect)).toEqual([
      "analytics",
    ]);
    expect(workersThatDidNotAnswer(listing(emptyDocument))).toEqual([]);
  });
});

describe("the voice line", () => {
  it("counts the rows this browser is holding and says the count is partial when it is", () => {
    /* One of the two workers is rebalancing, so a flat "3 connectors" would be a smaller number in
       the reassuring direction over a cluster nobody has finished reading. */
    const line = connectVoice(listing(responseDocument));
    expect(line).toContain("3 connectors");
    expect(line).toContain("from the workers that answered");
    expect(line).toContain("1 failed and sulking");
  });

  it("counts a failure by the wire's flag and not by the state word", () => {
    // `elastic-sink` is RUNNING and failed. A line counting state words would say "all running".
    expect(connectVoice(listing(responseDocument))).not.toContain("all running");
  });

  it("mentions the connectors a worker would not describe", () => {
    expect(connectVoice(listing(partialDocument))).toContain("would not describe");
  });

  it("says nothing is deployed rather than drawing a zero", () => {
    const line = connectVoice(listing(emptyDocument));
    expect(line).toContain("No connectors are deployed");
    expect(line).not.toContain("0");
  });

  it("does not call a cluster healthy when a connector is not running", () => {
    const paused = connectVoice(
      voiceListing([
        connector({ name: "orders-source", state: "RUNNING" }),
        connector({ name: "archive-sink", state: "PAUSED", kind: "sink" }),
      ]),
    );
    expect(paused).not.toContain("all running");
    expect(paused).toContain("paused");

    for (const [state, word] of [
      ["UNASSIGNED", "unassigned"],
      ["RESTARTING", "restarting"],
      ["UNKNOWN", "unknown"],
    ] as const) {
      const line = connectVoice(
        voiceListing([
          connector({ name: "orders-source", state: "RUNNING" }),
          connector({ name: "other", state }),
        ]),
      );
      expect(line, state).not.toContain("all running");
      expect(line, state).toContain(word);
    }
  });

  it("says all running when every described connector is running", () => {
    const line = connectVoice(
      voiceListing([
        connector({ name: "orders-source", state: "RUNNING" }),
        connector({ name: "archive-sink", state: "RUNNING", kind: "sink" }),
      ]),
    );
    expect(line).toContain("all running");
  });

  it("does not say nothing is deployed when the only connectors are undescribed", () => {
    const one = connectVoice(voiceListing([], ["a"]));
    expect(one).not.toContain("No connectors are deployed");
    expect(one).toContain("a");
    expect(one).toContain("would not describe");

    const two = connectVoice(voiceListing([], ["a", "b"]));
    expect(two).not.toContain("No connectors are deployed");
    expect(two).toContain("a");
    expect(two).toContain("b");
  });
});
