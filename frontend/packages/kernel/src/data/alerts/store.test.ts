/**
 * The alert feed store: the one number the bell and the card are not allowed to disagree about.
 *
 * ## Where the bytes in this file come from
 *
 * `services/alerts/contract/src/kui/alerts/contract/dto/AlertDtos.scala` — the `Json.obj(…)` in
 * `AlertEventDto`, `AlertFeedDto`, `AlertFeedResponse` and `AlertChangeDto`, field for field. They
 * are **not** the shape the store's author found convenient, and that distinction is the whole of
 * wave 6's rule 12: wave 5's producers wire had a green unit test on each side and the two sides
 * read different field names, so the decode succeeded, answered an empty array, and the card
 * announced that a source naming five producers had named none.
 *
 * `wireEvent`/`wireFeed`/`body` below are the encoder's shape written once, and every case builds
 * from them rather than from a literal beside its own assertion — so a rename on the server has one
 * place to be wrong in and it is a place that fails.
 */
import { describe, expect, it } from "vitest";
import { ErrorCodes, type ApiResult } from "@kui/api";
import { createRoot, createSignal, flush } from "solid-js";

import {
  backoffFor,
  openEventSourceWith,
  type EventSourceLike,
  type SseConnection,
  type SseHandle,
  type SseSubscriber,
} from "../sse/stream.js";
import {
  createAlerts,
  ALERTS_EVENT_NAME,
  ALERTS_SECTION_KEY,
  type AlertFeedCache,
} from "./store.js";
import { decodeAlertFeed, type AlertChange } from "./events.js";

/** One event, as `AlertEventDto`'s encoder writes it. Overridable field by field. */
function wireEvent(over: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    id: "partition:analytics.clickstream",
    severity: "critical",
    tone: "danger",
    category: "partition",
    glyph: "partition",
    openedAt: "2026-09-06T09:00:00Z",
    lastSeenAt: "2026-09-06T09:04:00Z",
    title: "2 offline partitions",
    detail: "analytics.clickstream p3, p7",
    resolution: null,
    ...over,
  };
}

/** The `data` half, as `AlertFeedDto`'s encoder writes it. */
function wireFeed(over: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    items: [wireEvent()],
    total: 1,
    openCount: 2,
    unreadCount: 2,
    lastReadAt: null,
    evaluatedAt: "2026-09-06T09:05:00Z",
    rules: [],
    ...over,
  };
}

/** The whole body, as `AlertFeedResponse` writes it: one section under the key `events`. */
function body(data: Record<string, unknown>, status = "ok"): unknown {
  return { events: { status, data, fetchedAt: "2026-09-06T09:05:00Z" } };
}

/** One `AlertChangeDto`. It carries a count and no events — that is the service's design. */
function wireChange(over: Record<string, unknown> = {}): Record<string, unknown> {
  return { cluster: "quickstart", openCount: 3, at: "2026-09-06T09:06:00Z", ...over };
}

function answering(value: unknown): Promise<ApiResult<unknown>> {
  return Promise.resolve<ApiResult<unknown>>({ ok: true, value });
}

/**
 * A stream that behaves the way `../sse/stream.ts` does, including the two things a counter misses.
 *
 * Every `open()` produces a **new** handle with its own connection signal, and `close()` moves that
 * handle to `closed / "closed by the client"` — which is what `SseTransport.close()` does, through
 * `closeWith`. Both matter to this store: it releases one handle and opens another, and a fake that
 * shared one signal across opens, or that counted `close()` without emitting anything, would let a
 * store that kept following a released stream pass a case the transport fails.
 */
function fakeStream() {
  let subscriber: SseSubscriber<AlertChange> | undefined;
  let opens = 0;
  let closes = 0;
  let live: { readonly handle: SseHandle; readonly set: (next: SseConnection) => void } | undefined;

  function openOne(): { handle: SseHandle; set: (next: SseConnection) => void } {
    const [connection, setConnection] = createSignal<SseConnection>(
      { phase: "connecting" },
      { ownedWrite: true },
    );
    const handle: SseHandle = {
      connection,
      close: () => {
        closes += 1;
        setConnection({ phase: "closed", reason: "closed by the client" });
      },
      endMarker: () => undefined,
    };
    return { handle, set: setConnection };
  }

  const setConnection = (next: SseConnection): void => {
    if (live === undefined) throw new Error("nothing is open");
    live.set(next);
  };

  return {
    get opens(): number {
      return opens;
    },
    get closes(): number {
      return closes;
    },
    get listensFor(): readonly string[] {
      return subscriber?.events ?? [];
    },
    open(next: SseSubscriber<AlertChange>): SseHandle {
      subscriber = next;
      opens += 1;
      live = openOne();
      return live.handle;
    },
    /** Feeds a frame the way the wire does: as the text of one `data:` line. */
    send(frame: unknown): void {
      const decoded = subscriber?.decode(ALERTS_EVENT_NAME, JSON.stringify(frame));
      if (decoded === undefined) throw new Error("nothing is subscribed");
      if (decoded.ok) subscriber?.onEvent(decoded.value);
      else subscriber?.onError({ kind: "decode", event: ALERTS_EVENT_NAME, cause: decoded.cause });
      flush();
    },
    /**
     * A terminal transport failure, exactly as the stream module delivers one: the subscriber is
     * told and the connection then reads `closed`. Both halves matter — a fake that only called
     * `onError` would let the store's connection accessor pass a test the real transport fails.
     */
    fail(cause: string): void {
      subscriber?.onError({ kind: "transport", cause });
      setConnection({ phase: "closed", reason: cause });
      flush();
    },
    /** The transport reconnecting on its own, which only the handle can report. */
    reconnecting(attempt: number): void {
      setConnection({ phase: "reconnecting", attempt });
      flush();
    },
    /** The transport is live. */
    live(): void {
      setConnection({ phase: "open" });
      flush();
    },
  };
}

/**
 * A clock the case advances by hand.
 *
 * Every harness passes one, so that a reopen the store schedules after a dropped stream can never
 * fire on a real timer after its case has finished and open a stream into the next one.
 */
function handDrivenTimers() {
  const pending: Array<{ readonly delayMs: number; readonly action: () => void }> = [];
  return {
    schedule: (delayMs: number, action: () => void): void => {
      pending.push({ delayMs, action });
    },
    /** The delays asked for and not yet run, oldest first. */
    get delays(): readonly number[] {
      return pending.map((one) => one.delayMs);
    },
    /** Runs everything that is due, as the clock would once the longest delay has passed. */
    runAll(): void {
      for (const one of pending.splice(0)) one.action();
      flush();
    },
  };
}

function harness(
  options: {
    load?: (markRead: boolean) => Promise<ApiResult<unknown>>;
    cluster?: () => string | undefined;
    cache?: AlertFeedCache;
  } = {},
) {
  const stream = fakeStream();
  const warnings: string[] = [];
  const reads: boolean[] = [];
  const timers = handDrivenTimers();

  const alerts = createAlerts({
    openStream: (subscriber) => stream.open(subscriber),
    load: (markRead) => {
      reads.push(markRead);
      return options.load?.(markRead) ?? answering(body(wireFeed()));
    },
    cluster: options.cluster ?? (() => "quickstart"),
    cache: options.cache,
    warn: (message) => warnings.push(message),
    schedule: timers.schedule,
  });

  return {
    stream,
    alerts,
    warnings,
    /** One entry per read, holding the `markRead` it asked for. */
    reads,
    /** What the store asked to run later. Nothing runs until a case says so. */
    timers,
    async settle(): Promise<void> {
      await Promise.resolve();
      await Promise.resolve();
      flush();
    },
  };
}

describe("the alert feed store", () => {
  it("shows a validated saved feed while the backend refreshes, then replaces and updates it", async () => {
    await createRoot(async (dispose) => {
      let answer: (result: ApiResult<unknown>) => void = () => {};
      const writes: unknown[] = [];
      const world = harness({
        load: () => new Promise<ApiResult<unknown>>((resolve) => (answer = resolve)),
        cache: {
          read: () => Promise.resolve(wireFeed({ openCount: 7, unreadCount: 4 })),
          write: (value) => {
            writes.push(value);
            return Promise.resolve();
          },
          remove: () => Promise.resolve(),
        },
      });

      world.alerts.start();
      await world.settle();

      const warmed = world.alerts.feed();
      expect(warmed.kind).toBe("stale");
      expect(warmed.kind === "stale" && warmed.reason).toBe(
        "Showing saved alerts while KUI refreshes.",
      );
      expect(world.alerts.openCount()).toBe(7);
      expect(writes).toEqual([]);

      answer({ ok: true, value: body(wireFeed({ openCount: 9, unreadCount: 1 })) });
      await world.settle();

      expect(world.alerts.feed().kind).toBe("ready");
      expect(world.alerts.openCount()).toBe(9);
      expect(writes).toHaveLength(1);
      expect(writes[0]).toMatchObject({ openCount: 9, unreadCount: 1 });
      dispose();
    });
  });

  it("ignores an unreadable saved feed", async () => {
    await createRoot(async (dispose) => {
      const world = harness({
        load: () => new Promise<ApiResult<unknown>>(() => {}),
        cache: {
          read: () => Promise.resolve({ entries: [wireEvent()], openCount: 99 }),
          write: () => Promise.resolve(),
          remove: () => Promise.resolve(),
        },
      });

      world.alerts.start();
      await world.settle();

      expect(world.alerts.feed()).toEqual({ kind: "loading" });
      expect(world.alerts.openCount()).toBeNull();
      dispose();
    });
  });

  it("does not apply a saved feed from an earlier start", async () => {
    await createRoot(async (dispose) => {
      const cacheReads: Array<(value: unknown) => void> = [];
      const world = harness({
        load: () => new Promise<ApiResult<unknown>>(() => {}),
        cache: {
          read: () => new Promise<unknown>((resolve) => cacheReads.push(resolve)),
          write: () => Promise.resolve(),
          remove: () => Promise.resolve(),
        },
      });

      world.alerts.start();
      world.alerts.stop();
      world.alerts.start();
      expect(cacheReads).toHaveLength(2);

      cacheReads[0]?.(wireFeed({ openCount: 41 }));
      await world.settle();

      expect(world.alerts.feed()).toEqual({ kind: "loading" });
      expect(world.alerts.openCount()).toBeNull();
      dispose();
    });
  });

  it("clears the previous cluster's rows as soon as a new start begins", async () => {
    await createRoot(async (dispose) => {
      let first = true;
      const world = harness({
        load: () => {
          if (first) {
            first = false;
            return answering(body(wireFeed({ openCount: 8 })));
          }
          return new Promise<ApiResult<unknown>>(() => {});
        },
      });

      world.alerts.start();
      await world.settle();
      expect(world.alerts.events()).toHaveLength(1);
      expect(world.alerts.openCount()).toBe(8);

      world.alerts.stop();
      world.alerts.start();
      flush();

      expect(world.alerts.feed()).toEqual({ kind: "loading" });
      expect(world.alerts.events()).toEqual([]);
      expect(world.alerts.openCount()).toBeNull();
      dispose();
    });
  });

  it("the alerts store answers one open count to two subscribers", async () => {
    await createRoot(async (dispose) => {
      const world = harness();
      world.alerts.start();
      await world.settle();

      // The bell and the card. They are in packages that cannot see each other, so this is the
      // only place they can be made to agree — and a store that opened a stream per reader would
      // give them two answers and two subscriptions on the gateway.
      const bell = (): number | null => world.alerts.openCount();
      const card = (): number | null => world.alerts.openCount();
      expect([bell(), card()]).toEqual([2, 2]);
      expect(world.stream.opens).toBe(1);
      expect(world.reads).toEqual([false]);

      // And they move together, on the frame, before the re-read it triggers has landed. That is
      // why `AlertChangeDto` carries the count at all.
      world.stream.send(wireChange({ openCount: 3 }));
      expect([bell(), card()]).toEqual([3, 3]);
      dispose();
    });
  });

  it("draws the server's open count and never a count of the rows it holds", async () => {
    await createRoot(async (dispose) => {
      // Five rows on this page, of which four are open, and a cluster-wide count of eleven. Every
      // one of those is a different number, and only one of them is the answer — the service's own
      // scaladoc says `openCount` is over the whole store and `items` is one page.
      const items = [
        wireEvent({ id: "a" }),
        wireEvent({ id: "b" }),
        wireEvent({ id: "c" }),
        wireEvent({ id: "d" }),
        wireEvent({ id: "e", resolution: { at: "2026-09-06T05:30:00Z", kind: "acknowledged" } }),
      ];
      const world = harness({
        load: () => answering(body(wireFeed({ items, total: 5, openCount: 11, unreadCount: 7 }))),
      });
      world.alerts.start();
      await world.settle();

      expect(world.alerts.events()).toHaveLength(5);
      expect(world.alerts.openCount()).toBe(11);
      expect(world.alerts.openCount()).not.toBe(4);
      expect(world.alerts.unreadCount()).toBe(7);
      expect(world.alerts.unreadCount()).not.toBe(5);
      dispose();
    });
  });

  it("a feed that carries no counts answers unknown and never zero", async () => {
    await createRoot(async (dispose) => {
      const world = harness({
        load: () => answering(body({ items: [wireEvent()] })),
      });
      world.alerts.start();
      await world.settle();

      // `null` is "KUI does not know how many are open". A zero here is the most reassuring
      // possible rendering of that, and the bell would draw it as "nothing wrong".
      expect(world.alerts.openCount()).toBeNull();
      expect(world.alerts.unreadCount()).toBeNull();
      // And an unknown unread count does not light the bell either.
      expect(world.alerts.unread()).toBe(false);
      expect(world.alerts.events()).toHaveLength(1);
      dispose();
    });
  });

  it("refuses a document it cannot read rather than answering an empty feed", async () => {
    await createRoot(async (dispose) => {
      // The wave-5 wire, in miniature: the encoder called the list `entries` and this build looks
      // for `items`. The section key matches and the status is `ok`, so nothing above this line
      // notices — which is exactly how the producers card came to announce an empty source.
      const world = harness({
        load: () => answering(body({ entries: [wireEvent()], openCount: 2 })),
      });
      world.alerts.start();
      await world.settle();

      const state = world.alerts.feed();
      expect(state.kind).toBe("failed");
      expect(state.kind === "failed" && state.message).toContain("entries");
      expect(world.alerts.events()).toEqual([]);
      expect(world.alerts.openCount()).toBeNull();
      dispose();
    });
  });

  it("carries a severity and a tone it does not recognise, and maps neither", async () => {
    await createRoot(async (dispose) => {
      const world = harness({
        load: () =>
          answering(body(wireFeed({ items: [wireEvent({ severity: "catastrophic", tone: "puce" })] }))),
      });
      world.alerts.start();
      await world.settle();

      // The server sends both halves of the pair so that three screens cannot map them
      // differently. Folding an unfamiliar word onto the nearest one KUI knows would put a tone on
      // screen saying how bad something is when the server never said it.
      expect(world.alerts.events()[0]?.severity).toBe("catastrophic");
      expect(world.alerts.events()[0]?.tone).toBe("puce");
      // And the four words the service does send survive the same way, so the case above is not
      // simply asserting that this decoder drops everything on the floor.
      const known = world.alerts.events();
      expect(known).toHaveLength(1);
      expect(known[0]?.category).toBe("partition");
      expect(known[0]?.glyph).toBe("partition");
      dispose();
    });
  });

  it("refuses an event that carries no tone to draw it in", async () => {
    await createRoot(async (dispose) => {
      const toneless = { ...wireEvent() };
      delete (toneless as Record<string, unknown>)["tone"];
      const world = harness({ load: () => answering(body(wireFeed({ items: [toneless] }))) });
      world.alerts.start();
      await world.settle();

      // There is no honest colour for a row that arrived without one, and the browser choosing it
      // from the severity is the fabrication house rule 7 names — which is why the server derives
      // the tone and sends it.
      const state = world.alerts.feed();
      expect(state.kind).toBe("failed");
      expect(state.kind === "failed" && state.message).toContain("no tone");
      dispose();
    });
  });

  it("reads the resolution the server sent and never infers one", async () => {
    await createRoot(async (dispose) => {
      const world = harness({
        load: () =>
          answering(
            body(
              wireFeed({
                items: [
                  wireEvent({ id: "open-one" }),
                  wireEvent({
                    id: "closed-one",
                    resolution: { at: "2026-09-06T09:30:00Z", kind: "acknowledged", by: "alice" },
                  }),
                ],
              }),
            ),
          ),
      });
      world.alerts.start();
      await world.settle();

      const [open, closed] = world.alerts.events();
      expect(open?.resolution).toBeUndefined();
      expect(closed?.resolution).toEqual({
        at: "2026-09-06T09:30:00Z",
        kind: "acknowledged",
        by: "alice",
      });

      // And a resolution the server sent without a word for it stays without one, rather than
      // acquiring "resolved" from this build and reading as something the service said.
      const bare = decodeAlertFeed(
        wireFeed({ items: [wireEvent({ resolution: { at: "2026-09-06T09:30:00Z" } })] }),
      );
      expect(bare.ok && bare.value.items[0]?.resolution).toEqual({
        at: "2026-09-06T09:30:00Z",
        kind: undefined,
        by: undefined,
      });
      dispose();
    });
  });

  it("does not read a resolution that carries no time as an event that ended", async () => {
    /* Filed by W7-A3. `readResolution`'s own comment argues that "a resolution without a time
       cannot establish that the condition ended" — and nothing asserted it. A resolution that
       carried a `kind` and a `by` and no `at` would have been enough to make `isOpen` answer
       false and the alerts screen's `resolved` filter hide a live event, and the mutation that
       kept it (answering `{ at: "", kind, by }`) left all 452 kernel cases green.

       An event the server has not said ended is open, and that is the direction the mistake has
       to fall in: an open event drawn as resolved is one nobody goes and looks at. */
    const timeless = decodeAlertFeed(
      wireFeed({
        items: [wireEvent({ id: "no-time", resolution: { kind: "acknowledged", by: "alice" } })],
      }),
    );
    expect(timeless.ok).toBe(true);
    expect(timeless.ok && timeless.value.items[0]?.resolution).toBeUndefined();

    // Nor one whose instant is a word rather than an instant.
    const nonsense = decodeAlertFeed(
      wireFeed({
        items: [wireEvent({ id: "bad-time", resolution: { at: "yesterday", kind: "cleared" } })],
      }),
    );
    expect(nonsense.ok && nonsense.value.items[0]?.resolution).toBeUndefined();
  });

  it("refuses an instant that is not one, rather than carrying the server's word for a date", async () => {
    /* Filed by W7-A3. `optionalInstant` parses before it accepts, and the whole decoder rests on
       it: `openedAt` is required, so a row whose `openedAt` is unparseable must refuse the feed
       rather than reach a screen that renders `new Date("last Tuesday")` as `Invalid Date` and
       prints it beside a severity. Deleting the `Date.parse` check left all 452 cases green,
       because every document in this file carries well-formed instants.

       `lastReadAt` takes the same route and matters for a different reason: it is compared
       lexically against `openedAt` to decide what this principal has read, and a value that is
       not an RFC 3339 instant would mark rows read or unread by string order alone. */
    const unparseable = decodeAlertFeed(
      wireFeed({ items: [wireEvent({ id: "when", openedAt: "last Tuesday" })] }),
    );
    expect(unparseable.ok).toBe(false);
    expect(!unparseable.ok && unparseable.cause).toContain("openedAt");

    const marker = decodeAlertFeed(wireFeed({ lastReadAt: "soon" }));
    expect(marker.ok && marker.value.lastReadAt).toBeUndefined();

    // And a real instant still arrives in the server's own spelling, unreformatted.
    const good = decodeAlertFeed(wireFeed({ lastReadAt: "2026-09-06T09:03:00Z" }));
    expect(good.ok && good.value.lastReadAt).toBe("2026-09-06T09:03:00Z");
  });

  it("refuses a feed whose rules are not a list, rather than reporting no rules", async () => {
    /* Filed by W7-A3. `RuleReports` draws nothing at all when `rules` is empty, and its whole
       argument is that a feed drawn without the rule rows "looks identical whether four rules ran
       and found nothing or whether one of them has been refused by an ACL all afternoon". So a
       `rules` value this build cannot read has to refuse the document; silently answering `[]`
       hides the one panel that says which of the two the reader is looking at. Neutering the
       guard left all 452 cases green. */
    const wrong = decodeAlertFeed(wireFeed({ rules: { partition: "ok" } }));
    expect(wrong.ok).toBe(false);
    expect(!wrong.ok && wrong.cause).toContain("rules");

    // An absent `rules` key is still the ordinary case and is not a refusal.
    const absent = decodeAlertFeed(wireFeed({ rules: undefined }));
    expect(absent.ok && absent.value.rules).toEqual([]);
  });

  it("says what the transport says about the connection, holding no second opinion", async () => {
    await createRoot(async (dispose) => {
      const world = harness();
      world.alerts.start();
      await world.settle();

      // A healthy stream with nothing happening on it. Set by hand from the first frame instead,
      // this reads "connecting" for as long as the cluster stays quiet — which on a well-run
      // cluster is for ever, and the indicator then says the feed never came up.
      world.stream.live();
      expect(world.alerts.connection()).toEqual({ phase: "open" });

      // And the one phase only the transport can know. Nothing in this store could report it.
      world.stream.reconnecting(3);
      expect(world.alerts.connection()).toEqual({ phase: "reconnecting", attempt: 3 });
      dispose();
    });
  });

  it("keeps what it holds when the stream drops, and says it is stale", async () => {
    await createRoot(async (dispose) => {
      const world = harness();
      world.alerts.start();
      await world.settle();
      expect(world.alerts.feed().kind).toBe("ready");

      world.stream.fail("the connection was lost");

      const state = world.alerts.feed();
      expect(state.kind).toBe("stale");
      // Still readable, still counted, and now badged. Blanking the card at the moment the stream
      // goes takes the last known figures away exactly when they are wanted.
      expect(world.alerts.openCount()).toBe(2);
      expect(world.alerts.connection().phase).toBe("closed");
      dispose();
    });
  });

  it("reopens a stream that closed without being asked to, and re-reads once it is back", async () => {
    // The defect: `connect()` was reached only from `start()`, so a stream that ended — the browser
    // giving up on a proxy's 502 while the gateway restarted, or the heartbeat watchdog after a long
    // outage — was never opened again. The bell stayed on the pre-restart count for the rest of the
    // session, and no new alert arrived until a reload or a cluster switch.
    await createRoot(async (dispose) => {
      let served = 2;
      const world = harness({ load: () => answering(body(wireFeed({ openCount: served }))) });
      world.alerts.start();
      await world.settle();
      world.stream.live();
      expect(world.stream.opens).toBe(1);

      world.stream.fail("the connection was lost and will not be retried");
      expect(world.alerts.feed().kind).toBe("stale");
      expect(world.timers.delays).toHaveLength(1);

      world.timers.runAll();
      expect(world.stream.opens).toBe(2);
      // Opened again is not the same as caught up: nothing that changed during the outage reached
      // this store, so the rows stay badged until a read after the new stream is up says otherwise.
      expect(world.alerts.feed().kind).toBe("stale");
      expect(world.reads).toEqual([false]);

      served = 5;
      world.stream.live();
      await world.settle();
      expect(world.reads).toEqual([false, false]);
      expect(world.alerts.feed().kind).toBe("ready");
      expect(world.alerts.openCount()).toBe(5);
      expect(world.alerts.connection()).toEqual({ phase: "open" });
      dispose();
    });
  });

  it("backs off between reopen attempts, and a stopped or restarted store reopens nothing", async () => {
    await createRoot(async (dispose) => {
      const world = harness();
      world.alerts.start();
      await world.settle();
      world.stream.live();

      const withinBackoff = (attempt: number): void => {
        const [delay] = world.timers.delays;
        expect(delay).toBeGreaterThanOrEqual(backoffFor(attempt) * 0.8);
        expect(delay).toBeLessThanOrEqual(backoffFor(attempt));
      };

      // Two failures in a row with no `open` between them: the second waits longer.
      world.stream.fail("the connection was lost");
      withinBackoff(1);
      world.timers.runAll();
      world.stream.fail("the connection was lost");
      withinBackoff(2);

      // A teardown is not an outage. The reopen already on the clock must not resurrect a stream
      // the shell has released, and the release itself must not schedule another one.
      world.alerts.stop();
      world.timers.runAll();
      expect(world.stream.opens).toBe(2);
      expect(world.timers.delays).toEqual([]);

      // Nor may one from an earlier life open a second stream beside the one a new start opened.
      world.alerts.start();
      await world.settle();
      world.stream.live();
      world.stream.fail("the connection was lost");
      world.alerts.start();
      await world.settle();
      expect(world.stream.opens).toBe(4);
      world.timers.runAll();
      expect(world.stream.opens).toBe(4);

      // And the count starts again from the first attempt in the new life.
      world.stream.live();
      world.stream.fail("the connection was lost");
      withinBackoff(1);
      world.alerts.stop();
      dispose();
    });
  });

  it("a read answered after the store was stopped is not applied", async () => {
    await createRoot(async (dispose) => {
      let answer: (result: ApiResult<unknown>) => void = () => {};
      const world = harness({
        load: () => new Promise<ApiResult<unknown>>((resolve) => (answer = resolve)),
      });
      world.alerts.start();
      world.alerts.stop();

      // The request cannot be recalled and the shell has already torn the card down. Painting this
      // over the store repopulates a bell nobody is looking at, on a session the user may have
      // signed out of.
      answer({ ok: true, value: body(wireFeed()) });
      await world.settle();

      expect(world.alerts.feed().kind).toBe("loading");
      expect(world.alerts.openCount()).toBeNull();
      dispose();
    });
  });

  it("an older read that answers last does not overwrite a newer one", async () => {
    await createRoot(async (dispose) => {
      const pending: Array<(result: ApiResult<unknown>) => void> = [];
      const world = harness({
        load: () => new Promise<ApiResult<unknown>>((resolve) => pending.push(resolve)),
      });
      world.alerts.start();
      world.alerts.refresh();
      expect(pending).toHaveLength(2);

      // The second read answers first — a slow gateway and a fast one, or a retried connection —
      // and then the first arrives with the picture as it was before.
      pending[1]?.({ ok: true, value: body(wireFeed({ openCount: 9 })) });
      await world.settle();
      pending[0]?.({ ok: true, value: body(wireFeed({ openCount: 2 })) });
      await world.settle();

      expect(world.alerts.openCount()).toBe(9);
      dispose();
    });
  });

  it("closes its stream when it is stopped", async () => {
    await createRoot(async (dispose) => {
      const world = harness();
      world.alerts.start();
      await world.settle();
      expect(world.stream.opens).toBe(1);

      world.alerts.stop();
      expect(world.stream.closes).toBe(1);

      // A second stop changes nothing, and a stopped store opens nothing.
      world.alerts.stop();
      expect(world.stream.opens).toBe(1);
      dispose();
    });
  });

  it("says the teardown was its own, and stops following the stream it released", async () => {
    await createRoot(async (dispose) => {
      const world = harness();
      world.alerts.start();
      await world.settle();
      world.stream.live();
      expect(world.alerts.connection()).toEqual({ phase: "open" });

      world.alerts.stop();
      flush();
      // The close is itself a connection event on the handle. What the store publishes is its own
      // sentence about a deliberate teardown — the frame's connectivity banner reads this, and an
      // operator told the feed went down when they navigated away goes looking for an outage.
      const byTheClient = { phase: "closed", reason: "closed by the client" };
      expect(world.alerts.connection()).toEqual(byTheClient);

      // And a released handle is no longer followed: a transport retrying on a stream nobody is
      // reading would otherwise repaint the banner as `reconnecting` on a store the shell has torn
      // down. `releaseHandle()` holds that with two guards — clearing `handle` and disposing the
      // watcher — and this line was measured against both: it goes red when they are removed
      // together and green when either survives, because either is sufficient alone.
      world.stream.reconnecting(4);
      expect(world.alerts.connection()).toEqual(byTheClient);
      dispose();
    });
  });

  it("a completed read supersedes the count the frame carried", async () => {
    await createRoot(async (dispose) => {
      let served = 2;
      const world = harness({ load: () => answering(body(wireFeed({ openCount: served }))) });
      world.alerts.start();
      await world.settle();
      expect(world.alerts.openCount()).toBe(2);

      // The frame lands first and moves the bell at once; that is the whole reason the service puts
      // a count on it. Then the read it triggered answers with the server's settled figure — which
      // here is neither the old count nor the one the frame guessed at.
      served = 4;
      world.stream.send(wireChange({ openCount: 9 }));
      expect(world.alerts.openCount()).toBe(9);
      await world.settle();

      // The hint is a round trip's worth of head start and not a second source. Kept alive past the
      // read, it wins over every later answer for the life of the tab: the bell draws 9 while the
      // panel it opens lists the 4 the same store just decoded.
      expect(world.alerts.openCount()).toBe(4);
      expect(world.alerts.feed().kind).toBe("ready");
      dispose();
    });
  });

  it("a read that fails at the transport keeps the rows and drops the count the frame carried", async () => {
    await createRoot(async (dispose) => {
      let reachable = true;
      const world = harness({
        load: () =>
          reachable
            ? answering(body(wireFeed()))
            : Promise.resolve<ApiResult<unknown>>({
                ok: false,
                error: { kind: "unreachable", cause: "the gateway is not answering" },
              }),
      });
      world.alerts.start();
      await world.settle();
      expect(world.alerts.openCount()).toBe(2);

      // The frame moves the bell to 9 at once, and the read it triggers never reaches the gateway.
      // Nothing was disproved, so the rows stay and are badged — this is the same event as the
      // stream dropping, one transport down, and blanking the card here would take the last known
      // figures away at the moment they are most wanted.
      reachable = false;
      world.stream.send(wireChange({ openCount: 9 }));
      expect(world.alerts.openCount()).toBe(9);
      await world.settle();

      const state = world.alerts.feed();
      expect(state.kind).toBe("stale");
      expect(state.kind === "stale" && state.reason).toBe("KUI cannot reach the server.");
      expect(world.alerts.events()).toHaveLength(1);

      // And the hint goes with the read that failed. This is the outcome the header's *"on every
      // outcome"* is about and the only one that can see it: a completed read supersedes the frame,
      // and a read that completed by failing is still a completed read. Left alive, the bell would
      // draw a 9 that came from a frame whose read never landed, over rows that say plainly they are
      // the last answer KUI received — the bell is then the most confident thing on the screen and
      // the least supported. `applyRead()` drops it before it branches; moving that line below the
      // failure branch reddens exactly here.
      expect(world.alerts.openCount()).toBe(2);
      dispose();
    });
  });

  it("reports a first read that never reached the gateway under the transport's own code", async () => {
    await createRoot(async (dispose) => {
      const world = harness({
        load: () =>
          Promise.resolve<ApiResult<unknown>>({
            ok: false,
            error: { kind: "unreachable", cause: "the gateway is not answering" },
          }),
      });
      world.alerts.start();
      await world.settle();

      // Nothing is held, so there are no rows to badge and the honest state is a failure. The code
      // is the half that matters and the half a store can get wrong without anybody noticing: it is
      // what an operator quotes in a ticket, and routing this through the stream's staleness path
      // would file the browser's own unreachable gateway under `STREAM_CLOSED` — a code about a
      // subscription that was never opened. Deleting `current() !== undefined` from the transport
      // branch in `applyRead()` reddens exactly this line and nothing else.
      const state = world.alerts.feed();
      expect(state.kind).toBe("failed");
      expect(state.kind === "failed" && state.code).toBe("UNREACHABLE");
      expect(state.kind === "failed" && state.message).toBe("KUI cannot reach the server.");
      dispose();
    });
  });

  it("blanks the feed when the server answers with a refusal rather than not answering at all", async () => {
    await createRoot(async (dispose) => {
      let refuse = false;
      let removals = 0;
      const world = harness({
        load: () =>
          refuse
            ? Promise.resolve<ApiResult<unknown>>({
                ok: false,
                error: {
                  kind: "envelope",
                  code: ErrorCodes.Forbidden,
                  message: "You may not read this cluster's alerts.",
                  details: [],
                  correlationId: "c-1",
                  retryable: false,
                },
              })
            : answering(body(wireFeed())),
        cache: {
          read: () => Promise.resolve(undefined),
          write: () => Promise.resolve(),
          remove: () => {
            removals += 1;
            return Promise.resolve();
          },
        },
      });
      world.alerts.start();
      await world.settle();
      expect(world.alerts.feed().kind).toBe("ready");

      // The other side of the branch above, and the reason it is a branch. An envelope is the server
      // having an opinion — here, that this principal may no longer see the cluster — and rows kept
      // on screen under a staleness badge would be data the server has just refused. Only "nothing
      // answered" earns the rows.
      refuse = true;
      world.alerts.refresh();
      await world.settle();

      expect(world.alerts.feed().kind).toBe("failed");
      expect(world.alerts.events()).toEqual([]);
      expect(world.alerts.openCount()).toBeNull();
      expect(removals).toBe(1);
      dispose();
    });
  });

  it("a live count does not outlive the refusal that answered it", async () => {
    await createRoot(async (dispose) => {
      let refused = false;
      const world = harness({
        load: () =>
          refused ? answering({ events: { status: "forbidden" } }) : answering(body(wireFeed())),
      });
      world.alerts.start();
      await world.settle();
      expect(world.alerts.openCount()).toBe(2);

      // The principal loses the capability between one frame and the next read — a role change, or
      // a cluster they no longer see. The frame still arrives and still carries a number.
      refused = true;
      world.stream.send(wireChange({ openCount: 9 }));
      await world.settle();

      // A bell reading 9 over "You do not have permission to see this cluster's alerts" is two
      // answers from one store, and the more confident of them is the wrong one.
      expect(world.alerts.feed().kind).toBe("forbidden");
      expect(world.alerts.openCount()).toBeNull();
      dispose();
    });
  });

  it("answers unknown for a count the rules have never evaluated", async () => {
    await createRoot(async (dispose) => {
      const world = harness({
        load: () => answering(body(wireFeed({ openCount: 7, evaluatedAt: null }))),
      });
      world.alerts.start();
      await world.settle();

      // `evaluatedAt` absent means the rules have not run against this cluster here — after a
      // restart, or before the first pass. Whatever `openCount` says beside that is not a
      // measurement, and the bell must not draw it. The rows are still real and still shown.
      expect(world.alerts.feed().kind).toBe("ready");
      expect(world.alerts.events()).toHaveLength(1);
      expect(world.alerts.openCount()).toBeNull();
      dispose();
    });
  });

  it("a refresh issued after stop() is not applied", async () => {
    await createRoot(async (dispose) => {
      let served = 2;
      const world = harness({ load: () => answering(body(wireFeed({ openCount: served }))) });
      world.alerts.start();
      await world.settle();
      expect(world.alerts.openCount()).toBe(2);

      // A Retry click landing in the same tick the frame unmounts. `stop()` raises the episode, so
      // the read this issues matches its own episode and only `stopped` can refuse it — which is
      // why that clause is not the redundant half of the guard it looks like.
      world.alerts.stop();
      served = 9;
      world.alerts.refresh();
      await world.settle();

      expect(world.reads).toEqual([false, false]);
      expect(world.alerts.openCount()).toBe(2);
      dispose();
    });
  });

  it("subscribes to the feed's own event name and not to a shared one", async () => {
    await createRoot(async (dispose) => {
      const world = harness();
      world.alerts.start();
      await world.settle();
      // `AlertChangeDto.EventName`. ADR-035's four shared names are handled by the transport, and
      // listing one is a programming error the transport throws on.
      expect(world.stream.listensFor).toEqual([ALERTS_EVENT_NAME]);
      expect(world.stream.listensFor).not.toContain("heartbeat");
      dispose();
    });
  });

  it("a change frame re-reads the feed rather than carrying the rows itself", async () => {
    await createRoot(async (dispose) => {
      let count = 2;
      const world = harness({ load: () => answering(body(wireFeed({ openCount: count }))) });
      world.alerts.start();
      await world.settle();
      expect(world.reads).toEqual([false]);

      count = 3;
      world.stream.send(wireChange({ openCount: 3 }));
      // The count is taken from the frame at once, because that is what it is on the frame for.
      expect(world.alerts.openCount()).toBe(3);
      await world.settle();

      // And the rows come from a read the subscriber makes for itself, which is what keeps one
      // principal's feed off every other subscriber's socket. It does not mark anything read.
      expect(world.reads).toEqual([false, false]);
      expect(world.alerts.openCount()).toBe(3);
      dispose();
    });
  });

  it("re-reads after a same-count change because the rows may have changed", async () => {
    await createRoot(async (dispose) => {
      const world = harness();
      world.alerts.start();
      await world.settle();

      // One event may close while another opens in the same evaluation. The net count is still two,
      // but the page and its per-principal unread state have changed.
      world.stream.send(wireChange({ openCount: 2 }));
      await world.settle();

      expect(world.reads).toEqual([false, false]);
      expect(world.alerts.openCount()).toBe(2);
      dispose();
    });
  });

  it("ignores a change frame for a cluster this store is not watching", async () => {
    await createRoot(async (dispose) => {
      const world = harness();
      world.alerts.start();
      await world.settle();
      expect(world.reads).toEqual([false]);

      world.stream.send(wireChange({ cluster: "staging-eu-01", openCount: 41 }));
      await world.settle();

      // A number from a cluster the operator is not looking at, on the bell of the one they are.
      expect(world.alerts.openCount()).toBe(2);
      expect(world.reads).toEqual([false]);
      dispose();
    });
  });

  it("skips a frame it cannot read and keeps the feed on screen", async () => {
    await createRoot(async (dispose) => {
      const world = harness();
      world.alerts.start();
      await world.settle();

      world.stream.send({ openCount: 9 });
      await world.settle();

      expect(world.warnings).toHaveLength(1);
      expect(world.alerts.events()).toHaveLength(1);
      // And nothing the frame carried is taken, because the frame was never read.
      expect(world.alerts.openCount()).toBe(2);
      expect(world.reads).toEqual([false]);
      dispose();
    });
  });

  it("a forbidden section is not a failure, and a not-configured one is neither", async () => {
    await createRoot(async (dispose) => {
      const forbidden = harness({ load: () => answering({ events: { status: "forbidden" } }) });
      forbidden.alerts.start();
      await forbidden.settle();
      // A retry button here is a button that cannot work.
      expect(forbidden.alerts.feed().kind).toBe("forbidden");

      const absent = harness({ load: () => answering({ events: { status: "not_configured" } }) });
      absent.alerts.start();
      await absent.settle();
      // ADR-032: nothing is broken and there is nothing to wait for. The row is hidden, not empty.
      expect(absent.alerts.feed().kind).toBe("not-configured");
      dispose();
    });
  });

  it("the bell goes quiet because the server said so, not because the browser zeroed it", async () => {
    await createRoot(async (dispose) => {
      let unreadCount = 2;
      let cacheRemovals = 0;
      const world = harness({
        load: (markRead) => {
          if (markRead) unreadCount = 0;
          const lastReadAt = markRead ? "2026-09-06T09:07:00Z" : null;
          return answering(body(wireFeed({ unreadCount, lastReadAt })));
        },
        cache: {
          read: () => Promise.resolve(undefined),
          write: () => Promise.resolve(),
          remove: () => {
            cacheRemovals += 1;
            return Promise.resolve();
          },
        },
      });
      world.alerts.start();
      await world.settle();
      expect(world.alerts.unread()).toBe(true);

      world.alerts.markAllRead();
      expect(cacheRemovals).toBe(1);
      await world.settle();

      // `markAllRead` is the endpoint's own `markRead` query and the answer is the server's new
      // count. A store that set its own number to zero would go quiet even when the write was
      // refused, and the panel it opens would still be full of unread rows.
      expect(world.reads).toEqual([false, true]);
      expect(world.alerts.unreadCount()).toBe(0);
      expect(world.alerts.unread()).toBe(false);
      expect(world.alerts.lastReadAt()).toBe("2026-09-06T09:07:00Z");
      dispose();
    });
  });

  it("stays unread when the server refuses to mark it read", async () => {
    await createRoot(async (dispose) => {
      let refuse = false;
      const world = harness({
        load: (markRead) => {
          if (markRead && refuse) {
            return Promise.resolve<ApiResult<unknown>>({
              ok: false,
              error: { kind: "unreachable", cause: "the gateway is not answering" },
            });
          }
          return answering(body(wireFeed()));
        },
      });
      world.alerts.start();
      await world.settle();
      expect(world.alerts.unread()).toBe(true);

      refuse = true;
      world.alerts.markAllRead();
      await world.settle();

      // The failure is stated rather than swallowed, and nothing pretends the bell was cleared. The
      // gateway did not answer, so what is held is still the truth as of the last answer: the rows
      // stay, badged, and the unread mark stays lit on the server's own count. That is what this
      // case has always been named for — until wave 8 it asserted the opposite, because a failed
      // read blanked the feed and `unreadCount()` then answered `null`, which is "nobody has said"
      // rather than "nothing is unread". A bell that goes quiet because a request timed out is the
      // browser zeroing it by another route.
      const state = world.alerts.feed();
      expect(state.kind).toBe("stale");
      expect(state.kind === "stale" && state.reason).toBe("KUI cannot reach the server.");
      expect(world.alerts.unread()).toBe(true);
      expect(world.alerts.unreadCount()).toBe(2);
      dispose();
    });
  });

  it("names the section key and the rows the milestone will be closed against", () => {
    // M8's exit criterion in `docs/plan/ROADMAP.md` closes the milestone with
    // `jq -e '.events.status == "ok" and (.events.data.items | length) > 0'` and
    // `jq '.events.data.openCount'`, and `AlertDtos.scala` writes exactly those three. A rename on
    // either side has to break something before it reaches a released build; this is the something.
    expect(ALERTS_SECTION_KEY).toBe("events");
    const decoded = decodeAlertFeed(wireFeed());
    expect(decoded.ok).toBe(true);
    expect(decoded.ok && decoded.value.items).toHaveLength(1);
    expect(decoded.ok && decoded.value.openCount).toBe(2);
    // The names wave 5 would have shipped instead, each refused rather than folded into nothing.
    expect(decodeAlertFeed({ events: [wireEvent()], openCount: 2 }).ok).toBe(false);
    expect(decodeAlertFeed({ entries: [wireEvent()], openCount: 2 }).ok).toBe(false);
  });
});

/** An `EventSource` driven by hand, for the case that runs the store over the real transport. */
function fakeEventSource(): EventSourceLike & { emit(name: string): void; state: number } {
  const listeners = new Map<string, Array<(event: Event) => void>>();
  return {
    state: 0,
    get readyState(): number {
      return this.state;
    },
    addEventListener(name: string, handler: (event: Event) => void): void {
      listeners.set(name, [...(listeners.get(name) ?? []), handler]);
    },
    close(): void {
      this.state = 2;
    },
    emit(name: string): void {
      for (const handler of listeners.get(name) ?? []) handler(new Event(name));
    },
  };
}

describe("the alert store over the real stream transport", () => {
  it("goes stale when the browser gives up on the stream, and opens a new one", async () => {
    /*
     * The fake in the cases above delivers a terminal failure the way `SseSubscriber.onError`
     * promises one — the subscriber is told and the connection then reads `closed` — and the real
     * transport did not keep that promise when the browser itself gave up: it closed the handle and
     * told nobody. Every case above stayed green while the bell went on reading as live. This one
     * runs the store over `openEventSourceWith`, so the two halves cannot drift apart again.
     */
    await createRoot(async (dispose) => {
      const sources: Array<ReturnType<typeof fakeEventSource>> = [];
      const timers = handDrivenTimers();
      const reads: boolean[] = [];
      const alerts = createAlerts({
        openStream: (subscriber) =>
          openEventSourceWith(() => {
            const source = fakeEventSource();
            sources.push(source);
            return source;
          }, subscriber),
        load: (markRead) => {
          reads.push(markRead);
          return answering(body(wireFeed()));
        },
        cluster: () => "quickstart",
        schedule: timers.schedule,
        warn: () => {},
      });
      const settle = async (): Promise<void> => {
        await Promise.resolve();
        await Promise.resolve();
        flush();
      };

      alerts.start();
      await settle();
      sources[0]?.emit("open");
      flush();
      expect(alerts.feed().kind).toBe("ready");

      // A proxy's 502 on the browser's own reconnect: the browser fails the connection for good,
      // reports a bare `error` event and reads CLOSED.
      if (sources[0] !== undefined) sources[0].state = 2;
      sources[0]?.emit("error");
      flush();

      expect(alerts.feed().kind).toBe("stale");
      expect(alerts.openCount()).toBe(2);
      expect(alerts.connection().phase).toBe("closed");

      timers.runAll();
      expect(sources).toHaveLength(2);
      sources[1]?.emit("open");
      flush();
      await settle();

      expect(reads).toEqual([false, false]);
      expect(alerts.feed().kind).toBe("ready");
      expect(alerts.connection()).toEqual({ phase: "open" });
      alerts.stop();
      dispose();
    });
  });
});
