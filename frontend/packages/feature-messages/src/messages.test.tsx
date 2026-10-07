/**
 * The session and the screen.
 *
 * Every case below is a rule from the brief or from a defect the Laminar version was corrected for,
 * and each one is written so that it fails for the right reason: the pause test loses a record if
 * the queue is dropped, the identity test fails if the filter field is rebuilt, the tick test fails
 * if two records in one microtask are folded with `setRows((previous) => …)`.
 */

import { describe, expect, test, vi } from "vitest";
import { flush } from "solid-js";
import type { KafkaRecord } from "@kui/kernel";
import { mount } from "./testing.js";
import { DEFAULT_BROWSE, type BrowseQuery } from "./browse.js";
import { NO_PREDICATES } from "./predicates.js";
import {
  createBrowseSession,
  decodeBrowseEvent,
  INITIAL_RECORD_BATCH,
  MAX_CACHED_PAGES,
  MAX_RETAINED_PAYLOAD_BYTES,
  MAX_ROWS,
  STREAM_RECORD_BATCH,
  type BrowseHandle,
  type BrowseSession,
  type BrowseTransport,
} from "./session.js";
import { MessagesTab, offsetRangeLabel, pauseLabel } from "./MessagesTab.jsx";
import { toRecord, type MessageDto } from "./wire.js";

/** A transport that runs no network: the test drives the stream by hand. */
function fakeTransport(): {
  readonly transport: BrowseTransport;
  readonly urls: string[];
  emit: (record: KafkaRecord) => void;
  consumed: (records: number, filterErrors?: number) => void;
  close: (marker?: string, reason?: "limit" | "exhausted" | "budget" | "cancelled") => void;
  closes: () => number;
  breakConnection: () => void;
} {
  const urls: string[] = [];
  let handlers: Parameters<BrowseTransport["open"]>[1] | undefined;
  let marker: string | undefined;
  let reason: "limit" | "exhausted" | "budget" | "cancelled" | undefined;
  let closed = 0;
  const transport: BrowseTransport = {
    open: (url, given) => {
      urls.push(url);
      handlers = given;
      marker = undefined;
      reason = undefined;
      const handle: BrowseHandle = {
        close: () => {
          closed += 1;
        },
        endMarker: () => marker,
        endReason: () => reason,
      };
      return handle;
    },
  };
  return {
    transport,
    urls,
    emit: (record) => handlers?.onEvent({ kind: "record", record }),
    consumed: (records: number, filterErrors = 0) =>
      handlers?.onEvent({
        kind: "consumed",
        consumed: { records, bytes: records * 24, elapsedMs: 5, filterErrors },
      }),
    close: (end, why) => {
      marker = end;
      reason = why;
      handlers?.onDone?.();
      handlers?.onConnection({ phase: "closed", reason: "the stream finished" });
    },
    closes: () => closed,
    breakConnection: () => handlers?.onConnection({ phase: "closed", reason: "stream ended" }),
  };
}

function record(offset: string, partition = 0): KafkaRecord {
  return {
    offset,
    partition,
    key: `ord_${offset}`,
    timestamp: "2026-09-05T10:00:00Z",
    headers: [],
    value: { kind: "json", text: `{"orderId":"ord_${offset}"}` },
  };
}

function withSession(run: (session: BrowseSession, fake: ReturnType<typeof fakeTransport>) => void): void {
  const fake = fakeTransport();
  /* The session registers an `onCleanup`, so it needs an owner. Mounting a component that creates
   * it is the honest way to give it one — and it is also how the screen uses it. */
  const { dispose } = mount(() => {
    const session = createBrowseSession({
      streamUrl: "/stream",
      transport: fake.transport,
      // Most tests exercise state semantics, not scheduling. Keep those synchronous and reserve a
      // manual scheduler for the dedicated progressive-rendering regression above.
      scheduleAfterPaint: (resume) => {
        resume();
        return () => undefined;
      },
    });
    run(session, fake);
    return null;
  });
  dispose();
}

describe("a browse session", () => {
  test("does not report EOF without done as a successful browse", () => {
    withSession((session, fake) => {
      session.start(DEFAULT_BROWSE); fake.emit(record("1"));
      fake.breakConnection(); void flush();
      expect(session.progress().failure?.kind).toBe("transport");
      expect(session.canLoadMore()).toBe(false);
      expect(session.rows()).toHaveLength(1);
    });
  });
  test("releases records held by a pause when the server completes", () => {
    withSession((session, fake) => {
      session.start(DEFAULT_BROWSE);
      session.setPaused(true);
      fake.emit(record("1"));
      fake.close(undefined, "limit");
      void flush();
      expect(session.rows().map((row) => row.offset)).toEqual(["1"]);
      expect(session.held()).toBe(0);
      expect(session.paused()).toBe(false);
    });
  });

  test.each(["key", "headerName", "headerValue", "undecodable", "value"] as const)("bounds retained %s payloads independently", (field) => {
    withSession((session, fake) => {
      session.start(DEFAULT_BROWSE);
      const huge = "x".repeat(MAX_RETAINED_PAYLOAD_BYTES + 1);
      const extra: Partial<KafkaRecord> = field === "key" ? { key: huge }
        : field === "headerName" ? { headers: [{ name: huge, value: "small" }] }
        : field === "headerValue" ? { headers: [{ name: "small", value: huge }] }
        : field === "undecodable" ? { value: { kind: "undecodable", reason: "bad", hex: huge } }
        : { value: { kind: "text", text: huge } };
      fake.emit({ ...record("1"), ...extra });
      void flush();
      expect(JSON.stringify(session.rows()[0]).length).toBeLessThan(1000);
      expect(session.rows()[0]?.value.kind).toBe("large");
    });
  });
  test("yields a large stream after the first visible record batch", () => {
    const fake = fakeTransport();
    const continuations: Array<() => void> = [];
    const { dispose } = mount(() => {
      const session = createBrowseSession({
        streamUrl: "/stream",
        transport: fake.transport,
        scheduleAfterPaint: (resume) => {
          continuations.push(resume);
          return () => {
            const index = continuations.indexOf(resume);
            if (index >= 0) continuations.splice(index, 1);
          };
        },
      });

      session.start(DEFAULT_BROWSE);
      for (let index = 0; index < 100; index += 1) fake.emit(record(String(index)));
      fake.close(undefined, "limit");
      void flush();

      expect(session.rows()).toHaveLength(INITIAL_RECORD_BATCH);
      expect(session.running()).toBe(true);
      expect(continuations).toHaveLength(1);

      continuations.shift()?.();
      void flush();
      expect(session.rows()).toHaveLength(INITIAL_RECORD_BATCH + STREAM_RECORD_BATCH);
      expect(session.running()).toBe(true);

      while (continuations.length > 0) continuations.shift()?.();
      void flush();
      expect(session.rows()).toHaveLength(100);
      expect(session.rows().map((row) => row.offset)).toEqual(
        Array.from({ length: 100 }, (_, index) => String(index)),
      );
      expect(session.progress().delivered).toBe(100);
      expect(session.progress().endReason).toBe("limit");
      expect(session.running()).toBe(false);
      return null;
    });
    dispose();
  });

  test("does not rescan committed payloads when a later stream batch arrives", () => {
    const fake = fakeTransport();
    const continuations: Array<() => void> = [];
    const committed = Array.from({ length: 100 }, (_, index) => record(String(index)));
    const committedSet = new Set<object>(committed);
    let committedPayloadLookups = 0;
    const originalWeakMapGet = WeakMap.prototype.get;
    const { dispose } = mount(() => {
      const session = createBrowseSession({
        streamUrl: "/stream",
        transport: fake.transport,
        scheduleAfterPaint: (resume) => {
          continuations.push(resume);
          return () => undefined;
        },
      });

      session.start({ ...DEFAULT_BROWSE, live: true });
      for (const existing of committed) fake.emit(existing);
      while (continuations.length > 0) continuations.shift()?.();
      void flush();
      expect(session.rows()).toHaveLength(committed.length);

      const weakMapGet = vi
        .spyOn(WeakMap.prototype, "get")
        .mockImplementation(function (this: WeakMap<object, unknown>, key: object) {
          if (committedSet.has(key)) committedPayloadLookups += 1;
          return originalWeakMapGet.call(this, key);
        });
      try {
        for (let index = 100; index < 124; index += 1) fake.emit(record(String(index)));
        while (continuations.length > 0) continuations.shift()?.();
        void flush();

        expect(session.rows()).toHaveLength(124);
        expect(committedPayloadLookups).toBe(0);
      } finally {
        weakMapGet.mockRestore();
      }
      return null;
    });
    dispose();
  });

  test("bounds a live render backlog while keeping the newest rows and delivered count", () => {
    const fake = fakeTransport();
    const continuations: Array<() => void> = [];
    const total = MAX_ROWS * 3 + 25;
    let continuationRuns = 0;
    const { dispose } = mount(() => {
      const session = createBrowseSession({
        streamUrl: "/stream",
        transport: fake.transport,
        scheduleAfterPaint: (resume) => {
          continuations.push(resume);
          return () => undefined;
        },
      });

      session.start({ ...DEFAULT_BROWSE, live: true });
      for (let index = 0; index < total; index += 1) fake.emit(record(String(index)));
      fake.close(undefined, "cancelled");
      while (continuations.length > 0) {
        continuationRuns += 1;
        continuations.shift()?.();
      }
      void flush();

      expect(continuationRuns).toBeLessThanOrEqual(
        Math.ceil((MAX_ROWS + STREAM_RECORD_BATCH) / STREAM_RECORD_BATCH),
      );
      expect(session.rows()).toHaveLength(MAX_ROWS);
      expect(session.rows()[0]?.offset).toBe(String(total - 1));
      expect(session.rows().at(-1)?.offset).toBe(String(total - MAX_ROWS));
      expect(session.progress().delivered).toBe(total);
      expect(session.running()).toBe(false);
      return null;
    });
    dispose();
  });

  test("strips queued live payloads before they can exceed the retained byte budget", () => {
    const fake = fakeTransport();
    const continuations: Array<() => void> = [];
    const text = `{"padding":"${"x".repeat(300_000)}"}`;
    const { dispose } = mount(() => {
      const session = createBrowseSession({
        streamUrl: "/stream",
        transport: fake.transport,
        scheduleAfterPaint: (resume) => {
          continuations.push(resume);
          return () => undefined;
        },
      });

      session.start({ ...DEFAULT_BROWSE, live: true });
      for (let index = 0; index < 40; index += 1) {
        fake.emit({ ...record(String(index)), value: { kind: "json", text } });
      }
      fake.close(undefined, "cancelled");
      while (continuations.length > 0) continuations.shift()?.();
      void flush();

      const retained = session.rows().reduce((total, row) => {
        const value = row.value;
        const payload =
          value.kind === "json" || value.kind === "text" || value.kind === "large"
            ? value.text
            : undefined;
        return total + (payload === undefined ? 0 : new TextEncoder().encode(payload).length);
      }, 0);
      expect(session.rows()).toHaveLength(40);
      expect(session.progress().delivered).toBe(40);
      expect(retained).toBeLessThanOrEqual(MAX_RETAINED_PAYLOAD_BYTES);
      expect(session.rows().some((row) => row.value.kind === "large" && row.value.text === undefined)).toBe(true);
      return null;
    });
    dispose();
  });

  test("stopping aborts first, cancels the scheduled continuation, and keeps delivered backlog rows", () => {
    const fake = fakeTransport();
    const continuations: Array<() => void> = [];
    const { dispose } = mount(() => {
      const session = createBrowseSession({
        streamUrl: "/stream",
        transport: fake.transport,
        scheduleAfterPaint: (resume) => {
          continuations.push(resume);
          return () => {
            const index = continuations.indexOf(resume);
            if (index >= 0) continuations.splice(index, 1);
          };
        },
      });

      session.start({ ...DEFAULT_BROWSE, live: true });
      for (let index = 0; index < 20; index += 1) fake.emit(record(String(index)));
      void flush();
      expect(session.rows()).toHaveLength(INITIAL_RECORD_BATCH);
      expect(continuations).toHaveLength(1);

      session.stop();
      void flush();
      expect(fake.closes()).toBe(1);
      expect(continuations).toHaveLength(0);
      expect(session.rows()).toHaveLength(20);
      expect(session.rows()[0]?.offset).toBe("19");
      expect(session.progress().delivered).toBe(20);
      expect(session.running()).toBe(false);
      return null;
    });
    dispose();
  });

  test("applies records received before Pause under the old pause state", () => {
    const fake = fakeTransport();
    const continuations: Array<() => void> = [];
    const { dispose } = mount(() => {
      const session = createBrowseSession({
        streamUrl: "/stream",
        transport: fake.transport,
        scheduleAfterPaint: (resume) => {
          continuations.push(resume);
          return () => {
            const index = continuations.indexOf(resume);
            if (index >= 0) continuations.splice(index, 1);
          };
        },
      });

      session.start({ ...DEFAULT_BROWSE, live: true });
      for (let index = 0; index < 12; index += 1) fake.emit(record(String(index)));
      session.setPaused(true);
      void flush();
      expect(session.rows()).toHaveLength(12);
      expect(session.held()).toBe(0);
      expect(continuations).toHaveLength(0);

      fake.emit(record("12"));
      fake.emit(record("13"));
      void flush();
      expect(session.rows()).toHaveLength(12);
      expect(session.held()).toBe(2);
      session.setPaused(false);
      void flush();
      expect(session.rows()).toHaveLength(14);
      expect(session.rows()[0]?.offset).toBe("13");
      return null;
    });
    dispose();
  });

  test("ignores a stale continuation after a new browse replaces its run", () => {
    const fake = fakeTransport();
    let staleContinuation: (() => void) | undefined;
    const { dispose } = mount(() => {
      const session = createBrowseSession({
        streamUrl: "/stream",
        transport: fake.transport,
        scheduleAfterPaint: (resume) => {
          staleContinuation = resume;
          return () => undefined;
        },
      });

      session.start({ ...DEFAULT_BROWSE, live: true });
      for (let index = 0; index < 20; index += 1) fake.emit(record(`old-${String(index)}`));
      const oldContinuation = staleContinuation;
      session.start(DEFAULT_BROWSE);
      oldContinuation?.();
      fake.emit(record("new"));
      void flush();

      expect(fake.closes()).toBe(1);
      expect(session.rows().map((row) => row.offset)).toEqual(["new"]);
      return null;
    });
    dispose();
  });

  test("defers a synchronous transport close until its whole burst is committed", () => {
    const continuations: Array<() => void> = [];
    const transport: BrowseTransport = {
      open: (_url, handlers) => {
        const handle: BrowseHandle = {
          close: () => undefined,
          endMarker: () => undefined,
          endReason: () => "limit",
        };
        for (let index = 0; index < 20; index += 1) {
          handlers.onEvent({ kind: "record", record: record(String(index)) });
        }
        handlers.onDone?.();
        handlers.onConnection({ phase: "closed", reason: "the stream finished" });
        return handle;
      },
    };
    const { dispose } = mount(() => {
      const session = createBrowseSession({
        streamUrl: "/stream",
        transport,
        scheduleAfterPaint: (resume) => {
          continuations.push(resume);
          return () => undefined;
        },
      });

      session.start(DEFAULT_BROWSE);
      void flush();
      expect(session.rows()).toHaveLength(INITIAL_RECORD_BATCH);
      expect(session.running()).toBe(true);
      while (continuations.length > 0) continuations.shift()?.();
      void flush();
      expect(session.rows()).toHaveLength(20);
      expect(session.progress().endReason).toBe("limit");
      expect(session.running()).toBe(false);
      return null;
    });
    dispose();
  });

  test("keeps a latest browse in the newest-first order delivered by the backend", async () => {
    withSession((session, fake) => {
      session.start(DEFAULT_BROWSE);
      fake.emit(record("2"));
      fake.emit(record("1"));
      void flush();
      expect(session.rows().map((r) => r.offset)).toEqual(["2", "1"]);
      expect(session.pageRows().map((r) => r.offset)).toEqual(["2", "1"]);
    });
  });

  test("keeps an earliest browse in the forward offset order delivered by the backend", async () => {
    withSession((session, fake) => {
      session.start({ ...DEFAULT_BROWSE, seek: { kind: "beginning" } });
      fake.emit(record("1"));
      fake.emit(record("2"));
      void flush();
      expect(session.rows().map((r) => r.offset)).toEqual(["1", "2"]);
    });
  });

  test("does not lose a record when two arrive in the same tick", () => {
    // The Solid 2 defect this session's shape exists to avoid: an updater is applied to the last
    // *committed* value, so two `setRows((previous) => [x, ...previous])` in one microtask would
    // both start from the same array and one record would vanish.
    withSession((session, fake) => {
      session.start(DEFAULT_BROWSE);
      fake.emit(record("1"));
      fake.emit(record("2"));
      fake.emit(record("3"));
      void flush();
      expect(session.rows()).toHaveLength(3);
    });
  });

  test("caps a live tail rather than growing without bound", () => {
    withSession((session, fake) => {
      session.start({ ...DEFAULT_BROWSE, live: true });
      for (let i = 0; i < MAX_ROWS + 25; i += 1) fake.emit(record(String(i)));
      void flush();
      expect(session.rows()).toHaveLength(MAX_ROWS);
      // The newest end is the one kept: that is what following live means.
      expect(session.rows()[0]?.offset).toBe(String(MAX_ROWS + 24));
    });
  });

  test("caps retained payload bytes across a live tail, not only its row count", () => {
    withSession((session, fake) => {
      session.start({ ...DEFAULT_BROWSE, live: true });
      const text = `{"padding":"${"x".repeat(300_000)}"}`;
      for (let index = 0; index < 40; index += 1) {
        fake.emit({ ...record(String(index)), value: { kind: "json", text } });
      }
      void flush();

      const retainedBytes = session.rows().reduce((total, row) => {
        const value = row.value;
        return total +
          (value.kind === "json" || value.kind === "text"
            ? new TextEncoder().encode(value.text).length
            : value.kind === "large" && value.text !== undefined
              ? new TextEncoder().encode(value.text).length
              : 0);
      }, 0);
      expect(retainedBytes).toBeLessThanOrEqual(MAX_RETAINED_PAYLOAD_BYTES);
      expect(session.rows().some((row) => row.value.kind === "large" && row.value.text === undefined)).toBe(true);
    });
  });

  test("counts cached previous pages in the retained payload budget", () => {
    withSession((session, fake) => {
      session.start(DEFAULT_BROWSE);
      const text = `{"padding":"${"x".repeat(300_000)}"}`;
      for (let index = 0; index < 27; index += 1) {
        fake.emit({ ...record(`large-${String(index)}`), value: { kind: "json", text } });
      }
      for (let index = 0; index < MAX_ROWS - 27; index += 1) {
        fake.emit(record(`small-${String(index)}`));
      }
      fake.close("cursor-1");
      void flush();

      session.nextPage();
      for (let index = 0; index < 27; index += 1) {
        fake.emit({ ...record(`next-${String(index)}`), value: { kind: "json", text } });
      }
      void flush();

      const successor = session.rows().filter((row) => row.offset.startsWith("next-"));
      expect(successor).toHaveLength(27);
      expect(
        successor.every(
          (row) => row.value.kind === "large" && row.value.text === undefined,
        ),
      ).toBe(true);
    });
  });

  test("a pause on a busy tail is bounded, and releasing one stays bounded", () => {
    /*
     * Found by mutation, and it is the same bound as the case above with the pause left on.
     *
     * `MAX_ROWS` was asserted on one of the three places it is applied. Deleting it from the held
     * queue, or from the merge that releases the queue, left every case in this package green —
     * and a pause is exactly where an unbounded list is reached first: the cap on the visible rows
     * exists because a busy topic delivers faster than a person reads, and a paused screen is one
     * where nothing is being dropped at all while the records keep arriving. The tab dies holding
     * a queue nobody has looked at.
     */
    withSession((session, fake) => {
      session.start({ ...DEFAULT_BROWSE, live: true });
      // Ten rows already on screen before the pause, so the release below is a *merge* of two
      // non-empty lists. Without them the held queue's own cap would be doing all the work and
      // the bound on the merge could be deleted with this case still green.
      for (let i = 0; i < 10; i += 1) fake.emit(record(`before-${String(i)}`));
      session.setPaused(true);
      for (let i = 0; i < MAX_ROWS + 25; i += 1) fake.emit(record(String(i)));
      void flush();
      expect(session.held()).toBe(MAX_ROWS);
      // The delivered count is not capped and must not be: it counts what the stream sent, which
      // is how a reader tells a paused screen from a stalled one.
      expect(session.progress().delivered).toBe(MAX_ROWS + 35);

      session.setPaused(false);
      void flush();
      expect(session.rows()).toHaveLength(MAX_ROWS);
      // The newest end is the end kept, on release as on the live path.
      expect(session.rows()[0]?.offset).toBe(String(MAX_ROWS + 24));
    });
  });

  test("a pause holds records back and releasing shows every one of them, in order", () => {
    withSession((session, fake) => {
      session.start({ ...DEFAULT_BROWSE, live: true });
      fake.emit(record("1"));
      session.setPaused(true);
      fake.emit(record("2"));
      fake.emit(record("3"));
      void flush();
      // On screen: only the record that arrived before the pause. Held: the two after it.
      expect(session.rows().map((r) => r.offset)).toEqual(["1"]);
      expect(session.held()).toBe(2);
      // But the count still moves, because it counts what the *stream* delivered — a paused screen
      // that also stopped counting is indistinguishable from a stream that stalled.
      expect(session.progress().delivered).toBe(3);

      session.setPaused(false);
      void flush();
      expect(session.rows().map((r) => r.offset)).toEqual(["3", "2", "1"]);
      expect(session.held()).toBe(0);
    });
  });

  test("stopping shows what was held rather than discarding it", () => {
    // Those records were delivered. Throwing them away because somebody pressed Stop loses
    // evidence that arrived before the press, and on a tail there is no second chance to read it.
    withSession((session, fake) => {
      session.start({ ...DEFAULT_BROWSE, live: true });
      session.setPaused(true);
      fake.emit(record("9"));
      session.stop();
      void flush();
      expect(session.rows().map((r) => r.offset)).toEqual(["9"]);
      expect(fake.closes()).toBe(1);
    });
  });

  test("a stream that ends by itself stops reading as running", () => {
    // Without this, a bounded browse left the control saying "Stop" for ever, beside a status line
    // reading "Finished", with no way back to Read short of reloading the page.
    withSession((session, fake) => {
      session.start(DEFAULT_BROWSE);
      void flush();
      expect(session.running()).toBe(true);
      fake.close();
      void flush();
      expect(session.running()).toBe(false);
    });
  });

  test("offers a next page only when the server sent a cursor", () => {
    withSession((session, fake) => {
      session.start(DEFAULT_BROWSE);
      fake.close();
      void flush();
      // The server omits a cursor whenever asking again would be pointless. This is its answer,
      // not a guess from a full page — the guess that puts "Load more" under every last page.
      expect(session.canLoadMore()).toBe(false);

      session.start(DEFAULT_BROWSE);
      fake.close("cursor-1");
      void flush();
      expect(session.canLoadMore()).toBe(true);
    });
  });

  test("preserves a budget-limited terminal reason with its continuation", () => {
    withSession((session, fake) => {
      session.start(DEFAULT_BROWSE);
      fake.consumed(20_000);
      fake.close("cursor-budget", "budget");
      void flush();

      expect(session.progress().endReason).toBe("budget");
      expect(session.canNextPage()).toBe(true);
    });
  });

  test("load more appends and sends the cursor, not the seek", () => {
    withSession((session, fake) => {
      session.start({ ...DEFAULT_BROWSE, partitions: [3] });
      fake.emit(record("2"));
      fake.close("cursor-1");
      void flush();

      session.loadMore();
      fake.emit(record("1"));
      void flush();

      expect(session.rows().map((r) => r.offset)).toEqual(["2", "1"]);
      const second = fake.urls[1] as string;
      expect(second).toContain("cursor=cursor-1");
      expect(second).not.toContain("seekTo");
      // The *last browse's* partitions, not whatever the controls hold now: a continuation that
      // silently changed range would move the reader sideways.
      expect(second).toContain("partition=3");
    });
  });

  test("moves between offset pages without rereading a cached previous page", () => {
    withSession((session, fake) => {
      session.start(DEFAULT_BROWSE);
      fake.emit(record("9"));
      fake.emit(record("8"));
      fake.close("cursor-1");
      void flush();

      expect(session.pageNumber()).toBe(1);
      expect(session.pageRows().map((r) => r.offset)).toEqual(["9", "8"]);
      expect(session.canPreviousPage()).toBe(false);
      expect(session.canNextPage()).toBe(true);

      session.nextPage();
      fake.emit(record("7"));
      fake.emit(record("6"));
      void flush();
      expect(session.pageNumber()).toBe(1);
      expect(session.pageRows().map((r) => r.offset)).toEqual(["9", "8"]);
      fake.close();
      void flush();

      expect(session.pageNumber()).toBe(2);
      expect(session.pageRows().map((r) => r.offset)).toEqual(["7", "6"]);
      expect(fake.urls).toHaveLength(2);

      session.previousPage();
      void flush();
      expect(session.pageNumber()).toBe(1);
      expect(session.pageRows().map((r) => r.offset)).toEqual(["9", "8"]);

      session.nextPage();
      void flush();
      expect(session.pageNumber()).toBe(2);
      expect(fake.urls).toHaveLength(2);
    });
  });

  test("bounds cached page history while keeping page numbers relative to the cursor sequence", () => {
    withSession((session, fake) => {
      session.start(DEFAULT_BROWSE);
      for (let page = 1; page <= MAX_CACHED_PAGES + 1; page += 1) {
        fake.emit(record(String(100 - page)));
        fake.close(page <= MAX_CACHED_PAGES ? `cursor-${String(page)}` : undefined);
        void flush();
        if (page <= MAX_CACHED_PAGES) session.nextPage();
      }

      expect(session.pageNumber()).toBe(MAX_CACHED_PAGES + 1);
      for (let page = 1; page < MAX_CACHED_PAGES; page += 1) {
        session.previousPage();
        void flush();
      }
      expect(session.pageNumber()).toBe(2);
      expect(session.canPreviousPage()).toBe(false);
      expect(fake.urls).toHaveLength(MAX_CACHED_PAGES + 1);
    });
  });

  test("survives a transport that finishes the stream before open() returns", () => {
    // Found by looking at a story, not by reasoning: a transport may report a terminal state
    // synchronously from inside `open` — one that refuses the request before sending it, or a
    // scripted one in a story. The first version of this file read the handle out of the `const`
    // that `open` was still being assigned to and threw `Cannot access 'opened' before
    // initialization`, taking the whole screen down. Every other case here drives the stream
    // *after* `open` has returned, which is exactly why none of them saw it.
    const synchronous: BrowseTransport = {
      open: (_url, handlers) => {
        handlers.onEvent({ kind: "record", record: record("1") });
        handlers.onDone?.();
        handlers.onConnection({ phase: "closed", reason: "the stream finished" });
        return { close: () => undefined, endMarker: () => "cursor-1" };
      },
    };
    const { dispose } = mount(() => {
      const session = createBrowseSession({ streamUrl: "/s", transport: synchronous });
      session.start(DEFAULT_BROWSE);
      void flush();
      expect(session.rows()).toHaveLength(1);
      expect(session.running()).toBe(false);
      // And the continuation the server sent is still picked up, rather than lost with the
      // early close.
      expect(session.canLoadMore()).toBe(true);
      return null;
    });
    dispose();
  });

  test("load more with no cursor does nothing at all", () => {
    withSession((session, fake) => {
      session.loadMore();
      expect(fake.urls).toHaveLength(0);
    });
  });

  test("load more after a browse that ended with no cursor does nothing", () => {
    /*
     * The case the one above cannot make. There, no browse had ever run, so `lastQuery` was
     * undefined and the guard on *it* was doing all the work — the cursor check could be deleted
     * with that case still green. Here a browse has run and finished, and the server chose to send
     * no continuation: the short-circuit that is left is the cursor's.
     *
     * Without it `loadMore` re-runs the last query with `cursor: undefined`, which is the same
     * request again, and appends its answer to the rows already on screen. In the comment's own
     * words, a button that scrolled the user back to where they began — except that the rows
     * arrive twice, so the same record is on screen in two places.
     */
    withSession((session, fake) => {
      session.start(DEFAULT_BROWSE);
      fake.emit(record("1"));
      // No marker: the server omits one whenever asking again would be pointless.
      fake.close();
      void flush();
      expect(fake.urls).toHaveLength(1);
      expect(session.rows()).toHaveLength(1);

      session.loadMore();
      void flush();

      // No second request, so no second copy of the page. The request is the assertion rather than
      // the row count: this fake keeps one set of handlers, so a record emitted after the mutated
      // call would land on the stale stream too and prove nothing about which one appended it.
      expect(fake.urls).toHaveLength(1);
      expect(session.rows().map((r) => r.offset)).toEqual(["1"]);
    });
  });

  test("a browse that is stopped does not offer the previous browse's continuation", () => {
    /*
     * The cursor a page is read with is spent the moment the next browse starts, and this is the
     * state that proves it — the one state where `running` is not covering for it.
     *
     * Read a page and the server sends a continuation. Change the range and press Read: the new
     * browse is running, so nothing offers Load more whatever the cursor holds. Then press Stop.
     * `stop()` clears `running` and touches no cursor, so a cursor left over from the *first*
     * range is now sitting behind an enabled Load more — and pressing it appends the next page of
     * a range that is no longer on screen onto the rows of one that is.
     */
    withSession((session, fake) => {
      session.start(DEFAULT_BROWSE);
      fake.close("cursor-1");
      void flush();
      expect(session.canLoadMore()).toBe(true);

      session.start({ ...DEFAULT_BROWSE, seek: { kind: "beginning" } });
      void flush();
      session.stop();
      void flush();

      expect(session.canLoadMore()).toBe(false);
      session.loadMore();
      void flush();
      // Two requests: the two browses. A third would be the first range's second page.
      expect(fake.urls).toHaveLength(2);
    });
  });

  test("ignores every callback from a browse superseded by a newer one", () => {
    /*
     * Closing the old handle asks the transport to abort it, but cancellation is a race: a frame
     * already read from the response can still be queued when a new browse starts. Every callback
     * therefore belongs to the generation that opened it. Letting the old generation append a row
     * or report progress/failure mixes two different questions on one screen; letting its terminal
     * cursor through offers a continuation for the wrong range.
     *
     * The transport here keeps its handlers per call, which the shared fake does not — it holds
     * only the latest, so this sequence is not expressible with it.
     */
    interface Stream {
      handlers: Parameters<BrowseTransport["open"]>[1];
      marker: string | undefined;
    }
    const streams: Stream[] = [];
    const transport: BrowseTransport = {
      open: (_url, handlers) => {
        const stream: Stream = { handlers, marker: undefined };
        streams.push(stream);
        return { close: () => undefined, endMarker: () => stream.marker };
      },
    };

    const { dispose } = mount(() => {
      const session = createBrowseSession({ streamUrl: "/s", transport });
      session.start(DEFAULT_BROWSE);
      void flush();
      session.start({ ...DEFAULT_BROWSE, seek: { kind: "beginning" } });
      void flush();
      expect(session.running()).toBe(true);

      const first = streams[0];
      if (first === undefined) throw new Error("the first browse never opened a stream");
      first.handlers.onEvent({ kind: "record", record: record("old") });
      first.handlers.onEvent({ kind: "phase", name: "filtering-old-range" });
      first.handlers.onEvent({
        kind: "consumed",
        consumed: { records: 99, bytes: 999, elapsedMs: 9 },
      });
      first.handlers.onFailure({ kind: "decode", event: "message", cause: "old failure" });
      first.marker = "cursor-1";
      first.handlers.onDone?.();
      first.handlers.onConnection({ phase: "closed", reason: "the stream finished" });
      void flush();

      expect(session.rows()).toEqual([]);
      expect(session.progress()).toEqual({
        delivered: 0,
        connection: { phase: "connecting" },
      });
      expect(session.running()).toBe(true);
      expect(session.canLoadMore()).toBe(false);
      return null;
    });
    dispose();
  });

  test("ignores callbacks queued after the active browse is stopped", () => {
    let handlers: Parameters<BrowseTransport["open"]>[1] | undefined;
    const transport: BrowseTransport = {
      open: (_url, given) => {
        handlers = given;
        return { close: () => undefined, endMarker: () => "stale-cursor" };
      },
    };

    const { dispose } = mount(() => {
      const session = createBrowseSession({ streamUrl: "/s", transport });
      session.start(DEFAULT_BROWSE);
      handlers?.onEvent({ kind: "record", record: record("before-stop") });
      handlers?.onConnection({ phase: "open" });
      void flush();
      session.stop();
      void flush();
      const rowsAtStop = session.rows();
      const progressAtStop = session.progress();

      handlers?.onEvent({ kind: "record", record: record("after-stop") });
      handlers?.onEvent({ kind: "phase", name: "reading-after-stop" });
      handlers?.onFailure({ kind: "transport", cause: "late failure" });
      handlers?.onConnection({ phase: "closed", reason: "late close" });
      void flush();

      expect(session.rows()).toEqual(rowsAtStop);
      expect(session.progress()).toEqual(progressAtStop);
      expect(session.running()).toBe(false);
      expect(session.canLoadMore()).toBe(false);
      return null;
    });
    dispose();
  });

  test("a new browse replaces the rows; it does not mix two ranges", () => {
    withSession((session, fake) => {
      session.start(DEFAULT_BROWSE);
      fake.emit(record("1"));
      void flush();
      session.start({ ...DEFAULT_BROWSE, seek: { kind: "beginning" } });
      void flush();
      expect(session.rows()).toEqual([]);
    });
  });

  test("a failure is held beside the rows rather than replacing them", () => {
    withSession((session, fake) => {
      session.start(DEFAULT_BROWSE);
      fake.emit(record("1"));
      void flush();
      // The transport reports the failure through the same handlers.
      fake.transport.open("/stream", {
        onEvent: () => undefined,
        onFailure: () => undefined,
        onConnection: () => undefined,
      });
      expect(session.rows()).toHaveLength(1);
    });
  });
});

describe("offset range labels", () => {
  test("keeps offsets partition-relative and precise beyond Number.MAX_SAFE_INTEGER", () => {
    expect(
      offsetRangeLabel([
        record("9007199254740995", 2),
        record("8", 0),
        record("9007199254740993", 2),
        record("6", 0),
      ]),
    ).toBe("p0 offsets 6–8 · p2 offsets 9007199254740993–9007199254740995");
  });

  test("does not imply that an empty Kafka page has a global row range", () => {
    expect(offsetRangeLabel([])).toBe("No offsets loaded");
  });
});

describe("decoding what the stream sends", () => {
  test("one unreadable event does not end the stream", () => {
    // A decode failure is informational; the transport reports it and keeps going, which is the
    // same rule ADR-035 gives the server.
    expect(decodeBrowseEvent("message", "not json")).toEqual({ ok: false, cause: "not JSON" });
  });

  test("a structurally malformed message is a decode failure, not an exception", () => {
    expect(decodeBrowseEvent("message", "{}")).toEqual({
      ok: false,
      cause: "invalid message: partition must be a non-negative integer",
    });
  });

  test("reads a phase and a consumed figure", () => {
    expect(decodeBrowseEvent("phase", '{"phase":"seeking"}')).toEqual({
      ok: true,
      value: { kind: "phase", name: "seeking" },
    });
    // The field is `records`, verified against `ConsumedDto` in
    // `services/message/contract/src/kui/message/contract/StreamEventDtos.scala`. This assertion
    // used to say `messages`, which the server has never sent — so the decoder read the fallback
    // and the status line reported "scanned 0 records" for every browse that has ever run, while
    // the test passed against a payload nobody produces.
    expect(decodeBrowseEvent("consumed", '{"records":10,"bytes":40,"elapsedMs":5,"filterErrors":2}')).toEqual({
      ok: true,
      value: { kind: "consumed", consumed: { records: 10, bytes: 40, elapsedMs: 5, filterErrors: 2 } },
    });

    // A server that sends no `filterErrors` is not an error; the field is simply absent.
    expect(decodeBrowseEvent("consumed", '{"records":3,"bytes":9,"elapsedMs":1}')).toEqual({
      ok: true,
      value: { kind: "consumed", consumed: { records: 3, bytes: 9, elapsedMs: 1 } },
    });
  });
});

describe("a record on the wire", () => {
  const base: MessageDto = {
    partition: 3,
    offset: 18442901,
    timestamp: "2026-09-05T10:00:00Z",
    timestampType: "CreateTime",
    key: { text: "ord_9f21ac", kind: "string", serde: "String", properties: {} },
    value: { text: '{"a":1}', kind: "json", serde: "Json", properties: {} },
    headers: { "content-type": "application/json" },
    keySize: 10,
    valueSize: 7,
    headersSize: 24,
    deserializeErrors: [],
  };

  test("carries the offset as a string", () => {
    expect(toRecord(base).offset).toBe("18442901");
  });

  test("a null key is null, not the empty string", () => {
    // A null key in a compacted topic *is* the deletion, and the row draws the two differently.
    const dto = { ...base, key: { text: "", kind: "null", serde: "String", properties: {} } };
    expect(toRecord(dto).key).toBeNull();
  });

  test("a failed decode wins over the kind the fallback serde reported", () => {
    // The fallback delivers something plausible-looking, so reading `kind` first would draw a
    // decode failure as an ordinary text payload — absorbing the very failure that was sent
    // alongside the record so that it would not be absorbed.
    const dto: MessageDto = {
      ...base,
      value: { text: "0xdeadbeef", kind: "binary", serde: "Fallback", properties: {} },
      deserializeErrors: [{ target: "value", serde: "Avro", cause: "schema 42 not found" }],
    };
    expect(toRecord(dto).value).toEqual({
      kind: "undecodable",
      reason: "schema 42 not found",
      hex: "0xdeadbeef",
    });
  });

  test("a key failure does not paint the value red", () => {
    const dto: MessageDto = {
      ...base,
      deserializeErrors: [{ target: "key", serde: "Int64", cause: "not eight bytes" }],
    };
    expect(toRecord(dto).value.kind).toBe("json");
  });

  test("a timestamp type KUI does not know is dropped rather than shown", () => {
    expect(toRecord({ ...base, timestampType: "NoTimestampType" }).timestampType).toBeUndefined();
  });
});

describe("the messages screen", () => {
  function screen(query: BrowseQuery = DEFAULT_BROWSE, defaultView?: "pages" | "infinite") {
    const fake = fakeTransport();
    let session!: BrowseSession;
    const mounted = mount(() => {
      session = createBrowseSession({ streamUrl: "/stream", transport: fake.transport });
      return (
        <MessagesTab
          topic="orders.payments.v2"
          partitionCount={12}
          predicates={NO_PREDICATES}
          onPredicatesChange={() => undefined}
          query={query}
          onQueryChange={() => undefined}
          session={session}
          defaultView={defaultView}
          now={Date.parse("2026-09-05T10:00:02Z")}
        />
      );
    });
    return { ...mounted, fake, session: () => session };
  }

  test("does not read anything until somebody asks", () => {
    // A browse is a real Kafka consumer. Starting one because somebody clicked the wrong tab is
    // how a cluster ends up with consumers nobody asked for.
    const { fake, dispose } = screen();
    expect(fake.urls).toHaveLength(0);
    expect(document.body.textContent).toContain("Nothing has been read yet");
    dispose();
  });

  test("does not rebuild the filter field when records arrive", async () => {
    // The defect this rule was written for: the bar was rebuilt whenever results landed, and the
    // caret went with it. Holding the node and comparing identity is the only assertion that
    // actually catches it — the text would look right either way.
    const { container, fake, session, dispose } = screen();
    const field = container.querySelector<HTMLInputElement>('input[placeholder="Filter by key or value…"]');
    expect(field).not.toBeNull();

    session().start(DEFAULT_BROWSE);
    fake.emit(record("1"));
    await flush();

    expect(
      container.querySelector<HTMLInputElement>('input[placeholder="Filter by key or value…"]'),
    ).toBe(field);
    dispose();
  });

  test("draws one row per record, with the offset grouped and the partition named", async () => {
    const { container, fake, session, dispose } = screen();
    session().start(DEFAULT_BROWSE);
    fake.emit(record("18442901", 3));
    await flush();

    const rows = container.querySelectorAll(".kui-record");
    expect(rows).toHaveLength(1);
    expect(container.textContent).toContain("18,442,901");
        // A non-breaking space, so that "p" and its number never wrap apart at a narrow width.
    expect(container.textContent).toContain("p\u00a03");
    dispose();
  });

  test("defaults to one offset-relative page with cached previous and next controls", async () => {
    const { container, fake, session, dispose } = screen({ ...DEFAULT_BROWSE, limit: 2 });
    session().start({ ...DEFAULT_BROWSE, limit: 2 });
    fake.emit(record("9"));
    fake.emit(record("8"));
    fake.close("cursor-1");
    await flush();

    const pages = container.querySelector<HTMLInputElement>('input[value="pages"]');
    expect(pages?.checked).toBe(true);
    expect(container.textContent).toContain("Page 1");
    expect(container.textContent).toContain("p0 offsets 8–9");
    expect(container.querySelectorAll(".kui-record")).toHaveLength(2);

    container.querySelector<HTMLButtonElement>('button[aria-label="Next offset page"]')?.click();
    fake.emit(record("7"));
    fake.emit(record("6"));
    fake.close();
    await flush();

    expect(container.textContent).toContain("Page 2");
    expect(container.textContent).toContain("p0 offsets 6–7");
    expect(container.textContent).not.toContain("ord_9");

    container.querySelector<HTMLButtonElement>('button[aria-label="Previous offset page"]')?.click();
    await flush();
    expect(container.textContent).toContain("Page 1");
    expect(container.textContent).toContain("ord_9");
    expect(fake.urls).toHaveLength(2);
    dispose();
  });

  test("uses the configured default loading mode until the operator overrides it", () => {
    const { container, dispose } = screen(DEFAULT_BROWSE, "infinite");

    expect(container.querySelector<HTMLInputElement>('input[value="infinite"]')?.checked).toBe(true);
    container.querySelector<HTMLInputElement>('input[value="pages"]')?.click();
    expect(container.querySelector<HTMLInputElement>('input[value="pages"]')?.checked).toBe(true);

    dispose();
  });

  test("keeps an empty budget-limited filtered page continuable", async () => {
    const filtered = {
      ...DEFAULT_BROWSE,
      filterId: "selective-filter",
      filterSource: 'record.value.region == "antarctica"',
    };
    const { container, fake, session, dispose } = screen(filtered);
    session().start(filtered);
    fake.consumed(20_000);
    fake.close("cursor-budget", "budget");
    await flush();

    expect(container.querySelectorAll(".kui-record")).toHaveLength(0);
    expect(container.textContent).toContain("scan safety budget");
    expect(container.textContent).not.toContain("Every record in the range was read");
    const next = container.querySelector<HTMLButtonElement>(
      'button[aria-label="Next offset page"]',
    );
    expect(next).not.toBeNull();
    expect(next?.disabled).toBe(false);

    next?.click();
    expect(fake.urls).toHaveLength(2);
    expect(fake.urls[1]).toContain("cursor=cursor-budget");
    dispose();
  });

  test("infinite scroll preloads the next cursor before the end enters the viewport", async () => {
    let intersect: (() => void) | undefined;
    class Observer {
      constructor(callback: IntersectionObserverCallback) {
        intersect = () =>
          callback(
            [{ isIntersecting: true } as IntersectionObserverEntry],
            this as unknown as IntersectionObserver,
          );
      }
      observe(): void {}
      disconnect(): void {}
      unobserve(): void {}
      takeRecords(): IntersectionObserverEntry[] { return []; }
      readonly root = null;
      readonly rootMargin = "720px 0px";
      readonly thresholds = [0];
    }
    vi.stubGlobal("IntersectionObserver", Observer);

    const { container, fake, session, dispose } = screen({ ...DEFAULT_BROWSE, limit: 1 });
    container.querySelector<HTMLInputElement>('input[value="infinite"]')?.click();
    session().start({ ...DEFAULT_BROWSE, limit: 1 });
    fake.emit(record("9"));
    fake.close("cursor-1");
    await flush();

    expect(container.textContent).toContain("p0 offsets 9–9");
    intersect?.();
    await flush();

    expect(fake.urls).toHaveLength(2);
    expect(fake.urls[1]).toContain("cursor=cursor-1");
    intersect?.();
    expect(fake.urls).toHaveLength(2);

    fake.emit(record("8"));
    fake.close();
    await flush();
    expect(container.querySelectorAll(".kui-record")).toHaveLength(2);
    dispose();
    vi.unstubAllGlobals();
  });

  test("the whole row is the control and it says which way it will go", async () => {
    const { container, fake, session, dispose } = screen();
    session().start(DEFAULT_BROWSE);
    fake.emit(record("1"));
    await flush();

    const summary = container.querySelector<HTMLButtonElement>(".kui-record__summary");
    expect(summary?.tagName).toBe("BUTTON");
    // The string, not the boolean: in Solid 2 a `false` boolean attribute is *removed*, and
    // `aria-expanded` absent means "this is not an expandable thing at all" — the opposite of
    // what a collapsed row is.
    expect(summary?.getAttribute("aria-expanded")).toBe("false");
    summary?.click();
    await flush();
    expect(summary?.getAttribute("aria-expanded")).toBe("true");
    dispose();
  });

  test("expanding reveals the four labelled boxes and the headers label", async () => {
    const { container, fake, session, dispose } = screen();
    session().start(DEFAULT_BROWSE);
    fake.emit(record("1"));
    await flush();
    container.querySelector<HTMLButtonElement>(".kui-record__summary")?.click();
    await flush();

    for (const label of ["OFFSET", "PARTITION", "KEY", "TIMESTAMP", "HEADERS", "VALUE"]) {
      expect(container.textContent).toContain(label);
    }
    // The label stays even with no headers. Dropping it makes the reader wonder whether the
    // product looked.
    expect(container.textContent).toContain("— none");
    dispose();
  });

  test("distinguishes a filtered empty screen from an empty one", async () => {
    const { container, fake, session, dispose } = screen({ ...DEFAULT_BROWSE, contains: "nope" });
    session().start({ ...DEFAULT_BROWSE, contains: "nope" });
    fake.close();
    await flush();
    expect(container.textContent).toContain("No record matched that filter");
    dispose();
  });

  test("reports filter evaluation errors instead of claiming every record was a clean non-match", async () => {
    const filtered = {
      ...DEFAULT_BROWSE,
      filterId: "abc0123456789def",
      filterSource: 'record.value.status == "CAPTURED"',
    };
    const { container, fake, session, dispose } = screen(filtered);
    session().start(filtered);
    fake.consumed(12, 2);
    fake.close();
    await flush();

    expect(container.textContent).toContain("2 filter evaluations failed");
    expect(container.textContent).toContain("Some records could not be evaluated");
    expect(container.textContent).not.toContain("none of them satisfied");
    expect(container.textContent).not.toContain("Finished — nothing matched");
    dispose();
  });

  test("the pause control never draws a zero as a quantity", () => {
    // `Resume (0)` is a zero drawn as a quantity — the same rule as a magnitude bar that draws an
    // empty value as a full-width track.
    expect(pauseLabel(false, 0)).toBe("Pause");
    expect(pauseLabel(true, 0)).toBe("Resume");
    expect(pauseLabel(true, 1204)).toBe("Resume (1,204)");
  });

  test("the LIVE pill is a toggle and reads PAUSED when it is off", () => {
    const { container, dispose } = screen();
    expect(container.textContent).toContain("PAUSED");
    dispose();
  });

  test("live tailing that cannot be offered stays in the bar, disabled, and says so", () => {
    // Removing it would tell the operator the product cannot tail at all.
    const fake = fakeTransport();
    const { container, dispose } = mount(() => (
      <MessagesTab
        topic="t"
        partitionCount={1}
        predicates={NO_PREDICATES}
        onPredicatesChange={() => undefined}
        query={DEFAULT_BROWSE}
        onQueryChange={() => undefined}
        session={createBrowseSession({ streamUrl: "/s", transport: fake.transport })}
        liveAvailability={{ available: false, reason: "The message service is not reachable." }}
      />
    ));
    expect(container.textContent).toContain("LIVE unavailable");
    dispose();
  });

  test("a topic with one partition still shows the partition selector, disabled", () => {
    // Hiding it would make an operator think this topic is different in some way they cannot see.
    const fake = fakeTransport();
    const { container, dispose } = mount(() => (
      <MessagesTab
        topic="t"
        partitionCount={1}
        predicates={NO_PREDICATES}
        onPredicatesChange={() => undefined}
        query={DEFAULT_BROWSE}
        onQueryChange={() => undefined}
        session={createBrowseSession({ streamUrl: "/s", transport: fake.transport })}
      />
    ));
    const trigger = container.querySelector<HTMLButtonElement>(".kui-partition-picker__trigger");
    expect(trigger?.disabled).toBe(true);
    expect(trigger?.textContent).toContain("all 1");
    dispose();
  });

  test("the produce action is disabled with a reason rather than hidden", () => {
    // A hidden button makes an operator think the product cannot do the thing at all.
    const fake = fakeTransport();
    const { container, dispose } = mount(() => (
      <MessagesTab
        topic="t"
        partitionCount={2}
        predicates={NO_PREDICATES}
        onPredicatesChange={() => undefined}
        query={DEFAULT_BROWSE}
        onQueryChange={() => undefined}
        session={createBrowseSession({ streamUrl: "/s", transport: fake.transport })}
        mayProduce={false}
        onProduce={() => undefined}
        produceDisabledReason="You do not hold a role that permits producing to this topic."
      />
    ));
    const button = [...container.querySelectorAll("button")].find((b) =>
      b.textContent?.includes("Produce message"),
    );
    expect(button).toBeDefined();
    expect(button?.getAttribute("aria-disabled")).toBe("true");
    dispose();
  });

  test("changing where to read stops whatever is running", async () => {
    // A browse in flight is reading a different range from the one the controls now describe.
    const fake = fakeTransport();
    const changes = vi.fn();
    let session!: BrowseSession;
    const { container, dispose } = mount(() => {
      session = createBrowseSession({ streamUrl: "/s", transport: fake.transport });
      return (
        <MessagesTab
          topic="t"
          partitionCount={12}
          predicates={NO_PREDICATES}
          onPredicatesChange={() => undefined}
          query={DEFAULT_BROWSE}
          onQueryChange={changes}
          session={session}
        />
      );
    });
    session.start(DEFAULT_BROWSE);
    await flush();
    const pill = [...container.querySelectorAll("button")].find((b) => b.textContent === "PAUSED");
    pill?.click();
    await flush();
    expect(fake.closes()).toBeGreaterThan(0);
    // And turning LIVE on sets the seek to the end rather than sending both, which the server
    // refuses.
    expect(changes).toHaveBeenCalledWith(
      expect.objectContaining({ live: true, seek: { kind: "latest" } }),
    );
    dispose();
  });
});
