import { describe, expect, it, vi } from "vitest";
import { createRoot, flush } from "solid-js";

import {
  backoff,
  backoffFor,
  openEventSourceWith,
  openFetchStreamWith,
  type EventSourceLike,
  type SseError,
  type SseSubscriber,
  type StreamResponse,
  type StreamTransport,
} from "./stream.js";

/** An `EventSource` a test drives by hand. jsdom has none, which is why the wrapper takes one. */
function fakeSource(): EventSourceLike & {
  emit(name: string, data?: string): void;
  closed: boolean;
  state: number;
} {
  const listeners = new Map<string, Array<(event: Event) => void>>();
  return {
    closed: false,
    state: 0,
    get readyState(): number {
      return this.state;
    },
    addEventListener(name: string, handler: (event: Event) => void): void {
      const existing = listeners.get(name) ?? [];
      existing.push(handler);
      listeners.set(name, existing);
    },
    close(): void {
      this.closed = true;
    },
    emit(name: string, data?: string): void {
      const event =
        data === undefined ? new Event(name) : new MessageEvent(name, { data });
      for (const handler of listeners.get(name) ?? []) handler(event);
    },
  };
}

/** A subscriber that records everything, and decodes JSON with a `value` field. */
function recorder(events: readonly string[] = ["row"]) {
  const values: string[] = [];
  const errors: SseError[] = [];
  const subscriber: SseSubscriber<string> = {
    events,
    decode: (_event, data) => {
      try {
        const parsed: unknown = JSON.parse(data);
        const value = (parsed as { value?: unknown }).value;
        if (typeof value !== "string") return { ok: false, cause: "no string 'value' field" };
        return { ok: true, value };
      } catch (cause) {
        return { ok: false, cause: String(cause) };
      }
    },
    onEvent: (value) => {
      values.push(value);
    },
    onError: (error) => {
      errors.push(error);
    },
  };
  return { values, errors, subscriber };
}

const ENVELOPE = JSON.stringify({
  code: "KUI-UPSTREAM-UNAVAILABLE",
  message: "the cluster is not answering",
  correlationId: "abc123",
  retryable: true,
});

describe("a stream over EventSource", () => {
  it("delivers decoded events and swallows heartbeats, which still prove the connection is alive", () => {
    const source = fakeSource();
    const { values, errors, subscriber } = recorder();

    createRoot((dispose) => {
      const handle = openEventSourceWith(() => source, subscriber);
      source.emit("open");
      flush();
      expect(handle.connection().phase).toBe("open");

      source.emit("row", '{"value":"first"}');
      source.emit("heartbeat", "{}");
      source.emit("row", '{"value":"second"}');
      flush();

      expect(values).toEqual(["first", "second"]);
      expect(errors).toEqual([]);
      // A heartbeat is not an event anybody asked for, and it is not a failure either.
      expect(handle.connection()).toEqual({ phase: "open" });
      dispose();
    });
  });

  it("reports a frame it cannot decode and keeps the stream running", () => {
    const source = fakeSource();
    const { values, errors, subscriber } = recorder();

    createRoot((dispose) => {
      const handle = openEventSourceWith(() => source, subscriber);
      source.emit("open");
      source.emit("row", "{ this is not json");
      source.emit("row", '{"value":"the next one is fine"}');
      flush();

      expect(errors).toHaveLength(1);
      expect(errors[0]).toMatchObject({ kind: "decode", event: "row" });
      expect(values).toEqual(["the next one is fine"]);
      // The rule the whole module exists for: one bad frame must not end a stream that is
      // otherwise delivering good ones.
      expect(handle.connection().phase).toBe("open");
      dispose();
    });
  });

  it("ends on the server's error event and reports the envelope", () => {
    const source = fakeSource();
    const { errors, subscriber } = recorder();

    createRoot((dispose) => {
      const handle = openEventSourceWith(() => source, subscriber);
      source.emit("open");
      source.emit("error", ENVELOPE);
      flush();

      expect(errors).toHaveLength(1);
      expect(errors[0]).toEqual({
        kind: "server",
        error: {
          kind: "envelope",
          code: "KUI-UPSTREAM-UNAVAILABLE",
          message: "the cluster is not answering",
          details: [],
          correlationId: "abc123",
          retryable: true,
        },
      });
      expect(handle.connection()).toEqual({
        phase: "closed",
        reason: "the server sent an error event",
      });
      expect(source.closed).toBe(true);
      dispose();
    });
  });

  it("delivers nothing after a terminal event", () => {
    const source = fakeSource();
    const { values, subscriber } = recorder();

    createRoot((dispose) => {
      const handle = openEventSourceWith(() => source, subscriber);
      source.emit("open");
      source.emit("done");
      source.emit("row", '{"value":"too late"}');
      flush();

      expect(values).toEqual([]);
      // The reason of the *first* close is the one that stands.
      expect(handle.connection()).toEqual({ phase: "closed", reason: "the stream finished" });
      dispose();
    });
  });

  it("tells a dropped connection that will be retried from one that will not", () => {
    const source = fakeSource();
    const { subscriber } = recorder();

    createRoot((dispose) => {
      const handle = openEventSourceWith(() => source, subscriber);
      source.emit("open");

      // An "error" DOM event with no data is a transport failure, not the server's error event.
      source.state = 0;
      source.emit("error");
      flush();
      expect(handle.connection()).toEqual({ phase: "reconnecting", attempt: 1 });

      source.emit("error");
      flush();
      expect(handle.connection()).toEqual({ phase: "reconnecting", attempt: 2 });

      source.state = 2;
      source.emit("error");
      flush();
      expect(handle.connection()).toEqual({
        phase: "closed",
        reason: "the connection was lost and will not be retried",
      });
      dispose();
    });
  });

  it("closes once, however many times close is called", () => {
    const source = fakeSource();
    const { subscriber } = recorder();
    const spy = vi.spyOn(source, "close");

    createRoot((dispose) => {
      const handle = openEventSourceWith(() => source, subscriber);
      handle.close();
      handle.close();
      flush();

      expect(spy).toHaveBeenCalledTimes(1);
      expect(handle.connection()).toEqual({ phase: "closed", reason: "closed by the client" });
      dispose();
    });
  });

  it("refuses to listen for an event name every stream already handles", () => {
    const { subscriber } = recorder(["done"]);
    expect(() => openEventSourceWith(() => fakeSource(), subscriber)).toThrow(/must not be listed/);
  });

  it("closes itself if the connection goes silent for too long, rather than reading 'open' forever", () => {
    vi.useFakeTimers();
    try {
      const source = fakeSource();
      const { errors, subscriber } = recorder();

      createRoot((dispose) => {
        const handle = openEventSourceWith(() => source, subscriber);
        source.emit("open");
        flush();
        expect(handle.connection().phase).toBe("open");

        // A heartbeat shortly before the deadline keeps the connection alive.
        vi.advanceTimersByTime(40_000);
        source.emit("heartbeat", "{}");
        flush();
        expect(handle.connection().phase).toBe("open");

        // But once nothing arrives at all for that long, the connection is presumed wedged.
        vi.advanceTimersByTime(45_000);
        flush();

        expect(handle.connection()).toEqual({ phase: "closed", reason: "no heartbeat received" });
        expect(errors).toEqual([{ kind: "transport", cause: "stream went silent" }]);
        expect(source.closed).toBe(true);
        dispose();
      });
    } finally {
      vi.useRealTimers();
    }
  });
});

/** A response whose chunks the test hands over one at a time. */
function fakeResponse(status: number, body = ""): StreamResponse & {
  push(chunk: string): void;
  finish(): void;
  fail(): void;
} {
  let handlers:
    | { onChunk: (chunk: string) => void; onDone: () => void; onFailure: () => void }
    | undefined;
  return {
    status,
    text: () => Promise.resolve(body),
    readChunks(next) {
      handlers = next;
    },
    push(chunk: string) {
      handlers?.onChunk(chunk);
    },
    finish() {
      handlers?.onDone();
    },
    fail() {
      handlers?.onFailure();
    },
  };
}

/**
 * A transport whose request never resolves until the test says so, and whose failure is the one
 * `fetch` actually produces on the teardown path: the browser rejects the in-flight promise with an
 * `AbortError` the moment the `AbortSignal` fires. `fakeTransport` below cannot reach that branch
 * at all — its send promise is already settled — which is why the rule guarding it went unnoticed.
 */
function abortingTransport(): StreamTransport & { failSend: () => void; isAborted: boolean } {
  let fail: ((cause: unknown) => void) | undefined;
  const pending = new Promise<StreamResponse>((_resolve, reject) => {
    fail = reject;
  });
  return {
    isAborted: false,
    send: () => pending,
    abort(): void {
      this.isAborted = true;
    },
    aborted(): boolean {
      return this.isAborted;
    },
    failSend(): void {
      fail?.(new Error("AbortError: The user aborted a request."));
    },
  };
}

function fakeTransport(response: Promise<StreamResponse>): StreamTransport & { isAborted: boolean } {
  return {
    isAborted: false,
    send: () => response,
    abort(): void {
      this.isAborted = true;
    },
    aborted(): boolean {
      return this.isAborted;
    },
  };
}

describe("a stream over fetch", () => {
  it("bounds connection establishment even when send never settles", async () => {
    vi.useFakeTimers();
    try {
      const transport = abortingTransport();
      const { errors, subscriber } = recorder();
      const handle = openFetchStreamWith(transport, subscriber);
      await vi.advanceTimersByTimeAsync(45_000);
      flush();
      expect(handle.connection().phase).toBe("closed");
      expect(errors).toEqual([{ kind: "transport", cause: "stream connection timed out" }]);
      expect(transport.isAborted).toBe(true);
      transport.failSend();
      await Promise.resolve();
      expect(errors).toHaveLength(1);
    } finally { vi.useRealTimers(); }
  });

  it.each(["fail", "finish"] as const)("reports body %s without done as a terminal transport failure", async (ending) => {
    const response = fakeResponse(200);
    const transport = fakeTransport(Promise.resolve(response));
    const { errors, subscriber } = recorder();
    const handle = openFetchStreamWith(transport, subscriber);
    await Promise.resolve();
    response[ending]();
    flush();
    expect(errors).toHaveLength(1);
    expect(errors[0]?.kind).toBe("transport");
    expect(handle.connection().phase).toBe("closed");
    response.fail();
    expect(errors).toHaveLength(1);
    expect(transport.isAborted).toBe(true);
  });

  it("ignores a rejection body arriving after the client closed", async () => {
    let resolve!: (body: string) => void;
    const response = { ...fakeResponse(403), text: () => new Promise<string>((done) => { resolve = done; }) };
    const { errors, subscriber } = recorder();
    const handle = openFetchStreamWith(fakeTransport(Promise.resolve(response)), subscriber);
    await Promise.resolve();
    handle.close();
    resolve(ENVELOPE);
    await Promise.resolve();
    expect(errors).toEqual([]);
  });

  it("parses the wire format and carries the continuation cursor off the done event", async () => {
    const response = fakeResponse(200);
    const transport = fakeTransport(Promise.resolve(response));
    const { values, subscriber } = recorder();
    const done: string[] = [];
    const subscriberWithDone = Object.assign(subscriber, {
      onDone: (data: string) => done.push(data),
    });

    await createRoot(async (dispose) => {
      const handle = openFetchStreamWith(transport, subscriberWithDone);
      await Promise.resolve();
      flush();

      // Split across chunk boundaries, because that is what a network does.
      response.push('event: row\ndata: {"value":"one"}\n\nevent: hea');
      response.push(
        'rtbeat\ndata: {}\n\nevent: done\nid: eyJ2Ijox\ndata: {"reason":"budget","cursor":"eyJ2Ijox"}\n\n',
      );
      flush();

      expect(values).toEqual(["one"]);
      expect(done).toEqual(['{"reason":"budget","cursor":"eyJ2Ijox"}']);
      expect(handle.endMarker()).toBe("eyJ2Ijox");
      expect(handle.connection()).toEqual({ phase: "closed", reason: "the stream finished" });
      dispose();
    });
  });

  it("reports a stream the server rejected rather than one that finished normally", async () => {
    const response = fakeResponse(403, JSON.stringify({ code: "KUI-FORBIDDEN", message: "no" }));
    const transport = fakeTransport(Promise.resolve(response));
    const { errors, subscriber } = recorder();

    await createRoot(async (dispose) => {
      const handle = openFetchStreamWith(transport, subscriber);
      await Promise.resolve();
      await Promise.resolve();
      await Promise.resolve();
      flush();

      // Without the status check the body — which has no `data:` lines — parses to nothing, the
      // reader reaches the end, and a 403 is indistinguishable from an empty stream.
      expect(errors).toEqual([
        {
          kind: "server",
          error: {
            kind: "envelope",
            code: "KUI-FORBIDDEN",
            message: "no",
            details: [],
            correlationId: "",
            retryable: false,
          },
        },
      ]);
      expect(handle.connection()).toEqual({
        phase: "closed",
        reason: "the server rejected the stream with 403",
      });
      dispose();
    });
  });

  it("does not report an abort as a failure, and aborts the request when it is closed", async () => {
    const response = fakeResponse(200);
    const transport = fakeTransport(Promise.resolve(response));
    const { errors, subscriber } = recorder();

    await createRoot(async (dispose) => {
      const handle = openFetchStreamWith(transport, subscriber);
      await Promise.resolve();
      flush();

      handle.close();
      // Solid 2 batches writes into a microtask, so a signal written by `close` still reads as its
      // previous value until the queue drains. `flush()` is how a test asks for that synchronously.
      flush();
      // The chain this exists for: the abort cancels the gateway's stream, which cancels the
      // service's fiber and closes its Kafka consumer.
      expect(transport.isAborted).toBe(true);
      expect(handle.connection()).toEqual({ phase: "closed", reason: "closed by the client" });

      // Whatever the body does afterwards is not news.
      response.fail();
      flush();
      expect(errors).toEqual([]);
      expect(handle.connection()).toEqual({ phase: "closed", reason: "closed by the client" });
      dispose();
    });
  });

  it("a client-initiated close is not reported as a transport failure", async () => {
    const transport = abortingTransport();
    const { errors, subscriber } = recorder();

    await createRoot(async (dispose) => {
      const handle = openFetchStreamWith(transport, subscriber);
      await Promise.resolve();
      flush();

      // The user navigates away, or the browse is stopped. `close()` aborts the request, and the
      // abort is what makes the request the browser had in flight reject.
      handle.close();
      flush();
      transport.failSend();
      await Promise.resolve();
      await Promise.resolve();
      flush();

      // The rejection is the *consequence* of the close, not a failure to report: without the
      // `aborted()` check every ordinary close raises a transport error at the subscriber, and a
      // message browser the operator closed on purpose would offer to retry a connection that was
      // never broken.
      expect(errors).toEqual([]);
      expect(handle.connection()).toEqual({ phase: "closed", reason: "closed by the client" });
      dispose();
    });
  });

  it("reports a connection that could never be established", async () => {
    const transport = fakeTransport(Promise.reject(new Error("network down")));
    const { errors, subscriber } = recorder();

    await createRoot(async (dispose) => {
      const handle = openFetchStreamWith(transport, subscriber);
      await Promise.resolve();
      await Promise.resolve();
      flush();

      expect(errors).toEqual([{ kind: "transport", cause: "Error: network down" }]);
      expect(handle.connection()).toEqual({
        phase: "closed",
        reason: "the connection could not be established",
      });
      dispose();
    });
  });

  it("keeps a decode failure from ending the stream here too", async () => {
    const response = fakeResponse(200);
    const transport = fakeTransport(Promise.resolve(response));
    const { values, errors, subscriber } = recorder();

    await createRoot(async (dispose) => {
      const handle = openFetchStreamWith(transport, subscriber);
      await Promise.resolve();
      flush();

      response.push("event: row\ndata: not json\n\n");
      response.push('event: row\ndata: {"value":"still here"}\n\n');
      flush();

      expect(errors).toHaveLength(1);
      expect(values).toEqual(["still here"]);
      expect(handle.connection().phase).toBe("open");
      dispose();
    });
  });

  it("closes itself and aborts the request if the body goes silent for too long", async () => {
    vi.useFakeTimers();
    try {
      const response = fakeResponse(200);
      const transport = fakeTransport(Promise.resolve(response));
      const { errors, subscriber } = recorder();

      await createRoot(async (dispose) => {
        const handle = openFetchStreamWith(transport, subscriber);
        await vi.advanceTimersByTimeAsync(0);
        flush();

        // A chunk shortly before the deadline keeps the connection alive.
        await vi.advanceTimersByTimeAsync(40_000);
        response.push('event: heartbeat\ndata: {}\n\n');
        flush();
        expect(handle.connection().phase).toBe("open");

        // But once nothing arrives at all for that long, the connection is presumed wedged.
        await vi.advanceTimersByTimeAsync(45_000);
        flush();

        expect(handle.connection()).toEqual({ phase: "closed", reason: "no heartbeat received" });
        expect(errors).toEqual([{ kind: "transport", cause: "stream went silent" }]);
        expect(transport.isAborted).toBe(true);
        dispose();
      });
    } finally {
      vi.useRealTimers();
    }
  });
});

describe("reconnection backoff", () => {
  it("starts almost immediately and has a low ceiling", () => {
    expect([1, 2, 3, 4, 9].map(backoffFor)).toEqual([1000, 2000, 5000, 10_000, 10_000]);
  });

  it("only ever shortens the wait, so the ceiling stays a ceiling", () => {
    // Without jitter every browser that lost the same gateway reconnects in the same millisecond,
    // and the gateway that just came back up is knocked over by its own clients.
    expect(backoff(1, () => 0)).toBe(800);
    expect(backoff(1, () => 1)).toBe(1000);
    expect(backoff(4, () => 0.5)).toBe(9000);
  });
});
