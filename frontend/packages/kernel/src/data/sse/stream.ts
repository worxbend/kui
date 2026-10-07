/**
 * The browser half of KUI's streaming (ADR-035).
 *
 * Two wrappers, because the browser offers two mechanisms and neither can do the other's job:
 *
 * - {@link openEventSource} uses the native `EventSource`. It reconnects on its own, sends cookies,
 *   and survives a tab being backgrounded. It cannot `POST` and it cannot be given headers. Use it
 *   for `GET` streams — `/api/v1/capabilities/stream` is the one the shell has.
 * - {@link openFetchStream} reads a `fetch` response by hand. It can `POST`, it can carry headers,
 *   and it can be aborted, which the native object cannot. Use it when the stream needs a request
 *   body or when the user must be able to stop it — message browsing needs both.
 *
 * ## The rules both obey, each of which exists because of a shipped defect
 *
 * - **A decode failure skips the frame and the stream continues.** One record KUI cannot deserialise
 *   must not end a stream that is otherwise delivering good ones — the same rule ADR-035 gives the
 *   server.
 * - **An `error` event is terminal and is reported.** It carries an ordinary error envelope, so a
 *   failure after the headers were sent is handled by the same code as one before them.
 * - **A `heartbeat` is swallowed.** It carries `{}` and exists only to stop a proxy from reaping an
 *   idle connection. Surfacing it would make every caller filter it out, and one of them would
 *   forget. It does prove the connection is alive, so it re-asserts `open`.
 * - **A terminal event cannot be followed by more.** Once the connection reads `closed`, nothing
 *   further is emitted and the reason of the *first* close is the one that stands: "the server sent
 *   an error event" explains more than "the body ran out", which is what would arrive a moment later.
 * - **A stream the server rejected is reported, not rendered as one that finished normally.** `fetch`
 *   resolves for a 403 exactly as it does for a 200, and a body with no `data:` lines parses to
 *   nothing — so without the status check a refusal is indistinguishable from an empty stream.
 * - **Closing is idempotent and propagates.** `close()` aborts the request, which cancels the
 *   gateway's stream, which cancels the service's fiber and closes its Kafka consumer.
 */
import { decodeEnvelope, SseEventNames, type ApiError } from "@kui/api";
import { createSignal, type Accessor } from "solid-js";

import { EMPTY_PARSER_STATE, feed, type ParserState, type RawSseEvent } from "./parser.js";

/**
 * Where a stream is in its life.
 *
 * Rendered as the connection indicator on the capability banner, which is the only honest way to
 * tell a user that what they are looking at may be a few seconds out of date. A screen that silently
 * stops updating is worse than one that says it has stopped.
 */
export type SseConnection =
  | { readonly phase: "connecting" }
  | { readonly phase: "open" }
  /**
   * Between attempts. `attempt` counts from 1 and is shown, because "reconnecting" that has been
   * trying for twenty minutes means something different from one that started a second ago.
   */
  | { readonly phase: "reconnecting"; readonly attempt: number }
  /** Finished, and not coming back without somebody asking. */
  | { readonly phase: "closed"; readonly reason: string };

/** Why one event, or one stream, did not work out. */
export type SseError =
  /**
   * One event's payload was not what its decoder expected. The stream keeps running.
   */
  | { readonly kind: "decode"; readonly event: string; readonly cause: string }
  /** The connection itself failed. */
  | { readonly kind: "transport"; readonly cause: string }
  /**
   * The server's own terminal `error` event, or its refusal of the stream before it started —
   * deliberately the same case, carrying the same `ApiError` an ordinary request would have produced.
   */
  | { readonly kind: "server"; readonly error: ApiError };

/** What a caller does with a live stream. */
export interface SseHandle {
  /** What the transport is doing, for the indicator. */
  readonly connection: Accessor<SseConnection>;
  /** Stops the stream and releases the server's resources. Idempotent. */
  readonly close: () => void;
  /**
   * The `id:` on the terminal `done` event, when there was one.
   *
   * KUI puts the signed continuation cursor there (ADR-026, ADR-035), and it is what "load more"
   * sends back — so a stream that ended with one can be continued and a stream that ended without
   * one cannot, which is exactly what the server means by omitting it. It reads `undefined` until
   * the stream finishes, and for {@link openEventSource} it is always `undefined`: the native
   * `EventSource` keeps the last id to itself for its own reconnection and does not hand it over.
   */
  readonly endMarker: () => string | undefined;
}

/** How a caller receives what the stream delivers. */
export interface SseSubscriber<A> {
  /**
   * The data events to listen for.
   *
   * The shared events of ADR-035 are always handled and must not be listed. Listing one is a
   * programming error rather than a runtime one, so it is caught here.
   */
  readonly events: readonly string[];
  /** Turns `(eventName, data)` into a value, or says why it could not. */
  readonly decode: (event: string, data: string) => { ok: true; value: A } | { ok: false; cause: string };
  /** Called once per decoded event. */
  readonly onEvent: (value: A) => void;
  /**
   * Called for every failure. A `decode` failure is informational — the stream is still running —
   * while `server` and `transport` are terminal and the connection will read `closed`.
   */
  readonly onError: (error: SseError) => void;
  /**
   * Called for a `phase` frame, with its raw payload.
   *
   * `phase` is one of ADR-035's shared events, so a caller must not list it among `events` — but
   * unlike the other three it carries *data* the screen needs. The message browser's status line is
   * built from it: which partition is being scanned, how far through, whether the scan is seeking or
   * reading. Neither transport delivered it before this callback existed, so the frames arrived and
   * were dropped on the floor, and the status line was permanently blank with nothing to say why.
   *
   * It is separate from `onEvent` rather than folded into it because `decode` is keyed on the
   * caller's own event names; a shared event has no place in that switch, and callers that do not
   * care about progress should not have to handle it at all — hence optional.
   */
  readonly onPhase?: ((data: string) => void) | undefined;
  /**
   * Called with the terminal `done` frame's raw payload before the connection closes.
   *
   * The payload names why the server stopped (`limit`, `exhausted`, `budget`, or `cancelled`).
   * Most streams only need the connection state and can omit this callback; consumers that offer
   * continuation controls use it to distinguish a complete range from a safety-budget pause.
   */
  readonly onDone?: ((data: string) => void) | undefined;
}

/**
 * How long to wait before the *n*th reconnection attempt, before jitter.
 *
 * One second, two, five, then every ten. The shape matters more than the numbers: the first retry is
 * almost immediate, because most disconnections are a proxy timing out an idle connection and
 * reconnecting works instantly; the ceiling is low, because a stream is how the user finds out the
 * world changed, and making them wait a minute for it is worse than a little extra traffic.
 */
export function backoffFor(attempt: number): number {
  if (attempt <= 1) return 1000;
  if (attempt === 2) return 2000;
  if (attempt === 3) return 5000;
  return 10_000;
}

/**
 * The same, with up to 20% subtracted at random.
 *
 * Without it, every browser that lost the same gateway reconnects in the same millisecond, and the
 * gateway that just came back up is knocked over by its own clients. The jitter only ever shortens
 * the wait, so the ceiling above stays a ceiling.
 */
export function backoff(attempt: number, random: () => number = Math.random): number {
  return Math.round(backoffFor(attempt) * (0.8 + 0.2 * random()));
}

/**
 * The slice of the browser's `EventSource` the kernel actually uses.
 *
 * An interface rather than the DOM class, for one reason: jsdom — the fake document these suites run
 * against — has no `EventSource` at all, so a test could otherwise only be written against a real
 * browser. With this, everything the kernel does *around* the browser's object (routing named
 * events, turning readiness into a connection state, keeping the stream alive through a decode
 * failure) is testable without one, and only the browser's own behaviour is not.
 */
export interface EventSourceLike {
  addEventListener(name: string, handler: (event: Event) => void): void;
  close(): void;
  /** `0` connecting, `1` open, `2` closed — the constants `EventSource` defines. */
  readonly readyState: number;
}

const EVENT_SOURCE_CLOSED = 2;

/**
 * How long a stream may go without a byte before it is presumed wedged.
 *
 * The server sends a heartbeat every 15 idle seconds (`Sse.scala`'s `heartbeatInterval`) for the sole
 * purpose of proving liveness. Neither transport enforced that on the browser side: a connection whose
 * bytes simply stop arriving — a dropped network path, a middlebox that swallows packets without
 * resetting the socket — never fires `error`, so `readyState`/the abort signal never change and the
 * connection indicator was stuck on "open" forever, with no way out short of the user pressing Stop.
 * Three times the interval, so one merely slow heartbeat is not mistaken for a dead connection.
 */
const HEARTBEAT_WATCHDOG_MS = 45_000;

/** Subscribes to a `GET` stream through the browser's own `EventSource`. */
export function openEventSource<A>(url: string, subscriber: SseSubscriber<A>): SseHandle {
  return openEventSourceWith(
    () => new EventSource(url, { withCredentials: true }) as unknown as EventSourceLike,
    subscriber,
  );
}

/** {@link openEventSource} against a source a test supplies. */
export function openEventSourceWith<A>(
  open: () => EventSourceLike,
  subscriber: SseSubscriber<A>,
): SseHandle {
  rejectSharedNames(subscriber.events);

  const [connection, setConnection] = createSignal<SseConnection>(
    { phase: "connecting" },
    // The writers are network callbacks and a `close()` a component may call from an owned scope;
    // this is module-level state with one writer path, which is the narrow case the option is for.
    { ownedWrite: true },
  );
  const source = open();

  // Per handle, not per application: two streams that both lost their connection are each on their
  // own attempt, and sharing a counter would make one of them report the other's history.
  let attempts = 0;
  let closed = false;
  let watchdog: ReturnType<typeof setTimeout> | undefined;

  const clearWatchdog = (): void => {
    if (watchdog !== undefined) {
      clearTimeout(watchdog);
      watchdog = undefined;
    }
  };

  // Rearmed by every frame that proves the connection is still alive. If it ever fires, nothing has
  // arrived for three heartbeat intervals, so the connection is presumed wedged rather than left to
  // read "open" indefinitely.
  const armWatchdog = (): void => {
    clearWatchdog();
    watchdog = setTimeout(() => {
      subscriber.onError({ kind: "transport", cause: "stream went silent" });
      closeWith("no heartbeat received");
    }, HEARTBEAT_WATCHDOG_MS);
  };

  const closeWith = (reason: string): void => {
    if (closed) return;
    closed = true;
    clearWatchdog();
    source.close();
    setConnection({ phase: "closed", reason });
  };

  source.addEventListener("open", () => {
    if (!closed) {
      setConnection({ phase: "open" });
      armWatchdog();
    }
  });

  // `EventSource` reports both a transport failure and a server-sent event *named* `error` as a DOM
  // event of type "error". They are told apart by whether the event carries data: only a message
  // does. Getting this wrong either swallows the server's explanation or invents a disconnection.
  source.addEventListener(SseEventNames.Error, (event: Event) => {
    if (closed) return;
    const payload = payloadOf(event);
    if (payload !== undefined) {
      subscriber.onError(serverError(payload));
      closeWith("the server sent an error event");
      return;
    }
    // A dropped connection: `EventSource` is either already retrying or has given up, and
    // `readyState` is the only way to tell which.
    if (source.readyState === EVENT_SOURCE_CLOSED) {
      closeWith("the connection was lost and will not be retried");
    } else {
      attempts += 1;
      setConnection({ phase: "reconnecting", attempt: attempts });
    }
  });

  source.addEventListener(SseEventNames.Done, (event: Event) => {
    if (closed) return;
    try {
      subscriber.onDone?.(payloadOf(event) ?? "");
    } finally {
      closeWith("the stream finished");
    }
  });

  // Forwarded raw: `phase` is shared, so it never goes through `decode`, which is keyed on the
  // caller's own event names. See `SseSubscriber.onPhase`.
  source.addEventListener(SseEventNames.Phase, (event: Event) => {
    if (closed) return;
    armWatchdog();
    const payload = payloadOf(event);
    if (payload !== undefined) subscriber.onPhase?.(payload);
  });

  // Heartbeats are deliberately not forwarded: they carry `{}` and exist only to keep the
  // connection from being reaped. They do prove it is alive, which is why one re-asserts `open`
  // and rearms the watchdog above.
  source.addEventListener(SseEventNames.Heartbeat, () => {
    if (!closed) {
      setConnection({ phase: "open" });
      armWatchdog();
    }
  });

  for (const name of subscriber.events) {
    source.addEventListener(name, (event: Event) => {
      if (closed) return;
      armWatchdog();
      const payload = payloadOf(event);
      if (payload === undefined) return;
      deliver(subscriber, name, payload);
    });
  }

  return {
    connection,
    close: () => {
      closeWith("closed by the client");
    },
    endMarker: () => undefined,
  };
}

/** One request for {@link openFetchStream}. */
export interface StreamRequest {
  readonly url: string;
  readonly method?: string;
  readonly headers?: Readonly<Record<string, string>>;
  readonly body?: string;
}

/**
 * The slice of a `fetch` response {@link openFetchStream} uses.
 *
 * An interface rather than `Response` directly, for the same reason {@link EventSourceLike} exists:
 * jsdom implements neither `fetch` nor `ReadableStream`, so everything this module does *around* the
 * response — checking the status, decoding a rejection envelope, feeding chunks to the parser —
 * could otherwise only be tested in a real browser.
 */
export interface StreamResponse {
  /**
   * The HTTP status. `fetch` resolves for 4xx and 5xx exactly as it does for 200, so this is the
   * only thing that tells a stream the server accepted from one it rejected.
   */
  readonly status: number;
  /** The whole body as text. Used only on the rejection path, where the body is a small envelope. */
  text(): Promise<string>;
  /** Pulls the body a chunk at a time, already decoded from UTF-8. */
  readChunks(handlers: {
    readonly onChunk: (chunk: string) => void;
    readonly onDone: () => void;
    readonly onFailure: () => void;
  }): void;
}

/** How a stream is carried. Supplied by a test; defaulted to the browser's `fetch`. */
export interface StreamTransport {
  send(): Promise<StreamResponse>;
  abort(): void;
  aborted(): boolean;
}

/**
 * Subscribes to a stream over `fetch`, so that it can `POST` and can be stopped.
 *
 * `close()` aborts the request, which propagates all the way down: the gateway's stream is
 * cancelled, the service's fiber is cancelled and its consumer is closed (ADR-035). That chain is
 * the reason this exists at all — a user who navigates away from a message browser must not leave a
 * Kafka consumer running.
 *
 * There is no reconnection here. The native `EventSource` retries because it can safely replay a
 * `GET`; a `POST` cannot be replayed without asking whether it should be, and that is a decision for
 * the screen that started it, not for the transport.
 */
export function openFetchStream<A>(
  request: StreamRequest,
  subscriber: SseSubscriber<A>,
): SseHandle {
  const controller = new AbortController();
  return openFetchStreamWith(
    {
      send: async () => {
        const response = await fetch(request.url, {
          method: request.method ?? "GET",
          // The same reason the API client sets it: without it `fetch` omits the session cookie on
          // anything the browser considers cross-origin, which a reverse proxy is enough to cause.
          credentials: "include",
          headers: { ...request.headers },
          ...(request.body === undefined ? {} : { body: request.body }),
          signal: controller.signal,
        });
        return browserResponse(response);
      },
      abort: () => {
        controller.abort();
      },
      aborted: () => controller.signal.aborted,
    },
    subscriber,
  );
}

/** {@link openFetchStream} against a transport a test supplies. */
export function openFetchStreamWith<A>(
  transport: StreamTransport,
  subscriber: SseSubscriber<A>,
): SseHandle {
  rejectSharedNames(subscriber.events);

  const [connection, setConnection] = createSignal<SseConnection>(
    { phase: "connecting" },
    { ownedWrite: true },
  );
  let marker: string | undefined;
  let ended = false;
  let watchdog: ReturnType<typeof setTimeout> | undefined;

  const clearWatchdog = (): void => {
    if (watchdog !== undefined) {
      clearTimeout(watchdog);
      watchdog = undefined;
    }
  };

  // Rearmed by every chunk the body delivers, heartbeats included. If it ever fires, nothing has
  // arrived for three heartbeat intervals — the connection is presumed wedged rather than left to
  // read "open" indefinitely, which is all `fetch`'s reader would otherwise ever tell us.
  const armWatchdog = (): void => {
    clearWatchdog();
    watchdog = setTimeout(() => {
      fail("stream went silent", "no heartbeat received");
    }, HEARTBEAT_WATCHDOG_MS);
  };

  /**
   * Ends the stream, unless something already said why it ended — a `done` event or the client's own
   * `close()` is a better explanation than "the body ran out".
   */
  const end = (reason: string): void => {
    if (ended) return;
    ended = true;
    clearWatchdog();
    setConnection({ phase: "closed", reason });
    transport.abort();
  };

  const fail = (cause: string, reason = cause): void => {
    if (ended) return;
    try { subscriber.onError({ kind: "transport", cause }); }
    finally { end(reason); }
  };

  const handle = (raw: RawSseEvent): void => {
    if (ended) return;
    switch (raw.name) {
      case SseEventNames.Heartbeat:
        return;
      case SseEventNames.Phase:
        // Shared, and carrying data. Without this case the frame fell to `default`, where
        // `subscriber.events.includes("phase")` is false by construction — `rejectSharedNames`
        // forbids listing it — so every phase frame was silently discarded.
        subscriber.onPhase?.(raw.data);
        return;
      case SseEventNames.Done:
        // The cursor rides on the event's `id:`, which the parser has already carried forward. It is
        // recorded before the connection is closed, so a caller watching `connection` for the end
        // finds the marker already there rather than a tick later.
        marker = raw.id;
        try {
          subscriber.onDone?.(raw.data);
        } finally {
          end("the stream finished");
        }
        return;
      case SseEventNames.Error:
        subscriber.onError(serverError(raw.data));
        end("the server sent an error event");
        return;
      default:
        // An event this caller did not ask for is not a failure: ADR-035 lets a stream add events.
        if (subscriber.events.includes(raw.name)) deliver(subscriber, raw.name, raw.data);
    }
  };

  const accept = (response: StreamResponse): void => {
    if (ended) return;
    setConnection({ phase: "open" });
    armWatchdog();
    let state: ParserState = EMPTY_PARSER_STATE;
    response.readChunks({
      onChunk: (chunk) => {
        if (ended) return;
        armWatchdog();
        const fed = feed(state, chunk);
        state = fed.state;
        for (const event of fed.events) handle(event);
      },
      onDone: () => {
        fail("the server closed the stream without a done event");
      },
      onFailure: () => {
        fail("the stream ended unexpectedly");
      },
    });
  };

  /**
   * The server refused the stream before it started.
   *
   * ADR-035 says that is an ordinary HTTP error response carrying the ADR-034 envelope, so it is
   * reported as the same `server` failure a mid-stream `error` event produces. Without this the body
   * — which has no `data:` lines — parses to nothing, the reader reaches the end, and a 403 is
   * indistinguishable from a stream that ran and finished.
   */
  const reject = (response: StreamResponse): void => {
    const reason = `the server rejected the stream with ${response.status}`;
    response.text().then(
      (body) => {
        if (ended) return;
        subscriber.onError(serverError(body));
        end(reason);
      },
      () => {
        fail(reason);
      },
    );
  };

  // Covers both missing headers and a stalled HTTP rejection body. Acceptance replaces it
  // with the heartbeat watchdog; every terminal path clears it.
  watchdog = setTimeout(() => fail("stream connection timed out"), HEARTBEAT_WATCHDOG_MS);
  transport.send().then(
    (response) => {
      if (ended) return;
      if (response.status >= 200 && response.status < 300) accept(response);
      else reject(response);
    },
    (cause: unknown) => {
      // An abort is not a failure: it is what `close()` does, and the state is already closed.
      if (ended || transport.aborted()) return;
      fail(String(cause), "the connection could not be established");
    },
  );

  return {
    connection,
    close: () => {
      end("closed by the client");
    },
    endMarker: () => marker,
  };
}

// ---------------------------------------------------------------------------------------------

function deliver<A>(subscriber: SseSubscriber<A>, name: string, data: string): void {
  const decoded = subscriber.decode(name, data);
  if (decoded.ok) subscriber.onEvent(decoded.value);
  // Reported, and the stream carries on. This is the rule the whole module is built around.
  else subscriber.onError({ kind: "decode", event: name, cause: decoded.cause });
}

/** An `error` frame's body, as the same `ApiError` an ordinary failed request would have produced. */
function serverError(body: string): SseError {
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch (cause) {
    return {
      kind: "server",
      error: { kind: "decoding", cause: `the error event was not JSON: ${String(cause)}` },
    };
  }
  return { kind: "server", error: decodeEnvelope(parsed) };
}

/** The `data` of an event, when it has one. A transport error event does not. */
function payloadOf(event: Event): string | undefined {
  const data: unknown = (event as MessageEvent<unknown>).data;
  return typeof data === "string" ? data : undefined;
}

/**
 * A caller listing a shared event name is a mistake this catches at the moment it is made.
 *
 * Listing `done` among your data events means your decoder is asked to read the terminal frame, and
 * the stream then both ends and delivers a value — which is the kind of contradiction that shows up
 * as one duplicated row on screen, weeks later.
 */
function rejectSharedNames(events: readonly string[]): void {
  const shared = events.filter((name) =>
    ([SseEventNames.Phase, SseEventNames.Done, SseEventNames.Error, SseEventNames.Heartbeat] as readonly string[]).includes(name),
  );
  if (shared.length > 0) {
    throw new Error(
      `these event names are handled by every stream and must not be listed: ${shared.join(", ")}. ` +
        `A stream that needs the progress payload takes it through the subscriber's onPhase callback.`,
    );
  }
}

/** The real thing, wrapping the browser's `Response`. */
function browserResponse(response: Response): StreamResponse {
  return {
    status: response.status,
    text: () => response.text(),
    readChunks: ({ onChunk, onDone, onFailure }) => {
      const body = response.body;
      if (body === null) {
        onDone();
        return;
      }
      const reader = body.getReader();
      // `stream: true` so that a multi-byte character split across two network chunks is held back
      // rather than turned into a replacement character — which, in a JSON payload, would be a
      // decode failure for an event that was perfectly well formed on the wire.
      const decoder = new TextDecoder();

      const pump = (): void => {
        reader.read().then(
          ({ done, value }) => {
            if (done) {
              onDone();
              return;
            }
            onChunk(decoder.decode(value, { stream: true }));
            pump();
          },
          () => {
            onFailure();
          },
        );
      };

      pump();
    },
  };
}
