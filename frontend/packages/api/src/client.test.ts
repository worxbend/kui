import { describe, expect, test, vi } from "vitest";

import { createApiClient } from "./client.js";
import { CsrfHeaderName, createCsrfTokens } from "./csrf.js";
import { ErrorCodes } from "./constants.generated.js";
import { FallbackBootstrap } from "./bootstrap.js";

/**
 * What the client must do on every request, and what it must never do to the page.
 *
 * Every test here corresponds to a defect this product has shipped, and the comment says which. A
 * test that is only "coverage" would be deleted the first time it was inconvenient; a test that
 * names the outage it prevents is not.
 */

/** Records what was sent, and answers with whatever the test asked for. */
function recordingFetch(reply: (request: Request) => Response): {
  fetch: (request: Request) => Promise<Response>;
  sent: Request[];
} {
  const sent: Request[] = [];
  return {
    sent,
    fetch: (request: Request) => {
      sent.push(request);
      return Promise.resolve(reply(request));
    },
  };
}

/** The gate, already released, with no token — an anonymous session. */
function settledTokens() {
  const tokens = createCsrfTokens();
  tokens.settle(undefined);
  return tokens;
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

/**
 * A client whose session has already been established.
 *
 * The default gate is *settled*, because every request other than `/auth/me` now waits for start-up
 * to finish before it is sent — see `isSessionCall` in the client for the seven-concurrent-sessions
 * defect that rule exists to prevent. A test that left it unsettled would not fail, it would sit
 * there for the gate's ten-second deadline and then time out, which says nothing about the thing
 * under test.
 */
function clientWith(
  transport: (request: Request) => Promise<Response>,
  csrf = settledTokens(),
  onUnauthorized?: () => void,
) {
  return createApiClient({
    bootstrap: FallbackBootstrap,
    origin: "https://kui.example.com",
    csrf,
    fetch: transport,
    ...(onUnauthorized ? { onUnauthorized } : {}),
  });
}

describe("the CSRF header", () => {
  test("a mutation carries the token under the name the gateway reads", async () => {
    // The defect: the browser sent `X-Kui-Csrf` while the gateway read `X-Csrf-Token`, so every
    // mutation came back 403 and looked like a permissions problem.
    const csrf = createCsrfTokens();
    csrf.settle("token-abc");
    const transport = recordingFetch(() => json({}));

    await clientWith(transport.fetch, csrf).post("/api/v1/clusters/{clusterId}/refresh", {
      params: { path: { clusterId: "local" } },
    });

    expect(transport.sent).toHaveLength(1);
    expect(transport.sent[0]?.headers.get(CsrfHeaderName)).toBe("token-abc");
    expect(CsrfHeaderName).toBe("X-Csrf-Token");
  });

  test("a GET carries no token, so a deep link works in a fresh tab", async () => {
    const csrf = createCsrfTokens();
    csrf.settle("token-abc");
    const transport = recordingFetch(() => json({ entries: [] }));

    await clientWith(transport.fetch, csrf).get("/api/v1/capabilities");

    expect(transport.sent[0]?.headers.get(CsrfHeaderName)).toBeNull();
  });

  test("a mutation issued before start-up finishes waits rather than being sent without a token", async () => {
    // The defect: nothing called `/auth/me`, the token stayed absent for the life of the page, and
    // every mutation was refused — including the "Retry now" button in the degraded-feature panel.
    const csrf = createCsrfTokens();
    const transport = recordingFetch(() => json({}));
    const client = clientWith(transport.fetch, csrf);

    const inFlight = client.post("/api/v1/clusters/{clusterId}/refresh", {
      params: { path: { clusterId: "local" } },
    });

    // Nothing has been sent: the request is parked on the gate, not on the network.
    await Promise.resolve();
    expect(transport.sent).toHaveLength(0);

    csrf.settle("late-token");
    await inFlight;

    expect(transport.sent).toHaveLength(1);
    expect(transport.sent[0]?.headers.get(CsrfHeaderName)).toBe("late-token");
  });

  test("a session with no token sends the request anyway, so the gateway can say why", async () => {
    const csrf = createCsrfTokens();
    csrf.settle(undefined);
    const transport = recordingFetch(() => json({}));

    await clientWith(transport.fetch, csrf).post("/api/v1/clusters/{clusterId}/refresh", {
      params: { path: { clusterId: "local" } },
    });

    expect(transport.sent).toHaveLength(1);
    expect(transport.sent[0]?.headers.get(CsrfHeaderName)).toBeNull();
  });

  test("an empty token is no token", async () => {
    // `X-Csrf-Token: ` is rejected exactly as a missing header is, but with a far more confusing
    // message in the gateway's log.
    const csrf = createCsrfTokens();
    csrf.settle("");
    expect(csrf.currentToken()).toBeUndefined();
  });
});

describe("failures are values, never exceptions", () => {
  test("a network failure answers with `unreachable` instead of rejecting", async () => {
    // The defect: an unhandled failure travelled through the reactive graph and took the
    // subscription with it, so the page went blank instead of showing "cannot reach the server".
    const client = clientWith(() => Promise.reject(new TypeError("Failed to fetch")));

    const answer = await client.get("/api/v1/capabilities");

    expect(answer.ok).toBe(false);
    if (!answer.ok) {
      expect(answer.error.kind).toBe("unreachable");
    }
  });

  test("an error envelope decodes to its stable code", async () => {
    const client = clientWith(() =>
      Promise.resolve(
        json(
          {
            code: ErrorCodes.ClusterNotFound,
            message: "no cluster 'nope'",
            details: [],
            correlationId: "cid-1",
            timestamp: "2026-09-05T10:00:00.000Z",
            retryable: false,
          },
          404,
        ),
      ),
    );

    const answer = await client.get("/api/v1/capabilities");

    expect(answer.ok).toBe(false);
    if (!answer.ok && answer.error.kind === "envelope") {
      expect(answer.error.code).toBe(ErrorCodes.ClusterNotFound);
      expect(answer.error.correlationId).toBe("cid-1");
      expect(answer.error.retryable).toBe(false);
    }
  });

  test("a body that is not an envelope becomes a decoding failure, not a crash", async () => {
    // A reverse proxy substituting an HTML error page for a JSON one is the usual source.
    const client = clientWith(() =>
      Promise.resolve(new Response("<html>502 Bad Gateway</html>", { status: 502 })),
    );

    const answer = await client.get("/api/v1/capabilities");

    expect(answer.ok).toBe(false);
    if (!answer.ok) expect(answer.error.kind).toBe("decoding");
  });

  test("a success status whose body is not JSON is a decoding failure, not `unreachable`", async () => {
    // Something answered, so the gateway is not unreachable: a same-origin proxy's sign-in page or
    // an SPA fallback served under /api. Reported as `unreachable`, three of these put up the
    // full-screen "KUI cannot reach the server" and sent the operator to check the network.
    const client = clientWith(() =>
      Promise.resolve(
        new Response("<!doctype html><title>Sign in</title>", {
          status: 200,
          headers: { "Content-Type": "text/html" },
        }),
      ),
    );

    const answer = await client.get("/api/v1/capabilities");

    expect(answer.ok).toBe(false);
    if (!answer.ok) expect(answer.error.kind).toBe("decoding");
  });

  test("an error status with an empty body is a failure, not a success with no value", async () => {
    // The defect: `openapi-fetch` answers a non-2xx with `Content-Length: 0` as `{ error: undefined }`,
    // and success was decided from `error` alone, so Netty's bare 503 during a gateway restart came
    // back as `ok(undefined)`. A read then threw on `undefined` inside a memo, and a sign-out or a
    // delete in that window reported `done` when nothing had happened.
    const client = clientWith(() =>
      Promise.resolve(new Response(null, { status: 503, headers: { "Content-Length": "0" } })),
    );

    const answer = await client.get("/api/v1/clusters");

    expect(answer.ok).toBe(false);
    if (!answer.ok) {
      expect(answer.error.kind).toBe("decoding");
      expect(answer.error.kind === "decoding" && answer.error.cause).toContain("503");
    }
  });
});

describe("the session", () => {
  test("a 401 is reported before the caller sees the failure, and clears the token", async () => {
    let expired = 0;
    const csrf = createCsrfTokens();
    csrf.settle("token-abc");
    const client = clientWith(
      () => Promise.resolve(json({ code: ErrorCodes.Unauthenticated, message: "gone" }, 401)),
      csrf,
      () => {
        expired += 1;
      },
    );

    await client.get("/api/v1/auth/me");

    expect(expired).toBe(1);
    expect(csrf.currentToken()).toBeUndefined();
  });
});

describe("the request itself", () => {
  test("path parameters are substituted and the deployment prefix is kept", async () => {
    const csrf = createCsrfTokens();
    csrf.settle("t");
    const transport = recordingFetch(() => json({}));

    await createApiClient({
      bootstrap: { basePath: "/kafka", apiBase: "/kafka/api/v1", buildVersion: "test" },
      origin: "https://tools.example.com",
      csrf,
      fetch: transport.fetch,
    }).post("/api/v1/clusters/{clusterId}/refresh", { params: { path: { clusterId: "local" } } });

    expect(transport.sent[0]?.url).toBe(
      "https://tools.example.com/kafka/api/v1/clusters/local/refresh",
    );
  });

  test("no X-Kui-* header is ever sent", async () => {
    // The gateway strips every inbound `X-Kui-*` header at the edge (ADR-040), so one the browser
    // sent would be discarded and appear in no log. The old client sent `X-Kui-Request-Id` and it
    // could never have been read.
    const transport = recordingFetch(() => json({ entries: [] }));

    await clientWith(transport.fetch).get("/api/v1/capabilities");

    const names = [...(transport.sent[0]?.headers.keys() ?? [])];
    expect(names.filter((name) => name.toLowerCase().startsWith("x-kui-"))).toEqual([]);
  });
});

describe("the session gate", () => {
  /**
   * The defect: the gateway mints an anonymous session for any API request arriving without a
   * cookie, and stamps `Set-Cookie` on the answer. The shell opened with seven cookieless requests
   * at once, so the gateway minted seven sessions, the browser kept whichever `Set-Cookie` landed
   * last, and the CSRF token the client kept belonged to whichever session answered `/auth/me`.
   * Every read worked; every write came back "X-Csrf-Token does not match the session's token".
   */
  test("holds every other call until the session has been established", async () => {
    const sent: string[] = [];
    const csrf = createCsrfTokens();
    const client = clientWith(async (request) => {
      sent.push(new URL(request.url).pathname);
      return json({});
    }, csrf);

    // Fired together, exactly as start-up fires them.
    const others = Promise.all([
      client.get("/api/v1/capabilities"),
      client.get("/api/v1/auth/settings"),
    ]);
    await Promise.resolve();
    await Promise.resolve();

    // Nothing has gone out: a second cookieless request would mint a second session.
    expect(sent).toEqual([]);

    csrf.settle("token-abc");
    await others;
    expect(sent).toContain("/api/v1/capabilities");
    expect(sent).toContain("/api/v1/auth/settings");
  });

  test("lets the session call itself through, or nothing would ever settle", async () => {
    const sent: string[] = [];
    // Deliberately never settled: `/auth/me` is what settles it, so it cannot wait for it.
    const client = clientWith(async (request) => {
      sent.push(new URL(request.url).pathname);
      return json({ csrfToken: "token-abc" });
    }, createCsrfTokens());

    await client.get("/api/v1/auth/me", {});
    expect(sent).toEqual(["/api/v1/auth/me"]);
  });

  test("does not exempt /auth/settings, which used to race /auth/me", async () => {
    // The two ran side by side in one `Promise.all`. That is the same defect in miniature: two
    // cookieless requests, two sessions, one surviving cookie.
    const sent: string[] = [];
    const client = clientWith(async (request) => {
      sent.push(new URL(request.url).pathname);
      return json({});
    }, createCsrfTokens());

    void client.get("/api/v1/auth/settings");
    await Promise.resolve();
    await Promise.resolve();
    expect(sent).toEqual([]);
  });
});

describe("a 401", () => {
  test("tears down the session for an ordinary call", async () => {
    const lapsed = vi.fn();
    const client = clientWith(
      async () => new Response(JSON.stringify({ code: "KUI-UNAUTHENTICATED" }), { status: 401 }),
      settledTokens(),
      lapsed,
    );
    await client.get("/api/v1/clusters", {});
    expect(lapsed).toHaveBeenCalled();
  });

  test("does not tear it down for the sign-in call itself", async () => {
    /*
     * A 401 from `/auth/login` means "those credentials are wrong", not "your session has ended",
     * and the difference shipped as a real defect. The shell's expired-session handler clears the
     * identity; `mustSignIn()` needs an identity to conclude the principal is anonymous. So a
     * *failed* sign-in cleared the identity, `mustSignIn()` fell to false, and the sign-in screen
     * vanished — leaving the anonymous application on screen, with no error, to somebody who had
     * just typed the wrong password.
     */
    const lapsed = vi.fn();
    const client = clientWith(
      async () => new Response(JSON.stringify({ code: "KUI-UNAUTHENTICATED" }), { status: 401 }),
      settledTokens(),
      lapsed,
    );
    await client.post("/api/v1/auth/login", { body: { username: "ada", password: "wrong" } });
    expect(lapsed).not.toHaveBeenCalled();
  });

  test("nor for a refused password change", async () => {
    // Same flow, same reasoning: the challenge was rejected, no session existed to lose.
    const lapsed = vi.fn();
    const client = clientWith(
      async () => new Response(JSON.stringify({ code: "KUI-UNAUTHENTICATED" }), { status: 401 }),
      settledTokens(),
      lapsed,
    );
    await client.post("/api/v1/auth/password", { body: { challenge: "x", newPassword: "y" } });
    expect(lapsed).not.toHaveBeenCalled();
  });
});

describe("the request deadline", () => {
  test("a request that never answers becomes a `timeout` failure, not a hang", async () => {
    // The defect `errors.ts` documented but never triggered: `ApiError`'s `timeout` case was fully
    // defined, yet nothing ever constructed one for a plain request, because no request-level
    // deadline existed. A stalled backend left the caller's promise — and whatever "loading" state
    // was waiting on it — unresolved for good.
    const client = createApiClient({
      bootstrap: FallbackBootstrap,
      origin: "https://kui.example.com",
      csrf: settledTokens(),
      requestTimeoutMs: 20,
      fetch: () => new Promise<Response>(() => {}),
    });

    const answer = await client.get("/api/v1/capabilities");

    expect(answer.ok).toBe(false);
    if (!answer.ok) expect(answer.error.kind).toBe("timeout");
  });
});
