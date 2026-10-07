import { afterEach, expect, it, vi } from "vitest";
import { createApiClient } from "./client.js";
import { createCsrfTokens } from "./csrf.js";
import { FallbackBootstrap } from "./bootstrap.js";

afterEach(() => vi.useRealTimers());

it("does not invalidate a refreshed session when an old request returns 401 late", async () => {
  const csrf = createCsrfTokens(); csrf.settle("old");
  const expired = vi.fn();
  let reply!: (response: Response) => void;
  const client = createApiClient({ bootstrap: FallbackBootstrap, origin: "https://example.test", csrf,
    onUnauthorized: expired,
    fetch: () => new Promise<Response>((resolve) => { reply = resolve; }),
  });
  const request = client.get("/api/v1/capabilities");
  await new Promise((resolve) => setTimeout(resolve, 0));
  csrf.settle("fresh");
  reply(Response.json({ code: "KUI-UNAUTHENTICATED", message: "old session expired" }, { status: 401 }));
  await request;
  expect(csrf.currentToken()).toBe("fresh");
  expect(expired).not.toHaveBeenCalled();
});

it.each([
  ["https://api.test/kafka/api/v1/", "https://api.test/kafka/api/v1/capabilities"],
  ["/proxy/api/v1/", "https://example.test/proxy/api/v1/capabilities"],
  ["/api/v1", "https://example.test/api/v1/capabilities"],
])("normalizes %s for ordinary requests and streaming URLs", async (apiBase, expected) => {
  const csrf = createCsrfTokens();
  csrf.settle("token");
  let sent = "";
  const client = createApiClient({
    bootstrap: { ...FallbackBootstrap, apiBase }, origin: "https://example.test", csrf,
    fetch: async (request) => { sent = request.url; return Response.json({}); },
  });
  await client.get("/api/v1/capabilities");
  expect(sent).toBe(expected);
  expect(client.url("/api/v1/capabilities")).toBe(expected);
});

it.each([200, 503])("bounds a stalled %s response body, not just headers", async (status) => {
  vi.useFakeTimers();
  const csrf = createCsrfTokens();
  csrf.settle("token");
  let signal: AbortSignal | undefined;
  const client = createApiClient({
    bootstrap: FallbackBootstrap, origin: "https://example.test", csrf, requestTimeoutMs: 50,
    fetch: async (request) => {
      signal = request.signal;
      return new Response(new ReadableStream({ start() {} }), { status, headers: { "Content-Type": "application/json" } });
    },
  });
  let result: unknown;
  void client.get("/api/v1/capabilities").then((answer) => { result = answer; });
  await vi.advanceTimersByTimeAsync(51);
  expect(result).toEqual({ ok: false, error: { kind: "timeout" } });
  expect(signal?.aborted).toBe(true);
});
