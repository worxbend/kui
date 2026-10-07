import { afterEach, expect, it, vi } from "vitest";
import { createRoot, flush } from "solid-js";
import { createBrowseSession } from "./session.js";
import { createBrowseTransport } from "./transport.js";
import { DEFAULT_BROWSE } from "./browse.js";

const settle = async () => {
  for (let i = 0; i < 30; i++) { await Promise.resolve(); await flush(); }
};
afterEach(() => vi.unstubAllGlobals());

it.each(["limit", "budget", "exhausted"])("completes a real %s done frame and continues only with its cursor", async (reason) => {
  const fetcher = vi.fn(async (_url: string) => new Response(
    `id: next-page-cursor\nevent: done\ndata: ${JSON.stringify({ reason })}\n\n`,
    { headers: { "Content-Type": "text/event-stream" } },
  ));
  vi.stubGlobal("fetch", fetcher);
  let dispose!: () => void;
  const session = createRoot((cleanup) => {
    dispose = cleanup;
    return createBrowseSession({ streamUrl: "/api/v1/messages/stream", transport: createBrowseTransport() });
  });
  try {
    session.start(DEFAULT_BROWSE);
    await settle();
    expect(session.running()).toBe(false);
    expect(session.progress().failure).toBeUndefined();
    expect(session.progress().endReason).toBe(reason);
    expect(session.canLoadMore()).toBe(true);
    session.loadMore();
    await settle();
    expect(fetcher).toHaveBeenCalledTimes(2);
    expect(String(fetcher.mock.calls[1]?.[0])).toContain("cursor=next-page-cursor");
  } finally { session.stop(); dispose(); }
});

it.each([
  ['event: done\ndata: {"reason":"exhausted"}\n\n', false],
  ['', true],
  ['event: error\ndata: {"code":"KUI-UNAVAILABLE","message":"lost upstream"}\n\n', true],
])("never continues a stream without a successful terminal cursor: %s", async (body, failed) => {
  vi.stubGlobal("fetch", vi.fn(async () => new Response(body)));
  let dispose!: () => void;
  const session = createRoot((cleanup) => {
    dispose = cleanup;
    return createBrowseSession({ streamUrl: "/api/v1/messages/stream", transport: createBrowseTransport() });
  });
  try {
    session.start(DEFAULT_BROWSE);
    await settle();
    expect(session.running()).toBe(false);
    expect(session.canLoadMore()).toBe(false);
    expect(session.progress().failure !== undefined).toBe(failed);
  } finally { session.stop(); dispose(); }
});
