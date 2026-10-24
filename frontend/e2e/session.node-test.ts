/**
 * The sign-in check every case runs before its first protected read, against a stand-in gateway.
 *
 * Not a Playwright spec, deliberately: the suite's `globalSetup` refuses to run without a stack, and
 * what is tested here is the harness's answer to a stack that misbehaves. `node --test` runs it, and
 * the name keeps it out of Playwright's `*.test.ts` match. `pnpm test:e2e-harness`.
 */
import assert from "node:assert/strict";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { mkdtemp, readFile, rm, stat } from "node:fs/promises";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, beforeEach, test } from "node:test";
import { request as playwrightRequest } from "@playwright/test";
import { signedIn } from "./session.ts";

type Handler = (request: IncomingMessage, response: ServerResponse) => void;

let server: Server;
let origin = "";
let handler: Handler = () => {};
let seen: string[] = [];
let directory = "";

before(async () => {
  server = createServer((request, response) => {
    seen.push(request.url ?? "");
    handler(request, response);
  });
  await new Promise<void>((listening) => server.listen(0, "127.0.0.1", listening));
  origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  directory = await mkdtemp(join(tmpdir(), "kui-e2e-session-"));
});

after(async () => {
  await new Promise<void>((closed) => server.close(() => closed()));
  await rm(directory, { recursive: true, force: true });
});

beforeEach(() => {
  seen = [];
});

function answer(status: number, body: string, headers: Record<string, string> = {}): Handler {
  return (_request, response) => {
    response.writeHead(status, { "content-type": "application/json", ...headers });
    response.end(body);
  };
}

async function exists(path: string): Promise<boolean> {
  return stat(path).then(
    () => true,
    () => false,
  );
}

test("a 429 from the session limiter is refused by status and code, before anything else is read", async () => {
  handler = answer(
    429,
    JSON.stringify({ code: "KUI-AUTH-RATE-LIMITED", message: "too many new sessions; try again later" }),
  );
  const state = join(directory, "limited.json");
  const context = await playwrightRequest.newContext();
  try {
    await assert.rejects(signedIn("api fixture", context, origin, state), (error: Error) => {
      assert.match(error.message, /^api fixture: sign-in refused/);
      assert.match(error.message, /answered 429 KUI-AUTH-RATE-LIMITED: too many new sessions/);
      assert.match(error.message, /30 per client address and 600 overall per 60s/);
      return true;
    });
  } finally {
    await context.dispose();
  }
  assert.deepEqual(seen, ["/api/v1/auth/me"]);
  assert.equal(await exists(state), false, "a refused sign-in must not overwrite the stored session");
});

test("a non-envelope failure still names its status", async () => {
  handler = answer(502, "<html>bad gateway</html>", { "content-type": "text/html" });
  const context = await playwrightRequest.newContext();
  try {
    await assert.rejects(
      signedIn("browser session", context, origin, join(directory, "proxy.json")),
      /browser session: sign-in refused, .*\/api\/v1\/auth\/me answered 502 no error code: <html>bad gateway/,
    );
  } finally {
    await context.dispose();
  }
});

test("a live session answers its CSRF token and leaves the stored state alone", async () => {
  handler = answer(200, JSON.stringify({ csrfToken: "csrf-live" }));
  const state = join(directory, "live.json");
  const context = await playwrightRequest.newContext();
  try {
    assert.equal(await signedIn("api fixture", context, origin, state), "csrf-live");
  } finally {
    await context.dispose();
  }
  assert.equal(await exists(state), false);
});

test("a session the gateway replaced is written back, so later cases reuse it instead of minting", async () => {
  handler = answer(200, JSON.stringify({ csrfToken: "csrf-new" }), {
    "set-cookie": "kui_session=replacement; Path=/; HttpOnly; SameSite=Lax",
  });
  const state = join(directory, "replaced.json");
  const context = await playwrightRequest.newContext();
  try {
    assert.equal(await signedIn("browser session", context, origin, state), "csrf-new");
  } finally {
    await context.dispose();
  }
  const stored = JSON.parse(await readFile(state, "utf8")) as {
    cookies: { name: string; value: string }[];
  };
  assert.deepEqual(
    stored.cookies.map((cookie) => [cookie.name, cookie.value]),
    [["kui_session", "replacement"]],
  );
});
