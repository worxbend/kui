import { expect, it } from "vitest";
import { createMemo, flush } from "solid-js";
import { Actions, createApiClient, createCsrfTokens, FallbackBootstrap } from "@kui/api";
import { createSession } from "@kui/kernel";
import { createClusterWritePolicy } from "./writePolicy.js";

const tick = async () => { await new Promise((resolve) => setTimeout(resolve, 0)); flush(); };

it("loads policy for the supplied cluster and reacts to permissions and read-only changes", async () => {
  const csrf = createCsrfTokens();
  const session = createSession({ settleCsrf: csrf.settle, invalidateCsrf: csrf.invalidate });
  const identity = { authType: "form", csrfToken: "token", principal: { kind: "session", name: "ada" },
    permissions: [{ resource: Actions.TopicCreate.resource, actions: [Actions.TopicCreate.action], clusters: ["destination", "readonly"], value: ".*" }] };
  session.accept(identity);
  flush();
  let readOnly = false;
  const urls: string[] = [];
  const api = createApiClient({ bootstrap: FallbackBootstrap, origin: "https://test.local", csrf,
    fetch: async (request) => {
      urls.push(request.url);
      return Response.json({ cluster: { readOnly: request.url.endsWith("/readonly") || readOnly } });
    },
  });
  const policy = createClusterWritePolicy(api, session);
  const reason = createMemo(() => policy.writeBlocked("destination", Actions.TopicCreate));
  expect(reason()).toMatch(/not.*known|not.*available/i);
  await tick();
  expect(reason()).toBeUndefined();
  expect(urls).toEqual(["https://test.local/api/v1/clusters/destination"]);
  expect(policy.writeBlocked("other", Actions.TopicCreate)).toMatch(/permission/i);
  expect(policy.writeBlocked("readonly", Actions.TopicCreate)).toBeDefined();
  await tick();
  expect(policy.writeBlocked("readonly", Actions.TopicCreate)).toMatch(/read.only/i);
  readOnly = true;
  policy.refresh();
  await tick();
  expect(reason()).toMatch(/read.only/i);
  session.markExpired();
  flush();
  expect(reason()).toMatch(/session|sign.in/i);
  policy.dispose();
});

it("fails closed on missing policy and ignores a response arriving after disposal", async () => {
  const csrf = createCsrfTokens(); csrf.settle("token");
  let release!: (response: Response) => void;
  const api = createApiClient({ bootstrap: FallbackBootstrap, origin: "https://test.local", csrf,
    fetch: () => new Promise<Response>((resolve) => { release = resolve; }),
  });
  const policy = createClusterWritePolicy(api, { identity: () => ({}), permits: () => true });
  expect(policy.writeBlocked("unknown", Actions.TopicCreate)).toBeDefined();
  await tick();
  release(Response.json({ cluster: {} }));
  await tick();
  expect(policy.writeBlocked("unknown", Actions.TopicCreate)).toBeDefined();
  policy.refresh();
  await tick();
  policy.dispose();
  release(Response.json({ cluster: { readOnly: false } }));
  await tick();
  expect(policy.writeBlocked("unknown", Actions.TopicCreate)).toBeDefined();
});
