/**
 * One gateway session for the whole suite, minted once by `globalSetup` and handed to every browser
 * context and every API request context.
 *
 * The gateway mints an anonymous session for any `/api` request that arrives without its cookie,
 * and since bf1393c that minting is budgeted: thirty new sessions per client address per minute
 * (`SessionMiddleware.attachInterceptor`). Playwright opens a fresh, cookie-less context for every
 * case, so the suite spent one new session per case on the browser's path and another per case on
 * the `api` fixture's — and `globalSetup`'s own scrape poll spent four every two seconds. Fast
 * stretches of the suite went over thirty a minute, the gateway answered `429
 * KUI-AUTH-RATE-LIMITED` to the whole first page load, `/auth/me` included, and the shell drew its
 * sign-in dialog over the screen. That is what "a login page intercepts pointer events", "the
 * message browser did not render" and "the partition picker was not found" were in CI, in whichever
 * cases happened to land past the budget.
 *
 * The limiter is right for a product and stays. The suite simply stops asking it for a session per
 * case: the browser carries one session for the whole run and the `api` fixture another. Each
 * context still starts from the stored state, so nothing a case writes to `localStorage` reaches the
 * next one.
 *
 * Sharing a session is not enough on its own: once the gateway forgets it, every context built from
 * the stale file mints again, one or more per case, and the run is back over budget. So each case
 * checks its principal's session first (`signedIn`), refuses a non-2xx answer by status and code,
 * and writes a replacement the gateway handed out back to the file. `session.node-test.ts` holds
 * both behaviours; run it with `pnpm test:e2e-harness`, which needs no stack.
 */
import type { APIRequestContext } from "@playwright/test";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const AUTH_DIRECTORY = resolve(dirname(fileURLToPath(import.meta.url)), "../playwright/.auth");

/** The browser's session; `playwright.config.ts` points `storageState` at it. */
export const SESSION_STATE = resolve(AUTH_DIRECTORY, "browser.json");

/**
 * The `api` fixture's session: a second one, minted once beside the browser's rather than shared
 * with it, because `fixtures.ts` keeps the two principals apart on purpose.
 */
export const API_SESSION_STATE = resolve(AUTH_DIRECTORY, "api.json");

/**
 * The gateway's new-session budget, as `SessionMiddleware.attachInterceptor` spends it: per client
 * address, then for every address together. Written here only so that a refusal can say it; the
 * gateway's numbers are the real ones.
 */
export const NEW_SESSION_BUDGET = { perAddress: 30, overall: 600, windowSeconds: 60 } as const;

/** What a sign-in answer has to offer for the checks below: Playwright's and `fetch`'s both do. */
export interface SignInAnswer {
  readonly status: number;
  readonly url: string;
  readonly text: () => Promise<string>;
}

/**
 * The error for a sign-in the gateway refused, or `undefined` when it did not.
 *
 * The fixture used to read the body of `/auth/me` whatever the status was, so a `429` became an
 * empty CSRF token, the case went on to its protected reads, and what failed was a screen fifteen
 * seconds later with the sign-in dialog drawn over it. The status and the gateway's own error code
 * are the whole diagnosis, so they go in the message, before anything else is asked.
 */
export async function signInRefusal(who: string, answer: SignInAnswer): Promise<Error | undefined> {
  if (answer.status >= 200 && answer.status < 300) return undefined;

  const text = await answer.text().catch(() => "");
  let code = "no error code";
  let message = text.slice(0, 200);
  try {
    const envelope = JSON.parse(text) as { code?: unknown; message?: unknown };
    if (typeof envelope.code === "string") code = envelope.code;
    if (typeof envelope.message === "string") message = envelope.message;
  } catch {
    /* Not an error envelope (a proxy's HTML page, say). The raw text is the best there is. */
  }

  const lines = [
    `${who}: sign-in refused, ${answer.url} answered ${answer.status} ${code}` +
      (message === "" ? "." : `: ${message}`),
  ];
  if (code === "KUI-AUTH-RATE-LIMITED") {
    lines.push(
      "",
      "The gateway's new-session budget is spent: " +
        `${NEW_SESSION_BUDGET.perAddress} per client address and ${NEW_SESSION_BUDGET.overall} overall ` +
        `per ${NEW_SESSION_BUDGET.windowSeconds}s. The suite mints one session per principal and reuses ` +
        "it, so something else is minting them: the gateway restarted and forgot the shared sessions, " +
        "a context was opened without a stored session, or another client shares this address.",
    );
  }
  return new Error(lines.join("\n"));
}

/** Throws {@link signInRefusal}'s error, when there is one. */
export async function requireSignedIn(who: string, answer: SignInAnswer): Promise<void> {
  const refusal = await signInRefusal(who, answer);
  if (refusal !== undefined) throw refusal;
}

/**
 * Asks `/auth/me` for the session `request` carries, and answers its CSRF token.
 *
 * A refusal stops here, naming its status and code, before the case makes a single protected read
 * ({@link signInRefusal} says why). And when the gateway answered with a cookie, the stored session
 * is gone — it only stamps one on a session it has just minted, which for a stored cookie means it
 * no longer knows it, most often because it restarted. The jar goes back to `state` then, so the replacement is
 * what every later case and every restarted worker starts from: losing the shared session costs one
 * new session for the rest of the run rather than one or more per case, which is what spent the
 * limiter's budget and turned a single loss into a run of `429`s.
 */
export async function signedIn(
  who: string,
  request: APIRequestContext,
  origin: string,
  state: string,
): Promise<string> {
  const me = await request.get(`${origin}/api/v1/auth/me`);
  await requireSignedIn(who, { status: me.status(), url: me.url(), text: () => me.text() });
  if (me.headersArray().some((header) => header.name.toLowerCase() === "set-cookie")) {
    await request.storageState({ path: state });
  }
  const body = (await me.json()) as { csrfToken?: string };
  return body.csrfToken ?? "";
}

/** How long to keep asking when the gateway's session budget is spent: one budget window, and some. */
const MINT_DEADLINE_MS = 75_000;
const MINT_RETRY_MS = 2_000;

/**
 * Mints one session through `origin` and writes it to `state`, where every context that should
 * carry it loads it from.
 *
 * Answers the `Cookie` header that carries it, for the plain `fetch` calls `globalSetup` makes.
 */
export async function mintSharedSession(origin: string, state: string): Promise<string> {
  const deadline = Date.now() + MINT_DEADLINE_MS;
  for (;;) {
    const response = await fetch(`${origin}/api/v1/auth/me`, { signal: AbortSignal.timeout(5_000) });
    const cookies = response.headers.getSetCookie();
    if (response.ok && cookies.length > 0) {
      const host = new URL(origin).hostname;
      const parsed = cookies.map((line) => parseSetCookie(line, host));
      await mkdir(dirname(state), { recursive: true });
      await writeFile(state, JSON.stringify({ cookies: parsed, origins: [] }, null, 2));
      return parsed.map((cookie) => `${cookie.name}=${cookie.value}`).join("; ");
    }
    if (response.status !== 429 || Date.now() > deadline) {
      const refusal = await signInRefusal("globalSetup", response);
      throw new Error(
        refusal?.message ??
          `${origin}/api/v1/auth/me answered ${response.status} without a session cookie; ` +
            "the suite has no session to share.",
      );
    }
    await new Promise((resolveWait) => setTimeout(resolveWait, MINT_RETRY_MS));
  }
}

interface StoredCookie {
  readonly name: string;
  readonly value: string;
  readonly domain: string;
  readonly path: string;
  readonly expires: number;
  readonly httpOnly: boolean;
  readonly secure: boolean;
  readonly sameSite: "Strict" | "Lax" | "None";
}

/** A `Set-Cookie` line as Playwright's storage state spells a cookie. */
function parseSetCookie(line: string, host: string): StoredCookie {
  const [pair = "", ...attributes] = line.split(";").map((part) => part.trim());
  const equals = pair.indexOf("=");
  const attribute = (key: string): string | undefined => {
    const found = attributes.find((part) => part.toLowerCase().startsWith(`${key.toLowerCase()}=`));
    return found?.slice(key.length + 1);
  };
  const flag = (key: string): boolean =>
    attributes.some((part) => part.toLowerCase() === key.toLowerCase());
  const sameSite = (attribute("SameSite") ?? "Lax").toLowerCase();
  return {
    name: pair.slice(0, equals),
    value: pair.slice(equals + 1),
    domain: attribute("Domain") ?? host,
    path: attribute("Path") ?? "/",
    expires: -1,
    httpOnly: flag("HttpOnly"),
    secure: flag("Secure"),
    sameSite: sameSite === "strict" ? "Strict" : sameSite === "none" ? "None" : "Lax",
  };
}
