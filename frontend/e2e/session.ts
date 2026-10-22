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
 */
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
      throw new Error(
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
