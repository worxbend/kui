/**
 * The handful of facts the server tells the browser before any request is made.
 *
 * The gateway injects them as a `<script id="kui-bootstrap" type="application/json">` block into
 * `index.html`. They cannot be compiled in, because they depend on where KUI is *deployed* rather
 * than on how it was built: an operator who mounts KUI at `https://tools.example.com/kafka/` needs
 * every asset URL and every API URL to gain that prefix, and neither the build nor a hard-coded
 * constant can know it.
 */
export interface Bootstrap {
  /**
   * The prefix the whole application is served under, without a trailing slash — `""` at the root of
   * a domain, `"/kafka"` behind a path-mounting reverse proxy.
   */
  readonly basePath: string;
  /** Where the API lives, including `basePath` — `"/api/v1"` or `"/kafka/api/v1"`. */
  readonly apiBase: string;
  /**
   * Which build the *server* is, which is not necessarily which build the browser has: a user with an
   * old tab open after a deploy has two different answers, and the header shows both when they
   * disagree.
   */
  readonly buildVersion: string;
}

/** The id of the `<script>` element the gateway writes. A contract with the gateway's HTML. */
export const BootstrapElementId = "kui-bootstrap";

/**
 * What the frontend assumes when the block is missing.
 *
 * Missing is a real case rather than a defensive one: a `vite build` output opened from a bare
 * `index.html`, and every unit test, have no gateway to inject anything. Defaulting to the root
 * deployment keeps those working instead of making an absent script a start-up crash.
 */
export const FallbackBootstrap: Bootstrap = {
  basePath: "",
  apiBase: "/api/v1",
  buildVersion: "dev",
};

/**
 * Reads the block out of a document.
 *
 * Never throws and never returns a partly-filled value: anything unreadable — no element, empty text,
 * malformed JSON, a field of the wrong type — yields {@link FallbackBootstrap}. A frontend that
 * refuses to start because one string was mistyped is worse than one that starts pointed at the
 * conventional path, which is what an operator would have configured anyway.
 */
export function readBootstrap(source: Document = document): Bootstrap {
  const element = source.getElementById(BootstrapElementId);
  const text = element?.textContent;
  if (!text) return FallbackBootstrap;

  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return FallbackBootstrap;
  }
  if (typeof parsed !== "object" || parsed === null) return FallbackBootstrap;

  const raw = parsed as Record<string, unknown>;
  const read = (key: keyof Bootstrap): string => {
    const value = raw[key];
    return typeof value === "string" ? value : FallbackBootstrap[key];
  };

  return {
    basePath: read("basePath"),
    apiBase: read("apiBase"),
    buildVersion: read("buildVersion"),
  };
}

/**
 * The URL the generated client's paths are appended to.
 *
 * Every path in `schema.d.ts` already carries the full public prefix — `/api/v1/clusters`, not
 * `/clusters` — because that is what the OpenAPI document says. So the base is the *deployment*
 * prefix and not `apiBase`: joining `apiBase` to a generated path would ask for
 * `/api/v1/api/v1/clusters`. `apiBase` stays what it is, the one place that states where the API
 * lives for anything that has to print or build that address itself.
 *
 * An `apiBase` that is already absolute means a deployment has genuinely split the two origins, and
 * that is honoured rather than overridden.
 */
export function apiBaseUrl(bootstrap: Bootstrap, origin: string): string {
  const base = new URL(bootstrap.apiBase, `${origin.replace(/\/+$/, "")}/`);
  base.pathname = base.pathname.replace(/\/+$/, "").replace(/\/api\/v1$/, "");
  base.search = "";
  base.hash = "";
  return base.toString().replace(/\/+$/, "");
}
