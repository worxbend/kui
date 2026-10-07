import { describe, expect, it } from "vitest";
import { flush } from "solid-js";
import { createSession } from "./session.js";
import { createCurrentCluster, CurrentClusterStorageKey, soleClusterChoice } from "./currentCluster.js";

/**
 * Runs a test body with no reactive owner, which is where these stores are actually written to.
 *
 * Two Solid 2 behaviours shape every test below, and both were observed rather than recalled.
 *
 * **Updates are batched into a microtask.** A signal written here still reads its old value until
 * something flushes, so every assertion that follows a write calls `flush()` first. That is not
 * ceremony: a test that happened to pass without it would be asserting on a race.
 *
 * **A synchronous write inside an owned scope is an error in development**
 * (`REACTIVE_WRITE_IN_OWNED_SCOPE`). Wrapping these tests in `createRoot` therefore fails on the
 * first `select()` or `accept()` — even though the same call is correct in the product, where it
 * happens in an event handler or a promise continuation, both of which are outside the owning
 * scope. So the body runs unowned, which is the shape the real writes have.
 */
function inRoot<T>(body: () => T): T {
  return body();
}

describe("the session", () => {
  const anonymous = { kind: "anonymous", name: "anonymous" };
  const person = { kind: "session", name: "ada", roles: ["operator"] };

  function session() {
    const settled: (string | undefined)[] = [];
    const state = createSession({
      settleCsrf: (token) => settled.push(token),
      invalidateCsrf: () => settled.push("invalidated"),
    });
    return { state, settled };
  }

  /* The demonstration environment's front door. A locked door here is worse than any other bug. */
  it("shows no sign-in screen when authentication is disabled", () => {
    inRoot(() => {
      const { state } = session();
      state.acceptSettings({ authType: "disabled", rbacEnabled: false });
      state.accept({ authType: "disabled", csrfToken: "", principal: anonymous });
      flush();
      expect(state.mustSignIn()).toBe(false);
    });
  });

  it("shows no sign-in screen while the settings call is still in flight", () => {
    inRoot(() => {
      const { state } = session();
      expect(state.mustSignIn()).toBe(false);
    });
  });

  it("uses the session's auth mode when the settings call fails", () => {
    inRoot(() => {
      const { state } = session();
      state.acceptSettings(undefined);
      state.accept({ authType: "oidc", csrfToken: "", principal: anonymous });
      flush();
      expect(state.mustSignIn()).toBe(true);
    });
  });

  it("asks an anonymous visitor to sign in when the deployment is configured for it", () => {
    inRoot(() => {
      const { state } = session();
      state.acceptSettings({ authType: "oidc", rbacEnabled: true });
      state.accept({ authType: "oidc", csrfToken: "", principal: anonymous });
      flush();
      expect(state.mustSignIn()).toBe(true);
    });
  });

  it("does not ask a signed-in user to sign in again", () => {
    inRoot(() => {
      const { state } = session();
      state.acceptSettings({ authType: "oidc", rbacEnabled: true });
      state.accept({ authType: "oidc", csrfToken: "t", principal: person });
      flush();
      expect(state.mustSignIn()).toBe(false);
      expect(state.signedIn()).toBe(true);
    });
  });

  it("hands the CSRF token on, and treats an empty one as no token", () => {
    inRoot(() => {
      const { state, settled } = session();
      state.accept({ authType: "oidc", csrfToken: "", principal: anonymous });
      expect(settled).toEqual([undefined]);
      state.accept({ authType: "oidc", csrfToken: "abc", principal: person });
      expect(settled).toEqual([undefined, "abc"]);
    });
  });

  it("empties the identity and the token when the session lapses", () => {
    inRoot(() => {
      const { state, settled } = session();
      state.accept({ authType: "oidc", csrfToken: "abc", principal: person });
      state.acceptSettings({ authType: "oidc", rbacEnabled: true });
      state.markExpired();
      flush();
      expect(state.identity()).toBeUndefined();
      expect(state.signedIn()).toBe(false);
      expect(state.authType()).toBe("oidc");
      expect(settled).toContain("invalidated");
      expect(state.mustSignIn()).toBe(true);
      expect(state.permits("topic", "edit", "prod")).toBe(false);
      state.acceptSettings(undefined);
      flush();
      expect(state.mustSignIn()).toBe(true);
    });
  });

  it("answers what a control may do from the grants the session carried", () => {
    inRoot(() => {
      const { state } = session();
      state.accept({
        authType: "oidc",
        csrfToken: "abc",
        principal: person,
        permissions: [
          { resource: "topic", actions: ["view", "edit"], clusters: ["prod"] },
          { resource: "cluster", actions: ["view"], clusters: ["*"] },
        ],
      });
      flush();
      expect(state.permits("topic", "edit", "prod")).toBe(true);
      expect(state.permits("topic", "edit", "staging")).toBe(false);
      expect(state.permits("topic", "delete", "prod")).toBe(false);
      expect(state.permits("cluster", "view", "anywhere")).toBe(true);
    });
  });
});

describe("the current cluster", () => {
  it("remembers the choice, and survives storage that throws", () => {
    inRoot(() => {
      const store = new Map<string, string>();
      const working: Storage = {
        getItem: (key) => store.get(key) ?? null,
        setItem: (key, value) => void store.set(key, value),
        removeItem: (key) => void store.delete(key),
        clear: () => store.clear(),
        key: () => null,
        length: 0,
      };
      const chosen = createCurrentCluster({ storage: working });
      chosen.select("prod");
      flush();
      expect(store.get(CurrentClusterStorageKey)).toBe("prod");
      expect(createCurrentCluster({ storage: working }).selected()).toBe("prod");

      const hostile: Storage = {
        ...working,
        getItem: () => {
          throw new Error("blocked");
        },
        setItem: () => {
          throw new Error("blocked");
        },
      };
      const degraded = createCurrentCluster({ storage: hostile });
      degraded.select("staging");
      flush();
      expect(degraded.selected()).toBe("staging");
    });
  });

  it("chooses for a deployment with exactly one cluster and never for one with two", () => {
    expect(soleClusterChoice([{ id: "only" }], undefined)).toBe("only");
    expect(soleClusterChoice([{ id: "a" }, { id: "b" }], undefined)).toBeUndefined();
    /* An existing choice is never overridden; this only fills in a blank. */
    expect(soleClusterChoice([{ id: "only" }], "other")).toBeUndefined();
  });
});
