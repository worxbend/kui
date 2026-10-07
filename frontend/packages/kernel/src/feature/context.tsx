/**
 * What a feature can reach: the API client, the selected cluster, the permission answer, and the
 * links to the product's own pages.
 *
 * ## Why this exists
 *
 * A feature's root is a zero-argument `Component` — the shell renders it for every one of that
 * feature's routes and passes it nothing (see `registration.ts`). That is right: props would have to
 * be threaded through the router and the lazy boundary, and every feature would take a different
 * shape.
 *
 * But it left the features with no way to reach anything. Until this file existed, every feature in
 * the workspace rendered fixtures — `feature-clusters` drew `SAMPLE_CLUSTERS`, `feature-consumers`
 * drew `SAMPLE_GROUPS` — because there was no seam through which a real client could arrive. That is
 * a screen that looks like it works and is showing invented data, which is the single most dangerous
 * failure this product can have.
 *
 * ## Why a context rather than a module-level singleton
 *
 * A singleton would be shorter and would make the whole tree untestable in one step: a story or a
 * test could not render a feature against a stub client, against a cluster that is not answering, or
 * against a principal with no permissions — and those are exactly the states this project's worst
 * defects have lived in. A context is set up once by the shell and overridden freely by a story.
 *
 * It also makes the dependency honest. A feature that reads `useKui()` says so in its imports; a
 * feature reaching into a global says nothing, and the day two clusters are open at once — which is
 * the shape of the environment rail — a global is wrong in a way that is very hard to find.
 *
 * ## Everything here is a function, and that is deliberate
 *
 * `cluster()` and `permits()` are called, not read. The selected cluster changes while a feature is
 * mounted (the rail is one click away), and a permission answer changes when the session settles.
 * Handing a feature a *value* would freeze both at the moment the feature was first rendered, and
 * the symptom is a screen that keeps showing the previous cluster's topics — the most convincing
 * kind of wrong data there is.
 */
import { createContext, useContext } from "solid-js";
import type { JSX } from "@solidjs/web";
import { Actions, type KuiApiClient } from "@kui/api";

/**
 * The links a feature needs to build.
 *
 * Declared here, in the kernel, and implemented by the shell from its typed router. The kernel
 * naming the set is not a layering violation: it already names the features themselves
 * (`FeatureId`), and a feature that had to construct `/clusters/${id}/topics` by hand would go on
 * working after somebody renamed the segment, silently producing dead links — which is precisely
 * what the shell's typed path proxy exists to prevent, and what this interface carries across the
 * lazy boundary.
 */
export interface KuiPaths {
  readonly home: () => string;
  readonly settings: () => string;
  readonly clusters: () => string;
  readonly manageClusters: () => string;
  /**
   * The cluster's dashboard, on the named tab.
   *
   * `tab` is optional and the implementation supplies the default, so that every caller that just
   * wants "the cluster's page" produces the same address as the tab strip's first tab. A default
   * chosen per call site is how two links to the same page end up spelled differently, and only one
   * of them then matches whatever the navigation highlights.
   */
  readonly dashboard: (cluster: string, tab?: string) => string;
  readonly brokers: (cluster: string) => string;
  readonly broker: (cluster: string, brokerId: number) => string;
  readonly topics: (cluster: string) => string;
  readonly topic: (cluster: string, name: string) => string;
  readonly topicMessages: (cluster: string, name: string) => string;
  readonly trackMessages: (cluster: string) => string;
  readonly consumerGroups: (cluster: string) => string;
  readonly consumerGroup: (cluster: string, groupId: string) => string;
}

/**
 * Where a failed call happened, so the shell's connectivity tracker can tell "the gateway is down"
 * from "this one upstream is down".
 *
 * A feature reports the scope; only the shell knows what to do about it. Without this, a feature's
 * failed request either goes unreported — and the gateway-unreachable screen never appears — or the
 * feature has to reach into the shell's health tracker, which is the wrong direction entirely.
 */
export type CallScope = "shell" | "feature";

/**
 * One of the actions the server actually has, as opposed to any `{resource, action}` pair.
 *
 * Derived from the generated `Actions` table, so an action the server drops or renames breaks the
 * call sites that name it rather than silently answering `false` at run time.
 */
export type KnownAction = (typeof Actions)[keyof typeof Actions];

/** The two bounded-browse presentations a message screen can start in. */
export type MessageViewMode = "pages" | "infinite";

/** Reactive defaults supplied by the shell; explicit values in a message URL still win. */
export interface MessageBrowserDefaults {
  readonly pageSize: () => number;
  readonly mode: () => MessageViewMode;
}

export interface KuiContextValue {
  readonly api: KuiApiClient;
  /** The cluster the user has selected, or `undefined` before one is chosen. */
  readonly cluster: () => string | undefined;
  /**
   * Whether the signed-in principal may do this.
   *
   * Takes a whole `{resource, action}` pair from `@kui/api`'s
   * generated `Actions` — rather than two strings, because the pair is the unit: `VIEW` alone is
   * ambiguous, and two loose strings let a call site pair `TOPIC` with an action that belongs to
   * `SCHEMA` and get a confident `false` for a permission the principal actually holds. Call it as
   * `permits(Actions.TopicEdit)`, and the type now insists on it: the parameter is the *union of
   * the generated constants* rather than `PermissionAction`, whose `action` is a bare `string`. A
   * hand-written `{ resource: "TOPIC", action: "MESSAGES_PURGE" }` satisfied `PermissionAction`
   * perfectly and named an action the server has never heard of — Kafka's purge is
   * `MESSAGES_DELETE` — so it answered `false` for everyone, for ever, and the button it guarded
   * was permanently disabled with a message blaming the operator's permissions. Nothing failed;
   * a control simply never worked. Narrowing the parameter makes that a compile error.
   *
   * `name` narrows it to one object, where the server grants per-object permissions — a single
   * topic, a single connector. Omitted, it asks about the resource as a whole.
   *
   * Unknown or expired sessions fail closed. The server remains authoritative on every request.
   */
  readonly permits: (action: KnownAction, name?: string) => boolean;
  /**
   * Reactive cluster-write policy: read-only, permission or unknown-state reason; undefined allows.
   * Always checks the supplied cluster (including a copy destination), not the selected cluster.
   * Optional for isolated tests/stories: consumers use their existing local checks ONLY when this
   * function is absent. Do not use `writeBlocked?.(...) ?? fallback`: undefined means allowed.
   */
  readonly writeBlocked?: (clusterId: string, action: KnownAction, name?: string) => string | undefined;
  readonly paths: KuiPaths;
  /** Report a call's outcome. `undefined` means it succeeded. */
  readonly report: (scope: CallScope, failed: boolean) => void;
  /** Principal-and-cluster-scoped message browsing defaults, when the shell has them. */
  readonly messageBrowser?: MessageBrowserDefaults | undefined;
}

/**
 * `undefined` rather than a default value.
 *
 * A default would let a feature rendered outside the provider silently talk to a client that goes
 * nowhere, and the screen would show an empty list rather than an error. `useKui()` throws instead,
 * which fails at the first render in development and names the missing provider.
 */
const KuiContext = createContext<KuiContextValue>();

/**
 * The provider.
 *
 * In Solid 2 the context object *is* the provider component — there is no `.Provider` property, as
 * there was in Solid 1 and as React still has. This wrapper exists anyway so that call sites name
 * what they are doing (`<KuiProvider>` rather than `<KuiContext>`) and so the context object itself
 * stays private to this module: exporting it would let a caller read it with a bare `useContext`
 * and skip the error `useKui` raises when it is missing.
 */
export function KuiProvider(props: {
  readonly value: KuiContextValue;
  readonly children: JSX.Element;
}): JSX.Element {
  return <KuiContext value={props.value}>{props.children}</KuiContext>;
}

/**
 * The context, or a thrown error naming what is missing.
 *
 * Throwing is right here and is not a style choice. The alternative — returning `undefined` and
 * letting each feature decide — puts a null check in front of every call site, and the one place
 * somebody forgets it is a feature that renders an empty list instead of saying anything. This
 * cannot be reached in a shipped product: the shell wraps every route.
 */
export function useKui(): KuiContextValue {
  const value = useContext(KuiContext);
  if (value === undefined) {
    throw new Error(
      "useKui() was called outside <KuiProvider>. The shell provides it around every route; a story " +
        "or a test has to provide its own — see `testContext` in each feature's testing helper.",
    );
  }
  return value;
}
