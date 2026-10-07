/**
 * What the signed-in user is allowed to do, and the one place a screen asks.
 *
 * ## Why the browser decides this at all
 *
 * It does not, really — the server decides, and re-decides on every request. What the browser does
 * is *predict* the server's answer so it can disable a control rather than offer one that will be
 * refused. A button that is offered and then refused teaches an operator that KUI's refusals are
 * bugs, and the next real refusal is not believed.
 *
 * A prediction is only worth making if it is right every time. Under the previous frontend that was
 * guaranteed by construction: `libs/security-core` cross-compiled to Scala.js, so the browser ran
 * the server's own evaluator. In TypeScript it cannot, so the guarantee is rebuilt out of two
 * halves:
 *
 * 1. **The vocabulary is generated.** `Resources`, `Actions` and `ConnectorFallbackActions` in
 *    `@kui/api` are emitted from the Scala enums by the build, and an action carries its own
 *    resource because `VIEW` alone names thirteen different things. Rename an action on the server and
 *    this file stops compiling.
 * 2. **The evaluation is a lookup, not a re-derivation.** The server sends grants with their action
 *    sets *already expanded* — `DELETE` already carries the `VIEW` it implies — so nothing here
 *    re-implements the closure or role expansion. First scope grants to the cluster and select
 *    explicit grants over fallback-only defaults, then check resource, name and action.
 *
 * The connector-parent fallback's action vocabulary is generated too (see {@link grantsAllow}).
 *
 * ## Before the session has answered
 *
 * The list is empty and every question answers "no", so a write control starts disabled and becomes
 * enabled a moment later. That is the right way round: a control that starts enabled and disables
 * itself can be clicked in the gap, and the click goes to a server that will refuse it.
 *
 * ## What this store deliberately does not know
 *
 * Whether the cluster is read-only. That is a fact about the cluster, not about the user, and it
 * arrives on the capability document. Two sources for one fact is two ways to get it wrong, and this
 * is the one that does not know — a read-only cluster's controls are disabled by the capability half
 * of the pair, with its own explanation.
 */
import { ConnectorFallbackActions, Resources, type PermissionAction, type components } from "@kui/api";
import { createSignal, type Accessor } from "solid-js";

/** How "every cluster" is spelled in a grant's cluster list. A cluster id is a lowercase slug, so
 * `*` cannot collide with one — which is what makes it safe to put in the same list as real ids. */
export const EVERY_CLUSTER = "*";

/**
 * One grant, exactly as `GET /api/v1/auth/me` sends it.
 *
 * Structurally the generated `PermissionDto`, restated here with the optional fields resolved,
 * because {@link adoptGrants} is the boundary that turns the wire shape into this one and every
 * field is checked there.
 */
export interface PermissionGrant {
  /** Fallback only: ignored when any explicit grant covers the requested cluster. */
  readonly defaultRole?: boolean;
  /** The cluster ids this grant applies on, or {@link EVERY_CLUSTER}. */
  readonly clusters: readonly string[];
  /** The resource's configuration spelling: `TOPIC`, `CONSUMER`, … */
  readonly resource: string;
  /** The resource-name pattern, as the regular expression an operator wrote. */
  readonly value: string | undefined;
  /** The action spellings this grant allows, already expanded by the server. */
  readonly actions: readonly string[];
}

/** Why a control is disabled, or that it is not. */
export type PermissionDecision =
  | { readonly allowed: true }
  | { readonly allowed: false; readonly reason: string };

export interface Permissions {
  /** The grants the last session answer carried. */
  readonly granted: Accessor<readonly PermissionGrant[]>;
  /** Replaces the whole set. Called when a session is established; nothing else should call it. */
  adopt(grants: readonly PermissionGrant[]): void;
  /**
   * Whether this user may take this action on this named resource on this cluster, and if not, the
   * sentence to put in the disabled control's tooltip.
   */
  decide(cluster: string, action: PermissionAction, name?: string): PermissionDecision;
  /** {@link decide}, as a boolean, for the places that only need to disable something. */
  allows(cluster: string, action: PermissionAction, name?: string): boolean;
  /**
   * Whether the user holds this action on **some** resource of this kind, whatever its name.
   *
   * The question a create asks. A new topic's name does not exist yet, so there is nothing to match
   * a grant's pattern against.
   *
   * This is deliberately the *same* weakening the gateway applies to an endpoint whose resource is
   * named only in the request body: somebody with no topic grant at all may not reach the create
   * endpoint, and somebody whose grant is `payments\..*` may — and is then refused by the owning
   * service, with the name in hand, if they ask for `orders`. Matching the server's rule exactly is
   * the point: a stricter browser hides a control the server would have allowed, and a looser one
   * offers a control the server refuses.
   */
  allowsAny(cluster: string, action: PermissionAction): boolean;
  /**
   * The subset of `items` the user may see, for a list a screen renders.
   *
   * List screens filter rather than refuse, which is what the server does too: somebody who may see
   * three of a hundred topics should see three topics, not an error. Hiding the other ninety-seven
   * is also what stops the list from leaking that they exist.
   */
  visible<A>(
    cluster: string,
    action: PermissionAction,
    items: readonly A[],
    nameOf: (item: A) => string,
  ): readonly A[];
}

/**
 * Whether these grants allow this action on this named resource on this cluster.
 *
 * The evaluation, as a pure function over the grant list, so that a caller holding grants from
 * somewhere other than this store — the session store does — asks exactly the same question and can
 * never answer it differently. {@link createPermissions} is a reactive wrapper around this and
 * {@link grantsAllowAny}, and adds nothing to the rule.
 *
 * `name` of `undefined` asks about the *unnamed* resource — the audit trail, ksqlDB, the ACL list.
 * It is not "any name": for that question, which is the one a create asks, use
 * {@link grantsAllowAny}.
 */
export function grantsAllow(
  grants: readonly PermissionGrant[],
  cluster: string | undefined,
  action: PermissionAction,
  name: string | undefined,
): boolean {
  const selected = effectiveGrants(grants, cluster);
  if (holds(selected, action, name)) return true;

  // The connector fallback uses the already-selected grants: a grant on
  // the connect cluster `payments` covers all forty of its connectors without a permission naming
  // each one. A connector is named `<connect>/<connector>`, and the fallback asks for the same
  // action on `CONNECT` with the connect cluster's name — which the build asserts is always the
  // same action name, so no mapping is needed here.
  if (action.resource === Resources.Connector && name !== undefined && CONNECTOR_FALLBACK.has(action.action)) {
    const connect = name.split("/")[0];
    if (connect !== undefined) {
      return holds(selected, { resource: Resources.Connect, action: action.action }, connect);
    }
  }

  return false;
}

/** Whether these grants allow this action on **some** resource of this kind. See {@link Permissions.allowsAny}. */
export function grantsAllowAny(
  grants: readonly PermissionGrant[],
  cluster: string | undefined,
  action: PermissionAction,
): boolean {
  return effectiveGrants(grants, cluster).some((grant) =>
    grant.resource === action.resource && grant.actions.includes(action.action),
  );
}

/** Select roles before resources, patterns, or actions; defaults are not additive rights.
 * A global request considers every cluster, just like Rbac.effectivePermissions(None). */
function effectiveGrants(
  grants: readonly PermissionGrant[],
  cluster: string | undefined,
): readonly PermissionGrant[] {
  const scoped = grants.filter(
    (grant) => cluster === undefined ||
      grant.clusters.includes(EVERY_CLUSTER) || grant.clusters.includes(cluster),
  );
  const explicit = scoped.filter((grant) => !grant.defaultRole);
  return explicit.length > 0 ? explicit : scoped;
}

function holds(
  grants: readonly PermissionGrant[],
  action: PermissionAction,
  name: string | undefined,
): boolean {
  return grants.some(
    (grant) => grant.resource === action.resource && covers(grant, name) && grant.actions.includes(action.action),
  );
}

export function createPermissions(): Permissions {
  // Written when a session lands, which can be from inside an owned scope. One writer path.
  const [granted, setGranted] = createSignal<readonly PermissionGrant[]>([], { ownedWrite: true });

  const store: Permissions = {
    granted,

    adopt(grants: readonly PermissionGrant[]): void {
      setGranted(grants);
    },

    decide(cluster: string, action: PermissionAction, name?: string): PermissionDecision {
      return grantsAllow(granted(), cluster, action, name)
        ? { allowed: true }
        : { allowed: false, reason: refusal(action, name) };
    },

    allows(cluster: string, action: PermissionAction, name?: string): boolean {
      return this.decide(cluster, action, name).allowed;
    },

    allowsAny(cluster: string, action: PermissionAction): boolean {
      return grantsAllowAny(granted(), cluster, action);
    },

    visible<A>(
      cluster: string,
      action: PermissionAction,
      items: readonly A[],
      nameOf: (item: A) => string,
    ): readonly A[] {
      return items.filter((item) => this.allows(cluster, action, nameOf(item)));
    },
  };

  return store;
}

/**
 * The sentence a disabled control shows.
 *
 * It names the action and the thing, and never the grant: telling somebody which pattern they would
 * need is telling them about a policy they cannot read, and it reads as an accusation. "You do not
 * have permission to" is the phrasing the rest of the product uses for a refusal.
 */
function refusal(action: PermissionAction, name: string | undefined): string {
  const subject = name === undefined ? action.resource.toLowerCase() : `'${name}'`;
  return `You do not have permission to ${action.action.toLowerCase().replaceAll("_", " ")} ${subject}.`;
}

/**
 * Whether a grant is about the thing being accessed at all — a name that matches its pattern.
 *
 * `value` of `undefined` means "the unnamed one": a permission over `AUDIT` names nothing because
 * there is one audit trail, and a permission over `TOPIC` with no pattern therefore grants nothing,
 * because every topic access names a topic. That asymmetry is the server's and is deliberate — it
 * makes a forgotten `value` deny rather than grant.
 */
function covers(grant: PermissionGrant, name: string | undefined): boolean {
  if (grant.value === undefined) return name === undefined;
  if (name === undefined) return false;
  return patternFor(grant.value)?.test(name) ?? false;
}

/**
 * Compiled patterns, cached by their source text.
 *
 * The match is a **full** match, not a search: `orders` must not grant `orders-dlq`. JavaScript has
 * no full-match flag, so the pattern is anchored — and wrapped in a non-capturing group first,
 * because `^a|b$` and `^(?:a|b)$` are not the same expression and the difference is a silently wider
 * grant.
 *
 * A pattern that will not compile matches nothing. The server validated it when it read the
 * configuration, so reaching here means the two engines disagree about the syntax; denying is the
 * safe direction, and the control simply stays disabled rather than offering something the server
 * would refuse.
 */
const compiled = new Map<string, RegExp | undefined>();

function patternFor(raw: string): RegExp | undefined {
  if (compiled.has(raw)) return compiled.get(raw);
  let pattern: RegExp | undefined;
  try {
    pattern = new RegExp(`^(?:${raw})$`, "u");
  } catch {
    pattern = undefined;
  }
  compiled.set(raw, pattern);
  return pattern;
}

const CONNECTOR_FALLBACK: ReadonlySet<string> = new Set(ConnectorFallbackActions);

/**
 * Reads the grant list off a session response.
 *
 * The wire type is the generated `PermissionDto`, whose every field is optional because the server
 * omits empty lists. This is the one boundary where that is resolved, so that nothing downstream has
 * to ask whether `actions` might be missing — and a grant that names no resource is dropped rather
 * than turned into a grant over `undefined`, which would match nothing and confuse the next person
 * to read a support log.
 */
export function grantsFromWire(
  permissions: readonly components["schemas"]["PermissionDto"][] | undefined,
): readonly PermissionGrant[] {
  return (permissions ?? [])
    .filter((permission) => permission.resource.length > 0)
    .map((permission) => ({
      clusters: permission.clusters ?? [],
      resource: permission.resource,
      value: permission.value,
      actions: permission.actions ?? [],
      defaultRole: permission.defaultRole ?? false,
    }));
}
