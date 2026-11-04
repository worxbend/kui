import type { KuiApiClient } from "@kui/api";
import type { KnownAction } from "@kui/kernel";
import { createSignal } from "solid-js";

type Session = {
  readonly identity: () => unknown | undefined;
  readonly permits: (resource: string, action: string, cluster?: string, name?: string) => boolean;
};

/** One reactive, fail-closed policy for every cluster, including a copy's destination.
 * Reads may register a cluster for loading but never write reactive state synchronously.
 * The shell refreshes observed clusters periodically and resets them on identity changes.
 * A refresh keeps each known flag until an answer replaces it, so neither a reload in flight nor a
 * failed one closes controls the last answer opened; the server enforces read-only regardless. A
 * reset forgets every flag: nothing is known for a new principal until it has been asked.
 */
export function createClusterWritePolicy(api: KuiApiClient, session: Session) {
  const [states, setStates] = createSignal<ReadonlyMap<string, boolean | undefined>>(new Map());
  const observed = new Set<string>();
  const attempted = new Set<string>();
  const pending = new Set<AbortController>();
  let generation = 0;
  let disposed = false;

  const load = (clusterId: string): void => {
    if (disposed || attempted.has(clusterId)) return;
    attempted.add(clusterId);
    const episode = generation;
    const request = new AbortController();
    pending.add(request);
    void Promise.resolve().then(async () => {
      if (disposed || episode !== generation) return;
      const answer = await api.get("/api/v1/clusters/{clusterId}", {
        params: { path: { clusterId } }, signal: request.signal,
      });
      pending.delete(request);
      // A failure has learned nothing, so whatever was known before, or nothing, still stands.
      if (disposed || episode !== generation || !answer.ok) return;
      const body = answer.value as { cluster?: { readOnly?: unknown } } | null;
      const flag = body?.cluster?.readOnly;
      setStates((previous) => new Map(previous).set(clusterId, typeof flag === "boolean" ? flag : undefined));
    });
  };

  const reload = (forget: boolean): void => {
    if (disposed) return;
    generation += 1;
    for (const request of pending) request.abort();
    pending.clear();
    attempted.clear();
    if (forget) setStates(new Map());
    if (session.identity() !== undefined) for (const clusterId of observed) load(clusterId);
  };

  return {
    writeBlocked(clusterId: string, action: KnownAction, name?: string): string | undefined {
      if (session.identity() === undefined) return "Your session is not available. Sign in again.";
      if (!session.permits(action.resource, action.action, clusterId, name)) {
        return `You do not have the ${action.resource}:${action.action} permission on this cluster.`;
      }
      observed.add(clusterId);
      const readOnly = states().get(clusterId);
      load(clusterId);
      if (readOnly === true) return "This cluster is read-only.";
      if (readOnly === undefined || disposed) return "The cluster write policy is not available yet.";
      return undefined;
    },
    /** Asks again for every observed cluster, keeping each known flag until it is answered. */
    refresh(): void {
      reload(false);
    },
    /** Forgets every flag and asks again, for an identity that has changed. */
    reset(): void {
      reload(true);
    },
    dispose(): void {
      disposed = true;
      generation += 1;
      for (const request of pending) request.abort();
      pending.clear();
    },
  };
}
