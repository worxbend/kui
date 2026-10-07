/**
 * The ksqlDB feature's route entry.
 *
 *   /clusters/:clusterId/ksql        the streams, tables and queries on this cluster's ksqlDB
 *
 * The route itself is the shell's — `routing/routes.tsx` holds every URL pattern as a literal, and
 * a feature that assembled its own address would go on compiling after somebody renamed a segment
 * and would simply stop matching. This package is what is behind the route; the `ECOSYSTEM` nav row
 * and the route are W8-07's.
 *
 * ## The permission question is asked here, once, and it is asked **without** a subject
 *
 * `kui.permits(Actions.KsqlExecute)` — no second argument — and that is not an oversight. It is the
 * question the server asks, and naming a subject here would refuse every correctly-permitted
 * operator:
 *
 *   - `KsqlEndpoints.executing` declares
 *     `ResourceRequirement.unnamed(Resource.Ksql, Action.KsqlExecute)`, and `KsqlStreamEndpoint`
 *     declares the same. That is the question, verbatim.
 * - `libs/security-core/.../Vocabulary.scala:84` puts `Ksql` in the **unnamed** half of
 *     `Resource.isNamed`, beside `Audit` and `Alerts`: there is at most one ksqlDB per cluster, so
 *     a ksqlDB access names nothing the way a topic access names a topic.
 * - `libs/config/.../RbacConfigSection.scala:281` therefore **refuses the configuration file** for
 *     a `KSQL` permission that carries a `value` — *"KSQL has no name to match against, so '…'
 *     would never apply; remove the value"*. Every KSQL grant that can exist has no pattern.
 *   - `@kui/kernel`'s `grantsAllow` evaluates `covers(grant, name)` as
 *     `grant.value === undefined ? name === undefined : …`. So a named question asked against a
 *     pattern-less grant answers **false**, for everybody, for ever — the permanently-disabled
 *     control `KuiContextValue.permits` documents at length, and the one `MESSAGES_PURGE` already
 *     cost this project once.
 *
 * The cluster is **not** lost by asking it this way: `useKui().permits` passes the frame's cluster
 * id separately (`shell/src/App.tsx:694` → `session.permits(resource, action, cluster, name)`), and
 * a grant's `clusters` list is what scopes it. `feature-alerts` asks the identical question for
 * `Resource.Alerts`, which is unnamed for the same reason.
 *
 * W8-05's plan asked for the *subject-aware* form and named the subjectless one as the mutation. On
 * this resource that is the wrong way round, so the rule is closed in the direction the vocabulary
 * supports and **both** directions are gated: `ksql.test.tsx` has a case that goes red if a subject
 * is added and cases that go red if the gate is dropped or weakened to `KSQL:VIEW`.
 *
 * ## A read-only cluster refuses a statement before the principal is even asked
 *
 * `writeBlockedReason` puts the read-only sentence first because it is the one an operator can do
 * nothing about, and the server agrees: `RbacLawsSuite:315` — *a read-only cluster still lists
 * ksqlDB objects and still refuses a ksqlDB statement*, because `KsqlView` does not alter and
 * `KsqlExecute` does. The flag is the cluster document's own, read through the endpoint
 * `feature-topics` reads it through: one fact, two readers, not two sources. `feature-alerts`
 * passes `readOnly: false` as a constant and a permitted principal there gets a live Acknowledge on
 * a read-only cluster; this screen does not repeat that.
 *
 * ## Every statement is planned, and the plan is what routes it
 *
 * ADR-045 over free-form text. The browser plans **every** statement rather than the ones it
 * suspects, because which statements are destructive is the decision `services/ksql` exists to
 * make. The plan's `shape` then says where the statement goes: a `push_query` opens the stream —
 * the execute endpoint refuses one, because it never finishes — and everything else is applied,
 * after a confirmation when the plan says one is needed.
 *
 * ## The stream is closed when this screen goes away
 *
 * A push query never ends by itself. `onCleanup` aborts it, which cancels the gateway's relay,
 * which cancels the service's fiber, which stops the query on the ksqlDB server. Without that,
 * navigating away leaves a query running on somebody's cluster until
 * `kui.clusters.<n>.ksql.streamTimeout` expires — and it is one click away.
 */
import { Show, createMemo, createSignal, onCleanup, untrack } from "solid-js";
import type { JSX } from "@solidjs/web";
import { useParams } from "@solidjs/router";
import { Actions, userMessage } from "@kui/api";
import {
  Banner,
  EmptyState,
  KsqlWorkspace,
  PageHeader,
  createMutation,
  useKui,
  useQuery,
  writeBlockedReason,
  type Fetched,
  type KsqlObject,
  type QueryRegistry,
  type SseHandle,
} from "@kui/kernel";

import { ConfirmStatement } from "./ConfirmStatement.jsx";
import { KsqlResult } from "./KsqlResult.jsx";
import { RunningQueries } from "./RunningQueries.jsx";
import {
  fetchClusterWriteState,
  fetchObjects,
  openPushQuery,
  planStatement,
  runStatement,
  type ClusterWriteState,
  type PushQueryOpener,
} from "./data.js";
import {
  EXECUTE_ACTION,
  IDLE,
  NOT_CONFIGURED,
  NO_OBJECTS,
  OFFSET_RESET_NOT_SETTABLE,
  OPENING,
  appendRow,
  endLive,
  interrupt,
  isLive,
  ksqlVoice,
  regionFor,
  unreadableSentence,
  withColumns,
  type ResultRegion,
} from "./model.js";
import type { KsqlObjects, StatementPlan } from "./wire.js";

export default function Ksql(): JSX.Element {
  const params = useParams<{ readonly clusterId?: string }>();
  return (
    <Show when={params.clusterId} fallback={<NoCluster />}>
      {(clusterId) => <KsqlScreen clusterId={clusterId()} />}
    </Show>
  );
}

function NoCluster(): JSX.Element {
  const kui = useKui();
  return (
    <section aria-label="ksqlDB">
      <p role="status">
        No cluster is selected, so there is no ksqlDB server to ask.{" "}
        <a href={kui.paths.clusters()}>Choose a cluster</a> and try again.
      </p>
    </section>
  );
}

export interface KsqlScreenProps {
  readonly clusterId: string;
  /**
   * Which shared answers to read.
   *
   * A test seam and nothing else: the product passes nothing and gets the application-wide cache,
   * which is what makes two screens reading one key read one answer. A story or a case that used
   * that cache would inherit whatever the previous one left in it.
   */
  readonly queries?: QueryRegistry | undefined;
  /**
   * How a push query is opened.
   *
   * The product passes nothing and gets {@link openPushQuery}, which talks to the gateway. A case
   * passes a stream it drives itself, which is the only way to put this screen into `streaming`,
   * `ended` and `interrupted` — ADR-056's three hardest decisions — without a network.
   */
  readonly openStream?: PushQueryOpener | undefined;
}

export function KsqlScreen(props: KsqlScreenProps): JSX.Element {
  return <Show when={props.clusterId} keyed>{(clusterId) =>
    <KsqlSession {...props} clusterId={clusterId} />
  }</Show>;
}

function KsqlSession(props: KsqlScreenProps): JSX.Element {
  const kui = useKui();

  const objects = useQuery<KsqlObjects>({
    key: () => `ksql:${props.clusterId}`,
    load: () => fetchObjects(kui.api, props.clusterId),
    ...(props.queries === undefined ? {} : { registry: props.queries }),
  });

  const writeState = useQuery<ClusterWriteState>({
    key: () => `cluster-write-state|${props.clusterId}`,
    load: () => fetchClusterWriteState(kui.api, props.clusterId),
    ...(props.queries === undefined ? {} : { registry: props.queries }),
  });

  const readOnly = (): boolean => {
    const state = writeState.state();
    return (state.kind === "ready" || state.kind === "stale") && state.value.readOnly;
  };

  /**
   * The gate. This is the packet's owned rule; the header says why it takes no subject.
   *
   * Asking it *here* rather than only inside the control is the other half of it. Wave 5 shipped a
   * destructive consumer-group control whose component refused correctly and whose route never
   * mounted without the permission, so the wiring that decided it was asserted by nothing at all.
   * `KsqlWorkspace` cannot invent a permission it was not handed; this is where it is handed one.
   */
  const runRefusal = createMemo((): string | undefined =>
    kui.writeBlocked !== undefined ? kui.writeBlocked(props.clusterId, Actions.KsqlExecute) : writeBlockedReason({
      permitted: kui.permits(Actions.KsqlExecute),
      readOnly: readOnly(),
      action: EXECUTE_ACTION,
    }));

  const [sql, setSql] = createSignal("");
  const [region, setRegion] = createSignal<ResultRegion>(IDLE);
  const [confirming, setConfirming] = createSignal<StatementPlan | undefined>(undefined);

  /* The open push query, if one is open. Held outside the signal graph because closing it is a side
     effect on the network rather than a value anything renders. */
  let stream: SseHandle | undefined;

  const stopStream = (): void => {
    stream?.close();
    stream = undefined;
  };

  // The leak this exists to stop: a push query the reader navigated away from goes on running on
  // the ksqlDB server until its stream budget expires. See the header.
  onCleanup(() => { generation += 1; stopStream(); });

  const plan = createMutation((statement: string) =>
    planStatement(kui.api, props.clusterId, statement),
  );
  const apply = createMutation((confirmed: StatementPlan) =>
    runStatement(kui.api, props.clusterId, confirmed),
  );

  /* Which `plan.run()`/`apply.run()` invocation is still allowed to touch the signals when it
     settles. `onCancel`, `onClear` and a fresh `onRun` all bump this, so a promise from a
     statement the reader already cancelled or cleared finds itself superseded and leaves the
     region alone — the same guard `useQuery` already applies to a stale fetch. */
  let generation = 0;

  const nextGeneration = (): number => {
    generation += 1;
    return generation;
  };

  const failWith = (outcome: {
    readonly kind: string;
    readonly message?: string;
    readonly code?: string;
  }): void => {
    setRegion({ kind: "failed", message: sentenceOf(outcome), code: outcome.code });
  };

  const onRun = (): void => {
    if (untrack(runRefusal) !== undefined) return;
    const gen = nextGeneration();
    stopStream();
    setConfirming(undefined);
    setRegion({ kind: "running" });
    void plan.run(sql()).then((outcome) => {
      if (gen !== generation) return;
      const refusal = untrack(runRefusal);
      if (refusal !== undefined) { failWith({ kind: "forbidden", message: refusal }); return; }
      if (outcome.kind !== "done") {
        failWith(outcome);
        return;
      }
      const planned = outcome.value;
      // A push query never finishes, so it is refused at the execute address and answered here.
      if (planned.shape === "push_query") {
        setRegion(OPENING);
        openStream(planned.statement);
        return;
      }
      /* The service decides whether this needs confirming, not the browser. A plan with no token on
         a destructive statement is a plan this build must not apply — the token is the confirmation
         and its absence means the service did not mint one. */
      if (planned.destructive) {
        setRegion({ kind: "confirming", plan: planned });
        setConfirming(planned);
        return;
      }
      applyNow(planned, gen);
    });
  };

  const applyNow = (planned: StatementPlan, gen: number = nextGeneration()): void => {
    const refusal = untrack(runRefusal);
    if (refusal !== undefined) { failWith({ kind: "forbidden", message: refusal }); return; }
    setRegion({ kind: "running" });
    void apply.run(planned).then((outcome) => {
      if (gen !== generation) return;
      setConfirming(undefined);
      if (outcome.kind !== "done") {
        failWith(outcome);
        return;
      }
      setRegion(regionFor(outcome.value));
    });
  };

  const openStream = (statement: string): void => {
    const open: PushQueryOpener = props.openStream ?? ((clusterId, sql, subscriber) =>
      openPushQuery(clusterId, sql, subscriber, undefined, (path) => kui.api.url(path)));
    const gen = generation;
    // Stream frames can arrive in one microtask; do not fold from a stale signal snapshot.
    let current: ResultRegion = OPENING;
    const update = (change: (held: ResultRegion) => ResultRegion): void => {
      if (gen !== generation) return;
      current = change(current);
      setRegion(current);
    };
    stream = open(props.clusterId, statement, {
      onColumns: (columns) => update((held) => withColumns(held, columns)),
      onRow: (row) => update((held) => appendRow(held, row)),
      onDone: () => update((held) => endLive(held, "the server completed the query.")),
      onError: (error) => {
        /* A decode failure is informational: one malformed frame must not end a query that is
           otherwise delivering good rows, which is the kernel's rule and the message browser's too.
           The other two are terminal and the rows already received are kept. */
        if (error.kind === "decode") return;
        const message = error.kind === "server" ? userMessage(error.error) : error.cause;
        update((held) => interrupt(held, message));
      },
    });
  };

  const onCancel = (): void => {
    nextGeneration();
    stopStream();
    // `endLive` only knows what to do with a live stream's rows; a plan or apply still in flight
    // has produced nothing to keep, so cancelling it is a return to idle rather than a fake "ended".
    setRegion((held) => (held.kind === "streaming" ? endLive(held, "you stopped it.") : IDLE));
  };

  const onClear = (): void => {
    nextGeneration();
    stopStream();
    setConfirming(undefined);
    setSql("");
    setRegion(IDLE);
  };

  return (
    <section class="kui-ksql-screen" aria-label="ksqlDB">
      <PageHeader
        title="ksqlDB"
        /* No voice line until something has answered. A sentence about how many streams there are,
           written over a document nobody has read yet, is the product asserting what it does not
           know — and a failed read has its own sentence below, where a reader looks. */
        voice={voiceOf(objects.state())}
        testId="ksql-header"
      />

      <Show when={objects.state().kind === "loading"}>
        <p role="status" data-testid="ksql-loading">
          Asking the ksqlDB server what it is running…
        </p>
      </Show>

      <Show when={objects.state().kind === "not-configured"}>
        <EmptyState
          kind="empty"
          title="No ksqlDB server is configured for this cluster."
          description={NOT_CONFIGURED}
          testId="ksql-not-configured"
        />
      </Show>

      <Show when={objects.state().kind === "forbidden"}>
        {/* No retry. A permission decision does not change because somebody pressed a button, and a
            button that cannot help teaches an operator that this product's buttons do nothing. */}
        <EmptyState
          kind="forbidden"
          title="You do not have permission to see this cluster's ksqlDB."
          description="Ask for KSQL:VIEW on this cluster."
          testId="ksql-forbidden"
        />
      </Show>

      <Show when={failureOf(objects.state())}>
        {(failed) => (
          <EmptyState
            kind="unavailable"
            title="KUI could not read the ksqlDB object list."
            description={failed().message}
            code={failed().code}
            testId="ksql-failed"
          />
        )}
      </Show>

      <Show when={staleReason(objects.state())}>
        {(reason) => <Banner tone="warning" message={reason()} testId="ksql-stale" />}
      </Show>

      <Show when={valueIn(objects.state())}>
        {(held) => (
          <>
            {/* Rows the server returned and KUI could not describe, named rather than dropped: a
                row missing from a list is indistinguishable from a row that is not there. The
                sentence is bounded — see `MAX_NAMED_UNREADABLE` — because the list on the wire is
                not, and a banner naming ten thousand rows is a banner nobody reads. */}
            <Show when={held().unreadable.length > 0}>
              <Banner
                tone="warning"
                message={unreadableSentence(held().unreadable)}
                testId="ksql-unreadable"
              />
            </Show>

            <KsqlWorkspace
              objects={workspaceObjects(held())}
              sql={sql()}
              onSql={setSql}
              /* Disabled, with the sentence below saying why. `KsqlWorkspace` disables the control
                 when no handler is given, which is the honest rendering of a setting this wire has
                 nowhere to carry — see `OFFSET_RESET_NOT_SETTABLE`. */
              offsetReset=""
              running={isLive(region())}
              onRun={onRun}
              onCancel={onCancel}
              onClear={onClear}
              runDisabledReason={runRefusal()}
              result={<KsqlResult region={region()} />}
              testId="ksql-workspace"
            />

            <p class="kui-ksql-screen__note" data-testid="ksql-offset-note">
              {OFFSET_RESET_NOT_SETTABLE}
            </p>

            <Show when={held().items.length === 0}>
              <p class="kui-ksql-screen__empty" role="status" data-testid="ksql-no-objects">
                {NO_OBJECTS}
              </p>
            </Show>

            <RunningQueries queries={held().items.filter((one) => one.kind === "query")} />
          </>
        )}
      </Show>

      <ConfirmStatement
        plan={confirming()}
        busy={apply.busy()}
        onConfirm={() => {
          const planned = confirming();
          if (planned !== undefined) applyNow(planned);
        }}
        onCancel={() => {
          nextGeneration();
          setConfirming(undefined);
          setRegion(IDLE);
        }}
      />
    </section>
  );
}

/**
 * The objects the kernel's workspace draws.
 *
 * `KsqlObject.kind` is `stream | table` and this wire's is wider, so a query, a topic or a kind
 * this build has never seen is left out of the pane — queries have their own table below and the
 * other two are counted in the voice line, which is what keeps "left out" from meaning "hidden".
 * The server's order is kept: it orders by kind and then by name, and a row that moves between
 * polls is one an operator cannot click accurately.
 */
function workspaceObjects(objects: KsqlObjects): readonly KsqlObject[] {
  return objects.items
    .filter((one) => one.kind === "stream" || one.kind === "table")
    .map((one) => ({
      name: one.name,
      kind: one.kind === "table" ? "table" : "stream",
      topic: one.topic,
    }));
}

/**
 * The failure sentence for a statement that did not run, in the words whoever refused it used.
 *
 * `forbidden` and `failed` both carry one and neither is paraphrased. The remaining arms cannot be
 * reached from a settled `run()` and answer a sentence rather than an empty string, because an
 * empty one renders as a blank panel that says the product knows and will not tell.
 */
function sentenceOf(outcome: { readonly kind: string; readonly message?: string }): string {
  if (outcome.kind === "failed" || outcome.kind === "forbidden") {
    return outcome.message ?? "The ksqlDB server refused that, and did not say why.";
  }
  return "The ksqlDB server did not accept that, and did not say why.";
}

function voiceOf(state: Fetched<KsqlObjects>): string | undefined {
  const held = valueIn(state);
  return held === undefined ? undefined : ksqlVoice(held);
}

function failureOf(
  state: Fetched<KsqlObjects>,
): { readonly message: string; readonly code: string } | undefined {
  return state.kind === "failed" ? { message: state.message, code: state.code } : undefined;
}

function staleReason(state: Fetched<KsqlObjects>): string | undefined {
  return state.kind === "stale" ? state.reason : undefined;
}

/** The value a query holds, including a stale one — which is a real answer with a badge on it. */
function valueIn(state: Fetched<KsqlObjects>): KsqlObjects | undefined {
  return state.kind === "ready" || state.kind === "stale" ? state.value : undefined;
}
