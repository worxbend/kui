/**
 * The Connect feature's route entry.
 *
 *   /clusters/:clusterId/connect        the connectors on this cluster's Connect workers
 *
 * The route itself is the shell's — `routing/routes.tsx` holds every URL pattern as a literal, and
 * a feature that assembled its own address would go on compiling after somebody renamed a segment
 * and would simply stop matching. This package is what is behind the route; the `ECOSYSTEM` nav row
 * is W7-04's `registry.ts` entry and the route is W7-06's.
 *
 * ## The permission question is asked here, once, with a subject in it
 *
 * `kui.permits(Actions.ConnectOperate, operateSubject(connector))` — **with the subject**, and the
 * subject is the *Connect cluster's* name because that is the question the server asks:
 * `ConnectEndpoints.operating` declares
 * `ResourceRequirement.named(Resource.Connect, connectName, Action.ConnectOperate)`. A browser that
 * asked a different question would produce a control that is enabled and then refused, or one that
 * is disabled for somebody who holds the permission. (`ConnectorFallbackActions` and the
 * `<connect>/<connector>` spelling belong to a `Resource.Connector` requirement this endpoint
 * deliberately does not declare; ADR-054 §3 carries that gap and `model.ts` restates it.)
 *
 * The subjectless form asks a **weaker** question, and `@kui/kernel`'s `session.ts` says so in as
 * many words: *"asked without a name this can only answer 'do they hold this action on anything of
 * this kind', which is the right answer for a list heading or a create button and the wrong one for
 * a row's delete button."* Wave 6 shipped two consumer-group controls asking the weaker question
 * and no case could see it, because that packet's test harness threw the subject away before
 * comparing. This package's harness (`testing.ts`) evaluates real grants through `@kui/kernel`'s
 * own `grantsAllow`, so `connect.test.tsx`'s *"a principal granted OPERATE on one Connect cluster
 * may not operate a connector on another"* goes red the moment the subject is dropped.
 *
 * Asking it *here* rather than only inside the control is the other half. Wave 5 shipped a
 * destructive consumer-group control whose component refused correctly and whose route never
 * mounted without the permission, so the wiring that decided it was asserted by nothing at all.
 * `ConnectorPanel` cannot invent a handler it was not given; this is where it is given or withheld.
 *
 * ## The three commands do not paint their own outcome
 *
 * Connect answers a pause, a resume and a restart with `202 Accepted` and an empty body, and the
 * connector's state changes when its workers have agreed. So a command that succeeds re-reads the
 * list, and a command that is refused leaves the card exactly as it was with the server's sentence
 * beneath it. Re-reading after a refusal would replace the reason with a spinner and then with the
 * same card, which reads as though nothing had been clicked.
 */
import { Show, createSignal } from "solid-js";
import type { JSX } from "@solidjs/web";
import { useParams } from "@solidjs/router";
import { Actions } from "@kui/api";
import {
  PageHeader,
  createMutation,
  useKui,
  useQuery,
  writeBlockedReason,
  type Fetched,
  type Mutation,
  type QueryRegistry,
} from "@kui/kernel";

import { ConnectorList } from "./ConnectorList.jsx";
import { command, fetchConnectors, type ConnectorCommand } from "./data.js";
import { connectVoice, connectorLabel, operateAction, operateSubject } from "./model.js";
import type { Connector, ConnectorListing } from "./wire.js";

export default function Connect(): JSX.Element {
  const params = useParams<{ readonly clusterId?: string }>();
  return (
    <Show when={params.clusterId} fallback={<NoCluster />}>
      {(clusterId) => <ConnectScreen clusterId={clusterId()} />}
    </Show>
  );
}

function NoCluster(): JSX.Element {
  const kui = useKui();
  return (
    <section aria-label="Kafka Connect">
      <p role="status">
        No cluster is selected, so there are no Connect workers to ask.{" "}
        <a href={kui.paths.clusters()}>Choose a cluster</a> and try again.
      </p>
    </section>
  );
}

export interface ConnectScreenProps {
  readonly clusterId: string;
  /**
   * Which shared answers to read.
   *
   * A test seam and nothing else: the product passes nothing and gets the application-wide cache,
   * which is what makes two screens reading one key read one answer. A story or a case that used
   * that cache would inherit whatever the previous one left in it.
   */
  readonly queries?: QueryRegistry | undefined;
}

export function ConnectScreen(props: ConnectScreenProps): JSX.Element {
  const kui = useKui();

  const connectors = useQuery<ConnectorListing>({
    key: () => `connect:${props.clusterId}`,
    load: () => fetchConnectors(kui.api, props.clusterId),
    ...(props.queries === undefined ? {} : { registry: props.queries }),
  });

  /**
   * The gate, per connector, with the Connect cluster's name as the subject.
   *
   * This is the packet's owned rule. Removing the second argument leaves a principal who holds
   * `CONNECT:OPERATE` on *any* Connect cluster with live Pause and Restart buttons on every
   * connector of every other one — and the server refuses the call, so the operator learns only
   * that KUI's buttons sometimes do not work.
   */
  const refusalFor = (connector: Connector): string | undefined =>
    kui.writeBlocked !== undefined
      ? kui.writeBlocked(props.clusterId, Actions.ConnectOperate, operateSubject(connector))
      : writeBlockedReason({
      permitted: kui.permits(Actions.ConnectOperate, operateSubject(connector)),
      // Compatibility for standalone stories without the shell's write policy.
      readOnly: false,
      action: operateAction(connector),
    });

  /*
   * Maps, keyed by `connectorLabel`, and not a single slot: commands against different connectors
   * run concurrently (see `mutationFor` below), so a single `{subject, ...}` pair let a command
   * against connector B clear connector A's still-genuinely-in-flight busy indicator the instant B
   * started, and let B's failure banner wipe A's the instant B's own command was issued — neither
   * of which is a fact about A. Keying by subject makes each connector's indicator and banner
   * exactly as independent as its mutation already is.
   */
  const [pending, setPending] = createSignal<ReadonlyMap<string, ConnectorCommand>>(new Map());
  const [failure, setFailure] = createSignal<ReadonlyMap<string, string>>(new Map());

  /**
   * One mutation per connector, keyed by its label.
   *
   * A single shared mutation's re-entrancy guard is per-*call*, not per-connector, so a command
   * against connector B while connector A's was still outstanding would be reported as `running`
   * and dropped — a collision entirely inside KUI, not a refusal from Connect. Keying the guard by
   * `connectorLabel` lets concurrent commands against different connectors proceed independently;
   * the guard still catches a second click on the *same* connector's own button.
   */
  const mutations = new Map<
    string,
    ReturnType<
      typeof createMutation<
        [{ readonly connector: Connector; readonly which: ConnectorCommand }],
        void
      >
    >
  >();

  const mutationFor = (
    subject: string,
  ): ReturnType<
    typeof createMutation<[{ readonly connector: Connector; readonly which: ConnectorCommand }], void>
  > => {
    const existing = mutations.get(subject);
    if (existing !== undefined) return existing;
    const created = createMutation(
      (request: { readonly connector: Connector; readonly which: ConnectorCommand }) =>
        command(
          kui.api,
          props.clusterId,
          { connect: request.connector.connect, name: request.connector.name },
          request.which,
        ),
    );
    mutations.set(subject, created);
    return created;
  };

  const onCommand = (connector: Connector, which: ConnectorCommand): void => {
    if (refusalFor(connector) !== undefined) return;
    const subject = connectorLabel(connector);
    setPending((current) => new Map(current).set(subject, which));
    // Only this connector's own prior failure is cleared by a fresh attempt — another
    // connector's banner describes an outcome nobody just retried.
    setFailure((current) => {
      if (!current.has(subject)) return current;
      const next = new Map(current);
      next.delete(subject);
      return next;
    });
    void mutationFor(subject)
      .run({ connector, which })
      .then((outcome) => {
        setPending((current) => {
          const next = new Map(current);
          next.delete(subject);
          return next;
        });
        if (outcome.kind === "done") {
          connectors.reload();
          return;
        }
        /* The server's own sentence, beside the connector it refused. Never "something
           went wrong". */
        setFailure((current) => new Map(current).set(subject, sentenceOf(outcome)));
      });
  };

  return (
    <section class="kui-connect-screen" aria-label="Kafka Connect">
      <PageHeader
        title="Kafka Connect"
        /* No voice line until something has answered. A sentence about how many connectors there
           are, written over a document nobody has read yet, is the product asserting what it does
           not know — and a failed read has its own sentence below, where a reader looks. */
        voice={voiceOf(connectors.state())}
        testId="connect-header"
      />
      <ConnectorList
        state={connectors.state()}
        refusalFor={refusalFor}
        onRetry={() => connectors.reload()}
        pending={pending()}
        failure={failure()}
        onCommand={onCommand}
      />
    </section>
  );
}

/**
 * The failure sentence for a command that did not happen, in the words whoever refused it used.
 *
 * `forbidden` and `failed` both carry one and neither is paraphrased. `running` *can* reach here —
 * a second click on the same connector's own button, still guarded by its own mutation — and gets
 * its own sentence rather than being told the Connect cluster refused it, which it never saw. The
 * remaining arm (`idle`) cannot be reached from a settled `run()` and answers a sentence rather
 * than an empty string, because an empty one renders as a blank red block that says the product
 * knows and will not tell.
 */
function sentenceOf(outcome: Mutation<void>): string {
  if (outcome.kind === "failed" || outcome.kind === "forbidden") return outcome.message;
  if (outcome.kind === "running") {
    return "KUI is still waiting on this connector's last command to finish.";
  }
  return "The Connect cluster did not accept that, and did not say why.";
}

function voiceOf(state: Fetched<ConnectorListing>): string | undefined {
  const held = valueIn(state);
  return held === undefined ? undefined : connectVoice(held);
}

/** The value a query holds, including a stale one — which is a real answer with a badge on it. */
function valueIn(state: Fetched<ConnectorListing>): ConnectorListing | undefined {
  return state.kind === "ready" || state.kind === "stale" ? state.value : undefined;
}
