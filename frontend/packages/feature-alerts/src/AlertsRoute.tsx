/**
 * The alerts feature's route entry.
 *
 *   /clusters/:clusterId/alerts        the feed, with its two filters
 *
 * ## Why this is a route and not a dashboard tab
 *
 * `SCREENS-V4.md` §4.4 draws Alerts as the dashboard's fourth tab, and it is also a destination in
 * its own right. Wave 6 settles it as a route with a navigation row: the dashboard's tabs live in
 * the shell's `overview/`, and a feature package reaching into them would invert the dependency the
 * microfrontend split exists to keep. The shell adds the route and the nav row; this package is what
 * is behind them, and the same {@link AlertsFeed} can be mounted one third of the width on the
 * Overview tab by whoever owns that grid.
 *
 * ## The count, the filters, and which of them the browser is allowed to do arithmetic on
 *
 * The filters are the reader's, applied to the page this browser is holding, and the caption under
 * the list says so in those words. The open count in the card's header is the alerts service's own
 * figure and survives every filter untouched — a filtered count would be the browser answering a
 * question the API was asked, which is the contract this packet shares with every other card packet
 * in the wave.
 *
 * ## Acknowledgement
 *
 * A write, gated on `ALERTS:ACKNOWLEDGE` **here**, at the wiring, and not only in the component that
 * draws the button. Wave 5 shipped a destructive consumer-group control whose component refused
 * correctly and whose route never mounted without the permission, so the wiring that decided it was
 * asserted by nothing at all and `true ?` in place of the check left 1449 cases green. The case
 * `alertsRoute.test.tsx` names — "a principal without ALERTS:ACKNOWLEDGE is never handed an enabled
 * acknowledge control" — mounts this route unpermitted for that reason.
 */
import { Show, createMemo, createSignal } from "solid-js";
import type { JSX } from "@solidjs/web";
import { useParams } from "@solidjs/router";
import { Actions } from "@kui/api";
import {
  PageHeader,
  SingleSelectChips,
  createMutation,
  useAlerts,
  useKui,
  useQuery,
  writeBlockedReason,
  type AlertFeed,
  type Fetched,
} from "@kui/kernel";

import { AlertsFeed } from "./AlertsFeed.jsx";
import { RuleReports } from "./RuleReports.jsx";
import { acknowledge, fetchClusterWriteState, type ClusterWriteState } from "./data.js";
import {
  feedVoice,
  severityChip,
  SEVERITIES,
  type AlertEvent,
  type AlertsFeedPage,
  type FeedFilter,
  type SeverityFilter,
  type StateFilter,
} from "./model.js";

export default function Alerts(): JSX.Element {
  const params = useParams<{ readonly clusterId?: string }>();
  return (
    <Show when={params.clusterId} fallback={<NoCluster />}>
      {(clusterId) => <AlertsScreen clusterId={clusterId()} />}
    </Show>
  );
}

function NoCluster(): JSX.Element {
  const kui = useKui();
  return (
    <section aria-label="Alerts">
      <p role="status">
        No cluster is selected, so there are no alerts to read.{" "}
        <a href={kui.paths.clusters()}>Choose a cluster</a> and try again.
      </p>
    </section>
  );
}

/**
 * Whether this cluster is registered read-only, for the one write control on this screen.
 *
 * ADR-047's flag is a property of the **deployment**, not of the principal, and
 * `writeBlockedReason` prints a different sentence for each because the two need different actions
 * from the reader: telling somebody to ask an administrator for a permission they already hold
 * wastes their afternoon.
 *
 * One query, keyed by the cluster and by nothing else, on the shared registry — which is the same
 * key `feature-topics` uses, so a reader who walks from the topic list to the alerts feed asks the
 * gateway once between the two screens rather than once per screen.
 *
 * Answers `false` while the question is still out, and `false` when it could not be asked at all,
 * for the reason `useKui().permits` gives for the same choice: a screenful of refusals drawn over a
 * fact KUI does not have is worse than a refusal that arrives from the server, and the server is
 * the authority either way. This decides only whether a control explains itself in advance.
 */
function useClusterReadOnly(clusterId: () => string): () => boolean {
  const kui = useKui();
  const query = useQuery<ClusterWriteState>({
    key: () => kui.writeBlocked === undefined ? `cluster-write-state|${clusterId()}` : undefined,
    load: () => fetchClusterWriteState(kui.api, clusterId()),
  });
  return () => {
    const current = query.state();
    return (current.kind === "ready" || current.kind === "stale") && current.value.readOnly;
  };
}

export interface AlertsScreenProps {
  readonly clusterId: string;
  /** The clock, held still by stories and tests so a relative age is a fact and not a race. */
  readonly now?: (() => Date) | undefined;
}

export function AlertsScreen(props: AlertsScreenProps): JSX.Element {
  const kui = useKui();
  const alerts = useAlerts();
  const [severity, setSeverity] = createSignal<SeverityFilter>("all");
  const [lifecycle, setLifecycle] = createSignal<StateFilter>("all");
  const filter = createMemo<FeedFilter>(() => ({ severity: severity(), state: lifecycle() }));

  /* The deployment's own answer to "may anything here write at all", which the gate below asks
     before it asks about the principal. See `useClusterReadOnly`. */
  const readOnly = useClusterReadOnly(() => props.clusterId);

  /*
   * The gate for the whole control, decided once and threaded into the card. `undefined` for
   * `onAcknowledge` is what makes the button draw disabled with its reason: the component cannot
   * invent a handler it was not given, so there is no path from an unpermitted principal to an
   * enabled control.
   *
   * Two facts, and the button says which one applies. Until wave 8 the second was the literal
   * `false`, so a cluster somebody had deliberately registered read-only handed a permitted
   * principal a fully enabled `Acknowledge`, issued the write, and let the gateway refuse it — the
   * defect `feature-topics` had on seven controls and closed in wave 7, shipped here on one.
   */
  const refusal = createMemo(() =>
    kui.writeBlocked !== undefined
      ? kui.writeBlocked(props.clusterId, Actions.AlertsAcknowledge)
      : writeBlockedReason({
      permitted: kui.permits(Actions.AlertsAcknowledge),
      readOnly: readOnly(),
      action: "acknowledge alerts on this cluster",
    }),
  );

  const [pending, setPending] = createSignal<string | undefined>(undefined);
  const acknowledgement = createMutation((eventId: string) =>
    acknowledge(kui.api, props.clusterId, eventId),
  );

  const onAcknowledge = (event: AlertEvent): void => {
    if (refusal() !== undefined) return;
    setPending(event.id);
    void acknowledgement.run(event.id).then((outcome) => {
      setPending(undefined);
      // Only a success re-reads the feed. A refusal leaves the row exactly as it was and says why
      // above the list: refetching after a refusal would replace the reason with a spinner and then
      // with the same unacknowledged row, which reads as though nothing had been clicked.
      if (outcome.kind === "done") alerts.refresh();
    });
  };

  const failure = createMemo(() => {
    const state = acknowledgement.state();
    if (state.kind === "failed") return { message: state.message, code: state.code };
    if (state.kind === "forbidden") return { message: state.message };
    return undefined;
  });

  const page = (): AlertsFeedPage | undefined => valueIn(alerts.feed());

  return (
    <section class="kui-alerts-screen" aria-label="Alerts">
      <PageHeader
        title="Alerts"
        /* No voice line until the feed has answered: a sentence about how much is open, written
           over a document nobody has read yet, is the product asserting something it does not know. */
        voice={voiceOf(alerts.feed())}
        testId="alerts-header"
      />
      <div class="kui-alerts-screen__filters">
        <SingleSelectChips
          label="Severity"
          testId="alerts-severity-filter"
          value={severity()}
          onChange={setSeverity}
          options={[
            { value: "all", label: "All severities" },
            ...SEVERITIES.map((one) => ({ value: one, label: severityChip(one) })),
          ]}
        />
        <SingleSelectChips
          label="Alert state"
          testId="alerts-state-filter"
          value={lifecycle()}
          onChange={setLifecycle}
          options={[
            { value: "all", label: "All" },
            { value: "open", label: "Open" },
            { value: "resolved", label: "Resolved" },
          ]}
        />
      </div>
      <AlertsFeed
        wide
        state={alerts.feed()}
        filter={filter()}
        now={props.now}
        onRetry={() => alerts.refresh()}
        acknowledging={pending()}
        acknowledgeFailure={failure()}
        {...(refusal() === undefined
          ? { onAcknowledge }
          : { acknowledgeRefusal: refusal() as string })}
      />
      {/* The rules, under the feed, because they answer the question an empty feed raises. */}
      <RuleReports reports={page()?.rules ?? []} />
    </section>
  );
}

/**
 * The voice line, or none at all until something has answered.
 *
 * A sentence about how much is open, written over a document nobody has read yet, is the product
 * asserting something it does not know — and a failed read has its own sentence in the card below,
 * which is where a reader looks for a reason.
 */
function voiceOf(state: Fetched<AlertFeed>): string | undefined {
  const held = valueIn(state);
  return held === undefined ? undefined : feedVoice(held);
}

/** The value a query holds, including a stale one — which is a real answer with a badge on it. */
function valueIn(state: Fetched<AlertFeed>): AlertFeed | undefined {
  return state.kind === "ready" || state.kind === "stale" ? state.value : undefined;
}
