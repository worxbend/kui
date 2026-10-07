/**
 * Adding a cluster to this KUI, changing one, and removing one.
 *
 * ## Why a cluster's *origin* decides whether it can be edited here
 *
 * A cluster can arrive two ways: from the deployment's configuration file, or from this screen. One
 * of those is under version control and reproducible, and the other is not — so a file-defined
 * cluster is shown and is not editable, because a UI that silently overrode the file would make the
 * file a lie and the next deployment would quietly change the cluster back.
 *
 * ## Why the password box starts empty even when a password is stored
 *
 * KUI never sends a stored credential back to the browser, so there is nothing to put in the box.
 * The alternative — drawing dots to suggest a value is there — would let somebody who did not touch
 * the field wipe the credential by saving, because a `PUT` replaces the whole record and the write
 * contract has no "leave it as it was" value. The screen says so instead.
 *
 * ## Test before save, and it is not a formality
 *
 * `POST /clusters/connection-test` takes the same request the save would and reports whether KUI can
 * reach the cluster with it. Getting a broker address or a SASL mechanism wrong is the ordinary
 * failure here, and finding out at save time means a cluster registered in a state that does not
 * work — which then shows up on the dashboard as an outage.
 */
import { For, Show, createSignal } from "solid-js";
import type { JSX } from "@solidjs/web";
import {
  Banner,
  Button,
  Card,
  Checkbox,
  ConfirmDialog,
  Select,
  Skeleton,
  StatusPill,
  TextField,
  type Mutation,
} from "@kui/kernel";
import {
  MECHANISMS,
  PASSWORD_WARNING,
  PROTOCOLS,
  TUNING_WARNING,
  isSasl,
  isTls,
  suggestId,
  toRequest,
  type ClusterForm,
} from "./clusterForm.js";
import { isEditable, isRemovable, type ClusterOrigin } from "./data.js";

/** One configured cluster, as this screen needs it. */
export interface ManagedCluster {
  readonly id: string;
  readonly name: string;
  readonly bootstrapServers: string;
  readonly readOnly: boolean;
  /** Where the definition comes from, which decides what may be done to it. See the header. */
  readonly origin: ClusterOrigin;
  /** The optimistic-concurrency version, sent back as `If-Match`. */
  readonly version: number | undefined;
  readonly security: { readonly protocol: string; readonly mechanism: string | null };
}

/** What a connection test came back with. */
export interface Connectivity {
  readonly reachable: boolean;
  readonly status: string;
  readonly detail?: string | undefined;
}

export interface ClusterAdminProps {
  readonly clusters: readonly ManagedCluster[];
  readonly loading?: boolean | undefined;
  /** The form on screen, or `undefined` when nothing is being edited. */
  readonly editing: { readonly id: string | undefined; readonly form: ClusterForm } | undefined;
  /**
   * Opens a blank form.
   *
   * Separate from {@link onEdit} and from {@link onCancel}, because one callback taking
   * `ManagedCluster | undefined` had to mean both "add a new one" and "close the form", and it
   * quietly meant the second: clicking Add cleared the form instead of opening one.
   */
  readonly onAdd: () => void;
  readonly onEdit: (cluster: ManagedCluster) => void;
  readonly onCancel: () => void;
  readonly onFormChange: (form: ClusterForm) => void;
  readonly onSave: () => void;
  readonly onTest: () => void;
  readonly onDelete: (cluster: ManagedCluster) => void | Promise<boolean>;
  readonly connectivity?: Connectivity | undefined;
  readonly saveState: Mutation<unknown>;
  readonly testState: Mutation<unknown>;
  readonly deleteState: Mutation<unknown>;
  /** Absent when this principal may change clusters. Present disables every write control. */
  readonly disabledReason?: string | undefined;
  /** Set when the initial `GET /clusters` call failed. Replaces the list with a retry card. */
  readonly failure?: { readonly message: string; readonly code: string; readonly onRetry: () => void } | undefined;
}

export function ClusterAdmin(props: ClusterAdminProps): JSX.Element {
  const [confirming, setConfirming] = createSignal<ManagedCluster | undefined>(undefined);

  const problems = (): readonly string[] => {
    const current = props.editing;
    if (current === undefined) return [];
    const answer = toRequest(current.form);
    return answer.ok ? [] : answer.problems;
  };

  const failure = (state: Mutation<unknown>) =>
    state.kind === "failed" || state.kind === "forbidden" ? state : undefined;

  return (
    <section class="kui-cluster-admin" aria-label="Manage clusters">
      <header class="kui-cluster-admin__head">
        <h1 class="kui-cluster-admin__title">Clusters</h1>
        <Show
          when={props.disabledReason === undefined}
          fallback={
            <Button
              variant="primary"
              icon="plus"
              disabled
              disabledReason={props.disabledReason ?? ""}
            >
              Add a cluster
            </Button>
          }
        >
          <Button variant="primary" icon="plus" onClick={props.onAdd}>
            Add a cluster
          </Button>
        </Show>
      </header>

      <Show
        when={props.failure === undefined}
        fallback={
          <Card
            title="Clusters"
            state="unavailable"
            message={props.failure?.message}
            description="KUI could not read its cluster configuration."
            code={props.failure?.code}
            stateAction={
              <Button variant="secondary" icon="refresh" onClick={() => props.failure?.onRetry()}>
                Retry
              </Button>
            }
            testId="cluster-admin-card"
          />
        }
      >
        <Show
          when={props.loading !== true}
          fallback={
            <ul class="kui-cluster-admin__list" aria-hidden="true">
              <For each={[0, 1, 2]}>{() => <li><Skeleton height="4rem" /></li>}</For>
            </ul>
          }
        >
          <ul class="kui-cluster-admin__list">
            <For each={props.clusters}>
              {(cluster) => (
                <li>
                  <Card title={cluster.name}>
                <div class="kui-cluster-admin__row">
                  <code class="kui-cluster-admin__brokers">{cluster.bootstrapServers}</code>
                  <StatusPill tone="neutral">{cluster.security.protocol}</StatusPill>
                  <Show when={cluster.readOnly}>
                    <StatusPill tone="warning">read-only</StatusPill>
                  </Show>

                  <Show
                    when={isEditable(cluster.origin)}
                    fallback={
                      /* Defined in the deployment's configuration file and nowhere else. Shown, not
                         editable: a UI that silently overrode the file would make the file a lie,
                         and the next deployment would quietly change the cluster back. */
                      <span class="kui-table__cell-muted">
                        defined in this deployment's configuration file
                      </span>
                    }
                  >
                    <div class="kui-cluster-admin__actions">
                      <Show
                        when={props.disabledReason === undefined}
                        fallback={
                          <Button
                            variant="ghost"
                            disabled
                            disabledReason={props.disabledReason ?? ""}
                          >
                            Edit
                          </Button>
                        }
                      >
                        <Button variant="ghost" onClick={() => props.onEdit(cluster)}>
                          Edit
                        </Button>
                        <Show
                          when={isRemovable(cluster.origin)}
                          fallback={
                            /* The file still names it, so deleting the stored record would leave the
                               row on screen — which reads as a delete that silently failed. The
                               server refuses it; saying so before the click beats saying it after. */
                            <Button
                              variant="danger"
                              icon="trash"
                              disabled
                              disabledReason="This cluster is also named in the deployment's configuration file, so removing the stored settings would leave it here with the file's settings. Remove it from the file instead."
                            >
                              Remove
                            </Button>
                          }
                        >
                          <Button
                            variant="danger"
                            icon="trash"
                            onClick={() => setConfirming(cluster)}
                          >
                            Remove
                          </Button>
                        </Show>
                      </Show>
                    </div>
                  </Show>
                </div>
              </Card>
                </li>
              )}
            </For>
          </ul>
        </Show>
      </Show>

      <Show when={props.editing}>
        {(current) => (
          <Card
            title={current().id === undefined ? "Add a cluster" : `Edit ${current().form.name}`}
          >
            <form
              class="kui-cluster-admin__form"
              onSubmit={(event) => {
                event.preventDefault();
                if (props.disabledReason === undefined && props.saveState.kind !== "running") props.onSave();
              }}
            >
              <TextField
                label="Name"
                value={current().form.name}
                onInput={(name) =>
                  props.onFormChange({
                    ...current().form,
                    name,
                    /* The id follows the name until somebody edits it, and only while adding: an
                       edit addresses an existing record, and moving it would break every link and
                       bookmark anybody has kept. */
                    ...(current().id === undefined &&
                    current().form.id === suggestId(current().form.name)
                      ? { id: suggestId(name) }
                      : {}),
                  })
                }
                required
              />
              <Show
                when={current().id === undefined}
                fallback={
                  <p class="kui-cluster-admin__help">
                    This cluster's id is <code>{current().form.id}</code>. It is in every link and
                    every audit line for this cluster, so it cannot be changed here.
                  </p>
                }
              >
                <TextField
                  label="Id"
                  value={current().form.id}
                  onInput={(id) => props.onFormChange({ ...current().form, id })}
                  mono
                  required
                  placeholder="staging-eu-west"
                />
                <p class="kui-cluster-admin__help">
                  Lowercase letters, digits and hyphens. It appears in every URL for this cluster
                  and cannot be changed afterwards.
                </p>
              </Show>
              <TextField
                label="Broker addresses"
                value={current().form.bootstrapServers}
                onInput={(bootstrapServers) =>
                  props.onFormChange({ ...current().form, bootstrapServers })
                }
                mono
                required
                placeholder="broker-1:9092,broker-2:9092"
              />
              <Checkbox
                label="Read-only"
                checked={current().form.readOnly}
                onChange={(readOnly) => props.onFormChange({ ...current().form, readOnly })}
              />
              <p class="kui-cluster-admin__help">
                {/* ADR-047: a property of how KUI is configured for this cluster, not of the
                    principal. Everything that changes the cluster is refused, including for an
                    operator who holds every permission. */}
                A read-only cluster can be looked at and not changed, whatever permissions anybody
                holds.
              </p>

              <Select
                label="Security protocol"
                value={current().form.protocol}
                options={PROTOCOLS}
                onChange={(protocol) => props.onFormChange({ ...current().form, protocol })}
              />

              <Show when={isSasl(current().form)}>
                <Select
                  label="SASL mechanism"
                  value={current().form.mechanism}
                  options={MECHANISMS}
                  onChange={(mechanism) => props.onFormChange({ ...current().form, mechanism })}
                />
                <TextField
                  label="Username"
                  value={current().form.username}
                  onInput={(username) => props.onFormChange({ ...current().form, username })}
                />
                <TextField
                  label="Password"
                  type="password"
                  value={current().form.password}
                  onInput={(password) => props.onFormChange({ ...current().form, password })}
                />
                <Show when={current().id !== undefined}>
                  <p class="kui-cluster-admin__help">{PASSWORD_WARNING}</p>
                </Show>
              </Show>

              <Show when={isTls(current().form)}>
                <Checkbox
                  label="Verify the broker's hostname"
                  checked={current().form.verifyHostname}
                  onChange={(verifyHostname) =>
                    props.onFormChange({ ...current().form, verifyHostname })
                  }
                />
                <p class="kui-cluster-admin__help">
                  {/* Named rather than left to be discovered at save time: this form cannot supply a
                      truststore or a keystore, and that is a real gap. A textarea for a base64
                      keystore is not a usable way to give KUI a certificate. */}
                  A certificate the JVM's default trust store already accepts — which is every
                  managed service — works from this form. A private certificate authority has to be
                  configured in the deployment's file.
                </p>
              </Show>

              <details class="kui-cluster-admin__tuning">
                <summary>Admin client tuning</summary>
                <p class="kui-cluster-admin__help">{TUNING_WARNING}</p>
                <TextField
                  label="Timeout (ms)"
                  type="number"
                  value={current().form.timeoutMs}
                  onInput={(timeoutMs) => props.onFormChange({ ...current().form, timeoutMs })}
                />
                <TextField
                  label="Batch size"
                  type="number"
                  value={current().form.batchSize}
                  onInput={(batchSize) => props.onFormChange({ ...current().form, batchSize })}
                />
                <TextField
                  label="Parallelism"
                  type="number"
                  value={current().form.parallelism}
                  onInput={(parallelism) => props.onFormChange({ ...current().form, parallelism })}
                />
              </details>

              {/* Every reason at once. Somebody who got three fields wrong should be told about all
                  three rather than discovering them one save at a time. */}
              <Show when={problems().length > 0}>
                <ul class="kui-cluster-admin__problems">
                  <For each={problems()}>{(problem) => <li>{problem}</li>}</For>
                </ul>
              </Show>

              <Show when={props.connectivity}>
                {(result) => (
                  <Banner
                    tone={result().reachable ? "info" : "warning"}
                    message={
                      result().reachable
                        ? "KUI reached this cluster with these settings."
                        : `KUI could not reach this cluster: ${result().detail ?? result().status}`
                    }
                  />
                )}
              </Show>

              <Show when={failure(props.testState)}>
                {(problem) => (
                  <Banner tone="danger" message={problem().message}
                    code={problem().kind === "failed" ? (problem() as { code: string }).code : undefined} />
                )}
              </Show>

              <Show when={failure(props.saveState)}>
                {(problem) => (
                  <Banner
                    tone="danger"
                    message={problem().message}
                    code={
                      problem().kind === "failed" ? (problem() as { code: string }).code : undefined
                    }
                  />
                )}
              </Show>

              <div class="kui-cluster-admin__form-actions">
                <Button variant="ghost" onClick={props.onCancel}>
                  Cancel
                </Button>
                {/* Before Save, not after: getting a broker address or a mechanism wrong is the
                    ordinary failure here, and finding out at save time leaves a cluster registered
                    in a state that does not work — which then shows on the dashboard as an outage. */}
                <Show when={props.disabledReason === undefined} fallback={
                  <Button variant="secondary" icon="refresh" disabled disabledReason={props.disabledReason ?? "Configuration changes are not permitted."}>Test the connection</Button>
                }>
                  <Button variant="secondary" icon="refresh" busy={props.testState.kind === "running"}
                    onClick={() => { if (props.disabledReason === undefined) props.onTest(); }}>
                    Test the connection
                  </Button>
                </Show>
                <Show
                  when={props.disabledReason === undefined && problems().length === 0 && props.saveState.kind !== "running"}
                  fallback={
                    <Button
                      variant="primary"
                      icon="check"
                      busy={props.saveState.kind === "running"}
                      disabled
                      disabledReason={
                        props.disabledReason ?? (props.saveState.kind === "running"
                          ? "The cluster is being saved."
                          : (problems()[0] ?? "This form is not complete."))
                      }
                    >
                      Save
                    </Button>
                  }
                >
                  <Button variant="primary" icon="check" type="submit">
                    Save
                  </Button>
                </Show>
              </div>
            </form>
          </Card>
        )}
      </Show>

      <Show when={confirming()}>
        {(cluster) => (
          <ConfirmDialog
            open={props.disabledReason === undefined}
            onClose={() => setConfirming(undefined)}
            title={`Remove ${cluster().name}?`}
            /* What is destroyed is the *registration*, not the cluster. Saying so plainly is the
               point: an operator who thinks this deletes their Kafka will not use it, and one who
               thinks it does and is wrong will be very relieved. */
            consequence="Removes this cluster from KUI. The Kafka cluster itself, its topics and its data are untouched, and it can be added again with the same settings."
            confirmLabel="Remove cluster"
            confirmIcon="trash"
            typeToConfirm={cluster().name}
            busy={props.deleteState.kind === "running"}
            error={
              failure(props.deleteState) === undefined
                ? undefined
                : { message: failure(props.deleteState)!.message }
            }
            onConfirm={() => {
              if (props.disabledReason !== undefined) return;
              const target = cluster();
              void Promise.resolve(props.onDelete(target)).then((removed) => {
                if (removed && confirming() === target) setConfirming(undefined);
              });
            }}
          />
        )}
      </Show>
    </section>
  );
}
