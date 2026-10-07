/**
 * One consumer group: the page, the offset-reset wizard and the delete confirmation.
 *
 * `GroupDetail` and `ResetWizard` were both fully built and neither had a route. Every link the
 * group list produced — `/clusters/:clusterId/consumer-groups/:groupId` — resolved to the list
 * again, so a wizard that knows about clamping, no-op plans and expired tokens was unreachable from
 * the product.
 */
import { Show, createEffect, createSignal } from "solid-js";
import type { JSX } from "@solidjs/web";
import { useNavigate, useParams } from "@solidjs/router";
import { Actions } from "@kui/api";
import { ConfirmDialog, createMutation, notify, useKui, valueOf, type Fetched } from "@kui/kernel";
import { GroupDetail as GroupDetailPage } from "./GroupDetail.jsx";
import { fetchGroup } from "./data.js";
import { DEFAULT_POLL_MS } from "./lag.js";
import { applyReset, deleteGroup, deleteOffsets, planReset } from "./write.js";
import { subscriptions, type GroupDetail } from "./detail.js";

export function GroupRoute(): JSX.Element {
  const params = useParams<{ readonly clusterId?: string; readonly groupId?: string }>();
  return (
    <Show when={params.clusterId} fallback={<NoSubject what="cluster" />}>
      {(clusterId) => (
        <Show when={params.groupId} fallback={<NoSubject what="group" />}>
          {(groupId) => <GroupScreen clusterId={clusterId()} groupId={groupId()} />}
        </Show>
      )}
    </Show>
  );
}

function NoSubject(props: { readonly what: string }): JSX.Element {
  const kui = useKui();
  return (
    <section aria-label="Consumer group">
      <p role="status">
        This address names no {props.what}.{" "}
        <a href={kui.paths.clusters()}>Start from the cluster list</a>.
      </p>
    </section>
  );
}

function GroupScreen(props: { readonly clusterId: string; readonly groupId: string }): JSX.Element {
  const kui = useKui();
  /*
   * The router's navigation, not `window.location.assign`.
   *
   * `assign` is a *document* navigation: it tears down the application and loads a new one. The
   * toast raised on the line above it therefore went into a module-level store that ceased to exist
   * a moment later, so "a toast on every destructive success" was, for this success, a toast nobody
   * could ever have seen — while the comment beside it argued that the shell's region survives the
   * route change. It does; the reload was what did not let it.
   *
   * `resolve: false` for the reason `App.tsx` names: a `KuiPaths` address has already had the
   * deployment's base applied, and `useNavigate`'s default would apply it a second time and land on
   * `/ui/ui/clusters/…`, which matches no route.
   */
  const navigate = useNavigate();
  const [state, setState] = createSignal<Fetched<GroupDetail>>({ kind: "loading" });
  const [attempt, setAttempt] = createSignal(0);
  const [confirmingDelete, setConfirmingDelete] = createSignal(false);
  /* Which topic's offsets are about to be forgotten, or nothing. The topic is the state rather
     than a boolean beside a second signal: the confirmation names it, the request carries it and
     the receipt is read against it, and two signals is two places for them to disagree. */
  const [forgetting, setForgetting] = createSignal<string | undefined>(undefined);

  createEffect(
    () => [props.clusterId, props.groupId, attempt()] as const,
    ([clusterId, groupId]) => {
      let cancelled = false;
      let timer: ReturnType<typeof setTimeout> | undefined;
      setState({ kind: "loading" });

      // Reused for the initial fetch and every poll after it, so a group whose page an operator is
      // actively watching stays live the way the group list does — via `pollLag` — instead of going
      // stale the moment it is opened and only moving again on the next manual reload or mutation.
      const refresh = (background: boolean): void => {
        void fetchGroup(kui.api, clusterId, groupId).then((next) => {
          // Switching group while a request is out must not land the old group's offsets on the new
          // group's page: real figures for the wrong subject is the most convincing wrong data there
          // is, and this page's figures are what a reset is composed from.
          if (cancelled) return;
          // Keep last-known data only for transient failures, explicitly marked stale. An
          // authorization refusal must replace it rather than retaining now-forbidden offsets.
          setState((previous) => {
            if (background && next.kind === "failed" &&
                (previous.kind === "ready" || previous.kind === "stale")) {
              return { kind: "stale", value: previous.value, reason: next.message };
            }
            return next;
          });
          timer = setTimeout(() => refresh(true), DEFAULT_POLL_MS);
        });
      };

      refresh(false);
      return () => {
        cancelled = true;
        if (timer !== undefined) clearTimeout(timer);
      };
    },
  );

  createEffect(
    () => state(),
    (current) => {
      if (current.kind !== "loading") kui.report("feature", current.kind === "failed");
    },
  );

  const remove = createMutation(() => deleteGroup(kui.api, props.clusterId, props.groupId));
  const forget = createMutation((topic: string) =>
    deleteOffsets(kui.api, props.clusterId, props.groupId, topic),
  );

  const resetRefusal = () => kui.writeBlocked !== undefined
    ? kui.writeBlocked(props.clusterId, Actions.ConsumerGroupResetOffsets, props.groupId)
    : kui.permits(Actions.ConsumerGroupResetOffsets, props.groupId) ? undefined
      : "You do not have permission to reset this group's offsets.";
  const deleteRefusal = () => kui.writeBlocked !== undefined
    ? kui.writeBlocked(props.clusterId, Actions.ConsumerGroupDelete, props.groupId)
    : kui.permits(Actions.ConsumerGroupDelete, props.groupId) ? undefined
      : "You do not have permission to delete this consumer group.";
  const mayReset = () => resetRefusal() === undefined;
  const mayDelete = () => deleteRefusal() === undefined;

  const group = () => (state().kind === "loading" ? undefined : valueOf(state(), undefined));
  const staleReason = (): string | undefined => {
    const current = state();
    return current.kind === "stale" ? current.reason : undefined;
  };

  return (
    <Show
      when={group()}
      fallback={<Loading state={state()} onRetry={() => setAttempt(attempt() + 1)} />}
    >
      {(detail) => (
        <>
          <GroupDetailPage
            group={detail()}
            stale={staleReason()}
            listHref={kui.paths.consumerGroups(props.clusterId)}
            topicHref={(topic) => kui.paths.topic(props.clusterId, topic)}
            reset={{
              plan: async (request) => {
                const problem = resetRefusal();
                return problem === undefined ? planReset(kui.api, props.clusterId, props.groupId, request)
                  : { ok: false, problem };
              },
              apply: async (token) => {
                const problem = resetRefusal();
                if (problem !== undefined) return { ok: false, problem };
                const outcome = await applyReset(kui.api, props.clusterId, props.groupId, token);
                // The group's committed offsets have just moved. Everything on the page behind the
                // wizard — the lag, the per-partition positions — now describes the state before the
                // reset, which is the one state it must not be showing.
                if (outcome.ok) {
                  setAttempt(attempt() + 1);
                  /*
                   * The wizard's receipt is on screen and says what the broker wrote, so this is not
                   * the only confirmation — but the wizard closes and the page behind it looks
                   * exactly as it did before, only with different numbers. The toast is what
                   * survives that transition and names the group the offsets belong to.
                   */
                  notify("Offsets reset", {
                    message: `${props.groupId} now starts from the offsets in the plan you applied.`,
                  });
                }
                return outcome;
              },
              permitted: mayReset(),
              refusal: resetRefusal(),
            }}
            /* Offered only where it is permitted. `GroupDetail` handles the other refusal itself —
               a group with members cannot be deleted, and it says so before the click rather than
               after, because that is the one refusal an operator can act on directly. */
            onDelete={mayDelete() ? () => setConfirmingDelete(true) : undefined}
            deleteRefusal={
              deleteRefusal()
            }
            /* Authorized by `ConsumerGroupResetOffsets`, which is what the endpoint's own
               `EndpointAuthorization` names — the same permission the wizard needs, because both
               write this group's committed positions. */
            onForgetOffsets={mayReset() ? (topic) => {
              // The previous topic's receipt or refusal belongs to the confirmation that showed
              // it. Reopening onto "3 partitions forgotten" from another topic would read as this
              // topic's answer, and the figure is the whole content of the receipt.
              forget.reset();
              setForgetting(topic);
            } : undefined}
            forgetRefusal={
              kui.writeBlocked !== undefined ? resetRefusal() : mayReset() ? undefined
                : "You do not have permission to change this group's committed offsets."
            }
          />

          <Show when={forgetting()}>
            {(topic) => (
              <ConfirmDialog
                open={mayReset()}
                /* Named, because this page opens two confirmations and both are portalled into
                   `document.body` as `[role="dialog"]`. "This confirmation is no longer open" has
                   to be an assertion about *this* one, and by role alone it is not. */
                testId="group-forget-confirm"
                onClose={() => setForgetting(undefined)}
                title={`Forget ${props.groupId}'s offsets on ${topic()}?`}
                consequence={consequenceOfForget(detail(), topic())}
                confirmLabel="Forget offsets"
                confirmIcon="trash"
                /* No type-to-confirm, for the reason the group delete gives: no records are
                   destroyed, and demanding a typed name for everything teaches operators to type
                   names without reading them. */
                busy={forget.busy()}
                error={deleteError(forget.state())}
                onConfirm={() => {
                  if (!mayReset()) return;
                  void forget.run(topic()).then((outcome) => {
                    if (outcome.kind !== "done") return;
                    setForgetting(undefined);
                    // The page behind this dialog is drawn from offsets that have just changed;
                    // the assignments table would otherwise keep showing positions Kafka no
                    // longer holds.
                    setAttempt(attempt() + 1);
                    /*
                       Two sentences, chosen by the server's own figure, and never one sentence
                       with a number in it.

                       `DELETE …/offsets` answers 200 with the partitions it removed — and an empty
                       list is a 200 too, because "the group held nothing here" and "they are gone"
                       are different outcomes that a bare status code cannot tell apart. Reporting
                       the empty case as a success is the same failure as a green tick over a copy
                       that moved no records: the operator goes away believing a position they can
                       no longer see was removed, when it was never there.
                     */
                    const removed = outcome.value.partitions.length;
                    notify(
                      removed === 0 ? "Nothing was forgotten" : "Committed offsets forgotten",
                      {
                        tone: removed === 0 ? "warning" : "success",
                        message:
                          removed === 0
                            ? `${props.groupId} held no committed offset on ` +
                              `${outcome.value.topic}, so nothing was removed.`
                            : `${props.groupId} no longer has a committed position on ` +
                              `${describeRemoved(removed)} of ${outcome.value.topic}. ` +
                              "No records were deleted.",
                      },
                    );
                  });
                }}
              />
            )}
          </Show>

          <ConfirmDialog
            open={confirmingDelete() && mayDelete()}
            onClose={() => setConfirmingDelete(false)}
            title={`Delete ${props.groupId}?`}
            consequence={consequenceOfDelete(detail())}
            confirmLabel="Delete group"
            confirmIcon="trash"
            /* No type-to-confirm. Unlike a topic, deleting an empty group destroys no records —
               the group's committed offsets go, and a consumer that starts up again follows its own
               auto.offset.reset. Demanding a typed name for everything is how operators learn to
               type names without reading them. */
            busy={remove.busy()}
            error={deleteError(remove.state())}
            onConfirm={() => {
              if (!mayDelete()) return;
              void remove.run().then((outcome) => {
                if (outcome.kind !== "done") return;
                setConfirmingDelete(false);
                // Raised before the navigation, deliberately: the toast region lives in the shell
                // and survives a route change, and the list this lands on has no other trace of
                // what just happened — the group is simply not there any more, which is
                // indistinguishable from having mistyped the address.
                notify("Consumer group deleted", {
                  message:
                    `${props.groupId} and its committed offsets are gone. ` +
                    "No records were deleted.",
                });
                navigate(kui.paths.consumerGroups(props.clusterId), { resolve: false });
              });
            }}
          />
        </>
      )}
    </Show>
  );
}

/** What deleting this group actually costs, in this group's own figures. */
export function consequenceOfDelete(group: GroupDetail): string {
  const partitions = group.offsets.length;
  const where = partitions === 1 ? "1 partition" : `${partitions} partitions`;
  return (
    `Removes the group and its committed offsets on ${where}. ` +
    // The part that decides whether this is safe, and the part an operator forgets: the data is
    // untouched, but any consumer that comes back under this group id starts from wherever its own
    // auto.offset.reset says, which for the default is the end of the log.
    "No records are deleted. A consumer that starts up again under this group id follows its own " +
    "auto.offset.reset, which by default means it begins at the end of the log and skips everything " +
    "currently in it."
  );
}

/**
 * What forgetting this topic's offsets costs, in this group's own figures.
 *
 * The partition count comes off the group on screen rather than from a constant, because it is the
 * figure the receipt afterwards is read against: "3 partitions held" before, "3 partitions" after.
 * A group that holds none there is a state the confirmation can still be opened in — the page may
 * have been fetched before somebody else's reset — so it says so rather than printing a zero.
 */
export function consequenceOfForget(group: GroupDetail, topic: string): string {
  const held = subscriptions(group).find((one) => one.topic === topic)?.partitions.length ?? 0;
  const partitions = held === 1 ? "1 partition" : `${String(held)} partitions`;
  const where =
    held === 0
      ? `KUI last read this group as holding no committed offset on ${topic}`
      : `Removes this group's committed offsets on ${partitions} of ${topic}`;
  return (
    `${where}. The group itself stays, and every other topic it holds offsets on is untouched. ` +
    // The same sentence the group delete carries, and for the same reason: this is the part an
    // operator forgets, and it is what decides whether the action is safe.
    "No records are deleted. A consumer that comes back for this topic under this group id " +
    "follows its own auto.offset.reset, which by default means it begins at the end of the log."
  );
}

/** How many partitions the server says it forgot. Never a bare number beside a topic name. */
function describeRemoved(count: number): string {
  return count === 1 ? "1 partition" : `${String(count)} partitions`;
}

function deleteError(
  state: ReturnType<ReturnType<typeof createMutation<[], unknown>>["state"]>,
): { readonly message: string; readonly code?: string | undefined } | undefined {
  if (state.kind === "forbidden") return { message: state.message, code: "KUI-FORBIDDEN" };
  if (state.kind === "failed") return { message: state.message, code: state.code };
  return undefined;
}

function Loading(props: {
  readonly state: Fetched<GroupDetail>;
  readonly onRetry: () => void;
}): JSX.Element {
  return (
    <section aria-label="Consumer group">
      <Show when={props.state.kind === "loading"}>
        <p role="status">Asking the coordinator about this group…</p>
      </Show>
      <Show when={props.state.kind === "forbidden"}>
        <p role="status">You do not have permission to see this consumer group.</p>
      </Show>
      <Show when={props.state.kind === "failed"}>
        <p role="alert">
          {props.state.kind === "failed" ? props.state.message : ""}{" "}
          <button type="button" onClick={props.onRetry}>
            Try again
          </button>
        </p>
      </Show>
    </section>
  );
}

export default GroupRoute;
