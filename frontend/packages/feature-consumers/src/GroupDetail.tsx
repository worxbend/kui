/**
 * One consumer group: who is in it, what they hold, how far behind each partition is, and the two
 * things you can do to it.
 *
 * The design does not draw this screen, so its vocabulary is taken from the two that are drawn: the
 * page header, chip and outlined-danger action of the topic page (`02`), and the ruled table with
 * state chips and a threshold-coloured lag column of the consumer list (`04`). Nothing new is
 * invented; a screen assembled from parts the operator has already learnt needs no explaining.
 *
 * ## No voice line
 *
 * SPEC §5.2's note: the dashboard and the consumer list carry a sentence under the title, and an
 * object page does not. This is an object page. A page about one named group has nothing general to
 * say about it, and a cheerful line here would be a sentence about a thing rather than about a
 * situation.
 *
 * ## Three tables, and why the third is not a fourth column of the second
 *
 * Members answer "who is consuming"; assignments answer "and how far behind is each partition".
 * They are separate tables because they have different keys — a member holds many partitions and a
 * partition has at most one member — and joining them would repeat each member's host and client id
 * down a column, which is how a table stops being scannable.
 */

import { For, Show, createMemo } from "solid-js";
import type { JSX } from "@solidjs/web";
import {
  Banner,
  Button,
  Card,
  DataTable,
  EmptyState,
  MISSING,
  PageHeader,
  StatusPill,
  ThresholdValue,
  formatCount,
  formatRate,
  type Column,
} from "@kui/kernel";
import { UNREADABLE_STATE_CHIP, lagAnnouncement, lagLevel, stateChip } from "./model.js";
import { partitionLag, subscriptions, type GroupDetail as Group, type Member, type PartitionOffset } from "./detail.js";
import { ResetWizard, type ResetWizardProps } from "./ResetWizard.jsx";

export interface GroupDetailProps {
  readonly group: Group;
  /** Where "Consumer groups" in the breadcrumb points. */
  readonly listHref: string;
  /** Builds the topic destination used by assignment rows. */
  readonly topicHref?: ((topic: string) => string) | undefined;
  /** Everything the offset-reset wizard needs except the topic list, which is read off the group. */
  readonly reset: Omit<ResetWizardProps, "topics">;
  /** Opens the delete confirmation. Absent when this user may not delete groups. */
  readonly onDelete?: (() => void) | undefined;
  /** Why deletion is not offered, when it is not. */
  readonly deleteRefusal?: string | undefined;
  /**
   * Opens the confirmation for forgetting this group's committed offsets on one topic.
   *
   * Named by topic and not by partition, because that is the shape of the endpoint behind it:
   * `DELETE …/consumer-groups/{group}/offsets?topic=` removes the group's positions on one topic
   * and nothing else. Absent when this user may not reset this group's offsets — the endpoint is
   * authorized by `ConsumerGroupResetOffsets`, the same action the wizard needs.
   */
  readonly onForgetOffsets?: ((topic: string) => void) | undefined;
  /** Why forgetting is not offered, when it is not. */
  readonly forgetRefusal?: string | undefined;
  /**
   * The coordinator didn't answer in time, and what's on screen is the last snapshot KUI held.
   * Absent means the figures below are fresh. The reason is the same sentence a stale `Section`
   * carries elsewhere in the product — see `@kui/kernel`'s `Fetched` and `fromSection`.
   */
  readonly stale?: string | undefined;
}

export function GroupDetail(props: GroupDetailProps): JSX.Element {
  const chip = () => (props.group.state === null ? UNREADABLE_STATE_CHIP : stateChip(props.group.state));
  const topics = createMemo(() => subscriptions(props.group));
  const assignmentColumns = createMemo(() => offsetColumns(props.topicHref));

  /**
   * A group with members cannot be deleted, and Kafka refuses it with a code an operator can act on
   * directly — by stopping the consumers. Saying so before the click is better than after it.
   */
  const hasMembers = () => props.group.members.length > 0;

  return (
    <section class="kui-cg-page" data-testid="consumer-group-detail">
      <PageHeader
        title={props.group.groupId}
        crumbs={[{ label: "Consumer groups", href: props.listHref }, { label: props.group.groupId }]}
        chip={
          <StatusPill tone={chip().tone} title={chip().title}>
            {chip().label}
          </StatusPill>
        }
        actions={
          <Show
            when={props.onDelete !== undefined && !hasMembers()}
            fallback={
              <Button
                variant="danger"
                icon="trash"
                disabled
                disabledReason={
                  props.onDelete === undefined
                    ? (props.deleteRefusal ?? "You do not have permission to delete consumer groups on this cluster.")
                    : "The group still has members. Stop its consumers first."
                }
              >
                Delete group
              </Button>
            }
          >
            <Button variant="danger" icon="trash" onClick={() => props.onDelete?.()}>
              Delete group
            </Button>
          </Show>
        }
        testId="consumer-group-head"
      />

      <Show when={props.stale}>
        {(reason) => <Banner tone="warning" message={reason()} testId="group-detail-stale" />}
      </Show>

      <Facts group={props.group} />

      <Card title="Members" testId="group-members-card">
        <DataTable<Member>
          caption={`Consumers in ${props.group.groupId}`}
          columns={memberColumns(props.group.members)}
          rows={props.group.members}
          rowKey={(row) => row.memberId}
          testId="group-members-table"
          empty={
            <EmptyState
              kind="empty"
              title="No members."
              description="The group holds offsets but nothing is consuming right now. Normal for a job that runs on a schedule."
            />
          }
        />
      </Card>

      <Card
        title="Assignments and lag"
        caption={
          props.group.excludedPartitions === 0
            ? undefined
            : `${formatCount(props.group.excludedPartitions)} ${props.group.excludedPartitions === 1 ? "partition" : "partitions"} could not be read, so the total above is lower than the real lag.`
        }
        testId="group-assignments-card"
      >
        <DataTable<PartitionOffset>
          caption={`Partition offsets and lag for ${props.group.groupId}`}
          columns={assignmentColumns()}
          rows={props.group.offsets}
          rowKey={(row) => `${row.topic}/${row.partition}`}
          testId="group-assignments-table"
          empty={
            <EmptyState
              kind="empty"
              title="No committed offsets."
              description="This group has not committed a position on any partition yet."
            />
          }
        />
      </Card>

      <ResetWizard {...props.reset} topics={topics()} />

      {/*
        Forgetting one topic's offsets — a separate action from the wizard above it and from the
        delete in the header, and it is per topic rather than a column of the assignments table.
        The endpoint names a topic; that table's rows are partitions, so a per-row control would
        offer a gesture the server cannot perform, and a single button would have to guess which of
        the group's topics the operator meant.

        Absent, rather than empty, for a group holding no offsets at all: there is nothing to
        forget, and a heading over an empty list reads as a control that failed to load.
      */}
      <Show when={topics().length > 0}>
        <section class="kui-cg-forget" data-testid="group-forget-offsets">
          <h2 class="kui-cg-section__title">Forget this group's offsets on one topic</h2>
          <p class="kui-cg-reset__note">
            The group itself stays, every other topic it holds offsets on is untouched, and no
            records are deleted. A consumer that comes back for this topic starts wherever its own
            auto.offset.reset says, which for the default is the end of the log.
          </p>
          <ul class="kui-cg-forget__list">
            <For each={topics()}>
              {(held) => (
                <li class="kui-cg-forget__row">
                  <span class="kui-cg-mono kui-cg-forget__topic">{held.topic}</span>
                  {/* The figure the receipt will be compared against, said before the click. */}
                  <span class="kui-cg-forget__held">{describeHeld(held.partitions.length)}</span>
                  <Show
                    when={props.onForgetOffsets}
                    fallback={
                      <Button
                        variant="secondary"
                        icon="trash"
                        disabled
                        /* The fallback speaks about the cluster, not about this group, and the
                           difference is not stylistic: the caller's sentence names the group and
                           this one cannot, because a component handed no callback has not been
                           told which permission is missing. Wording them identically made the two
                           indistinguishable on screen, so the route's ternary could be deleted
                           with every case still green — the same shape the delete control above
                           already avoids. */
                        disabledReason={
                          props.forgetRefusal ??
                          "You do not have permission to change committed offsets on this cluster."
                        }
                      >
                        Forget offsets
                      </Button>
                    }
                  >
                    {(forget) => (
                      <Button
                        variant="secondary"
                        icon="trash"
                        onClick={() => forget()(held.topic)}
                      >
                        Forget offsets
                      </Button>
                    )}
                  </Show>
                </li>
              )}
            </For>
          </ul>
        </section>
      </Show>
    </section>
  );
}

/**
 * How many partitions this group holds a committed position on for one topic.
 *
 * Spelled out rather than left as a bare number beside a topic name: "3" next to
 * `orders.v1` could as easily be a version, a replica count or a lag, and this figure is the one
 * the receipt after the click is read against.
 */
function describeHeld(count: number): string {
  return count === 1 ? "1 partition held" : `${formatCount(count)} partitions held`;
}

/**
 * The strip of facts under the title.
 *
 * Each is a label and a value, and a value that is not known is an em dash with a reason — never a
 * blank and never a zero. The PACE row is where a rate would be printed, with the words "records
 * per second" beside it. The detail endpoint does not send one — `fetchGroup` leaves `pace` null —
 * so the row stays unavailable rather than inventing a rate from how often the page was reloaded.
 */
function Facts(props: { readonly group: Group }): JSX.Element {
  const facts = createMemo<readonly { readonly label: string; readonly value: JSX.Element; readonly title?: string | undefined }[]>(() => [
    {
      label: "Total lag",
      value:
        props.group.totalLag === null ? (
          <Missing what="total lag" />
        ) : (
          <ThresholdValue
            value={formatCount(props.group.totalLag)}
            level={lagLevel(props.group.totalLag)}
            announcement={lagAnnouncement}
            data-testid="group-total-lag"
          />
        ),
    },
    {
      label: "Pace",
      value: <Pace pace={props.group.pace} />,
      title: "How fast the group is committing. Negative means its committed offsets moved backwards, which is what somebody else's reset looks like from here.",
    },
    { label: "Members", value: <>{formatCount(props.group.members.length)}</> },
    { label: "Partitions", value: <>{formatCount(props.group.offsets.length)}</> },
    {
      label: "Coordinator",
      value: props.group.coordinator === null ? <Missing what="coordinator" /> : <span class="kui-cg-mono">{props.group.coordinator}</span>,
    },
    { label: "Assignor", value: <span class="kui-cg-mono">{props.group.partitionAssignor}</span> },
    {
      label: "Protocol",
      value: <span class="kui-cg-mono">{props.group.protocol}</span>,
      title: props.group.isSimple ? "A simple consumer: it commits offsets without joining a group protocol." : undefined,
    },
  ]);

  return (
    <dl class="kui-cg-facts" data-testid="group-facts">
      <For each={facts()}>
        {(fact) => (
          <div class="kui-cg-facts__item" title={fact.title}>
            <dt class="kui-cg-facts__label">{fact.label}</dt>
            <dd class="kui-cg-facts__value">{fact.value}</dd>
          </div>
        )}
      </For>
    </dl>
  );
}

/**
 * The commit rate, with a word rather than a bare figure when it is zero.
 *
 * A `0` beside a large lag is the single most important cell on this page and reads as nothing at
 * all, so a stalled group says `Stalled`. An unknown rate is an em dash: a group KUI has only seen
 * once has no rate yet, which is not a rate of zero.
 */
function Pace(props: { readonly pace: number | null }): JSX.Element {
  return (
    <Show when={props.pace !== null} fallback={<Missing what="pace" />}>
      <Show
        when={props.pace !== 0}
        fallback={
          <span class="kui-cg-pace kui-cg-pace--stalled" data-testid="group-pace">
            Stalled
          </span>
        }
      >
        <span class="kui-cg-pace" data-testid="group-pace">
          {formatRate(props.pace ?? 0)} <span class="kui-cg-facts__unit">records/s</span>
        </span>
      </Show>
    </Show>
  );
}

function Missing(props: { readonly what: string }): JSX.Element {
  return (
    <span class="kui-cg-missing" title={`KUI could not read this group's ${props.what}.`}>
      <span aria-hidden="true">{MISSING}</span>
      <span class="kui-visually-hidden">{`${props.what} unavailable`}</span>
    </span>
  );
}

const MEMBER_COLUMNS: readonly Column<Member>[] = [
  {
    id: "clientId",
    header: "Client id",
    render: (row) => (
      <span class="kui-cg-name">
        <span class="kui-cg-mono">{row.clientId}</span>
        <Show when={row.rebalancing}>
          <StatusPill tone="warning" title="This member is joining or leaving the group.">
            rebalancing
          </StatusPill>
        </Show>
      </span>
    ),
  },
  { id: "host", header: "Host", render: (row) => <span class="kui-cg-mono">{row.host}</span> },
  {
    id: "instance",
    header: "Static id",
    // A group that does not use static membership has no instance ids at all, so this column would
    // be an em dash on every row of every table — the thing the brief forbids. It is rendered as a
    // dash for the *one* group where some members are static and some are not, which happens during
    // a rolling upgrade and is worth seeing; on a group where nobody is static, `MEMBER_COLUMNS` is
    // filtered before it reaches the table. See `memberColumns` below.
    render: (row) => (row.groupInstanceId === null ? MISSING : <span class="kui-cg-mono">{row.groupInstanceId}</span>),
  },
  {
    id: "partitions",
    header: "Partitions",
    align: "numeric",
    width: "8rem",
    render: (row) => formatCount(row.partitions.length),
  },
  {
    id: "assigned",
    header: "Assigned",
    render: (row) => (
      <span class="kui-cg-assigned" title={row.partitions.join(", ")}>
        {row.partitions.length === 0 ? MISSING : row.partitions.join(", ")}
      </span>
    ),
  },
];

/**
 * The member columns for this particular group.
 *
 * `Static id` is dropped when no member in the group has one, which is the common case: a column
 * that is an em dash on every row of every group teaches people to stop reading columns.
 */
export function memberColumns(members: readonly Member[]): readonly Column<Member>[] {
  if (members.some((member) => member.groupInstanceId !== null)) return MEMBER_COLUMNS;
  return MEMBER_COLUMNS.filter((column) => column.id !== "instance");
}

function offsetColumns(
  topicHref: ((topic: string) => string) | undefined,
): readonly Column<PartitionOffset>[] {
  return [
    {
      id: "topic",
      header: "Topic",
      render: (row) =>
        topicHref === undefined ? (
          <span class="kui-cg-mono">{row.topic}</span>
        ) : (
          <a class="kui-cg-name__link kui-cg-mono kui-cg-topic-link" href={topicHref(row.topic)}>
            {row.topic}
          </a>
        ),
    },
    {
      id: "partition",
      header: "Partition",
      align: "numeric",
      width: "7rem",
      render: (row) => formatCount(row.partition),
    },
    {
      id: "committed",
      header: "Committed",
      align: "numeric",
      render: (row) =>
        row.committed === null ? (
          <span title="This group has never committed an offset on this partition.">{MISSING}</span>
        ) : (
          formatCount(row.committed)
        ),
    },
    {
      id: "end",
      header: "End offset",
      align: "numeric",
      render: (row) =>
        row.endOffset === null ? (
          <span title="The partition's end offset could not be read.">{MISSING}</span>
        ) : (
          formatCount(row.endOffset)
        ),
    },
    {
      id: "lag",
      header: "Lag",
      align: "numeric",
      width: "9rem",
      render: (row) => {
        const lag = partitionLag(row);
        if (lag === null) {
          return (
            <span title="Lag needs both a committed offset and an end offset; one of them is missing.">
              {MISSING}
            </span>
          );
        }
        return (
          <ThresholdValue
            value={formatCount(lag)}
            level={lagLevel(lag)}
            announcement={lagAnnouncement}
          />
        );
      },
    },
    {
      id: "member",
      header: "Held by",
      render: (row) =>
        row.memberId === null ? (
          <span title="No member currently holds this partition.">{MISSING}</span>
        ) : (
          <span class="kui-cg-mono">{row.memberId}</span>
        ),
    },
  ];
}

const OFFSET_COLUMNS: readonly Column<PartitionOffset>[] = offsetColumns(undefined);

export { MEMBER_COLUMNS, OFFSET_COLUMNS };
