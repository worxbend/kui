/**
 * The consumer feature's data layer.
 *
 * Field names are taken from a response a running gateway produced (`src/recorded/groups.json`),
 * not from reading the DTOs — the server documents every section with `Schema.any`, so the payloads
 * are `unknown` here and a misspelled field is a type-correct `undefined` that renders as an em
 * dash. A mapping can therefore be wrong in every field and look exactly like a coordinator that
 * did not answer. The clusters feature shipped that bug; these documents are what stop it recurring.
 */
import { decodeSection, isForbidden, ReasonCodes, ReasonSentences, type KuiApiClient } from "@kui/api";
import { apiFailure, fromSection, type Fetched } from "@kui/kernel";
import type { GroupState, GroupSummary } from "./model.js";
import type { GroupDetail, Member, PartitionOffset } from "./detail.js";

interface GroupRowPayload {
  readonly groupId: string;
  readonly state?: string | null;
  readonly members?: number | null;
  readonly topics?: number | null;
  readonly partitions?: number | null;
  readonly coordinatorId?: number | null;
  /**
   * The coordinator's address. Both halves travel together or neither does — the contract has a
   * property asserting it — so the mapping below reads them as a pair rather than filling one in.
   */
  readonly coordinatorHost?: string | null;
  readonly coordinatorPort?: number | null;
  readonly totalLag?: number | null;
  readonly excludedPartitions?: number | null;
  /**
   * Present when part of this group's picture could not be read; `null` when it is complete.
   *
   * The wire's shape here is not pinned by the recorded document — the quickstart's groups are all
   * complete, so every one of them sends `null` and there is no example of the populated case. The
   * three booleans below are the screen's vocabulary (`Incomplete` in `model.ts`) and are read
   * defensively: anything the server sends that is not `false` is treated as known, because
   * claiming a figure is missing when it is present is the milder of the two errors here.
   */
  readonly incomplete?: {
    readonly note?: string;
    readonly offsetsKnown?: boolean;
    readonly membersKnown?: boolean;
    readonly endOffsetsKnown?: boolean;
  } | null;
}

interface GroupListPagePayload {
  readonly page?: number;
  readonly pageSize?: number;
  readonly totalItems?: number;
}

interface GroupListPayload {
  readonly items: readonly GroupRowPayload[];
  readonly page?: GroupListPagePayload | null;
}

/** The states Kafka reports. Anything else is `null` — an unknown state is not a state. */
const STATES: readonly string[] = [
  "STABLE",
  "EMPTY",
  "PREPARING_REBALANCE",
  "COMPLETING_REBALANCE",
  "DEAD",
  "UNKNOWN",
];

/**
 * The group's state, or `null`.
 *
 * A string the browser does not recognise becomes `null` rather than being passed through: the
 * screen's chip is a closed set, and rendering an unknown word in it would style it as whatever the
 * default happens to be — which on this screen is the healthy colour.
 */
export function stateOf(raw: string | null | undefined): GroupState | null {
  if (typeof raw !== "string") return null;
  const upper = raw.toUpperCase();
  return STATES.includes(upper) ? (upper as GroupState) : null;
}

function figure(value: number | null | undefined): number | null {
  return typeof value === "number" ? value : null;
}

/**
 * The coordinating broker's address, as the screen prints it.
 *
 * `host:port` or nothing. The wire also carries `coordinatorId`, and the previous mapping fell back
 * to `broker ${id}` when the address was absent — which reads as an address, is not one, and is
 * indistinguishable on screen from a coordinator that answered. A broker id is not somewhere an
 * operator can point a tool, so a row with no address says it has none and the column draws its
 * reason.
 */
export function coordinatorAddress(
  host: string | null | undefined,
  port: number | null | undefined,
): string | null {
  // Both or neither: the contract asserts the two arrive together, and half an address — `kafka:`
  // or `:9092` — is worse than none, because it looks like a value that got truncated in transit.
  if (typeof host !== "string" || host === "") return null;
  if (typeof port !== "number") return null;
  return `${host}:${port}`;
}

function toGroupSummary(payload: GroupRowPayload): GroupSummary {
  return {
    groupId: payload.groupId,
    state: stateOf(payload.state),
    members: figure(payload.members),
    // `topics` is not nullable on the row: a group with no subscriptions genuinely has zero, and
    // that is a fact worth printing rather than a gap.
    topics: payload.topics ?? 0,
    coordinator: coordinatorAddress(payload.coordinatorHost, payload.coordinatorPort),
    // The most expensive `0` on this screen: a group with no lag is caught up, and a group whose
    // lag could not be computed is a group nobody knows about. They must not look alike.
    totalLag: figure(payload.totalLag),
    excludedPartitions: payload.excludedPartitions ?? 0,
    incomplete:
      payload.incomplete === null || payload.incomplete === undefined
        ? null
        : {
            note: payload.incomplete.note ?? "Part of this group could not be read.",
            offsetsKnown: payload.incomplete.offsetsKnown !== false,
            membersKnown: payload.incomplete.membersKnown !== false,
            endOffsetsKnown: payload.incomplete.endOffsetsKnown !== false,
          },
  };
}

/**
 * Which page was asked for, and what the server said about the whole list.
 *
 * `totalItems` is `null` when the server did not carry one. That is not `0` and it is not the
 * number of rows in hand: the screen has to say it does not know the cluster's figure rather than
 * publish this page's length as if it were the total, which is what it did before this existed.
 */
export interface GroupPage {
  readonly page: number;
  readonly pageSize: number;
  readonly totalItems: number | null;
}

/** What one page of the list costs to ask for. The server's default is 25; the screen picks 16. */
export const DEFAULT_PAGE_SIZE = 16;

export interface GroupListResult {
  readonly groups: readonly GroupSummary[];
  /** How many coordinators did not answer. Drives the voice line and the incomplete chips. */
  readonly coordinatorsMissing: number;
  /** The server's own account of where this page sits. Never derived from `groups.length`. */
  readonly page: GroupPage;
}

export interface GroupQuery {
  readonly page: number;
  readonly pageSize: number;
}

/** The query a page-one request makes, so callers that do not page still name a page. */
export const FIRST_PAGE: GroupQuery = { page: 1, pageSize: DEFAULT_PAGE_SIZE };

export async function fetchGroups(
  api: KuiApiClient,
  clusterId: string,
  query: GroupQuery = FIRST_PAGE,
): Promise<Fetched<GroupListResult>> {
  const answer = await api.get("/api/v1/clusters/{clusterId}/consumer-groups", {
    params: { path: { clusterId }, query: { page: query.page, pageSize: query.pageSize } },
  });
  if (!answer.ok) return apiFailure(answer.error);

  // Outside the section, like the topic list's `incompleteTopics`: how many coordinators failed is
  // known even when the groups they hold are not.
  const missing =
    typeof answer.value.incompleteCoordinators === "number"
      ? answer.value.incompleteCoordinators
      : 0;

  const section = decodeSection<GroupListPayload>(answer.value.groups);
  return fromSection(section, (listing) => ({
    groups: listing.items.map(toGroupSummary),
    coordinatorsMissing: missing,
    page: pageOf(listing.page, query),
  }));
}

/**
 * The server's page block, or the request's own figures where it said nothing.
 *
 * The request's `page` and `pageSize` are safe to fall back on: they are what this browser asked
 * for, so they describe the request even when the answer does not. `totalItems` has no such
 * fallback — nothing in this response knows how many groups the cluster has — so it stays `null`
 * and the screen says so in words.
 */
function pageOf(payload: GroupListPagePayload | null | undefined, query: GroupQuery): GroupPage {
  return {
    page: figure(payload?.page) ?? query.page,
    pageSize: figure(payload?.pageSize) ?? query.pageSize,
    totalItems: figure(payload?.totalItems),
  };
}

/* ---------------------------------------------------------------------------------------------- */
/* One group                                                                                        */
/* ---------------------------------------------------------------------------------------------- */

/**
 * The group detail endpoint's shape, from a response a running gateway produced
 * (`src/recorded/group.json`).
 *
 * Unlike the list, this response is **not** wrapped in an ADR-039 section — the whole thing is the
 * group. A `stale` field beside the data carries the ADR-039 idea instead: present when the picture
 * was served from a cache because the coordinator did not answer in time.
 *
 * The wire nests partitions under `topics`, one entry per subscribed topic; the screen wants one
 * flat list of partition positions, because that is what the offsets table draws and what the reset
 * wizard groups by topic again through `subscriptions`.
 */
interface GroupPartitionPayload {
  readonly partition: number;
  readonly committed?: number | null;
  readonly begin?: number | null;
  readonly end?: number | null;
  readonly lag?: number | null;
  readonly memberId?: string | null;
}

interface GroupTopicPayload {
  readonly topic: string;
  readonly partitions?: readonly GroupPartitionPayload[];
}

interface MemberPayload {
  readonly memberId: string;
  readonly clientId?: string | null;
  readonly host?: string | null;
  readonly groupInstanceId?: string | null;
  readonly partitions?: readonly string[];
  readonly rebalancing?: boolean;
}

interface GroupDetailPayload {
  readonly groupId: string;
  readonly state?: string | null;
  readonly protocol?: string | null;
  readonly isSimple?: boolean;
  readonly partitionAssignor?: string | null;
  readonly coordinatorId?: number | null;
  readonly coordinatorHost?: string | null;
  readonly coordinatorPort?: number | null;
  readonly members?: readonly MemberPayload[];
  readonly topics?: readonly GroupTopicPayload[];
  readonly totalLag?: number | null;
  readonly excludedPartitions?: number | null;
  readonly observedAt?: string | null;
  /**
   * Present when `observedAt` is older than the snapshot's refresh interval and a refresh has
   * failed — the picture below is the coordinator's last good answer, not a current one.
   */
  readonly stale?: {
    readonly fetchedAt?: string | null;
    readonly reason?: string | null;
  } | null;
}

/** The wire's `stale.reason` — a `ReasonCode`'s wire spelling — as the sentence shown on screen. */
function staleSentence(reason: string | null | undefined): string {
  const known = ReasonSentences as Readonly<Record<string, string | undefined>>;
  const sentence = typeof reason === "string" ? known[reason] : undefined;
  return sentence ?? known[ReasonCodes.Unknown] ?? "KUI could not refresh this.";
}

function toMember(payload: MemberPayload): Member {
  return {
    memberId: payload.memberId,
    clientId: payload.clientId ?? "",
    /*
     * Kafka reports a member's host with a leading slash — `/172.21.0.4` — because it is rendering a
     * Java `InetSocketAddress`. Stripping it is not cosmetic: an operator copies this into `ssh` or
     * a `grep`, and `/172.21.0.4` matches nothing.
     */
    host: (payload.host ?? "").replace(/^\//, ""),
    // `null` is a real answer: this group does not use static membership. Not a missing value.
    groupInstanceId: payload.groupInstanceId ?? null,
    partitions: payload.partitions ?? [],
    rebalancing: payload.rebalancing === true,
  };
}

function toOffsets(topics: readonly GroupTopicPayload[]): readonly PartitionOffset[] {
  return topics.flatMap((topic) =>
    (topic.partitions ?? []).map((partition) => ({
      topic: topic.topic,
      partition: partition.partition,
      // Never `0` by default. A group that has never committed here and a group sitting at the first
      // record are different facts, and the table draws the first as a dash.
      committed: typeof partition.committed === "number" ? partition.committed : null,
      endOffset: typeof partition.end === "number" ? partition.end : null,
      memberId: partition.memberId ?? null,
    })),
  );
}

export async function fetchGroup(
  api: KuiApiClient,
  clusterId: string,
  groupId: string,
): Promise<Fetched<GroupDetail>> {
  const answer = await api.get("/api/v1/clusters/{clusterId}/consumer-groups/{groupId}", {
    params: { path: { clusterId, groupId } },
  });
  if (!answer.ok) return isForbidden(answer.error) ? { kind: "forbidden" } : apiFailure(answer.error);

  const payload = answer.value as unknown as GroupDetailPayload;
  const observedAt =
    payload.observedAt === null || payload.observedAt === undefined
      ? new Date()
      : new Date(payload.observedAt);

  const value: GroupDetail = {
    groupId: payload.groupId,
    state: stateOf(payload.state),
    coordinator: coordinatorAddress(payload.coordinatorHost, payload.coordinatorPort),
    partitionAssignor: payload.partitionAssignor ?? "",
    protocol: payload.protocol ?? "UNKNOWN",
    isSimple: payload.isSimple === true,
    totalLag: typeof payload.totalLag === "number" ? payload.totalLag : null,
    /*
     * Not on this endpoint. `null` says "not measured", which is what the figure means — the
     * alternative, computing it from two observations the browser happens to hold, would produce a
     * rate that changes with how often somebody reloaded the page.
     */
    pace: null,
    members: (payload.members ?? []).map(toMember),
    offsets: toOffsets(payload.topics ?? []),
    excludedPartitions: payload.excludedPartitions ?? 0,
    observedAt,
  };

  // `stale` beside the data, not a status on it (this endpoint is not a Section): present when the
  // coordinator did not answer in time and this snapshot is the last good one KUI has. Reported
  // through `Fetched`'s own `stale` kind — the vocabulary `fromSection` already gives the list — so
  // the reason travels with the value instead of being read onto `GroupDetail` and then forgotten.
  if (payload.stale === null || payload.stale === undefined) {
    return { kind: "ready", value };
  }
  return { kind: "stale", value, reason: staleSentence(payload.stale.reason) };
}
