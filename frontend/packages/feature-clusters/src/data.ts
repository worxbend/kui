/**
 * The cluster feature's data layer: what it asks the gateway for, and how the answers become the
 * view models the screens already take.
 *
 * ## Why the mapping lives here and not in the screens
 *
 * `ClusterList` and `BrokerList` take finished view models and fetch nothing. That is what lets
 * every state of those screens — a cluster that is not answering, a broker whose disk could not be
 * read, a principal who may look but not change — be rendered in a story and asserted in a test
 * with no server anywhere. Moving a single `await` into a screen would take all of that away, so
 * the boundary is kept sharp: this file talks to the gateway, the screens draw.
 *
 * ## Sections, and the two failures they distinguish
 *
 * Every aggregated response arrives in *sections* (ADR-039): the request succeeded, and each part
 * of the answer separately says whether it did. A cluster row always carries its identity — the id,
 * the name, the bootstrap servers, whether it is read-only — because that came from configuration
 * and cannot fail. Its `summary` is a section, because that came from a live scrape and can.
 *
 * So a cluster whose brokers did not answer still appears in the list, named, with its figures
 * absent rather than zero. The alternative — dropping the row, or filling it with zeroes — is the
 * one an operator cannot recover from: a cluster that has vanished from the list looks like a
 * cluster that was deleted, and a cluster reporting 0 brokers looks like a cluster that is down.
 * They are three different situations and the product must not merge them.
 *
 * ## Health is derived here, once
 *
 * `Health` is the screens' vocabulary, not the wire's. The wire says how many partitions are
 * offline and how many are under-replicated; deciding that "any offline partition is unhealthy" is
 * a product judgement, and it is made in one place so that the list, the broker page and the
 * environment rail cannot disagree about what a healthy cluster is.
 */
import { decodeSection, type ApiResult, type KuiApiClient } from "@kui/api";
import { apiFailure, figure, fromSection, type Fetched } from "@kui/kernel";
import type { Broker, ClusterSummary, ConfigEntry, ConfigSource, Health, LogDir } from "./model.js";

/**
 * What the gateway reports about one cluster once a scrape has succeeded.
 *
 * Declared here because the generated types stop at `unknown` for every section payload: the server
 * documents `Section[A]` with `Schema.any`, so the browser's generated type cannot see inside it
 * (`packages/api/src/section.ts` explains why, and records the server-side fix). This is the shape
 * `ClusterSummaryDto` really has, and `decodeSection` is the one boundary at which it is asserted.
 */
interface ClusterSummaryPayload {
  readonly kafkaClusterId?: string | null;
  readonly version?: string | null;
  readonly controllerId?: number | null;
  readonly controllerKind?: string;
  readonly brokerCount?: number;
  readonly onlinePartitionCount?: number | null;
  readonly offlinePartitionCount?: number | null;
  readonly underReplicatedPartitionCount?: number | null;
  readonly totalDiskUsageBytes?: number | null;
  readonly scrapedAt?: string;
}

/** One entry of the cluster list. The row itself is nested under `cluster`; see `toClusterSummary`. */
interface ClusterEntryPayload {
  readonly cluster: {
    readonly id: string;
    readonly name: string;
    readonly readOnly: boolean;
    readonly bootstrapServers: string;
    readonly summary: unknown;
  };
  /** The topic service's answer for this cluster: counts, and the largest few. Its own section. */
  readonly topics?: unknown;
}

/** The topic counts the list endpoint returns per cluster. */
interface ClusterTopicsPayload {
  readonly topicCount?: number | null;
  readonly partitionCount?: number | null;
}

/**
 * What the brokers endpoint reports, once decoded.
 *
 * These names are the server's, taken from a recorded response
 * (`src/recorded/brokers.json`) rather than guessed. They are worth reading carefully, because a
 * plausible guess is wrong for almost every one of them: it is `leaderCount`, not
 * `leaderPartitions`; `replicaCount`, not `replicaPartitions`; `diskUsageBytes`, not
 * `diskUsedBytes`. A mismatch does not fail — every field falls back and the whole card renders as
 * em dashes, which reads as "this broker did not answer".
 */
interface BrokerPayload {
  readonly id: number;
  readonly host: string;
  readonly port: number;
  readonly rack?: string | null;
  readonly isController?: boolean;
  readonly leaderCount?: number | null;
  readonly replicaCount?: number | null;
  readonly partitionCount?: number | null;
  readonly diskUsageBytes?: number | null;
  /*
   * `leaderSkewPercent` is drawn, since this wave, and it took three to get there: the cluster
   * service hard-coded it `null` until 2026-09-06, at which point it began deriving it from the
   * partition census (`ClusterTopology.leaderSkewOn`) and nothing in the browser read it. An
   * expanded broker card now says it in words — `leaderSkewSentence` — which is the honest form:
   * a signed percentage beside the word LEADERS reads as a negative partition count, and the
   * PARTITION SKEW tile a few inches away is a *different* figure (a cluster-wide spread over
   * replica counts) that this must never be printed under.
   *
   * `segmentCount` is the one field this mapping still reads nothing from, declared so the next
   * reader does not think it was missed: it is `null` on every cluster KUI has been run against,
   * because the cluster service hard-codes it, so there is no figure to draw and no absence worth
   * a sentence.
   */
  readonly leaderSkewPercent?: number | null;
  readonly segmentCount?: number | null;
}

/**
 * How healthy a cluster is, from its scrape.
 *
 * The order matters and is a product judgement: an offline partition is not readable and not
 * writable, so it outranks an under-replicated one, which is readable and at risk. A cluster whose
 * summary never arrived is `unknown` — not `unhealthy`, because we did not establish that.
 */
export function healthOf(summary: ClusterSummaryPayload | undefined): Health {
  if (summary === undefined) return "unknown";
  if ((summary.offlinePartitionCount ?? 0) > 0) return "offline";
  if ((summary.underReplicatedPartitionCount ?? 0) > 0) return "degraded";
  return "healthy";
}

/**
 * One list entry, as the screen's row.
 *
 * The entry is *not* the cluster: it is `{ cluster, capability, topics, consumerGroups }`, where
 * each of the last three is its own section. That shape is the whole design — the cluster's
 * identity comes from configuration and cannot fail, while its scrape, its topic counts and its
 * group counts come from three different services and can each fail on their own. A row therefore
 * draws with whichever of them arrived.
 *
 * Two sections are read here. `cluster.summary` carries the scrape; `topics` carries the counts,
 * and reading it is what puts a topic count on the row at all — taking the partition count from the
 * scrape instead gives `onlinePartitionCount`, which the quickstart's own broker reports as `null`.
 */
function toClusterSummary(entry: ClusterEntryPayload): ClusterSummary {
  const row = entry.cluster;
  const section = decodeSection<ClusterSummaryPayload>(row.summary);
  const summary = section.status === "ok" || section.status === "stale" ? section.data : undefined;

  const topicsSection = decodeSection<ClusterTopicsPayload>(entry.topics);
  const topics =
    topicsSection.status === "ok" || topicsSection.status === "stale"
      ? topicsSection.data
      : undefined;

  return {
    id: row.id,
    name: row.name,
    health: healthOf(summary),
    version: summary?.version ?? null,
    controllerKind: summary?.controllerKind ?? null,
    // The wire reports how many brokers answered, not how many are configured, so "3 of 3" is the
    // only honest reading when a scrape succeeded — and both are absent when it did not.
    brokersOnline: figure(summary?.brokerCount),
    brokersTotal: figure(summary?.brokerCount),
    topics: figure(topics?.topicCount),
    // The topic service's count, not the scrape's `onlinePartitionCount` — a single-broker cluster
    // reports the latter as `null`, so taking it from there is a dash on a screen that has the
    // number a few bytes away.
    partitions: figure(topics?.partitionCount),
    underReplicatedPartitions: figure(summary?.underReplicatedPartitionCount),
    readOnly: row.readOnly,
    /*
     * `== null` and not `=== undefined`, which is a two-character fix for a fabricated date.
     * `new Date(null)` is the Unix epoch, so a wire `scrapedAt: null` — which every other absent
     * figure on this row arrives as — rendered "Read 20702d ago" in the one line whose own comment
     * forbids inventing a date. It is latent rather than live today only because
     * `ClusterSummaryDto.scrapedAt` is non-optional; the row beside it reads `?? null` for exactly
     * the same wire, so the asymmetry was the defect.
     */
    observedAt: summary?.scrapedAt == null ? null : new Date(summary.scrapedAt),
  };
}

function toBroker(payload: BrokerPayload): Broker {
  return {
    id: payload.id,
    host: payload.host,
    port: payload.port,
    rack: payload.rack ?? null,
    isController: payload.isController === true,
    // A broker in the list answered `describeCluster`; whether its own metrics answered is a
    // separate question, so health is derived from what is missing rather than assumed from its
    // presence. `replicaCount` is the field that is populated on every cluster KUI has seen —
    // `leaderCount` is null on a single-broker cluster — so it is the one that decides.
    health:
      payload.replicaCount === null || payload.replicaCount === undefined ? "unknown" : "healthy",
    leaderPartitions: figure(payload.leaderCount),
    replicaPartitions: figure(payload.replicaCount),
    // Not on the wire. The brokers endpoint reports no out-of-sync count per broker; it is a
    // cluster-level figure on the scrape. `null` says so, rather than `0` claiming everything is in
    // sync on evidence nobody produced.
    outOfSyncReplicas: null,
    heldBytes: figure(payload.diskUsageBytes),
    /*
     * The capacity pair is not on this endpoint at all, and is filled from the log directories by
     * `withDisks` below when they answer. It stays `null` here rather than being invented from
     * `diskUsageBytes`, which would draw every broker as a disk that is exactly 100% full.
     */
    diskUsedBytes: null,
    diskTotalBytes: null,
    leaderSkewPercent: figure(payload.leaderSkewPercent),
  };
}

/** Every configured cluster. Rows whose scrape failed are kept, with their figures absent. */
export async function fetchClusters(
  api: KuiApiClient,
): Promise<Fetched<readonly ClusterSummary[]>> {
  const answer = await api.get("/api/v1/clusters", {});
  if (!answer.ok) return apiFailure(answer.error);
  const section = decodeSection<readonly ClusterEntryPayload[]>(answer.value.clusters);
  return fromSection(section, (rows) => rows.map(toClusterSummary));
}

/**
 * One cluster, on its own.
 *
 * The brokers screen needs three things the brokers endpoint does not carry: the under-replicated
 * partition count its voice line reads, the Kafka version its cards tag, and the moment the scrape
 * was taken. All three are on the cluster's own summary, and asking for one cluster rather than
 * filtering the list means a deployment with forty registered clusters does not scrape thirty-nine
 * of them to draw one screen.
 *
 * The entry here has no sibling `topics` section — that belongs to the list endpoint — so the topic
 * and partition counts come back absent, which is the honest reading of a document that does not
 * carry them.
 */
export async function fetchCluster(
  api: KuiApiClient,
  clusterId: string,
): Promise<Fetched<ClusterSummary>> {
  const answer = await api.get("/api/v1/clusters/{clusterId}", { params: { path: { clusterId } } });
  if (!answer.ok) return apiFailure(answer.error);
  const row = (answer.value as { readonly cluster?: ClusterEntryPayload["cluster"] }).cluster;
  if (row === undefined) {
    return {
      kind: "failed",
      message: "The cluster document carried no cluster.",
      code: "DECODING_FAILED",
    };
  }
  return { kind: "ready", value: toClusterSummary({ cluster: row }) };
}

/** One cluster's brokers. */
export async function fetchBrokers(
  api: KuiApiClient,
  clusterId: string,
): Promise<Fetched<readonly Broker[]>> {
  const answer = await api.get("/api/v1/clusters/{clusterId}/brokers", {
    params: { path: { clusterId } },
  });
  if (!answer.ok) return apiFailure(answer.error);
  const section = decodeSection<readonly BrokerPayload[]>(answer.value.brokers);
  return fromSection(section, (rows) => rows.map(toBroker));
}

/* ---------------------------------------------------------------------------------------------- */
/* One broker: its settings and its disks                                                           */
/* ---------------------------------------------------------------------------------------------- */

/**
 * One row of `describeConfigs`, as the gateway actually sends it.
 *
 * Recorded from `GET /clusters/development/brokers/1/configs` (`src/recorded/brokerConfigs.json`)
 * rather than read off the schema, and two things in it are worth stating because a plausible guess
 * gets both wrong:
 *
 * - the booleans are `isSensitive` and `isReadOnly`, not `sensitive` and `readOnly` — which is what
 *   the *topic* configuration endpoint calls them, so the two are genuinely different spellings for
 *   the same idea and copying the topic mapping across would silently disclose nothing and hide
 *   nothing;
 * - there is no `isDefault` on the wire at all, although the server's own domain entity has one.
 *   `overridden` is therefore derived from the source below.
 */
interface BrokerConfigPayload {
  readonly name: string;
  readonly value?: string | null;
  readonly source?: string | null;
  readonly isSensitive?: boolean;
  readonly isReadOnly?: boolean;
  readonly documentation?: string | null;
}

/**
 * The wire's source token as the screen's vocabulary.
 *
 * The tokens come from `kui.cluster.domain.ConfigSource#token` and are hyphenated lower case, not
 * Kafka's own `DYNAMIC_BROKER_CONFIG` shouting. An unrecognised token becomes `UNKNOWN`, which sorts
 * last and reads "Unknown" — mapping it to `DEFAULT` instead would put a setting somebody changed at
 * the bottom of the table among three hundred untouched defaults, which is precisely where nobody
 * looks.
 */
export function brokerConfigSourceOf(raw: string | null | undefined): ConfigSource {
  switch (raw) {
    case "dynamic-broker":
      return "DYNAMIC_BROKER";
    case "dynamic-default-broker":
      return "DYNAMIC_DEFAULT";
    case "static-broker":
      return "STATIC";
    case "default":
      return "DEFAULT";
    default:
      return "UNKNOWN";
  }
}

function toConfigEntry(payload: BrokerConfigPayload): ConfigEntry {
  const sensitive = payload.isSensitive === true;
  const source = brokerConfigSourceOf(payload.source);
  return {
    name: payload.name,
    /*
     * A sensitive setting carries no value on the wire, and the screen must say "hidden" rather than
     * draw an em dash. `null` is what both a withheld value and an unset one arrive as, so the
     * `sensitive` flag beside it is the only thing that can tell them apart — which is why it is
     * carried rather than inferred from the value being absent.
     */
    value: sensitive ? null : (payload.value ?? null),
    sensitive,
    source,
    // Not on the wire. Anything the broker did not fall back to its own default for was set by
    // somebody, whether in the file or at runtime, and those are the rows this page exists for.
    overridden: source !== "DEFAULT",
  };
}

/** One broker's settings, ordered and filtered by the screen rather than here. */
export async function fetchBrokerConfigs(
  api: KuiApiClient,
  clusterId: string,
  brokerId: number,
): Promise<Fetched<readonly ConfigEntry[]>> {
  const answer = await api.get("/api/v1/clusters/{clusterId}/brokers/{brokerId}/configs", {
    params: { path: { clusterId, brokerId } },
  });
  if (!answer.ok) return apiFailure(answer.error);
  const section = decodeSection<readonly BrokerConfigPayload[]>(answer.value.configs);
  return fromSection(section, (rows) => rows.map(toConfigEntry));
}

/**
 * One log directory, as `describeLogDirs` reports it through the gateway.
 *
 * `totalBytes` and `usableBytes` describe the *filesystem the directory sits on*, not the directory:
 * on the recorded document they are 503 GB and 199 GB against 32 MB of actual Kafka data. Reading
 * either as the directory's size would draw a bar saying this broker holds half a terabyte of
 * messages. What the directory holds is the sum of its replicas, which is also the figure the
 * brokers endpoint reports as `diskUsageBytes`, and the two agreeing is the check that this is the
 * right field.
 */
interface LogDirPayload {
  readonly brokerId?: number;
  readonly path: string;
  readonly error?: string | null;
  readonly partitionCount?: number | null;
  /** The filesystem the directory sits on: its size, and how much of it is free. See above. */
  readonly totalBytes?: number | null;
  readonly usableBytes?: number | null;
  readonly replicas?: readonly { readonly sizeBytes?: number | null }[];
}

function toLogDir(payload: LogDirPayload): LogDir {
  const error = payload.error ?? null;
  const replicas = payload.replicas ?? [];
  return {
    path: payload.path,
    /*
     * A directory that reported an error has no size, and must never be summed as zero: the total
     * under the table is described as the sum of the directories that answered, and quietly adding a
     * failed disk as 0 makes it a claim about a disk nobody could read.
     */
    sizeBytes:
      error !== null
        ? null
        : replicas.reduce((sum, replica) => sum + (replica.sizeBytes ?? 0), 0),
    partitions: error !== null ? null : figure(payload.partitionCount),
    error,
  };
}

/**
 * One broker's log directories.
 *
 * The endpoint is the cluster's — `GET /clusters/{id}/log-dirs` — with the broker as a query
 * parameter, so this asks for one broker's disks rather than fetching every broker's and discarding
 * the rest. On a fifty-broker cluster the difference is the whole response.
 */
export async function fetchBrokerLogDirs(
  api: KuiApiClient,
  clusterId: string,
  brokerId: number,
): Promise<Fetched<readonly LogDir[]>> {
  const answer = await api.get("/api/v1/clusters/{clusterId}/log-dirs", {
    params: { path: { clusterId }, query: { brokerId } },
  });
  if (!answer.ok) return apiFailure(answer.error);
  const section = decodeSection<readonly LogDirPayload[]>(answer.value.logDirs);
  return fromSection(section, (rows) => rows.map(toLogDir));
}

/**
 * What one broker's disks add up to: how much of them is in use, and how large they are.
 *
 * Neither figure is about Kafka. `describeLogDirs` reports the filesystem each directory sits on,
 * so `totalBytes - usableBytes` is everything on that disk — logs, the operating system, whatever
 * else the machine keeps there — and that is exactly the right numerator for "is this broker about
 * to run out of disk", which is the question the card's bar and its 75%/90% thresholds exist to
 * answer.
 */
export interface BrokerDisk {
  readonly brokerId: number;
  readonly usedBytes: number;
  readonly capacityBytes: number;
}

/**
 * Every broker's disks, from one request.
 *
 * The endpoint takes an optional `brokerId`; omitting it asks for the whole cluster, which is one
 * request for a screen that draws every broker rather than one per card. The shell's storage meter
 * reads the same document by the same rule, which is why the percentage here and the percentage in
 * the drawer cannot disagree.
 *
 * A directory that did not report **both** halves is skipped in both sums. Half a broker's capacity
 * produces a percentage that looks perfectly plausible and is wrong, and a failed disk is not a
 * disk of size zero.
 */
export async function fetchClusterDisks(
  api: KuiApiClient,
  clusterId: string,
): Promise<Fetched<readonly BrokerDisk[]>> {
  const answer = await api.get("/api/v1/clusters/{clusterId}/log-dirs", {
    params: { path: { clusterId } },
  });
  if (!answer.ok) return apiFailure(answer.error);
  const section = decodeSection<readonly LogDirPayload[]>(answer.value.logDirs);
  return fromSection(section, disksOf);
}

function disksOf(dirs: readonly LogDirPayload[]): readonly BrokerDisk[] {
  const sums = new Map<number, { used: number; capacity: number }>();
  for (const dir of dirs) {
    if (dir.brokerId === undefined) continue;
    if (dir.totalBytes === undefined || dir.totalBytes === null) continue;
    if (dir.usableBytes === undefined || dir.usableBytes === null) continue;
    const held = sums.get(dir.brokerId) ?? { used: 0, capacity: 0 };
    sums.set(dir.brokerId, {
      used: held.used + (dir.totalBytes - dir.usableBytes),
      capacity: held.capacity + dir.totalBytes,
    });
  }
  return [...sums]
    .sort(([left], [right]) => left - right)
    .map(([brokerId, sum]) => ({ brokerId, usedBytes: sum.used, capacityBytes: sum.capacity }));
}

/**
 * The brokers, with whatever the log directories were able to say about their disks.
 *
 * A separate step rather than a second field on the brokers request, because the two endpoints fail
 * separately: a cluster that refuses `describeLogDirs` still draws every card, with a sentence
 * where the percentage would be. A broker with no entry here keeps its `null`s and says so.
 */
export function withDisks(
  brokers: readonly Broker[],
  disks: readonly BrokerDisk[],
): readonly Broker[] {
  if (disks.length === 0) return brokers;
  const byBroker = new Map(disks.map((disk) => [disk.brokerId, disk]));
  return brokers.map((broker) => {
    const disk = byBroker.get(broker.id);
    return disk === undefined
      ? broker
      : { ...broker, diskUsedBytes: disk.usedBytes, diskTotalBytes: disk.capacityBytes };
  });
}

/* ---------------------------------------------------------------------------------------------- */
/* Managing the registration itself                                                                 */
/* ---------------------------------------------------------------------------------------------- */

/**
 * One configured cluster, for the screen that adds, edits and removes them.
 *
 * Read from the same `GET /clusters` document the list uses, but for a different purpose: this cares
 * about the *registration* — where it came from and which version of it the server holds — where the
 * list cares about how the cluster is doing.
 */
export interface ManagedClusterRow {
  readonly id: string;
  readonly name: string;
  readonly bootstrapServers: string;
  readonly readOnly: boolean;
  /**
   * Where this cluster's definition comes from, and therefore what may be done to it.
   *
   * `static` — only the file this process was started with names it.
   * `stored` — only a record in KUI's metadata store.
   * `staticThenStored` — both, and the stored record won.
   */
  readonly origin: ClusterOrigin;
  /**
   * The optimistic-concurrency version, echoed back as `If-Match`.
   *
   * `undefined` when the server did not send one, which is not zero: a save then goes without the
   * header and the server decides. Inventing a version would be claiming to have read a record that
   * was never read.
   */
  readonly version: number | undefined;
  readonly security: {
    readonly protocol: string;
    readonly mechanism: string | null;
    /** From the read model. Absent or false means this screen did not see a configured store. */
    readonly truststoreConfigured?: boolean;
    readonly keystoreConfigured?: boolean;
    /**
     * The stored hostname-verification choice, when the response carried one.
     *
     * The public read model does not send it today. Absent is not `false`: the edit form shows the
     * default and says so, rather than pretending the box is the record.
     */
    readonly verifyHostname?: boolean;
  };
}

/** The three origins the cluster registry distinguishes, plus what an unknown word becomes. */
export type ClusterOrigin = "static" | "stored" | "staticThenStored" | "unknown";

/**
 * Reading the origin, conservatively.
 *
 * A word the browser does not recognise becomes `unknown`, which this screen treats as *not
 * editable*. That is the safe direction and it is worth saying why: offering an edit for a cluster
 * whose definition lives in the deployment's configuration file would make the file a lie — the next
 * deployment would quietly change the cluster back — whereas withholding an edit for one that is in
 * fact editable is an inconvenience the operator can work around, and the server refuses the write
 * anyway.
 */
export function originOf(raw: string | null | undefined): ClusterOrigin {
  if (typeof raw !== "string") return "unknown";
  const lower = raw.toLowerCase();
  if (lower === "stored") return "stored";
  if (lower === "static") return "static";
  if (lower === "staticthenstored") return "staticThenStored";
  return "unknown";
}

/** Whether this cluster's definition can be replaced from the browser. */
export function isEditable(origin: ClusterOrigin): boolean {
  // `staticThenStored` included: a stored record already wins over the file, so replacing it changes
  // what is actually in force.
  return origin === "stored" || origin === "staticThenStored";
}

/**
 * Whether it can be removed.
 *
 * Only a purely stored cluster. Deleting the store record for one the file also names would leave
 * the row on screen — which reads as a delete that silently failed — so the server refuses it, and
 * so does this screen, before the click rather than after.
 */
export function isRemovable(origin: ClusterOrigin): boolean {
  return origin === "stored";
}

interface ManagedRowPayload {
  readonly id: string;
  readonly name: string;
  readonly bootstrapServers: string;
  readonly readOnly: boolean;
  readonly origin?: string | null;
  readonly version?: number | null;
  readonly security?: {
    readonly protocol?: string;
    readonly mechanism?: string | null;
    readonly truststoreConfigured?: boolean;
    readonly keystoreConfigured?: boolean;
    readonly verifyHostname?: boolean;
  };
}

export async function fetchManagedClusters(
  api: KuiApiClient,
): Promise<Fetched<readonly ManagedClusterRow[]>> {
  const answer = await api.get("/api/v1/clusters", {});
  if (!answer.ok) return apiFailure(answer.error);

  /*
   * A section, not a bare array. `GET /clusters` answers `{"clusters": {"status": "ok", "data": …}}`
   * — the same ADR-039 envelope every aggregated response uses — and reading `.clusters` as a list
   * produced an empty screen that looked exactly like a deployment with no clusters configured.
   */
  const section = decodeSection<readonly { readonly cluster?: ManagedRowPayload }[]>(
    (answer.value as { clusters?: unknown }).clusters,
  );
  return fromSection(section, (entries) =>
    entries
      .map((entry) => entry.cluster)
      .filter((row): row is ManagedRowPayload => row !== undefined)
      .map((row) => ({
        id: row.id,
        name: row.name,
        bootstrapServers: row.bootstrapServers,
        readOnly: row.readOnly,
        origin: originOf(row.origin),
        version: typeof row.version === "number" ? row.version : undefined,
        security: {
          protocol: row.security?.protocol ?? "PLAINTEXT",
          mechanism: row.security?.mechanism ?? null,
          truststoreConfigured: row.security?.truststoreConfigured === true,
          keystoreConfigured: row.security?.keystoreConfigured === true,
          // Kept when a response carries it. The public read model does not, yet; dropping it here
          // would put the checkbox back to the default the moment the field is added.
          ...(typeof row.security?.verifyHostname === "boolean"
            ? { verifyHostname: row.security.verifyHostname }
            : {}),
        },
      })),
  );
}

/** Registers a cluster, or replaces one. A `PUT` replaces the whole record — there is no patch. */
export async function saveCluster(
  api: KuiApiClient,
  clusterId: string,
  request: Record<string, unknown>,
  version: number | undefined,
): Promise<ApiResult<unknown>> {
  return api.put("/api/v1/clusters/{clusterId}", {
    params: { path: { clusterId }, header: { "If-Match": ifMatch(version) } },
    body: request as never,
  });
}

/**
 * The `If-Match` value: the version being replaced, quoted, or `"0"` to create.
 *
 * Required by the contract rather than optional, and the reasoning there is worth keeping in view
 * from this side too: an unconditional write to a versioned record is a lost update waiting for a
 * second person with the same screen open. `"0"` means "create this, and fail if it already exists",
 * which is what keeps one code path here instead of two endpoints.
 *
 * The quotes are part of the value — it is an HTTP entity tag, not a number in a header.
 */
function ifMatch(version: number | undefined): string {
  return version === undefined ? '"0"' : `"${version}"`;
}

export async function deleteCluster(
  api: KuiApiClient,
  clusterId: string,
  version: number | undefined,
): Promise<ApiResult<unknown>> {
  return api.delete("/api/v1/clusters/{clusterId}", {
    params: { path: { clusterId }, header: { "If-Match": ifMatch(version) } },
  });
}

/**
 * Asks whether KUI could reach a cluster with these settings, without registering anything.
 *
 * Takes the same request the save would, which is what makes it worth having: it exercises the
 * broker addresses, the protocol and the credentials exactly as the save will use them.
 */
export async function testConnection(
  api: KuiApiClient,
  request: Record<string, unknown>,
): Promise<
  ApiResult<{ readonly reachable: boolean; readonly status: string; readonly detail?: string }>
> {
  const answer = await api.post("/api/v1/clusters/connection-test", { body: request as never });
  if (!answer.ok) return answer;
  return {
    ok: true,
    value: {
      reachable: answer.value.reachable,
      status: answer.value.status,
      ...(answer.value.detail === undefined ? {} : { detail: answer.value.detail }),
    },
  };
}
