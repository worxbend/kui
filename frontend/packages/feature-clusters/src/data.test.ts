import { describe, expect, it, vi } from "vitest";
import type { KuiApiClient } from "@kui/api";
import { fetchBrokers, fetchClusterDisks, fetchClusters, fetchManagedClusters, healthOf } from "./data.js";

/**
 * The cluster feature's data layer.
 *
 * The case that matters most here is the one the fixtures used to hide: a cluster whose scrape
 * failed must still appear in the list, named, with its figures **absent** rather than zero. All
 * three of the obvious shortcuts are wrong in a way an operator cannot recover from —
 *
 *   - dropping the row makes a broken cluster look like a deleted one;
 *   - filling it with zeroes makes it look like a cluster with no brokers;
 *   - failing the whole request makes one broken cluster hide the three healthy ones.
 *
 * The section envelope exists to distinguish those, and this suite is what keeps the mapping
 * honest.
 */

/** A client that answers with whatever is given, without a server. */
function client(answer: unknown, ok = true): KuiApiClient {
  const get = vi.fn(async () => (ok ? { ok: true, value: answer } : { ok: false, error: answer }));
  return { get, post: get, put: get, delete: get, patch: get, raw: {} } as unknown as KuiApiClient;
}

const scrape = {
  version: "3.7.0",
  brokerCount: 3,
  onlinePartitionCount: 1536,
  offlinePartitionCount: 0,
  underReplicatedPartitionCount: 0,
  scrapedAt: "2026-09-05T12:00:00Z",
};

/**
 * One entry of the cluster list, in the shape the server really sends.
 *
 * The row is nested under `cluster`, and the counts come from a sibling `topics` section — not from
 * the scrape. Both were got wrong here first, and `recorded.test.ts` (which decodes a document
 * captured from a running gateway) is what found it. A hand-written fixture is only ever as right
 * as the author's memory of the contract, which is why that suite exists beside this one.
 */
function row(id: string, summary: unknown, topics?: unknown) {
  return {
    cluster: { id, name: id, readOnly: false, bootstrapServers: `${id}:9092`, summary },
    ...(topics === undefined ? {} : { topics }),
  };
}

describe("fetchClusters", () => {
  it("keeps a cluster whose scrape failed, with its figures absent rather than zero", async () => {
    const answer = await fetchClusters(
      client({
        clusters: {
          status: "ok",
          fetchedAt: "2026-09-05T12:00:00Z",
          data: [
            row("healthy-01", { status: "ok", fetchedAt: "2026-09-05T12:00:00Z", data: scrape }),
            row("silent-01", { status: "unavailable", reason: "UPSTREAM_UNAVAILABLE", message: "no answer" }),
          ],
        },
      }),
    );

    expect(answer.kind).toBe("ready");
    const clusters = answer.kind === "ready" ? answer.value : [];
    expect(clusters.map((c) => c.id)).toEqual(["healthy-01", "silent-01"]);

    const silent = clusters[1]!;
    expect(silent.name).toBe("silent-01");
    // The whole point: not zero.
    expect(silent.brokersOnline).toBeNull();
    expect(silent.partitions).toBeNull();
    expect(silent.version).toBeNull();
    expect(silent.health).toBe("unknown");

    const healthy = clusters[0]!;
    expect(healthy.brokersOnline).toBe(3);
    expect(healthy.health).toBe("healthy");
    // Absent because this fixture carries no `topics` section: the counts live there, not on the
    // scrape, so a cluster whose topic service did not answer has no topic count — and says so.
    expect(healthy.topics).toBeNull();
  });

  it("shows a stale list rather than hiding it, and says it is stale", async () => {
    const answer = await fetchClusters(
      client({
        clusters: {
          status: "stale",
          fetchedAt: "2026-09-05T11:00:00Z",
          data: [row("prod", { status: "ok", fetchedAt: "x", data: scrape })],
          // The wire shape, verified against `libs/contracts-core/src/kui/contracts/Section.scala`:
          // `reason` is a flat code string with `message` and `since` as its siblings, not a nested
          // object. Getting this wrong in a fixture is how a test passes against a shape the server
          // never sends.
          reason: "SCRAPE_FAILED",
          message: "The last scrape did not finish.",
        },
      }),
    );

    expect(answer.kind).toBe("stale");
    if (answer.kind === "stale") {
      expect(answer.value).toHaveLength(1);
      expect(answer.reason).toBe("The last scrape did not finish.");
    }
  });

  it("keeps forbidden and not-configured separate from a failure", async () => {
    // Three states that all render "no clusters here" and mean different things. Only one of them
    // has a retry that could ever work, which is why they must not be merged.
    const forbidden = await fetchClusters(client({ clusters: { status: "forbidden" } }));
    expect(forbidden.kind).toBe("forbidden");

    const absent = await fetchClusters(client({ clusters: { status: "not_configured" } }));
    expect(absent.kind).toBe("not-configured");

    const broken = await fetchClusters(
      client({ clusters: { status: "unavailable", reason: "UPSTREAM_UNAVAILABLE", message: "no" } }),
    );
    expect(broken.kind).toBe("failed");
  });

  it("reports a transport failure with a sentence rather than undefined", async () => {
    // Only the `envelope` error carries a `message`. Reaching for `.message` on an unreachable
    // gateway is how a screen renders "undefined" at exactly the wrong moment.
    const answer = await fetchClusters(client({ kind: "unreachable", cause: "ECONNREFUSED" }, false));
    expect(answer.kind).toBe("failed");
    if (answer.kind === "failed") {
      expect(answer.message).not.toContain("undefined");
      expect(answer.message.length).toBeGreaterThan(0);
      expect(answer.code).toBe("UNREACHABLE");
    }
  });

  it("treats a section it cannot read as unavailable rather than throwing", async () => {
    const answer = await fetchClusters(client({ clusters: { status: "something-new" } }));
    expect(answer.kind).toBe("failed");
  });

  it("never turns a null scrape instant into the Unix epoch", async () => {
    /*
     * `new Date(null)` is 1970-01-01, and the row that draws this figure prints it as "Read 20702d
     * ago" — a fabricated date in the one line whose own comment forbids inventing one, and a
     * figure that says the cluster was last read half a century ago. `=== undefined` let it
     * through; `== null` is the whole fix, and this is the case that holds it.
     *
     * Latent rather than live today, because `ClusterSummaryDto.scrapedAt` is non-optional. Every
     * other absent figure on this row arrives as a wire `null`, so the field becoming optional is a
     * server-side edit away and nothing else in this package would have noticed.
     */
    const answer = await fetchClusters(
      client({
        clusters: {
          status: "ok",
          fetchedAt: "2026-09-05T12:00:00Z",
          data: [
            row("silent-01", {
              status: "ok",
              fetchedAt: "2026-09-05T12:00:00Z",
              data: { ...scrape, scrapedAt: null },
            }),
          ],
        },
      }),
    );
    expect(answer.kind).toBe("ready");
    if (answer.kind === "ready") {
      expect(answer.value[0]?.observedAt).toBeNull();
    }
  });
});

describe("fetchManagedClusters", () => {
  it("keeps whether a truststore or a keystore is configured", async () => {
    /*
     * The read model already says so. Dropping the flags made an edit look like a cluster with no
     * stores, and Save then replaced the profile without them.
     */
    const answer = await fetchManagedClusters(
      client({
        clusters: {
          status: "ok",
          fetchedAt: "2026-09-05T12:00:00Z",
          data: [
            {
              cluster: {
                id: "edge",
                name: "Edge",
                readOnly: false,
                bootstrapServers: "10.0.0.8:9093",
                origin: "stored",
                version: 2,
                security: {
                  protocol: "SSL",
                  mechanism: null,
                  truststoreConfigured: true,
                  keystoreConfigured: false,
                },
              },
            },
          ],
        },
      }),
    );

    expect(answer.kind).toBe("ready");
    if (answer.kind !== "ready") return;
    expect(answer.value[0]?.security.truststoreConfigured).toBe(true);
    expect(answer.value[0]?.security.keystoreConfigured).toBe(false);
    // Not sent by the public read model. The mapper must not invent `false` for a field it did
    // not see: that is a stored choice, and the form treats absent as "unknown".
    expect(answer.value[0]?.security.verifyHostname).toBeUndefined();
  });

  it("keeps a hostname-verification choice when a response carries one", async () => {
    /*
     * `ClusterSecurityDto` does not include `verifyHostname` yet. The mapper still copies a
     * boolean when one is present, so adding the field server-side does not come back as the
     * default `true` on the next edit.
     */
    const answer = await fetchManagedClusters(
      client({
        clusters: {
          status: "ok",
          fetchedAt: "2026-09-05T12:00:00Z",
          data: [
            {
              cluster: {
                id: "edge",
                name: "Edge",
                readOnly: false,
                bootstrapServers: "10.0.0.8:9093",
                origin: "stored",
                version: 2,
                security: {
                  protocol: "SSL",
                  mechanism: null,
                  truststoreConfigured: false,
                  keystoreConfigured: false,
                  verifyHostname: false,
                },
              },
            },
          ],
        },
      }),
    );

    expect(answer.kind).toBe("ready");
    if (answer.kind !== "ready") return;
    expect(answer.value[0]?.security.verifyHostname).toBe(false);
  });
});

describe("fetchBrokers", () => {
  it("maps a broker whose disk could not be read to nulls, not zeroes", async () => {
    const answer = await fetchBrokers(
      client({
        brokers: {
          status: "ok",
          fetchedAt: "2026-09-05T12:00:00Z",
          data: [
            {
              id: 1,
              host: "broker-1",
              port: 9092,
              isController: true,
              // The server's names, verified against `src/recorded/brokers.json`. Every plausible
              // guess — `leaderPartitions`, `replicaPartitions`, `diskUsedBytes` — is wrong, and
              // wrong silently: the card renders as em dashes, which reads as a broker that did not
              // answer.
              leaderCount: 512,
              replicaCount: 1536,
              diskUsageBytes: 100,
            },
            { id: 2, host: "broker-2", port: 9092 },
          ],
        },
      }),
      "prod",
    );

    expect(answer.kind).toBe("ready");
    const brokers = answer.kind === "ready" ? answer.value : [];

    expect(brokers[0]).toMatchObject({ id: 1, isController: true, leaderPartitions: 512, health: "healthy" });
    expect(brokers[0]?.replicaPartitions).toBe(1536);
    // `diskUsageBytes` is what Kafka's own replicas occupy, and it lands on `heldBytes`. It is not
    // the disk's usage: the disk under it also carries whatever else the machine keeps there.
    expect(brokers[0]?.heldBytes).toBe(100);
    // None of these is on the wire at all: the endpoint carries no per-broker out-of-sync count and
    // no disk capacity, so a percentage is not knowable from this document alone. `null` says so; a
    // `0` would be a claim nobody made, and a total of `0` would make every disk bar read as full.
    expect(brokers[0]?.outOfSyncReplicas).toBeNull();
    expect(brokers[0]?.diskUsedBytes).toBeNull();
    expect(brokers[0]?.diskTotalBytes).toBeNull();

    expect(brokers[1]?.heldBytes).toBeNull();
    expect(brokers[1]?.leaderPartitions).toBeNull();
    expect(brokers[1]?.isController).toBe(false);
    expect(brokers[1]?.health).toBe("unknown");
  });
});

describe("healthOf", () => {
  it("ranks an offline partition above an under-replicated one", () => {
    // Offline is neither readable nor writable; under-replicated is readable and at risk. A cluster
    // with both is reported as the worse of the two.
    expect(healthOf({ offlinePartitionCount: 1, underReplicatedPartitionCount: 40 })).toBe("offline");
    expect(healthOf({ offlinePartitionCount: 0, underReplicatedPartitionCount: 40 })).toBe("degraded");
    expect(healthOf({ offlinePartitionCount: 0, underReplicatedPartitionCount: 0 })).toBe("healthy");
  });

  it("says unknown when there was no scrape, rather than guessing", () => {
    expect(healthOf(undefined)).toBe("unknown");
  });
});

describe("fetchClusterDisks", () => {
  it("skips a directory that reported only half a capacity, rather than reading the gap as zero", async () => {
    /*
     * Closed by mutation: dropping either `continue` from `disksOf` leaves all 147 cases in this
     * package green, and `dir.totalBytes - dir.usableBytes` over a pair of `null`s is `0` — so a
     * directory that failed contributes a silent nothing, and a directory that reported a size and
     * no free space contributes a disk that is exactly 100% full. The second one is the damaging
     * shape: it draws a red bar on the card for a broker whose disk was never measured, which is
     * the reading an operator acts on first.
     *
     * Kafka answers `describeLogDirs` per directory, so this is the ordinary partial answer and not
     * a broken server.
     */
    const answer = await fetchClusterDisks(
      client({
        logDirs: {
          status: "ok",
          fetchedAt: "2026-09-05T12:00:00Z",
          data: [
            { brokerId: 1, path: "/data/a", error: null, totalBytes: 1_000, usableBytes: 400 },
            // Answered with an error: neither half is known.
            { brokerId: 1, path: "/data/b", error: "KafkaStorageException", totalBytes: null, usableBytes: null },
            // A size with no free space beside it. Half a capacity is not a capacity.
            { brokerId: 2, path: "/data/c", error: null, totalBytes: 2_000, usableBytes: null },
          ],
        },
      }),
      "prod",
    );

    expect(answer.kind).toBe("ready");
    const disks = answer.kind === "ready" ? answer.value : [];
    // Broker 2 contributes nothing at all: no entry, so its card keeps its `null`s and says so.
    expect(disks.map((disk) => disk.brokerId)).toEqual([1]);
    // And broker 1 is the sum of the one directory that answered both halves, not of both.
    expect(disks[0]).toEqual({ brokerId: 1, usedBytes: 600, capacityBytes: 1_000 });
  });
});
