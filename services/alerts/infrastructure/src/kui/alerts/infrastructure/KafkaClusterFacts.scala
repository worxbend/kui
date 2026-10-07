package kui.alerts.infrastructure

import scala.jdk.CollectionConverters.*

import cats.effect.kernel.Async
import cats.syntax.all.*
import org.apache.kafka.clients.admin.ListTopicsOptions
import org.apache.kafka.common.{Node, TopicCollection, TopicPartitionInfo}
import org.typelevel.log4cats.StructuredLogger

import kui.alerts.domain.*
import kui.kafka.admin.{ClusterAdmin, GroupAdmin, LogDir}
import kui.kafka.{AdminBatch, AdminClientPool, KafkaErrorMapper, KafkaFutures}
import kui.kernel.cluster.ClusterConnection
import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.kernel.group.GroupState
import kui.kernel.{BrokerId, ClusterId, TopicName}

/** The three admin calls behind the four rules.
  *
  * ==Why the alerts service makes them itself==
  *
  * Every one of these facts is already read somewhere in KUI — the cluster service sweeps partitions, the
  * consumer service lists groups, the cluster service describes log directories — and reading them through
  * those services would make one service's rules a property of another service's paging. That is
  * `KafkaPartitionSweeper`'s own argument for why the cluster service sweeps rather than folding the topic
  * service's pages, and it holds harder here: an alert that fired on a page would be an alert about how KUI
  * had chunked its request.
  *
  * ==Three readings, three independent refusals==
  *
  * A cluster whose `describeLogDirs` is refused by an ACL still has readable partition counts. Each call
  * answers into its own [[FactReading]], so one refusal costs one rule and the feed keeps the other three —
  * and each refusal travels as a `KuiError` so the row can say what happened in the operator's own vocabulary
  * rather than as an empty list.
  *
  * ==Nothing here decides anything==
  *
  * There is no threshold in this file and no severity. It reads numbers and says which of them it could not
  * read; [[kui.alerts.domain.AlertRules]] is where a number becomes an event.
  */
final class KafkaClusterFacts[F[_]: Async](
    cluster: ClusterId,
    connection: ClusterConnection,
    clusters: ClusterAdmin[F],
    groups: GroupAdmin[F],
    pool: AdminClientPool[F],
    logger: StructuredLogger[F]
) extends ClusterFactsPort[F] {

  import KafkaClusterFacts.*

  /** One pass's worth of facts. The three calls are sequential rather than parallel on purpose: they share
    * one admin client and one bulkhead, and a pass that ran once a minute has no reason to spend concurrency
    * it would only take from a request on the same pool.
    *
    * The argument is ignored: one of these is built per configured cluster and holds that cluster's
    * connection, so the id it would answer for is already fixed. The port still takes one because a
    * composition root that resolved the connection per call would be a second place the cluster lookup lives,
    * and `AlertsWiring` is the first.
    */
  def read(requested: ClusterId): F[ClusterFacts] =
    for {
      partitions <- sweepPartitions
      rebalancing <- listRebalancing
      directories <- describeDirectories
    } yield ClusterFacts(partitions, rebalancing, directories)

  /** `listTopics` then `describeTopics`, chunked, counted into offline and under-replicated.
    *
    * An incomplete sweep is reported **as a fact that was read** and marked incomplete, rather than as an
    * unreadable one, because the two are different and the domain treats them differently: a sweep that could
    * not describe six topics is a sweep whose count is not the cluster's, and `AlertRules` is where that
    * decision is written down and tested.
    */
  private def sweepPartitions: F[FactReading[PartitionFacts]] =
    listTopics.flatMap {
      case Left(failure) => (FactReading.Unreadable(failure): FactReading[PartitionFacts]).pure[F]
      case Right(names) if names.isEmpty =>
        // A cluster with no topics is completely counted and its counts are zeros. That is a measurement,
        // and it must not be confused with the refusal a partial sweep produces.
        (FactReading.Read(PartitionFacts(0, 0, complete = true)): FactReading[PartitionFacts]).pure[F]
      case Right(names) => describeTopics(names).map(FactReading.Read(_))
    }

  private def listTopics: F[Either[KuiError, List[TopicName]]] =
    pool
      .run(connection, ListTopics) { admin =>
        KafkaFutures
          .fromFuture(
            // Internal topics included, for `KafkaPartitionSweeper`'s reason: `__consumer_offsets` holds
            // fifty partitions on most clusters and an under-replicated one of them is an outage.
            Async[F].delay(admin.listTopics(new ListTopicsOptions().listInternal(true)).names())
          )
          .map(_.asScala.toList.map(TopicName.unsafe))
      }
      .attempt
      .map(
        _.leftMap(failure => KafkaErrorMapper.map(ListTopics, failure, connection.admin.apiTimeout.toMillis))
      )

  private def describeTopics(names: List[TopicName]): F[PartitionFacts] =
    AdminBatch
      .chunked[F, TopicName, PartitionCounts](
        names,
        AdminBatch.topicChunk(connection.admin),
        connection.admin.parallelism,
        DescribeTopics
      )(chunk => describeChunk(chunk))
      .map { batch =>
        val counted = batch.values.values.foldLeft(PartitionCounts.empty)(_.combine(_))

        PartitionFacts(
          offline = counted.offline,
          underReplicated = counted.underReplicated,
          complete = batch.skipped.isEmpty
        )
      }

  private def describeChunk(chunk: List[TopicName]): F[Map[TopicName, PartitionCounts]] =
    pool.run(connection, DescribeTopics) { admin =>
      val result = admin.describeTopics(TopicCollection.ofTopicNames(chunk.map(_.value).asJava))

      result.topicNameValues.asScala.toList
        .traverse { (raw, future) =>
          KafkaFutures
            .fromFuture(Async[F].delay(future))
            .map(description =>
              Option(
                TopicName.unsafe(raw) ->
                  PartitionCounts.of(description.partitions.asScala.toList)
              )
            )
            .handleErrorWith(failure =>
              logger
                .debug(failure)(
                  s"topic '$raw' could not be described; the partition rules will report an incomplete sweep"
                )
                .as(None)
            )
        }
        .map(_.flatten.toMap)
    }

  /** The groups the broker says are reassigning, right now.
    *
    * A listing and not a describe: a listing carries the state and costs one round trip for the whole
    * cluster, while describing four thousand groups every minute is a cost nobody agreed to. **How long** a
    * group has been in that state is not in the answer — Kafka publishes no such timestamp — which is why the
    * rule measures from KUI's own first sighting and says so in the event it opens.
    *
    * An incomplete listing is unreadable rather than short. `GroupListingResult` distinguishes "these are the
    * groups" from "these are the groups whose coordinator answered", and a rule that fired on the second
    * would be a rule about which coordinators were up.
    */
  private def listRebalancing: F[FactReading[Set[kui.kernel.GroupId]]] =
    groups.listGroups(connection, RebalancingStates).map {
      case Left(failure) => FactReading.Unreadable(failure)
      case Right(result) if !result.isComplete =>
        FactReading.Unreadable(
          ApplicationError.Refused(
            ErrorCode.UpstreamUnavailable,
            s"${result.coordinatorFailures.size} group coordinator(s) on cluster ${cluster.value} did " +
              "not answer, so KUI cannot say which groups are rebalancing"
          )
        )
      case Right(result) => FactReading.Read(result.groups.map(_.groupId).toSet)
    }

  /** Every broker's log directories, as percentages where a percentage exists.
    *
    * Two calls, because `describeLogDirs` is per broker and the broker list comes from `describeCluster`. A
    * broker that refuses is left out of the list rather than failing the entire reading. The disk rule judges
    * only directories with measured percentages and preserves open alerts for absent subjects, including
    * every directory on a skipped broker. Missing is never evidence of recovery.
    */
  private def describeDirectories: F[FactReading[List[LogDirectoryFact]]] =
    clusters.describeCluster(connection).flatMap {
      case Left(failure) => (FactReading.Unreadable(failure): FactReading[List[LogDirectoryFact]]).pure[F]
      case Right(description) =>
        val brokers = description.nodes.map(_.id).toSet

        if brokers.isEmpty then
          (FactReading.Read(List.empty[LogDirectoryFact]): FactReading[List[LogDirectoryFact]]).pure[F]
        else
          clusters.describeLogDirs(connection, brokers).map {
            case Left(failure) => FactReading.Unreadable(failure)
            case Right(batch) =>
              FactReading.Read(
                batch.values.toList
                  .flatMap((broker, dirs) => dirs.map(factOf(broker, _)))
                  .sortBy(fact => (fact.broker.value, fact.path))
              )
          }
    }
}

object KafkaClusterFacts {

  /** The operation labels. They are the `AdminClientPool` metric attribute and must come from a short closed
    * set, which is why they are constants rather than literals at the call sites.
    */
  val ListTopics: String = "listTopics"
  val DescribeTopics: String = "describeTopics"

  /** The two states Kafka calls a rebalance.
    *
    * `PREPARING_REBALANCE` is members leaving and rejoining; `COMPLETING_REBALANCE` is the assignment being
    * handed out. A group wedged behind a dead member sits in the first for as long as the session timeout
    * allows and then goes round again, so a rule that watched only the second would never see the case it
    * exists for.
    */
  val RebalancingStates: Set[GroupState] =
    Set(GroupState.PreparingRebalance, GroupState.CompletingRebalance)

  /** One log directory, with a percentage only where the broker reported a capacity.
    *
    * `usableBytes` is what is left on the filesystem and `totalBytes` is its size; the share **used** is what
    * remains. Brokers before 3.3 report neither, and a directory with no capacity has no percentage at all —
    * the replica bytes alone cannot say whether they are most of a disk or a rounding error on one. Filling
    * that gap with anything is the fabrication ADR-053 §4 forbids, so the field is `None` and the rule skips
    * the directory and counts it.
    *
    * A directory whose own `error` is set is also `None`: an offline disk answers `KafkaStorageException` for
    * itself, and a disk KUI cannot read is not a disk that is empty.
    */
  def factOf(broker: BrokerId, dir: LogDir): LogDirectoryFact =
    LogDirectoryFact(
      broker = broker,
      path = dir.path,
      usedPercent = for {
        _ <- Option.when(dir.error.isEmpty)(())
        total <- dir.totalBytes if total > 0L
        usable <- dir.usableBytes
        used = total - usable
      } yield percentOf(used, total)
    )

  /** A share as a whole percent, rounded **down** and clamped into `0..100`.
    *
    * Down, because the threshold comparison is `>=` and a directory rounded up to 80% would open an event
    * naming a number the disk had not reached. Clamped, because `usableBytes` can briefly exceed `totalBytes`
    * on a filesystem with reserved blocks, and a negative percentage on a screen reads as a bug in KUI rather
    * than as the rounding it is.
    */
  def percentOf(used: Long, total: Long): Int =
    math.max(0, math.min(100, ((used.toDouble / total.toDouble) * 100.0).floor.toInt))

  /** How many partitions in one topic have no leader, and how many are short of an in-sync replica.
    *
    * The two conditions are not exclusive: a partition with no leader is usually also under-replicated, and
    * both counts include it. That is deliberate — the two rules answer different questions and an operator
    * reading "4 offline, 4 under-replicated" is reading one incident, while a count that subtracted one from
    * the other would be a number neither rule could explain.
    */
  final case class PartitionCounts(offline: Int, underReplicated: Int) {

    def combine(other: PartitionCounts): PartitionCounts =
      PartitionCounts(offline + other.offline, underReplicated + other.underReplicated)
  }

  object PartitionCounts {

    val empty: PartitionCounts = PartitionCounts(0, 0)

    def of(partitions: List[TopicPartitionInfo]): PartitionCounts =
      partitions.foldLeft(empty) { (counts, info) =>
        PartitionCounts(
          offline = counts.offline + (if brokerOf(info.leader).isEmpty then 1 else 0),
          underReplicated = counts.underReplicated +
            (if info.isr.asScala.size < info.replicas.asScala.size then 1 else 0)
        )
      }

    given CanEqual[PartitionCounts, PartitionCounts] = CanEqual.derived
  }

  /** A partition with no leader arrives either as a `null` node or as `Node.noNode()`, whose id is `-1`. Both
    * mean "being elected, or every replica is offline", and neither is a broker.
    */
  private def brokerOf(node: Node): Option[BrokerId] =
    Option(node).filterNot(_.isEmpty).map(found => BrokerId.unsafe(found.id))
}
