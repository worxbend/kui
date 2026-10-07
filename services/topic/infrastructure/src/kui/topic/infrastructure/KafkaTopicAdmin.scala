package kui.topic.infrastructure

import scala.jdk.CollectionConverters.*

import cats.effect.kernel.Async
import cats.syntax.all.*
import org.apache.kafka.clients.admin.{
  Admin,
  Config,
  DescribeConfigsOptions,
  DescribeLogDirsOptions,
  ListTopicsOptions,
  OffsetSpec,
  TopicDescription
}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.{Node, TopicCollection, TopicPartition, TopicPartitionInfo}
import org.typelevel.log4cats.StructuredLogger

import kui.kafka.admin.{AdminConversions, LogDir}
import kui.kafka.{AdminBatch, AdminClientPool, KafkaErrorMapper, KafkaFutures}
import kui.kernel.cluster.ClusterConnection
import kui.kernel.{BrokerId, ClusterId, PartitionId, TopicName}
import kui.topic.application.InternalTopics
import kui.topic.domain as dom
import kui.topic.domain.{ScrapeResult, TopicAdmin as TopicAdminPort}

/** The topic domain's `TopicAdmin` port, over the raw Kafka `Admin` client.
  *
  * This is the file the port's own scaladoc promises: "one exhaustively-matched file in `infrastructure`,
  * which is where every shape a real cluster produces gets a name". Kafka's vocabulary goes in — a `Node`
  * whose id is `-1` because the partition has no leader, a `KafkaFuture` per topic so that one unreadable
  * topic does not fail the other nine thousand — and the topic domain's vocabulary comes out.
  *
  * ==Why it does not go through `libs/kafka`==
  *
  * It should. The M2 plan's tasks TOP-002…TOP-005 put a `TopicAdmin` beside `ClusterAdmin` and `GroupAdmin`
  * in `libs/kafka`, so that the message service and the consumer service can reuse the same offset and
  * describe calls. That module was not built. Rather than leave the whole topic service unreachable waiting
  * for it, the Kafka calls live here, written against the same `AdminClientPool` that `libs/kafka` uses — so
  * the client lifecycle, the timeouts, the metrics and the reconnect handling are still the shared ones, and
  * only the four call shapes are local. Hoisting them later is a move, not a rewrite.
  *
  * ==Guarantees==
  *
  * Every method is total: a failure is a `TopicError`, never a raised exception. That is the port's contract
  * and `PortContractSuite` asserts it.
  *
  * @param connections
  *   turns a cluster id into connection material. `None` is `TopicError.ClusterNotFound`, which is a 404 at
  *   the edge and never an empty list.
  * @param internalPrefix
  *   `kui.topics.internalPrefix`. This adapter is the one place in the product that holds both halves of
  *   "internal" at once — Kafka's `isInternal` flag comes back on the wire here, and the prefix is pure
  *   configuration — so this is where [[InternalTopics.isInternal]] combines them, exactly once, for both the
  *   list and the detail page. Before this parameter existed the flag was passed through alone and the
  *   configured prefix did nothing at all: a cluster's `_schemas` or `__kui_config` sat in the operator's own
  *   topic list with no way to hide it.
  */
final class KafkaTopicAdmin[F[_]: Async](
    pool: AdminClientPool[F],
    connections: ClusterId => Option[ClusterConnection],
    internalPrefix: String,
    logger: StructuredLogger[F]
) extends TopicAdminPort[F] {

  import KafkaTopicAdmin.*

  // ---------------------------------------------------------------------------------- scrape

  def scrape(cluster: ClusterId): F[Either[dom.TopicError, ScrapeResult]] =
    withConnection(cluster) { connection =>
      for {
        listings <- listTopics(connection)
        described <- describeTopics(connection, listings.keys.toList)
        // Offsets are asked for only for the partitions that have a leader. A leaderless partition
        // cannot answer a `listOffsets`, and `PartitionView.from` rejects a leaderless partition that
        // carries offsets anyway — an invariant that exists precisely so that a number nobody could
        // have measured never reaches a screen.
        offsets <- listOffsets(connection, described.values.toList.flatMap(leadPartitions))
        // The one call that can answer "how much disk is this topic using". It is a per-broker call and
        // it is made once per scrape, not once per page view, which is what makes it affordable here and
        // not on the topic detail page.
        sizes <- replicaSizes(connection, described.values.toList)
        // The list's cleanup column, in as few round trips as the batch size allows. One
        // `describeConfigs` per row is what made this field expensive enough that nobody added it: on ten
        // thousand topics that is ten thousand calls for one string each. Batched at `ConfigBatch` = 200, it
        // is fifty.
        policies <- cleanupPolicies(connection, described.keys.toList)
        rows = described.toList.map { case (name, description) =>
          dom.TopicSummary.of(
            name = name,
            isInternal = InternalTopics
              .isInternal(name, listings.getOrElse(name, description.isInternal), internalPrefix),
            partitions =
              description.partitions.asScala.toList.flatMap(partitionView(name, _, offsets, sizes)),
            cleanupPolicy = policies.get(name)
          )
        }
        incomplete = listings.keySet.diff(described.keySet).map(_ -> UnreadableTopic).toMap
      } yield ScrapeResult(rows.sortBy(_.name.value), incomplete)
    }

  // ---------------------------------------------------------------------------------- detail

  def detail(cluster: ClusterId, topic: TopicName): F[Either[dom.TopicError, dom.TopicDetail]] =
    withConnection(cluster, Some(topic)) { connection =>
      for {
        described <- describeOne(connection, topic)
        offsets <- listOffsets(connection, leadPartitions(described))
        // The Settings tab reads the whole configuration; the detail header needs exactly one key of
        // it, and asking for it here is what lets the header say "compact" without the user opening
        // another tab. A configuration KUI may not read costs the header that one field and nothing
        // else, which is why this is an `attempt`-shaped read rather than a second failure mode.
        policy <- cleanupPolicy(connection, topic)
      } yield dom.TopicDetail.of(
        name = topic,
        isInternal = InternalTopics.isInternal(topic, described.isInternal, internalPrefix),
        partitions = described.partitions.asScala.toList.flatMap(partitionView(topic, _, offsets)),
        cleanupPolicy = policy,
        // Segment counts come from `describeLogDirs`, which is a per-broker call over every partition
        // on the cluster. It is not worth a topic page's latency, and a number that is sometimes there
        // and sometimes not is worse than one that is honestly absent.
        segmentCount = None,
        topicId =
          Option(described.topicId).filterNot(_ == org.apache.kafka.common.Uuid.ZERO_UUID).map(_.toString)
      )
    }

  // ---------------------------------------------------------------------------------- config

  def config(cluster: ClusterId, topic: TopicName): F[Either[dom.TopicError, dom.TopicConfigView]] =
    withConnection(cluster, Some(topic)) { connection =>
      describeConfigs(connection, topic).map(entries => dom.TopicConfigView.of(entries))
    }.flatMap {
      // A topic KUI may see and may not describe is `NotPermitted` and never `Forbidden`: a 403 here
      // would take the whole topic page down and the partitions the user *is* entitled to see would
      // vanish with the tab they are not. The port's own scaladoc requires this, and it is the one
      // place in the adapter where a typed failure is deliberately turned back into a value.
      case Left(dom.TopicError.Forbidden(detail)) =>
        dom.TopicConfigView.NotPermitted(detail).asRight[dom.TopicError].pure[F].widen
      case other => other.pure[F]
    }

  // ---------------------------------------------------------------------------------- Kafka calls

  private def listTopics(connection: ClusterConnection): F[Map[TopicName, Boolean]] =
    pool.run(connection, "listTopics") { admin =>
      KafkaFutures
        .fromFuture(Async[F].delay(admin.listTopics(new ListTopicsOptions().listInternal(true)).listings()))
        .map(_.asScala.toList.map(listing => TopicName.unsafe(listing.name) -> listing.isInternal).toMap)
    }

  /** Every topic, one future each, so that a topic KUI may not describe becomes a row in `incomplete` rather
    * than the failure of the whole list.
    *
    * `topicNameValues` and not `all`: `all` is a single future that fails if any one topic fails, which would
    * turn one unauthorized topic into an empty topics screen.
    */
  private def describeTopics(
      connection: ClusterConnection,
      names: List[TopicName]
  ): F[Map[TopicName, TopicDescription]] =
    if names.isEmpty then Map.empty[TopicName, TopicDescription].pure[F]
    else
      names
        .grouped(DescribeBatch)
        .toList
        .flatTraverse { batch =>
          pool.run(connection, "describeTopics") { admin =>
            val result = admin.describeTopics(TopicCollection.ofTopicNames(batch.map(_.value).asJava))

            result.topicNameValues.asScala.toList.traverse { case (raw, future) =>
              KafkaFutures
                .fromFuture(Async[F].delay(future))
                .map(description => Option(TopicName.unsafe(raw) -> description))
                .handleErrorWith(failure =>
                  logger
                    .debug(failure)(s"topic '$raw' could not be described and is reported as incomplete")
                    .as(None)
                )
            }
          }
        }
        .map(_.flatten.toMap)

  private def describeOne(connection: ClusterConnection, topic: TopicName): F[TopicDescription] =
    pool.run(connection, "describeTopic") { admin =>
      KafkaFutures.fromFuture(
        Async[F].delay(
          admin
            .describeTopics(TopicCollection.ofTopicNames(List(topic.value).asJava))
            .topicNameValues
            .get(topic.value)
        )
      )
    }

  /** The earliest and latest offset of every partition given, in one round trip per bound.
    *
    * A failure here costs the counts and not the page: a topic list whose message counts are blank is
    * readable, and one that failed to render is not. So this returns what it managed to read.
    */
  private def listOffsets(
      connection: ClusterConnection,
      partitions: List[TopicPartition]
  ): F[OffsetBounds] =
    if partitions.isEmpty then OffsetBounds.empty.pure[F]
    else
      (
        bound(connection, partitions, OffsetSpec.earliest(), "listOffsets.earliest"),
        bound(connection, partitions, OffsetSpec.latest(), "listOffsets.latest")
      ).tupled.map(OffsetBounds.apply)

  private def bound(
      connection: ClusterConnection,
      partitions: List[TopicPartition],
      spec: OffsetSpec,
      operation: String
  ): F[Map[TopicPartition, Long]] =
    partitions
      .grouped(OffsetBatch)
      .toList
      .flatTraverse { batch =>
        pool
          .run(connection, operation) { admin =>
            KafkaFutures
              .fromFuture(Async[F].delay(admin.listOffsets(batch.map(_ -> spec).toMap.asJava).all()))
              .map(_.asScala.toList.map((partition, info) => partition -> info.offset))
          }
          .handleErrorWith(failure =>
            logger
              .warn(failure)(
                s"cluster ${connection.id.value} did not answer $operation; message counts will be absent"
              )
              .as(Nil)
          )
      }
      .map(_.toMap)

  /** How many bytes each topic-partition occupies on disk, read from the brokers' log directories.
    *
    * `describeLogDirs` is the only Kafka call that reports a size, and it reports it per replica: the same
    * partition is listed once by every broker that stores a copy. The sizes are summed, so a topic with three
    * replicas reports the disk all three copies occupy together — the figure an operator sizing a cluster
    * needs, and the one the reference products show.
    *
    * One call per broker, bounded by the cluster's own `admin.parallelism`, because a single call covering
    * every broker is stalled by one slow disk and its timeout then loses every broker's figures
    * (`research/kafka/admin-capabilities.md` §1, "Log dirs"). A broker that does not answer is not silently
    * dropped: it comes back in `unreadableBrokers`, and every partition with a replica there reports no size
    * at all rather than the sum of the copies that did answer, which would be a real number that is too
    * small.
    *
    * The brokers to ask are taken from the replica assignments already in hand, so this adds no
    * `describeCluster` round trip and asks only brokers that actually store something.
    */
  private def replicaSizes(
      connection: ClusterConnection,
      descriptions: List[TopicDescription]
  ): F[ReplicaSizes] = {
    val brokers = descriptions
      .flatMap(_.partitions.asScala.toList.flatMap(_.replicas.asScala.toList))
      .map(_.id)
      .filter(_ >= 0)
      .distinct
      .sorted
      .map(BrokerId.unsafe)

    if brokers.isEmpty then ReplicaSizes.unavailable.pure[F]
    else
      AdminBatch
        .perBroker[F, BrokerId, List[LogDir]](
          brokers,
          connection.admin.parallelism,
          "describeLogDirs"
        )(broker => logDirsOf(connection, broker))
        .flatMap { result =>
          val unreadable = result.skipped.keySet

          val warn =
            if unreadable.isEmpty then Async[F].unit
            else
              logger.warn(
                s"cluster ${connection.id.value} did not report log directories for brokers " +
                  s"${unreadable.toList.map(_.value).sorted.mkString(", ")}; the topics stored there " +
                  "will report no size"
              )

          warn.as(ReplicaSizes(sizesOf(result.values.values.toList.flatten), unreadable))
        }
  }

  /** One broker's log directories. `descriptions()` and not `allDescriptions()`, so that a broker answering
    * for itself is not lost to another broker's failure.
    */
  private def logDirsOf(connection: ClusterConnection, broker: BrokerId): F[List[LogDir]] =
    pool.run(connection, "describeLogDirs") { admin =>
      val result =
        admin.describeLogDirs(List(Integer.valueOf(broker.value)).asJava, new DescribeLogDirsOptions())

      Option(result.descriptions.get(Integer.valueOf(broker.value))) match {
        case Some(future) =>
          KafkaFutures.fromFuture(Async[F].delay(future)).map(AdminConversions.logDirs)
        case None => Async[F].pure(List.empty[LogDir])
      }
    }

  private def describeConfigs(
      connection: ClusterConnection,
      topic: TopicName
  ): F[List[dom.TopicConfigEntry]] =
    pool.run(connection, "describeConfigs") { admin =>
      val resource = new ConfigResource(ConfigResource.Type.TOPIC, topic.value)
      val options = new DescribeConfigsOptions().includeSynonyms(true).includeDocumentation(true)

      KafkaFutures
        .fromFuture(Async[F].delay(admin.describeConfigs(List(resource).asJava, options).all()))
        .map(_.asScala.get(resource).toList.flatMap(_.entries.asScala.toList.map(configEntry)))
    }

  private def cleanupPolicy(connection: ClusterConnection, topic: TopicName): F[Option[String]] =
    describeConfigs(connection, topic)
      .map(_.find(_.name == CleanupPolicy).flatMap(_.value))
      .handleError(_ => None)

  /** Every topic's `cleanup.policy`, batched, for the topics the batch could cover.
    *
    * ==Batched, and one future per topic inside each batch==
    *
    * `describeConfigs` takes a collection of resources, and its `values` map gives a future per resource, so
    * this borrows [[describeTopics]]'s shape for the same reason: a topic KUI may see and may not describe
    * costs that topic's policy and not the whole batch. A batch that fails as a whole — the call refused, the
    * connection dropped — costs the policies of the topics in it and nothing else. Either way the rows are
    * still returned with `cleanupPolicy = None`, because a scrape that dropped its rows over a cosmetic
    * column would be a worse answer than the em dash.
    *
    * No synonyms and no documentation: this asks for one key of each topic's configuration and the Settings
    * tab's own read is what needs the rest. Including them here would multiply the size of a response
    * covering two hundred topics for two fields nothing on the list renders.
    */
  private def cleanupPolicies(
      connection: ClusterConnection,
      topics: List[TopicName]
  ): F[Map[TopicName, String]] =
    if topics.isEmpty then Map.empty[TopicName, String].pure[F]
    else
      topics
        .grouped(ConfigBatch)
        .toList
        .flatTraverse { batch =>
          pool
            .run(connection, "describeConfigs.batch") { admin =>
              val resources = batch.map(topic => new ConfigResource(ConfigResource.Type.TOPIC, topic.value))
              val result = admin.describeConfigs(resources.asJava, new DescribeConfigsOptions())

              result.values.asScala.toList.traverse { case (resource, future) =>
                KafkaFutures
                  .fromFuture(Async[F].delay(future))
                  .map(config => cleanupPolicyOf(config).map(TopicName.unsafe(resource.name) -> _))
                  .handleErrorWith(failure =>
                    logger
                      .debug(failure)(
                        s"the cleanup policy of '${resource.name}' could not be read; its row reports none"
                      )
                      .as(None)
                  )
              }
            }
            .handleErrorWith(failure =>
              logger
                .debug(failure)(
                  s"cluster ${connection.id.value} did not answer describeConfigs for a batch of " +
                    s"${batch.size} topics; those rows report no cleanup policy"
                )
                .as(Nil)
            )
        }
        .map(_.flatten.toMap)

  // ---------------------------------------------------------------------------------- plumbing

  /** Resolves the cluster, runs the call, and turns anything thrown into a `TopicError`. */
  private def withConnection[A](
      cluster: ClusterId,
      topic: Option[TopicName] = None
  )(call: ClusterConnection => F[A]): F[Either[dom.TopicError, A]] =
    connections(cluster) match {
      case None => dom.TopicError.ClusterNotFound(cluster).asLeft[A].pure[F]
      case Some(connection) =>
        call(connection).attempt.map(_.leftMap(topicError(_, topic)))
    }

  /** @param topic
    *   the topic the call was about, when it was about one. It is what lets `UnknownTopicOrPartition` become
    *   a `NotFound` that names the topic the user asked for; a scrape is about no topic in particular, so the
    *   same exception there is reported as an unreachable cluster rather than as a 404 for a name nobody
    *   typed.
    */
  private def topicError(failure: Throwable, topic: Option[TopicName]): dom.TopicError =
    KafkaErrorMapper.classify(failure) match {
      case KafkaErrorMapper.FailureClass.NotAuthorized =>
        dom.TopicError.Forbidden(KafkaTopicAdmin.describe(failure))
      case KafkaErrorMapper.FailureClass.NotFound =>
        topic.fold(dom.TopicError.Unreachable(KafkaTopicAdmin.describe(failure), retryable = true))(
          dom.TopicError.NotFound.apply
        )
      case KafkaErrorMapper.FailureClass.Unsupported =>
        dom.TopicError.Unreachable(KafkaTopicAdmin.describe(failure), retryable = false)
      case _ =>
        dom.TopicError.Unreachable(KafkaTopicAdmin.describe(failure), retryable = true)
    }
}

object KafkaTopicAdmin {

  /** How many topics are described in one admin request.
    *
    * Kafka answers a `describeTopics` of ten thousand names in a single response that the broker has to build
    * in memory, and a request big enough to matter is also a request big enough to time out as a whole.
    * Batching is what turns one all-or-nothing call into a sequence whose failures are localised.
    */
  private val DescribeBatch: Int = 500

  /** The same, for partitions: a cluster with ten thousand topics has far more partitions than topics. */
  private val OffsetBatch: Int = 2000

  /** How many topics' configurations are asked for in one `describeConfigs`.
    *
    * Smaller than [[DescribeBatch]] because a config response carries every key of every topic in it, which
    * is two orders of magnitude more bytes per topic than a description. The number that matters is that it
    * is not one: the reason the list's cleanup column did not exist is that a call per row is a call per row.
    */
  private val ConfigBatch: Int = 200

  private val CleanupPolicy: String = "cleanup.policy"

  /** The sentence shown against a topic that could not be described. Display text, one sentence, safe to show
    * a user — `ScrapeResult.incomplete`'s contract.
    */
  private val UnreadableTopic: String =
    "this topic could not be described; KUI may not be authorized to read it, or it was deleted during " +
      "the scrape"

  /** KUI's words for a failure, from the exception's class and never its message.
    *
    * A Kafka exception's message routinely carries the bootstrap string and, on some SASL paths, the
    * principal. The original goes to the log with its stack trace; the class name goes to the user.
    */
  private def describe(failure: Throwable): String = {
    val name = KafkaFutures.unwrap(failure).getClass.getSimpleName
    if name.endsWith("Exception") then name.dropRight("Exception".length) else name
  }

  /** The earliest and latest offset of each partition, whichever of the two calls answered. */
  final case class OffsetBounds(earliest: Map[TopicPartition, Long], latest: Map[TopicPartition, Long])

  object OffsetBounds {
    val empty: OffsetBounds = OffsetBounds(Map.empty, Map.empty)
  }

  /** What the brokers said about disk, and which brokers said nothing.
    *
    * The second field is why this is a type and not a `Map`. A partition's size is the sum over its replicas,
    * and a sum is only correct when every replica was counted; without the set of brokers that failed there
    * is no way to tell "this partition holds 40 MB" from "this partition holds 40 MB on the one broker of
    * three that answered". The first is a fact and the second is an understatement that looks like a fact.
    */
  final case class ReplicaSizes(bytes: Map[TopicPartition, Long], unreadableBrokers: Set[BrokerId]) {

    /** The partition's size, or `None` when nobody could have measured it.
      *
      * `None` in three situations, all of them honest: a replica sits on a broker that did not answer, the
      * whole call was never made or failed everywhere, or the partition appeared in no directory listing at
      * all — which is what a replica on an offline disk looks like, since the directory carrying it reports
      * an error instead of its contents.
      */
    def sizeOf(partition: TopicPartition, replicas: List[BrokerId]): Option[Long] =
      if replicas.exists(unreadableBrokers.contains) then None else bytes.get(partition)
  }

  object ReplicaSizes {

    /** No sizes and no named failure: what a scrape uses before it has asked, and what the pure conversions
      * are tested against.
      */
    val unavailable: ReplicaSizes = ReplicaSizes(Map.empty, Set.empty)
  }

  /** Sums the replica entries of every directory by topic-partition.
    *
    * Future replicas are excluded. A `isFuture` entry is a second copy being written by an in-progress
    * `alterReplicaLogDirs` move, and counting it would report a partition as twice its size for as long as
    * the move runs.
    */
  private[infrastructure] def sizesOf(dirs: List[LogDir]): Map[TopicPartition, Long] =
    dirs
      .flatMap(_.replicas)
      .filterNot(_.isFuture)
      .foldLeft(Map.empty[TopicPartition, Long]) { (totals, replica) =>
        val key = new TopicPartition(replica.topic.value, replica.partition.value)
        totals.updated(key, totals.getOrElse(key, 0L) + replica.sizeBytes)
      }

  /** The partitions of a topic that have a leader, as Kafka's own key type. */
  private[infrastructure] def leadPartitions(description: TopicDescription): List[TopicPartition] =
    description.partitions.asScala.toList
      .filter(info => leaderOf(info).isDefined)
      .map(info => new TopicPartition(description.name, info.partition))

  /** A partition's leader, with Kafka's two ways of saying "there isn't one" folded into `None`.
    *
    * `null` and a `Node` whose id is `-1` (`Node.noNode`) both mean leaderless, and code that checks only one
    * of them reports broker `-1` as the leader of an offline partition.
    */
  private[infrastructure] def leaderOf(info: TopicPartitionInfo): Option[Node] =
    Option(info.leader).filter(_.id >= 0)

  /** One partition, in the domain's words, or nothing when the cluster described something impossible.
    *
    * `PartitionView.from` can refuse: a replica listed twice, an in-sync replica that is not a replica, a
    * leader that is not one of the replicas, an earliest offset after the latest. Every one of those is a
    * statement no healthy broker makes, and each has an invariant precisely because rendering it would put a
    * number on a screen that cannot be true — "more replicas in sync than exist" is the example the
    * invariant's own message gives.
    *
    * So a refusal drops that one partition rather than raising. Dropping is visible (the partition table is
    * short and the count beside it does not match) and raising is not (the whole topic list would blank over
    * one bad row). Neither is good; the visible one is the one an operator can act on.
    */
  private[infrastructure] def partitionView(
      topic: TopicName,
      info: TopicPartitionInfo,
      offsets: OffsetBounds,
      sizes: ReplicaSizes = ReplicaSizes.unavailable
  ): Option[dom.PartitionView] = {
    val key = new TopicPartition(topic.value, info.partition)
    val leader = leaderOf(info).map(node => BrokerId.unsafe(node.id))
    val replicas = info.replicas.asScala.toList.map(node => BrokerId.unsafe(node.id))

    dom.PartitionView
      .from(
        partition = PartitionId.unsafe(info.partition),
        leader = leader,
        replicas = replicas,
        inSync = info.isr.asScala.toList.map(node => BrokerId.unsafe(node.id)),
        // Offsets only where there is a leader. A leaderless partition cannot answer a `listOffsets`,
        // and the invariant rejects one that carries offsets anyway — which is how a number nobody
        // could have measured is kept off a screen.
        earliestOffset = leader.flatMap(_ => offsets.earliest.get(key)),
        latestOffset = leader.flatMap(_ => offsets.latest.get(key)),
        sizeBytes = sizes.sizeOf(key, replicas)
      )
      .toOption
  }

  /** One topic's `cleanup.policy`, out of the configuration the broker reported for it.
    *
    * `None` for a key the broker did not report and for one it reported without a value. Never a default
    * substituted here: `delete` is Kafka's default and writing it in would turn "KUI could not read this
    * topic's configuration" into a confident statement about a topic whose policy might be `compact`, which
    * is the difference between a topic that keeps its records and one that does not.
    */
  private[infrastructure] def cleanupPolicyOf(config: Config): Option[String] =
    Option(config.get(CleanupPolicy)).flatMap(entry => Option(entry.value))

  /** Kafka's `ConfigEntry` in the topic domain's words, synonyms included.
    *
    * The synonyms are the point. `TopicConfigEntry.defaultValue` derives the default from the synonym whose
    * source is `DEFAULT_CONFIG` rather than storing it, so "is this setting overridden" is answered from what
    * the broker reported instead of from a table KUI would have to keep in step with every Kafka release.
    */
  private[infrastructure] def configEntry(
      raw: org.apache.kafka.clients.admin.ConfigEntry
  ): dom.TopicConfigEntry =
    dom.TopicConfigEntry(
      name = raw.name,
      value = Option(raw.value),
      source = configSource(raw.source),
      isSensitive = raw.isSensitive,
      isReadOnly = raw.isReadOnly,
      documentation = Option(raw.documentation),
      synonyms = Option(raw.synonyms).toList.flatMap(_.asScala.toList).map { synonym =>
        dom.ConfigSynonym(synonym.name, Option(synonym.value), configSource(synonym.source))
      }
    )

  private[infrastructure] def configSource(
      raw: org.apache.kafka.clients.admin.ConfigEntry.ConfigSource
  ): dom.ConfigSource = {
    import org.apache.kafka.clients.admin.ConfigEntry.ConfigSource as Kafka

    raw match {
      case Kafka.DYNAMIC_TOPIC_CONFIG => dom.ConfigSource.DynamicTopic
      case Kafka.DYNAMIC_BROKER_CONFIG | Kafka.DYNAMIC_DEFAULT_BROKER_CONFIG =>
        dom.ConfigSource.DynamicDefaultBroker
      case Kafka.STATIC_BROKER_CONFIG => dom.ConfigSource.StaticBroker
      case Kafka.DEFAULT_CONFIG => dom.ConfigSource.Default
      case _ => dom.ConfigSource.Unknown
    }
  }

  /** An `Admin` call, for a test that wants to drive this adapter without a pool. */
  private[infrastructure] type Call[F[_], A] = Admin => F[A]
}
