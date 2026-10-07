package kui.topic.infrastructure

import java.util.Optional

import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*

import cats.effect.kernel.Async
import cats.syntax.all.*
import org.apache.kafka.clients.admin.{
  AlterConfigOp,
  ConfigEntry,
  DescribeConfigsOptions,
  NewPartitions,
  NewTopic
}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.*
import org.typelevel.log4cats.StructuredLogger

import kui.kafka.{AdminClientPool, KafkaFutures}
import kui.kernel.cluster.ClusterConnection
import kui.kernel.{ClusterId, TopicName}
import kui.topic.domain as dom
import kui.topic.domain.{NewTopicSpec, TopicConfigChange, TopicWriter}

/** The topic domain's `TopicWriter` port, over the raw Kafka `Admin` client.
  *
  * The companion of [[KafkaTopicAdmin]], and it exists as a second class for the same reason the two ports
  * do: this is the file that destroys things, and a reader of the wiring can see at a glance which component
  * was handed that. Everything else about it follows the sibling — the same `AdminClientPool`, so the client
  * lifecycle, the timeouts, the metrics and the reconnect handling are the shared ones; the same `Either`
  * contract, so a failure is a `TopicError` and never a raised exception.
  *
  * ==Every call shape here is documented in the research==
  *
  * `research/kafka/admin-capabilities.md` lists, per operation, the admin call, the broker version it needs
  * and what it throws. This adapter is that table turned into code:
  *
  *   - create — `createTopics(NewTopic)`, Kafka 0.10.1. Absent partitions or replication factor are
  *     `Optional.empty()`, which is what makes the broker apply `num.partitions` and
  *     `default.replication.factor` instead of a number KUI invented.
  *   - alter config — `incrementalAlterConfigs`, Kafka 2.3. Incremental and not the deprecated
  *     `alterConfigs`, which replaces the whole dynamic set and so silently reverts every override the caller
  *     did not resend.
  *   - increase partitions — `createPartitions(NewPartitions.increaseTo)`, Kafka 1.0. Never shrinks; Kafka
  *     has no call that can.
  *   - delete — `deleteTopics`, Kafka 0.10.1. Asynchronous: it returns when the controller has accepted the
  *     deletion, and the topic can still be listed for a moment afterwards.
  *
  * ==Nothing here decides whether it is allowed to run==
  *
  * The read-only refusal and the audit record are `MutationGuard`'s, in the application layer, and they
  * happen before this class is reached. A second check here would be a copy of the rule.
  */
final class KafkaTopicWriter[F[_]: Async](
    pool: AdminClientPool[F],
    connections: ClusterId => Option[ClusterConnection],
    logger: StructuredLogger[F]
) extends TopicWriter[F] {

  import KafkaTopicWriter.*

  def create(cluster: ClusterId, spec: NewTopicSpec): F[Either[dom.TopicError, Unit]] =
    withConnection(cluster, Some(spec.name)) { connection =>
      pool.run(connection, "createTopics") { admin =>
        val topic = new NewTopic(
          spec.name.value,
          // `Optional.empty()` and not a default: see the class comment, and `NewTopicSpec`'s.
          spec.partitions.map(Integer.valueOf).toJava: Optional[Integer],
          spec.replicationFactor.map(java.lang.Short.valueOf).toJava: Optional[java.lang.Short]
        ).configs(spec.config.asJava)

        KafkaFutures.fromFuture(Async[F].delay(admin.createTopics(List(topic).asJava).all())).void
      }
    }

  def alterConfig(
      cluster: ClusterId,
      topic: TopicName,
      change: TopicConfigChange
  ): F[Either[dom.TopicError, Unit]] =
    withConnection(cluster, Some(topic)) { connection =>
      pool.run(connection, "incrementalAlterConfigs") { admin =>
        val resource = new ConfigResource(ConfigResource.Type.TOPIC, topic.value)

        val operations =
          change.set.toList.map((key, value) =>
            new AlterConfigOp(new ConfigEntry(key, value), AlterConfigOp.OpType.SET)
          ) ++
            // `DELETE` puts the key back to whatever the broker's default for it is, which is a
            // different outcome from setting it to the default's current value: a broker default that
            // changes later then moves this topic with it, which is what "not overridden" means.
            //
            // The empty string is not a value being set: `AlterConfigOp` requires a `ConfigEntry`, and
            // Kafka ignores its value entirely for `DELETE`. An empty string rather than a `null`
            // because a `null` in this codebase is a lint failure, and because the two reach the broker
            // identically.
            change.remove.toList.map(key =>
              new AlterConfigOp(new ConfigEntry(key, ""), AlterConfigOp.OpType.DELETE)
            )

        KafkaFutures
          .fromFuture(
            Async[F].delay(admin.incrementalAlterConfigs(Map(resource -> operations.asJava).asJava).all())
          )
          .void
      }
    }

  def increasePartitions(
      cluster: ClusterId,
      topic: TopicName,
      target: Int
  ): F[Either[dom.TopicError, Unit]] =
    withConnection(cluster, Some(topic)) { connection =>
      pool.run(connection, "createPartitions") { admin =>
        KafkaFutures
          .fromFuture(
            Async[F].delay(
              admin.createPartitions(Map(topic.value -> NewPartitions.increaseTo(target)).asJava).all()
            )
          )
          .void
      }
    }

  def delete(cluster: ClusterId, topic: TopicName): F[Either[dom.TopicError, Unit]] =
    withConnection(cluster, Some(topic)) { connection =>
      pool.run(connection, "deleteTopics") { admin =>
        KafkaFutures
          .fromFuture(
            Async[F].delay(
              admin
                .deleteTopics(org.apache.kafka.common.TopicCollection.ofTopicNames(List(topic.value).asJava))
                .all()
            )
          )
          .void
      }
    }

  def deleteById(cluster: ClusterId, topic: TopicName, topicId: String): F[Either[dom.TopicError, Unit]] =
    withConnection(cluster, Some(topic)) { connection =>
      pool.run(connection, "deleteTopics") { admin =>
        KafkaFutures
          .fromFuture(Async[F].delay {
            val id = org.apache.kafka.common.Uuid.fromString(topicId)
            require(id != org.apache.kafka.common.Uuid.ZERO_UUID, "topic identity must be known")
            admin.deleteTopics(org.apache.kafka.common.TopicCollection.ofTopicIds(List(id).asJava)).all()
          })
          .void
      }
    }

  /** `auto.create.topics.enable`, read off any one broker.
    *
    * It is a static broker setting, so every broker in a healthy cluster has the same value and the first one
    * that answers is enough. `None` on any failure at all — an unreachable cluster, a broker that will not be
    * described, no brokers in the metadata — because this is a *warning* on a plan, and a plan that refused
    * to describe a delete because it could not compute a warning would be worse than a plan that says it does
    * not know.
    */
  def autoCreateEnabled(cluster: ClusterId): F[Option[Boolean]] =
    connections(cluster) match {
      case None => Option.empty[Boolean].pure[F]
      case Some(connection) =>
        pool
          .run(connection, "describeConfigs.broker") { admin =>
            for {
              nodes <- KafkaFutures.fromFuture(Async[F].delay(admin.describeCluster().nodes()))
              answer <- nodes.asScala.headOption match {
                case None => Option.empty[Boolean].pure[F]
                case Some(node) =>
                  val resource = new ConfigResource(ConfigResource.Type.BROKER, node.idString)

                  KafkaFutures
                    .fromFuture(
                      Async[F].delay(
                        admin
                          .describeConfigs(List(resource).asJava, new DescribeConfigsOptions())
                          .all()
                      )
                    )
                    .map(
                      _.asScala
                        .get(resource)
                        .flatMap(config => Option(config.get(AutoCreateTopics)))
                        .flatMap(entry => Option(entry.value))
                        .flatMap(_.toBooleanOption)
                    )
              }
            } yield answer
          }
          .handleErrorWith(failure =>
            logger
              .debug(failure)(
                s"cluster ${cluster.value} did not answer describeConfigs for $AutoCreateTopics; a " +
                  "deletion plan will report that it does not know whether the topic can be recreated"
              )
              .as(Option.empty[Boolean])
          )
    }

  // ---------------------------------------------------------------------------------- plumbing

  private def withConnection(
      cluster: ClusterId,
      topic: Option[TopicName]
  )(call: ClusterConnection => F[Unit]): F[Either[dom.TopicError, Unit]] =
    connections(cluster) match {
      case None => dom.TopicError.ClusterNotFound(cluster).asLeft[Unit].pure[F]
      case Some(connection) => call(connection).attempt.map(_.leftMap(writeError(_, topic)))
    }
}

object KafkaTopicWriter {

  private val AutoCreateTopics: String = "auto.create.topics.enable"

  /** Every way a Kafka mutation can be refused, given a name.
    *
    * The read adapter's `topicError` is deliberately not reused: its whole vocabulary is "not found",
    * "forbidden" and "unreachable", because those are the only ways a *read* fails. A write has a fifth
    * shape, and it is the common one — the cluster is up, KUI is authorized, the topic is there, and the
    * request is refused because a replication factor is larger than the cluster, a configuration key does not
    * exist, a partition count is not an increase, or the operator's cluster has topic deletion turned off.
    * Reporting those as `Unreachable` would put a broker outage on the screen when the fix is to change a
    * number in a form.
    *
    * The exceptions are matched by type and the sentence shown is KUI's own, never the exception's message: a
    * Kafka exception's message routinely carries the bootstrap string and, on some SASL paths, the principal.
    * The original goes to the log with its stack trace.
    */
  private[infrastructure] def writeError(
      failure: Throwable,
      topic: Option[TopicName]
  ): dom.TopicError = {
    val unwrapped = KafkaFutures.unwrap(failure)

    unwrapped match {
      case _: TopicExistsException =>
        topic.fold(dom.TopicError.Rejected("the topic already exists"))(dom.TopicError.AlreadyExists.apply)

      case _: UnknownTopicOrPartitionException | _: UnknownTopicIdException =>
        topic.fold(dom.TopicError.Rejected("the topic does not exist"))(dom.TopicError.NotFound.apply)

      case _: TopicAuthorizationException | _: ClusterAuthorizationException =>
        dom.TopicError.Forbidden(describe(unwrapped))

      case _: TopicDeletionDisabledException =>
        dom.TopicError.Rejected(
          "This cluster has delete.topic.enable=false, so its brokers refuse every topic deletion. It is a " +
            "broker setting and cannot be changed from here."
        )

      // Before the `InvalidRequestException` case below, and it has to be: Kafka's
      // `UnsupportedVersionException` is a *subclass* of `InvalidRequestException`, so matching the
      // parent first would swallow it and tell an operator to fix their configuration when the real
      // answer is that their brokers are too old for the call.
      case _: UnsupportedVersionException =>
        dom.TopicError.Unreachable(
          "this cluster's brokers are too old for this operation",
          retryable = false
        )

      case _: InvalidTopicException =>
        dom.TopicError.Rejected("the broker will not accept that topic name")

      case _: InvalidReplicationFactorException =>
        dom.TopicError.Rejected(
          "the replication factor is larger than the number of brokers this cluster has"
        )

      case _: InvalidPartitionsException =>
        dom.TopicError.Rejected(
          "the broker refused the partition count; a topic's partitions can only ever be increased"
        )

      case _: InvalidConfigurationException | _: InvalidRequestException =>
        dom.TopicError.Rejected(
          "the broker refused one of the configuration entries; a key it does not know, or a value it " +
            "will not accept for that key"
        )

      case _: PolicyViolationException =>
        dom.TopicError.Rejected("a policy configured on the broker refused this change")

      case _: InvalidReplicaAssignmentException =>
        dom.TopicError.Rejected("the broker refused the replica assignment")

      case _: ReassignmentInProgressException =>
        dom.TopicError.Rejected(
          "a partition reassignment is already running on this topic; wait for it to finish and try again"
        )

      case _: TimeoutException =>
        // Retryable, and worth stating plainly in the message the caller will show: a timed-out
        // mutation is not a mutation that did not happen. `deleteTopics` in particular can be accepted
        // by the controller and still time out on the way back.
        dom.TopicError.Unreachable(
          "the cluster did not answer in time; the change may still have been applied",
          retryable = true
        )

      case _ => dom.TopicError.Unreachable(describe(unwrapped), retryable = true)
    }
  }

  /** KUI's words for a failure, from the exception's class and never its message. */
  private def describe(failure: Throwable): String = {
    val name = failure.getClass.getSimpleName
    if name.endsWith("Exception") then name.dropRight("Exception".length) else name
  }
}
