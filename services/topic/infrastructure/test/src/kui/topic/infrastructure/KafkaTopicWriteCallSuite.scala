package kui.topic.infrastructure

import java.util.concurrent.atomic.AtomicReference

import scala.jdk.CollectionConverters.*

import cats.effect.IO
import org.apache.kafka.clients.admin.{
  Admin,
  AlterConfigOp,
  CreateTopicsResult,
  KuiTopicAdminResults,
  NewTopic
}
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.config.ConfigResource

import kui.kernel.cluster.{
  AdminTuning,
  BootstrapServers,
  ClientProperties,
  ClusterConnection,
  ClusterSecurity
}
import kui.kernel.{ClusterId, TopicName}
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger
import kui.topic.domain.{NewTopicSpec, TopicConfigChange}

/** What `KafkaTopicWriter` actually asks the broker for, rather than what it does with the answer.
  *
  * `KafkaTopicWriterSuite` beside this one asserts `writeError`, which is pure. The rest of the adapter is a
  * translation from the domain's vocabulary into an admin call, and that translation was reachable from no
  * suite at all: the module had no Kafka `Admin` stub when the class shipped, and `KafkaTopicWriterSuite`'s
  * own scaladoc records the consequence — a configuration `DELETE` turned into a `SET` "needs an admin client
  * this module has no stub for and is reported rather than pretended". `StubAdmin` and `RecordingAdminPool`
  * have since arrived for the read adapter, so it can be driven now.
  *
  * Both rules below are ones where the *wrong* call succeeds. The broker accepts it, the operation reports
  * success, and the cluster is left in a state the operator did not ask for — which is why neither could ever
  * have been caught by asserting on a `TopicError`.
  *
  * The arguments are captured into an `AtomicReference` rather than a `Ref`, because `StubAdmin`'s answers
  * are a plain synchronous partial function: the adapter calls it from inside its own `delay`, so there is no
  * effect to run there and nothing to interleave with.
  */
final class KafkaTopicWriteCallSuite extends KuiIOSuite {

  private val cluster: ClusterId = ClusterId.unsafe("local")
  private val orders: TopicName = TopicName.unsafe("orders.v1")

  private val connection: ClusterConnection = ClusterConnection(
    id = cluster,
    bootstrapServers = BootstrapServers.unsafe("broker-1:9092"),
    security = ClusterSecurity.Plaintext,
    overrides = ClientProperties.empty,
    admin = AdminTuning.default
  )

  private def writerOver(admin: Admin): IO[KafkaTopicWriter[IO]] =
    for {
      pool <- RecordingAdminPool(admin)
      logger <- FakeStructuredLogger[IO]
    } yield new KafkaTopicWriter[IO](pool, id => Option.when(id == cluster)(connection), logger)

  test("confirmed deletion sends the approved UUID, never the reusable topic name") {
    val id = org.apache.kafka.common.Uuid.fromString("AAAAAAAAAAAAAAAAAAAAAQ")
    val seen = new AtomicReference[List[org.apache.kafka.common.Uuid]](Nil)
    val admin = StubAdmin {
      case ("deleteTopics", (asked: org.apache.kafka.common.TopicCollection.TopicIdCollection) :: _) =>
        val _ = seen.set(asked.topicIds.asScala.toList)
        KuiTopicAdminResults.deleteTopicIds(Map(id -> KuiTopicAdminResults.completedVoid).asJava)
    }
    for {
      writer <- writerOver(admin)
      result <- writer.deleteById(cluster, orders, id.toString)
    } yield {
      assertEquals(result, Right(()))
      assertEquals(seen.get, List(id))
    }
  }

  // --------------------------------------------------------------------------------- alterConfig

  /** An admin that accepts every `incrementalAlterConfigs` and remembers what it was handed. */
  private def recordingAlterConfigs(seen: AtomicReference[List[(String, AlterConfigOp)]]): Admin =
    StubAdmin { case ("incrementalAlterConfigs", (asked: java.util.Map[?, ?]) :: _) =>
      val operations = asked.asScala.toList.flatMap {
        case (resource: ConfigResource, ops: java.util.Collection[?]) =>
          ops.asScala.toList.collect { case op: AlterConfigOp => (resource.name, op) }
        case _ => Nil
      }

      val _ = seen.updateAndGet(_ ++ operations)

      KuiTopicAdminResults.alterConfigs(
        asked.keySet.asScala.toList
          .collect { case resource: ConfigResource => resource -> KuiTopicAdminResults.completedVoid }
          .toMap
          .asJava
      )
    }

  test("a removed configuration entry is deleted, never set to what the default happens to be today") {
    /*
     * Ungated until now: `AlterConfigOp.OpType.DELETE` -> `SET` in `alterConfig` left
     * `./mill services.topic.__.test` green, because nothing anywhere drove the adapter with an `Admin`.
     *
     * The two are not the same operation and the difference only shows up later. `DELETE` removes the
     * topic's override, so the key follows the broker's default from then on -- which is what "not
     * overridden" means and what the operator asked for by clearing the field. `SET` with today's default
     * value pins the topic to that number for good: an operator who raises the broker's `retention.ms`
     * next year finds this one topic did not move, and nothing on any screen says why.
     */
    val seen = new AtomicReference[List[(String, AlterConfigOp)]](Nil)

    for {
      writer <- writerOver(recordingAlterConfigs(seen))
      change = TopicConfigChange
        .of(set = Map("retention.ms" -> "604800000"), remove = Set("segment.bytes"))
        .getOrElse(fail("the change under test is not a legal configuration change"))
      result <- writer.alterConfig(cluster, orders, change)
    } yield {
      assertEquals(result, Right(()))

      val operations = seen.get
      val byKey = operations.map((_, op) => op.configEntry.name -> op.opType).toMap

      assertEquals(byKey.get("retention.ms").map(_.name), Some("SET"))
      assertEquals(
        byKey.get("segment.bytes").map(_.name),
        Some("DELETE"),
        clue = "a removed key was written as SET, which pins the topic to today's broker default for good"
      )
      // And it addressed the topic rather than a broker: the same call shape can do both.
      assertEquals(operations.map(_._1).distinct, List(orders.value))
    }
  }

  // -------------------------------------------------------------------------------------- create

  /** An admin that accepts every `createTopics` and remembers the `NewTopic`s it was handed. */
  private def recordingCreateTopics(seen: AtomicReference[List[NewTopic]]): Admin =
    StubAdmin { case ("createTopics", (asked: java.util.Collection[?]) :: _) =>
      val topics = asked.asScala.toList.collect { case topic: NewTopic => topic }

      val _ = seen.updateAndGet(_ ++ topics)

      KuiTopicAdminResults.createTopics(
        topics
          .map(topic => topic.name -> KafkaFuture.completedFuture(KuiTopicAdminResults.created(1, 1)))
          .toMap
          .asJava: java.util.Map[String, KafkaFuture[CreateTopicsResult.TopicMetadataAndConfig]]
      )
    }

  test("a create that names no partition count lets the broker apply its own") {
    /*
     * Ungated until now: replacing `Optional.empty()` with a `1` KUI invented left
     * `./mill services.topic.__.test` green for the same reason — the `NewTopic` the adapter builds
     * reached no assertion anywhere.
     *
     * The class comment states the rule and `NewTopicSpec` repeats it: an absent count is what makes the
     * broker apply `num.partitions` and `default.replication.factor`. A one-partition default invented
     * here is the difference between a topic sized for the cluster it is on and one that cannot be
     * consumed in parallel at all — created silently, from a form the operator left blank.
     */
    val seen = new AtomicReference[List[NewTopic]](Nil)

    for {
      writer <- writerOver(recordingCreateTopics(seen))
      spec = NewTopicSpec
        .of(orders, partitions = None, replicationFactor = None, config = Map.empty)
        .getOrElse(fail("the spec under test is not a legal topic specification"))
      result <- writer.create(cluster, spec)
    } yield {
      assertEquals(result, Right(()))

      val created = seen.get
      assertEquals(created.map(_.name), List(orders.value))
      // Kafka reports an unset `Optional` as -1 from both accessors, which is the wire's way of saying
      // "the broker decides".
      assertEquals(created.head.numPartitions, -1, clue = "KUI invented a partition count")
      assertEquals(created.head.replicationFactor.toInt, -1, clue = "KUI invented a replication factor")
    }
  }

  test("a create that names both sends both, so the case above cannot pass by ignoring the spec") {
    val seen = new AtomicReference[List[NewTopic]](Nil)

    for {
      writer <- writerOver(recordingCreateTopics(seen))
      spec = NewTopicSpec
        .of(orders, partitions = Some(6), replicationFactor = Some(3), config = Map("retention.ms" -> "1"))
        .getOrElse(fail("the spec under test is not a legal topic specification"))
      _ <- writer.create(cluster, spec)
    } yield {
      val created = seen.get
      assertEquals(created.head.numPartitions, 6)
      assertEquals(created.head.replicationFactor.toInt, 3)
      assertEquals(created.head.configs.asScala.toMap, Map("retention.ms" -> "1"))
    }
  }
}
