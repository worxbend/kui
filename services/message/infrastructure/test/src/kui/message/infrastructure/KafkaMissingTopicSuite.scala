package kui.message.infrastructure

import java.util.Properties

import scala.concurrent.duration.{Duration, DurationInt}
import scala.jdk.CollectionConverters.*

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import fs2.io.file.Files
import org.apache.kafka.clients.admin.{Admin, NewTopic}

import kui.kafka.{AdminClientPool, AdminMetrics}
import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.kernel.{ClusterId, TopicName}
import kui.observability.Telemetry
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger
import kui.testkit.kafka.{ClusterConfigs, KafkaFixture, KafkaTopology}

/** A topic that is not there is `KUI-TOPIC-NOT-FOUND`, not a minute inside the producer.
  *
  * `KafkaProducer.partitionsFor` treats `UNKNOWN_TOPIC_OR_PARTITION` as retriable and blocks until
  * `max.block.ms` (60s by default), then surfaces a timeout. Auto-create is off in this fixture: the stock
  * one would create the topic on that metadata request and the missing-topic branch would never run.
  */
final class KafkaMissingTopicSuite extends KuiIOSuite {

  override val munitIOTimeout: Duration = 3.minutes

  private val cluster: ClusterId = ClusterId.unsafe("live")
  private val missing: TopicName = TopicName.unsafe("does-not-exist")

  private val broker =
    ResourceSuiteLocalFixture(
      "kafka-no-autocreate",
      Resource
        .eval(IO(requireDocker())) >> KafkaFixture[IO](KafkaTopology.Plaintext, autoCreateTopics = false)
    )

  override def munitFixtures = List(broker)

  private def requireDocker(): Unit =
    assume(
      KafkaFixture.dockerAvailable,
      "Docker is not available, so a missing topic was not probed against a broker and this case is UNVERIFIED"
    )

  test("a topic that exists reports its partition count") {
    val name = TopicName.unsafe(broker().uniqueName("orders"))
    for {
      _ <- create(name)
      result <- count(name)
    } yield assertEquals(result, Right(1))
  }

  test("a missing topic is not found within a few seconds and the error names it") {
    for {
      before <- IO.monotonic
      result <- count(missing).timeoutTo(8.seconds, IO.pure(slow))
      after <- IO.monotonic
    } yield {
      val took = after - before
      assert(took < 5.seconds, s"partitionCount took $took and returned $result")
      assertEquals(result, Left(ApplicationError.NotFound("topic", missing.value, ErrorCode.TopicNotFound)))
    }
  }

  private def slow: Either[KuiError, Int] =
    Left(kui.kernel.error.InfrastructureError.Timeout("partitionCount", 8_000L))

  private def count(topic: TopicName): IO[Either[KuiError, Int]] = {
    given Files[IO] = Files.forAsync[IO]
    (for {
      logger <- Resource.eval(FakeStructuredLogger[IO])
      metrics <- Resource.eval(AdminMetrics.otel[IO](Telemetry.noop[IO]))
      pool <- AdminClientPool.resource[IO](metrics, Some(logger))
    } yield (logger, pool)).use { (logger, pool) =>
      val connection = ClusterConfigs.forBroker(broker(), cluster).connection
      KafkaRecordProducer
        .resource[IO](id => Option.when(id == cluster)(connection), pool, logger)
        .forCluster(cluster)
        .use {
          case Left(error) => IO(fail(s"the producer did not open: ${error.message}"))
          case Right(producer) => producer.partitionCount(topic)
        }
    }
  }

  private def create(topic: TopicName): IO[Unit] =
    IO.blocking {
      val properties = new Properties()
      broker().clientProperties.foreach((key, value) => properties.put(key, value))
      val admin = Admin.create(properties)
      try {
        val _ = admin.createTopics(List(new NewTopic(topic.value, 1, 1.toShort)).asJava).all().get()
      } finally admin.close()
    }
}
