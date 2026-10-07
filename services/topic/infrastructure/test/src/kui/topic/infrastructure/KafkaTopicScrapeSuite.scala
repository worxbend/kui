package kui.topic.infrastructure

import scala.jdk.CollectionConverters.*

import cats.effect.IO
import org.apache.kafka.clients.admin.{
  Admin,
  Config,
  ConfigEntry,
  KuiTopicAdminResults,
  ListOffsetsResult,
  LogDirDescription,
  ReplicaInfo,
  TopicDescription,
  TopicListing
}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.{KafkaFuture, Node, TopicCollection, TopicPartition, Uuid}

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

/** The batched cleanup-policy read, driven through the adapter rather than around it.
  *
  * `KafkaTopicAdminSuite` beside this one asserts the pure conversions, and W2-08's acceptance case for "a
  * `describeConfigs` failure costs the column, not the page" was met there by a hand-built snapshot whose
  * `policy` was already `None` — an arrangement that satisfies the rule by construction and could never have
  * failed. The three claims `KafkaTopicAdmin.cleanupPolicies`'s twelve-line scaladoc makes are about what
  * happens when a *call* fails, and this is where a call fails.
  */
final class KafkaTopicScrapeSuite extends KuiIOSuite {

  private val cluster = ClusterId.unsafe("local")

  private val connection = ClusterConnection(
    id = cluster,
    bootstrapServers = BootstrapServers.unsafe("broker-1:9092"),
    security = ClusterSecurity.Plaintext,
    overrides = ClientProperties.empty,
    admin = AdminTuning.default
  )

  private val broker = new Node(1, "broker-1.example", 9092)

  private def description(name: String, id: Uuid): TopicDescription =
    new TopicDescription(
      name,
      false,
      List(
        new org.apache.kafka.common.TopicPartitionInfo(
          0,
          broker,
          List(broker).asJava,
          List(broker).asJava
        )
      ).asJava,
      java.util.Set.of[org.apache.kafka.common.acl.AclOperation](),
      id
    )

  private def policyConfig(value: String): Config =
    new Config(
      List(
        new ConfigEntry(
          "cleanup.policy",
          value,
          ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG,
          false,
          false,
          List.empty[ConfigEntry.ConfigSynonym].asJava,
          ConfigEntry.ConfigType.STRING,
          null
        )
      ).asJava
    )

  /** A cluster of `names`, every topic describable, whose configuration answers as `policy` says.
    *
    * `policy` returning `Left` is Kafka's per-resource failure — `describeConfigs` hands back one future per
    * resource — and `batchFails` is the other one: the whole call refused, which is what an admin client that
    * lost its connection mid-scrape does.
    */
  private def cluster(
      names: List[String],
      policy: String => Either[Throwable, String],
      batchFails: Boolean = false,
      topicId: Uuid = Uuid.ZERO_UUID
  ): Admin =
    StubAdmin {
      case ("listTopics", _) =>
        KuiTopicAdminResults.listTopics(
          names.map(name => name -> new TopicListing(name, Uuid.randomUuid(), false)).toMap.asJava
        )

      case ("describeTopics", (asked: TopicCollection.TopicNameCollection) :: _) =>
        KuiTopicAdminResults.describeTopics(
          asked.topicNames.asScala.toList
            .map(name => name -> KafkaFuture.completedFuture(description(name, topicId)))
            .toMap
            .asJava
        )

      case ("listOffsets", (asked: java.util.Map[?, ?]) :: _) =>
        new ListOffsetsResult(
          asked.keySet.asScala.toList
            .collect { case partition: TopicPartition => partition }
            .map(partition =>
              partition -> KafkaFuture.completedFuture(
                new ListOffsetsResult.ListOffsetsResultInfo(0L, 0L, java.util.Optional.empty[Integer])
              )
            )
            .toMap
            .asJava
        )

      case ("describeLogDirs", (asked: java.util.Collection[?]) :: _) =>
        KuiTopicAdminResults.describeLogDirs(
          asked.asScala.toList
            .collect { case id: Integer => id }
            .map(id =>
              id -> KafkaFuture.completedFuture(
                Map(
                  "/var/lib/kafka/data" -> new LogDirDescription(
                    null,
                    Map.empty[TopicPartition, ReplicaInfo].asJava,
                    1000L,
                    900L
                  )
                ).asJava
              )
            )
            .toMap
            .asJava
        )

      case ("describeConfigs", (asked: java.util.Collection[?]) :: _) =>
        if batchFails then throw new TopicAuthorizationException("this cluster refuses describeConfigs")
        else
          KuiTopicAdminResults.describeConfigs(
            asked.asScala.toList
              .collect { case resource: ConfigResource => resource }
              .map(resource =>
                resource -> (policy(resource.name) match {
                  case Left(refusal) => KuiTopicAdminResults.failed[Config](refusal)
                  case Right(value) => KafkaFuture.completedFuture(policyConfig(value))
                })
              )
              .toMap
              .asJava
          )
    }

  private def adminOver(client: Admin): IO[(KafkaTopicAdmin[IO], RecordingAdminPool)] =
    for {
      pool <- RecordingAdminPool(client)
      logger <- FakeStructuredLogger[IO]
    } yield (new KafkaTopicAdmin[IO](pool, _ => Some(connection), "__", logger), pool)

  test("topic detail retains Kafka identity but never treats the zero UUID as known") {
    val id = Uuid.fromString("AAAAAAAAAAAAAAAAAAAAAQ")
    for {
      (known, _) <- adminOver(cluster(List("orders"), _ => Right("delete"), topicId = id))
      detail <- known.detail(cluster, TopicName.unsafe("orders"))
      (unknown, _) <- adminOver(cluster(List("orders"), _ => Right("delete")))
      absent <- unknown.detail(cluster, TopicName.unsafe("orders"))
    } yield {
      assertEquals(detail.map(_.topicId), Right(Some(id.toString)))
      assertEquals(absent.map(_.topicId), Right(None))
    }
  }

  test("aDescribeConfigsThatFailedForOneTopicLeavesThatRowsPolicyAbsentAndFillsTheOthers") {
    // `delete` is Kafka's own default, so the failure mode this guards is not a blank cell: it is a row
    // confidently labelled `delete` for a topic that might be `compact`.
    val client = cluster(
      List("orders", "audit"),
      {
        case "audit" => Left(new TopicAuthorizationException("audit"))
        case _ => Right("compact")
      }
    )

    for {
      (admin, _) <- adminOver(client)
      scraped <- admin.scrape(cluster)
    } yield {
      val rows = scraped.toOption.map(_.topics.map(row => row.name.value -> row.cleanupPolicy).toMap)

      assertEquals(rows, Some(Map("orders" -> Some("compact"), "audit" -> None)))
    }
  }

  test("aWholeBatchFailureLeavesEveryPolicyAbsentAndTheScrapeStillAnswers") {
    // The `handleErrorWith` around the batch. Without it the scrape fails, and a topics screen that went
    // blank because one cosmetic column could not be read would be a worse answer than the em dash.
    val client = cluster(List("orders", "audit"), _ => Right("compact"), batchFails = true)

    for {
      (admin, _) <- adminOver(client)
      scraped <- admin.scrape(cluster)
    } yield {
      val result = scraped.toOption.getOrElse(fail(s"the scrape failed over a cleanup policy: $scraped"))

      assertEquals(result.topics.map(_.name), List(TopicName.unsafe("audit"), TopicName.unsafe("orders")))
      assertEquals(result.topics.flatMap(_.cleanupPolicy), Nil)
      // The rows are rows, not a partial page: an unreadable configuration is not an unreadable topic.
      assertEquals(result.incomplete, Map.empty[TopicName, String])
    }
  }

  test("aTopicKuiMaySeeAndMayNotDescribeCostsItsSettingsTabAndNotTheWholePage") {
    // The one place in this adapter where a typed failure is deliberately turned back into a value. A 403
    // here would take the topic page down and the partitions the user *is* entitled to see would vanish
    // with the tab they are not; the port's own scaladoc requires the conversion. Removing it left
    // `./mill libs.__.test + services.*` at 2633/2633.
    val client = cluster(List("orders"), _ => Right("compact"), batchFails = true)

    for {
      (admin, _) <- adminOver(client)
      view <- admin.config(cluster, TopicName.unsafe("orders"))
    } yield view match {
      case Right(kui.topic.domain.TopicConfigView.NotPermitted(detail)) =>
        assert(detail.nonEmpty, "a refusal the operator is shown must say something")
      case other =>
        fail(s"a refused describeConfigs must be a NotPermitted view rather than an error: $other")
    }
  }

  test("aTopicWhoseSettingsCanBeReadStillAnswersWithThem") {
    // The positive half, so the case above is not satisfied by an adapter that always answers NotPermitted.
    val client = cluster(List("orders"), _ => Right("compact"))

    for {
      (admin, _) <- adminOver(client)
      view <- admin.config(cluster, TopicName.unsafe("orders"))
    } yield view match {
      case Right(kui.topic.domain.TopicConfigView.Entries(entries)) =>
        assertEquals(entries.map(_.name), List("cleanup.policy"))
      case other => fail(s"a readable configuration must be settings: $other")
    }
  }

  test("tenThousandTopicsCostFiftyConfigCallsAndNotTenThousand") {
    // The number the adapter's own scaladoc publishes. It was arithmetic nobody had run: a `grouped` that
    // lost its argument, or a fold that asked per row, returns exactly the same policies.
    val names = (1 to 10000).toList.map(index => f"topic-$index%05d")
    val client = cluster(names, _ => Right("delete"))

    for {
      (admin, pool) <- adminOver(client)
      scraped <- admin.scrape(cluster)
      configCalls <- pool.runsOf("describeConfigs.batch")
    } yield {
      assertEquals(scraped.toOption.map(_.topics.size), Some(10000))
      assertEquals(configCalls, 50)
    }
  }
}
