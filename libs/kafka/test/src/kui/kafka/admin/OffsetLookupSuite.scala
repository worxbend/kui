package kui.kafka.admin

import scala.jdk.CollectionConverters.*

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import org.apache.kafka.clients.admin.{Admin, DescribeTopicsResult, ListOffsetsResult, TopicDescription}
import org.apache.kafka.common.errors.{TopicAuthorizationException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.{KafkaFuture, Node, TopicPartition as KafkaTopicPartition, TopicPartitionInfo}

import kui.kafka.SkipReason
import kui.kernel.cluster.{
  AdminTuning,
  BootstrapServers,
  ClientProperties,
  ClusterConnection,
  ClusterSecurity
}
import kui.kernel.error.ErrorCode
import kui.kernel.{ClusterId, Offset, PartitionId, TopicName, TopicPartition}
import kui.testkit.KuiIOSuite

/** The sixty-second timeout, and the code that stops it happening.
  *
  * A `listOffsets` request naming a partition with no leader does not fail: the client retries metadata
  * silently until `default.api.timeout.ms` expires. So the assertions here are as much about the call that is
  * *not* made as about the answers that come back.
  */
final class OffsetLookupSuite extends KuiIOSuite {

  private val connection: ClusterConnection = ClusterConnection(
    id = ClusterId.unsafe("prod"),
    bootstrapServers = BootstrapServers.unsafe("broker:9092"),
    security = ClusterSecurity.Plaintext,
    overrides = ClientProperties.empty,
    admin = AdminTuning.default
  )

  private val leader = new Node(1, "broker", 9092)

  private def partition(n: Int): TopicPartition =
    TopicPartition(TopicName.unsafe("orders"), PartitionId.unsafe(n))

  private def kafkaPartition(n: Int): KafkaTopicPartition = new KafkaTopicPartition("orders", n)

  /** Kafka's own "there is no leader" value on the metadata path.
    *
    * `TopicPartitionInfo.leader` is documented as null when there is no leader, and `KafkaAdminClient` builds
    * these from `MetadataResponse`, which carries a leader id of `-1` and turns it into `Node.noNode()`
    * rather than into a null. Both shapes therefore reach KUI, and the second is the one a real broker sends.
    */
  private def noNodeLeader(n: Int): TopicPartitionInfo =
    new TopicPartitionInfo(n, Node.noNode(), List(leader).asJava, List(leader).asJava)

  private def info(n: Int, hasLeader: Boolean): TopicPartitionInfo =
    new TopicPartitionInfo(
      n,
      if hasLeader then leader else null,
      List(leader).asJava,
      List(leader).asJava
    )

  private def describeTopics(partitions: List[TopicPartitionInfo]): DescribeTopicsResult = {
    val description = new TopicDescription("orders", false, partitions.asJava)
    // `ofTopicNames` is package-private: the client builds these results and applications read them.
    val factory = classOf[DescribeTopicsResult].getDeclaredMethods
      .find(m => m.getName == "ofTopicNames")
      .getOrElse(fail("kafka-clients no longer offers DescribeTopicsResult.ofTopicNames"))
    factory.setAccessible(true)
    factory
      .invoke(null, Map("orders" -> KafkaFuture.completedFuture(description)).asJava)
      .asInstanceOf[DescribeTopicsResult]
  }

  private def listOffsets(offsets: Map[KafkaTopicPartition, Long]): ListOffsetsResult = {
    val infos = offsets.map { (tp, offset) =>
      tp -> new ListOffsetsResult.ListOffsetsResultInfo(offset, -1L, java.util.Optional.empty())
    }
    val constructor = classOf[ListOffsetsResult].getDeclaredConstructors.head
    constructor.setAccessible(true)
    constructor
      .newInstance(infos.map((tp, resultInfo) => tp -> KafkaFuture.completedFuture(resultInfo)).asJava)
      .asInstanceOf[ListOffsetsResult]
  }

  /** Records whether `listOffsets` was called at all, which is the assertion for a fully offline topic. */
  private def lookupWith(
      partitions: List[TopicPartitionInfo],
      offsets: Map[KafkaTopicPartition, Long]
  ): IO[(OffsetLookup[IO], Ref[IO, Int])] =
    Ref.of[IO, Int](0).map { calls =>
      val admin: Admin = StubAdmin {
        case "describeTopics" => describeTopics(partitions)
        case "listOffsets" =>
          calls.update(_ + 1).unsafeRunSyncHere()
          listOffsets(offsets)
      }

      (OffsetLookup.make[IO](StubAdmin.pool(admin)), calls)
    }

  extension (io: IO[Unit]) {
    private def unsafeRunSyncHere(): Unit = {
      import cats.effect.unsafe.implicits.global
      io.unsafeRunSync()
    }
  }

  test("a leaderless partition is filtered out before the request and returned as a skip") {
    for {
      pair <- lookupWith(
        List(info(0, hasLeader = true), info(1, hasLeader = false)),
        Map(kafkaPartition(0) -> 100L)
      )
      (lookup, calls) = pair
      answer <- lookup.endOffsets(connection, Set(partition(0), partition(1)))
      made <- calls.get
    } yield {
      assertEquals(answer.map(_.values), Right(Map(partition(0) -> Offset.unsafe(100L))))
      assertEquals(answer.map(_.skipped), Right(Map(partition(1) -> SkipReason.NoLeader)))
      assertEquals(made, 1)
    }
  }

  test("a request whose every partition is leaderless makes no Kafka call at all") {
    for {
      pair <- lookupWith(List(info(0, hasLeader = false)), Map.empty)
      (lookup, calls) = pair
      answer <- lookup.endOffsets(connection, Set(partition(0)))
      made <- calls.get
    } yield {
      assertEquals(answer.map(_.values), Right(Map.empty[TopicPartition, Offset]))
      assertEquals(answer.map(_.skipped), Right(Map(partition(0) -> SkipReason.NoLeader)))
      assertEquals(
        made,
        0,
        clue = "the one-minute timeout was paid for a question metadata had already answered"
      )
    }
  }

  test("no record at or after the timestamp is None, not the end offset") {
    for {
      pair <- lookupWith(List(info(0, hasLeader = true)), Map(kafkaPartition(0) -> -1L))
      (lookup, _) = pair
      answer <- lookup.offsetsForTimes(connection, Map(partition(0) -> 1700000000000L))
    } yield assertEquals(answer.map(_.values), Right(Map(partition(0) -> None)))
  }

  test("asking for nothing makes no call") {
    val exploding = OffsetLookup.make[IO](StubAdmin.pool(StubAdmin(PartialFunction.empty)))
    exploding
      .endOffsets(connection, Set.empty)
      .map(answer => assertEquals(answer.map(_.isEmpty), Right(true)))
  }

  test("a partition the broker answered without an offset is a skip, never a zero") {
    // `required`'s own docstring: "a skip with a stated reason, never a substituted zero. A zero here
    // would read on screen as 'this partition is empty', which is a different and quite specific lie."
    // Nothing asserted it: substituting `Offset.unsafe(0)` for the missing partition left all 279 cases
    // in this module, `libs/kafka-auth` and `libs/serde-confluent` green. Both partitions have leaders
    // here, so the filter is not what removes the second one — the broker simply did not answer for it.
    for {
      pair <- lookupWith(
        List(info(0, hasLeader = true), info(1, hasLeader = true)),
        Map(kafkaPartition(0) -> 100L)
      )
      (lookup, _) = pair
      answer <- lookup.endOffsets(connection, Set(partition(0), partition(1)))
    } yield {
      assertEquals(answer.map(_.values), Right(Map(partition(0) -> Offset.unsafe(100L))))
      assertEquals(
        answer.map(_.values.get(partition(1))),
        Right(None),
        clue = "a partition with no answer was given a number instead of a reason"
      )
      assertEquals(answer.map(_.skipped.keySet), Right(Set(partition(1))))
      assertEquals(
        answer.map(_.skipped.get(partition(1)).map {
          case SkipReason.Failed(code, _) => code
          case other => fail(s"expected a stated failure, got $other")
        }),
        Right(Some(ErrorCode.UpstreamUnavailable))
      )
    }
  }

  test("a leader Kafka reports as the no-node is no leader, and no request is sent for it") {
    // The branch a real broker takes. `Option(info.leader).isEmpty` alone is true only for the null
    // shape; dropping the `info.leader.id < 0` half of the test left every case in this module green
    // while restoring the sixty-second timeout this component exists to prevent.
    for {
      pair <- lookupWith(List(noNodeLeader(0)), Map.empty)
      (lookup, calls) = pair
      offline <- lookup.leaderless(connection, Set(partition(0)))
      answer <- lookup.endOffsets(connection, Set(partition(0)))
      made <- calls.get
    } yield {
      assertEquals(offline, Right(Set(partition(0))))
      assertEquals(answer.map(_.skipped), Right(Map(partition(0) -> SkipReason.NoLeader)))
      assertEquals(made, 0, clue = "listOffsets was sent for a partition whose leader is node -1")
    }
  }

  test("leaderless names the offline partitions, so a reset can refuse before it plans") {
    for {
      pair <- lookupWith(List(info(0, hasLeader = true), info(1, hasLeader = false)), Map.empty)
      (lookup, _) = pair
      offline <- lookup.leaderless(connection, Set(partition(0), partition(1)))
    } yield assertEquals(offline, Right(Set(partition(1))))
  }

  // ------------------------------------------------------------------ one topic, one partition

  private def topicPartition(topic: String, n: Int): TopicPartition =
    TopicPartition(TopicName.unsafe(topic), PartitionId.unsafe(n))

  private def failed[A](failure: Throwable): KafkaFuture[A] = {
    val future = new KafkaFutureImpl[A]()
    val _ = future.completeExceptionally(failure)
    future
  }

  /** One future per topic, which is what a broker's per-topic error code becomes in the client. */
  private def describeEach(answers: Map[String, KafkaFuture[TopicDescription]]): DescribeTopicsResult = {
    val factory = classOf[DescribeTopicsResult].getDeclaredMethods
      .find(m => m.getName == "ofTopicNames")
      .getOrElse(fail("kafka-clients no longer offers DescribeTopicsResult.ofTopicNames"))
    factory.setAccessible(true)
    factory.invoke(null, answers.asJava).asInstanceOf[DescribeTopicsResult]
  }

  private def described(topic: String, partitions: Int*): KafkaFuture[TopicDescription] =
    KafkaFuture.completedFuture(
      new TopicDescription(topic, false, partitions.map(info(_, hasLeader = true)).toList.asJava)
    )

  private def listEach(
      answers: Map[KafkaTopicPartition, KafkaFuture[ListOffsetsResult.ListOffsetsResultInfo]]
  ): ListOffsetsResult = {
    val constructor = classOf[ListOffsetsResult].getDeclaredConstructors.head
    constructor.setAccessible(true)
    constructor.newInstance(answers.asJava).asInstanceOf[ListOffsetsResult]
  }

  private def answered(offset: Long): KafkaFuture[ListOffsetsResult.ListOffsetsResultInfo] =
    KafkaFuture.completedFuture(
      new ListOffsetsResult.ListOffsetsResultInfo(offset, -1L, java.util.Optional.empty())
    )

  /** The consumer refresh pass asks about every group's partitions in one call, so the topic that fails here
    * is not the only one in the request — and before this was per topic, it was the only one that answered.
    */
  private def withUndescribable(failure: Throwable): OffsetLookup[IO] =
    OffsetLookup.make[IO](
      StubAdmin.pool(StubAdmin {
        case "describeTopics" =>
          describeEach(Map("payments" -> described("payments", 0), "retired" -> failed(failure)))
        case "listOffsets" =>
          listEach(Map(new KafkaTopicPartition("payments", 0) -> answered(100L)))
      })
    )

  test("a topic KUI is not told about costs only its own partitions, not every other topic's offsets") {
    // `allTopicNames()` is `KafkaFuture.allOf`: one deleted topic still named by a member's assignment
    // failed the whole lookup, and the refresh pass then showed every group on the cluster as NO_LEADER.
    val lookup = withUndescribable(new UnknownTopicOrPartitionException("retired"))

    lookup.endOffsets(connection, Set(topicPartition("payments", 0), topicPartition("retired", 0))).map {
      answer =>
        assertEquals(answer.map(_.values), Right(Map(topicPartition("payments", 0) -> Offset.unsafe(100L))))
        assertEquals(
          answer.map(_.skipped.get(topicPartition("retired", 0)).map {
            case SkipReason.NotFound(_) => "not-found"
            case other => s"unexpected $other"
          }),
          Right(Some("not-found")),
          clue = "a topic that is not there was reported as something other than not found"
        )
    }
  }

  test("a topic KUI may not describe costs only its own partitions, and says it is not authorized") {
    val lookup = withUndescribable(new TopicAuthorizationException(java.util.Set.of("retired")))

    lookup
      .beginningOffsets(connection, Set(topicPartition("payments", 0), topicPartition("retired", 0)))
      .map { answer =>
        assertEquals(answer.map(_.values), Right(Map(topicPartition("payments", 0) -> Offset.unsafe(100L))))
        assertEquals(
          answer.map(_.skipped.get(topicPartition("retired", 0)).map {
            case SkipReason.NotAuthorized(_) => "not-authorized"
            case other => s"unexpected $other"
          }),
          Right(Some("not-authorized")),
          clue = "an ACL KUI lacks was reported as something other than not authorized"
        )
      }
  }

  test("a partition whose own offset lookup failed is a skip; the rest of its chunk keeps its offsets") {
    // The other half of the all-or-nothing: `ListOffsetsResult.all()` is `allOf` too, and a topic
    // deleted between the metadata read and the offset request failed the chunk it shared.
    val lookup = OffsetLookup.make[IO](
      StubAdmin.pool(StubAdmin {
        case "describeTopics" => describeTopics(List(info(0, hasLeader = true), info(1, hasLeader = true)))
        case "listOffsets" =>
          listEach(
            Map(
              kafkaPartition(0) -> answered(100L),
              kafkaPartition(1) -> failed(new UnknownTopicOrPartitionException("orders-1"))
            )
          )
      })
    )

    lookup.endOffsets(connection, Set(partition(0), partition(1))).map { answer =>
      assertEquals(answer.map(_.values), Right(Map(partition(0) -> Offset.unsafe(100L))))
      assertEquals(answer.map(_.skipped.keySet), Right(Set(partition(1))))
    }
  }

  test("a timeout is still the lookup's failure, never a quietly smaller answer") {
    // `KafkaErrorMapper.suppressible`: a partial result caused by slowness is indistinguishable from a
    // cluster that has less data, so only a true statement about the one topic becomes a skip.
    val lookup = withUndescribable(new org.apache.kafka.common.errors.TimeoutException("slow"))

    lookup
      .endOffsets(connection, Set(topicPartition("payments", 0), topicPartition("retired", 0)))
      .map(answer => assertEquals(answer.map(_ => ()).leftMap(_.code), Left(ErrorCode.Timeout)))
  }

  test("the reset's leader check still refuses a topic it cannot describe, rather than calling it online") {
    withUndescribable(new UnknownTopicOrPartitionException("retired"))
      .leaderless(connection, Set(topicPartition("payments", 0), topicPartition("retired", 0)))
      .map(answer =>
        assert(answer.isLeft, s"a reset would have been planned over a topic KUI cannot see: $answer")
      )
  }
}
