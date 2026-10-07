package kui.topic.application

import java.time.Instant

import cats.effect.{IO, Ref}
import fs2.Stream

import kui.cache.{Snapshot, SnapshotCell}
import kui.kernel.error.{ErrorCode, KuiError}
import kui.kernel.{ClusterId, PartitionId, Secret, TopicName}
import kui.security.Principal
import kui.security.audit.{AuditSink, MutationKind, MutationOutcome, MutationRecord}
import kui.testkit.fakes.FakeStructuredLogger
import kui.topic.domain.*

/** What has to be true of topic administration before it is allowed near a cluster.
  *
  * The three things ADR-047 requires and the one ADR-045 does, asserted as behaviour rather than as
  * documentation: a read-only cluster is refused **without the writer being called at all**, every attempt
  * leaves exactly one audit record, and a destructive change can only be made against a token that names it.
  */
final class TopicAdministrationSuite extends munit.CatsEffectSuite {

  private val cluster: ClusterId = ClusterId.unsafe("local")
  private val orders: TopicName = TopicName.unsafe("orders.v1")
  private val at: Instant = Instant.parse("2026-09-04T09:00:00Z")

  /** A writer that records what it was asked to do, and does none of it.
    *
    * The recording is the assertion. "The cluster is refused before any Kafka client is touched" is not
    * observable from a return value — a refusal and a failed write look the same to the caller — so it is
    * asserted by this list being empty.
    */
  final class FakeWriter(
      val calls: Ref[IO, List[String]],
      answer: Either[TopicError, Unit],
      autoCreate: Option[Boolean]
  ) extends TopicWriter[IO] {

    private def record(what: String): IO[Either[TopicError, Unit]] =
      calls.update(_ :+ what).as(answer)

    def create(id: ClusterId, spec: NewTopicSpec): IO[Either[TopicError, Unit]] =
      record(s"create:${spec.name.value}")

    def alterConfig(
        id: ClusterId,
        topic: TopicName,
        change: TopicConfigChange
    ): IO[Either[TopicError, Unit]] = record(s"alterConfig:${topic.value}")

    def increasePartitions(id: ClusterId, topic: TopicName, target: Int): IO[Either[TopicError, Unit]] =
      record(s"increasePartitions:${topic.value}:$target")

    def delete(id: ClusterId, topic: TopicName): IO[Either[TopicError, Unit]] =
      record(s"delete:${topic.value}")

    def deleteById(id: ClusterId, topic: TopicName, topicId: String): IO[Either[TopicError, Unit]] =
      if topicId == "AAAAAAAAAAAAAAAAAAAAAQ" then record(s"delete:${topic.value}")
      else IO.pure(Left(TopicError.Rejected("unexpected topic identity")))

    def autoCreateEnabled(id: ClusterId): IO[Option[Boolean]] = IO.pure(autoCreate)
  }

  private def profiles(readOnly: Boolean): ClusterProfiles[IO] = new ClusterProfiles[IO] {
    def all: IO[List[ClusterRef]] = IO.pure(List(ClusterRef(cluster, "Local", readOnly)))
    def get(id: ClusterId): IO[Option[ClusterRef]] = all.map(_.find(_.id == id))
    def onChange(handler: Set[ClusterId] => IO[Unit]): IO[IO[Unit]] = IO.pure(IO.unit)
  }

  private def snapshots(refreshes: Ref[IO, List[ClusterId]]): TopicSnapshots[IO] = new TopicSnapshots[IO] {
    def of(id: ClusterId): IO[Option[SnapshotCell[IO, TopicSnapshot]]] =
      IO.pure(Some(new SnapshotCell[IO, TopicSnapshot] {
        private val snapshot = Snapshot.online(TopicSnapshot.empty(at), at)
        def get: IO[Snapshot[TopicSnapshot]] = IO.pure(snapshot)
        def refresh: IO[Snapshot[TopicSnapshot]] = IO.pure(snapshot)
        def invalidate: IO[Snapshot[TopicSnapshot]] = IO.pure(snapshot)
        def updates: Stream[IO, Snapshot[TopicSnapshot]] = Stream.emit(snapshot)
      }))

    def requestRefresh(id: ClusterId): IO[Boolean] = refreshes.update(_ :+ id).as(true)
  }

  /** The verified principal these mutations are made by: the anonymous one a deployment without
    * authentication produces.
    */
  private val Caller: Principal = Principal.Anonymous

  private def sink(records: Ref[IO, List[MutationRecord]]): AuditSink[IO] = new AuditSink[IO] {
    def record(entry: MutationRecord): IO[Unit] = records.update(_ :+ entry)
  }

  /** A topic with `count` partitions, each holding `perPartition` records. */
  private def topicOf(count: Int, perPartition: Long): TopicDetail =
    TopicDetail.of(
      orders,
      isInternal = false,
      topicId = Some("AAAAAAAAAAAAAAAAAAAAAQ"),
      partitions = (0 until count).toList.map(index =>
        PartitionView
          .from(
            partition = PartitionId.unsafe(index),
            leader = Some(kui.kernel.BrokerId.unsafe(1)),
            replicas = List(kui.kernel.BrokerId.unsafe(1)),
            inSync = List(kui.kernel.BrokerId.unsafe(1)),
            earliestOffset = Some(0L),
            latestOffset = Some(perPartition),
            sizeBytes = None
          )
          .getOrElse(fail("the fixture built a partition the domain refuses"))
      )
    )

  private case class Fixture(
      admin: TopicAdminUseCase[IO],
      writer: FakeWriter,
      records: Ref[IO, List[MutationRecord]],
      refreshes: Ref[IO, List[ClusterId]]
  )

  private def fixture(
      readOnly: Boolean = false,
      topics: List[TopicDetail] = List(topicOf(3, 5L)),
      writeAnswer: Either[TopicError, Unit] = Right(()),
      autoCreate: Option[Boolean] = Some(false)
  ): IO[Fixture] =
    for {
      logger <- FakeStructuredLogger[IO]
      reads <- FakeTopicAdmin.of(topics)
      calls <- Ref.of[IO, List[String]](Nil)
      records <- Ref.of[IO, List[MutationRecord]](Nil)
      refreshes <- Ref.of[IO, List[ClusterId]](Nil)
      writer = new FakeWriter(calls, writeAnswer, autoCreate)
      profileSource = profiles(readOnly)
      guard = MutationGuard.make[IO](profileSource, snapshots(refreshes), sink(records), logger, toKui)
      tokens = TopicPlanToken.make[IO](Secret("a key long enough for HMAC-SHA256".getBytes("UTF-8")))
    } yield Fixture(
      TopicAdminUseCase.make[IO](reads, writer, profileSource, guard, tokens, logger, toKui),
      writer,
      records,
      refreshes
    )

  /** The api module's mapping is not visible from here (rule A3), and none of these assertions depends on the
    * exact codes, only on the refusal reaching the caller. This is the smallest total function that satisfies
    * the parameter.
    */
  private def toKui(error: TopicError): KuiError =
    kui.kernel.error.ApplicationError.InvalidState(error.message)

  // ------------------------------------------------------------------- ADR-047: the read-only refusal

  test("aReadOnlyClusterIsRefusedBeforeTheWriterIsTouchedAtAll") {
    for {
      f <- fixture(readOnly = true)
      spec = NewTopicSpec.of(orders, Some(3), Some(1), Map.empty).getOrElse(fail("spec"))
      answer <- f.admin.create(Caller, cluster, spec)
      calls <- f.writer.calls.get
    } yield {
      assertEquals(answer.left.map(_.code), Left(ErrorCode.ReadOnly))
      // The whole point. Not "the write failed" — the write was never attempted.
      assertEquals(calls, Nil)
    }
  }

  test("aRefusedMutationIsStillAudited") {
    // An attempt to change a read-only cluster is exactly the kind of thing an audit trail exists to have
    // noticed.
    for {
      f <- fixture(readOnly = true)
      _ <- f.admin.alterConfig(
        Caller,
        cluster,
        orders,
        TopicConfigChange.of(Map("retention.ms" -> "1000"), Set.empty).getOrElse(fail("change"))
      )
      records <- f.records.get
    } yield {
      assertEquals(records.map(_.outcome), List(MutationOutcome.Refused))
      assertEquals(records.map(_.kind), List(MutationKind.AlterTopicConfig))
      assertEquals(records.map(_.resource), List(orders.value))
    }
  }

  test("aPlanIsRefusedOnAReadOnlyClusterToo") {
    // So that the screen never renders a plan the operator is not allowed to apply, and never teaches them
    // that the refusal at the end is a bug.
    for {
      f <- fixture(readOnly = true)
      partitions <- f.admin.planPartitions(cluster, orders, 6)
      deletion <- f.admin.planDelete(cluster, orders)
      calls <- f.writer.calls.get
    } yield {
      assertEquals(partitions.left.map(_.code), Left(ErrorCode.ReadOnly))
      assertEquals(deletion.left.map(_.code), Left(ErrorCode.ReadOnly))
      assertEquals(calls, Nil)
    }
  }

  test("aSuccessfulMutationIsAuditedOnceAndAsksForAReScrape") {
    // The re-scrape is what stops an operator being sent back to a topic list that will not contain the
    // topic they just made for up to a minute — which reads as a create that silently failed.
    for {
      f <- fixture()
      spec = NewTopicSpec
        .of(TopicName.unsafe("new.topic"), Some(3), Some(1), Map.empty)
        .getOrElse(fail("spec"))
      answer <- f.admin.create(Caller, cluster, spec)
      records <- f.records.get
      refreshes <- f.refreshes.get
    } yield {
      assert(answer.isRight, answer.toString)
      assertEquals(records.map(_.outcome), List(MutationOutcome.Succeeded))
      assertEquals(refreshes, List(cluster))
    }
  }

  test("theAuditRecordCarriesTheConfigurationKeysAndNeverTheirValues") {
    // ADR-023: an audit log is routinely more widely readable than the thing it describes, and a topic
    // configuration can carry a credential on the clusters that use one.
    for {
      f <- fixture()
      change = TopicConfigChange
        .of(Map("retention.ms" -> "604800000", "ssl.keystore.password" -> "hunter2"), Set("cleanup.policy"))
        .getOrElse(fail("change"))
      _ <- f.admin.alterConfig(Caller, cluster, orders, change)
      records <- f.records.get
    } yield {
      val detail = records.flatMap(_.detail.values).mkString(" ")
      assert(detail.contains("retention.ms"), detail)
      assert(detail.contains("cleanup.policy"), detail)
      assert(!detail.contains("hunter2"), detail)
    }
  }

  // ------------------------------------------------------------------------ ADR-045: the plan token

  test("aPartitionIncreaseCannotBeAppliedWithoutAPlan") {
    for {
      f <- fixture()
      answer <- f.admin.applyPartitions(Caller, cluster, orders, "not-a-token")
      calls <- f.writer.calls.get
    } yield {
      assertEquals(answer.left.map(_.code), Left(ErrorCode.Validation))
      assertEquals(calls, Nil)
    }
  }

  test("aPlanForOneTopicCannotBeAppliedToAnother") {
    // The token binds the cluster and the topic. Without that, a plan read for a staging topic could be
    // confirmed against the production topic of the same name in another tab.
    for {
      f <- fixture()
      planned <- f.admin.planPartitions(cluster, orders, 6)
      token = planned.getOrElse(fail("plan")).token
      elsewhere <- f.admin.applyPartitions(Caller, cluster, TopicName.unsafe("payments.v1"), token)
      calls <- f.writer.calls.get
    } yield {
      assert(elsewhere.isLeft, elsewhere.toString)
      assertEquals(calls, Nil)
    }
  }

  test("aDeletionPlanCannotBeUsedToGrowTheTopic") {
    // The operation is inside the signature, so the two flows cannot be crossed.
    for {
      f <- fixture()
      planned <- f.admin.planDelete(cluster, orders)
      token = planned.getOrElse(fail("plan")).token
      crossed <- f.admin.applyPartitions(Caller, cluster, orders, token)
      calls <- f.writer.calls.get
    } yield {
      assert(crossed.isLeft, crossed.toString)
      assertEquals(calls, Nil)
    }
  }

  test("aPartitionPlanIsReResolvedAgainstTheClusterBeforeItIsApplied") {
    // The count can move in the five minutes a token is valid for. A token for twelve must not be applied
    // to a topic somebody else has already grown to sixteen, and the re-plan is what refuses that.
    for {
      planning <- fixture(topics = List(topicOf(3, 5L)))
      planned <- planning.admin.planPartitions(cluster, orders, 6)
      token = planned.getOrElse(fail("plan")).token
      grown <- fixture(topics = List(topicOf(6, 5L)))
      answer <- grown.admin.applyPartitions(Caller, cluster, orders, token)
      calls <- grown.writer.calls.get
    } yield {
      assertEquals(answer.left.map(_.code), Left(ErrorCode.Validation))
      assertEquals(calls, Nil)
    }
  }

  test("appliedPartitionsWriteExactlyTheTargetThatWasPlanned") {
    for {
      f <- fixture()
      planned <- f.admin.planPartitions(cluster, orders, 12)
      answer <- f.admin.applyPartitions(Caller, cluster, orders, planned.getOrElse(fail("plan")).token)
      calls <- f.writer.calls.get
      records <- f.records.get
    } yield {
      assertEquals(answer.map(_.target), Right(12))
      assertEquals(calls, List("increasePartitions:orders.v1:12"))
      assertEquals(records.map(_.kind), List(MutationKind.IncreasePartitions))
    }
  }

  List(
    "new records" -> List(topicOf(3, 8L)),
    "new partitions" -> List(topicOf(4, 7L)),
    "missing topic" -> Nil
  ).foreach { (reason, changed) =>
    test(s"deletion rechecks signed state: $reason") {
      for {
        original <- fixture(topics = List(topicOf(3, 7L)))
        planned <- original.admin.planDelete(cluster, orders)
        current <- fixture(topics = changed)
        answer <- current.admin.applyDelete(Caller, cluster, orders, planned.getOrElse(fail("plan")).token)
        calls <- current.writer.calls.get
      } yield {
        assert(answer.isLeft)
        assertEquals(calls, Nil)
      }
    }
  }

  test("deletion rechecks auto-create state") {
    for {
      original <- fixture(autoCreate = Some(false))
      planned <- original.admin.planDelete(cluster, orders)
      current <- fixture(autoCreate = Some(true))
      answer <- current.admin.applyDelete(Caller, cluster, orders, planned.getOrElse(fail("plan")).token)
      calls <- current.writer.calls.get
    } yield {
      assert(answer.isLeft)
      assertEquals(calls, Nil)
    }
  }

  test("deletion refuses a recreated topic with identical name and offsets") {
    for {
      original <- fixture()
      planned <- original.admin.planDelete(cluster, orders)
      current <- fixture(topics = List(topicOf(3, 5L).copy(topicId = Some("AAAAAAAAAAAAAAAAAAAAAg"))))
      answer <- current.admin.applyDelete(Caller, cluster, orders, planned.getOrElse(fail("plan")).token)
      calls <- current.writer.calls.get
    } yield {
      assert(answer.isLeft)
      assertEquals(calls, Nil)
    }
  }

  test("deletion refuses unknown identity or offset bounds") {
    for {
      unknownId <- fixture(topics = List(topicOf(3, 5L).copy(topicId = None)))
      noIdentity <- unknownId.admin.planDelete(cluster, orders)
      detail = topicOf(3, 5L)
      degraded = detail.copy(partitions =
        detail.partitions.map(p =>
          PartitionView
            .from(
              p.partition,
              p.leader,
              List(kui.kernel.BrokerId.unsafe(1)),
              List(kui.kernel.BrokerId.unsafe(1)),
              None,
              None,
              None
            )
            .getOrElse(fail("partition"))
        )
      )
      unknownOffsets <- fixture(topics = List(degraded))
      noOffsets <- unknownOffsets.admin.planDelete(cluster, orders)
    } yield {
      assert(noIdentity.isLeft)
      assert(noOffsets.isLeft)
    }
  }

  test("deletion rejects retention movement even if record counts are unchanged") {
    val detail = topicOf(3, 5L)
    val moved = detail.copy(partitions =
      detail.partitions.map(p =>
        PartitionView
          .from(
            p.partition,
            p.leader,
            List(kui.kernel.BrokerId.unsafe(1)),
            List(kui.kernel.BrokerId.unsafe(1)),
            Some(5L),
            Some(10L),
            None
          )
          .getOrElse(fail("partition"))
      )
    )
    for {
      original <- fixture()
      planned <- original.admin.planDelete(cluster, orders)
      current <- fixture(topics = List(moved))
      answer <- current.admin.applyDelete(Caller, cluster, orders, planned.getOrElse(fail("plan")).token)
      calls <- current.writer.calls.get
    } yield {
      assert(answer.isLeft)
      assertEquals(calls, Nil)
    }
  }

  test("aDeletionPlanCountsTheRecordsAndTheReceiptRepeatsThem") {
    // The number the operator weighed the decision against is in the token, so the receipt cannot report a
    // different one — and the topic is gone by then, so it cannot be re-read.
    for {
      f <- fixture(topics = List(topicOf(3, 7L)), autoCreate = Some(true))
      planned <- f.admin.planDelete(cluster, orders)
      plan = planned.getOrElse(fail("plan")).plan
      answer <- f.admin.applyDelete(Caller, cluster, orders, planned.getOrElse(fail("plan")).token)
      calls <- f.writer.calls.get
    } yield {
      assertEquals(plan.records, Some(21L))
      assertEquals(plan.autoCreateEnabled, Some(true))
      assertEquals(answer.map(_.records), Right(Some(21L)))
      assert(
        answer.exists(_.warnings.map(_.code).contains(PlanWarning.AutoCreate)),
        "the receipt must repeat the warning that the topic can come straight back"
      )
      assertEquals(calls, List("delete:orders.v1"))
    }
  }

  test("aClusterKUIDoesNotHaveIsNotFoundAndIsRecorded") {
    for {
      f <- fixture()
      spec = NewTopicSpec.of(orders, None, None, Map.empty).getOrElse(fail("spec"))
      answer <- f.admin.create(Caller, ClusterId.unsafe("elsewhere"), spec)
      records <- f.records.get
      calls <- f.writer.calls.get
    } yield {
      assertEquals(answer.left.map(_.code), Left(ErrorCode.ClusterNotFound))
      assertEquals(records.map(_.outcome), List(MutationOutcome.Failed))
      assertEquals(calls, Nil)
    }
  }
}
