package kui.message.application.purge

import cats.effect.IO
import cats.effect.kernel.Ref

import kui.kernel.error.{ErrorCode, KuiError}
import kui.kernel.{ClusterId, Offset, PartitionId, Secret, TopicName}
import kui.message.application.produce.{MutationGuard, ProduceRig}
import kui.message.domain.ports.RecordDeleter
import kui.message.domain.{PlannedPurge, PurgeResult}
import kui.security.Principal
import kui.security.audit.{MutationOutcome, MutationRecord}
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** The two-phase purge, which is the operation ADR-045 was written for and had no suite of its own.
  *
  * Two mutations, applied one at a time against `./mill services.message.__.test`, each leaving it at 183/183
  * green:
  *
  *   - `Either.cond(planned.nonEmpty, …)` made unconditional, so a token planned against an already-empty
  *     topic is sent to Kafka — "a call that can still fail and can never help", in the code's own words;
  *   - the audit detail's `sortBy(_.partition.value)` reversed, so two records of one purge render
  *     differently and a diff between them means nothing.
  *
  * The read-only refusal in `plan` could not be mutated cleanly — removing it leaves `writable` unused and
  * `-Werror` rejects the compile — so it is asserted here rather than measured.
  */
final class PurgeUseCaseSuite extends KuiIOSuite {

  private val Caller: Principal = Principal.Anonymous

  private def planned(partition: Int, low: Long, high: Long): PlannedPurge =
    PlannedPurge(PartitionId.unsafe(partition), Offset.unsafe(low), Offset.unsafe(high))

  /** A deleter that answers a fixed set of watermarks and remembers what it was asked to delete. */
  final private class FakeDeleter(
      val asked: Ref[IO, List[Map[PartitionId, Offset]]],
      watermarksOf: List[PlannedPurge],
      val identity: Ref[IO, String]
  ) extends RecordDeleter[IO] {

    def topicId(cluster: ClusterId, topic: TopicName): IO[Either[KuiError, String]] =
      identity.get.map(Right(_))

    def watermarks(cluster: ClusterId, topic: TopicName): IO[Either[KuiError, List[PlannedPurge]]] =
      IO.pure(Right(watermarksOf))

    def cleanupPolicy(cluster: ClusterId, topic: TopicName): IO[Option[String]] = IO.pure(Some("delete"))

    def deleteBefore(
        cluster: ClusterId,
        topic: TopicName,
        offsets: Map[PartitionId, Offset]
    ): IO[Either[KuiError, PurgeResult]] =
      asked.update(_ :+ offsets).as(Right(PurgeResult(offsets, Map.empty)))
  }

  private case class Rig(
      purge: PurgeUseCase[IO],
      deleter: FakeDeleter,
      records: Ref[IO, List[MutationRecord]]
  )

  private def rig(watermarks: List[PlannedPurge], readOnly: Boolean = false): IO[Rig] =
    for {
      logger <- FakeStructuredLogger[IO]
      audit <- ProduceRig.RecordingAudit.make
      asked <- Ref.of[IO, List[Map[PartitionId, Offset]]](Nil)
      identity <- Ref.of[IO, String]("original-topic-uuid")
      deleter = new FakeDeleter(asked, watermarks, identity)
      profiles = new ProduceRig.Profiles(readOnly)
      guard = MutationGuard.make[IO](profiles, audit, logger)
      tokens = PurgeToken.make[IO](Secret("a key long enough for HMAC-SHA256".getBytes("UTF-8")))
    } yield Rig(PurgeUseCase.make[IO](deleter, profiles, guard, tokens, logger), deleter, audit.entries)

  test("recreating a topic with identical offsets invalidates its purge token") {
    for {
      built <- rig(List(planned(0, 0L, 100L)))
      offer <- built.purge.plan(ProduceRig.Cluster, ProduceRig.Topic)
      token = offer.getOrElse(fail("the plan must be offered")).token
      _ <- built.deleter.identity.set("replacement-topic-uuid")
      applied <- built.purge.apply(Caller, ProduceRig.Cluster, ProduceRig.Topic, token)
      asked <- built.deleter.asked.get
    } yield {
      assert(applied.isLeft)
      assertEquals(asked, Nil)
    }
  }

  test("a purge deletes exactly the offsets the plan named, and records them in partition order") {
    for {
      built <- rig(List(planned(2, 0L, 900L), planned(0, 0L, 100L), planned(1, 0L, 500L)))
      offer <- built.purge.plan(ProduceRig.Cluster, ProduceRig.Topic)
      token = offer.getOrElse(fail("the plan must be offered")).token
      applied <- built.purge.apply(Caller, ProduceRig.Cluster, ProduceRig.Topic, token)
      asked <- built.deleter.asked.get
      records <- built.records.get
    } yield {
      assert(applied.isRight, s"the purge must be applied: $applied")
      // Exactly the offsets the plan named, and not a recomputation: records that arrived after the
      // operator agreed to lose the figure they read must not be included.
      assertEquals(
        asked,
        List(
          Map(
            PartitionId.unsafe(0) -> Offset.unsafe(100L),
            PartitionId.unsafe(1) -> Offset.unsafe(500L),
            PartitionId.unsafe(2) -> Offset.unsafe(900L)
          )
        )
      )
      assertEquals(records.map(_.outcome), List(MutationOutcome.Succeeded))
      assertEquals(records.head.detail.get("deletedBefore"), Some("0:100,1:500,2:900"))
      assertEquals(records.head.detail.get("records"), Some("1500"))
    }
  }

  test("a token planned against a topic with no records is refused rather than sent to Kafka") {
    for {
      built <- rig(List(planned(0, 40L, 40L), planned(1, 12L, 12L)))
      offer <- built.purge.plan(ProduceRig.Cluster, ProduceRig.Topic)
      token = offer.getOrElse(fail("the plan must be offered")).token
      applied <- built.purge.apply(Caller, ProduceRig.Cluster, ProduceRig.Topic, token)
      asked <- built.deleter.asked.get
      records <- built.records.get
    } yield {
      assertEquals(applied.left.map(_.code), Left(ErrorCode.InvalidState))
      // Nothing reached the broker, and nothing was audited: there was no mutation to record.
      assertEquals(asked, Nil)
      assertEquals(records, Nil)
    }
  }

  test("planning a purge on a read-only cluster is refused before a plan is ever rendered") {
    for {
      built <- rig(List(planned(0, 0L, 100L)), readOnly = true)
      offer <- built.purge.plan(ProduceRig.Cluster, ProduceRig.Topic)
    } yield assertEquals(offer.left.map(_.code), Left(ErrorCode.ReadOnly))
  }

  test("a token minted for one topic does not apply to another") {
    for {
      built <- rig(List(planned(0, 0L, 100L)))
      offer <- built.purge.plan(ProduceRig.Cluster, ProduceRig.Topic)
      token = offer.getOrElse(fail("the plan must be offered")).token
      applied <- built.purge.apply(Caller, ProduceRig.Cluster, ProduceRig.ReplayTopic, token)
      asked <- built.deleter.asked.get
    } yield {
      assert(applied.isLeft, s"a token names the topic it was planned for: $applied")
      assertEquals(asked, Nil)
    }
  }
}
