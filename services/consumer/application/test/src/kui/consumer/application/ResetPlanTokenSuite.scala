package kui.consumer.application

import java.time.Instant

import scala.concurrent.duration.*

import cats.effect.IO
import cats.effect.kernel.Ref

import kui.consumer.domain.*
import kui.consumer.domain.fixtures.GroupFixtures
import kui.kernel.group.GroupState
import kui.kernel.{GroupId, Offset, Secret}
import kui.security.Principal
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** Three rules of ADR-045's plan→token→confirm that `MutationSuite` leaves open. Each was found by mutation
  * against `./mill services.consumer.application.test` (48 cases, 5 suites), which stayed green for all
  * three.
  *
  *   - **`PlanToken.render` sorts the partitions it signs.** The comment on it says why — *"the token is only
  *     as good as the guarantee that the same plan renders to the same bytes"* — and dropping the `sortBy`
  *     left the suite green, because the planner happens to hand it a sorted list today. A token is a
  *     signature over text: the moment anything mints one from a plan assembled elsewhere, two identical
  *     plans sign to two different strings and a confirm compares unequal.
  *   - **`OffsetResetUseCase.TokenTtl` is five minutes.** Nothing read the constant. Setting it to five days
  *     left the suite green, and the expiry cases in `MutationSuite` pass their own instants, so the figure
  *     the comment argues for was checked by nothing (house rule 11).
  *   - **The audit record's `before` is the planned partitions only.** `currentOffsets` filters the window it
  *     read down to `plan.offsets.keySet`; removing that filter left the suite green, because the fake port
  *     answers every `offsetWindow` with a window covering exactly the planned partition. A record whose
  *     `before` names partitions the reset did not touch says a reset moved offsets it never wrote.
  */
final class ResetPlanTokenSuite extends KuiIOSuite {

  private val group: GroupId = GroupId.unsafe("orders-consumer")

  private val emptyGroup: ConsumerGroup =
    GroupFixtures.group(
      id = group.value,
      state = GroupState.Empty,
      partitions = List(GroupFixtures.state(0, Some(40L)))
    )

  private val scope: ResetScope = ResetScope(GroupFixtures.Orders, Set(GroupFixtures.partition(0)))

  /** A window that covers more than the plan does: partition 5 has a committed offset and is not in scope. */
  private val widerWindow: OffsetWindow = OffsetWindow(
    begin = Map(
      GroupFixtures.partition(0) -> Offset.unsafe(0L),
      GroupFixtures.partition(5) -> Offset.unsafe(0L)
    ),
    end = Map(
      GroupFixtures.partition(0) -> Offset.unsafe(100L),
      GroupFixtures.partition(5) -> Offset.unsafe(100L)
    ),
    committed = Map(
      GroupFixtures.partition(0) -> Offset.unsafe(40L),
      GroupFixtures.partition(5) -> Offset.unsafe(77L)
    ),
    atTimestamp = Map.empty,
    leaderless = Set.empty
  )

  private val tokens: PlanToken[IO] = PlanToken.make[IO](Secret("a-test-key".getBytes("UTF-8")))

  // -- the canonical rendering -----------------------------------------------------------------------

  private def plannedPartition(n: Int, proposed: Long): PlannedPartition =
    PlannedPartition(GroupFixtures.partition(n), None, Offset.unsafe(proposed), None)

  private def planOf(partitions: List[PlannedPartition]): ResetPlan =
    ResetPlan(
      group = group,
      scope = ResetScope(GroupFixtures.Orders, partitions.map(_.partition).toSet),
      spec = ResetSpec.ToLatest,
      partitions = partitions,
      warnings = Nil,
      computedAt = Instant.parse("2026-09-11T09:00:00Z")
    )

  test("twoPlansOverTheSamePartitionsInAnyOrderMintTheSameToken") {
    val ascending = List(plannedPartition(0, 10L), plannedPartition(1, 20L), plannedPartition(2, 30L))
    val shuffled = List(plannedPartition(2, 30L), plannedPartition(0, 10L), plannedPartition(1, 20L))
    val expiresAt = Instant.parse("2026-09-11T09:05:00Z")

    for {
      first <- tokens.mint(ConsumerRig.Cluster, planOf(ascending), expiresAt)
      second <- tokens.mint(ConsumerRig.Cluster, planOf(shuffled), expiresAt)
      // And the other direction: a plan that really is different must not mint the same token, or the
      // assertion above would also hold for a `render` that signed a constant.
      elsewhere = planOf(ascending.map(_.copy(proposed = Offset.unsafe(99L))))
      third <- tokens.mint(ConsumerRig.Cluster, elsewhere, expiresAt)
    } yield {
      assertEquals(second, first)
      assertNotEquals(third, first)
    }
  }

  test("aTokenBindsTheCommittedOffsetEachPartitionWasPlannedAgainst") {
    // `none` has to round-trip as absence. Encoding it as `0` would let a later commit of zero pass the
    // staleness check, which is the partition the group had never committed for.
    val withCommit =
      PlannedPartition(GroupFixtures.partition(0), Some(Offset.unsafe(40L)), Offset.unsafe(10L), Some(-30L))
    val without = PlannedPartition(GroupFixtures.partition(1), None, Offset.unsafe(0L), None)
    val now = Instant.parse("2026-09-11T09:00:00Z")

    for {
      token <- tokens.mint(ConsumerRig.Cluster, planOf(List(withCommit, without)), now.plusSeconds(60L))
      verified <- tokens.verify(ConsumerRig.Cluster, group, token, now)
    } yield verified match {
      case Right(plan) =>
        assertEquals(plan.partitions.map(_.current.map(_.value)), List(Some(40L), None))
        assertEquals(plan.partitions.map(_.proposed.value), List(10L, 0L))
      case Left(error) => fail(s"the token did not verify: $error")
    }
  }

  test("aTokenMintedFromAnUnsortedPlanVerifiesToTheSortedOffsets") {
    val shuffled = List(plannedPartition(2, 30L), plannedPartition(0, 10L), plannedPartition(1, 20L))
    val now = Instant.parse("2026-09-11T09:00:00Z")

    for {
      token <- tokens.mint(ConsumerRig.Cluster, planOf(shuffled), now.plusSeconds(60L))
      verified <- tokens.verify(ConsumerRig.Cluster, group, token, now)
    } yield verified match {
      case Right(plan) =>
        assertEquals(plan.partitions.map(_.partition.partition.value), List(0, 1, 2))
        assertEquals(
          plan.offsets.map((p, o) => p.partition.value -> o.value),
          Map(0 -> 10L, 1 -> 20L, 2 -> 30L)
        )
      case Left(error) => fail(s"the token did not verify: $error")
    }
  }

  // -- the lifetime ----------------------------------------------------------------------------------

  test("theTokenLifetimeIsTheFiguresItsCommentArguesFor") {
    // "Long enough to read a plan of a hundred partitions, short enough that the cluster it was computed
    // against is still recognisably the same one." Both halves, as bounds, so the constant cannot drift to
    // a value nobody would defend without this case saying so.
    assert(
      OffsetResetUseCase.TokenTtl >= 1.minute,
      s"${OffsetResetUseCase.TokenTtl} is too short to read a plan in"
    )
    assert(
      OffsetResetUseCase.TokenTtl <= 15.minutes,
      s"${OffsetResetUseCase.TokenTtl} outlives the cluster state the plan was computed against"
    )
  }

  test("aPlansExpiryIsExactlyTokenTtlAfterItWasComputed") {
    for {
      rigged <- rig()
      (reset, _, _) = rigged
      answered <- reset.plan(ConsumerRig.Cluster, group, scope, ResetSpec.ToLatest)
    } yield answered match {
      case Right(PlannedReset(plan, _, expiresAt)) =>
        assertEquals(
          expiresAt.toEpochMilli - plan.computedAt.toEpochMilli,
          OffsetResetUseCase.TokenTtl.toMillis
        )
      case Left(error) => fail(s"the plan failed: $error")
    }
  }

  test("aTokenIsRefusedOneMillisecondPastItsOwnLifetime") {
    // The end-to-end half: the constant reaches the expiry the verifier enforces, rather than only being a
    // number in the answer.
    for {
      rigged <- rig()
      (reset, _, _) = rigged
      answered <- reset.plan(ConsumerRig.Cluster, group, scope, ResetSpec.ToLatest)
      plannedReset = answered.getOrElse(fail("the plan must succeed"))
      // The token carries `expiresAt.toEpochMilli`, so the instant the verifier compares against is the
      // millisecond, not the nanosecond the plan was answered with. The boundary is asserted at that
      // resolution rather than at the answer's, which is where this case first failed.
      onTheWire = java.time.Instant.ofEpochMilli(plannedReset.expiresAt.toEpochMilli)
      justAlive <- tokens.verify(ConsumerRig.Cluster, group, plannedReset.token, onTheWire)
      expired <- tokens.verify(ConsumerRig.Cluster, group, plannedReset.token, onTheWire.plusMillis(1L))
      wellInside <- tokens.verify(
        ConsumerRig.Cluster,
        group,
        plannedReset.token,
        onTheWire.minusMillis(OffsetResetUseCase.TokenTtl.toMillis / 2)
      )
    } yield {
      assert(wellInside.isRight, "a token halfway through its lifetime was refused")
      assert(justAlive.isRight, "a token at its own expiry is still good")
      assert(expired.isLeft, "a token past its expiry was accepted")
    }
  }

  // -- the audit record's `before` -------------------------------------------------------------------

  test("theAuditedBeforeNamesOnlyThePartitionsTheResetWrote") {
    for {
      rigged <- rig()
      (reset, audit, _) = rigged
      answered <- reset.plan(ConsumerRig.Cluster, group, scope, ResetSpec.ToLatest)
      plannedReset = answered.getOrElse(fail("the plan must succeed"))
      applied <- reset.apply(Principal.Anonymous, ConsumerRig.Cluster, group, plannedReset.token)
      records <- audit.written.get
    } yield {
      assert(applied.isRight, s"the apply failed: $applied")
      val succeeded = records.find(_.outcome == kui.security.audit.MutationOutcome.Succeeded)
      val before = succeeded.flatMap(_.before).getOrElse(fail("a successful reset records where it started"))

      // Partition 5 has a committed offset in the window the port answers with and is not in the plan.
      assertEquals(before, "orders-0=40")
      assert(!before.contains("orders-5"), s"the record names a partition the reset never wrote: $before")
    }
  }

  /** The use case, its audit sink and the port, over a window wider than the plan. */
  private def rig(): IO[(OffsetResetUseCase[IO], ConsumerRig.RecordingAudit, ConsumerRig.FakePort)] =
    for {
      port <- ConsumerRig.port(
        ConsumerRig.PortState.Empty.copy(
          listing = Right(ConsumerRig.listingOf(List(emptyGroup))),
          described = Right(Map(emptyGroup.groupId -> emptyGroup)),
          window = Right(widerWindow)
        )
      )
      profiles <- ConsumerRig.profiles()
      audit <- ConsumerRig.audit
      logger <- FakeStructuredLogger[IO]
      invalidations <- Ref.of[IO, List[String]](Nil)
      snapshots = new GroupSnapshots[IO] {
        def of(cluster: kui.kernel.ClusterId) = IO.pure(None)
        def all = IO.pure(Nil)
        def previousOf(cluster: kui.kernel.ClusterId) = IO.pure(None)
        def requestRefresh(cluster: kui.kernel.ClusterId) = IO.pure(false)
        def invalidate(cluster: kui.kernel.ClusterId, reason: String) = invalidations.update(_ :+ reason)
      }
      guard = MutationGuard.make[IO](profiles, audit, snapshots, logger)
      useCase = OffsetResetUseCase.make[IO](_ => port, guard, profiles, tokens, logger)
    } yield (useCase, audit, port)
}
