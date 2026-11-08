package kui.consumer.application

import java.time.Instant

import scala.concurrent.duration.*

import cats.effect.IO
import cats.effect.kernel.{Deferred, Ref}
import cats.syntax.all.*

import kui.consumer.domain.*
import kui.consumer.domain.fixtures.GroupFixtures
import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.kernel.group.{GroupState, ResetTarget}
import kui.kernel.{GroupId, Offset, Secret, TopicPartition}
import kui.security.Principal
import kui.security.audit.{AuditPrincipal, MutationKind, MutationOutcome}
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** The first destructive operation in the product, and the three substitutes it ships with (ADR-047).
  *
  * Read-only refusal before any Kafka client is touched, an audit record for every attempt including the
  * refused ones, and a plan token that makes "apply" mean "apply exactly what was shown".
  */
final class MutationSuite extends KuiIOSuite {

  private val group: GroupId = GroupId.unsafe("orders-consumer")

  private val emptyGroup: ConsumerGroup =
    GroupFixtures.group(
      id = group.value,
      state = GroupState.Empty,
      partitions = List(GroupFixtures.state(0, Some(40L)))
    )

  private val liveGroup: ConsumerGroup =
    GroupFixtures.group(
      id = group.value,
      state = GroupState.Stable,
      members = List(GroupFixtures.member("m-1", Set(0))),
      partitions = List(GroupFixtures.state(0, Some(40L)))
    )

  private val scope: ResetScope = ResetScope(GroupFixtures.Orders, Set(GroupFixtures.partition(0)))

  private val window: OffsetWindow = OffsetWindow(
    begin = Map(GroupFixtures.partition(0) -> Offset.unsafe(0L)),
    end = Map(GroupFixtures.partition(0) -> Offset.unsafe(100L)),
    committed = Map(GroupFixtures.partition(0) -> Offset.unsafe(40L)),
    atTimestamp = Map.empty,
    leaderless = Set.empty
  )

  private def portState(described: ConsumerGroup): ConsumerRig.PortState =
    ConsumerRig.PortState.Empty.copy(
      listing = Right(ConsumerRig.listingOf(List(described))),
      described = Right(Map(described.groupId -> described)),
      window = Right(window)
    )

  private val tokens: PlanToken[IO] = PlanToken.make[IO](Secret("a-test-key".getBytes("UTF-8")))

  /** The verified principal every mutation in this suite is made by.
    *
    * `Principal.Anonymous` and not an invented name, because that is exactly what a deployment without
    * authentication produces, and the audit assertions below are about what such a deployment records.
    */
  private val Caller: Principal = Principal.Anonymous

  /** A guard over a snapshots component that records the invalidations it was asked for. */
  private def rig(
      described: ConsumerGroup,
      readOnly: Boolean = false
  ): IO[(ConsumerRig.FakePort, ConsumerRig.RecordingAudit, MutationGuard[IO], Ref[IO, List[String]])] =
    for {
      port <- ConsumerRig.port(portState(described))
      profiles <- ConsumerRig.profiles(readOnly)
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
    } yield (port, audit, guard, invalidations)

  private def resetUseCase(port: GroupAdminPort[IO], guard: MutationGuard[IO], readOnly: Boolean) =
    for {
      profiles <- ConsumerRig.profiles(readOnly)
      logger <- FakeStructuredLogger[IO]
    } yield OffsetResetUseCase.make[IO](_ => port, guard, profiles, tokens, logger)

  test("reset planning and apply refuse a missing group description") {
    for {
      rigged <- rig(emptyGroup)
      (port, _, guard, _) = rigged
      reset <- resetUseCase(port, guard, readOnly = false)
      planned <- reset.plan(ConsumerRig.Cluster, group, scope, ResetSpec.ToEarliest)
      token = planned.getOrElse(fail("no plan")).token
      _ <- port.state.update(_.copy(described = Right(Map.empty)))
      refusedPlan <- reset.plan(ConsumerRig.Cluster, group, scope, ResetSpec.ToEarliest)
      refusedApply <- reset.apply(Caller, ConsumerRig.Cluster, group, token)
      state <- port.state.get
    } yield {
      assert(refusedPlan.isLeft)
      assert(refusedApply.isLeft)
      assertEquals(state.applied, Nil)
    }
  }

  test("delete mutations refuse unknown committed offsets rather than successful empty deletion") {
    for {
      rigged <- rig(emptyGroup.copy(completeness = emptyGroup.completeness.withoutCommittedOffsets))
      (port, _, guard, _) = rigged
      logger <- FakeStructuredLogger[IO]
      offsets = DeleteOffsetsUseCase.make[IO](_ => port, guard, logger)
      groups = DeleteGroupUseCase.make[IO](_ => port, guard, logger)
      deletedOffsets <- offsets.delete(Caller, ConsumerRig.Cluster, group, GroupFixtures.Orders)
      deletedGroup <- groups.delete(Caller, ConsumerRig.Cluster, group)
      state <- port.state.get
    } yield {
      assert(deletedOffsets.isLeft)
      assert(deletedGroup.isLeft)
      assertEquals(state.deletedOffsets, Nil)
      assertEquals(state.deletedGroups, Nil)
    }
  }

  List(
    "retention moved the beginning" -> Right(
      window.copy(begin = Map(GroupFixtures.partition(0) -> Offset.unsafe(1L)))
    ),
    "the partition disappeared" -> Right(window.copy(begin = Map.empty)),
    "the partition lost its leader" -> Right(window.copy(leaderless = scope.partitions)),
    "the lookup failed" -> Left(ApplicationError.InvalidState("offset lookup failed"))
  ).foreach { (reason, fresh) =>
    test(s"reset apply refuses when $reason") {
      for {
        rigged <- rig(emptyGroup)
        (port, _, guard, _) = rigged
        reset <- resetUseCase(port, guard, readOnly = false)
        planned <- reset.plan(ConsumerRig.Cluster, group, scope, ResetSpec.ToEarliest)
        token = planned.getOrElse(fail("no plan")).token
        _ <- port.state.update(_.copy(window = fresh))
        result <- reset.apply(Caller, ConsumerRig.Cluster, group, token)
        state <- port.state.get
      } yield {
        assert(result.isLeft)
        assertEquals(state.applied, Nil)
      }
    }
  }

  test("reset apply rejects a signed target beyond a truncated log end") {
    for {
      rigged <- rig(emptyGroup)
      (port, _, guard, _) = rigged
      reset <- resetUseCase(port, guard, readOnly = false)
      planned <- reset.plan(ConsumerRig.Cluster, group, scope, ResetSpec.ToLatest)
      _ <- port.state.update(
        _.copy(window = Right(window.copy(end = Map(GroupFixtures.partition(0) -> Offset.unsafe(90L)))))
      )
      result <- reset.apply(Caller, ConsumerRig.Cluster, group, planned.getOrElse(fail("plan")).token)
      state <- port.state.get
    } yield {
      assert(result.isLeft)
      assertEquals(state.applied, Nil)
    }
  }

  test("reset apply refuses when committed offsets moved since the plan") {
    // SHIFT_BY is resolved from the committed offset the operator was shown. A token that binds only
    // the proposed absolute offsets will still write them after the group has consumed and stopped
    // again, which is a different rewind from the one on the screen.
    val partition = GroupFixtures.partition(0)
    val plannedAgainst = window.copy(
      end = Map(partition -> Offset.unsafe(10_000L)),
      committed = Map(partition -> Offset.unsafe(1_000L))
    )

    for {
      rigged <- rig(emptyGroup)
      (port, _, guard, _) = rigged
      reset <- resetUseCase(port, guard, readOnly = false)
      _ <- port.state.update(_.copy(window = Right(plannedAgainst)))
      planned <- reset.plan(ConsumerRig.Cluster, group, scope, ResetSpec.ShiftBy(-100L))
      token = planned.map(_.token).getOrElse(fail(s"no plan: $planned"))
      _ <- port.state.update(
        _.copy(window = Right(plannedAgainst.copy(committed = Map(partition -> Offset.unsafe(5_000L)))))
      )
      result <- reset.apply(Caller, ConsumerRig.Cluster, group, token)
      state <- port.state.get
    } yield {
      assertEquals(result.left.map(_.code), Left(ErrorCode.InvalidState))
      assert(
        result.left.exists(_.message.contains("committed offsets moved")),
        clue = result
      )
      assertEquals(state.applied, Nil)
    }
  }

  test("a reset token cannot be applied again after the write moved the commits") {
    val partition = GroupFixtures.partition(0)

    for {
      rigged <- rig(emptyGroup)
      (port, _, guard, _) = rigged
      reset <- resetUseCase(port, guard, readOnly = false)
      planned <- reset.plan(ConsumerRig.Cluster, group, scope, ResetSpec.ToEarliest)
      token = planned.map(_.token).getOrElse(fail(s"no plan: $planned"))
      first <- reset.apply(Caller, ConsumerRig.Cluster, group, token)
      // The broker's committed offset is now the one just written. A second tab re-sending the
      // same token inside its five-minute life must not rewind the group again.
      _ <- port.state.update(
        _.copy(window = Right(window.copy(committed = Map(partition -> Offset.unsafe(0L)))))
      )
      second <- reset.apply(Caller, ConsumerRig.Cluster, group, token)
      state <- port.state.get
    } yield {
      assert(first.isRight, clue = first)
      assertEquals(second.left.map(_.code), Left(ErrorCode.InvalidState))
      assertEquals(state.applied.map(_._2.values.map(_.value).toList), List(List(0L)))
    }
  }

  test("a plan resolves the spec against live offsets and returns what would be written") {
    for {
      rigged <- rig(emptyGroup)
      (port, _, guard, _) = rigged
      reset <- resetUseCase(port, guard, readOnly = false)
      planned <- reset.plan(ConsumerRig.Cluster, group, scope, ResetSpec.ToEarliest)
    } yield planned match {
      case Right(PlannedReset(plan, token, _)) =>
        assertEquals(plan.partitions.map(_.proposed.value), List(0L))
        assertEquals(plan.partitions.map(_.current.map(_.value)), List(Some(40L)))
        assert(token.nonEmpty)
      case Left(error) => fail(s"planning failed: $error")
    }
  }

  test("a read-only cluster is refused at plan time, so no plan is ever shown that cannot be applied") {
    for {
      rigged <- rig(emptyGroup, readOnly = true)
      (port, _, guard, _) = rigged
      reset <- resetUseCase(port, guard, readOnly = true)
      planned <- reset.plan(ConsumerRig.Cluster, group, scope, ResetSpec.ToEarliest)
    } yield assertEquals(planned.left.map(_.code), Left(ErrorCode.ReadOnly))
  }

  test("a group that does not exist is a 404, checked by listing rather than by describing") {
    for {
      rigged <- rig(emptyGroup)
      (port, _, guard, _) = rigged
      _ <- port.state.update(_.copy(listing = Right(GroupListingPage.complete(Nil))))
      reset <- resetUseCase(port, guard, readOnly = false)
      planned <- reset.plan(ConsumerRig.Cluster, group, scope, ResetSpec.ToEarliest)
    } yield assertEquals(planned.left.map(_.code), Left(ErrorCode.GroupNotFound))
  }

  test("a group with members is refused with the code that names the remedy") {
    for {
      rigged <- rig(liveGroup)
      (port, _, guard, _) = rigged
      reset <- resetUseCase(port, guard, readOnly = false)
      planned <- reset.plan(ConsumerRig.Cluster, group, scope, ResetSpec.ToEarliest)
    } yield {
      assertEquals(planned.left.map(_.code), Left(ErrorCode.GroupNotEmpty))
      assert(planned.left.exists(_.message.contains("stop its consumers")))
    }
  }

  test("apply writes exactly the offsets the plan named, and records them") {
    for {
      rigged <- rig(emptyGroup)
      (port, audit, guard, invalidations) = rigged
      reset <- resetUseCase(port, guard, readOnly = false)
      planned <- reset.plan(ConsumerRig.Cluster, group, scope, ResetSpec.ToEarliest)
      token = planned.map(_.token).getOrElse(fail("no plan"))
      applied <- reset.apply(Caller, ConsumerRig.Cluster, group, token)
      state <- port.state.get
      records <- audit.written.get
      invalidated <- invalidations.get
    } yield {
      assert(applied.isRight, s"apply failed: $applied")
      assertEquals(
        state.applied.map((id, offsets) => id -> offsets.values.map(_.value).toList),
        List(group -> List(0L))
      )
      assertEquals(records.size, 1)
      assertEquals(records.head.kind, MutationKind.ResetOffsets)
      assertEquals(records.head.outcome, MutationOutcome.Succeeded)
      assertEquals(records.head.before, Some("orders-0=40"))
      assertEquals(records.head.after, Some("orders-0=0"))
      assertEquals(invalidated.size, 1)
    }
  }

  test("the apply receipt names the target that was planned, not OFFSET") {
    // The token carries the target's wire name. Parsing it back used to keep only EARLIEST and LATEST
    // and turn everything else into ToOffsets, so the receipt said OFFSET for a timestamp reset.
    // `ConsumerMapping.appliedPlan` copies `plan.spec.target` onto the DTO; this module cannot see that
    // mapping without a cycle, and the field it would copy is this one.
    val partition = GroupFixtures.partition(0)
    val cases = List(
      ResetSpec.ToTimestamp(Instant.parse("2026-01-01T09:00:00Z")) -> ResetTarget.Timestamp,
      ResetSpec.ShiftBy(-10L) -> ResetTarget.ShiftBy,
      ResetSpec.ByDuration(1.hour) -> ResetTarget.Duration,
      ResetSpec.ToOffsets(Map(partition -> Offset.unsafe(10L))) -> ResetTarget.Offset
    )

    cases.traverse { (spec, target) =>
      for {
        rigged <- rig(emptyGroup)
        (port, _, guard, _) = rigged
        reset <- resetUseCase(port, guard, readOnly = false)
        planned <- reset.plan(ConsumerRig.Cluster, group, scope, spec)
        token = planned.map(_.token).getOrElse(fail(s"no plan for $target: $planned"))
        applied <- reset.apply(Caller, ConsumerRig.Cluster, group, token)
      } yield assertEquals(applied.map(_.spec.target), Right(target))
    }.void
  }

  test("the apply receipt says what each partition's offset was, not a dash") {
    // The receipt is the document the wizard renders after the write, and its whole job is to say
    // "partition 0 moved from 40 to 0". That "from" is the pre-write reading the audit record uses,
    // not a zero invented for a partition the group had never committed.
    for {
      rigged <- rig(emptyGroup)
      (port, _, guard, _) = rigged
      reset <- resetUseCase(port, guard, readOnly = false)
      planned <- reset.plan(ConsumerRig.Cluster, group, scope, ResetSpec.ToEarliest)
      token = planned.map(_.token).getOrElse(fail("no plan"))
      applied <- reset.apply(Caller, ConsumerRig.Cluster, group, token)
    } yield {
      val partitions = applied.map(_.partitions).getOrElse(fail(s"apply failed: $applied"))
      assertEquals(partitions.map(_.current.map(_.value)), List(Some(40L)))
      assertEquals(partitions.map(_.proposed.value), List(0L))
      assertEquals(partitions.map(_.delta), List(Some(-40L)))
    }
  }

  test("a group that gained a member between planning and applying is refused before the write") {
    for {
      rigged <- rig(emptyGroup)
      (port, _, guard, _) = rigged
      reset <- resetUseCase(port, guard, readOnly = false)
      planned <- reset.plan(ConsumerRig.Cluster, group, scope, ResetSpec.ToEarliest)
      token = planned.map(_.token).getOrElse(fail("no plan"))
      // The race the two-phase flow widens, and the reason the precondition is checked twice.
      _ <- port.state.update(_.copy(described = Right(Map(group -> liveGroup))))
      applied <- reset.apply(Caller, ConsumerRig.Cluster, group, token)
      state <- port.state.get
    } yield {
      assertEquals(applied.left.map(_.code), Left(ErrorCode.GroupNotEmpty))
      assertEquals(state.applied, Nil, clue = "offsets were written to a group that had come back to life")
    }
  }

  test("a token for another group does not apply here, and says nothing about why") {
    for {
      rigged <- rig(emptyGroup)
      (port, _, guard, _) = rigged
      reset <- resetUseCase(port, guard, readOnly = false)
      planned <- reset.plan(ConsumerRig.Cluster, group, scope, ResetSpec.ToEarliest)
      token = planned.map(_.token).getOrElse(fail("no plan"))
      applied <- reset.apply(Caller, ConsumerRig.Cluster, GroupId.unsafe("someone-else"), token)
    } yield {
      assertEquals(applied.left.map(_.code), Left(ErrorCode.Validation))
      // The message must not say which half was wrong: that is an oracle.
      assert(applied.left.exists(!_.message.toLowerCase.contains("signature")))
    }
  }

  test("an expired token is refused") {
    val plan = ResetPlan(
      group,
      scope,
      ResetSpec.ToEarliest,
      List(PlannedPartition(GroupFixtures.partition(0), None, Offset.unsafe(0L), None)),
      Nil,
      ConsumerRig.At
    )

    for {
      token <- tokens.mint(ConsumerRig.Cluster, plan, Instant.parse("2026-01-01T00:00:00Z"))
      verified <- tokens.verify(ConsumerRig.Cluster, group, token, Instant.parse("2026-01-02T00:00:00Z"))
    } yield assertEquals(verified.left.map(_.code), Left(ErrorCode.Validation))
  }

  test("a token minted for another cluster does not apply to this one") {
    val plan = ResetPlan(
      group,
      scope,
      ResetSpec.ToEarliest,
      List(PlannedPartition(GroupFixtures.partition(0), None, Offset.unsafe(0L), None)),
      Nil,
      ConsumerRig.At
    )

    for {
      token <- tokens.mint(kui.kernel.ClusterId.unsafe("staging"), plan, ConsumerRig.At.plusSeconds(300))
      verified <- tokens.verify(ConsumerRig.Cluster, group, token, ConsumerRig.At)
    } yield assertEquals(verified.left.map(_.code), Left(ErrorCode.Validation))
  }

  test("a cluster whose id only appears inside the binding is not the cluster the token was minted for") {
    // The cluster is its own field and has to be equal to the one verify was asked about. A search
    // for the cluster id inside the payload would accept this token on a cluster called `orders`
    // (that string is the topic field) or on `pro` (a prefix of `prod`). The offsets on the screen
    // were read from a different cluster's log ends.
    val plan = ResetPlan(
      group,
      scope,
      ResetSpec.ToEarliest,
      List(PlannedPartition(GroupFixtures.partition(0), None, Offset.unsafe(0L), None)),
      Nil,
      ConsumerRig.At
    )
    val neighbour = kui.kernel.ClusterId.unsafe("orders")
    val prefix = kui.kernel.ClusterId.unsafe("pro")

    for {
      token <- tokens.mint(ConsumerRig.Cluster, plan, ConsumerRig.At.plusSeconds(300))
      verified <- tokens.verify(neighbour, group, token, ConsumerRig.At)
      prefixed <- tokens.verify(prefix, group, token, ConsumerRig.At)
      // The other direction, so a mutation that refuses everything fails here too.
      mine <- tokens.verify(ConsumerRig.Cluster, group, token, ConsumerRig.At)
    } yield {
      assertEquals(
        verified.left.map(_.code),
        Left(ErrorCode.Validation),
        s"a token minted for ${ConsumerRig.Cluster.value} applied on ${neighbour.value}"
      )
      assertEquals(prefixed.left.map(_.code), Left(ErrorCode.Validation))
      assert(mine.isRight, "the cluster the token was minted for was refused its own token")
    }
  }

  test("a tampered token is refused rather than applied with the offsets somebody edited in") {
    val plan = ResetPlan(
      group,
      scope,
      ResetSpec.ToEarliest,
      List(PlannedPartition(GroupFixtures.partition(0), None, Offset.unsafe(0L), None)),
      Nil,
      ConsumerRig.At
    )

    for {
      token <- tokens.mint(ConsumerRig.Cluster, plan, ConsumerRig.At.plusSeconds(300))
      forged = token.take(4) + "X" + token.drop(5)
      verified <- tokens.verify(ConsumerRig.Cluster, group, forged, ConsumerRig.At)
    } yield assert(verified.isLeft)
  }

  test("a refused mutation is audited too, and the Kafka client is never touched") {
    for {
      rigged <- rig(emptyGroup, readOnly = true)
      (port, audit, guard, _) = rigged
      logger <- FakeStructuredLogger[IO]
      deleteGroup = DeleteGroupUseCase.make[IO](_ => port, guard, logger)
      result <- deleteGroup.delete(Caller, ConsumerRig.Cluster, group)
      state <- port.state.get
      records <- audit.written.get
    } yield {
      assertEquals(result.left.map(_.code), Left(ErrorCode.ReadOnly))
      assertEquals(state.deletedGroups, Nil, clue = "a read-only cluster was asked to delete a group")
      assertEquals(records.size, 1)
      assertEquals(records.head.outcome, MutationOutcome.Refused)
    }
  }

  test("deleting a group records where its offsets were, so the record answers what was lost") {
    for {
      rigged <- rig(emptyGroup)
      (port, audit, guard, _) = rigged
      logger <- FakeStructuredLogger[IO]
      deleteGroup = DeleteGroupUseCase.make[IO](_ => port, guard, logger)
      result <- deleteGroup.delete(Caller, ConsumerRig.Cluster, group)
      state <- port.state.get
      records <- audit.written.get
    } yield {
      assert(result.isRight)
      assertEquals(state.deletedGroups, List(group))
      assertEquals(records.head.before, Some("orders-0=40"))
      assertEquals(records.head.after, None)
    }
  }

  test("deleting offsets uses the group's own commits, not a topic's partition list") {
    for {
      rigged <- rig(emptyGroup)
      (port, _, guard, _) = rigged
      logger <- FakeStructuredLogger[IO]
      deleteOffsets = DeleteOffsetsUseCase.make[IO](_ => port, guard, logger)
      result <- deleteOffsets.delete(Caller, ConsumerRig.Cluster, group, GroupFixtures.Orders)
      state <- port.state.get
    } yield {
      assertEquals(result.map(_.partitions), Right(Set(GroupFixtures.partition(0))))
      assertEquals(
        state.deletedOffsets.map((_, partitions) => partitions),
        List(Set(GroupFixtures.partition(0)))
      )
    }
  }

  test("deleting offsets for a topic the group never committed on changes nothing and is not an error") {
    for {
      rigged <- rig(emptyGroup)
      (port, _, guard, _) = rigged
      logger <- FakeStructuredLogger[IO]
      deleteOffsets = DeleteOffsetsUseCase.make[IO](_ => port, guard, logger)
      result <- deleteOffsets.delete(
        Caller,
        ConsumerRig.Cluster,
        group,
        kui.kernel.TopicName.unsafe("untouched")
      )
      state <- port.state.get
    } yield {
      assertEquals(result.map(_.partitions), Right(Set.empty[TopicPartition]))
      assertEquals(state.deletedOffsets, Nil)
    }
  }

  test("a mutation that fails is audited as a failure rather than silently dropped") {
    for {
      rigged <- rig(emptyGroup)
      (port, audit, guard, _) = rigged
      failing = new GroupAdminPort[IO] {
        def list(states: Set[GroupState]) = port.list(states)
        def describe(ids: List[GroupId]) = port.describe(ids)
        def exists(id: GroupId) = port.exists(id)
        def offsetWindow(g: GroupId, s: ResetScope, at: Option[Instant]) = port.offsetWindow(g, s, at)
        def applyOffsets(g: GroupId, offsets: Map[TopicPartition, Offset]) =
          IO.pure(Left(ApplicationError.Refused(ErrorCode.GroupNotEmpty, "the broker said no"): KuiError))
        def deleteOffsets(g: GroupId, partitions: Set[TopicPartition]) = port.deleteOffsets(g, partitions)
        def deleteGroup(id: GroupId) =
          IO.pure(Left(ApplicationError.Refused(ErrorCode.GroupNotEmpty, "the broker said no"): KuiError))
      }
      logger <- FakeStructuredLogger[IO]
      deleteGroup = DeleteGroupUseCase.make[IO](_ => failing, guard, logger)
      result <- deleteGroup.delete(Caller, ConsumerRig.Cluster, group)
      records <- audit.written.get
    } yield {
      assertEquals(result.left.map(_.code), Left(ErrorCode.GroupNotEmpty))
      assertEquals(records.size, 1)
      assertEquals(records.head.outcome, MutationOutcome.Refused)
    }
  }

  test("a cancelled mutation is recorded as unknown, and never as a success or a failure") {
    // `MutationOutcome.Unknown`'s own scaladoc is the rule: Kafka gives no guarantee that a cancelled
    // write was *not* applied, so a record claiming either would be a lie, and `Unknown` is what tells an
    // operator to go and look. Nothing asserted it — turning this branch into `Succeeded` left
    // `./mill libs.__.test + services.*` at 2633/2633, and the operator would then read that an offset
    // reset they aborted had gone through.
    for {
      rigged <- rig(emptyGroup)
      (_, audit, guard, invalidations) = rigged
      started <- Deferred[IO, Unit]
      running <- guard
        .guard(Caller, ConsumerRig.Cluster, MutationKind.ResetOffsets, group.value, Map.empty, Map.empty)(
          started.complete(()) >> IO.never[Either[KuiError, Unit]]
        )
        .start
      _ <- started.get
      _ <- running.cancel
      records <- audit.written.get
      invalidated <- invalidations.get
    } yield {
      assertEquals(records.map(_.outcome), List(MutationOutcome.Unknown))
      assertEquals(
        records.head.detail.get("reason"),
        Some("the operation was cancelled after the request was sent")
      )
      // And the snapshot is not dropped: invalidation is the success path's, and doing it here would
      // claim the same thing the outcome refuses to claim.
      assertEquals(invalidated, Nil)
    }
  }

  test("no audit record carries anything but the group, the offsets and the outcome") {
    for {
      rigged <- rig(emptyGroup)
      (port, audit, guard, _) = rigged
      logger <- FakeStructuredLogger[IO]
      deleteGroup = DeleteGroupUseCase.make[IO](_ => port, guard, logger)
      _ <- deleteGroup.delete(Caller, ConsumerRig.Cluster, group)
      records <- audit.written.get
    } yield {
      val record = records.head
      assertEquals(record.cluster, ConsumerRig.Cluster)
      assertEquals(record.resource, group.value)
      // The one placeholder principal, and the only one: E2 consolidated the two different strings
      // the topic and consumer services used to invent for "nobody was signed in".
      assertEquals(record.principal, Principal.Anonymous)
      assertEquals(AuditPrincipal.render(record.principal), "anonymous (authentication is not enabled)")
      // The record is a flat set of scalars; there is no field on it that could hold a connection, a
      // property map or a secret.
      assertEquals(record.detail, Map.empty[String, String])
    }
  }
}
