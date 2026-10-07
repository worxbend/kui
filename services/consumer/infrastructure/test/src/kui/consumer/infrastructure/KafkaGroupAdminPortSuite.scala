package kui.consumer.infrastructure

import java.time.Instant

import cats.effect.IO
import cats.effect.kernel.Ref

import kui.consumer.domain.{OffsetWindow, ResetScope}
import kui.kafka.admin.*
import kui.kafka.{BatchResult, SkipReason}
import kui.kernel.cluster.{
  AdminTuning,
  BootstrapServers,
  ClientProperties,
  ClusterConnection,
  ClusterSecurity
}
import kui.kernel.error.{InfrastructureError, KuiError}
import kui.kernel.group.{GroupProtocol, GroupState, LagAnomaly}
import kui.kernel.{ClusterId, GroupId, Offset, PartitionId, TopicName, TopicPartition}
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** The seam: `libs/kafka`'s results on one side, the consumer domain on the other.
  *
  * Both sides are unit-tested on their own, which is exactly the condition under which this seam breaks
  * silently. The assertions here are about the composition — that a group's commits and the log ends its lag
  * is measured against come from one pass, that a leaderless partition arrives as `NO_LEADER` and not as a
  * zero, and that a cluster which answers half the calls still produces a page.
  */
final class KafkaGroupAdminPortSuite extends KuiIOSuite {

  private val connection: ClusterConnection = ClusterConnection(
    id = ClusterId.unsafe("prod"),
    bootstrapServers = BootstrapServers.unsafe("broker:9092"),
    security = ClusterSecurity.Plaintext,
    overrides = ClientProperties.empty,
    admin = AdminTuning.default
  )

  private val orders: GroupId = GroupId.unsafe("orders-consumer")
  private val topic: TopicName = TopicName.unsafe("orders")

  private def partition(n: Int): TopicPartition = TopicPartition(topic, PartitionId.unsafe(n))

  private def member(held: Set[Int]): GroupMember =
    GroupMember.of("m-1", None, "client", "10.0.0.7", MemberAssignment(held.map(partition)), None)

  private def description(
      members: List[GroupMember],
      state: GroupState = GroupState.Stable
  ): GroupDescription =
    GroupDescription(
      groupId = orders,
      isSimple = false,
      state = state,
      protocol = GroupProtocol.Classic,
      partitionAssignor = "range",
      members = members,
      coordinator = Some(GroupCoordinator(kui.kernel.BrokerId.unsafe(1), "broker-1", 9092)),
      authorizedOperations = None
    )

  /** A `GroupAdmin` whose every answer is a field, so a test says what the cluster said. */
  final private class FakeAdmin(
      val listing: Ref[IO, Either[KuiError, GroupListingResult]],
      val described: Ref[IO, Either[KuiError, BatchResult[GroupId, GroupDescription]]],
      val committed: Ref[IO, Either[KuiError, BatchResult[GroupId, List[CommittedOffset]]]],
      val altered: Ref[IO, List[(GroupId, Map[TopicPartition, Offset])]],
      deleteSkips: Ref[IO, Map[GroupId, SkipReason]],
      /** Every `requireStable` this fake was asked for, newest last.
        *
        * The flag used to be swallowed, which is why swapping the port's two call sites — `false` on the read
        * path, `true` when a reset is being planned — left `./mill services.consumer.infrastructure.test` at
        * 16/16 green. A fixture that cannot express an argument cannot gate it.
        */
      val stability: Ref[IO, List[Boolean]]
  ) extends GroupAdmin[IO] {

    def listGroups(conn: ClusterConnection, states: Set[GroupState]) = listing.get

    def describeGroups(conn: ClusterConnection, ids: List[GroupId], includeAuthorizedOperations: Boolean) =
      described.get

    def committedOffsets(
        conn: ClusterConnection,
        groups: List[GroupId],
        partitions: Option[Set[TopicPartition]],
        requireStable: Boolean
    ) = stability.update(_ :+ requireStable) *> committed.get

    def alterOffsets(conn: ClusterConnection, group: GroupId, offsets: Map[TopicPartition, Offset]) =
      altered.update(_ :+ (group -> offsets)).as(Right(()))

    def deleteOffsets(conn: ClusterConnection, group: GroupId, partitions: Set[TopicPartition]) =
      IO.pure(Right(()))

    /** What the broker answers a `deleteGroups` with. A skip here is a per-group refusal, which is the shape
      * `BatchResult` exists to carry and the one a caller most easily reads as success.
      */
    val deletions: Ref[IO, Map[GroupId, SkipReason]] = deleteSkips

    def deleteGroups(conn: ClusterConnection, ids: List[GroupId]) =
      deletions.get.map(skips => Right(BatchResult(ids.filterNot(skips.contains).map(_ -> ()).toMap, skips)))
  }

  final private class FakeOffsets(
      ends: Either[KuiError, BatchResult[TopicPartition, Offset]],
      begins: Either[KuiError, BatchResult[TopicPartition, Offset]],
      offline: Set[TopicPartition],
      times: Option[BatchResult[TopicPartition, Option[Offset]]],
      metadataFailure: Option[KuiError]
  ) extends OffsetLookup[IO] {
    def endOffsets(conn: ClusterConnection, partitions: Set[TopicPartition]) = IO.pure(ends)
    def beginningOffsets(conn: ClusterConnection, partitions: Set[TopicPartition]) = IO.pure(begins)
    def offsetsForTimes(conn: ClusterConnection, timestamps: Map[TopicPartition, Long]) =
      IO.pure(
        Right(times.getOrElse(BatchResult.complete(timestamps.map((p, _) => p -> Option(Offset.unsafe(7L))))))
      )
    def leaderless(conn: ClusterConnection, partitions: Set[TopicPartition]) =
      IO.pure(metadataFailure.toLeft(offline))
  }

  private def rig(
      described: Either[KuiError, BatchResult[GroupId, GroupDescription]] = Right(
        BatchResult.complete(Map(orders -> description(List(member(Set(0, 1))))))
      ),
      committed: Either[KuiError, BatchResult[GroupId, List[CommittedOffset]]] = Right(
        BatchResult.complete(
          Map(
            orders -> List(
              CommittedOffset(
                TopicPartition(TopicName.unsafe("orders"), PartitionId.unsafe(0)),
                Offset.unsafe(90L),
                None,
                None
              )
            )
          )
        )
      ),
      ends: Either[KuiError, BatchResult[TopicPartition, Offset]] = Right(
        BatchResult.complete(
          Map(TopicPartition(TopicName.unsafe("orders"), PartitionId.unsafe(0)) -> Offset.unsafe(100L))
        )
      ),
      endsSkipped: Map[TopicPartition, SkipReason] = Map.empty,
      offline: Set[TopicPartition] = Set.empty,
      times: Option[BatchResult[TopicPartition, Option[Offset]]] = None,
      metadataFailure: Option[KuiError] = None
  ): IO[(kui.consumer.domain.GroupAdminPort[IO], FakeAdmin)] =
    for {
      listing <- Ref.of[IO, Either[KuiError, GroupListingResult]](
        Right(
          GroupListingResult.complete(
            List(GroupListing(orders, isSimple = false, GroupState.Stable, GroupProtocol.Classic))
          )
        )
      )
      describedRef <- Ref.of[IO, Either[KuiError, BatchResult[GroupId, GroupDescription]]](described)
      committedRef <- Ref.of[IO, Either[KuiError, BatchResult[GroupId, List[CommittedOffset]]]](committed)
      altered <- Ref.of[IO, List[(GroupId, Map[TopicPartition, Offset])]](Nil)
      deleteSkips <- Ref.of[IO, Map[GroupId, SkipReason]](Map.empty)
      stability <- Ref.of[IO, List[Boolean]](Nil)
      admin = new FakeAdmin(listing, describedRef, committedRef, altered, deleteSkips, stability)
      lookup = new FakeOffsets(
        ends.map(batch => BatchResult(batch.values, endsSkipped)),
        ends,
        offline,
        times,
        metadataFailure
      )
      logger <- FakeStructuredLogger[IO]
    } yield (KafkaGroupAdminPort.make[IO](admin, lookup, connection, logger), admin)

  test("a skipped commit lookup is unknown on reads and refused for reset windows") {
    for {
      rigged <- rig(committed =
        Right(BatchResult(Map.empty, Map(orders -> SkipReason.NotAuthorized("commit denied"))))
      )
      (port, _) = rigged
      described <- port.describe(List(orders))
      window <- port.offsetWindow(orders, ResetScope(topic, Set(partition(0))), None)
    } yield {
      assert(!described.getOrElse(fail("describe"))(orders).completeness.committedOffsetsKnown)
      assertEquals(window.left.toOption.map(_.message), Some("commit denied"))
    }
  }

  test("a skipped group description retains its failure rather than disappearing") {
    for {
      rigged <- rig(described =
        Right(BatchResult(Map.empty, Map(orders -> SkipReason.NotAuthorized("describe denied"))))
      )
      (port, _) = rigged
      result <- port.describe(List(orders))
    } yield assertEquals(result.left.toOption.map(_.message), Some("describe denied"))
  }

  test("failed timestamp lookups are not absent timestamps; explicit absence remains valid") {
    for {
      failed <- rig(times =
        Some(BatchResult(Map.empty, Map(partition(0) -> SkipReason.NotAuthorized("timestamp denied"))))
      )
      refused <- failed._1.offsetWindow(orders, ResetScope(topic, Set(partition(0))), Some(Instant.EPOCH))
      absent <- rig(times = Some(BatchResult.complete(Map(partition(0) -> None))))
      accepted <- absent._1.offsetWindow(orders, ResetScope(topic, Set(partition(0))), Some(Instant.EPOCH))
      missing <- rig(times = Some(BatchResult.empty))
      incomplete <- missing._1.offsetWindow(orders, ResetScope(topic, Set(partition(0))), Some(Instant.EPOCH))
    } yield {
      assertEquals(refused.left.toOption.map(_.message), Some("timestamp denied"))
      assertEquals(accepted.map(_.atTimestamp), Right(Map(partition(0) -> None)))
      assert(incomplete.isLeft)
    }
  }

  test("missing bounds and commit results cannot create a reset window") {
    for {
      missingBounds <- rig(ends = Right(BatchResult.empty))
      bounds <- missingBounds._1.offsetWindow(orders, ResetScope(topic, Set(partition(0))), None)
      missingCommit <- rig(committed = Right(BatchResult.empty))
      commit <- missingCommit._1.offsetWindow(orders, ResetScope(topic, Set(partition(0))), None)
      noCommit <- rig(committed = Right(BatchResult.complete(Map(orders -> Nil))))
      absent <- noCommit._1.offsetWindow(orders, ResetScope(topic, Set(partition(0))), None)
    } yield {
      assert(bounds.isLeft)
      assert(commit.isLeft)
      assertEquals(absent.map(_.committed), Right(Map.empty[TopicPartition, Offset]))
    }
  }

  test("a relevant skipped bound refuses a reset window even if a value is also present") {
    for {
      rigged <- rig(endsSkipped = Map(partition(0) -> SkipReason.NotAuthorized("end denied")))
      result <- rigged._1.offsetWindow(orders, ResetScope(topic, Set(partition(0))), None)
    } yield assertEquals(result.left.toOption.map(_.message), Some("end denied"))
  }

  test("a failed metadata lookup is not a topic without leaderless partitions") {
    val error = InfrastructureError.Unreachable("kafka", "metadata unavailable")
    for {
      rigged <- rig(metadataFailure = Some(error))
      result <- rigged._1.offsetWindow(orders, ResetScope(topic, Set(partition(0))), None)
    } yield assertEquals(result, Left(error))
  }

  test("a described group carries its members, its commits and the lag between them") {
    for {
      rigged <- rig()
      (port, _) = rigged
      described <- port.describe(List(orders))
    } yield described match {
      case Right(groups) =>
        val group = groups(orders)
        assertEquals(group.members.map(_.memberId), List("m-1"))
        assertEquals(group.subscriptions.map(_.topic), List(topic))
        assertEquals(group.lagTotal.value, Some(10L))
        assert(group.completeness.isComplete)
      case Left(error) => fail(s"describe failed: $error")
    }
  }

  test("a partition a member holds but has never committed on still has a row, with NO_COMMIT") {
    for {
      rigged <- rig()
      (port, _) = rigged
      described <- port.describe(List(orders))
    } yield {
      val states = described.map(_(orders).partitions).getOrElse(Nil)
      assertEquals(states.size, 2)
      val uncommitted = states.find(_.partition.value == 1).getOrElse(fail("partition 1 is missing"))
      assertEquals(uncommitted.lag.anomalies, Set(LagAnomaly.NoCommit))
      assertEquals(uncommitted.lag.value, None)
    }
  }

  test("a leaderless partition is NO_LEADER and contributes nothing, and the group says it was excluded") {
    for {
      rigged <- rig(endsSkipped = Map(partition(0) -> SkipReason.NoLeader))
      (port, _) = rigged
      described <- port.describe(List(orders))
    } yield {
      val group = described.getOrElse(fail("describe failed"))(orders)
      val offline = group.partitions.find(_.partition.value == 0).getOrElse(fail("partition 0 is missing"))

      assertEquals(offline.lag.anomalies, Set(LagAnomaly.NoLeader))
      assertEquals(group.lagTotal.value, None)
      assert(group.completeness.excludedPartitions.contains(partition(0)))
    }
  }

  test("committed offsets KUI may not read degrade the page rather than failing it") {
    for {
      rigged <- rig(committed = Left(kui.kernel.error.ApplicationError.Forbidden("no READ on the topic")))
      (port, _) = rigged
      described <- port.describe(List(orders))
    } yield {
      val group = described.getOrElse(fail("describe failed"))(orders)
      assert(!group.completeness.committedOffsetsKnown)
      assertEquals(group.members.size, 1, clue = "the members were lost with the offsets")
    }
  }

  test("log ends KUI cannot read leave every lag undefined, and say so") {
    for {
      rigged <- rig(ends = Left(InfrastructureError.Unreachable("kafka", "no leader anywhere")))
      (port, _) = rigged
      described <- port.describe(List(orders))
    } yield {
      val group = described.getOrElse(fail("describe failed"))(orders)
      assert(!group.completeness.endOffsetsKnown)
      assertEquals(group.lagTotal.value, None)
    }
  }

  test("existence is answered from the listing, so a group that is not listed does not exist") {
    for {
      rigged <- rig()
      (port, admin) = rigged
      present <- port.exists(orders)
      _ <- admin.listing.set(Right(GroupListingResult.complete(Nil)))
      absent <- port.exists(orders)
    } yield {
      assertEquals(present, Right(true))
      assertEquals(absent, Right(false))
    }
  }

  test("a fabricated dead group still exists as a describe, and still does not exist as a listing") {
    for {
      rigged <- rig(described = Right(BatchResult.complete(Map(orders -> GroupDescription.dead(orders)))))
      (port, admin) = rigged
      _ <- admin.listing.set(Right(GroupListingResult.complete(Nil)))
      described <- port.describe(List(orders))
      exists <- port.exists(orders)
    } yield {
      // This pair is the whole reason existence is checked by listing: the describe is perfectly
      // happy to answer for a group that is not there.
      assertEquals(described.map(_(orders).state), Right(GroupState.Dead))
      assertEquals(exists, Right(false))
    }
  }

  test("an offset window gathers begin, end, committed and the offline set in one pass") {
    for {
      rigged <- rig(offline = Set(partition(1)))
      (port, _) = rigged
      window <- port.offsetWindow(orders, ResetScope(topic, Set(partition(0), partition(1))), None)
    } yield window match {
      case Right(OffsetWindow(begin, end, committed, _, leaderless)) =>
        assertEquals(end.get(partition(0)).map(_.value), Some(100L))
        assertEquals(begin.get(partition(0)).map(_.value), Some(100L))
        assertEquals(committed.get(partition(0)).map(_.value), Some(90L))
        assertEquals(leaderless, Set(partition(1)))
      case Left(error) => fail(s"the window failed: $error")
    }
  }

  test("a timestamp window resolves offsets only for the partitions that have a leader") {
    for {
      rigged <- rig(offline = Set(partition(1)))
      (port, _) = rigged
      window <- port.offsetWindow(
        orders,
        ResetScope(topic, Set(partition(0), partition(1))),
        Some(Instant.parse("2026-01-01T00:00:00Z"))
      )
    } yield assertEquals(window.map(_.atTimestamp.keySet), Right(Set(partition(0))))
  }

  test("applying offsets passes them through unchanged") {
    for {
      rigged <- rig()
      (port, admin) = rigged
      _ <- port.applyOffsets(orders, Map(partition(0) -> Offset.unsafe(5L)))
      written <- admin.altered.get
    } yield assertEquals(
      written.map((g, offsets) => g -> offsets.values.map(_.value).toList),
      List(orders -> List(5L))
    )
  }

  test("a group the broker refused to delete is a failure, and never an empty success") {
    /*
     * Ungated until now: answering `Right(())` for a per-group skip left
     * `./mill services.consumer.__.test` at 201/201 green -- `FakeAdmin.deleteGroups` could only ever
     * answer a complete success, so the branch that reads the skip had no input that reached it. Forget
     * group is a destructive operation behind ADR-045's confirmation, and a refusal reported as success
     * tells an operator the group is gone while it is still there with its offsets intact. The screen has
     * no second way to find out.
     */
    for {
      rigged <- rig()
      (port, admin) = rigged
      _ <- admin.deletions.set(Map(orders -> SkipReason.NotAuthorized("DELETE on group orders-consumer")))
      refused <- port.deleteGroup(orders)
      _ <- admin.deletions.set(Map.empty)
      accepted <- port.deleteGroup(orders)
    } yield {
      assertEquals(refused.isLeft, true)
      assertEquals(refused.left.toOption.map(_.code), Some(kui.kernel.error.ErrorCode.Forbidden))
      // The other direction, so the case cannot pass by refusing everything.
      assertEquals(accepted, Right(()))
    }
  }

  test("a reset is planned against stable offsets, and a page is not made to wait for one") {
    /*
     * Ungated until now: swapping the two `requireStable` arguments in `KafkaGroupAdminPort` left
     * `./mill services.consumer.infrastructure.test` at 16/16 green, because `FakeAdmin.committedOffsets`
     * dropped the flag on the floor. The two call sites want opposite things and both matter. A plan is
     * signed into a token and applied minutes later (ADR-045), so it must not be computed from an offset
     * an open transaction can still roll back; the list and detail pages, by contrast, must not block
     * behind a producer's in-flight transaction to draw a lag column.
     */
    for {
      rigged <- rig()
      (port, admin) = rigged
      _ <- port.describe(List(orders))
      afterRead <- admin.stability.get
      _ <- admin.stability.set(Nil)
      _ <- port.offsetWindow(orders, ResetScope(topic, Set(partition(0))), None)
      afterPlan <- admin.stability.get
    } yield {
      assertEquals(afterRead, List(false), clue = "a read path asked for stable offsets and would block")
      assertEquals(afterPlan, List(true), clue = "a reset was planned from offsets a transaction can undo")
    }
  }

  test("a row built from a listing alone says what it does not know, rather than reporting zeros") {
    /*
     * Ungated until now: `completeness = GroupCompleteness.Complete` on a listing-derived summary left the
     * suite at 201/201 green. A listing knows a group exists and nothing else, so its `memberCount`,
     * `topicCount` and `partitionCount` are placeholders; marked complete, the list screen draws them as
     * measurements and a running group reads as a group with no members and no lag. `GroupCompleteness`
     * exists precisely so that "nobody is running this" and "KUI did not ask" cannot render the same.
     */
    for {
      rigged <- rig()
      (port, _) = rigged
      listed <- port.list(Set.empty)
    } yield {
      val summary = listed.toOption.flatMap(_.groups.headOption).getOrElse(fail("the listing must answer"))

      assertEquals(summary.groupId, orders)
      assertEquals(summary.completeness.isComplete, false)
      assertEquals(summary.completeness.membersKnown, false)
      assertEquals(summary.completeness.committedOffsetsKnown, false)
      assertEquals(summary.completeness.endOffsetsKnown, false)
      // The three figures the flags are about, so the reason they must not be trusted is on the page.
      assertEquals((summary.memberCount, summary.topicCount, summary.partitionCount), (0, 0, 0))
    }
  }
}
