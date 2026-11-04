package kui.consumer.api

import java.time.Instant

import munit.FunSuite

import kui.consumer.application.{AssignmentFreshness, GroupDetailView, SnapshotFreshness}
import kui.consumer.domain.{
  ConsumerGroup,
  GroupCompleteness,
  GroupCoordinatorRef,
  GroupSummary,
  LagMath,
  PartitionState,
  TopicSubscription
}
import kui.kernel.group.{GroupProtocol, GroupState}
import kui.kernel.{BrokerId, GroupId, Offset, PartitionId, TopicName}

/** That the coordinator reaches the wire as an address, and never as half of one.
  *
  * The domain has carried `host` and `port` since it was written; the mapping kept only the id, so the
  * consumer-groups table printed `broker 1` where `SCREENS-V4.md` §4.12 prints `broker-1:9092`. The
  * interesting case is not the happy one — it is the group that only ever appeared in a listing, whose
  * `DESCRIBE` was skipped or refused (`KafkaGroupAdminPort.summaryOf`). That group has no coordinator at all,
  * and the three fields have to go missing together: a host beside an id with no port is an address a screen
  * would render as `broker-1:undefined`.
  */
final class ConsumerMappingSuite extends FunSuite {

  private val At: Instant = Instant.parse("2026-09-06T10:00:00Z")

  private val coordinator: GroupCoordinatorRef =
    GroupCoordinatorRef(BrokerId.unsafe(1), "broker-1.kafka.svc", 9092)

  private def summary(ref: Option[GroupCoordinatorRef]): GroupSummary =
    GroupSummary(
      groupId = GroupId.unsafe("orders-indexer"),
      state = GroupState.Stable,
      protocol = GroupProtocol.Consumer,
      isSimple = false,
      memberCount = 3,
      topicCount = 1,
      partitionCount = 12,
      coordinator = ref,
      totalLag = Some(1240L),
      excludedPartitions = 0,
      pace = None,
      completeness = GroupCompleteness.Complete
    )

  private def detailView(ref: Option[GroupCoordinatorRef]): GroupDetailView =
    GroupDetailView(
      group = ConsumerGroup(
        groupId = GroupId.unsafe("orders-indexer"),
        state = GroupState.Stable,
        protocol = GroupProtocol.Consumer,
        isSimple = false,
        partitionAssignor = "cooperative-sticky",
        members = Nil,
        coordinator = ref,
        subscriptions = Nil,
        completeness = GroupCompleteness.Complete,
        observedAt = At
      ),
      topics = Nil,
      total = LagMath.LagTotal.Empty,
      assignments = AssignmentFreshness.Current,
      freshness = SnapshotFreshness.Fresh(At),
      computedAt = At
    )

  test("aDescribedGroupCarriesTheCoordinatorsWholeAddressOntoTheListRow") {
    val row = ConsumerMapping.summary(summary(Some(coordinator)))

    assertEquals(row.coordinatorId, Some(1))
    assertEquals(row.coordinatorHost, Some("broker-1.kafka.svc"))
    assertEquals(row.coordinatorPort, Some(9092))
  }

  test("anUndescribableGroupHasAllThreeCoordinatorFieldsAbsentTogether") {
    // The row a listing alone produces. `null` for all three, never a host with no port, and never a
    // fabricated `0` for the port — a port of zero is a valid number and would be read as one.
    val row = ConsumerMapping.summary(summary(None))

    assertEquals(row.coordinatorId, None)
    assertEquals(row.coordinatorHost, None)
    assertEquals(row.coordinatorPort, None)
  }

  test("theDetailPageSpellsTheCoordinatorTheSameWayTheListDoes") {
    // Two mappings, one fact. They are asserted separately because they are separate call sites, and the
    // defect this suite exists for was exactly one call site dropping fields the other kept.
    val described = ConsumerMapping.detail(detailView(Some(coordinator)))
    val undescribed = ConsumerMapping.detail(detailView(None))

    assertEquals(described.coordinatorId, Some(1))
    assertEquals(described.coordinatorHost, Some("broker-1.kafka.svc"))
    assertEquals(described.coordinatorPort, Some(9092))

    assertEquals(undescribed.coordinatorId, None)
    assertEquals(undescribed.coordinatorHost, None)
    assertEquals(undescribed.coordinatorPort, None)
  }

  test("theListRowCountsThePartitionsItsTotalLeavesOutTheWayTheDetailPageDoes") {
    // P0 is committed at the log end; P1 is held by a member that has never committed. The total is
    // the 0 of P0 alone, and the list row said nothing was excluded from it — so "never run" read as
    // "caught up" on the list while the detail page said one partition could not be counted.
    def state(n: Int, committed: Option[Long]): PartitionState =
      PartitionState(
        partition = PartitionId.unsafe(n),
        committed = committed.map(Offset.unsafe),
        begin = Some(Offset.unsafe(0L)),
        end = Some(Offset.unsafe(500L)),
        memberId = Some("m-1"),
        host = Some("10.0.0.7"),
        lag = LagMath.lagOf(committed.map(Offset.unsafe), Some(Offset.unsafe(0L)), Some(Offset.unsafe(500L)))
      )

    val group = detailView(Some(coordinator)).group.copy(
      subscriptions = List(
        TopicSubscription(TopicName.unsafe("orders"), List(state(0, Some(500L)), state(1, None)))
      )
    )
    val view = detailView(Some(coordinator)).copy(group = group, total = group.lagTotal)

    val row = ConsumerMapping.summary(group.summary)

    assertEquals(row.totalLag, Some(0L))
    assertEquals(
      row.excludedPartitions,
      ConsumerMapping.detail(view).excludedPartitions,
      clue = "the list and the detail page disagree about how much of the group the total covers"
    )
    assertEquals(row.excludedPartitions, 1)
  }
}
