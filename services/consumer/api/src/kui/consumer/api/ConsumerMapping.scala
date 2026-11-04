package kui.consumer.api

import kui.consumer.application.{
  AssignmentFreshness,
  GroupDetailView,
  GroupSortField as AppSortField,
  LagUpdate,
  PlannedReset,
  SnapshotFreshness,
  TopicConsumersView
}
import kui.consumer.contract.dto.*
import kui.consumer.domain.*
import kui.contracts.consumer.{
  AssignmentFreshness as WireAssignmentFreshness,
  AssignmentFreshnessDto,
  GroupSortField as WireSortField,
  GroupSummaryDto,
  IncompleteDto,
  MemberDto,
  PartitionDto,
  StaleDto,
  TopicConsumerRowDto,
  TopicConsumersDto,
  TopicSubscriptionDto
}
import kui.contracts.paging.PageDto
import kui.kernel.GroupId

/** Application types to wire types, and nothing else (ADR-033).
  *
  * Nothing here computes. Every number on the wire — a total lag, an excluded-partition count, a delta — was
  * computed once in `services/consumer/domain`, where the anomaly rules live, and is copied across. A mapping
  * that re-derived a total would be a second rule that can disagree with the first, which is the defect
  * `LagMath` exists to make impossible.
  *
  * The two `GroupSortField` enums meet here, and only here. They are two enums on purpose: one is the wire
  * vocabulary in `libs/contracts-core` (build rule A14), the other is the application's own, and rule A3
  * forbids the application module the import that would let them be one type.
  */
object ConsumerMapping {

  // ------------------------------------------------------------------ the list

  def sortField(field: WireSortField): AppSortField = field match {
    case WireSortField.Id => AppSortField.Id
    case WireSortField.Members => AppSortField.Members
    case WireSortField.Topics => AppSortField.Topics
    case WireSortField.Lag => AppSortField.Lag
    case WireSortField.State => AppSortField.State
  }

  /** Which parts of a row could not be read. `None` — rendered as `null` — is the ordinary case, so a browser
    * can treat any value at all as "show the warning".
    */
  def incomplete(completeness: GroupCompleteness): Option[IncompleteDto] =
    Option.when(!completeness.isComplete)(
      IncompleteDto(
        membersKnown = completeness.membersKnown,
        offsetsKnown = completeness.committedOffsetsKnown,
        endOffsetsKnown = completeness.endOffsetsKnown,
        note = note(completeness)
      )
    )

  /** The operator-facing sentence, written here because this is the layer that knows the reader is a person.
    *
    * The browser renders it as given rather than reconstructing a sentence from three booleans, so that a
    * partial answer explains itself rather than arriving as flags to be interpreted.
    */
  private def note(completeness: GroupCompleteness): String = {
    val missing = List(
      Option.unless(completeness.membersKnown)("its members"),
      Option.unless(completeness.committedOffsetsKnown)("its committed offsets"),
      Option.unless(completeness.endOffsetsKnown)("the log end offsets its lag is measured against")
    ).flatten

    val excluded =
      Option.when(completeness.excludedPartitions.nonEmpty)(
        s"${completeness.excludedPartitions.size} partition(s) were excluded from the lag total"
      )

    (missing.headOption, excluded) match {
      case (Some(_), _) =>
        s"KUI could not read ${missing.mkString(", ")} for this group" +
          excluded.fold("")(reason => s"; $reason")
      case (None, Some(reason)) =>
        s"This group was read in full, but $reason"
      case (None, None) => "This group was read in full"
    }
  }

  /** The list row.
    *
    * The three coordinator fields are each a `map` over the same `Option`, which is what makes them absent
    * together: `SCREENS-V4.md` §4.12 prints `broker-1:9092`, and a host beside an id with no port would let a
    * screen render half an address. Splitting the ref here rather than joining it into one string keeps the
    * port a number for anything that wants to compare it.
    */
  def summary(row: GroupSummary): GroupSummaryDto =
    GroupSummaryDto(
      groupId = row.groupId,
      state = row.state,
      protocol = row.protocol,
      isSimple = row.isSimple,
      members = row.memberCount,
      topics = row.topicCount,
      partitions = row.partitionCount,
      coordinatorId = row.coordinator.map(_.id.value),
      coordinatorHost = row.coordinator.map(_.host),
      coordinatorPort = row.coordinator.map(_.port),
      totalLag = row.totalLag,
      pace = row.pace,
      excludedPartitions = row.excludedPartitions,
      incomplete = incomplete(row.completeness)
    )

  def page(page: kui.kernel.Page[GroupSummary]): GroupPageDto = PageDto.of(page)(summary)

  // ------------------------------------------------------------------ the detail page

  def partition(state: PartitionState): PartitionDto =
    PartitionDto(
      partition = state.partition.value,
      committed = state.committed.map(_.value),
      begin = state.begin.map(_.value),
      end = state.end.map(_.value),
      // Copied, never recomputed from `committed` and `end`: a reader that subtracted would produce a
      // negative number for a commit past the end of the log, which `LagMath` reports as unknown.
      lag = state.lag.value,
      anomalies = state.lag.anomalies.toList.sortBy(_.toString),
      memberId = state.memberId,
      host = state.host
    )

  def subscription(topic: TopicSubscription): TopicSubscriptionDto =
    TopicSubscriptionDto(
      topic = topic.topic,
      lag = topic.totalLag,
      excludedPartitions = topic.excludedPartitions,
      partitions = topic.partitions.sorted.map(partition)
    )

  def member(member: GroupMember): MemberDto =
    MemberDto(
      memberId = member.memberId,
      groupInstanceId = member.groupInstanceId,
      clientId = member.clientId,
      host = member.host,
      partitions = member.partitions.toList.map(tp => s"${tp.topic.value}-${tp.partition.value}").sorted,
      rebalancing = member.isRebalancing
    )

  def assignments(freshness: AssignmentFreshness): AssignmentFreshnessDto = freshness match {
    case AssignmentFreshness.Current => AssignmentFreshnessDto(WireAssignmentFreshness.Current, None)
    case AssignmentFreshness.LastSeen(at) =>
      AssignmentFreshnessDto(WireAssignmentFreshness.LastSeen, Some(at))
    case AssignmentFreshness.Unknown => AssignmentFreshnessDto(WireAssignmentFreshness.Unknown, None)
  }

  /** `null` in the ordinary case, so that "stale" is a value a screen can test for rather than a flag it has
    * to combine with a timestamp.
    */
  def stale(freshness: SnapshotFreshness): Option[StaleDto] = freshness match {
    case SnapshotFreshness.Fresh(_) => None
    case SnapshotFreshness.Stale(at, reason) => Some(StaleDto(at, ConsumerReasons.of(reason)))
    case SnapshotFreshness.Unavailable(reason) =>
      Some(StaleDto(java.time.Instant.EPOCH, ConsumerReasons.of(reason)))
  }

  def detail(view: GroupDetailView): GroupDetailDto =
    GroupDetailDto(
      groupId = view.group.groupId,
      state = view.group.state,
      protocol = view.group.protocol,
      isSimple = view.group.isSimple,
      partitionAssignor = view.group.partitionAssignor,
      coordinatorId = view.group.coordinator.map(_.id.value),
      coordinatorHost = view.group.coordinator.map(_.host),
      coordinatorPort = view.group.coordinator.map(_.port),
      members = view.group.members.sorted.map(member),
      topics = view.topics.sorted.map(subscription),
      totalLag = view.total.value,
      excludedPartitions = view.total.excluded,
      assignments = assignments(view.assignments),
      // Always present, fresh or not: "as of" is what makes a lag figure interpretable at all.
      observedAt = view.freshness.observedAt.getOrElse(view.computedAt),
      stale = stale(view.freshness)
    )

  // ------------------------------------------------------------------ the lag poll

  def lagUpdate(update: LagUpdate): LagUpdateDto =
    LagUpdateDto(
      groupId = update.groupId,
      totalLag = update.totalLag,
      pace = update.pace,
      state = update.state,
      members = update.memberCount
    )

  // ------------------------------------------------------------------ the topic page's Consumers tab

  def topicConsumers(
      view: TopicConsumersView,
      groupsOf: GroupId => Option[ConsumerGroup]
  ): TopicConsumersDto =
    TopicConsumersDto(
      view.groups.map { row =>
        val onThisTopic = groupsOf(row.groupId).flatMap(_.subscriptions.find(_.topic == view.topic))

        TopicConsumerRowDto(
          group = summary(row),
          // This group's lag *on this topic*, which is not its total lag whenever it consumes more
          // than one. Falling back to the total would put a plausible wrong number on the tab.
          topicLag = onThisTopic.flatMap(_.totalLag),
          partitions = onThisTopic.fold(0)(_.partitions.size),
          // Committed offsets and no members: a batch job between runs looks exactly like this, so it
          // is a badge and not a warning.
          dormant = row.memberCount == 0
        )
      }
    )

  // ------------------------------------------------------------------ the reset wizard

  def plannedPartition(planned: PlannedPartition): PlannedPartitionDto =
    PlannedPartitionDto(
      partition = planned.partition.partition.value,
      current = planned.current.map(_.value),
      proposed = planned.proposed.value,
      delta = planned.delta
    )

  def warning(warning: ResetWarning): ResetWarningDto = {
    val (kind, partition) = warning match {
      case ResetWarning.Clamped(tp, _, _) => ("CLAMPED", Some(tp.partition.value))
      case ResetWarning.TimestampBeyondEnd(tp, _) => ("TIMESTAMP_BEYOND_END", Some(tp.partition.value))
      case ResetWarning.NoChange(tp) => ("NO_CHANGE", Some(tp.partition.value))
      case ResetWarning.ShiftedFromBeginning(tp) => ("SHIFTED_FROM_BEGINNING", Some(tp.partition.value))
    }

    ResetWarningDto(kind, partition, warning.message)
  }

  def plan(planned: PlannedReset): ResetPlanDto = planDto(planned.plan, planned.token, planned.expiresAt)

  /** The applied plan, answered with the token it was applied with so that the wizard shows what was written
    * rather than re-resolving the request against a cluster that has since moved.
    */
  def appliedPlan(plan: ResetPlan, token: String, expiresAt: java.time.Instant): ResetPlanDto =
    planDto(plan, token, expiresAt)

  private def planDto(plan: ResetPlan, token: String, expiresAt: java.time.Instant): ResetPlanDto =
    ResetPlanDto(
      groupId = plan.group,
      topic = plan.scope.topic,
      target = plan.spec.target,
      partitions = plan.partitions.sorted.map(plannedPartition),
      warnings = plan.warnings.map(warning),
      noOp = plan.isNoOp,
      token = token,
      expiresAt = expiresAt
    )

  def deletedOffsets(deleted: kui.consumer.application.DeletedOffsets, group: GroupId): DeletedOffsetsDto =
    DeletedOffsetsDto(
      groupId = group,
      topic = deleted.topic,
      partitions = deleted.partitions.toList.map(_.partition.value).sorted
    )
}
