package kui.consumer.domain

import java.time.Instant

import kui.kernel.group.{GroupProtocol, GroupState}
import kui.kernel.{GroupId, TopicName}

/** One topic a group consumes, with its partitions.
  *
  * A `TopicSubscription` rather than a bare `TopicName` because the detail page's rows are topics and its
  * expanded rows are partitions (`research/kafbat/ui-analysis.md`). A shape that already matches the screen
  * needs no reshaping in the API layer, and reshaping in the API layer is where a total gets recomputed by a
  * second rule that disagrees with the first.
  */
final case class TopicSubscription(topic: TopicName, partitions: List[PartitionState]) {

  /** The lag of this topic, summing only the partitions that have one. */
  def totalLag: Option[Long] = LagMath.total(partitions.map(_.lag)).value

  def excludedPartitions: Int = LagMath.total(partitions.map(_.lag)).excluded
}

object TopicSubscription {
  given CanEqual[TopicSubscription, TopicSubscription] = CanEqual.derived
  given Ordering[TopicSubscription] = Ordering.by(_.topic.value)
}

/** The list-page row.
  *
  * Everything on it is derivable from the snapshot with no further call, which is the property that keeps the
  * group list cheap on a cluster with four thousand groups.
  */
final case class GroupSummary(
    groupId: GroupId,
    state: GroupState,
    protocol: GroupProtocol,
    isSimple: Boolean,
    memberCount: Int,
    topicCount: Int,
    partitionCount: Int,
    coordinator: Option[GroupCoordinatorRef],
    /** `None` when no partition of this group has a computable lag — never `Some(0)`. */
    totalLag: Option[Long],
    /** How many partitions `totalLag` leaves out because they have no computable lag: no commit, a commit
      * past the log end, or a log end KUI could not read. `LagMath.total`'s count, the one the detail page
      * prints, so the list cannot call a total complete that the detail page says is not.
      */
    excludedPartitions: Int,
    /** Committed-offset movement per second between the last two snapshot passes. */
    pace: Option[Double],
    completeness: GroupCompleteness
)

object GroupSummary {
  given CanEqual[GroupSummary, GroupSummary] = CanEqual.derived
}

/** The detail-page aggregate: one group, on one cluster.
  *
  * The aggregate boundary is deliberately that narrow. It does not hold the cluster, the topics'
  * configuration or the broker set — those belong to other contexts, and a group that held them could not be
  * refreshed without refreshing them too, which is how a thirty-second group refresh turns into a
  * cluster-wide scrape.
  */
final case class ConsumerGroup(
    groupId: GroupId,
    state: GroupState,
    protocol: GroupProtocol,
    isSimple: Boolean,
    partitionAssignor: String,
    members: List[GroupMember],
    coordinator: Option[GroupCoordinatorRef],
    subscriptions: List[TopicSubscription],
    completeness: GroupCompleteness,
    observedAt: Instant
) {

  def partitions: List[PartitionState] = subscriptions.flatMap(_.partitions)

  def lagTotal: LagMath.LagTotal = LagMath.total(partitions.map(_.lag))

  def summary: GroupSummary =
    GroupSummary(
      groupId = groupId,
      state = state,
      protocol = protocol,
      isSimple = isSimple,
      memberCount = members.size,
      topicCount = subscriptions.size,
      partitionCount = partitions.size,
      coordinator = coordinator,
      totalLag = lagTotal.value,
      excludedPartitions = lagTotal.excluded,
      // A summary built from one observation cannot know a rate; the snapshot fills this in from
      // the previous pass (`GroupSnapshots`), which is the only place both observations exist.
      pace = None,
      completeness = completeness
    )

  /** The group is moving: either the broker says so, or some member is not yet where it is being sent. */
  def isRebalancing: Boolean =
    state == GroupState.PreparingRebalance ||
      state == GroupState.CompletingRebalance ||
      members.exists(_.isRebalancing)

  /** Whether an offset operation is allowed right now.
    *
    * Both halves, never one (DEVPLAN §10 D4), and this is the single place the domain expresses that rule.
    * Kafbat checks the state; Kouncil checks the member list; they disagree about a group that reports
    * `Empty` while a member is joining, and the union is the only safe reading of the two.
    */
  def permitsOffsetChange: Boolean =
    GroupState.permitsOffsetChange(state) && members.isEmpty

  /** Why an offset operation is refused, in the words an operator can act on. `None` when it is allowed. */
  def offsetChangeRefusal: Option[String] =
    if permitsOffsetChange then None
    else if members.nonEmpty then
      Some(
        s"the group still has ${members.size} member(s); stop its consumers, wait for the group to " +
          "become empty, and try again"
      )
    else Some(s"the group is ${state.wire}, and offsets can only be changed while it is EMPTY or DEAD")
}

object ConsumerGroup {
  given CanEqual[ConsumerGroup, ConsumerGroup] = CanEqual.derived
}
