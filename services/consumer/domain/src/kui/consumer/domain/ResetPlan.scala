package kui.consumer.domain

import java.time.Instant

import scala.concurrent.duration.FiniteDuration

import kui.kernel.group.ResetTarget
import kui.kernel.{GroupId, Offset, TopicName, TopicPartition}

/** What the operator asked for: one case per `ResetTarget`, carrying that mode's parameters.
  *
  * The scope — which partitions of which topic — is a separate value, because every mode applies to any
  * scope. Pairing them would give twelve cases where six plus one will do.
  */
enum ResetSpec {
  case ToEarliest
  case ToLatest
  case ToTimestamp(at: Instant)
  case ToOffsets(offsets: Map[TopicPartition, Offset])

  /** May be negative: shifting back is the point of it. */
  case ShiftBy(records: Long)

  /** "Rewind two hours": resolved to `ToTimestamp(now - back)` by the planner, at plan time. */
  case ByDuration(back: FiniteDuration)

  def target: ResetTarget = this match {
    case ToEarliest => ResetTarget.Earliest
    case ToLatest => ResetTarget.Latest
    case ToTimestamp(_) => ResetTarget.Timestamp
    case ToOffsets(_) => ResetTarget.Offset
    case ShiftBy(_) => ResetTarget.ShiftBy
    case ByDuration(_) => ResetTarget.Duration
  }
}

object ResetSpec {
  given CanEqual[ResetSpec, ResetSpec] = CanEqual.derived
}

/** Which partitions of which topic a reset covers. */
final case class ResetScope(topic: TopicName, partitions: Set[TopicPartition])

object ResetScope {
  given CanEqual[ResetScope, ResetScope] = CanEqual.derived
}

/** Everything the planner needs to know about the cluster, gathered by the caller.
  *
  * One record rather than five arguments, because the caller must gather all of it in a single pass: a
  * signature that lets the committed offsets come from one scrape and the end offsets from another is a
  * signature that invites exactly that, and a plan built from two moments is a plan for a cluster that never
  * existed (DEVPLAN §10 D7).
  */
final case class OffsetWindow(
    begin: Map[TopicPartition, Offset],
    end: Map[TopicPartition, Offset],
    committed: Map[TopicPartition, Offset],
    /** For `ToTimestamp`: the resolved offset per partition, `None` where no record is at or after it. */
    atTimestamp: Map[TopicPartition, Option[Offset]],
    leaderless: Set[TopicPartition]
)

object OffsetWindow {
  val Empty: OffsetWindow = OffsetWindow(Map.empty, Map.empty, Map.empty, Map.empty, Set.empty)
  given CanEqual[OffsetWindow, OffsetWindow] = CanEqual.derived
}

/** Something an operator has to be told about a plan before confirming it. */
enum ResetWarning(val message: String) {

  /** The offset asked for was outside `[begin, end]` and was moved to the nearest end.
    *
    * Reported, never applied silently. Kouncil applies no clamp at all and Kafbat applies one without saying
    * so; an operator who typed 900 000 into a partition that holds 400 records has to see what will actually
    * be written.
    */
  case Clamped(partition: TopicPartition, requested: Offset, applied: Offset)
      extends ResetWarning(
        s"partition ${partition.partition.value}: ${requested.value} is outside the range this partition " +
          s"holds, so ${applied.value} will be written instead"
      )

  /** No record at or after the requested time. KIP-122 says the end of the partition — which means "skip
    * everything that is there now", and is the single most surprising rule in the feature.
    */
  case TimestampBeyondEnd(partition: TopicPartition, applied: Offset)
      extends ResetWarning(
        s"partition ${partition.partition.value}: no record at or after that time, so the group will be " +
          s"moved to the end of the partition (${applied.value}) and will skip everything in it"
      )

  case NoChange(partition: TopicPartition)
      extends ResetWarning(
        s"partition ${partition.partition.value} is already at that offset; nothing will change"
      )

  /** `ShiftBy` had no committed offset to shift from, so it counted from the beginning of the log. */
  case ShiftedFromBeginning(partition: TopicPartition)
      extends ResetWarning(
        s"partition ${partition.partition.value} has no committed offset, so the shift was counted from " +
          "the beginning of the log"
      )
}

object ResetWarning {
  given CanEqual[ResetWarning, ResetWarning] = CanEqual.derived
}

/** Why a reset cannot be planned at all.
  *
  * A refusal, never a partial plan: a reset that silently skipped a partition leaves a group in a state
  * nobody asked for, and the operator finds out when one partition replays and the others do not.
  */
enum ResetRefusal(val message: String) {

  case Leaderless(partitions: Set[TopicPartition])
      extends ResetRefusal(
        s"${partitions.size} partition(s) have no leader " +
          s"(${partitions.toList.map(_.partition.value).sorted.mkString(", ")}), so their offsets cannot " +
          "be read or written; the whole reset is refused rather than applied to part of the group"
      )

  case NoPartitionsInScope
      extends ResetRefusal("the reset names no partitions, so there is nothing to change")

  case UnknownPartition(partitions: Set[TopicPartition])
      extends ResetRefusal(
        s"${partitions.size} partition(s) in the request are not on the cluster; the topic may have been " +
          "deleted or shrunk since the form was opened"
      )

  case NegativeResult(partition: TopicPartition)
      extends ResetRefusal(
        s"partition ${partition.partition.value} would be moved to a negative offset, which is not an offset"
      )
}

object ResetRefusal {
  given CanEqual[ResetRefusal, ResetRefusal] = CanEqual.derived
}

/** One partition of a plan: where it is, where it would go, and by how much. */
final case class PlannedPartition(
    partition: TopicPartition,
    current: Option[Offset],
    proposed: Offset,
    /** `proposed - current`, `None` when there is no current one. Rendered as "+412" / "-9 001". */
    delta: Option[Long]
)

object PlannedPartition {
  given CanEqual[PlannedPartition, PlannedPartition] = CanEqual.derived
  given Ordering[PlannedPartition] = Ordering.by(_.partition)
}

/** What a reset would actually write.
  *
  * This is the document the operator confirms. A form submission carries what the operator typed; it does not
  * carry what the cluster will do with it, and the number that matters — the offset that will be written
  * after clamping and after KIP-122's timestamp rule — can only be computed on the server (ADR-045).
  */
final case class ResetPlan(
    group: GroupId,
    scope: ResetScope,
    spec: ResetSpec,
    partitions: List[PlannedPartition],
    warnings: List[ResetWarning],
    computedAt: Instant
) {

  def isNoOp: Boolean = partitions.forall(planned => planned.current.contains(planned.proposed))

  def offsets: Map[TopicPartition, Offset] =
    partitions.map(planned => planned.partition -> planned.proposed).toMap

  /** The same plan, with each partition's `current` — and therefore its `delta` — taken from a reading of the
    * group's committed offsets immediately before the write.
    *
    * The plan token already carries those commits, so apply can refuse when they have moved. The receipt
    * still copies this reading rather than trusting the token alone: it is the reading the audit record uses,
    * and the two answers have to name the same offsets. A partition the reading did not cover keeps its
    * `None`, because "nobody knows" is a true statement and `0` is not.
    */
  def withCurrent(committed: Map[TopicPartition, Offset]): ResetPlan =
    copy(partitions = partitions.map { planned =>
      committed.get(planned.partition) match {
        case None => planned
        case Some(current) =>
          planned.copy(current = Some(current), delta = Some(planned.proposed.value - current.value))
      }
    })
}

object ResetPlan {
  given CanEqual[ResetPlan, ResetPlan] = CanEqual.derived
}
