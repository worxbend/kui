package kui.consumer.contract.dto

import java.time.Instant

import io.circe.syntax.*
import io.circe.{Codec, Decoder, DecodingFailure, HCursor, Json}
import sttp.tapir.Schema

import kui.consumer.contract.dto.ConsumerCodecs.given
import kui.kernel.group.ResetTarget
import kui.kernel.{GroupId, TopicName}

/** What one partition's offset would become.
  *
  * @param current
  *   what is committed now, or `null` if the group has never committed for this partition. The wizard renders
  *   an em dash for it — never a `0`, which would tell an operator the consumer is at the beginning when in
  *   fact nobody knows where it is
  * @param proposed
  *   what will be written. Not an `Option`: a plan that could not resolve a partition is not a plan with a
  *   missing number in it, it is a refusal, and it arrives as an error instead
  * @param delta
  *   `proposed - current`, when there is a `current`. Precomputed on the server because it is the number the
  *   operator actually reads ("this rewinds 4 200 records") and a browser that computed it would be a second
  *   place for the arithmetic to be wrong
  */
final case class PlannedPartitionDto(
    partition: Int,
    current: Option[Long],
    proposed: Long,
    delta: Option[Long]
)

object PlannedPartitionDto {

  given Codec[PlannedPartitionDto] = Codec.from(
    (cursor: HCursor) =>
      for {
        partition <- cursor.get[Int]("partition")
        current <- cursor.get[Option[Long]]("current")
        proposed <- cursor.get[Long]("proposed")
        delta <- cursor.get[Option[Long]]("delta")
      } yield PlannedPartitionDto(partition, current, proposed, delta),
    (dto: PlannedPartitionDto) =>
      Json.obj(
        "partition" -> dto.partition.asJson,
        "current" -> dto.current.asJson,
        "proposed" -> dto.proposed.asJson,
        "delta" -> dto.delta.asJson
      )
  )

  given Schema[PlannedPartitionDto] = Schema
    .derived[PlannedPartitionDto]
    .description("What one partition's committed offset is now and what it would become")

  given CanEqual[PlannedPartitionDto, PlannedPartitionDto] = CanEqual.derived
}

/** Something the operator should know about the plan before applying it.
  *
  * A warning is not a refusal: the plan is still valid and still applicable. The case this type exists for is
  * clamping — an operator asks for offset 9 000 000 on a partition whose log ends at 412 000, and KIP-122
  * clamps it to 412 000. The reference implementation this project studied applies that clamp silently, which
  * means the operator's mental model and the cluster's state part company with nothing on screen to say so
  * (`research/kafka/admin-capabilities.md` §3, "a foot-gun").
  *
  * @param kind
  *   a stable machine-readable name so the wizard can style the row; the sentence to display is `message`
  */
final case class ResetWarningDto(kind: String, partition: Option[Int], message: String)

object ResetWarningDto {

  given Codec[ResetWarningDto] = Codec.from(
    (cursor: HCursor) =>
      for {
        kind <- cursor.get[String]("kind")
        partition <- cursor.get[Option[Int]]("partition")
        message <- cursor.get[String]("message")
      } yield ResetWarningDto(kind, partition, message),
    (dto: ResetWarningDto) =>
      Json.obj(
        "kind" -> dto.kind.asJson,
        "partition" -> dto.partition.asJson,
        "message" -> dto.message.asJson
      )
  )

  given Schema[ResetWarningDto] =
    Schema.derived[ResetWarningDto].description("Something the operator should see before applying the plan")

  given CanEqual[ResetWarningDto, ResetWarningDto] = CanEqual.derived
}

/** What a reset would do, computed by the server against live offsets.
  *
  * This document is the whole of ADR-045. The destructive request is the *second* one, and it carries only
  * `token` — so the offsets that get written are the offsets in this plan, the ones the operator read, and
  * not a re-resolution of the same specification against a cluster that has moved since. A form submission
  * carries what the operator typed; it does not carry what the cluster will actually do.
  *
  * @param token
  *   an HMAC over `(clusterId, groupId, the resolved offsets, expiry)` using the cursor-signing key of
  *   ADR-026 — no new secret and no new configuration. It is not a credential: it grants no authority beyond
  *   applying this one plan to this one group, and it expires. That is why it is on the explicit allow-list
  *   in the contract suite's `noDtoHasASecretField` check rather than tripping it
  * @param noOp
  *   every partition is already where the plan would put it. The wizard says so and offers nothing to
  *   confirm, because a confirmation dialogue for an operation that changes nothing teaches operators to
  *   click through confirmation dialogues
  * @param expiresAt
  *   five minutes out. After it, applying is `KUI-VALIDATION` and the wizard re-plans rather than guessing
  */
final case class ResetPlanDto(
    groupId: GroupId,
    topic: TopicName,
    target: ResetTarget,
    partitions: List[PlannedPartitionDto],
    warnings: List[ResetWarningDto],
    noOp: Boolean,
    token: String,
    expiresAt: Instant
)

object ResetPlanDto {

  given Codec[ResetPlanDto] = Codec.from(
    (cursor: HCursor) =>
      for {
        groupId <- cursor.get[GroupId]("groupId")
        topic <- cursor.get[TopicName]("topic")
        target <- cursor.get[ResetTarget]("target")
        partitions <- cursor.get[List[PlannedPartitionDto]]("partitions")
        warnings <- cursor.get[List[ResetWarningDto]]("warnings")
        noOp <- cursor.get[Boolean]("noOp")
        token <- cursor.get[String]("token")
        expiresAt <- cursor.get[Instant]("expiresAt")
      } yield ResetPlanDto(groupId, topic, target, partitions, warnings, noOp, token, expiresAt),
    (dto: ResetPlanDto) =>
      Json.obj(
        "groupId" -> dto.groupId.asJson,
        "topic" -> dto.topic.asJson,
        "target" -> dto.target.asJson,
        "partitions" -> dto.partitions.asJson,
        "warnings" -> dto.warnings.asJson,
        "noOp" -> dto.noOp.asJson,
        "token" -> dto.token.asJson,
        "expiresAt" -> dto.expiresAt.asJson
      )
  )

  given Schema[ResetPlanDto] = Schema
    .derived[ResetPlanDto]
    .description("Exactly what a reset would write, and the token that applies it unchanged")

  given CanEqual[ResetPlanDto, ResetPlanDto] = CanEqual.derived
}

/** Ask the server what a reset would do.
  *
  * The mode-specific parameter is validated **here, at decode time**, not by the planner: `TIMESTAMP` with no
  * timestamp is a malformed request, and answering it by defaulting to `now` would reset a consumer group to
  * a point in time nobody asked for. Each of the four parameterised modes requires its own field and refuses
  * the others', so a request cannot carry a timestamp *and* an offset map and leave the server to pick.
  *
  * @param partitions
  *   empty means every partition of the topic that the group holds an offset for. Empty rather than an
  *   `Option` because "all" is the ordinary case and `None` and `Some(Nil)` would mean the same thing while
  *   looking different
  * @param offsets
  *   keyed by partition number as a string, because a JSON object key is a string and pretending otherwise
  *   produces two encodings of the same map
  */
final case class ResetPlanRequest(
    topic: TopicName,
    partitions: List[Int],
    target: ResetTarget,
    timestamp: Option[Instant],
    offsets: Option[Map[String, Long]],
    shiftBy: Option[Long],
    durationMs: Option[Long]
)

object ResetPlanRequest {

  /** One hundred 365-day years, in milliseconds.
    *
    * Longer than a rewind anybody means, and short enough that `FiniteDuration` can represent it: the
    * constructor throws past about 9.22e12 ms, and that throw is a 500 on the plan route.
    */
  val MaxDurationMs: Long = 100L * 365 * 24 * 60 * 60 * 1000

  /** Kafka timestamps before the epoch are not a reset this API accepts. The far end is year 9999, which is
    * inside `Instant.toEpochMilli`; a later instant throws `ArithmeticException` and that throw is a 500 once
    * the group has already been accepted. A decoder has no clock, so the cap is not "now".
    */
  val MinTimestamp: Instant = Instant.EPOCH
  val MaxTimestamp: Instant = Instant.parse("9999-12-31T23:59:59Z")

  def durationInRange(ms: Long): Boolean = ms > 0 && ms <= MaxDurationMs

  def timestampInRange(at: Instant): Boolean = !at.isBefore(MinTimestamp) && !at.isAfter(MaxTimestamp)

  /** The parameter each mode requires, named for the message a rejected request carries. */
  private def requiredParameter(target: ResetTarget): Option[String] = target match {
    case ResetTarget.Timestamp => Some("timestamp")
    case ResetTarget.Offset => Some("offsets")
    case ResetTarget.ShiftBy => Some("shiftBy")
    case ResetTarget.Duration => Some("durationMs")
    case ResetTarget.Earliest | ResetTarget.Latest => None
  }

  private def parameterIsPresent(request: ResetPlanRequest, name: String): Boolean = name match {
    case "timestamp" => request.timestamp.isDefined
    case "offsets" => request.offsets.exists(_.nonEmpty)
    case "shiftBy" => request.shiftBy.isDefined
    case "durationMs" => request.durationMs.isDefined
    case _ => false
  }

  private val decoder: Decoder[ResetPlanRequest] = (cursor: HCursor) =>
    for {
      topic <- cursor.get[TopicName]("topic")
      partitions <- cursor.getOrElse[List[Int]]("partitions")(Nil)
      target <- cursor.get[ResetTarget]("target")
      timestamp <- cursor.get[Option[Instant]]("timestamp")
      offsets <- cursor.get[Option[Map[String, Long]]]("offsets")
      shiftBy <- cursor.get[Option[Long]]("shiftBy")
      durationMs <- cursor.get[Option[Long]]("durationMs")
      request = ResetPlanRequest(topic, partitions, target, timestamp, offsets, shiftBy, durationMs)
      checked <- requiredParameter(target) match {
        case Some(name) if !parameterIsPresent(request, name) =>
          Left(
            DecodingFailure(
              s"target '${target.wire}' requires '$name'; without it there is nothing to reset to",
              cursor.history
            )
          )
        case _ =>
          outOfRange(request) match {
            case Some(message) => Left(DecodingFailure(message, cursor.history))
            case None => Right(request)
          }
      }
    } yield checked

  /** Rejects the values that would throw while the route turned them into a `ResetSpec`. */
  private def outOfRange(request: ResetPlanRequest): Option[String] = request.target match {
    case ResetTarget.Duration if request.durationMs.exists(!durationInRange(_)) =>
      Some(s"durationMs must be between 1 and $MaxDurationMs milliseconds")
    case ResetTarget.Timestamp if request.timestamp.exists(!timestampInRange(_)) =>
      Some(s"timestamp must be between $MinTimestamp and $MaxTimestamp")
    case _ => None
  }

  given Codec[ResetPlanRequest] = Codec.from(
    decoder,
    (dto: ResetPlanRequest) =>
      Json.obj(
        "topic" -> dto.topic.asJson,
        "partitions" -> dto.partitions.asJson,
        "target" -> dto.target.asJson,
        "timestamp" -> dto.timestamp.asJson,
        "offsets" -> dto.offsets.asJson,
        "shiftBy" -> dto.shiftBy.asJson,
        "durationMs" -> dto.durationMs.asJson
      )
  )

  given Schema[ResetPlanRequest] = Schema
    .derived[ResetPlanRequest]
    .description(
      "Ask what a reset would do. TIMESTAMP needs timestamp, OFFSET needs offsets, " +
        "SHIFT_BY needs shiftBy, DURATION needs durationMs."
    )

  given CanEqual[ResetPlanRequest, ResetPlanRequest] = CanEqual.derived
}

/** Apply a plan. One field, and it is the token.
  *
  * There is no second path that takes a raw specification — not for tests, not for the MCP server of M8. A
  * body that also carries a specification decodes successfully and the specification is *ignored*, which is
  * asserted in the mutation-endpoint suite: a client cannot smuggle one past the plan.
  */
final case class ResetApplyRequest(token: String)

object ResetApplyRequest {

  given Codec[ResetApplyRequest] = Codec.from(
    (cursor: HCursor) => cursor.get[String]("token").map(ResetApplyRequest(_)),
    (dto: ResetApplyRequest) => Json.obj("token" -> dto.token.asJson)
  )

  given Schema[ResetApplyRequest] =
    Schema.derived[ResetApplyRequest].description("The plan token, and nothing else")

  given CanEqual[ResetApplyRequest, ResetApplyRequest] = CanEqual.derived
}

/** What a delete-offsets call removed.
  *
  * It answers with the partitions rather than with `204 No Content` because "the group had no committed
  * offsets for that topic" and "the offsets were deleted" are different outcomes an operator wants
  * distinguished, and an empty body cannot tell them apart.
  */
final case class DeletedOffsetsDto(groupId: GroupId, topic: TopicName, partitions: List[Int])

object DeletedOffsetsDto {

  given Codec[DeletedOffsetsDto] = Codec.from(
    (cursor: HCursor) =>
      for {
        groupId <- cursor.get[GroupId]("groupId")
        topic <- cursor.get[TopicName]("topic")
        partitions <- cursor.get[List[Int]]("partitions")
      } yield DeletedOffsetsDto(groupId, topic, partitions),
    (dto: DeletedOffsetsDto) =>
      Json.obj(
        "groupId" -> dto.groupId.asJson,
        "topic" -> dto.topic.asJson,
        "partitions" -> dto.partitions.asJson
      )
  )

  given Schema[DeletedOffsetsDto] =
    Schema.derived[DeletedOffsetsDto].description("Which partitions' committed offsets were deleted")

  given CanEqual[DeletedOffsetsDto, DeletedOffsetsDto] = CanEqual.derived
}
