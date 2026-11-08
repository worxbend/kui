package kui.consumer.application

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

import scala.concurrent.duration.*
import scala.util.control.NonFatal

import cats.effect.kernel.Sync
import cats.syntax.all.*

import kui.consumer.domain.{PlannedPartition, ResetPlan, ResetScope, ResetSpec}
import kui.kernel.error.{ApplicationError, KuiError}
import kui.kernel.group.ResetTarget
import kui.kernel.{ClusterId, GroupId, Offset, PartitionId, Secret, TopicName}

/** The token that says "apply exactly the offsets the operator was shown" (ADR-045).
  *
  * It authorises nothing an operator could not do anyway. What it prevents is applying a *different* set of
  * offsets from the ones on screen — because the client edited them, because the cluster moved underneath the
  * plan, or because a second tab planned something else five minutes ago. The apply endpoint takes only a
  * token and never a raw spec, so there is no path by which an unshown offset can be written.
  *
  * The payload also binds the committed offset each partition had when the plan was computed (`none` when it
  * had none). Apply refuses when those have moved, so a shift planned against one position is not written
  * after the group has consumed further, and a token cannot be replayed once the write itself moved the
  * commits.
  *
  * The wire form is `base64url(payload) "." base64url(HMAC-SHA256(payload))`, and the payload is a
  * hand-written delimited line rather than a derived encoding, for the same reason the browse cursor's is: it
  * is a compatibility surface, and a renamed field must not silently invalidate every token in flight.
  *
  * The signature is verified before the payload is parsed. A codec that parses first is one an attacker can
  * drive with a payload they never had to sign.
  */
trait PlanToken[F[_]] {

  def mint(cluster: ClusterId, plan: ResetPlan, expiresAt: Instant): F[String]

  /** `Left(KUI-VALIDATION)` for a bad signature, an expired token, or one minted for another cluster or
    * group.
    *
    * The three are deliberately not distinguished in the message: telling a caller which part of a forged
    * token was wrong is an oracle. The log line distinguishes them; the response does not.
    */
  def verify(cluster: ClusterId, group: GroupId, token: String, now: Instant): F[Either[KuiError, ResetPlan]]
}

object PlanToken {

  private val Algorithm: String = "HmacSHA256"

  /** `v3` puts the cluster and the group in their own fields and base64url-encodes the group, and it signs
    * the committed offset beside each proposed one. Earlier payloads joined `cluster/group` and separated
    * fields with `|`, so a `/` in the group was truncated, a `|` added a field, and a token for `x/b`
    * verified as `b`. Those payloads are refused: a token lives five minutes, which is shorter than a deploy.
    */
  private val Version: String = "v3"
  private val Separator: Char = '.'
  private val Field: Char = '|'
  private val Entry: Char = ','
  private val Pair: Char = ':'

  /** Explicit, not an empty field: an empty third column is a truncated entry, and `0` is a real offset. */
  private val AbsentCommit: String = "none"

  /** Uses the existing cursor key (ADR-026): no new secret, and no new configuration for an operator to get
    * wrong. The two uses are separated by the payload's own version prefix and by the binding it carries.
    */
  def make[F[_]: Sync](key: Secret[Array[Byte]]): PlanToken[F] = new Impl[F](key)

  final private class Impl[F[_]: Sync](key: Secret[Array[Byte]]) extends PlanToken[F] {

    def mint(cluster: ClusterId, plan: ResetPlan, expiresAt: Instant): F[String] =
      Sync[F].delay {
        val payload = render(plan, cluster, expiresAt)
        val encoded = encode(payload.getBytes(StandardCharsets.UTF_8))
        s"$encoded$Separator${encode(sign(payload))}"
      }

    def verify(
        cluster: ClusterId,
        group: GroupId,
        token: String,
        now: Instant
    ): F[Either[KuiError, ResetPlan]] =
      Sync[F].delay {
        token.split(Separator) match {
          case Array(payloadPart, signaturePart) =>
            val payloadBytes = decode(payloadPart)
            val signature = decode(signaturePart)

            payloadBytes.zip(signature) match {
              case Some((bytes, mac)) =>
                val payload = new String(bytes, StandardCharsets.UTF_8)

                // Constant-time, and before any parsing.
                if !MessageDigest.isEqual(sign(payload), mac) then Left(invalid)
                else
                  parse(payload) match {
                    case Some((plan, expiresAt))
                        if plan.group == group && plan.scope.topic.value.nonEmpty && !now.isAfter(
                          expiresAt
                        ) =>
                      // The cluster is bound through the payload too: a token minted for another
                      // cluster's group of the same name must not apply here.
                      if boundTo(payload, cluster) then Right(plan)
                      else Left(invalid)
                    case _ => Left(invalid)
                  }
              case None => Left(invalid)
            }

          case _ => Left(invalid)
        }
      }

    private def sign(payload: String): Array[Byte] = {
      val mac = Mac.getInstance(Algorithm)
      mac.init(new SecretKeySpec(key.value, Algorithm))
      mac.doFinal(payload.getBytes(StandardCharsets.UTF_8))
    }
  }

  private val invalid: KuiError =
    ApplicationError.Invalid(
      "the reset plan token is not valid; plan the reset again and confirm the plan it returns",
      Nil
    )

  private def encode(bytes: Array[Byte]): String = Base64.getUrlEncoder.withoutPadding.encodeToString(bytes)

  private def decode(raw: String): Option[Array[Byte]] =
    try Some(Base64.getUrlDecoder.decode(raw))
    catch { case NonFatal(_) => None }

  /** The canonical rendering. Partitions are already sorted by the planner, and re-sorted here, because the
    * token is only as good as the guarantee that the same plan renders to the same bytes.
    */
  private def render(plan: ResetPlan, cluster: ClusterId, expiresAt: Instant): String = {
    val offsets = plan.partitions
      .sortBy(_.partition.partition.value)
      .map { one =>
        val committed = one.current.fold(AbsentCommit)(_.value.toString)
        s"${one.partition.partition.value}$Pair${one.proposed.value}$Pair$committed"
      }
      .mkString(Entry.toString)

    List(
      Version,
      cluster.value,
      encode(plan.group.value.getBytes(StandardCharsets.UTF_8)),
      plan.scope.topic.value,
      plan.spec.target.wire,
      offsets,
      expiresAt.toEpochMilli.toString
    ).mkString(Field.toString)
  }

  /** Field 1 is the cluster id alone. Equality, not a prefix: `pro` must not verify a token minted for
    * `prod`, and the group id is no longer appended here where a `/` inside it could be truncated.
    */
  private def boundTo(payload: String, cluster: ClusterId): Boolean =
    payload
      .split(java.util.regex.Pattern.quote(Field.toString), -1)
      .lift(1)
      .exists(_ == cluster.value)

  private def parse(payload: String): Option[(ResetPlan, Instant)] =
    // `split(String)` is a regex split and `|` is alternation, so the delimiter is quoted. Quoting
    // rather than using the `Char` overload keeps the `-1` limit, which is what preserves an empty
    // offsets field for a plan over no partitions.
    payload.split(java.util.regex.Pattern.quote(Field.toString), -1).toList match {
      case version :: _ :: encodedGroup :: topic :: target :: offsets :: expiry :: Nil
          if version == Version =>
        for {
          group <- decodeGroup(encodedGroup)
          topicName <- TopicName.from(topic).toOption
          expiresAt <- expiry.toLongOption.map(Instant.ofEpochMilli)
          planned <- parseOffsets(topicName, offsets)
          // The name is what the receipt reports. The parameters are not in the token: what is
          // applied is the offsets, and re-resolving a spec at apply time is what this token prevents.
          spec <- specOf(target)
        } yield (
          ResetPlan(
            group = group,
            scope = ResetScope(topicName, planned.map(_.partition).toSet),
            spec = spec,
            partitions = planned,
            warnings = Nil,
            computedAt = Instant.EPOCH
          ),
          expiresAt
        )
      case _ => None
    }

  private def parseOffsets(topic: TopicName, raw: String): Option[List[PlannedPartition]] =
    if raw.isEmpty then Some(Nil)
    else
      raw
        .split(Entry)
        .toList
        .traverse { entry =>
          entry.split(Pair) match {
            case Array(partition, offset, committed) =>
              for {
                number <- partition.toIntOption
                at <- offset.toLongOption
                id <- PartitionId.from(number).toOption
                value <- Offset.from(at).toOption
                current <- parseCommit(committed)
              } yield PlannedPartition(
                kui.kernel.TopicPartition(topic, id),
                current,
                value,
                current.map(seen => value.value - seen.value)
              )
            case _ => None
          }
        }

  /** The group id, base64url-encoded so `/` and `|` inside it stay one field. */
  private def decodeGroup(raw: String): Option[GroupId] =
    decode(raw).flatMap(bytes => GroupId.from(new String(bytes, StandardCharsets.UTF_8)).toOption)

  /** `Some(None)` is a partition that had no commit. `None` is a column this decoder does not understand. */
  private def parseCommit(raw: String): Option[Option[Offset]] =
    if raw == AbsentCommit then Some(None)
    else raw.toLongOption.flatMap(value => Offset.from(value).toOption).map(Some(_))

  /** The parameters are placeholders. Apply writes `partitions`, never this spec, and the receipt reads only
    * `spec.target`. An unknown name fails the token rather than being reported as `OFFSET`.
    */
  private def specOf(wire: String): Option[ResetSpec] =
    ResetTarget.from(wire).toOption.map {
      case ResetTarget.Earliest => ResetSpec.ToEarliest
      case ResetTarget.Latest => ResetSpec.ToLatest
      case ResetTarget.Timestamp => ResetSpec.ToTimestamp(Instant.EPOCH)
      case ResetTarget.Offset => ResetSpec.ToOffsets(Map.empty)
      case ResetTarget.ShiftBy => ResetSpec.ShiftBy(0L)
      case ResetTarget.Duration => ResetSpec.ByDuration(Duration.Zero)
    }

}
