package kui.message.application.purge

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

import scala.util.control.NonFatal

import cats.effect.kernel.Sync

import kui.kernel.error.{ApplicationError, KuiError}
import kui.kernel.{ClusterId, Offset, PartitionId, Secret, TopicName}
import kui.message.domain.PlannedPurge

/** The token that says "delete exactly the records the operator was shown" (ADR-045).
  *
  * It carries the per-partition offsets the plan resolved, so the apply step writes those and never a
  * recomputation. That distinction is the whole point on this operation: between the plan and the confirm,
  * producers keep writing, and a purge that re-read the end offsets at apply time would delete records that
  * arrived after the operator read the number — which is precisely what they did not agree to.
  *
  * The wire form is `base64url(payload) "." base64url(HMAC-SHA256(payload))`, the payload is a hand-written
  * delimited line, and the signature is verified before the payload is parsed. All three for the reasons the
  * browse cursor's codec gives, next door: a token is a compatibility surface, and a codec that parses before
  * it verifies is one an attacker can drive with a payload they never had to sign.
  *
  * The key is `kui.streaming.cursorKey`, which this service already holds for browse cursors (ADR-026). One
  * secret, one rotation procedure, and the two uses kept apart by the payload's version prefix and by the
  * operation name inside it.
  */
trait PurgeToken[F[_]] {

  def mint(
      cluster: ClusterId,
      topic: TopicName,
      partitions: List[PlannedPurge],
      expiresAt: Instant,
      topicId: String = ""
  ): F[String]

  /** `Left(KUI-VALIDATION)` for a bad signature, an expired token, or one minted for another cluster or
    * topic. `Right` carries the offsets that were signed.
    *
    * The three are deliberately not distinguished in the message: telling a caller which part of a forged
    * token was wrong is an oracle.
    */
  def verify(
      cluster: ClusterId,
      topic: TopicName,
      token: String,
      now: Instant,
      topicId: String = ""
  ): F[Either[KuiError, List[PlannedPurge]]]
}

object PurgeToken {

  private val Algorithm: String = "HmacSHA256"
  private val Version: String = "v2"
  private val Operation: String = "message.purge"
  private val Separator: Char = '.'
  private val Field: Char = '|'
  private val Entry: Char = ','
  private val Pair: Char = ':'

  /** How long a plan may be confirmed for. ADR-045 fixes five minutes. */
  val Ttl: java.time.Duration = java.time.Duration.ofMinutes(5)

  def make[F[_]: Sync](key: Secret[Array[Byte]]): PurgeToken[F] = new Impl[F](key)

  final private class Impl[F[_]: Sync](key: Secret[Array[Byte]]) extends PurgeToken[F] {

    def mint(
        cluster: ClusterId,
        topic: TopicName,
        partitions: List[PlannedPurge],
        expiresAt: Instant,
        topicId: String
    ): F[String] =
      Sync[F].delay {
        val payload = render(cluster, topic, partitions, expiresAt, topicId)
        s"${encode(payload.getBytes(StandardCharsets.UTF_8))}$Separator${encode(sign(payload))}"
      }

    def verify(
        cluster: ClusterId,
        topic: TopicName,
        token: String,
        now: Instant,
        topicId: String
    ): F[Either[KuiError, List[PlannedPurge]]] =
      Sync[F].delay {
        token.split(Separator) match {
          case Array(payloadPart, signaturePart) =>
            decode(payloadPart).zip(decode(signaturePart)) match {
              case Some((bytes, mac)) =>
                val payload = new String(bytes, StandardCharsets.UTF_8)

                // Constant-time, and before any parsing.
                if !MessageDigest.isEqual(sign(payload), mac) then Left(invalid)
                else
                  parse(payload) match {
                    case Some((subject, partitions, expiresAt))
                        if subject == subjectOf(cluster, topic, topicId) && !now.isAfter(expiresAt) =>
                      Right(partitions)
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
      "this purge confirmation is not valid; ask for the purge to be planned again and confirm the plan " +
        "that comes back",
      Nil
    )

  private def encode(bytes: Array[Byte]): String = Base64.getUrlEncoder.withoutPadding.encodeToString(bytes)

  private def decode(raw: String): Option[Array[Byte]] =
    try Some(Base64.getUrlDecoder.decode(raw))
    catch { case NonFatal(_) => None }

  /** A topic name cannot contain `/` — `TopicName`'s own pattern allows only letters, digits, `.`, `_` and
    * `-` — so a cluster and a topic can be joined without a name chosen to look like a pair confusing them.
    */
  private def subjectOf(cluster: ClusterId, topic: TopicName, topicId: String): String =
    s"${cluster.value}/${topic.value}/${encode(topicId.getBytes(StandardCharsets.UTF_8))}"

  /** The canonical rendering. Partitions are sorted, because the token is only as good as the guarantee that
    * the same plan renders to the same bytes.
    *
    * Each entry is `partition:low:high`, and the low watermark is in there for the receipt's sake: after the
    * purge the topic's start offset *is* the high watermark, so the number of records that were destroyed can
    * no longer be read off the cluster. The token is the only remaining record of it.
    */
  private def render(
      cluster: ClusterId,
      topic: TopicName,
      partitions: List[PlannedPurge],
      expiresAt: Instant,
      topicId: String
  ): String = {
    val rendered = partitions
      .sortBy(_.partition.value)
      .map(one => s"${one.partition.value}$Pair${one.lowWatermark.value}$Pair${one.highWatermark.value}")
      .mkString(Entry.toString)

    List(Version, subjectOf(cluster, topic, topicId), Operation, rendered, expiresAt.toEpochMilli.toString)
      .mkString(Field.toString)
  }

  /** `split(String)` is a regex split and `|` is alternation, so the delimiter is quoted. Quoting rather than
    * using the `Char` overload keeps the `-1` limit, which is what preserves an empty offsets field — a plan
    * over a topic that is already empty.
    */
  private def parse(payload: String): Option[(String, List[PlannedPurge], Instant)] =
    payload.split(java.util.regex.Pattern.quote(Field.toString), -1).toList match {
      case version :: subject :: operation :: offsets :: expiry :: Nil
          if version == Version && operation == Operation =>
        for {
          expiresAt <- expiry.toLongOption.map(Instant.ofEpochMilli)
          parsed <- parseOffsets(offsets)
        } yield (subject, parsed, expiresAt)
      case _ => None
    }

  private def parseOffsets(raw: String): Option[List[PlannedPurge]] =
    if raw.isEmpty then Some(Nil)
    else {
      val parsed = raw.split(Entry).toList.map { entry =>
        entry.split(Pair) match {
          case Array(partition, low, high) =>
            for {
              number <- partition.toIntOption
              start <- low.toLongOption
              end <- high.toLongOption
              id <- PartitionId.from(number).toOption
              from <- Offset.from(start).toOption
              to <- Offset.from(end).toOption
            } yield PlannedPurge(id, from, to)
          case _ => None
        }
      }

      if parsed.exists(_.isEmpty) then None else Some(parsed.flatten)
    }
}
