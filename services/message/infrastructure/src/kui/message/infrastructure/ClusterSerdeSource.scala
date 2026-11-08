package kui.message.infrastructure

import cats.effect.kernel.Sync
import cats.syntax.all.*
import io.circe.Json

import kui.kernel.error.{ApplicationError, KuiError}
import kui.kernel.serde.{PayloadKind, SerdeName, SerdeUse, Target}
import kui.kernel.{ClusterId, TopicName}
import kui.message.domain.Decoded
import kui.message.domain.ports.{SerdeChoice, SerdeSource}
import kui.serde.{
  ClusterSerdes,
  DeserializeResult,
  Deserializers,
  RawHeader as SerdeHeader,
  Serde,
  SerdeAutodetect
}

/** `libs/serde` behind the message domain's `SerdeSource` port.
  *
  * ==Why this module and not the use case==
  *
  * The domain states decoding as a port precisely so that it can say "a decode never fails a browse" without
  * naming a serde library. This is the file where that promise is kept, and it is kept by never returning a
  * failure at all from [[decode]]: every path ends in a [[Decoded]], and the reason the intended serde did
  * not work travels beside it as a `Some(cause)` that the record carries to the screen.
  *
  * ==How a serde is chosen when the caller names none==
  *
  * Auto-detection first, then the configured resolution order. That order matters for the seeded quickstart
  * and for every real cluster like it: `orders.v1` holds JSON and must render as JSON so the table view can
  * flatten it, while `audit.log.raw` holds plain log lines that no JSON parser will accept — and both have to
  * work on the same screen with nobody configuring anything. Auto-detection asks each serde whether *these
  * bytes* are its own, which answers both questions from the record rather than from a setting.
  *
  * A cluster that has no serdes built — one this deployment does not configure — is not an error either. Its
  * records decode through the fallback, which is the terminal case the whole resolution order is built
  * around.
  */
final class ClusterSerdeSource[F[_]: Sync](serdes: Map[ClusterId, ClusterSerdes[F]]) extends SerdeSource[F] {

  def decode(
      cluster: ClusterId,
      topic: TopicName,
      target: Target,
      requested: Option[SerdeName],
      bytes: Option[Array[Byte]]
  ): F[(Decoded, Option[String])] =
    serdes.get(cluster) match {
      case None =>
        // No serdes for a cluster nobody configured. The browse cannot have reached here for an unknown
        // cluster — the profile source refuses it first — so this is the honest floor rather than a case
        // anyone should see, and it still produces a record instead of an exception.
        (Decoded.absent(SerdeName.Fallback), Option.empty[String]).pure[F]

      case Some(cluster) => decodeWith(cluster, topic, target, requested, bytes)
    }

  private def decodeWith(
      serdes: ClusterSerdes[F],
      topic: TopicName,
      target: Target,
      requested: Option[SerdeName],
      bytes: Option[Array[Byte]]
  ): F[(Decoded, Option[String])] =
    // `all` and `unavailableChoice` are snapshots taken at startup unless somebody refreshes them. Auto-
    // detection reads `all` directly, so a registry that was down when the process started and has since
    // answered would otherwise stay out of the list for the life of the process.
    serdes.refresh.flatMap {
      _ =>
        val skipped: Option[String] =
          // Only when the caller did not name a serde. An explicit choice that cannot be honoured is already
          // refused by `resolve` with its own message, and reporting both would say the same thing twice.
          if requested.isDefined then None
          else
            serdes
              .unavailableChoice(topic, target)
              .map((name, reason) =>
                s"the ${name.value} serde is configured for this topic and could not be used: ${reason}"
              )

      chosen(serdes, topic, target, requested, bytes).flatMap {
        case Left(refusal) =>
          // The caller named a serde this cluster cannot use. The record is still shown — through the
          // fallback — and the refusal is what the row's marker says, so the user learns their choice was
          // not honoured instead of reading a screen of mojibake and blaming the data.
          fallbackOnly(serdes, topic, target, bytes).map((decoded, _) => (decoded, Some(refusal.message)))

        case Right(serde) =>
          for {
            primary <- serde.deserializer(topic, target)
            fallback <- serdes.fallback.deserializer(topic, target)
            outcome <- Deserializers.withFallback(primary, fallback, Nil, bytes.map(identity))
          } yield outcome match {
            case (result, None) => (rendered(result, serde.name), skipped)
            // The text on the record is the fallback's, so the serde named on it is the fallback's too:
            // `serde` answers "what produced this text?", and the failure beside it answers "what did I
            // configure wrongly?".
            //
            // `skipped` wins over the fallback's own complaint when both are present, and that ordering is
            // the whole point of it: "the SchemaRegistry serde is configured for this topic and could not be
            // used - the registry could not be reached" names the thing an operator can fix, while "the
            // payload is not valid UTF-8" describes the consequence and points at the data.
            case (result, Some(failure)) =>
              (rendered(result, serdes.fallback.name), skipped.orElse(Some(failure.cause)))
          }
      }
    }

  private def fallbackOnly(
      serdes: ClusterSerdes[F],
      topic: TopicName,
      target: Target,
      bytes: Option[Array[Byte]]
  ): F[(Decoded, Option[String])] =
    for {
      fallback <- serdes.fallback.deserializer(topic, target)
      outcome <- Deserializers.attempt(fallback, Nil, bytes)
    } yield (rendered(outcome.getOrElse(Deserializers.NullPayload), serdes.fallback.name), None)

  /** Which serde reads this payload: the caller's choice, then the bytes' own claim, then configuration. */
  private def chosen(
      serdes: ClusterSerdes[F],
      topic: TopicName,
      target: Target,
      requested: Option[SerdeName],
      bytes: Option[Array[Byte]]
  ): F[Either[KuiError, Serde[F]]] =
    requested match {
      case Some(_) => serdes.resolve(topic, target, requested)
      case None =>
        SerdeAutodetect.rank(serdes.all, topic, target, bytes).flatMap { ranked =>
          ranked.headOption.flatMap(name => serdes.all.find(_.name == name)) match {
            case Some(detected) => detected.asRight[KuiError].pure[F]
            case None => serdes.resolve(topic, target, None)
          }
        }
    }

  private def rendered(result: DeserializeResult, serde: SerdeName): Decoded =
    Decoded(
      text = result.text,
      kind = result.kind,
      serde = serde,
      properties = result.properties.map((key, value) => key -> asText(value))
    )

  /** A serde property as one line of text.
    *
    * A JSON string becomes its contents rather than a quoted literal, because a schema subject rendered as
    * `"orders.v1-value"` with the quotes visible is a value nobody can copy into another tool.
    */
  private def asText(value: Json): String = value.asString.getOrElse(value.noSpaces)

  def serialize(
      cluster: ClusterId,
      topic: TopicName,
      target: Target,
      requested: Option[SerdeName],
      properties: Map[String, String],
      text: Option[String]
  ): F[Either[KuiError, Option[Array[Byte]]]] =
    serdes.get(cluster) match {
      case None =>
        ApplicationError
          .Unsupported(s"no serdes are configured for cluster '${cluster.value}'")
          .asLeft[Option[Array[Byte]]]
          .pure[F]

      case Some(serdes) =>
        text match {
          // A tombstone: an absent value is a value, and it is written as `null` rather than as the
          // serialisation of an empty string, which would be an ordinary record with an empty payload.
          case None => Option.empty[Array[Byte]].asRight[KuiError].pure[F]
          case Some(input) =>
            serdes.resolve(topic, target, requested).flatMap {
              case Left(error) => error.asLeft[Option[Array[Byte]]].pure[F]
              case Right(serde) =>
                serde
                  .serializer(topic, target, properties)
                  .flatMap(_.serialize(input, List.empty[SerdeHeader]))
                  .map {
                    case Right(bytes) => Some(bytes).asRight[KuiError]
                    // Terminal, unlike a decode failure: bytes KUI could not encode would put a record
                    // in a topic that outlives the mistake.
                    case Left(failure) =>
                      ApplicationError
                        .Invalid(failure.cause, Nil)
                        .asLeft[Option[Array[Byte]]]
                  }
            }
        }
    }

  def choices(
      cluster: ClusterId,
      topic: TopicName,
      target: Target,
      use: SerdeUse
  ): F[Either[KuiError, List[SerdeChoice]]] =
    serdes.get(cluster) match {
      case None => List.empty[SerdeChoice].asRight[KuiError].pure[F]
      case Some(serdes) =>
        serdes
          .suggest(topic, target, use)
          .map(rows =>
            rows
              .map(row =>
                SerdeChoice(
                  name = row.name,
                  description = row.description,
                  preferred = row.preferred,
                  available = row.available,
                  unavailableReason = row.unavailableReason
                )
              )
              .asRight[KuiError]
          )
    }
}

object ClusterSerdeSource {

  /** The kind an absent payload is reported as, so that a caller building one by hand agrees with this
    * adapter about what "nothing here" looks like.
    */
  val AbsentKind: PayloadKind = PayloadKind.Text
}
