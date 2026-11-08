package kui.security

import java.time.Instant

import cats.MonadThrow
import cats.data.NonEmptyList
import cats.syntax.all.*
import com.nimbusds.jose.crypto.{MACSigner, MACVerifier}
import com.nimbusds.jose.{JWSAlgorithm, JWSHeader, JWSObject, Payload}
import io.circe.parser.decode
import io.circe.syntax.*

import kui.kernel.{Secret, ServiceId}

/** One HMAC key the gateway may sign with and the services accept.
  *
  * `notBefore` is what makes rotation a rolling change rather than an outage. A new key is added to every
  * service first, with a `notBefore` in the near future; the gateway is updated next and starts signing with
  * it when that time arrives; the old key is removed afterwards. At no point is there a moment when a service
  * can be handed a token signed with a key it has never heard of.
  */
final case class SigningKey(kid: String, key: Secret[Array[Byte]], notBefore: Instant)

/** Why [[JwsPrincipalCodec.make]] refused a key set, as a value rather than an exception. */
sealed trait RejectedSigningKey {
  def message: String
}

/** A signing key that HS256 cannot use.
  *
  * HMAC-SHA-256 requires a secret at least as long as its output — 256 bits — and a shorter one weakens the
  * signature rather than failing loudly. KUI refuses it at construction, as a value, so a misconfigured
  * deployment cannot start and then mint weak tokens.
  */
final case class WeakSigningKey(kid: String, bits: Int) extends RejectedSigningKey {
  def message: String = s"signing key '$kid' is $bits bits; HS256 needs at least 256"
}

/** Two configured keys share a key id.
  *
  * Verification looks the token's `kid` up in a map, so a repeated id keeps only the last key, while signing
  * still chooses by `notBefore` across every entry. A token signed with the earlier key then fails
  * verification on every service. The id is reported; the key material is not.
  */
final case class DuplicateSigningKey(kids: NonEmptyList[String]) extends RejectedSigningKey {
  def message: String = {
    val distinct = kids.toList.distinct.sorted
    val named = distinct.map(kid => s"'$kid'").mkString(", ")
    if distinct.sizeIs == 1 then
      s"signing key id $named is configured more than once; each id may appear only once. A repeated id " +
        "would keep only the last key, and a token signed with the earlier one would not verify"
    else
      s"signing key ids $named are each configured more than once; each id may appear only once. A repeated " +
        "id would keep only the last key, and a token signed with the earlier one would not verify"
  }
}

/** The real principal codec: a compact JWS, HS256, with the key id in the header (ADR-020).
  *
  * This is the only place in KUI that uses a JOSE library, and it lives in the JVM source set on purpose.
  * `libs/security-core` is cross-compiled, because the browser needs `Principal` and the role vocabulary; it
  * must not pull nimbus into that shared half, where it would make the browser build impossible.
  * `./mill checkArchitecture` (rule A6) enforces the split, and the `.js` module links without ever seeing
  * this file.
  */
object JwsPrincipalCodec {

  private val MinimumKeyBits: Int = 256

  /** Builds a codec over a key set.
    *
    * Signing uses the newest key whose `notBefore` has passed; verification accepts any key in the set, which
    * is what lets the two sides of a rotation overlap. Construction fails — as a value, not an exception —
    * when any key is too short for HS256, or when two keys share a key id. A set whose every `notBefore` is
    * still in the future is accepted here: that check needs a clock, and the config loader refuses it before
    * a process starts. `sign` still fails closed if one is used anyway.
    */
  def make[F[_]: MonadThrow](
      keys: NonEmptyList[SigningKey],
      issuer: String
  ): Either[RejectedSigningKey, PrincipalCodec[F]] = {
    val tooShort = keys.toList.collectFirst {
      case key if key.key.value.length * 8 < MinimumKeyBits =>
        WeakSigningKey(key.kid, key.key.value.length * 8)
    }
    val repeated = keys.toList
      .groupBy(_.kid)
      .collect { case (kid, group) if group.sizeIs > 1 => kid }
      .toList
      .sorted
    tooShort
      .orElse(NonEmptyList.fromList(repeated).map(DuplicateSigningKey.apply))
      .toLeft(new JwsCodec[F](keys, issuer))
  }

  final private class JwsCodec[F[_]: MonadThrow](keys: NonEmptyList[SigningKey], issuer: String)
      extends PrincipalCodec[F] {

    private val byKid: Map[String, SigningKey] = keys.toList.map(key => key.kid -> key).toMap

    def sign(claims: PrincipalClaims): F[SignedPrincipal] =
      signingKeyAt(claims.issuedAt) match {
        case None =>
          MonadThrow[F].raiseError(
            new IllegalStateException(
              "no signing key is active yet; every configured key has a notBefore in the future"
            )
          )
        case Some(key) =>
          MonadThrow[F].catchNonFatal {
            val header = new JWSHeader.Builder(JWSAlgorithm.HS256)
              .keyID(key.kid)
              .customParam("iss", issuer)
              .build()
            val jws = new JWSObject(header, new Payload(claims.asJson.noSpaces))
            jws.sign(new MACSigner(key.key.value))
            SignedPrincipal.unsafe(jws.serialize())
          }
      }

    /** The verification order of ADR-020: parse, find the key, check the signature, then check the claims.
      * Nothing later is attempted once something earlier has failed, so a caller can never learn from the
      * response which key or which claim value the service expected.
      */
    def verify(
        token: SignedPrincipal,
        expected: ServiceId,
        request: RequestDigest,
        now: Instant
    ): F[Either[PrincipalError, Principal]] =
      MonadThrow[F]
        .catchNonFatal(JWSObject.parse(token.value))
        .attempt
        .map {
          case Left(failure) => Left(PrincipalError.Malformed(failureText(failure)))
          case Right(jws) => verifyParsed(jws, expected, request, now)
        }

    private def verifyParsed(
        jws: JWSObject,
        expected: ServiceId,
        request: RequestDigest,
        now: Instant
    ): Either[PrincipalError, Principal] =
      for {
        key <- Option(jws.getHeader.getKeyID)
          .flatMap(byKid.get)
          .toRight(PrincipalError.UnknownKeyId(Option(jws.getHeader.getKeyID).getOrElse("")))
        _ <- Either.cond(
          Either.catchNonFatal(jws.verify(new MACVerifier(key.key.value))).getOrElse(false),
          (),
          PrincipalError.BadSignature
        )
        _ <- Either.cond(
          Option(jws.getHeader.getCustomParam("iss")).map(_.toString).contains(issuer),
          (),
          PrincipalError.Malformed("the token was not minted by this issuer")
        )
        claims <- decode[PrincipalClaims](jws.getPayload.toString).leftMap(failure =>
          PrincipalError.Malformed(failure.getMessage)
        )
        principal <- PrincipalCodec.checkClaims(claims, expected, request, now)
      } yield principal

    private def signingKeyAt(now: Instant): Option[SigningKey] =
      keys.toList.filter(!_.notBefore.isAfter(now)).maxByOption(_.notBefore.toEpochMilli)

    /** Exception text is for this service's own log; it never reaches an HTTP response, which is always
      * `401 KUI-UNAUTHENTICATED` regardless of what went wrong.
      */
    private def failureText(failure: Throwable): String =
      Option(failure.getMessage).getOrElse(failure.getClass.getSimpleName)
  }
}
