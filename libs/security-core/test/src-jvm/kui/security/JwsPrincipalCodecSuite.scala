package kui.security

import java.time.Instant
import java.util.Locale

import cats.data.NonEmptyList
import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

import kui.kernel.{RoleName, Secret, ServiceId, UserName}

/** The signed half of ADR-020: what a service accepts, and — mostly — what it refuses.
  *
  * A test that only proves a valid token works would pass against a codec that accepts everything. Every
  * check the verification performs therefore has a test that defeats it, including a property that flips one
  * character anywhere in a token and asserts the token stops verifying.
  */
final class JwsPrincipalCodecSuite extends ScalaCheckSuite {

  /** The codec needs a `MonadThrow`, and `Attempt` is the smallest one there is: no runtime, no thread pool,
    * and a failure that the test can look at directly.
    */
  private type Attempt[A] = Either[Throwable, A]

  private val now: Instant = Instant.parse("2026-09-03T10:00:00Z")
  private val service = ServiceId.unsafe("topic")
  private val digest = RequestDigests.of("POST", "/internal/v1/topics", "{}".getBytes("UTF-8"))

  private def key(kid: String, byte: Byte, notBefore: Instant): SigningKey =
    SigningKey(kid, Secret(Array.fill[Byte](32)(byte)), notBefore)

  private val oldKey = key("k1", 1, now.minusSeconds(3600))
  private val newKey = key("k2", 2, now.minusSeconds(60))

  private def codecOf(keys: SigningKey*): PrincipalCodec[Attempt] =
    JwsPrincipalCodec
      .make[Attempt](NonEmptyList.fromListUnsafe(keys.toList), "kui-gateway")
      .fold(weak => fail(weak.message), identity)

  private val codec = codecOf(oldKey, newKey)

  private def claims(
      audience: ServiceId = service,
      expiresAt: Instant = now.plusSeconds(60),
      request: RequestDigest = digest
  ): PrincipalClaims =
    PrincipalClaims(
      subject = UserName.unsafe("ada"),
      roles = Set(RoleName.unsafe("viewer")),
      kind = PrincipalKind.Session,
      sessionRef = Some(SessionRef("f00d")),
      issuedAt = now,
      expiresAt = expiresAt,
      audience = audience,
      requestDigest = request
    )

  private def sign(claimSet: PrincipalClaims, by: PrincipalCodec[Attempt] = codec): SignedPrincipal =
    by.sign(claimSet).fold(error => fail(s"signing failed: $error"), identity)

  test("a signed token verifies back to the principal it asserts") {
    val result = codec.verify(sign(claims()), service, digest, now)
    assertEquals(
      result,
      Right(Right(Principal(UserName.unsafe("ada"), Set(RoleName.unsafe("viewer")), PrincipalKind.Session)))
    )
  }

  property("changing any single character of a token stops it verifying") {
    val token = sign(claims()).value

    forAll(Gen.chooseNum(0, token.length - 1)) { index =>
      val replacement = if token.charAt(index) == 'A' then 'B' else 'A'
      val tampered = token.updated(index, replacement)
      val result = codec.verify(SignedPrincipal.unsafe(tampered), service, digest, now)
      assert(
        result.exists(_.isLeft),
        s"a token altered at index $index still verified"
      )
    }
  }

  test("a token signed with a key this service does not know is refused by key id") {
    val stranger = codecOf(key("k9", 9, now.minusSeconds(60)))
    val token = sign(claims(), by = stranger)

    assertEquals(codec.verify(token, service, digest, now), Right(Left(PrincipalError.UnknownKeyId("k9"))))
  }

  test("a token signed with the right key id but the wrong key is refused by signature") {
    val impostor = codecOf(SigningKey("k2", Secret(Array.fill[Byte](32)(7)), now.minusSeconds(60)))
    val token = sign(claims(), by = impostor)

    assertEquals(codec.verify(token, service, digest, now), Right(Left(PrincipalError.BadSignature)))
  }

  test("a token minted for another service is refused") {
    val token = sign(claims(audience = ServiceId.unsafe("cluster")))
    assertEquals(
      codec.verify(token, service, digest, now),
      Right(Left(PrincipalError.WrongAudience(service, ServiceId.unsafe("cluster"))))
    )
  }

  test("an expired token is refused, and one within five seconds of skew is not") {
    val expiry = now.minusSeconds(4)
    val token = sign(claims(expiresAt = expiry))

    assertEquals(codec.verify(token, service, digest, now), Right(Right(claims().principal)))
    assertEquals(
      codec.verify(token, service, digest, now.plusSeconds(2)),
      Right(Left(PrincipalError.Expired(expiry)))
    )
  }

  test("a token minted for a different call is refused") {
    val other = RequestDigests.of("DELETE", "/internal/v1/topics/orders", Array.emptyByteArray)
    val token = sign(claims(request = other))

    assertEquals(codec.verify(token, service, digest, now), Right(Left(PrincipalError.RequestMismatch)))
  }

  test("a token whose body hash differs by one byte is refused") {
    val token =
      sign(claims(request = RequestDigests.of("POST", "/internal/v1/topics", "{ }".getBytes("UTF-8"))))
    assertEquals(codec.verify(token, service, digest, now), Right(Left(PrincipalError.RequestMismatch)))
  }

  test("a token from another issuer is refused even when it is correctly signed") {
    val elsewhere = JwsPrincipalCodec
      .make[Attempt](NonEmptyList.of(newKey), "someone-else")
      .fold(weak => fail(weak.message), identity)
    val token = elsewhere.sign(claims()).fold(error => fail(error.toString), identity)

    codec.verify(token, service, digest, now) match {
      case Right(Left(PrincipalError.Malformed(_))) => ()
      case other => fail(s"expected a rejected issuer, got $other")
    }
  }

  test("rotation: both keys verify, and the newest active key signs") {
    val token = sign(claims())
    assert(
      codec.verify(token, service, digest, now).exists(_.isRight),
      "the freshly signed token must verify"
    )

    val onlyOldKey = codecOf(oldKey)
    assertEquals(
      onlyOldKey.verify(token, service, digest, now),
      Right(Left(PrincipalError.UnknownKeyId("k2")))
    )

    val onlyNewKey = codecOf(newKey)
    assert(
      onlyNewKey.verify(token, service, digest, now).exists(_.isRight),
      "the token must have been signed with k2, the newest key whose notBefore has passed"
    )
  }

  test("a key whose notBefore has not arrived is not used for signing") {
    val future = key("k3", 3, now.plusSeconds(3600))
    val token = sign(claims(), by = codecOf(oldKey, future))

    assert(
      codecOf(oldKey).verify(token, service, digest, now).exists(_.isRight),
      "a key that is not active yet must not sign"
    )
  }

  test("two keys that share a kid are refused at construction") {
    val earlier = key("k", 1, now.minusSeconds(3600))
    val later = key("k", 2, now.plusSeconds(3600))
    assert(
      JwsPrincipalCodec.make[Attempt](NonEmptyList.of(earlier, later), "kui-gateway").isLeft
    )
  }

  test("a key that is too short for HS256 is refused at construction, as a value") {
    val weak = SigningKey("weak", Secret(Array.fill[Byte](16)(1)), now)
    assertEquals(
      JwsPrincipalCodec.make[Attempt](NonEmptyList.of(weak), "kui-gateway").left.toOption,
      Some[RejectedSigningKey](WeakSigningKey("weak", 128))
    )
  }

  test("nothing prints the signing key: not the key, not the codec, not a rejection") {
    val secretBytes = "hunter2-hunter2-hunter2-hunter2!".getBytes("UTF-8")
    val signingKey = SigningKey("k1", Secret(secretBytes), now)
    val rendered = List(
      signingKey.toString,
      signingKey.key.toString,
      codec.toString,
      PrincipalError.UnknownKeyId("k1").toString,
      PrincipalError.BadSignature.toString,
      WeakSigningKey("k1", 128).message
    ).mkString(" ")

    assert(!rendered.contains("hunter2"), s"a signing key leaked into a rendering: $rendered")
    assertEquals(signingKey.key.toString, "Secret(***)")
  }

  test("the digest of a body is stable, and one changed byte changes it") {
    val body = "the quick brown fox".getBytes("UTF-8")
    assertEquals(RequestDigests.of("GET", "/x", body), RequestDigests.of("GET", "/x", body))
    assertNotEquals(
      RequestDigests.of("GET", "/x", body),
      RequestDigests.of("GET", "/x", "the quick brown fon".getBytes("UTF-8"))
    )
  }

  test("the digest of a body normalizes its method independently of the default locale") {
    val previousLocale = Locale.getDefault
    try {
      Locale.setDefault(Locale.forLanguageTag("tr-TR"))
      assertEquals(RequestDigests.of("options", "/x", Array.emptyByteArray).method, "OPTIONS")
    } finally Locale.setDefault(previousLocale)
  }

  test("the hash of an empty body is the constant the shared half writes down") {
    assertEquals(
      RequestDigests.sha256Hex(Array.emptyByteArray),
      RequestDigest.EmptyBodySha256
    )
  }

  test("the hash matches the published SHA-256 of 'abc'") {
    assertEquals(
      RequestDigests.sha256Hex("abc".getBytes("UTF-8")),
      "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    )
  }
}
