package kui.identity.infrastructure

import java.time.Instant
import java.util.Locale

import cats.effect.{IO, Ref}
import fs2.Stream
import io.circe.{Json, JsonObject}

import kui.config.store.{
  ConfigStore,
  SecretJson,
  StoreChange,
  StoreHealth,
  StoreKey,
  StoreRecord,
  StoreSection
}
import kui.identity.domain.{PasswordAlgorithm, PasswordHash, UserDirectory, UserRecord}
import kui.kernel.error.KuiError
import kui.kernel.{Secret, UserName}
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** The two account directories, and the three rules their comments argue for that nothing asserted.
  *
  * Measured one mutation at a time against `./mill services.identity.__.test + apps.allinone.test`, which
  * stayed at 98/98 green for each:
  *
  *   - `ConfiguredUserDirectory.key` losing its `toLowerCase`, so a person who signed in as `Admin` on Monday
  *     and `admin` on Tuesday is two people and the second has no account;
  *   - `ConfiguredUserDirectory.update` answering `Right(())`, so a password change against a file-backed
  *     deployment reports success and is gone at the next restart — the exact failure the refusal's own
  *     message was written to prevent;
  * Stored credential failures deliberately deny sign-in: falling back revives a replaced password.
  */
final class UserDirectoriesSuite extends KuiIOSuite {

  private val hash: PasswordHash =
    PasswordHash(PasswordAlgorithm.Pbkdf2HmacSha256, 210_000, "c2FsdA", "aGFzaA")

  private val changed: PasswordHash =
    PasswordHash(PasswordAlgorithm.Pbkdf2HmacSha256, 210_000, "c2FsdDI", "aGFzaDI")

  private val account: UserRecord =
    UserRecord(UserName.unsafe("Admin"), hash, Set("operators"), mustChangePassword = false)

  private val configured: UserDirectory[IO] = ConfiguredUserDirectory.fromRecords[IO](List(account))

  test("an account is found however the person capitalised their name when they signed in") {
    for {
      exact <- configured.find("Admin")
      lower <- configured.find("admin")
      shouty <- configured.find("ADMIN")
      padded <- configured.find("  admin  ")
      absent <- configured.find("someone-else")
    } yield {
      assertEquals(exact.map(_.name.value), Some("Admin"))
      assertEquals(lower.map(_.name.value), Some("Admin"))
      assertEquals(shouty.map(_.name.value), Some("Admin"))
      assertEquals(padded.map(_.name.value), Some("Admin"))
      // The other half of the rule: matching loosely must not mean matching everything.
      assertEquals(absent, None)
    }
  }

  test("case-insensitive account lookup does not depend on the host locale") {
    IO(Locale.getDefault)
      .bracket { _ =>
        IO(Locale.setDefault(Locale.forLanguageTag("tr-TR"))) *>
          IO.defer(ConfiguredUserDirectory.fromRecords[IO](List(account)).find("ADMIN"))
      }(previous => IO(Locale.setDefault(previous)))
      .map { found =>
        assertEquals(found.map(_.name.value), Some("Admin"))
      }
  }

  test("a file-backed deployment refuses a password change and says what to configure instead") {
    configured.update(account.withPassword(changed)).map {
      case Left(refused) =>
        // The message names the key, because "you cannot do that" without "here is how you could" is the
        // refusal that turns into a support ticket.
        assert(refused.message.contains("kui.store"), s"the refusal must name the key: ${refused.message}")
      case Right(()) =>
        fail("a configuration file is the operator's; a KUI that rewrote it would fight whatever deployed it")
    }
  }

  test("a stored password overrides the configured one") {
    for {
      logger <- FakeStructuredLogger[IO]
      stored = StoredUserDirectory.make[IO](configured, holding(payloadOf(changed)), logger)
      found <- stored.find("admin")
    } yield assertEquals(found.map(_.hash), Some(changed))
  }

  test("an unreadable override never resurrects the configured password") {
    for {
      logger <- FakeStructuredLogger[IO]
      stored = StoredUserDirectory.make[IO](configured, holding(Json.obj()), logger)
      found <- stored.find("admin")
      lines <- logger.entries
    } yield {
      assertEquals(found, None)
      assert(lines.nonEmpty, "an unreadable stored password must not be ignored silently")
    }
  }

  test("a metadata store failure denies authentication rather than resurrecting old credentials") {
    for {
      logger <- FakeStructuredLogger[IO]
      stored = StoredUserDirectory.make[IO](configured, unreachable, logger)
      found <- stored.find("admin")
    } yield assertEquals(found, None)
  }

  test("an uninitialized account can still use its configured password") {
    for {
      logger <- FakeStructuredLogger[IO]
      stored = StoredUserDirectory.make[IO](configured, new StubStore {}, logger)
      found <- stored.find("admin")
    } yield assertEquals(found.map(_.hash), Some(hash))
  }

  test("a store-skipped unreadable credential is not an uninitialized account") {
    for {
      logger <- FakeStructuredLogger[IO]
      store = new StubStore {
        override def health: IO[StoreHealth] =
          IO.pure(StoreHealth.ReadOnly("unreadable", List(StoredUserDirectory.keyFor("admin"))))
      }
      found <- StoredUserDirectory.make[IO](configured, store, logger).find("admin")
    } yield assertEquals(found, None)
  }

  test("a disconnected store cannot authorize against its stale credential view") {
    for {
      logger <- FakeStructuredLogger[IO]
      store = new StubStore {
        override def health: IO[StoreHealth] =
          IO.pure(StoreHealth.Degraded("offline", Instant.EPOCH, 0L, Nil))
      }
      found <- StoredUserDirectory.make[IO](configured, store, logger).find("admin")
    } yield assertEquals(found, None)
  }

  test("the store may not introduce an account the configuration does not declare") {
    // A record that could would be a privilege escalation through a password-change endpoint.
    for {
      logger <- FakeStructuredLogger[IO]
      stored = StoredUserDirectory.make[IO](configured, holding(payloadOf(changed)), logger)
      found <- stored.find("nobody")
    } yield assertEquals(found, None)
  }

  test("a password change is written against the version it read, so one change cannot erase another") {
    /*
     * Ungated until now: `existing.map(_.version)` filtered to `None` left
     * `./mill services.identity.__.test` at 79/79 green. The store's `put` takes the base version it is
     * expected to be replacing (ADR-042); passing `None` for a key that exists is a blind write, and two
     * operators changing one account's password at the same time then silently lose one of the two
     * changes, with the loser told it succeeded. The store cannot refuse what it is never told to check.
     */
    for {
      logger <- FakeStructuredLogger[IO]
      writes <- Ref.of[IO, List[(String, Option[Long], Json)]](Nil)
      present = StoredUserDirectory.make[IO](configured, recording(writes, Some(4L)), logger)
      _ <- present.update(account.copy(hash = changed))
      afterExisting <- writes.get
      fresh <- Ref.of[IO, List[(String, Option[Long], Json)]](Nil)
      absent = StoredUserDirectory.make[IO](configured, recording(fresh, None), logger)
      _ <- absent.update(account.copy(hash = changed))
      afterMissing <- fresh.get
    } yield {
      assertEquals(afterExisting.map((_, version, _) => version), List(Some(4L)))
      // And the other direction, which is what makes the first assertion mean "the version it read":
      // a key nothing holds yet is written as a create, not against a version that does not exist.
      assertEquals(afterMissing.map((_, version, _) => version), List(None))
    }
  }

  test("the stored password hash carries the secret marker, which is what makes the store encrypt it") {
    /*
     * Ungated until now: writing the hash as a bare `Json.fromString` left the suite at 79/79 green.
     * ADR-044's field encryption is driven by `SecretJson`'s `$secret` marker and by nothing else -- the
     * write path encrypts the paths `plaintextPaths` finds and asserts that none survives -- so a hash
     * written without the marker is a password hash in the clear on a compacted Kafka topic every
     * replica reads. The class comment claims the marker; nothing checked it.
     */
    for {
      logger <- FakeStructuredLogger[IO]
      writes <- Ref.of[IO, List[(String, Option[Long], Json)]](Nil)
      stored = StoredUserDirectory.make[IO](configured, recording(writes, None), logger)
      _ <- stored.update(account.copy(hash = changed))
      written <- writes.get
    } yield {
      val payload = written.map((_, _, json) => json).headOption.getOrElse(Json.Null)

      assertEquals(SecretJson.plaintextPaths(payload), List("passwordHash"))
      // The value inside the marker is the hash, so the marker is not merely present but wrapping the
      // thing that must not be readable.
      assertEquals(
        payload.hcursor.downField("passwordHash").as[Secret[String]](using SecretJson.decoder).map(_.value),
        Right(changed.encoded)
      )
    }
  }

  /** A store that records what was written and reports whether the key already held anything. */
  private def recording(
      writes: Ref[IO, List[(String, Option[Long], Json)]],
      held: Option[Long]
  ): ConfigStore[IO] =
    new StubStore {
      override def get(key: StoreKey): IO[Option[StoreRecord]] =
        IO.pure(held.map(version => storedAt(key, version)))

      override def put(
          key: StoreKey,
          payload: Json,
          baseVersion: Option[Long],
          updatedBy: String
      ): IO[Either[KuiError, StoreRecord]] =
        writes
          .update(_ :+ (key.render, baseVersion, payload))
          .as(Right(storedAt(key, baseVersion.getOrElse(0L) + 1L)))
    }

  private def storedAt(key: StoreKey, version: Long): StoreRecord =
    StoreRecord(
      envelopeVersion = 1,
      key = key,
      version = version,
      updatedAt = Instant.parse("2026-09-06T10:00:00Z"),
      updatedBy = "admin",
      deleted = false,
      payload = payloadOf(hash)
    )

  private def payloadOf(value: PasswordHash): Json =
    Json.fromJsonObject(JsonObject("passwordHash" -> SecretJson.encoder(Secret(value.encoded))))

  /** A store holding one record, under whichever key it is asked for. */
  private def holding(payload: Json): ConfigStore[IO] =
    new StubStore {
      override def get(key: StoreKey): IO[Option[StoreRecord]] =
        IO.pure(
          Some(
            StoreRecord(
              envelopeVersion = 1,
              key = key,
              version = 1L,
              updatedAt = Instant.parse("2026-09-06T10:00:00Z"),
              updatedBy = "admin",
              deleted = false,
              payload = payload
            )
          )
        )
    }

  private def unreachable: ConfigStore[IO] =
    new StubStore {
      override def get(key: StoreKey): IO[Option[StoreRecord]] =
        IO.raiseError(new RuntimeException("the metadata store is not reachable"))
    }

  abstract private class StubStore extends ConfigStore[IO] {
    def get(key: StoreKey): IO[Option[StoreRecord]] = IO.pure(None)
    def list(section: StoreSection): IO[List[StoreRecord]] = IO.pure(Nil)

    def put(
        key: StoreKey,
        payload: Json,
        baseVersion: Option[Long],
        updatedBy: String
    ): IO[Either[KuiError, StoreRecord]] = IO.raiseError(new RuntimeException("not reached"))

    def delete(key: StoreKey, baseVersion: Long, updatedBy: String): IO[Either[KuiError, Unit]] =
      IO.raiseError(new RuntimeException("not reached"))

    def changes: Stream[IO, StoreChange] = Stream.empty
    def health: IO[StoreHealth] = IO.pure(StoreHealth.ReadOnly("a stub", Nil))
  }
}
