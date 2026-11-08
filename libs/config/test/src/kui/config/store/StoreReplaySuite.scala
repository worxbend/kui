package kui.config.store

import java.time.Instant

import scala.concurrent.duration.*

import cats.effect.testkit.TestControl
import cats.effect.{Deferred, IO, Ref, Resource}
import fs2.Stream
import io.circe.Json
import io.circe.syntax.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpFactory

import kui.kernel.error.ErrorCode
import kui.testkit.KuiIOSuite

/** That replay terminates, that it fails by name when it cannot, and that cancelling it leaves nothing
  * running.
  *
  * This is the milestone's highest-risk loop. The roadmap's own risk register says a bug in the store's
  * bootstrap makes the service **hang** rather than fail, which is the worst failure shape a startup path can
  * have: no error, no readiness, and nothing to search a log for. The mitigation is entirely in this file's
  * subject — the end offset is taken once, the loop stops at it, and the whole thing is under a timeout that
  * names how far it got.
  *
  * It runs against a fake log rather than a broker on purpose. A container could not express "the log never
  * produces the record replay is waiting for", which is exactly the case that has to fail fast.
  */
final class StoreReplaySuite extends KuiIOSuite {

  private val logger: Logger[IO] = NoOpFactory[IO].getLogger

  private val at = Instant.parse("2026-09-03T10:00:00Z")
  private val crypto: FieldCrypto[IO] = FieldCrypto[IO](
    EncryptionKeyring
      .of(
        List(
          EncryptionKey
            .fromBase64("k1", "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=")
            .fold(e => sys.error(e.message), identity)
        ),
        "k1"
      )
      .fold(e => sys.error(e.message), identity)
  )

  private def undecryptable(id: String, version: Long, offset: Long): KafkaConfigStore.LogRecord = {
    val key = StoreKey(StoreSection.Cluster, id)
    val payload = Json.obj(
      "password" -> Json.obj(
        SecretJson.CipherField -> Json.obj(
          "alg" -> Json.fromString("AES-256-GCM"),
          "keyId" -> Json.fromString("gone"),
          "iv" -> Json.fromString("AAAAAAAAAAAAAAAA"),
          "ct" -> Json.fromString("AAAAAAAAAAAAAAAAAAAAAA==")
        )
      )
    )
    val record = StoreRecord(1, key, version, at, "test", deleted = false, payload)
    KafkaConfigStore.LogRecord(key.render, Some(record.asJson.noSpaces), offset)
  }

  private def entry(id: String, version: Long, offset: Long): KafkaConfigStore.LogRecord = {
    val key = StoreKey(StoreSection.Cluster, id)
    val record = StoreRecord(1, key, version, at, "test", deleted = false, Json.obj())
    KafkaConfigStore.LogRecord(key.render, Some(record.asJson.noSpaces), offset)
  }

  private def fakeLog(
      end: Long,
      entries: Stream[IO, KafkaConfigStore.LogRecord]
  ): KafkaConfigStore.StoreLog[IO] =
    new KafkaConfigStore.StoreLog[IO] {
      def endOffset: IO[Long] = IO.pure(end)
      def records: Stream[IO, KafkaConfigStore.LogRecord] = entries
    }

  private def run(
      end: Long,
      records: Stream[IO, KafkaConfigStore.LogRecord],
      timeout: FiniteDuration = 30.seconds
  ): IO[(Either[Throwable, Unit], StoreState)] =
    for {
      state <- Ref.of[IO, StoreState](StoreState.empty)
      changes <- StoreChanges.fanout[IO](KafkaConfigStore.ChangeBufferSize)
      outcome <- KafkaConfigStore
        .replay[IO](fakeLog(end, records), state, changes, crypto, "__kui_config", end, timeout, logger)
        .attempt
      finalState <- state.get
    } yield (outcome, finalState)

  test("anEmptyLogCompletesImmediatelyAndNeverWaitsForARecord") {
    // Waiting for a record that will never come is precisely the hang this whole design avoids. The
    // stream here never emits: if replay consulted it at all, this test would time out.
    TestControl.executeEmbed(run(0L, Stream.never[IO])).map { (outcome, state) =>
      assert(outcome.isRight, outcome.toString)
      assertEquals(state.lastAppliedOffset, -1L)
    }
  }

  test("replayStopsAtTheEndOffsetItTookBeforeConsuming") {
    // The log keeps producing after the end offset. Replay must not chase it: a replay that followed a
    // moving target would never terminate on a busy topic.
    val infinite =
      Stream.emits((0 until 100).toList.map(i => entry(s"c$i", 1L, i.toLong))) ++ Stream.never[IO]
    TestControl.executeEmbed(run(3L, infinite)).map { (outcome, state) =>
      assert(outcome.isRight, outcome.toString)
      assertEquals(state.lastAppliedOffset, 2L)
      assertEquals(state.records.size, 3)
    }
  }

  test("aReplayThatCannotReachTheEndFailsByNameInsideItsTimeout") {
    // The named exit criterion: a bounded failure instead of a hang. The log delivers one record and
    // then stalls for ever, so replay can never reach offset 9.
    val stalls = Stream.emit(entry("c0", 1L, 0L)) ++ Stream.never[IO]
    TestControl.executeEmbed(run(10L, stalls, timeout = 30.seconds)).map { (outcome, _) =>
      outcome match {
        case Left(failure: StoreFailure) =>
          failure.error match {
            case StoreError.ReplayTimeout(topic, reached, endOffset, afterMs) =>
              assertEquals(topic, "__kui_config")
              assertEquals(reached, 0L)
              assertEquals(endOffset, 10L)
              assertEquals(afterMs, 30000L)
              // All three numbers in the message: "reached 0 of 10" tells an operator to raise the
              // timeout or look at the store cluster; "replay timed out" tells them nothing.
              assert(failure.error.message.contains("offset 0 of 10"), failure.error.message)
              assertEquals(failure.error.code.wire, "KUI-STORE-REPLAY-TIMEOUT")
            case other => fail(s"expected ReplayTimeout, got $other")
          }
        case other => fail(s"expected a ReplayTimeout failure, got $other")
      }
    }
  }

  test("cancellingReplayReleasesTheLogPromptlyRatherThanWaitingOutTheTimeout") {
    // Cancelling the bootstrap while replay is in flight must complete promptly and must run the log's
    // release. Waiting out a 30-second replay timeout on a Ctrl-C is a shutdown nobody accepts.
    val test = for {
      released <- Ref.of[IO, Int](0)
      started <- Deferred[IO, Unit]
      stalling = Stream.eval(started.complete(()).void).drain ++ Stream.never[IO]
      log = stalling.onFinalize(released.update(_ + 1))
      fiber <- run(10L, log, timeout = 30.seconds).start
      _ <- started.get
      _ <- fiber.cancel
      count <- released.get
    } yield count
    TestControl
      .executeEmbed(test)
      .map(count => assertEquals(count, 1, "the log's release must run exactly once"))
  }

  test("cancellingTheFollowerReleasesTheLogExactlyOnce") {
    val test = for {
      released <- Ref.of[IO, Int](0)
      started <- Deferred[IO, Unit]
      state <- Ref.of[IO, StoreState](StoreState.empty)
      changes <- StoreChanges.fanout[IO](KafkaConfigStore.ChangeBufferSize)
      log = (Stream.eval(started.complete(()).void).drain ++ Stream.never[IO])
        .onFinalize(released.update(_ + 1))
      fiber <- KafkaConfigStore
        .follow[IO](fakeLog(0L, log), state, changes, crypto, None, logger, _ => IO.unit)
        .start
      _ <- started.get
      _ <- fiber.cancel
      outcome <- fiber.join
      count <- released.get
    } yield (outcome.isCanceled, count)
    TestControl.executeEmbed(test).map { (canceled, count) =>
      assert(canceled, "the follower should observe the cancellation rather than complete")
      assertEquals(count, 1)
    }
  }

  test("oneUnreadableRecordCostsOneKeyAndNoMore") {
    // ADR-042 §8's "keep serving from last known state", at the level of a single record: a hand-edited
    // or undecryptable entry must not stop KUI from serving the other clusters.
    val garbage = KafkaConfigStore.LogRecord("cluster/broken", Some("{not json"), 1L)
    val log = Stream.emits(List(entry("good", 1L, 0L), garbage, entry("later", 1L, 2L)))
    TestControl.executeEmbed(run(3L, log)).map { (outcome, state) =>
      assert(outcome.isRight, outcome.toString)
      assertEquals(state.get(StoreKey(StoreSection.Cluster, "good")).map(_.version), Some(1L))
      assertEquals(state.get(StoreKey(StoreSection.Cluster, "later")).map(_.version), Some(1L))
      assertEquals(state.unreadableKeys.map(_.render), List("cluster/broken"))
    }
  }

  test("aRecordEncryptedUnderAKeyThatIsGoneIsUnreadableRatherThanFatal") {
    val key = StoreKey(StoreSection.Cluster, "rotated")
    val payload = Json.obj(
      "password" -> Json.obj(
        SecretJson.CipherField -> Json.obj(
          "alg" -> Json.fromString("AES-256-GCM"),
          "keyId" -> Json.fromString("gone"),
          "iv" -> Json.fromString("AAAAAAAAAAAAAAAA"),
          "ct" -> Json.fromString("AAAAAAAAAAAAAAAAAAAAAA==")
        )
      )
    )
    val record = StoreRecord(1, key, 1L, at, "test", deleted = false, payload)
    val log = Stream.emits(
      List(KafkaConfigStore.LogRecord(key.render, Some(record.asJson.noSpaces), 0L), entry("fine", 1L, 1L))
    )
    TestControl.executeEmbed(run(2L, log)).map { (outcome, state) =>
      assert(outcome.isRight, outcome.toString)
      assertEquals(state.get(key), None)
      assertEquals(state.unreadableKeys.map(_.render), List("cluster/rotated"))
      assertEquals(state.get(StoreKey(StoreSection.Cluster, "fine")).map(_.version), Some(1L))
    }
  }

  test("anUndecryptableEnvelopeKeepsItsVersionSoTheNextRecordApplies") {
    // The envelope parsed, so its version is known, even though the payload was encrypted under a key
    // this replica does not have. Dropping that version keeps serving v1 and then ignores the readable
    // v3 every other replica accepts.
    val key = StoreKey(StoreSection.Cluster, "prod")
    val log = Stream.emits(List(entry("prod", 1L, 0L), undecryptable("prod", 2L, 1L), entry("prod", 3L, 2L)))
    TestControl.executeEmbed(run(3L, log)).map { (outcome, state) =>
      assert(outcome.isRight, outcome.toString)
      assertEquals(state.get(key).map(_.version), Some(3L))
      assertEquals(state.unreadableKeys, Nil)
      assertEquals(state.nextVersion(key), 4L)
    }
  }

  test("aWriteBasedOnTheRecordAnUndecryptableEnvelopeSupersededConflicts") {
    // Replica B still holds v1, cannot read v2, and lets an edit through against v1. That write is
    // version 2 locally and version-ignored by every replica that already applied v2.
    val key = StoreKey(StoreSection.Cluster, "prod")
    val program = for {
      state <- Ref.of[IO, StoreState](StoreState.empty)
      changes <- StoreChanges.fanout[IO](KafkaConfigStore.ChangeBufferSize)
      _ <- KafkaConfigStore.replay[IO](
        fakeLog(2L, Stream.emits(List(entry("prod", 1L, 0L), undecryptable("prod", 2L, 1L)))),
        state,
        changes,
        crypto,
        "__kui_config",
        2L,
        30.seconds,
        logger
      )
      waiter <- WriteWaiter.create[IO]
      health <- StoreHealthRef.of[IO](StoreHealth.Healthy(1L, at, Nil))
      sink = new KafkaConfigStore.RecordSink[IO] {
        def produce(topic: String, key: String, value: Option[String]) =
          IO.raiseError(new AssertionError(s"produced a write for $key based on a superseded record"))
      }
      store = KafkaConfigStore.writable[IO](
        state,
        changes,
        waiter,
        health,
        sink,
        crypto,
        "__kui_config",
        5.seconds,
        logger
      )
      written <- store.put(key, Json.obj("displayName" -> Json.fromString("edited")), Some(1L), "replica-b")
      after <- state.get
    } yield (written, after)
    TestControl.executeEmbed(program).map { (written, after) =>
      assertEquals(after.get(key), None)
      assertEquals(after.nextVersion(key), 3L)
      written match {
        case Left(error) => assertEquals(error.code, ErrorCode.ConfigVersionConflict)
        case Right(record) => fail(s"the stale edit was saved at version ${record.version}")
      }
    }
  }

  test("aCompactedLogKeepsEveryKeyAtTheVersionThatSurvived") {
    // What `__kui_config` looks like once the cleaner has run: only the newest record of each key, at its
    // original offset. Every one of them is a key the store holds, and none is ignored for not being v1.
    val gone = StoreKey(StoreSection.Cluster, "gone")
    val deleted = StoreRecord.tombstone(gone, 2L, "test", at)
    val log = Stream.emits(
      List(
        entry("analytics", 3L, 4L),
        entry("fresh", 1L, 7L),
        KafkaConfigStore.LogRecord(gone.render, Some(deleted.asJson.noSpaces), 9L)
      )
    )
    TestControl.executeEmbed(run(10L, log)).map { (outcome, state) =>
      assert(outcome.isRight, outcome.toString)
      assertEquals(state.get(StoreKey(StoreSection.Cluster, "analytics")).map(_.version), Some(3L))
      assertEquals(state.get(StoreKey(StoreSection.Cluster, "fresh")).map(_.version), Some(1L))
      assertEquals(state.get(gone), None)
      assertEquals(state.nextVersion(gone), 3L)
      assertEquals(state.outcomes.count(_._2.isInstanceOf[StoreApplied.Ignored]), 0)
    }
  }

  test("aPhysicalTombstoneDeletesAKeyThatExists") {
    // The key exists when the `null` arrives. It must be gone afterwards, and it must be gone the same way a
    // replica that replays after compaction sees it gone: not there at all, so a re-create is version 1.
    val key = StoreKey(StoreSection.Cluster, "x")
    val log = Stream.emits(
      List(entry("x", 1L, 0L), entry("x", 2L, 1L), KafkaConfigStore.LogRecord(key.render, None, 2L))
    )
    TestControl.executeEmbed(run(3L, log)).map { (outcome, state) =>
      assert(outcome.isRight, outcome.toString)
      assertEquals(state.get(key), None)
      assertEquals(state.nextVersion(key), 1L)
    }
  }

  test("aFollowerThatLosesTheBrokerBecomesHealthyAgainWhenItReads") {
    // A SASL failure degrades the store. The follower retries on a new log, and once the broker accepts it
    // again the store has to be Healthy: otherwise every write stays KUI-STORE-UNREACHABLE until restart.
    val key = StoreKey(StoreSection.Cluster, "analytics")
    val fresh = StoreKey(StoreSection.Cluster, "fresh")
    val program = for {
      state <- Ref.of[IO, StoreState](StoreState.empty)
      changes <- StoreChanges.fanout[IO](KafkaConfigStore.ChangeBufferSize)
      waiter <- WriteWaiter.create[IO]
      health <- StoreHealthRef.of[IO](StoreHealth.Healthy(-1L, at, Nil))
      attempts <- Ref.of[IO, Int](0)
      opened <- Ref.of[IO, Int](0)
      sawDegraded <- Ref.of[IO, Boolean](false)
      produced <- Deferred[IO, KafkaConfigStore.LogRecord]
      log = new KafkaConfigStore.StoreLog[IO] {
        def endOffset: IO[Long] = IO.pure(2L)
        def records: Stream[IO, KafkaConfigStore.LogRecord] =
          Stream.eval(attempts.updateAndGet(_ + 1)).flatMap { n =>
            if n == 1 then
              Stream.raiseError[IO](new org.apache.kafka.common.errors.SaslAuthenticationException("rotated"))
            else Stream.emit(entry("analytics", 1L, 0L)) ++ Stream.eval(produced.get)
          }
      }
      open = Resource.eval(opened.update(_ + 1)).map(_ => log)
      sink = new KafkaConfigStore.RecordSink[IO] {
        def produce(topic: String, key: String, value: Option[String]) =
          produced.complete(KafkaConfigStore.LogRecord(key, value, 1L)).map(_ => Right(1L))
      }
      store = KafkaConfigStore.writable[IO](
        state,
        changes,
        waiter,
        health,
        sink,
        crypto,
        "__kui_config",
        5.seconds,
        logger
      )
      fiber <- KafkaConfigStore
        .followForever[IO](open, state, changes, crypto, waiter, health, "kafka:9092", logger)
        .start
      _ <- waitUntilServing(health, state, sawDegraded, key)
      written <- store.put(fresh, Json.obj(), None, "test")
      _ <- fiber.join
      now <- health.get
      degraded <- sawDegraded.get
      opens <- opened.get
      version <- state.get.map(_.get(key).map(_.version))
      created <- state.get.map(_.get(fresh).map(_.version))
    } yield (degraded, now, version, written, opens, created)
    TestControl.executeEmbed(program).map { (degraded, now, version, written, opens, created) =>
      assert(degraded, "the authentication failure must degrade the store")
      assert(opens >= 2, s"each attempt needs its own log, opened $opens")
      assertEquals(version, Some(1L))
      now match {
        case StoreHealth.Healthy(_, _, _) => ()
        case other => fail(s"the follower resumed but the store stayed $other")
      }
      written match {
        case Right(_) => ()
        case Left(error) => fail(s"write refused after recovery: ${error.code.wire} ${error.message}")
      }
      assertEquals(created, Some(1L))
    }
  }

  private def waitUntilServing(
      health: StoreHealthRef[IO],
      state: Ref[IO, StoreState],
      sawDegraded: Ref[IO, Boolean],
      key: StoreKey
  ): IO[Unit] =
    health.get.flatMap { reported =>
      val mark = reported match {
        case StoreHealth.Degraded(_, _, _, _) => sawDegraded.set(true)
        case _ => IO.unit
      }
      mark.flatMap { _ =>
        state.get.flatMap { current =>
          val serving = reported match {
            case StoreHealth.Healthy(_, _, _) => current.get(key).isDefined
            case _ => false
          }
          if serving then IO.unit
          else IO.sleep(5.millis).flatMap(_ => waitUntilServing(health, state, sawDegraded, key))
        }
      }
    }

  test("aSubscriberThatDoesNotReadDoesNotStallTheFold") {
    // 300 accepts and a mailbox of 256. A subscriber that is registered and not reading must not
    // back-pressure the fold, and it must be told that it lost changes.
    val records = Stream.emits((0 until 300).toList.map(i => entry(s"c$i", 1L, i.toLong)))
    val program = for {
      state <- Ref.of[IO, StoreState](StoreState.empty)
      changes <- StoreChanges.fanout[IO](KafkaConfigStore.ChangeBufferSize)
      seen <- changes.listen.use { pending =>
        for {
          _ <- KafkaConfigStore.replay[IO](
            fakeLog(300L, records),
            state,
            changes,
            crypto,
            "__kui_config",
            300L,
            30.seconds,
            logger
          )
          offset <- state.get.map(_.lastAppliedOffset)
          taken <- pending.take(KafkaConfigStore.ChangeBufferSize.toLong).compile.toVector
        } yield (offset, taken)
      }
    } yield seen
    TestControl.executeEmbed(program).map { (offset, seen) =>
      assertEquals(offset, 299L)
      assert(
        seen.exists(_.isInstanceOf[StoreChange.Desynchronized]),
        s"expected Desynchronized, saw ${seen.size} changes"
      )
    }
  }

  test("aPhysicalTombstoneDeletesTheKey") {
    // KUI writes logical tombstones, but a compacted or hand-edited log contains null values, so they
    // are honoured rather than skipped.
    val key = StoreKey(StoreSection.Cluster, "gone")
    val log = Stream.emits(List(KafkaConfigStore.LogRecord(key.render, None, 0L)))
    TestControl.executeEmbed(run(1L, log)).map { (outcome, state) =>
      assert(outcome.isRight, outcome.toString)
      assertEquals(state.get(key), None)
    }
  }
}
