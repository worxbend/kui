package kui.alerts.infrastructure

import java.time.Instant

import scala.concurrent.duration.DurationInt

import cats.effect.{Deferred, IO, Ref}
import fs2.Stream
import io.circe.Json

import kui.alerts.domain.*
import kui.config.store.*
import kui.kernel.error.KuiError
import kui.kernel.{ClusterId, UserName}
import kui.security.{Principal, PrincipalKind}
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

final class DurableAlertStoreSuite extends KuiIOSuite {

  private val cluster = ClusterId.unsafe("prod-eu")
  private val at = Instant.parse("2026-09-21T12:00:00Z")
  private val reader = Principal(UserName.unsafe("ada@example.com"), Set.empty, PrincipalKind.Session)

  private def event: AlertEvent =
    AlertEvent.open(
      AlertKey(AlertRule.DiskUsage, "broker-1:/var/lib/kafka"),
      AlertSeverity.Warning,
      at,
      "Broker 1 disk usage is high",
      "82% used"
    )

  private def opening(opened: AlertEvent): Evaluation =
    Evaluation(
      opened = List(opened),
      refreshed = Nil,
      resolved = Nil,
      state = AlertRuleState.empty,
      reports = AlertRule.All.map(RuleReport(_, RuleOutcome.evaluated))
    )

  test("a continuing alert stays read after the store is reconstructed") {
    for {
      metadata <- TestConfigStore.create
      logger <- FakeStructuredLogger[IO]
      first = DurableAlertStore[IO](metadata, 7.days, logger)
      _ <- first.record(cluster, opening(event), at)
      marked <- first.feed(cluster, reader, 100, Some(at.plusSeconds(30)))
      _ = assertEquals(marked.unreadCount, 0)
      second = DurableAlertStore[IO](metadata, 7.days, logger)
      restored <- second.feed(cluster, reader, 100, None)
      _ = assertEquals(restored.events.map(_.id), List(event.id))
      _ = assertEquals(restored.lastReadAt, Some(at.plusSeconds(30)))
      _ = assertEquals(restored.unreadCount, 0)
    } yield ()
  }

  test("an acknowledgement and its actor survive reconstruction") {
    for {
      metadata <- TestConfigStore.create
      logger <- FakeStructuredLogger[IO]
      first = DurableAlertStore[IO](metadata, 7.days, logger)
      _ <- first.record(cluster, opening(event), at)
      acknowledged <- first.acknowledge(cluster, event.id, at.plusSeconds(45), "ada")
      _ = assert(acknowledged.exists(!_.isOpen), acknowledged)
      second = DurableAlertStore[IO](metadata, 7.days, logger)
      restored <- second.feed(cluster, reader, 100, None)
      resolution = restored.events.headOption.flatMap(_.resolution)
      _ = assertEquals(resolution.map(_.kind), Some(AlertResolutionKind.Acknowledged))
      _ = assertEquals(resolution.flatMap(_.by), Some("ada"))
    } yield ()
  }

  test("an evaluation losing its write race retries without clearing the acknowledgement") {
    for {
      metadata <- TestConfigStore.create
      logger <- FakeStructuredLogger[IO]
      entered <- Deferred[IO, Unit]
      resume <- Deferred[IO, Unit]
      armed <- Ref.of[IO, Boolean](false)
      delayed = new ConfigStore[IO] {
        def get(key: StoreKey) = metadata.get(key)
        def list(section: StoreSection) = metadata.list(section)
        def health = metadata.health
        def changes = metadata.changes
        def delete(key: StoreKey, version: Long, by: String) = metadata.delete(key, version, by)
        def put(key: StoreKey, payload: Json, baseVersion: Option[Long], updatedBy: String) =
          armed.getAndSet(false).flatMap { pause =>
            (if pause then entered.complete(()).void *> resume.get else IO.unit) *>
              metadata.put(key, payload, baseVersion, updatedBy)
          }
      }
      store = DurableAlertStore[IO](delayed, 7.days, logger)
      acknowledger = DurableAlertStore[IO](metadata, 7.days, logger)
      _ <- store.record(cluster, opening(event), at)
      _ <- armed.set(true)
      writer <- store
        .record(cluster, Evaluation(Nil, Nil, List(event.id), AlertRuleState.empty, Nil), at.plusSeconds(60))
        .start
      _ <- entered.get
      acknowledged <- acknowledger.acknowledge(cluster, event.id, at.plusSeconds(30), "ada")
      _ <- resume.complete(())
      _ <- writer.joinWithNever
      feed <- store.feed(cluster, reader, 10, None)
    } yield assertEquals(feed.events, acknowledged.toOption.toList)
  }

  test("a cleared condition can open a new unread event later") {
    val clearedAt = at.plusSeconds(60)
    val reopened = AlertEvent.open(event.key, event.severity, at.plusSeconds(120), event.title, event.detail)

    for {
      metadata <- TestConfigStore.create
      logger <- FakeStructuredLogger[IO]
      store = DurableAlertStore[IO](metadata, 7.days, logger)
      _ <- store.record(cluster, opening(event), at)
      _ <- store.feed(cluster, reader, 100, Some(at.plusSeconds(30)))
      _ <- store.record(
        cluster,
        Evaluation(Nil, Nil, List(event.id), AlertRuleState.empty, Nil),
        clearedAt
      )
      _ <- store.record(cluster, opening(reopened), reopened.openedAt)
      restored = DurableAlertStore[IO](metadata, 7.days, logger)
      feed <- restored.feed(cluster, reader, 100, None)
      _ = assertEquals(feed.unreadCount, 1)
      _ = assert(feed.events.exists(_.id == reopened.id), feed.events)
    } yield ()
  }

  test("a concurrent opening for an already-open condition becomes a refresh") {
    val racing = AlertEvent.open(
      event.key,
      AlertSeverity.Critical,
      at.plusSeconds(1),
      "Disk usage is critical",
      "95% used"
    )

    for {
      metadata <- TestConfigStore.create
      logger <- FakeStructuredLogger[IO]
      store = DurableAlertStore[IO](metadata, 7.days, logger)
      _ <- store.record(cluster, opening(event), at)
      _ <- store.record(cluster, opening(racing), racing.openedAt)
      feed <- store.feed(cluster, reader, 100, None)
      _ = assertEquals(feed.events.map(_.id), List(event.id))
      _ = assertEquals(feed.events.headOption.map(_.lastSeenAt), Some(racing.openedAt))
      _ = assertEquals(feed.events.headOption.map(_.severity), Some(AlertSeverity.Critical))
      _ = assertEquals(feed.events.headOption.map(_.title), Some(racing.title))
      _ = assertEquals(feed.events.headOption.map(_.detail), Some(racing.detail))
      _ = assertEquals(feed.openCount, 1)
    } yield ()
  }

  test("an older delayed evaluation cannot move the durable projection backwards") {
    val newer = at.plusSeconds(120)
    val stale = at.plusSeconds(60)

    for {
      metadata <- TestConfigStore.create
      logger <- FakeStructuredLogger[IO]
      store = DurableAlertStore[IO](metadata, 7.days, logger)
      _ <- store.record(cluster, opening(event), at)
      _ <- store.record(
        cluster,
        Evaluation(Nil, List(event), Nil, AlertRuleState.empty, Nil),
        newer
      )
      _ <- store.record(
        cluster,
        Evaluation(Nil, List(event), Nil, AlertRuleState.empty, Nil),
        stale
      )
      feed <- store.feed(cluster, reader, 100, None)
      _ = assertEquals(feed.evaluatedAt, Some(newer))
      _ = assertEquals(feed.events.headOption.map(_.lastSeenAt), Some(newer))
    } yield ()
  }

  test("an unsupported durable projection fails closed instead of reopening alerts") {
    val key = StoreKey(StoreSection.Alerts, cluster.value)
    val unsupported = Json.obj("formatVersion" -> Json.fromInt(999))

    for {
      metadata <- TestConfigStore.create
      _ <- metadata.put(key, unsupported, None, "test")
      logger <- FakeStructuredLogger[IO]
      store = DurableAlertStore[IO](metadata, 7.days, logger)
      result <- store.feed(cluster, reader, 100, None).attempt
      _ = assert(result.isLeft, result)
      _ = assert(
        result.swap.toOption.exists(_.getMessage.contains("unsupported alert formatVersion")),
        result
      )
    } yield ()
  }
}

final private class TestConfigStore private (records: Ref[IO, Map[StoreKey, StoreRecord]])
    extends ConfigStore[IO] {
  def get(key: StoreKey): IO[Option[StoreRecord]] = records.get.map(_.get(key))
  def list(section: StoreSection): IO[List[StoreRecord]] =
    records.get.map(_.values.filter(_.key.section == section).toList.sortBy(_.key.render))
  def put(
      key: StoreKey,
      payload: Json,
      baseVersion: Option[Long],
      updatedBy: String
  ): IO[Either[KuiError, StoreRecord]] =
    records.modify { held =>
      val current = held.get(key)
      if current.map(_.version) != baseVersion then held -> Left(TestConfigStore.conflict)
      else {
        val next = current.fold(StoreRecord.create(key, payload, updatedBy, TestConfigStore.At))(
          StoreRecord.next(_, payload, updatedBy, TestConfigStore.At)
        )
        held.updated(key, next) -> Right(next)
      }
    }
  def delete(key: StoreKey, baseVersion: Long, updatedBy: String): IO[Either[KuiError, Unit]] =
    records.update(_ - key).as(Right(()))
  def changes: Stream[IO, StoreChange] = Stream.never
  def health: IO[StoreHealth] = IO.pure(StoreHealth.Healthy(0L, TestConfigStore.At, Nil))
}

private object TestConfigStore {
  val At: Instant = Instant.parse("2026-09-21T10:00:00Z")
  val conflict: KuiError = kui.kernel.error.ApplicationError.Remote(
    kui.kernel.error.ErrorCode.ConfigVersionConflict,
    "conflict",
    Nil
  )
  def create: IO[TestConfigStore] =
    Ref.of[IO, Map[StoreKey, StoreRecord]](Map.empty).map(new TestConfigStore(_))
}
