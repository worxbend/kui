package kui.config.store

import java.time.Instant

import cats.effect.{IO, Ref}
import fs2.Stream
import io.circe.Json
import io.circe.syntax.*

import kui.kernel.error.KuiError
import kui.kernel.{ClusterId, RoleName, UserName}
import kui.security.{Principal, PrincipalKind}
import kui.testkit.KuiIOSuite

final class UserStateStoreSuite extends KuiIOSuite {

  private val cluster = ClusterId.unsafe("prod-eu")
  private val alice =
    Principal(UserName.unsafe("alice@example.com"), Set(RoleName.unsafe("ops")), PrincipalKind.Session)
  private val appearance = UiAppearance(UiTheme.Dark, UiAccent.Teal, UiDensity.Compact)
  private val messageBrowser = MessageBrowserSettings(250, MessageViewMode.Infinite)
  private val readAt = Instant.parse("2026-09-21T12:00:00Z")

  test("principal keys are stable, cluster scoped and contain no identity") {
    val same = UserStateKey.of(cluster, alice)
    val otherCluster = UserStateKey.of(ClusterId.unsafe("prod-us"), alice)
    val otherKind = UserStateKey.of(cluster, alice.copy(kind = PrincipalKind.Bearer))

    assertEquals(same, UserStateKey.of(cluster, alice))
    assertNotEquals(same, otherCluster)
    assertNotEquals(same, otherKind)
    assert(!same.render.contains("alice"), same.render)
    assert(!same.render.contains("example"), same.render)
    assertEquals(StoreKey.parse(same.render), Right(same))
  }

  test("appearance, message browsing and alert watermark survive reconstruction") {
    for {
      config <- RefStore.create
      first = UserStateStore[IO](config)
      saved <- first.putAppearance(cluster, alice, appearance)
      _ = assertEquals(saved, Right(appearance))
      browsing <- first.putMessageBrowser(cluster, alice, messageBrowser)
      _ = assertEquals(browsing, Right(messageBrowser))
      marked <- first.markAlertsRead(cluster, alice, readAt)
      _ = assertEquals(marked, Right(readAt))
      second = UserStateStore[IO](config)
      restored <- second.get(cluster, alice)
      _ = assertEquals(restored, Right(UserState(Some(appearance), Some(messageBrowser), Some(readAt))))
    } yield ()
  }

  test("independent writes preserve the field they do not own") {
    for {
      config <- RefStore.create
      state = UserStateStore[IO](config)
      _ <- state.markAlertsRead(cluster, alice, readAt)
      _ <- state.putAppearance(cluster, alice, appearance)
      _ <- state.putMessageBrowser(cluster, alice, messageBrowser)
      afterAppearance <- state.get(cluster, alice)
      _ = assertEquals(
        afterAppearance,
        Right(UserState(Some(appearance), Some(messageBrowser), Some(readAt)))
      )
      later = readAt.plusSeconds(60)
      _ <- state.markAlertsRead(cluster, alice, later)
      afterRead <- state.get(cluster, alice)
      _ = assertEquals(afterRead, Right(UserState(Some(appearance), Some(messageBrowser), Some(later))))
    } yield ()
  }

  test("a conflict is retried against the winning record instead of losing its field") {
    for {
      underlying <- RefStore.create
      racing <- ConflictOnceStore.create(underlying, UserStateKey.of(cluster, alice), readAt)
      state = UserStateStore[IO](racing)
      saved <- state.putAppearance(cluster, alice, appearance)
      _ = assertEquals(saved, Right(appearance))
      restored <- state.get(cluster, alice)
      _ = assertEquals(restored, Right(UserState(Some(appearance), None, Some(readAt))))
    } yield ()
  }

  test("a record the store could not read is an error rather than a principal with no state") {
    // The store names the key as unreadable and so cannot serve it. Reading that as "no state" resets the
    // principal's settings and alert watermark, and the next write would replace the record for good.
    val key = UserStateKey.of(cluster, alice)
    val unreadable = new ConfigStore[IO] {
      def get(k: StoreKey): IO[Option[StoreRecord]] = IO.pure(None)
      def list(section: StoreSection): IO[List[StoreRecord]] = IO.pure(Nil)
      def put(k: StoreKey, payload: Json, baseVersion: Option[Long], by: String) =
        IO.raiseError(new AssertionError(s"wrote $k over a record nobody could read"))
      def delete(k: StoreKey, baseVersion: Long, by: String) = IO.pure(Right(()))
      def changes: Stream[IO, StoreChange] = Stream.never
      def health: IO[StoreHealth] = IO.pure(StoreHealth.Healthy(3L, RefStore.At, List(key)))
    }
    val state = UserStateStore[IO](unreadable)
    for {
      read <- state.get(cluster, alice)
      marked <- state.markAlertsRead(cluster, alice, readAt)
    } yield {
      assertEquals(read.left.toOption.map(_.code.wire), Some("KUI-STORE-ENVELOPE"))
      assertEquals(marked.left.toOption.map(_.code.wire), Some("KUI-STORE-ENVELOPE"))
    }
  }

  test("unknown appearance values make the stored state unreadable rather than silently defaulting") {
    for {
      config <- RefStore.create
      key = UserStateKey.of(cluster, alice)
      _ <- config.put(
        key,
        Json.obj(
          "formatVersion" -> Json.fromInt(1),
          "appearance" -> Json.obj(
            "theme" -> Json.fromString("solarized"),
            "accent" -> Json.fromString("teal"),
            "density" -> Json.fromString("compact")
          )
        ),
        None,
        "test"
      )
      restored <- UserStateStore[IO](config).get(cluster, alice)
      _ = assert(restored.isLeft, restored)
      _ = assertEquals(restored.left.toOption.map(_.code.wire), Some("KUI-STORE-ENVELOPE"))
    } yield ()
  }
}

final private class RefStore private (records: Ref[IO, Map[StoreKey, StoreRecord]]) extends ConfigStore[IO] {
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
      if current.map(_.version) != baseVersion then held -> Left(RefStore.conflict)
      else {
        val next = current.fold(StoreRecord.create(key, payload, updatedBy, RefStore.At))(
          StoreRecord.next(_, payload, updatedBy, RefStore.At)
        )
        held.updated(key, next) -> Right(next)
      }
    }
  def delete(key: StoreKey, baseVersion: Long, updatedBy: String): IO[Either[KuiError, Unit]] =
    records.update(_ - key).as(Right(()))
  def changes: Stream[IO, StoreChange] = Stream.never
  def health: IO[StoreHealth] = IO.pure(StoreHealth.Healthy(0L, RefStore.At, Nil))
}

private object RefStore {
  val At: Instant = Instant.parse("2026-09-21T10:00:00Z")
  val conflict: KuiError = kui.kernel.error.ApplicationError.Remote(
    kui.kernel.error.ErrorCode.ConfigVersionConflict,
    "conflict",
    Nil
  )
  def create: IO[RefStore] = Ref.of[IO, Map[StoreKey, StoreRecord]](Map.empty).map(new RefStore(_))
}

final private class ConflictOnceStore private (
    delegate: RefStore,
    target: StoreKey,
    marker: Instant,
    first: Ref[IO, Boolean]
) extends ConfigStore[IO] {
  def get(key: StoreKey): IO[Option[StoreRecord]] = delegate.get(key)
  def list(section: StoreSection): IO[List[StoreRecord]] = delegate.list(section)
  def put(
      key: StoreKey,
      payload: Json,
      baseVersion: Option[Long],
      updatedBy: String
  ): IO[Either[KuiError, StoreRecord]] =
    first.getAndSet(false).flatMap {
      case true if key == target =>
        val winner = UserState(None, None, Some(marker)).asJson
        delegate.put(key, winner, baseVersion, "racing-writer").as(Left(RefStore.conflict))
      case _ => delegate.put(key, payload, baseVersion, updatedBy)
    }
  def delete(key: StoreKey, baseVersion: Long, updatedBy: String): IO[Either[KuiError, Unit]] =
    delegate.delete(key, baseVersion, updatedBy)
  def changes: Stream[IO, StoreChange] = delegate.changes
  def health: IO[StoreHealth] = delegate.health
}

private object ConflictOnceStore {
  def create(delegate: RefStore, target: StoreKey, marker: Instant): IO[ConflictOnceStore] =
    Ref.of[IO, Boolean](true).map(new ConflictOnceStore(delegate, target, marker, _))
}
