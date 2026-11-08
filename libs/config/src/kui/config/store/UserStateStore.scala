package kui.config.store

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant

import cats.Monad
import cats.syntax.all.*
import io.circe.syntax.*
import io.circe.{Codec, Decoder, DecodingFailure, Encoder, HCursor, Json}

import kui.kernel.ClusterId
import kui.kernel.error.{ErrorCode, KuiError}
import kui.security.Principal

enum UiTheme(val wire: String) {
  case Auto extends UiTheme("auto")
  case Light extends UiTheme("light")
  case Dark extends UiTheme("dark")
}

object UiTheme {
  given Codec[UiTheme] = enumCodec("theme", UiTheme.values, _.wire)
  given CanEqual[UiTheme, UiTheme] = CanEqual.derived
}

enum UiAccent(val wire: String) {
  case Blue extends UiAccent("blue")
  case Teal extends UiAccent("teal")
  case Green extends UiAccent("green")
  case Amber extends UiAccent("amber")
}

object UiAccent {
  given Codec[UiAccent] = enumCodec("accent", UiAccent.values, _.wire)
  given CanEqual[UiAccent, UiAccent] = CanEqual.derived
}

enum UiDensity(val wire: String) {
  case Comfortable extends UiDensity("comfortable")
  case Compact extends UiDensity("compact")
}

object UiDensity {
  given Codec[UiDensity] = enumCodec("density", UiDensity.values, _.wire)
  given CanEqual[UiDensity, UiDensity] = CanEqual.derived
}

final case class UiAppearance(theme: UiTheme, accent: UiAccent, density: UiDensity)

object UiAppearance {
  val Default: UiAppearance = UiAppearance(UiTheme.Auto, UiAccent.Blue, UiDensity.Comfortable)
  given Codec[UiAppearance] = Codec.from(
    (cursor: HCursor) =>
      for {
        theme <- cursor.get[UiTheme]("theme")
        accent <- cursor.get[UiAccent]("accent")
        density <- cursor.get[UiDensity]("density")
      } yield UiAppearance(theme, accent, density),
    (appearance: UiAppearance) =>
      Json.obj(
        "theme" -> appearance.theme.asJson,
        "accent" -> appearance.accent.asJson,
        "density" -> appearance.density.asJson
      )
  )
  given CanEqual[UiAppearance, UiAppearance] = CanEqual.derived
}

enum MessageViewMode(val wire: String) {
  case Pages extends MessageViewMode("pages")
  case Infinite extends MessageViewMode("infinite")
}

object MessageViewMode {
  given Codec[MessageViewMode] = enumCodec("mode", MessageViewMode.values, _.wire)
  given CanEqual[MessageViewMode, MessageViewMode] = CanEqual.derived
}

final case class MessageBrowserSettings(pageSize: Int, mode: MessageViewMode)

object MessageBrowserSettings {
  val MinPageSize: Int = 1
  val MaxPageSize: Int = 500

  given Codec[MessageBrowserSettings] = Codec.from(
    (cursor: HCursor) =>
      for {
        pageSize <- cursor.get[Int]("pageSize")
        _ <- Either.cond(
          pageSize >= MinPageSize && pageSize <= MaxPageSize,
          (),
          DecodingFailure(
            s"pageSize must be between $MinPageSize and $MaxPageSize",
            cursor.history
          )
        )
        mode <- cursor.get[MessageViewMode]("mode")
      } yield MessageBrowserSettings(pageSize, mode),
    (settings: MessageBrowserSettings) =>
      Json.obj(
        "pageSize" -> Json.fromInt(settings.pageSize),
        "mode" -> settings.mode.asJson
      )
  )

  given CanEqual[MessageBrowserSettings, MessageBrowserSettings] = CanEqual.derived
}

/** The principal-scoped portion of KUI metadata.
  *
  * Every field is optional so independent owners can update one record without inventing values for the
  * others. APIs map missing preferences to product defaults; alerts map a missing watermark to "never read".
  */
final case class UserState(
    appearance: Option[UiAppearance],
    messageBrowser: Option[MessageBrowserSettings],
    alertsReadAt: Option[Instant]
)

object UserState {
  val CurrentFormatVersion: Int = 1
  val empty: UserState = UserState(None, None, None)

  given Codec[UserState] = Codec.from(
    (cursor: HCursor) =>
      for {
        version <- cursor.get[Int]("formatVersion")
        _ <- Either.cond(
          version == CurrentFormatVersion,
          (),
          DecodingFailure(
            s"unsupported user-state formatVersion $version (expected $CurrentFormatVersion)",
            cursor.history
          )
        )
        appearance <- cursor.getOrElse[Option[UiAppearance]]("appearance")(None)
        messageBrowser <- cursor.getOrElse[Option[MessageBrowserSettings]]("messageBrowser")(None)
        alertsReadAt <- cursor.getOrElse[Option[Instant]]("alertsReadAt")(None)
      } yield UserState(appearance, messageBrowser, alertsReadAt),
    (state: UserState) =>
      Json.obj(
        "formatVersion" -> Json.fromInt(CurrentFormatVersion),
        "appearance" -> state.appearance.asJson,
        "messageBrowser" -> state.messageBrowser.asJson,
        "alertsReadAt" -> state.alertsReadAt.asJson
      )
  )

  given CanEqual[UserState, UserState] = CanEqual.derived
}

object UserStateKey {

  /** A readable cluster scope and a pseudonymous principal reference.
    *
    * Principal kind is part of the digest so an anonymous caller and an account literally named `anonymous`
    * do not share state. Roles and session identifiers are deliberately absent.
    */
  def of(cluster: ClusterId, principal: Principal): StoreKey = {
    val material = s"${principal.kind.wire}\u0000${principal.name.value}"
    val digest = MessageDigest
      .getInstance("SHA-256")
      .digest(material.getBytes(StandardCharsets.UTF_8))
      .take(DigestBytes)
      .map(byte => f"${byte & 0xff}%02x")
      .mkString

    StoreKey(StoreSection.Settings, s"${cluster.value}-$digest")
  }

  private val DigestBytes: Int = 16
}

/** Typed, field-preserving access to one principal's state in `__kui_config`.
  *
  * A preference click and an alert read can race. Each mutation therefore re-reads after a version conflict
  * and applies only its own field to the winner. The retry count is bounded: an unhealthy write storm must
  * fail visibly instead of keeping an HTTP request alive forever.
  */
final class UserStateStore[F[_]: Monad] private (store: ConfigStore[F]) {

  import UserStateStore.*

  def get(cluster: ClusterId, principal: Principal): F[Either[KuiError, UserState]] =
    getAt(UserStateKey.of(cluster, principal)).map(_.map(_._2))

  def putAppearance(
      cluster: ClusterId,
      principal: Principal,
      appearance: UiAppearance
  ): F[Either[KuiError, UiAppearance]] =
    modify(cluster, principal)(_.copy(appearance = Some(appearance))).map(_.map(_ => appearance))

  def putMessageBrowser(
      cluster: ClusterId,
      principal: Principal,
      settings: MessageBrowserSettings
  ): F[Either[KuiError, MessageBrowserSettings]] =
    modify(cluster, principal)(_.copy(messageBrowser = Some(settings))).map(_.map(_ => settings))

  def markAlertsRead(
      cluster: ClusterId,
      principal: Principal,
      at: Instant
  ): F[Either[KuiError, Instant]] =
    modify(cluster, principal) { current =>
      val next = current.alertsReadAt.fold(at)(existing => if existing.isAfter(at) then existing else at)
      current.copy(alertsReadAt = Some(next))
    }.map(_.map(_.alertsReadAt.getOrElse(at)))

  private def modify(
      cluster: ClusterId,
      principal: Principal
  )(change: UserState => UserState): F[Either[KuiError, UserState]] = {
    val key = UserStateKey.of(cluster, principal)

    def attempt(remaining: Int): F[Either[KuiError, UserState]] =
      getAt(key).flatMap {
        case Left(error) => error.asLeft[UserState].pure[F]
        case Right((version, current)) =>
          val next = change(current)
          store.put(key, next.asJson, version, writerOf(key)).flatMap {
            case Right(_) => next.asRight[KuiError].pure[F]
            case Left(error) if error.code == ErrorCode.ConfigVersionConflict && remaining > 1 =>
              attempt(remaining - 1)
            case Left(error) => error.asLeft[UserState].pure[F]
          }
      }

    attempt(MaxWriteAttempts)
  }

  private def getAt(key: StoreKey): F[Either[KuiError, (Option[Long], UserState)]] =
    store.health.flatMap { health =>
      if health.unreadableKeys.contains(key) then
        // Absent is not the same as unreadable. An empty state here would reset settings and the alert
        // watermark, and the next write would replace a record this replica could not read.
        StoreError
          .toKuiError(
            StoreError.MalformedRecord(key.render, "the stored record could not be read")
          )
          .asLeft[(Option[Long], UserState)]
          .pure[F]
      else readAt(key)
    }

  private def readAt(key: StoreKey): F[Either[KuiError, (Option[Long], UserState)]] =
    store.get(key).map {
      case None => Right(None -> UserState.empty)
      case Some(record) =>
        Decoder[UserState]
          .decodeJson(record.payload)
          .left
          .map(failure =>
            StoreError.toKuiError(
              StoreError.MalformedRecord(key.render, s"user state payload: ${failure.message}")
            )
          )
          .map(state => Some(record.version) -> state)
    }
}

object UserStateStore {
  val MaxWriteAttempts: Int = 4

  def apply[F[_]: Monad](store: ConfigStore[F]): UserStateStore[F] = new UserStateStore[F](store)

  private def writerOf(key: StoreKey): String = s"kui-user-state/${key.id.takeRight(16)}"
}

private def enumCodec[A](field: String, values: Array[A], wire: A => String): Codec[A] =
  Codec.from(
    Decoder.decodeString.emap(raw =>
      values
        .find(value => wire(value) == raw)
        .toRight(
          s"$field must be one of ${values.map(wire).mkString(", ")}, found '$raw'"
        )
    ),
    Encoder.encodeString.contramap(wire)
  )
