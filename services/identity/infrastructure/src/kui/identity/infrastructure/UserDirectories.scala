package kui.identity.infrastructure

import cats.effect.kernel.Sync
import cats.syntax.all.*
import io.circe.{Json, JsonObject}
import org.typelevel.log4cats.StructuredLogger

import kui.config.FormUserConfig
import kui.config.store.{ConfigStore, SecretJson, StoreHealth, StoreKey, StoreRecord, StoreSection}
import kui.identity.domain.{PasswordHash, UpdateRefused, UserDirectory, UserRecord}
import kui.kernel.{Secret, UserName}

/** The accounts an operator wrote in `kui.auth.users[]`, and nothing else (AU-002).
  *
  * Read-only by construction: a configuration file is the operator's, and a running process that rewrote it
  * would be a process fighting whatever deployed it. [[update]] therefore refuses, and
  * [[StoredUserDirectory]] is what a deployment adds when it wants passwords that people can change.
  *
  * Names are compared case-insensitively. A person who signs in as `Admin` on Monday and `admin` on Tuesday
  * is the same person, and any other answer is a support ticket rather than a security control.
  */
object ConfiguredUserDirectory {

  /** Reads the configured accounts, refusing the ones whose hash is not a hash.
    *
    * A malformed hash is a *startup* failure and not a login-time one: an operator who pasted half a hash
    * into their configuration should find out when they deploy, not when somebody cannot sign in. The failure
    * names the account and never the value.
    */
  def make[F[_]: Sync](
      users: List[FormUserConfig],
      logger: StructuredLogger[F]
  ): F[UserDirectory[F]] =
    users
      .traverse(user =>
        Sync[F].fromEither(
          PasswordHash
            .parse(user.passwordHash)
            .map(hash => UserRecord(UserName.unsafe(user.name), hash, user.groups, user.mustChangePassword))
            .leftMap(problem =>
              new IllegalArgumentException(
                s"kui.auth.users '${user.name}' has an unusable passwordHash: ${problem.message}"
              )
            )
        )
      )
      .flatTap(records =>
        logger.info(s"identity: ${records.size} configured account(s) are available for form sign-in")
      )
      .map(records => fromRecords[F](records))

  /** The same directory over records that have already been parsed — what a suite builds. */
  def fromRecords[F[_]: Sync](records: List[UserRecord]): UserDirectory[F] = {
    val byName: Map[String, UserRecord] = records.map(record => key(record.name.value) -> record).toMap

    new UserDirectory[F] {

      def find(username: String): F[Option[UserRecord]] = Sync[F].pure(byName.get(key(username)))

      def update(record: UserRecord): F[Either[UpdateRefused, Unit]] =
        Sync[F].pure(Left(ConfiguredUserDirectory.ReadOnly))
    }
  }

  /** What a change-password attempt is told in a deployment whose accounts come from the file alone.
    *
    * It names the key to set, because "you cannot do that" without "here is how you could" is the kind of
    * refusal that turns into a support ticket.
    */
  val ReadOnly: UpdateRefused =
    UpdateRefused(
      "accounts in kui.auth.users cannot be changed by KUI itself; configure kui.store so that changed " +
        "passwords have somewhere to live"
    )

  private[infrastructure] def key(username: String): String = kui.security.AccountName.canonical(username)
}

/** The configured accounts, with any password somebody has since changed laid over the top.
  *
  * ==Why the change lives in the metadata store==
  *
  * A password change has to outlive a restart, or a forced first change is a screen an operator meets every
  * morning. The metadata store (ADR-042) is where KUI already keeps state an operator edits at runtime; it is
  * a compacted Kafka topic in a real deployment, so every gateway replica sees the change, and the value it
  * holds is encrypted at rest by the store's own field encryption (ADR-044) because it is written through
  * `SecretJson`'s marker.
  *
  * A deployment with no store configured gets `ConfigStore.empty`, whose writes fail with the store's own
  * "this deployment has no metadata store" error. That is the honest outcome and it is a *reachable* one: the
  * change-password endpoint reports it, so an operator is told what to configure rather than watching a
  * change appear to succeed and vanish at the next restart.
  *
  * ==What is stored, and what is not==
  *
  * One record per account that has changed its password, holding the encoded hash and nothing else. Groups
  * and the login name stay in the configuration: a store record that could grant somebody a new group would
  * be a privilege escalation through a password change endpoint.
  */
object StoredUserDirectory {

  /** The record id for an account. Prefixed, because `StoreSection.Rbac` will also hold role records (RB-004)
    * and the two must not be able to collide.
    */
  def keyFor(username: String): StoreKey =
    StoreKey(StoreSection.Rbac, s"user-${ConfiguredUserDirectory.key(username)}")

  private val HashField: String = "passwordHash"

  def make[F[_]: Sync](
      base: UserDirectory[F],
      store: ConfigStore[F],
      logger: StructuredLogger[F]
  ): UserDirectory[F] =
    new UserDirectory[F] {

      def find(username: String): F[Option[UserRecord]] =
        base.find(username).flatMap {
          case None => none[UserRecord].pure[F]
          case Some(configured) =>
            store
              .get(keyFor(username))
              .flatMap(record => store.health.map(health => (record, health)))
              .flatMap {
                case (_, health) if health.unreadableKeys.contains(keyFor(username)) || (health match {
                      case StoreHealth.Degraded(_, _, _, _) => true
                      case _ => false
                    }) =>
                  logger
                    .error("identity: credential store view is not trustworthy; authentication refused")
                    .as(none[UserRecord])
                case (None, _) => configured.some.pure[F]
                case (Some(record), _) =>
                  hashOf(record) match {
                    case Right(hash) => configured.withPassword(hash).some.pure[F]
                    case Left(_) =>
                      // An unreadable override is not an uninitialized account. Never revive an old hash.
                      logger
                        .error(
                          "identity: a stored password could not be read; authentication refused"
                        )
                        .as(none[UserRecord])
                  }
              }
              .handleErrorWith(error =>
                logger
                  .error(error)("identity: the metadata store could not be read; authentication refused")
                  .as(none[UserRecord])
              )
        }

      def update(record: UserRecord): F[Either[UpdateRefused, Unit]] =
        (for {
          existing <- store.get(keyFor(record.name.value))
          written <- store.put(
            keyFor(record.name.value),
            payloadOf(record.hash),
            existing.map(_.version),
            record.name.value
          )
        } yield written.bimap(error => UpdateRefused(error.message), _ => ()))
          .handleErrorWith(error =>
            logger
              .error(error)("identity: a password change could not be written to the metadata store")
              .as(Left(UpdateRefused("the change could not be saved; KUI's metadata store is not available")))
          )
    }

  /** The stored shape: the encoded hash, marked as a secret so the store encrypts it (ADR-044). */
  private def payloadOf(hash: PasswordHash): Json =
    Json.fromJsonObject(
      JsonObject(HashField -> SecretJson.encoder(Secret(hash.encoded)))
    )

  private def hashOf(record: StoreRecord): Either[String, PasswordHash] =
    record.payload.hcursor
      .downField(HashField)
      .as[Secret[String]](using SecretJson.decoder)
      .leftMap(_.message)
      .flatMap(PasswordHash.parse(_).leftMap(_.message))

}
