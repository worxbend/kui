package kui.identity.application

import java.security.SecureRandom
import java.time.Instant
import java.util.Base64

import scala.concurrent.duration.FiniteDuration

import cats.effect.kernel.{Async, Sync}
import cats.effect.std.AtomicCell
import cats.syntax.all.*

import kui.kernel.Secret

/** Short-lived, single-use tokens with something remembered against them.
  *
  * Two flows in this service need exactly this and nothing more:
  *
  *   - the forced password change issues a challenge when a password is verified and the account may not have
  *     a session yet, and redeems it when the new password arrives. Without it the change endpoint would have
  *     to take a username and the old password a second time, which means a caller could reach it having
  *     never proved anything;
  *   - OpenID Connect issues a `state` before sending the browser to the provider and redeems it when the
  *     browser comes back. That is what makes the callback un-forgeable: a code delivered with a `state` this
  *     process never issued is not a login, it is somebody else's authorization code being replayed at us.
  *
  * ==Three properties, all of them load bearing==
  *
  * **Single use.** [[redeem]] removes the token as it returns it, atomically. An OIDC `state` that could be
  * redeemed twice is a replay; a password challenge that could be is a second password change nobody asked
  * for.
  *
  * **Expiring.** Five minutes is the default, which is the window the OAuth 2 specification's own guidance
  * assumes for an authorization request. An expired token is reported as absent, so that a caller cannot tell
  * "expired" from "never existed" — the same information a guessed token would otherwise leak.
  *
  * **Bounded.** Both flows are reachable without being signed in, so anything that grows per request is a way
  * for an unauthenticated client to exhaust memory. When the store is full the oldest entry goes. The TTL is
  * fixed and a replaced token's expiry moves forward with it, so insertion order — with a refresh moved to
  * the end — is expiry order, and remembering does not sort the store.
  *
  * ==Why there is no sweeper fiber==
  *
  * Expired entries are dropped on every [[issue]], which is the only operation that can make the store grow.
  * A background fiber would be a lifetime to supervise, a `Resource` to release and a cancellation path to
  * test, all to reclaim a few hundred bytes marginally sooner than the next login does.
  */
trait SingleUseTokens[F[_], A] {

  /** A fresh token, valid for `ttl`, with `payload` remembered against it. */
  def issue(payload: A, now: Instant): F[Secret[String]]

  /** A fresh token with nothing yet remembered against it, and nothing stored.
    *
    * It exists for the OpenID Connect flow, where the token has to be handed to the adapter that builds the
    * authorization URL *before* the value to remember against it exists — the nonce and the PKCE verifier are
    * the adapter's to choose. The alternative was to store a placeholder and overwrite it, which is two
    * writes and a window in which a redeemed state yields nonsense.
    */
  def mint: F[Secret[String]]

  /** Remembers `payload` against a token [[mint]] produced. Replaces whatever was there. */
  def remember(token: Secret[String], payload: A, now: Instant): F[Unit]

  /** What was remembered against this token, if it is still valid — and it is invalid from then on. */
  def redeem(token: Secret[String], now: Instant): F[Option[A]]
}

object SingleUseTokens {

  /** 32 random bytes, base64url. The same shape and the same reasoning as a session id (ADR-019): whoever
    * holds one can complete somebody else's flow, so nothing about it may be predictable.
    */
  private val TokenBytes: Int = 32

  /** How long a token lives. Five minutes is long enough for a person to type a password twice and short
    * enough that a token left in a browser's history is worthless by the time anyone reads it.
    */
  val DefaultTtl: FiniteDuration = FiniteDuration(5, java.util.concurrent.TimeUnit.MINUTES)

  /** The bound. Deliberately small: these tokens live for five minutes, so ten thousand of them at once is
    * already far more logins per minute than any KUI deployment will see.
    */
  val DefaultCapacity: Int = 10_000

  final private case class Entry[A](payload: A, expiresAt: Instant)

  /** Entries in expiry order. A fixed TTL makes that the order they were remembered, except a token
    * remembered again, whose new expiry is later, so it moves to the end.
    */
  final private case class Ledger[A](entries: Map[String, Entry[A]], order: Vector[String]) {
    def remove(key: String): Ledger[A] = Ledger(entries - key, order.filterNot(_ == key))
  }

  private object Ledger {
    def empty[A]: Ledger[A] = Ledger(Map.empty, Vector.empty)
  }

  def make[F[_]: Async, A](
      ttl: FiniteDuration = DefaultTtl,
      capacity: Int = DefaultCapacity
  ): F[SingleUseTokens[F, A]] =
    for {
      cell <- AtomicCell[F].of(Ledger.empty[A])
      random <- Sync[F].delay(new SecureRandom())
    } yield new SingleUseTokens[F, A] {

      def issue(payload: A, now: Instant): F[Secret[String]] =
        mint.flatTap(token => remember(token, payload, now))

      def mint: F[Secret[String]] = randomToken.map(Secret(_))

      def remember(token: Secret[String], payload: A, now: Instant): F[Unit] = {
        val expiry = now.plusMillis(ttl.toMillis)
        cell.evalUpdate { state =>
          // Expired entries go on every write, which is the only operation that can make the store
          // grow. What remains is already in expiry order, so the bound drops from the front.
          val alive = state.order.filter(key => state.entries.get(key).exists(_.expiresAt.isAfter(now)))
          val withoutThis = alive.filterNot(_ == token.value)
          val kept =
            if withoutThis.sizeIs < capacity then withoutThis
            else withoutThis.drop(withoutThis.size - (capacity - 1))
          val nextOrder = kept :+ token.value
          val nextEntries = nextOrder.iterator.map { key =>
            val entry = if key == token.value then Entry(payload, expiry) else state.entries(key)
            key -> entry
          }.toMap
          Sync[F].pure(Ledger(nextEntries, nextOrder))
        }
      }

      def redeem(token: Secret[String], now: Instant): F[Option[A]] =
        cell.evalModify { state =>
          state.entries.get(token.value) match {
            case Some(entry) if entry.expiresAt.isAfter(now) =>
              Sync[F].pure((state.remove(token.value), Some(entry.payload)))
            case Some(_) =>
              // Expired. Removed, and reported as absent: the caller must not be able to tell an
              // expired token from one that never existed.
              Sync[F].pure((state.remove(token.value), None))
            case None => Sync[F].pure((state, None))
          }
        }

      private def randomToken: F[String] =
        Sync[F].delay {
          val bytes = new Array[Byte](TokenBytes)
          random.nextBytes(bytes)
          Base64.getUrlEncoder.withoutPadding.encodeToString(bytes)
        }
    }
}
