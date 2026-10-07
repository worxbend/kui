package kui.gateway.application.session

import java.time.Instant

import scala.concurrent.duration.FiniteDuration

import kui.security.Principal

/** How the gateway remembers a browser between requests (ADR-019).
  *
  * A port, not a Kafka topic or an in-memory map, because M0's adapter and M6's are meant to be
  * interchangeable without a caller noticing: [[InMemorySessionStore]] is the only implementation today, and
  * TD-003 tracks the Kafka-backed one multiple gateway replicas will need.
  */
trait SessionStore[F[_]] {

  /** A new session for `principal`, with a fresh id and a fresh CSRF secret. */
  def create(principal: Principal, now: Instant): F[Session]

  /** The session for `id`, if one exists and has not expired — and, if it has not, its `lastSeenAt` moved to
    * `now`. Touching on every read is what makes the idle timeout mean "thirty minutes since the browser last
    * did anything" rather than "thirty minutes since the cookie was issued".
    */
  def get(id: SessionId, now: Instant): F[Option[Session]]

  /** A new id for the same principal, the old one no longer valid. The session fixation defence: whenever a
    * request should no longer trust an id that existed before this moment — a privilege change, in M6 — a
    * caller rotates rather than mutates, so a stolen pre-change id is worthless afterwards.
    */
  def rotate(id: SessionId, now: Instant): F[Option[Session]]

  /** Removes a session outright. What `POST /api/v1/auth/logout` calls. */
  def delete(id: SessionId): F[Unit]

  /** Invalidates a pre-login id and remembers its retirement for a bounded grace period. Late requests must
    * not mint a cookie that overwrites the replacement authenticated session.
    */
  def retire(id: SessionId, now: Instant): F[Unit]

  /** Retirement is not expiry: expired sessions can recover immediately, retired ids must wait. */
  def isRetired(id: SessionId, now: Instant): F[Boolean]

  /** Removes every session that has expired as of `now`, and answers how many were removed.
    *
    * A caller-invoked sweep rather than a lazy one only on `get`, because a store nobody reads from for an
    * hour should not silently accumulate an hour of expired entries in memory — the eviction has to be able
    * to happen even when nothing is asking about any particular session.
    */
  def sweep(now: Instant): F[Int]
}

/** [[InMemorySessionStore]]'s tunables (ADR-019).
  *
  * @param idleTimeout
  *   how long a session survives with no request. Thirty minutes.
  * @param absoluteTimeout
  *   the hard cutoff from creation, however active the session stays. Twelve hours.
  * @param maxSessions
  *   the bound that stops an unauthenticated flood from exhausting memory — anonymous mode creates one
  *   session per browser that visits, with no login to rate-limit it. Ten thousand.
  * @param sweepInterval
  *   how often a caller that wants continuous cleanup should call [[SessionStore.sweep]]. The store does not
  *   schedule this itself — a fiber that runs forever is a composition-root concern (ADR-010) — this is only
  *   the interval `GatewayWiring` is expected to use.
  */
final case class SessionConfig(
    idleTimeout: FiniteDuration,
    absoluteTimeout: FiniteDuration,
    maxSessions: Int,
    sweepInterval: FiniteDuration
)

object SessionConfig {

  import scala.concurrent.duration.DurationInt

  val Default: SessionConfig = SessionConfig(
    idleTimeout = 30.minutes,
    absoluteTimeout = 12.hours,
    maxSessions = 10_000,
    sweepInterval = 1.minute
  )

  given CanEqual[SessionConfig, SessionConfig] = CanEqual.derived
}
