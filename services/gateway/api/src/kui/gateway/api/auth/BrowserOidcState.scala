package kui.gateway.api.auth

import java.time.Instant

import cats.effect.kernel.{Clock, Ref, Sync}
import cats.syntax.all.*

import kui.gateway.application.session.SessionId

/** One short-lived login flow per browser session, consumed before code redemption. */
final private[auth] class BrowserOidcState[F[_]: Sync] {
  private val pending = Ref.unsafe[F, Map[SessionId, (String, Instant)]](Map.empty)

  def bind(session: SessionId, state: String): F[Boolean] =
    Clock[F].realTimeInstant.flatMap { now =>
      pending.modify { entries =>
        val live = entries.filter((_, entry) => entry._2.isAfter(now))
        if live.size >= 10000 && !live.contains(session) then (live, false)
        else (live.updated(session, (state, now.plusSeconds(600))), true)
      }
    }

  def consume(session: SessionId, state: String): F[Boolean] =
    Clock[F].realTimeInstant.flatMap { now =>
      pending.modify { entries =>
        val live = entries.filter((_, entry) => entry._2.isAfter(now))
        val matches = live.get(session).exists(_._1 == state)
        (if matches then live - session else live, matches)
      }
    }
}
