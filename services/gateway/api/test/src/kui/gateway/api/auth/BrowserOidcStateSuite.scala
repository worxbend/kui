package kui.gateway.api.auth

import scala.concurrent.duration.DurationInt

import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.syntax.all.*

import kui.gateway.application.session.SessionId
import kui.testkit.KuiIOSuite

final class BrowserOidcStateSuite extends KuiIOSuite {
  test("browser state expires and concurrent callbacks can consume it only once") {
    TestControl.executeEmbed {
      val states = new BrowserOidcState[IO]
      val session = SessionId.unsafe("session")
      for {
        bound <- states.bind(session, "state")
        results <- List.fill(20)(states.consume(session, "state")).parSequence
        held <- states.bind(session, "held")
        _ <- IO.sleep(299.seconds)
        still <- states.consume(session, "held")
        rebound <- states.bind(session, "expired")
        // Identity forgets a pending login after five minutes. A gateway binding that outlasts that
        // accepts a callback identity can no longer complete.
        _ <- IO.sleep(301.seconds)
        expired <- states.consume(session, "expired")
      } yield {
        assert(bound && held && rebound)
        assertEquals(results.count(identity), 1)
        assertEquals(still, true)
        assertEquals(expired, false)
      }
    }
  }
}
