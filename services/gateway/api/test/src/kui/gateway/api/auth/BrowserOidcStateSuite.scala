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
        rebound <- states.bind(session, "expired")
        _ <- IO.sleep(10.minutes)
        expired <- states.consume(session, "expired")
      } yield {
        assert(bound && rebound)
        assertEquals(results.count(identity), 1)
        assertEquals(expired, false)
      }
    }
  }
}
