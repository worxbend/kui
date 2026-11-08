package kui.identity.application

import java.time.Instant

import scala.concurrent.duration.DurationInt

import cats.effect.IO
import cats.syntax.all.*

import kui.kernel.Secret
import kui.testkit.KuiIOSuite

/** The three properties the password challenge and the OpenID Connect `state` both depend on.
  *
  * Each of them is a real attack rather than a tidiness rule: a replayable token is a replayable login, a
  * token that never expires is one an old browser history can still complete, and an unbounded store is a way
  * for an unauthenticated caller to exhaust this process's memory.
  */
final class SingleUseTokensSuite extends KuiIOSuite {

  private val now: Instant = Instant.parse("2026-09-04T09:00:00Z")

  test("a token redeems once and never again") {
    for {
      tokens <- SingleUseTokens.make[IO, String]()
      token <- tokens.issue("ada", now)
      first <- tokens.redeem(token, now)
      second <- tokens.redeem(token, now)
    } yield {
      assertEquals(first, Some("ada"))
      assertEquals(second, None)
    }
  }

  test("a token nobody issued is unknown, and so is an expired one — indistinguishably") {
    for {
      tokens <- SingleUseTokens.make[IO, String](ttl = 5.minutes)
      token <- tokens.issue("ada", now)
      expired <- tokens.redeem(token, now.plusSeconds(301))
      invented <- tokens.redeem(Secret("invented"), now)
    } yield {
      assertEquals(expired, None)
      assertEquals(invented, None)
    }
  }

  test("a token is still good one second before it expires") {
    for {
      tokens <- SingleUseTokens.make[IO, String](ttl = 5.minutes)
      token <- tokens.issue("ada", now)
      redeemed <- tokens.redeem(token, now.plusSeconds(299))
    } yield assertEquals(redeemed, Some("ada"))
  }

  test("two tokens are never the same, because a guessable one completes somebody else's flow") {
    for {
      tokens <- SingleUseTokens.make[IO, String]()
      issued <- List.fill(50)(()).traverse(_ => tokens.issue("ada", now))
    } yield assertEquals(issued.map(_.value).distinct.size, 50)
  }

  test("the store is bounded, and drops what is closest to expiring") {
    for {
      tokens <- SingleUseTokens.make[IO, String](capacity = 3)
      first <- tokens.issue("first", now)
      _ <- tokens.issue("second", now.plusSeconds(1))
      _ <- tokens.issue("third", now.plusSeconds(2))
      fourth <- tokens.issue("fourth", now.plusSeconds(3))
      oldest <- tokens.redeem(first, now.plusSeconds(3))
      newest <- tokens.redeem(fourth, now.plusSeconds(3))
    } yield {
      assertEquals(oldest, None, "the oldest entry should have been evicted")
      assertEquals(newest, Some("fourth"))
    }
  }

  test("remembering a token again moves its expiry, so an older neighbour is the one evicted") {
    for {
      tokens <- SingleUseTokens.make[IO, String](capacity = 2)
      first <- tokens.issue("first", now)
      second <- tokens.issue("second", now.plusSeconds(1))
      _ <- tokens.remember(first, "first-again", now.plusSeconds(2))
      third <- tokens.issue("third", now.plusSeconds(3))
      kept <- tokens.redeem(first, now.plusSeconds(3))
      evicted <- tokens.redeem(second, now.plusSeconds(3))
      newest <- tokens.redeem(third, now.plusSeconds(3))
    } yield {
      assertEquals(kept, Some("first-again"))
      assertEquals(evicted, None)
      assertEquals(newest, Some("third"))
    }
  }

  test("expired entries are dropped on a write, so an idle store does not accumulate them") {
    for {
      tokens <- SingleUseTokens.make[IO, String](ttl = 1.minute, capacity = 2)
      stale <- tokens.issue("stale", now)
      // Two more, well after the first expired. Without the sweep the bound would have evicted a live
      // entry to make room for one that was already dead.
      live <- tokens.issue("live", now.plusSeconds(120))
      alsoLive <- tokens.issue("also", now.plusSeconds(120))
      goneAnyway <- tokens.redeem(stale, now.plusSeconds(120))
      first <- tokens.redeem(live, now.plusSeconds(120))
      second <- tokens.redeem(alsoLive, now.plusSeconds(120))
    } yield {
      assertEquals(goneAnyway, None)
      assertEquals(first, Some("live"))
      assertEquals(second, Some("also"))
    }
  }

  test("a minted token is not stored until something is remembered against it") {
    for {
      tokens <- SingleUseTokens.make[IO, String]()
      token <- tokens.mint
      beforeRemembering <- tokens.redeem(token, now)
      _ <- tokens.remember(token, "ada", now)
      afterRemembering <- tokens.redeem(token, now)
    } yield {
      assertEquals(beforeRemembering, None)
      assertEquals(afterRemembering, Some("ada"))
    }
  }
}
