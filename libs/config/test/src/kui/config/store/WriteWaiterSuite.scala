package kui.config.store

import scala.concurrent.duration.*

import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.syntax.all.*

import kui.testkit.KuiIOSuite

/** That a writer parks until its own record comes back around the log, and is always woken — by the record
  * arriving, by the timeout, or by the follower dying.
  *
  * The one that earns this file its existence is `awaitCompletesImmediatelyWhenTheOffsetIsAlreadyPast`. The
  * follower can win the race against the writer that produced the record, and a waiter that checked "has it
  * arrived?" outside the same atomic step that registers it would park for ever against a record that has
  * already gone by. That bug does not fail a happy-path test; it fails in production, once a month, as an
  * unexplained write timeout.
  */
final class WriteWaiterSuite extends KuiIOSuite {

  test("awaitCompletesWhenTheOffsetIsAdvanced") {
    val test = for {
      waiter <- WriteWaiter.create[IO]
      fiber <- waiter.await(5L, 10.seconds).start
      _ <- IO.sleep(1.second)
      _ <- waiter.advance(5L)
      result <- fiber.joinWithNever
      pending <- waiter.pending
    } yield (result, pending)
    TestControl.executeEmbed(test).map { (result, pending) =>
      assertEquals(result, Right(()))
      assertEquals(pending, 0)
    }
  }

  test("awaitCompletesImmediatelyWhenTheOffsetIsAlreadyPast") {
    val test = for {
      waiter <- WriteWaiter.create[IO]
      _ <- waiter.advance(9L)
      result <- waiter.await(5L, 10.seconds)
      pending <- waiter.pending
    } yield (result, pending)
    TestControl.executeEmbed(test).map { (result, pending) =>
      assertEquals(result, Right(()))
      assertEquals(pending, 0)
    }
  }

  test("awaitTimesOutWithTheOffsetAndElapsedTime") {
    TestControl.executeEmbed(WriteWaiter.create[IO].flatMap(_.await(7L, 10.seconds))).map {
      case Left(error @ StoreError.WriteTimeout(offset, afterMs)) =>
        assertEquals(offset, 7L)
        assertEquals(afterMs, 10000L)
        // Retryable, and honest about not knowing: the record may well have landed.
        assertEquals(error.code.wire, "KUI-TIMEOUT")
        assert(error.message.contains("may still have"), error.message)
      case other => fail(s"expected a WriteTimeout, got $other")
    }
  }

  test("advanceIsMonotonicAndIdempotent") {
    val test = for {
      waiter <- WriteWaiter.create[IO]
      _ <- waiter.advance(10L)
      _ <- waiter.advance(3L)
      _ <- waiter.advance(10L)
      // The mark never moved backwards, so an offset below it is still satisfied at once.
      past <- waiter.await(10L, 1.second)
      fiber <- waiter.await(11L, 10.seconds).start
      _ <- IO.sleep(1.second)
      stillPending <- waiter.pending
      _ <- waiter.advance(11L)
      later <- fiber.joinWithNever
    } yield (past, stillPending, later)
    TestControl.executeEmbed(test).map { (past, stillPending, later) =>
      assertEquals(past, Right(()))
      assertEquals(stillPending, 1)
      assertEquals(later, Right(()))
    }
  }

  test("failWakesEveryWaiter") {
    // The follower dying must free every parked writer at once. Leaving each to wait out its own timeout
    // against a log nobody is reading turns one failure into a minute of stuck requests.
    val reason = StoreError.Unreachable("kafka-1:9092", "the tail follower stopped")
    val test = for {
      waiter <- WriteWaiter.create[IO]
      fibers <- (1 to 20).toList.traverse(offset => waiter.await(offset.toLong, 10.seconds).start)
      _ <- IO.sleep(1.second)
      _ <- waiter.fail(reason)
      results <- fibers.traverse(_.joinWithNever)
      // Sticky: a writer arriving afterwards is told at once rather than parked.
      afterwards <- waiter.await(99L, 10.seconds)
      pending <- waiter.pending
    } yield (results, afterwards, pending)
    TestControl.executeEmbed(test).map { (results, afterwards, pending) =>
      assertEquals(results.distinct, List(Left(reason)))
      assertEquals(afterwards, Left(reason))
      assertEquals(pending, 0)
    }
  }

  test("recoverLetsTheNextWriterWaitAfterAFailure") {
    // fail is sticky while the follower is down. A follower that retries and comes back has to clear it,
    // or the next write is refused with the reason of an outage that is already over.
    val reason = StoreError.Unreachable("kafka:9092", "authentication failed")
    val test = for {
      waiter <- WriteWaiter.create[IO]
      _ <- waiter.fail(reason)
      refused <- waiter.await(1L, 10.seconds)
      _ <- waiter.recover
      fiber <- waiter.await(2L, 10.seconds).start
      _ <- IO.sleep(1.second)
      pending <- waiter.pending
      _ <- waiter.advance(2L)
      resumed <- fiber.joinWithNever
    } yield (refused, pending, resumed)
    TestControl.executeEmbed(test).map { (refused, pending, resumed) =>
      assertEquals(refused, Left(reason))
      assertEquals(pending, 1)
      assertEquals(resumed, Right(()))
    }
  }

  test("aCancelledWaiterDoesNotStayInTheMap") {
    // A waiter left behind after its fiber is gone is a slow leak that only appears under load, which is
    // the worst time to find one.
    val test = for {
      waiter <- WriteWaiter.create[IO]
      fiber <- waiter.await(5L, 1.hour).start
      _ <- IO.sleep(1.second)
      before <- waiter.pending
      _ <- fiber.cancel
      after <- waiter.pending
      // And the waiter still works for the next write on the same key.
      next <- waiter.advance(5L) *> waiter.await(5L, 1.second)
    } yield (before, after, next)
    TestControl.executeEmbed(test).map { (before, after, next) =>
      assertEquals(before, 1)
      assertEquals(after, 0)
      assertEquals(next, Right(()))
    }
  }

  test("aTimedOutWaiterDoesNotStayInTheMap") {
    val test = for {
      waiter <- WriteWaiter.create[IO]
      _ <- waiter.await(5L, 10.seconds)
      pending <- waiter.pending
    } yield pending
    TestControl.executeEmbed(test).map(pending => assertEquals(pending, 0))
  }

  test("manyConcurrentWaitersEachSeeTheirOwnOffset") {
    val test = for {
      waiter <- WriteWaiter.create[IO]
      fibers <- (1 to 100).toList.traverse(offset => waiter.await(offset.toLong, 1.hour).start)
      _ <- IO.sleep(1.second)
      // Advancing one offset at a time must wake exactly the waiters at or below it, and no others.
      _ <- (1 to 100).toList.traverse_(offset => waiter.advance(offset.toLong))
      results <- fibers.traverse(_.joinWithNever)
      pending <- waiter.pending
    } yield (results, pending)
    TestControl.executeEmbed(test).map { (results, pending) =>
      assertEquals(results.distinct, List(Right(())))
      assertEquals(pending, 0)
    }
  }
}
