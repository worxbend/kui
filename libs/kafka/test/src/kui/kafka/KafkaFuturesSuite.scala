package kui.kafka

import java.util.concurrent.{CompletionException, ExecutionException}

import scala.concurrent.duration.*

import cats.effect.{IO, Ref}
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.internals.KafkaFutureImpl

import kui.testkit.KuiIOSuite

/** The bridge, checked against the two things that actually go wrong.
  *
  * A `KafkaFuture` reports failures wrapped, so a mapper that matches on the thrown type matches the wrapper.
  * And every completion callback runs on the admin client's single network thread, so a continuation that
  * runs there stalls every other in-flight request on that connection.
  */
final class KafkaFuturesSuite extends KuiIOSuite {

  private def pending[A]: IO[KafkaFutureImpl[A]] = IO(new KafkaFutureImpl[A]())

  test("unwrapsCompletionException") {
    val cause = new TimeoutException("timed out")

    assertEquals(KafkaFutures.unwrap(new CompletionException(cause)), cause)
  }

  test("unwrapsExecutionException") {
    val cause = new TimeoutException("timed out")

    assertEquals(KafkaFutures.unwrap(new ExecutionException(cause)), cause)
  }

  test("unwrapsANestedPair") {
    val cause = new TimeoutException("timed out")
    val nested = new CompletionException(new ExecutionException(cause))

    assertEquals(KafkaFutures.unwrap(nested), cause)
  }

  test("unwrapLeavesAnUnwrappedThrowableAlone") {
    val plain = new IllegalStateException("plain")

    assertEquals(KafkaFutures.unwrap(plain), plain)
  }

  test("aWrapperWithNoCauseIsReturnedAsItself") {
    val empty = new CompletionException("no cause", null)

    assertEquals(KafkaFutures.unwrap(empty), empty)
  }

  test("aCompletedFutureBecomesItsValue") {
    for {
      future <- pending[String]
      _ <- IO(future.complete("value"))
      value <- KafkaFutures.fromFuture[IO, String](IO.pure(future))
    } yield assertEquals(value, "value")
  }

  test("aFailedFutureFailsWithTheUnwrappedCause") {
    val cause = new TimeoutException("timed out")

    for {
      future <- pending[String]
      _ <- IO(future.completeExceptionally(cause))
      result <- KafkaFutures.fromFuture[IO, String](IO.pure(future)).attempt
    } yield assertEquals(result.left.toOption.map(_.getClass), Some(cause.getClass))
  }

  test("nullBecomesNone") {
    for {
      future <- pending[String]
      _ <- IO(future.complete(null))
      value <- KafkaFutures.fromNullableFuture[IO, String](IO.pure(future))
    } yield assertEquals(value, None)
  }

  test("cancellationCancelsTheKafkaFuture") {
    // The finaliser calls `KafkaFuture.cancel`, which is all this assertion can see. The admin
    // client does not dequeue the call; see `KafkaFutures.fromFuture`.
    for {
      future <- pending[String]
      fiber <- KafkaFutures.fromFuture[IO, String](IO.pure(future)).start
      _ <- IO.sleep(50.millis)
      _ <- fiber.cancel
      cancelled <- IO(future.isCancelled)
    } yield assert(cancelled, "the KafkaFuture was left pending after the effect was cancelled")
  }

  test("theCallerDoesNotRunOnTheCompletingThread") {
    // The continuation must not run on the thread that completed the future, because in production
    // that thread is the admin client's only network thread.
    val observed = Ref.unsafe[IO, Option[String]](None)

    for {
      future <- pending[String]
      fiber <- KafkaFutures
        .fromFuture[IO, String](IO.pure(future))
        .flatMap(_ => IO(Thread.currentThread.getName).flatMap(name => observed.set(Some(name))))
        .start
      _ <- IO.sleep(50.millis)
      _ <- IO {
        val completer = new Thread(() => { val _ = future.complete("value") }, "kafka-admin-io")
        completer.start()
        completer.join()
      }
      _ <- fiber.join
      name <- observed.get
    } yield assertEquals(name.map(_ == "kafka-admin-io"), Some(false), s"ran on $name")
  }
}
