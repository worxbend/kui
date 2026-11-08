package kui.kafka

import java.util.concurrent.{CompletionException, ExecutionException}

import scala.annotation.tailrec

import cats.effect.Async
import cats.syntax.all.*
import org.apache.kafka.common.KafkaFuture

/** The bridge from Kafka's own future to a cats-effect one.
  *
  * Two things go wrong if this is written at a call site, and both are recorded in
  * `research/kafka/admin-capabilities.md` §0.
  *
  * A `KafkaFuture` completes exceptionally with a `CompletionException` or an `ExecutionException` wrapping
  * the error that matters. Code that matches on the thrown type therefore matches the wrapper, misses every
  * specific case, and falls through to a generic branch — which is how "you are not authorized for that
  * topic" becomes "something went wrong".
  *
  * And every completion callback runs on the admin client's single network thread. Anything KUI does there —
  * a metric, a log line, a `flatMap` that happens to block — stalls every other in-flight request on that
  * connection until `request.timeout.ms` fires.
  */
object KafkaFutures {

  /** Issues the request and completes when the future does, with the cause unwrapped and execution back on
    * the effect runtime rather than on Kafka's I/O thread.
    *
    * Cancelling the resulting effect only stops KUI waiting. The finaliser calls `KafkaFuture.cancel`, which
    * completes this effect, but kafka-clients 4.3.1 does not dequeue the call: `KafkaAdminClient` never reads
    * `isCancelled`. The request keeps its place on the admin thread until the broker answers or
    * `default.api.timeout.ms` expires.
    */
  def fromFuture[F[_]: Async, A](make: F[KafkaFuture[A]]): F[A] =
    make.flatMap { future =>
      Async[F].async[A] { callback =>
        Async[F].delay {
          // `whenComplete` returns a new future; the callback is registered on `future` itself,
          // which is the one cancellation has to reach.
          val _ = future.whenComplete { (value, failure) =>
            Option(failure) match {
              case Some(thrown) => callback(Left(unwrap(thrown)))
              case None => callback(Right(value))
            }
          }

          // Stops KUI waiting. It does not dequeue the call; see the method comment.
          Some(Async[F].delay {
            val _ = future.cancel(true)
          })
        }
      }
    }

  /** As `fromFuture`, but a `null` result becomes `None` rather than a `NullPointerException` three frames
    * away.
    *
    * Kafka returns `null` for a missing committed offset and for the controller during a KRaft failover. Both
    * are normal states of a healthy cluster, not errors.
    */
  def fromNullableFuture[F[_]: Async, A](make: F[KafkaFuture[A]]): F[Option[A]] =
    fromFuture(make).map(Option(_))

  /** Unwraps `CompletionException` and `ExecutionException`, repeatedly, down to the first cause that is
    * neither.
    *
    * Public because `KafkaErrorMapper` (KAFKA-005) and `AdminInvalidation` both classify the unwrapped
    * throwable, and a classification applied to the wrapper classifies nothing.
    */
  @tailrec
  def unwrap(t: Throwable): Throwable = t match {
    case wrapper @ (_: CompletionException | _: ExecutionException) =>
      Option(wrapper.getCause) match {
        case Some(cause) => unwrap(cause)
        // A wrapper with no cause is all the information there is; returning it beats returning
        // nothing.
        case None => wrapper
      }
    case other => other
  }
}
