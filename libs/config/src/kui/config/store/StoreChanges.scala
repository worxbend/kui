package kui.config.store

import cats.effect.std.Queue
import cats.effect.{Async, Ref, Resource}
import cats.syntax.all.*
import fs2.Stream

/** One mailbox per subscriber. Publishing never waits.
  *
  * `fs2.concurrent.Topic` blocks `publish1` while any subscriber's mailbox is full, and that publish sits
  * inside the fold every read depends on. A subscriber that stops reading would stop the store. A full
  * mailbox drops its oldest change and is handed `Desynchronized` instead, which tells it to re-read.
  */
private[store] trait StoreChanges[F[_]] {

  def publish(change: StoreChange): F[Unit]

  /** Hot: it does not replay history, and it does not complete while this fan-out is open. */
  def subscribe: Stream[F, StoreChange]

  /** Registers a subscriber and returns its stream without pulling it.
    *
    * Pulling here would drain the mailbox. A test — and only a test — needs the subscriber registered and
    * still full, so it can see the `Desynchronized` the overflow produced.
    */
  def listen: Resource[F, Stream[F, StoreChange]]
}

private[store] object StoreChanges {

  def fanout[F[_]: Async](capacity: Int): F[StoreChanges[F]] =
    Ref.of[F, Vector[Mailbox[F]]](Vector.empty).map(new Fanout[F](_, math.max(1, capacity)))

  final private class Mailbox[F[_]](val queue: Queue[F, StoreChange], val missed: Ref[F, Long])

  final private class Fanout[F[_]: Async](subscribers: Ref[F, Vector[Mailbox[F]]], capacity: Int)
      extends StoreChanges[F] {

    def publish(change: StoreChange): F[Unit] =
      subscribers.get.flatMap(_.traverse_(offer(change)))

    def subscribe: Stream[F, StoreChange] =
      Stream.resource(listen).flatten

    def listen: Resource[F, Stream[F, StoreChange]] =
      Resource.make(open)(close).map(mailbox => Stream.fromQueueUnterminated(mailbox.queue))

    private def open: F[Mailbox[F]] =
      for {
        queue <- Queue.bounded[F, StoreChange](capacity)
        missed <- Ref.of[F, Long](0L)
        mailbox = new Mailbox(queue, missed)
        _ <- subscribers.update(_ :+ mailbox)
      } yield mailbox

    private def close(mailbox: Mailbox[F]): F[Unit] =
      subscribers.update(_.filterNot(_ eq mailbox))

    private def offer(change: StoreChange)(mailbox: Mailbox[F]): F[Unit] =
      mailbox.queue.tryOffer(change).flatMap {
        case true => Async[F].unit
        case false =>
          mailbox.missed.updateAndGet(_ + 1).flatMap { missed =>
            deliver(mailbox, StoreChange.Desynchronized(missed))
          }
      }

    /** One drop frees a slot. A second attempt covers a race with the subscriber taking at the same moment.
      */
    private def deliver(mailbox: Mailbox[F], signal: StoreChange): F[Unit] =
      mailbox.queue.tryTake *> mailbox.queue.tryOffer(signal).flatMap {
        case true => Async[F].unit
        case false => deliver(mailbox, signal)
      }
  }
}
