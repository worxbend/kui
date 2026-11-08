package kui.alerts.api

import java.time.Instant

import cats.effect.IO
import fs2.Stream
import io.circe.parser.parse
import munit.CatsEffectSuite

import kui.alerts.application.{AcknowledgedAlert, AlertFeed, AlertStore}
import kui.alerts.domain.*
import kui.kernel.ClusterId
import kui.kernel.error.KuiError
import kui.security.Principal

/** What one subscriber sees on the change stream.
  *
  * ==Why this is a suite rather than a line in a route suite==
  *
  * The frames are what stop the card's pill and the bell's dot disagreeing, and two properties of them are
  * decided in `AlertsRoutes.changes` and nowhere else: **which** cluster's changes reach a subscriber, and
  * **what** a frame carries. Neither is visible to `AlertsRoutesSuite`, which drives the JSON routes through
  * a stub backend that has no streaming capability at all.
  *
  * Both were ungated when this service was first written, and both were found by mutation: replacing the
  * filter's equality with a prefix match left every one of the service's 133 cases green, and a subscriber
  * watching `prod-eu` would then have been woken by every change on `prod-eu-2`.
  */
final class AlertsStreamSuite extends CatsEffectSuite {

  private val watched = ClusterId.unsafe("prod-eu")
  private val neighbour = ClusterId.unsafe("prod-eu-2")
  private val at = Instant.parse("2026-09-07T12:00:00Z")

  /** A store whose feed answers a fixed open count per cluster and whose changes are a fixed list.
    *
    * It also records how each read was **asked for**, because the third property decided in
    * `AlertsRoutes.changes` is not visible in a frame at all: reading a stream must not mark the feed read.
    */
  final private class Publishing(published: List[ClusterId], counts: Map[ClusterId, Int])
      extends AlertStore[IO] {

    private val asked = scala.collection.mutable.ListBuffer.empty[(Int, Option[Instant])]

    def reads: List[(Int, Option[Instant])] = asked.toList

    def feed(id: ClusterId, principal: Principal, limit: Int, markRead: Option[Instant]): IO[AlertFeed] =
      IO {
        asked += ((limit, markRead))
        AlertFeed(Nil, 0, counts.getOrElse(id, 0), Map.empty, 0, None, Some(at), Nil)
      }

    def acknowledge(
        id: ClusterId,
        event: AlertEventId,
        when: Instant,
        by: String
    ): IO[Either[KuiError, AcknowledgedAlert]] =
      IO.raiseError(new IllegalStateException("not under test"))

    def record(id: ClusterId, evaluation: Evaluation, when: Instant): IO[Unit] = IO.unit

    def ruleState(id: ClusterId): IO[AlertRuleState] = IO.pure(AlertRuleState.empty)

    def changes: Stream[IO, ClusterId] = Stream.emits(published)
  }

  test("a subscriber is woken by its own cluster and by no other, however alike the ids are") {
    val store = new Publishing(List(neighbour, watched, neighbour), Map(watched -> 3, neighbour -> 99))

    AlertsRoutes
      .changes[IO](store, watched, Principal.Anonymous)
      .compile
      .toList
      .map { frames =>
        assertEquals(frames.size, 1, clue = frames.map(_.data.noSpaces))
        assertEquals(
          frames.head.data.hcursor.get[String]("cluster"),
          Right(watched.value)
        )
      }
  }

  test("a frame carries the open count as the store answers it now, not a count it was published with") {
    // The store publishes an id and nothing else; the count is read back per frame, so a subscriber that
    // connects mid-burst gets the count as it *is* rather than as it was at whichever notification it
    // happened to catch.
    val store = new Publishing(List(watched), Map(watched -> 7))

    AlertsRoutes
      .changes[IO](store, watched, Principal.Anonymous)
      .compile
      .toList
      .map(frames => assertEquals(frames.head.data.hcursor.get[Int]("openCount"), Right(7)))
  }

  test("a frame is named so an EventSource listener can register for it, and parses as the DTO") {
    val store = new Publishing(List(watched), Map(watched -> 1))

    AlertsRoutes
      .changes[IO](store, watched, Principal.Anonymous)
      .compile
      .toList
      .map { frames =>
        assertEquals(frames.head.name, "alerts")
        assert(
          parse(frames.head.data.noSpaces)
            .flatMap(_.as[kui.alerts.contract.dto.AlertChangeDto])
            .isRight,
          clue = frames.head.data.noSpaces
        )
      }
  }

  test("a frame is not a read, so the stream never clears the bell it exists to ring") {
    // `changes` states this rule in words — *"a bell that cleared itself because its own stream delivered
    // a frame would be a bell that never lit up"* — and nothing asserted it: passing `Some(now)` instead
    // of `None` left all 1,289 alerts tasks green. It cannot be seen in a frame, because what `markRead`
    // changes is the principal's marker and not the document; so it is asserted on the argument.
    //
    // The page size is here for the same reason and is `0` on purpose: the frame needs a count and no
    // rows, and a stream that asked for a page would carry one subscriber's events into the cost of every
    // other subscriber's notification.
    val store = new Publishing(List(watched, watched), Map(watched -> 4))

    AlertsRoutes
      .changes[IO](store, watched, Principal.Anonymous)
      .compile
      .toList
      .map(_ => assertEquals(store.reads, List((0, None), (0, None))))
  }

  test("a frame carries no events, so one subscriber's feed never reaches another's socket") {
    val store = new Publishing(List(watched), Map(watched -> 1))

    AlertsRoutes
      .changes[IO](store, watched, Principal.Anonymous)
      .compile
      .toList
      .map { frames =>
        assertEquals(frames.head.data.hcursor.keys.map(_.toList), Some(List("cluster", "openCount", "at")))
      }
  }
}
