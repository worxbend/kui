package kui.alerts.infrastructure

import java.time.Instant

import scala.concurrent.duration.DurationInt

import cats.effect.{IO, Ref}
import munit.CatsEffectSuite

import kui.alerts.application.EvaluateAlerts
import kui.alerts.domain.*
import kui.cache.CacheMetrics
import kui.kernel.{ClusterId, UserName}
import kui.security.{Principal, PrincipalKind}

/** The two bounds, the read markers and the change stream. */
final class InMemoryAlertStoreSuite extends CatsEffectSuite {

  private val cluster = ClusterId.unsafe("prod-eu")
  private val at = Instant.parse("2026-09-07T12:00:00Z")
  private val retention = 7.days

  private val ada = Principal(UserName.unsafe("ada"), Set.empty, PrincipalKind.Session)
  private val grace = Principal(UserName.unsafe("grace"), Set.empty, PrincipalKind.Session)

  private def store = InMemoryAlertStore.resource[IO](retention, CacheMetrics.noop[IO])

  private def event(subject: String, openedAt: Instant): AlertEvent =
    AlertEvent.open(
      AlertKey(AlertRule.DiskUsage, subject),
      AlertSeverity.Warning,
      openedAt,
      s"$subject is full",
      ""
    )

  private def opening(events: List[AlertEvent]): Evaluation =
    Evaluation(
      events,
      Nil,
      Nil,
      AlertRuleState.empty,
      AlertRule.All.map(RuleReport(_, RuleOutcome.evaluated))
    )

  /** Two clusters nobody asserts about, which is what makes the publication cases exact. */
  private val warmUp = ClusterId.unsafe("warm-up")
  private val sentinel = ClusterId.unsafe("sentinel")

  /** Every frame `writes` caused for [[cluster]], and no other frame at all.
    *
    * ==Three timing assumptions, all removed==
    *
    * These cases used to count frames against a baseline read at a moment chosen by a sleep, and one of them
    * **failed under load**: measured here, `an acknowledgement publishes a frame` failed inside
    * `./mill services.connect.__.test + services.alerts.__.test + services.metrics.__.test` and passed when
    * `services.alerts.infrastructure.test` ran alone. The cause was not the store. It was that the seeding
    * loop can publish more than once before the first frame is observed, so the baseline is taken with frames
    * still in the subscriber's queue and the case that expects `before + 1` sees `before + 2`.
    *
    * So: `changes` subscribes when its stream is *pulled* and `.start` only schedules the fibre, so the
    * subscription is confirmed live by recording on `warmUp` until a `warmUp` frame arrives. Whatever that
    * loop published is on a cluster this method filters out. `writes` then runs on [[cluster]]. Finally a
    * record on `sentinel` closes the sequence and the wait is for **that** frame: fs2's `Topic` delivers to
    * one subscriber in publication order, so a sentinel frame in hand proves every earlier frame is in hand
    * too. Counting [[cluster]]'s frames after that is exact rather than a sleep-and-look, and a publication
    * that was dropped fails the count rather than a timeout.
    */
  private def framesFor(held: InMemoryAlertStore[IO])(writes: IO[Unit]): IO[List[ClusterId]] =
    for {
      seen <- Ref.of[IO, List[ClusterId]](Nil)
      watching <- held.changes.evalMap(id => seen.update(_ :+ id)).compile.drain.start
      _ <- (held.record(warmUp, opening(List(event("warm-up:/probe", at))), at) >> IO.sleep(20.millis))
        .untilM_(seen.get.map(_.contains(warmUp)))
      _ <- writes
      _ <- held.record(sentinel, opening(List(event("sentinel:/probe", at))), at)
      _ <- IO.sleep(10.millis).untilM_(seen.get.map(_.contains(sentinel)))
      frames <- seen.get
      _ <- watching.cancel
    } yield frames.filter(_ == cluster)

  test("an event older than the retention window is dropped on the next pass") {
    val stale = event("broker-1:/old", at.minusMillis(retention.toMillis + 1000L))
    val fresh = event("broker-1:/new", at)

    store.use { held =>
      for {
        _ <- held.record(cluster, opening(List(stale, fresh)), at)
        feed <- held.feed(cluster, ada, 100, None)
      } yield assertEquals(feed.events.map(_.key.subject), List("broker-1:/new"))
    }
  }

  test("an event inside the window survives, so the boundary is the window and not the pass") {
    val justInside = event("broker-1:/old", at.minusMillis(retention.toMillis - 1000L))

    store.use { held =>
      for {
        _ <- held.record(cluster, opening(List(justInside)), at)
        feed <- held.feed(cluster, ada, 100, None)
      } yield assertEquals(feed.total, 1)
    }
  }

  test("retention runs from when an event opened, not from when it was last seen firing") {
    // `bounded`'s own scaladoc states the anchor — *"An event is kept for `retention` after it opened,
    // resolved or not"* — and nothing asserted it: swapping `_.openedAt` for `_.lastSeenAt` left the
    // whole alerts tree green. The consequence is not cosmetic. A condition that keeps firing has its
    // `lastSeenAt` moved on every pass, so anchoring there would keep a persistently-firing event for
    // ever and the bound would stop being a bound. It is also the wrong question: an incident review
    // reads a week of events, and `retention` is what the operator configured that week to mean.
    val old = at.minusMillis(retention.toMillis + 1000L)
    val stale = event("broker-1:/persistent", old)

    store.use { held =>
      for {
        _ <- held.record(cluster, opening(List(stale)), old)
        // Still firing, so this pass moves `lastSeenAt` to now and leaves `openedAt` where it was.
        _ <- held.record(cluster, Evaluation(Nil, List(stale), Nil, AlertRuleState.empty, Nil), at)
        feed <- held.feed(cluster, ada, 100, None)
      } yield assertEquals(feed.total, 0, clue = "an event past its retention survived by still firing")
    }
  }

  test("the store keeps at most 500 events per cluster, oldest first out") {
    // 620 and 500 are both literals. Building the input as `MaxEventsPerCluster + 1` is how wave 5's
    // schema packet shipped a bound that could be raised a thousandfold with every suite green, and it is
    // the first thing an adversary looks for.
    val many = (1 to 620).toList.map(index => event(s"broker-1:/disk-$index", at.minusSeconds(620L - index)))

    store.use { held =>
      for {
        _ <- held.record(cluster, opening(many), at)
        feed <- held.feed(cluster, ada, 1000, None)
      } yield {
        assertEquals(feed.total, 500)
        assertEquals(InMemoryAlertStore.MaxEventsPerCluster, 500)
        // Newest kept: the last event seeded is the newest, and the first is the one dropped.
        assert(feed.events.exists(_.key.subject == "broker-1:/disk-620"))
        assert(!feed.events.exists(_.key.subject == "broker-1:/disk-1"))
      }
    }
  }

  test("one pass can see every event the store is allowed to hold") {
    // `EvaluateAlerts` asks for `OpenEventsPage` events so the fold can tell an already-open condition
    // from a new one. If the store could hold more than the pass reads, the pass would re-open the
    // events it could not see, and the feed would grow one duplicate per minute.
    assert(
      InMemoryAlertStore.MaxEventsPerCluster <= EvaluateAlerts.OpenEventsPage,
      clue = s"${InMemoryAlertStore.MaxEventsPerCluster} > ${EvaluateAlerts.OpenEventsPage}"
    )
  }

  test("a resolved event is closed rather than removed, so the design's card can draw it") {
    val open = event("broker-1:/var", at)

    store.use { held =>
      for {
        _ <- held.record(cluster, opening(List(open)), at)
        _ <- held.record(
          cluster,
          Evaluation(Nil, Nil, List(open.id), AlertRuleState.empty, Nil),
          at.plusSeconds(60)
        )
        feed <- held.feed(cluster, ada, 100, None)
      } yield {
        assertEquals(feed.total, 1)
        assertEquals(feed.openCount, 0)
        assertEquals(feed.events.head.resolution.map(_.kind), Some(AlertResolutionKind.Cleared))
      }
    }
  }

  test("a still-firing event has its lastSeenAt moved and its openedAt left alone") {
    val open = event("broker-1:/var", at)

    store.use { held =>
      for {
        _ <- held.record(cluster, opening(List(open)), at)
        _ <- held.record(
          cluster,
          Evaluation(Nil, List(open), Nil, AlertRuleState.empty, Nil),
          at.plusSeconds(300)
        )
        feed <- held.feed(cluster, ada, 100, None)
      } yield {
        assertEquals(feed.events.head.openedAt, at)
        assertEquals(feed.events.head.lastSeenAt, at.plusSeconds(300))
      }
    }
  }

  test("the open count is the whole store's and not the page's, against the shipped store") {
    // Three open events and a page of one, asserted against `InMemoryAlertStore` rather than against a
    // fixture that re-implements it. `AlertsRig.FakeStore` and `AlertsTestServer.CountingStore` each
    // hand-write this method, so a case that reads one of them proves the fixture counts correctly and
    // says nothing about the store the process runs.
    //
    // The `limit = 0` read is the one that matters. `AlertUseCases.acknowledge` and `AlertsRoutes.changes`
    // both ask for zero rows and read `openCount` off the answer, so a count taken over the page would
    // make every acknowledgement response and every SSE frame carry `openCount = 0` — the pill and the
    // bell going dark on a cluster with three open events, with no gate in this repository red.
    val open = List(
      event("broker-1:/var", at),
      event("broker-2:/var", at.plusSeconds(1)),
      event("broker-3:/var", at.plusSeconds(2))
    )

    store.use { held =>
      for {
        _ <- held.record(cluster, opening(open), at.plusSeconds(2))
        page <- held.feed(cluster, ada, 1, None)
        none <- held.feed(cluster, ada, 0, None)
      } yield {
        assertEquals(page.events.size, 1)
        assertEquals(page.total, 3)
        assertEquals(page.openCount, 3)
        assertEquals(page.openByRule, Map(AlertRule.DiskUsage -> 3))

        assertEquals(none.events, Nil)
        assertEquals(none.openCount, 3, clue = "the read the stream and the acknowledgement make")
        assertEquals(none.openByRule, Map(AlertRule.DiskUsage -> 3))
      }
    }
  }

  test("markRead returns the marker it wrote and a cleared unread count") {
    val opened = List(event("broker-1:/var", at), event("broker-2:/var", at.plusSeconds(1)))

    store.use { held =>
      for {
        _ <- held.record(cluster, opening(opened), at.plusSeconds(1))
        first <- held.feed(cluster, ada, 100, Some(at.plusSeconds(60)))
        second <- held.feed(cluster, ada, 100, None)
      } yield {
        assertEquals(first.unreadCount, 0, clue = "the response clears the bell immediately")
        assertEquals(first.lastReadAt, Some(at.plusSeconds(60)))
        assertEquals(second.unreadCount, 0)
        assertEquals(second.lastReadAt, Some(at.plusSeconds(60)))
      }
    }
  }

  test("an acknowledgement publishes a frame, so the bell moves without a reload") {
    // The service's one write, and the only thing an SSE subscriber can be woken by that is not a rule
    // pass. `record`'s publication has two cases of its own in this file; dropping `acknowledge`'s left
    // every case green, which means acknowledging on one tab would leave every other tab's bell showing
    // the old count until its next poll.
    //
    // The event is seeded *before* the subscription, so the only write this cluster's frames can come
    // from is the acknowledgement. See `framesFor` for why no part of this is timed.
    val open = event("broker-1:/var", at)

    store.use { held =>
      for {
        _ <- held.record(cluster, opening(List(open)), at)
        closed <- Ref.of[IO, Option[Boolean]](None)
        frames <- framesFor(held)(
          held.acknowledge(cluster, open.id, at.plusSeconds(30), "ada").flatMap { answer =>
            closed.set(answer.map(_.isOpen).toOption)
          }
        )
        acknowledged <- closed.get
      } yield {
        assertEquals(acknowledged, Some(false))
        assertEquals(frames, List(cluster), clue = "the acknowledgement woke no subscriber")
      }
    }
  }

  test("a pass that only resolves an event publishes a frame, so a cleared bell clears itself") {
    // **The rule this packet owns.** `record`'s publication is guarded by
    // `opened.nonEmpty || resolved.nonEmpty`, and every other case that records only ever *opens* an
    // event — so deleting
    // the `|| evaluation.resolved.nonEmpty` half left every alerts task SUCCESS. Under that deletion the
    // condition clearing itself is the one thing an operator never sees without a reload: the pill stays
    // red and the bell stays lit on a cluster whose disk went back under the threshold, until the next
    // pass happens to open something else.
    //
    // The event is seeded before the subscription, so the only write this cluster's frames can come from
    // is the resolving pass. See `framesFor` for why no part of this is timed.
    val open = event("broker-1:/var", at)

    store.use { held =>
      for {
        _ <- held.record(cluster, opening(List(open)), at)
        // Nothing opened, nothing refreshed: the only change this pass carries is the resolution.
        frames <- framesFor(held)(
          held.record(
            cluster,
            Evaluation(Nil, Nil, List(open.id), AlertRuleState.empty, Nil),
            at.plusSeconds(60)
          )
        )
        feed <- held.feed(cluster, ada, 100, None)
      } yield {
        assertEquals(feed.openCount, 0, clue = "the fixture did not actually resolve the event")
        assertEquals(frames, List(cluster), clue = "a pass that only resolved an event woke no subscriber")
      }
    }
  }

  test("the read-marker cache holds the bound it was built with, so a marker survives a full cache") {
    // **The rule this packet owns.** `MaxReadMarkers` is handed to `BoundedCache.make` and nothing
    // observed it: lowering it from 2000 to 2 left every alerts task SUCCESS, and under that a deployment
    // with three people watching alert feeds would have the third read evict the first person's marker and
    // relight a bell they had already cleared.
    //
    // Written from *both* directions, because only one of them can be behavioural. The lower direction is:
    // a marker written first is still readable after `MaxReadMarkers` distinct principals have written
    // theirs — Caffeine evicts nothing while the entry count is at or under the maximum, so this is exact
    // rather than approximate, and it fails for any bound the store actually passes that is smaller than
    // the constant. The upper direction cannot be behavioural here: Caffeine evicts *approximately*
    // (`BoundedCache.make`'s own scaladoc) and this store exposes no `stats`, so a raised bound is caught by
    // the literal instead — 2000 written out, not `MaxReadMarkers` compared with itself, which is the
    // mistake `MaxEventsPerCluster`'s case above names.
    val principals =
      (1 until InMemoryAlertStore.MaxReadMarkers.toInt).toList
        .map(index => Principal(UserName.unsafe(s"reader-$index"), Set.empty, PrincipalKind.Session))

    store.use { held =>
      for {
        _ <- held.record(cluster, opening(List(event("broker-1:/var", at))), at)
        // ada is the first of exactly `MaxReadMarkers` principals to write a marker.
        _ <- held.feed(cluster, ada, 0, Some(at.plusSeconds(60)))
        _ <- principals.foldLeft(IO.unit)((written, reader) =>
          written >> held.feed(cluster, reader, 0, Some(at.plusSeconds(60))).void
        )
        hers <- held.feed(cluster, ada, 100, None)
      } yield {
        assertEquals(InMemoryAlertStore.MaxReadMarkers, 2000L)
        assertEquals(
          hers.lastReadAt,
          Some(at.plusSeconds(60)),
          clue = s"${principals.size + 1} markers evicted the first one, so the bound is not 2000"
        )
        assertEquals(hers.unreadCount, 0)
      }
    }
  }

  test("a read marker is one principal's and does not clear anybody else's bell") {
    store.use { held =>
      for {
        _ <- held.record(cluster, opening(List(event("broker-1:/var", at))), at)
        _ <- held.feed(cluster, ada, 100, Some(at.plusSeconds(60)))
        hers <- held.feed(cluster, ada, 100, None)
        his <- held.feed(cluster, grace, 100, None)
      } yield {
        assertEquals(hers.unreadCount, 0)
        assertEquals(his.unreadCount, 1)
      }
    }
  }

  test("the rule state survives a pass, so a rebalance clock is not restarted every minute") {
    val remembered = AlertRuleState(Map(kui.kernel.GroupId.unsafe("payments") -> at))

    store.use { held =>
      for {
        _ <- held.record(cluster, Evaluation(Nil, Nil, Nil, remembered, Nil), at)
        state <- held.ruleState(cluster)
      } yield assertEquals(state, remembered)
    }
  }

  test("the last pass's rule reports are what a later read reports") {
    val refused = RuleReport(
      AlertRule.DiskUsage,
      RuleOutcome.NotEvaluated(kui.kernel.error.InfrastructureError.Unreachable("kafka-admin", "no"))
    )

    store.use { held =>
      for {
        _ <- held.record(cluster, Evaluation(Nil, Nil, Nil, AlertRuleState.empty, List(refused)), at)
        feed <- held.feed(cluster, ada, 100, None)
      } yield assertEquals(feed.reports, List(refused))
    }
  }

  test("a cluster that has never been evaluated has no evaluatedAt, so its zeros are readable") {
    store.use { held =>
      held.feed(cluster, ada, 100, None).map { feed =>
        assertEquals(feed.evaluatedAt, None)
        assertEquals(feed.openCount, 0)
      }
    }
  }

  test("acknowledging an event that is not there and one that is already closed are both 409s") {
    val open = event("broker-1:/var", at)

    store.use { held =>
      for {
        _ <- held.record(cluster, opening(List(open)), at)
        missing <- held.acknowledge(cluster, event("broker-1:/nowhere", at).id, at, "ada")
        first <- held.acknowledge(cluster, open.id, at, "ada")
        again <- held.acknowledge(cluster, open.id, at, "ada")
      } yield {
        assertEquals(missing.left.map(_.code), Left(kui.kernel.error.ErrorCode.InvalidState))
        assertEquals(first.map(_.isOpen), Right(false))
        assertEquals(again.left.map(_.code), Left(kui.kernel.error.ErrorCode.InvalidState))
      }
    }
  }

  test("a pass that changed nothing publishes no frame, so an idle cluster does not wake every bell") {
    // A no-change pass and a change, published in that order on the same cluster. `framesFor` waits for
    // the sentinel, which fs2's `Topic` delivers after both, so the answer is the complete list: one
    // frame if only the change published, two if the empty pass published as well. A sleep-and-look
    // would have given a false pass on a loaded machine, which is what this case did on its first run
    // under `./scripts/run-tests.sh`.
    store.use { held =>
      framesFor(held)(
        held.record(cluster, Evaluation(Nil, Nil, Nil, AlertRuleState.empty, Nil), at) >>
          held.record(cluster, opening(List(event("broker-1:/var", at))), at)
      ).map(frames => assertEquals(frames, List(cluster)))
    }
  }
}
