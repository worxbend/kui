package kui.alerts.application

import cats.effect.{IO, Ref}
import munit.CatsEffectSuite

import kui.alerts.domain.*
import kui.kernel.ClusterId

/** One pass, end to end: the facts it reads, the events it is told about, and what it files.
  *
  * ==Why this suite exists==
  *
  * `AlertRulesSuite` proves the fold does the right thing *given* the events already open, and
  * `InMemoryAlertStoreSuite` proves the store keeps what it is told to keep. Neither of them looks at the one
  * line that joins them — the pass reading the feed and handing those events to the fold — and that line was
  * ungated when it was written: replacing `current.events` with an empty list left all 137 of this service's
  * cases green, and every firing rule would then have opened a fresh event **every minute**, with the open
  * count on the dashboard climbing for as long as the process ran.
  *
  * That is the shape wave 5 named: a rule gets a test when a consumer inside its own module exercises it, and
  * this rule's only consumer is a loop.
  */
final class EvaluateAlertsSuite extends CatsEffectSuite {

  import AlertsRig.*

  private val offline =
    ClusterFacts(
      partitions = FactReading.Read(PartitionFacts(offline = 2, underReplicated = 0, complete = true)),
      rebalancingGroups = FactReading.Read(Set.empty),
      logDirectories = FactReading.Read(Nil)
    )

  private val healthy =
    ClusterFacts(
      partitions = FactReading.Read(PartitionFacts(0, 0, complete = true)),
      rebalancingGroups = FactReading.Read(Set.empty),
      logDirectories = FactReading.Read(Nil)
    )

  /** A facts port that answers whatever the `Ref` currently holds, and counts how often it was asked. */
  final private class Facts(current: Ref[IO, ClusterFacts], val reads: Ref[IO, Int])
      extends ClusterFactsPort[IO] {

    def read(cluster: ClusterId): IO[ClusterFacts] = reads.update(_ + 1) >> current.get
  }

  private def rig(facts: ClusterFacts): IO[(FakeStore, Ref[IO, ClusterFacts], Facts, EvaluateAlerts[IO])] =
    for {
      store <- FakeStore.create
      state <- Ref.of[IO, ClusterFacts](facts)
      reads <- Ref.of[IO, Int](0)
      port = new Facts(state, reads)
    } yield (store, state, port, EvaluateAlerts.make[IO](port, store, limits))

  test("a second pass over an unchanged cluster opens nothing, because it sees what is already open") {
    // The rule this suite exists for. Without it the feed grows one duplicate per rule per pass, and the
    // open count on the dashboard climbs for as long as the process runs.
    for {
      rigged <- rig(offline)
      (store, _, _, evaluate) = rigged
      first <- evaluate.pass(cluster)
      second <- evaluate.pass(cluster)
      feed <- store.feed(cluster, caller, 100, None)
    } yield {
      assertEquals(first.opened.size, 1, clue = first.opened.map(_.title))
      assertEquals(second.opened, Nil, clue = second.opened.map(_.title))
      assertEquals(second.refreshed.map(_.id), first.opened.map(_.id))
      assertEquals(feed.total, 1)
      assertEquals(feed.openCount, 1)
    }
  }

  test("a pass reads the facts once and files the decision once") {
    for {
      rigged <- rig(offline)
      (store, _, port, evaluate) = rigged
      _ <- evaluate.pass(cluster)
      reads <- port.reads.get
      feed <- store.feed(cluster, caller, 100, None)
    } yield {
      assertEquals(reads, 1)
      assertEquals(feed.events.map(_.key.rule), List(AlertRule.OfflinePartitions))
    }
  }

  test("a condition that clears is resolved rather than left open for ever") {
    for {
      rigged <- rig(offline)
      (_, facts, _, evaluate) = rigged
      first <- evaluate.pass(cluster)
      _ <- facts.set(healthy)
      second <- evaluate.pass(cluster)
    } yield assertEquals(second.resolved, first.opened.map(_.id))
  }

  test("a pass whose facts are unreadable closes nothing that was open") {
    // The most dangerous thing an alerting system can do is close an alert because it stopped being able
    // to check it. The rule is the domain's; this asserts that a whole pass honours it.
    for {
      rigged <- rig(offline)
      (_, facts, _, evaluate) = rigged
      first <- evaluate.pass(cluster)
      _ <- facts.set(ClusterFacts.allUnreadable(kui.kernel.error.InfrastructureError.Unreachable("k", "no")))
      second <- evaluate.pass(cluster)
    } yield {
      assertEquals(first.opened.size, 1)
      assertEquals(second.resolved, Nil)
      assertEquals(second.opened, Nil)
      assert(second.reports.forall(_.outcome.isInstanceOf[RuleOutcome.NotEvaluated]), clue = second.reports)
    }
  }

  test("the pass asks the store for more events than the store is allowed to hold") {
    // If the pass read fewer, the fold would not see the events it could not read, and would re-open
    // them. `InMemoryAlertStoreSuite` holds the other end of this pair.
    assert(EvaluateAlerts.OpenEventsPage >= 500, clue = EvaluateAlerts.OpenEventsPage)
  }
}
