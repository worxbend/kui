package kui.alerts.domain

import scala.concurrent.duration.DurationInt

import munit.FunSuite

import kui.kernel.GroupId

/** The four rules, one fact at a time.
  *
  * Every case here calls the product's own fold. There is no rule re-expressed in the test file and no
  * threshold comparison written out beside the one being asserted: `AlertRules.evaluate` is what the
  * evaluation loop calls, and it is what these cases call, so a rule deleted from the source cannot be
  * satisfied by a copy of it living here.
  */
final class AlertRulesSuite extends FunSuite {

  import AlertFixtures.*

  // ----------------------------------------------------------------------- the counting rules

  test("a cluster with one offline partition opens exactly one event carrying its severity and category") {
    val evaluation = firstPass(partitions(offline = 1, underReplicated = 0))

    val opened = evaluation.opened.filter(_.key.rule == AlertRule.OfflinePartitions)

    assertEquals(opened.size, 1, clue = evaluation.opened.map(_.title))
    assertEquals(opened.head.severity, AlertSeverity.Critical)
    assertEquals(opened.head.category, AlertCategory.Partition)
    // The glyph and the tone are what the two fields are *for*: a screen keys on them and they must not
    // both come from severity — SCREENS-V4 §3.9 is a correction of exactly that.
    assertEquals(opened.head.category.glyph, "partition")
    assertEquals(opened.head.severity.tone, "danger")
    assertEquals(opened.head.title, "1 partition offline")
  }

  test("a threshold raised above the fact closes no event and opens none") {
    val raised = limits.copy(offlinePartitions = 5, underReplicatedPartitions = 5)

    val evaluation = AlertRules.evaluate(
      now,
      raised,
      partitions(offline = 4, underReplicated = 4),
      Nil,
      AlertRuleState.empty
    )

    assertEquals(evaluation.opened, Nil)
    assertEquals(evaluation.resolved, Nil)
    // And the rules did run: an operator who raised a threshold gets "nothing above it", not "KUI could
    // not look". The difference is the whole reason `RuleOutcome` has three shapes.
    assertEquals(
      evaluation.reports.filter(_.outcome == RuleOutcome.Evaluated(0)).map(_.rule).toSet,
      AlertRule.All.toSet
    )
  }

  test("the same count on the next pass refreshes the open event instead of opening a second") {
    val first = firstPass(partitions(offline = 3, underReplicated = 0))
    val open = first.opened

    val second =
      AlertRules.evaluate(
        now.plusSeconds(60),
        limits,
        partitions(offline = 3, underReplicated = 0),
        open,
        first.state
      )

    assertEquals(second.opened, Nil)
    assertEquals(second.refreshed.map(_.id), open.map(_.id))
    assertEquals(second.resolved, Nil)
  }

  test("a count that falls back under its threshold resolves the event that was open") {
    val first = firstPass(partitions(offline = 3, underReplicated = 0))

    val second = AlertRules.evaluate(now.plusSeconds(60), limits, healthy, first.opened, first.state)

    assertEquals(second.resolved, first.opened.map(_.id))
    assertEquals(second.opened, Nil)
  }

  test("an incomplete topic sweep evaluates neither counting rule and closes nothing") {
    // A sweep that described most of the topics and found no offline partition has established nothing
    // about the ones it could not read. Firing would be wrong; answering "all clear" would be worse.
    val first = firstPass(partitions(offline = 4, underReplicated = 0))

    val second = AlertRules.evaluate(
      now.plusSeconds(60),
      limits,
      partitions(offline = 0, underReplicated = 0, complete = false),
      first.opened,
      first.state
    )

    assertEquals(second.opened, Nil)
    assertEquals(second.resolved, Nil, clue = "an unreadable rule must never close an event")

    val counting = second.reports.filter(report => CountingRules.contains(report.rule))
    assertEquals(counting.size, 2)
    assert(counting.forall(_.outcome.isInstanceOf[RuleOutcome.NotEvaluated]))
  }

  test("an unreadable partition fact leaves the disk rule free to run") {
    val facts = healthy.copy(
      partitions = FactReading.Unreadable(refused),
      logDirectories = FactReading.Read(List(directory("/var/lib/kafka", Some(95))))
    )

    val evaluation = firstPass(facts)

    assertEquals(evaluation.opened.map(_.key.rule), List(AlertRule.DiskUsage))
    assertEquals(
      evaluation.reports.find(_.rule == AlertRule.DiskUsage).map(_.outcome),
      Some(RuleOutcome.Evaluated(0))
    )
  }

  // ----------------------------------------------------------------------- the rebalance rule

  test("a group rebalancing for less than rebalanceDuration opens nothing and one past it opens one") {
    val first = firstPass(rebalancing(payments))

    // The first sighting is the clock's start: KUI does not know how long it had been going on before,
    // and an event that claimed to would be inventing the number that made it fire.
    assertEquals(first.opened, Nil)

    val shortOf = AlertRules.evaluate(
      now.plus(java.time.Duration.ofMinutes(4)),
      limits,
      rebalancing(payments),
      Nil,
      first.state
    )
    assertEquals(shortOf.opened, Nil, clue = "four minutes is under the five-minute threshold")

    val past = AlertRules.evaluate(
      now.plus(java.time.Duration.ofMinutes(6)),
      limits,
      rebalancing(payments),
      Nil,
      first.state
    )

    assertEquals(past.opened.size, 1)
    assertEquals(past.opened.head.key, AlertKey(AlertRule.StuckRebalance, payments.value))
    assertEquals(past.opened.head.severity, AlertSeverity.Warning)
    assertEquals(past.opened.head.category, AlertCategory.Rebalance)
  }

  test("the rebalance event says the age is measured from KUI's own first sighting") {
    // Kafka publishes no rebalance start time. The number in the detail line is therefore weaker than a
    // reader would assume, so the line says which number it is rather than leaving them to assume.
    val first = firstPass(rebalancing(payments))

    val past = AlertRules.evaluate(
      now.plus(java.time.Duration.ofMinutes(7)),
      limits,
      rebalancing(payments),
      Nil,
      first.state
    )

    assertEquals(
      past.opened.head.detail,
      "at least 7 minutes since KUI first saw it rebalancing · threshold 5 minutes"
    )
  }

  test("a group that stops rebalancing loses its sighting, so returning restarts the clock") {
    val first = firstPass(rebalancing(payments))
    val stopped = AlertRules.evaluate(now.plusSeconds(60), limits, healthy, Nil, first.state)

    assertEquals(stopped.state.rebalancingSince.get(payments), None)

    val returned = AlertRules.evaluate(
      now.plus(java.time.Duration.ofMinutes(20)),
      limits,
      rebalancing(payments),
      Nil,
      stopped.state
    )

    assertEquals(returned.opened, Nil, clue = "a rebalance that started twenty minutes late is not stuck")
  }

  test("a listing that failed keeps the sightings rather than restarting every group's clock") {
    val first = firstPass(rebalancing(payments))

    val blind = AlertRules.evaluate(
      now.plusSeconds(60),
      limits,
      healthy.copy(rebalancingGroups = FactReading.Unreadable(refused)),
      Nil,
      first.state
    )

    assertEquals(blind.state.rebalancingSince.get(payments), Some(now))
    assertEquals(blind.opened, Nil)
  }

  test("two stuck groups are two events, keyed on the group") {
    val orders = GroupId.unsafe("orders-consumer")
    val first = firstPass(rebalancing(payments, orders))

    val past = AlertRules.evaluate(
      now.plus(java.time.Duration.ofMinutes(6)),
      limits,
      rebalancing(payments, orders),
      Nil,
      first.state
    )

    assertEquals(past.opened.map(_.key.subject).toSet, Set(payments.value, orders.value))
  }

  // ----------------------------------------------------------------------- the disk rule

  test("a directory past the warning threshold opens a warning and past the critical one opens a critical") {
    val warning = firstPass(directories(directory("/var/lib/kafka", Some(diskWarningPercent))))
    val critical = firstPass(directories(directory("/var/lib/kafka", Some(diskCriticalPercent))))

    assertEquals(warning.opened.map(_.severity), List(AlertSeverity.Warning))
    assertEquals(critical.opened.map(_.severity), List(AlertSeverity.Critical))
    assertEquals(critical.opened.head.category, AlertCategory.Storage)
    assertEquals(critical.opened.head.detail, "threshold 90% · broker-1:/var/lib/kafka")
  }

  test("a directory one percent under the warning threshold opens nothing") {
    val evaluation = firstPass(directories(directory("/var/lib/kafka", Some(diskWarningPercent - 1))))

    assertEquals(evaluation.opened, Nil)
  }

  test("a directory whose broker reported no capacity opens nothing and is counted as unmeasured") {
    // The used bytes alone cannot say whether they are most of a disk or a rounding error on one.
    // Filling the gap with anything at all would put a severity on a screen no measurement supports.
    val evaluation = firstPass(
      directories(
        directory("/var/lib/kafka", None),
        directory("/mnt/data", Some(95))
      )
    )

    assertEquals(evaluation.opened.map(_.key.subject), List("broker-1:/mnt/data"))
    assertEquals(
      evaluation.reports.find(_.rule == AlertRule.DiskUsage).map(_.outcome),
      Some(RuleOutcome.Evaluated(1))
    )
  }

  // ----------------------------------------------------------------------- the shape of a pass
  test("unknown and missing disks preserve open alerts while measured healthy disks clear") {
    val first = firstPass(
      directories(
        directory("/unknown", Some(95)),
        directory("/missing", Some(95)),
        directory("/healthy", Some(95))
      )
    )
    val next = AlertRules.evaluate(
      now.plusSeconds(60),
      limits,
      directories(directory("/unknown", None), directory("/healthy", Some(20))),
      first.opened,
      first.state
    )
    assertEquals(next.resolved, first.opened.filter(_.key.subject.endsWith("/healthy")).map(_.id))
    assertEquals(next.refreshed, Nil)
  }

  test("every rule appears in the report of every pass, whatever it found") {
    val evaluation = firstPass(healthy)

    assertEquals(evaluation.reports.map(_.rule), AlertRule.All)
  }

  test("an event's id survives a second pass, so a screen's acknowledge link does not go stale") {
    val first = firstPass(partitions(offline = 2, underReplicated = 0))
    val second =
      AlertRules.evaluate(
        now.plusSeconds(120),
        limits,
        partitions(offline = 2, underReplicated = 0),
        first.opened,
        first.state
      )

    assertEquals(second.opened, Nil)
    assertEquals(second.refreshed.map(_.id), first.opened.map(_.id))
  }

  test("a duration reads to one unit and rounds down, so 'at least' is true rather than nearly true") {
    assertEquals(AlertRules.humanise(90.seconds), "1 minute")
    assertEquals(AlertRules.humanise(119.minutes), "1 hour")
    assertEquals(AlertRules.humanise(47.hours), "1 day")
    assertEquals(AlertRules.humanise(2.seconds), "2 seconds")
  }

  private val CountingRules: Set[AlertRule] =
    Set(AlertRule.OfflinePartitions, AlertRule.UnderReplicatedPartitions)
}
