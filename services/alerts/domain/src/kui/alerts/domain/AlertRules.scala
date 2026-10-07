package kui.alerts.domain

import java.time.Instant

import scala.concurrent.duration.{DurationLong, FiniteDuration}

import kui.kernel.GroupId
import kui.kernel.error.{InfrastructureError, KuiError}

/** Whether a rule reached a verdict on this pass.
  *
  * The third state is the one that matters and it is the reason this is not an `Option[List[AlertEvent]]`: a
  * rule that could not read its fact has **not** established that nothing is wrong, and the feed must be able
  * to say so in words rather than answer a zero. That is the product's central promise applied to its own
  * alerting: "no offline partitions" and "KUI could not count the partitions" are different sentences.
  */
enum RuleOutcome {

  /** The fact was read and the rule was applied to all of it.
    *
    * @param unmeasuredSubjects
    *   how many individual subjects the rule declined to judge because the number it compares was not
    *   measured — today, log directories whose broker reported no capacity. Zero for every other rule. It is
    *   counted rather than dropped so that "nothing is above 80%" can be qualified with "of the four
    *   directories whose capacity this broker reports".
    */
  case Evaluated(unmeasuredSubjects: Int)

  case NotEvaluated(reason: KuiError)
}

object RuleOutcome {

  val evaluated: RuleOutcome = Evaluated(0)

  given CanEqual[RuleOutcome, RuleOutcome] = CanEqual.derived
}

/** What one rule did on one pass. */
final case class RuleReport(rule: AlertRule, outcome: RuleOutcome)

object RuleReport {
  given CanEqual[RuleReport, RuleReport] = CanEqual.derived
}

/** What the rules have to remember between passes.
  *
  * One entry, and it exists because Kafka does not publish the fact the rebalance rule needs. A group's state
  * is `PREPARING_REBALANCE`; there is no "since". So the duration in that rule is measured from **KUI's own
  * first sighting**, which is a weaker fact than the operator might assume and is therefore said out loud in
  * the event's own detail line rather than left to be inferred from a number.
  *
  * It is carried through the fold rather than held in the store, so that the rule is a function of its inputs
  * and a suite can hand it two passes and assert what the second one does.
  */
final case class AlertRuleState(rebalancingSince: Map[GroupId, Instant])

object AlertRuleState {

  val empty: AlertRuleState = AlertRuleState(Map.empty)

  given CanEqual[AlertRuleState, AlertRuleState] = CanEqual.derived
}

/** What one pass decided.
  *
  * @param opened
  *   events for conditions that were not already open. A condition that is *still* firing opens nothing: it
  *   is the same event, and [[refreshed]] carries its latest severity, text and observation time.
  * @param resolved
  *   ids of events whose rule ran and no longer fires. An event whose rule could **not** run is not here:
  *   closing an alert because KUI stopped being able to check it is the single most dangerous thing an
  *   alerting system can do.
  */
final case class Evaluation(
    opened: List[AlertEvent],
    refreshed: List[AlertEvent],
    resolved: List[AlertEventId],
    state: AlertRuleState,
    reports: List[RuleReport]
)

object Evaluation {
  given CanEqual[Evaluation, Evaluation] = CanEqual.derived
}

/** The four rules, as one pure function.
  *
  * Nothing here performs I/O, reads a clock or generates a number: the instant, the limits, the facts and the
  * events already open are all arguments. That is what makes "a group rebalancing for four minutes opens
  * nothing and one rebalancing for six opens one" a two-line test rather than a fixture with a broker in it.
  *
  * ==What a rule may compare, and what it may not==
  *
  * A rule fires on a number the product **measured**. It never fires on a number the product derived from
  * something else, and it never fires on a fact it could not read. Both halves have a case below and both are
  * the same rule as ADR-052's: a severity inferred from a threshold KUI chose is an alert, and a severity
  * inferred from a number KUI did not measure is a fabrication.
  */
object AlertRules {

  /** One pass.
    *
    * @param open
    *   the events currently open, as the store holds them. It is an input rather than something this function
    *   discovers, because "is this already open" is the difference between an alert and a per-minute log.
    */
  def evaluate(
      now: Instant,
      limits: AlertLimits,
      facts: ClusterFacts,
      open: List[AlertEvent],
      state: AlertRuleState
  ): Evaluation = {
    val partitionRules = countRules(limits, facts.partitions)
    val rebalance = rebalanceRule(now, limits, facts.rebalancingGroups, state)
    val disk = diskRule(limits, facts.logDirectories)

    val fired: Map[AlertKey, Firing] =
      (partitionRules.fired ++ rebalance.fired ++ disk.fired).map(firing => firing.key -> firing).toMap

    val reports = partitionRules.reports ++ rebalance.reports ++ disk.reports

    // Only a rule that actually ran this pass may close one of its events. A rule whose fact was
    // unreadable leaves its events exactly as they were, which is why this is a set of rules rather than
    // "everything that did not fire".
    val ran: Set[AlertRule] = reports.collect { case RuleReport(rule, RuleOutcome.Evaluated(_)) =>
      rule
    }.toSet

    val openByKey = open.filter(_.isOpen).map(event => event.key -> event).toMap
    // Absence from a partial broker response is not a healthy measurement. Disk alerts may only
    // clear when this exact directory supplied a percentage on this pass.
    val measuredDisks = facts.logDirectories.toOption.toList.flatten
      .filter(_.usedPercent.nonEmpty)
      .map(directory => AlertKey(AlertRule.DiskUsage, directory.label))
      .toSet

    val opened = fired.collect {
      case (key, firing) if !openByKey.contains(key) =>
        AlertEvent.open(key, firing.severity, now, firing.title, firing.detail)
    }.toList

    val refreshed = openByKey.toList.flatMap { (key, event) =>
      fired
        .get(key)
        .map(firing =>
          event.copy(
            severity = firing.severity,
            title = firing.title,
            detail = firing.detail,
            lastSeenAt = now
          )
        )
    }

    val resolved = openByKey.collect {
      case (key, event)
          if ran.contains(key.rule) && !fired.contains(key) &&
            (key.rule != AlertRule.DiskUsage || measuredDisks.contains(key)) =>
        event.id
    }.toList

    Evaluation(
      opened = opened.sorted,
      refreshed = refreshed,
      resolved = resolved,
      state = rebalance.state,
      reports = reports
    )
  }

  /** What a rule decided about one subject: which key, how loud, and the two lines a row draws. */
  final private case class Firing(
      key: AlertKey,
      severity: AlertSeverity,
      title: String,
      detail: String
  )

  final private case class RuleResult(
      fired: List[Firing],
      reports: List[RuleReport],
      state: AlertRuleState
  )

  /** The two cluster-wide counting rules, which share one fact and therefore one refusal.
    *
    * An **incomplete** sweep is a refusal and not a zero, and this is the rule W6-01 was most at risk of
    * getting wrong. `describeTopics` over ten thousand topics is chunked and a chunk can fail on its own; a
    * sweep that read four hundred of four hundred and eighteen topics and found no offline partition has
    * established nothing about the other eighteen. Firing on it would be wrong in the safe direction and
    * *not* firing on it silently would be wrong in the dangerous one, so it is neither: the rule does not run
    * and the feed says why.
    */
  private def countRules(limits: AlertLimits, reading: FactReading[PartitionFacts]): RuleResult =
    reading match {
      case FactReading.Unreadable(failure) =>
        RuleResult(
          Nil,
          CountingRules.map(RuleReport(_, RuleOutcome.NotEvaluated(failure))),
          AlertRuleState.empty
        )

      case FactReading.Read(facts) if !facts.complete =>
        val refusal: KuiError = InfrastructureError.Unreachable(
          "kafka-admin",
          "the topic sweep did not describe every topic, so a partition count over it would be a count " +
            "of the topics KUI could read rather than of the cluster"
        )
        RuleResult(
          Nil,
          CountingRules.map(RuleReport(_, RuleOutcome.NotEvaluated(refusal))),
          AlertRuleState.empty
        )

      case FactReading.Read(facts) =>
        val offline = Option.when(facts.offline >= limits.offlinePartitions)(
          Firing(
            key = AlertKey.cluster(AlertRule.OfflinePartitions),
            severity = AlertSeverity.Critical,
            title = s"${plural(facts.offline, "partition")} offline",
            detail = s"no leader · threshold ${limits.offlinePartitions}"
          )
        )

        val underReplicated = Option.when(facts.underReplicated >= limits.underReplicatedPartitions)(
          Firing(
            key = AlertKey.cluster(AlertRule.UnderReplicatedPartitions),
            severity = AlertSeverity.Warning,
            title = s"${plural(facts.underReplicated, "partition")} under-replicated",
            detail = s"fewer in-sync replicas than replicas · threshold ${limits.underReplicatedPartitions}"
          )
        )

        RuleResult(
          fired = offline.toList ++ underReplicated.toList,
          reports = CountingRules.map(RuleReport(_, RuleOutcome.evaluated)),
          state = AlertRuleState.empty
        )
    }

  /** A group that has been reassigning for longer than an ordinary reassignment takes.
    *
    * The comparison is `elapsed >= limits.rebalanceDuration`, and `elapsed` is measured from the first pass
    * on which KUI saw this group rebalancing — never from a broker timestamp, because there is none. A group
    * KUI has only just noticed therefore has an elapsed of zero and cannot fire, which is the correct answer
    * on the pass after a restart: KUI does not know how long it had been going on, and an alert that claimed
    * to would be inventing the number that made it fire.
    */
  private def rebalanceRule(
      now: Instant,
      limits: AlertLimits,
      reading: FactReading[Set[GroupId]],
      state: AlertRuleState
  ): RuleResult =
    reading match {
      case FactReading.Unreadable(failure) =>
        // The sightings are kept. A pass that could not list the groups has not established that any of
        // them stopped rebalancing, and forgetting when they started would restart every clock.
        RuleResult(
          Nil,
          List(RuleReport(AlertRule.StuckRebalance, RuleOutcome.NotEvaluated(failure))),
          state
        )

      case FactReading.Read(groups) =>
        val since = groups.map(group => group -> state.rebalancingSince.getOrElse(group, now)).toMap

        val fired = since.toList.flatMap { (group, seenAt) =>
          val elapsed = (now.toEpochMilli - seenAt.toEpochMilli).millis

          Option.when(elapsed >= limits.rebalanceDuration)(
            Firing(
              key = AlertKey(AlertRule.StuckRebalance, group.value),
              severity = AlertSeverity.Warning,
              title = s"Consumer group ${group.value} is stuck rebalancing",
              detail = s"at least ${humanise(elapsed)} since KUI first saw it rebalancing · " +
                s"threshold ${humanise(limits.rebalanceDuration)}"
            )
          )
        }

        RuleResult(
          fired = fired,
          reports = List(RuleReport(AlertRule.StuckRebalance, RuleOutcome.evaluated)),
          state = AlertRuleState(since)
        )
    }

  /** A log directory past one of its two thresholds.
    *
    * A directory whose broker reported no capacity has **no** percentage and opens nothing. It is counted
    * into `unmeasuredSubjects` instead, so the feed can say "of the six directories whose capacity this
    * cluster reports" rather than implying it looked at all of them. Filling the gap with the used bytes, or
    * with a zero, would put a severity on a screen that no measurement supports.
    */
  private def diskRule(limits: AlertLimits, reading: FactReading[List[LogDirectoryFact]]): RuleResult =
    reading match {
      case FactReading.Unreadable(failure) =>
        RuleResult(
          Nil,
          List(RuleReport(AlertRule.DiskUsage, RuleOutcome.NotEvaluated(failure))),
          AlertRuleState.empty
        )

      case FactReading.Read(directories) =>
        val fired = directories.flatMap(directory =>
          directory.usedPercent.flatMap(percent =>
            severityFor(limits, percent).map { case (severity, threshold) =>
              Firing(
                key = AlertKey(AlertRule.DiskUsage, directory.label),
                severity = severity,
                title =
                  s"Log directory ${directory.path} is $percent% full on broker ${directory.broker.value}",
                detail = s"threshold $threshold% · ${directory.label}"
              )
            }
          )
        )

        RuleResult(
          fired = fired,
          reports = List(
            RuleReport(
              AlertRule.DiskUsage,
              RuleOutcome.Evaluated(directories.count(_.usedPercent.isEmpty))
            )
          ),
          state = AlertRuleState.empty
        )
    }

  /** Critical wins over warning, and a directory below both fires nothing.
    *
    * The two thresholds are held apart by the loader (`diskUsedCriticalPercent > diskUsedWarningPercent`), so
    * this does not re-check the ordering — see [[AlertLimits]] for why re-checking it would be worse than not
    * checking it.
    */
  private def severityFor(limits: AlertLimits, percent: Int): Option[(AlertSeverity, Int)] =
    if percent >= limits.diskUsedCriticalPercent then
      Some(AlertSeverity.Critical -> limits.diskUsedCriticalPercent)
    else if percent >= limits.diskUsedWarningPercent then
      Some(AlertSeverity.Warning -> limits.diskUsedWarningPercent)
    else None

  /** The two rules that read one `describeTopics` sweep, so that a refusal reports both. */
  private val CountingRules: List[AlertRule] =
    List(AlertRule.OfflinePartitions, AlertRule.UnderReplicatedPartitions)

  /** `1 partition` / `4 partitions`, because a title reading `1 partitions offline` is a title somebody stops
    * trusting the rest of.
    */
  private def plural(count: Int, noun: String): String =
    if count == 1 then s"1 $noun" else s"$count ${noun}s"

  /** A duration as a person reads it, to one unit.
    *
    * Rounded **down**, so that the sentence "at least six minutes" is true rather than nearly true. An age on
    * this screen is evidence in an incident review and the direction of the rounding is the difference
    * between a claim and an overstatement.
    */
  def humanise(duration: FiniteDuration): String = {
    val seconds = duration.toSeconds

    if seconds >= 86400 then plural((seconds / 86400).toInt, "day")
    else if seconds >= 3600 then plural((seconds / 3600).toInt, "hour")
    else if seconds >= 60 then plural((seconds / 60).toInt, "minute")
    else plural(seconds.toInt, "second")
  }
}
