package kui.alerts.app

import scala.concurrent.duration.FiniteDuration

import cats.effect.kernel.{Resource, Temporal}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger

import kui.alerts.application.EvaluateAlerts
import kui.alerts.domain.{Evaluation, RuleOutcome}
import kui.kernel.ClusterId

/** One fibre per configured cluster, running the rules on a cadence and filing what they decided.
  *
  * ==Why a loop and not an evaluation per request==
  *
  * An alert has an age, and an age needs somebody to have been watching. Evaluating on the request path would
  * make "opened four hours ago" mean "opened when you last opened this tab", would make every repaint of the
  * dashboard four admin calls, and would mean that a cluster nobody is looking at is a cluster with no alerts
  * — which is precisely the cluster an operator most needs the bell to light up for.
  *
  * ==What a failed pass does, and does not do==
  *
  * A pass cannot fail: `ClusterFactsPort` answers a `FactReading` for every fact and raises for none, so a
  * refused admin call becomes a rule that did not run and a row that says so. What is caught here is the
  * thing that should be impossible — an adapter that raised instead of answering — and it is logged as an
  * error and dropped, because a fibre that exited on one would leave a cluster silently unevaluated for as
  * long as the process ran, with nothing on any screen saying so.
  *
  * The pass also does **not** close events when the facts are unreadable. That rule is the domain's and is
  * tested there; it is named here because it is the property that makes this loop safe to run against a
  * cluster whose admin calls are being refused.
  *
  * ==Why the first pass is immediate==
  *
  * A process that slept for its interval first would answer every request in its first `evaluationInterval`
  * with a feed that has never been evaluated, and the operator watching a fresh deployment is the one most
  * likely to be looking.
  */
object AlertEvaluationLoop {

  /** Starts the fibre and hands back its lifetime. It is cancelled when the composition root's `Resource`
    * closes, which is what stops a pass in flight from outliving the process it belongs to.
    */
  def resource[F[_]: Temporal](
      cluster: ClusterId,
      evaluate: EvaluateAlerts[F],
      interval: FiniteDuration,
      logger: StructuredLogger[F]
  ): Resource[F, Unit] =
    pass[F](cluster, evaluate, logger).andWait(interval).foreverM[Unit].background.void

  /** One pass, logged.
    *
    * @return
    *   unit either way. The caller is a loop and there is nothing it could do differently with a failure that
    *   this has not already done with it.
    */
  def pass[F[_]: Temporal](
      cluster: ClusterId,
      evaluate: EvaluateAlerts[F],
      logger: StructuredLogger[F]
  ): F[Unit] =
    evaluate
      .pass(cluster)
      .flatMap(evaluation => log[F](cluster, evaluation, logger))
      .handleErrorWith(error =>
        logger.error(error)(
          s"the alert evaluation for cluster ${cluster.value} raised instead of answering; " +
            "the events already open are kept and the next pass runs as scheduled"
        )
      )

  /** What a pass did, when it did anything.
    *
    * A pass that opened nothing, closed nothing and read every fact says nothing at all: a healthy cluster
    * evaluated every minute would otherwise write one line a minute for ever, and a log nobody can read is a
    * log nobody reads. The two things worth a line are a change to the feed and a rule that could not run.
    */
  private def log[F[_]](
      cluster: ClusterId,
      evaluation: Evaluation,
      logger: StructuredLogger[F]
  )(using Temporal[F]): F[Unit] = {
    val unevaluated = evaluation.reports.collect {
      case report if report.outcome.isInstanceOf[RuleOutcome.NotEvaluated] => report.rule.wire
    }

    val resolved = evaluation.resolved.size + evaluation.released.size
    val moved =
      logger
        .info(
          Map(
            "cluster.id" -> cluster.value,
            "alerts.opened" -> evaluation.opened.size.toString,
            "alerts.resolved" -> resolved.toString
          )
        )(
          s"${evaluation.opened.size} alert(s) opened and $resolved resolved on cluster " +
            cluster.value
        )
        .whenA(evaluation.opened.nonEmpty || resolved > 0)

    val refused =
      logger
        .warn(Map("cluster.id" -> cluster.value, "alerts.unevaluatedRules" -> unevaluated.mkString(",")))(
          s"${unevaluated.size} alert rule(s) could not be evaluated on cluster ${cluster.value}; their " +
            "rows say so rather than reporting nothing wrong, and no event of theirs was closed"
        )
        .whenA(unevaluated.nonEmpty)

    moved *> refused
  }
}
