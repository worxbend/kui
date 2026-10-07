package kui.ksql.application

import java.time.Instant

import cats.Monad
import cats.effect.kernel.Clock
import cats.syntax.all.*
import fs2.Stream

import kui.kernel.ClusterId
import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.ksql.domain.*
import kui.security.Principal
import kui.security.audit.MutationOutcome

/** What the object read produced for one cluster.
  *
  * Two cases and not an `Either` that might be empty. `NotConfigured` is a deployment with no
  * `kui.clusters.<n>.ksql.url`, which is the ordinary case and reaches the browser as `not_configured` with a
  * 200. Rule A3 keeps `Section` out of this layer, so the distinction is made in this vocabulary and
  * `KsqlMapping` turns it into the wire's.
  */
enum KsqlListing {
  case NotConfigured
  case Answered(objects: Either[KuiError, KsqlObjects])
}

object KsqlListing {
  given CanEqual[KsqlListing, KsqlListing] = CanEqual.derived
}

/** What running this statement would do, and the confirmation it needs if it needs one.
  *
  * @param warnings
  *   sentences for the person about to confirm, in the order they should be read. Destructive plans are
  *   refused unless the exact object kind, name and backing topic can be identified and signed.
  * @param token
  *   `None` when nothing needs confirming, which is every statement that is not destructive.
  */
final case class StatementPlan(
    statement: KsqlStatement,
    warnings: List[String],
    token: Option[String],
    expiresAt: Option[Instant],
    computedAt: Instant
)

object StatementPlan {
  given CanEqual[StatementPlan, StatementPlan] = CanEqual.derived
}

/** A statement that ran, and what it produced. */
final case class ExecutedStatement(
    statement: KsqlStatement,
    outcome: StatementOutcome,
    executedAt: Instant
)

object ExecutedStatement {
  given CanEqual[ExecutedStatement, ExecutedStatement] = CanEqual.derived
}

/** The four things a caller can do to this service.
  *
  * ==`Left` on the read is only ever a wrong request==
  *
  * A cluster KUI has never heard of is a 404, because the caller followed a link to something that does not
  * exist. Everything else answers 200 with a document that says what it knows, exactly as the metrics, alerts
  * and connect services do: the ksqlDB screen sits in a product where the rest of the screens work, and a 4xx
  * would make a server behaving exactly as designed — down, starting, half-answering — indistinguishable from
  * a broken KUI.
  *
  * ==The writes are the other way round==
  *
  * A statement is a request to change something, so its failure is the caller's answer: a refused statement
  * must not answer 200 with a document saying it did not happen, or a browser that showed a success toast
  * would be telling the truth about the response and lying about the cluster.
  */
trait KsqlUseCases[F[_]] {

  def objects(principal: Principal, cluster: ClusterId): F[Either[KuiError, KsqlListing]]

  def plan(principal: Principal, cluster: ClusterId, statement: String): F[Either[KuiError, StatementPlan]]

  def execute(
      principal: Principal,
      cluster: ClusterId,
      statement: String,
      token: Option[String]
  ): F[Either[KuiError, ExecutedStatement]]

  /** A push query, opened.
    *
    * The outer `F` fails when the request cannot start at all — a cluster that does not exist, a deployment
    * with no ksqlDB, a statement that is not a push query — and the inner stream carries everything that goes
    * wrong afterwards, because a failure after the response headers have gone has to reach the browser as an
    * `error` frame rather than as a connection that simply closes (ADR-035).
    */
  def stream(
      principal: Principal,
      cluster: ClusterId,
      statement: String
  ): F[Either[KuiError, Stream[F, Either[KuiError, QueryFrame]]]]
}

object KsqlUseCases {

  /** The sentence a push query gets when it is sent to the JSON endpoint, and a finishing statement gets when
    * it is sent to the stream. Both name the address that *does* answer, because "wrong endpoint" with no
    * alternative is a dead end for whoever is holding a `curl`.
    */
  val PushQueryElsewhere: String =
    "a SELECT ... EMIT CHANGES is a push query: it never finishes, so it is answered by " +
      "GET .../ksql/stream?statement=... and not here"

  val FinishingQueryElsewhere: String =
    "this stream answers push queries only; a statement that finishes is answered by " +
      "POST .../ksql/statements"

  def make[F[_]: {Monad, Clock}](
      profiles: ClusterKsqlSource[F],
      tokens: KsqlPlanToken[F],
      guard: MutationGuard[F]
  ): KsqlUseCases[F] =
    new KsqlUseCases[F] {

      def objects(principal: Principal, cluster: ClusterId): F[Either[KuiError, KsqlListing]] =
        profiles.profileOf(cluster).flatMap {
          case Left(error) => error.asLeft[KsqlListing].pure[F]
          case Right(profile) if !profile.configured =>
            KsqlListing.NotConfigured.asRight[KuiError].pure[F]
          case Right(_) =>
            profiles.client(cluster).flatMap {
              case None => KsqlListing.Answered(notWired(cluster).asLeft).asRight[KuiError].pure[F]
              case Some(client) =>
                client.objects.map(answer => KsqlListing.Answered(answer).asRight[KuiError])
            }
        }

      /** The plan, which changes nothing and is therefore **not** inside the guard.
        *
        * It writes no audit record for the same reason: an audit trail that recorded every preview would
        * record the statements nobody ran, and "who dropped this topic" would then have to be answered by
        * ignoring most of its own rows. The read-only refusal still applies — it is at the route, because
        * `Action.KsqlExecute.isAlter` is what the plan endpoint declares — and ADR-055 §7 says why a preview
        * is gated by the permission of the thing it previews.
        */
      def plan(
          principal: Principal,
          cluster: ClusterId,
          raw: String
      ): F[Either[KuiError, StatementPlan]] =
        KsqlStatement.parse(raw) match {
          case Left(problem) => invalid(problem).asLeft[StatementPlan].pure[F]
          case Right(statement) =>
            profiles.profileOf(cluster).flatMap {
              case Left(error) => error.asLeft[StatementPlan].pure[F]
              case Right(profile) if !profile.configured =>
                notConfigured(cluster).asLeft[StatementPlan].pure[F]
              case Right(_) =>
                describe(cluster, statement).flatMap {
                  case Left(error) => error.asLeft[StatementPlan].pure[F]
                  case Right((warnings, binding)) =>
                    for {
                      now <- Clock[F].realTimeInstant
                      minted <- binding
                        .traverse(value => tokens.mint(cluster, value, now.plus(KsqlPlanToken.Ttl)))
                    } yield StatementPlan(
                      statement,
                      warnings,
                      minted,
                      minted.as(now.plus(KsqlPlanToken.Ttl)),
                      now
                    )
                      .asRight[KuiError]
                }
            }
        }

      def execute(
          principal: Principal,
          cluster: ClusterId,
          raw: String,
          token: Option[String]
      ): F[Either[KuiError, ExecutedStatement]] =
        KsqlStatement.parse(raw) match {
          case Left(problem) => invalid(problem).asLeft[ExecutedStatement].pure[F]

          // Before the guard, and deliberately: a push query sent here ran nothing, changed nothing and is
          // a malformed request rather than an attempt to alter a cluster. Auditing it would put rows in
          // the trail that describe things that did not happen.
          case Right(statement) if statement.push =>
            ApplicationError
              .Invalid(PushQueryElsewhere, Nil)
              .asLeft[ExecutedStatement]
              .pure[F]

          case Right(statement) =>
            guard.guard[ExecutedStatement](
              principal,
              cluster,
              statement,
              KsqlPlanToken.Operation,
              executed =>
                executed.outcome match {
                  case StatementOutcome.Pending(_, _) => MutationOutcome.Unknown
                  case _ => MutationOutcome.Succeeded
                }
            ) {
              profiles.profileOf(cluster).flatMap {
                case Left(error) => error.asLeft[ExecutedStatement].pure[F]

                case Right(profile) if !profile.configured =>
                  notConfigured(cluster).asLeft[ExecutedStatement].pure[F]

                case Right(_) =>
                  confirmed(cluster, statement, token).flatMap {
                    case Left(refusal) => refusal.asLeft[ExecutedStatement].pure[F]
                    case Right(_) =>
                      profiles.client(cluster).flatMap {
                        case None => notWired(cluster).asLeft[ExecutedStatement].pure[F]
                        case Some(client) =>
                          client.execute(statement).flatMap {
                            case Left(error) => error.asLeft[ExecutedStatement].pure[F]
                            case Right(outcome) =>
                              Clock[F].realTimeInstant
                                .map(now => ExecutedStatement(statement, outcome, now).asRight[KuiError])
                          }
                      }
                  }
              }
            }
        }

      def stream(
          principal: Principal,
          cluster: ClusterId,
          raw: String
      ): F[Either[KuiError, Stream[F, Either[KuiError, QueryFrame]]]] =
        KsqlStatement.parse(raw) match {
          case Left(problem) => invalid(problem).asLeft[Stream[F, Either[KuiError, QueryFrame]]].pure[F]

          case Right(statement) if !statement.push =>
            ApplicationError
              .Invalid(FinishingQueryElsewhere, Nil)
              .asLeft[Stream[F, Either[KuiError, QueryFrame]]]
              .pure[F]

          case Right(statement) =>
            profiles.profileOf(cluster).flatMap {
              case Left(error) => error.asLeft[Stream[F, Either[KuiError, QueryFrame]]].pure[F]

              // The same refusal `MutationGuard` makes for a statement, made here because a push query is
              // not a mutation and never reaches that guard. A push query asks ksqlDB to start and hold a
              // query, which `Action.KsqlExecute` classifies as altering, and a read-only cluster whose
              // ksqlDB anybody can set queries running on is not read-only in any sense an operator means.
              // ADR-055 §3 states the cost: a read-only cluster cannot watch a stream either.
              case Right(profile) if profile.readOnly =>
                ApplicationError
                  .Refused(
                    ErrorCode.ReadOnly,
                    s"cluster ${profile.displayName} is configured read-only, so no ksqlDB query is started"
                  )
                  .asLeft[Stream[F, Either[KuiError, QueryFrame]]]
                  .pure[F]

              case Right(profile) if !profile.configured =>
                notConfigured(cluster).asLeft[Stream[F, Either[KuiError, QueryFrame]]].pure[F]

              case Right(_) =>
                profiles.client(cluster).map {
                  case None => notWired(cluster).asLeft[Stream[F, Either[KuiError, QueryFrame]]]
                  case Some(client) => client.rows(statement).asRight[KuiError]
                }
            }
        }

      /** The plan's warnings, and the one figure it goes and looks up.
        *
        * A destructive statement's whole content is *which topic disappears*, so the object it names is
        * looked up in the cluster's own listing and the topic behind it is quoted. Failed or ambiguous
        * lookups cannot authorize deletion. The same identity is re-read and checked before execution.
        */
      private def describe(
          cluster: ClusterId,
          statement: KsqlStatement
      ): F[Either[KuiError, (List[String], Option[String])]] =
        if !statement.destructive then
          ((if statement.push then List(PushQueryElsewhere) else Nil), none[String]).asRight[KuiError].pure[F]
        else
          profiles.client(cluster).flatMap {
            case None => notWired(cluster).asLeft[(List[String], Option[String])].pure[F]
            case Some(client) =>
              client.objects.map(_.flatMap { objects =>
                val matches = objects.items.filter(item =>
                  statement.target.contains(item.name) && statement.targetKind.contains(item.kind)
                )
                val identified = matches match {
                  case (item @ KsqlObject.Stream(_, topic, format)) :: Nil =>
                    Some((item, topic, format.toList))
                  case (item @ KsqlObject.Table(_, topic, format, windowed)) :: Nil =>
                    Some((item, topic, windowed.toString :: format.toList))
                  case _ => None
                }
                identified
                  .toRight(
                    ApplicationError.Invalid(
                      "KUI cannot identify the exact ksqlDB object and Kafka topic to delete; refresh the objects and plan again",
                      Nil
                    ): KuiError
                  )
                  .map { case (item, topic, properties) =>
                    val fields = List(
                      "ksql-delete-v2",
                      statement.canonical,
                      item.kind.wire,
                      item.name,
                      topic
                    ) ++ properties
                    val binding = fields.map(value => s"${value.length}:$value").mkString
                    (
                      List(
                        s"This deletes the Kafka topic '$topic' and every record in it. Nothing in KUI can undo it."
                      ),
                      Some(binding)
                    )
                  }
              })
          }

      /** The confirmation, checked only for the statements that need one.
        *
        * A token sent with a harmless statement is **ignored rather than refused**: a client that always
        * sends back the token it last received is doing nothing wrong, and refusing it would make the editor
        * fail on the request after a confirmed one.
        */
      private def confirmed(
          cluster: ClusterId,
          statement: KsqlStatement,
          token: Option[String]
      ): F[Either[KuiError, Unit]] =
        if !statement.destructive then ().asRight[KuiError].pure[F]
        else
          token match {
            case None =>
              ApplicationError
                .Invalid(
                  "this statement deletes a Kafka topic, so it needs a confirmation: plan it at " +
                    "POST .../ksql/statements/plan and send the token it answers with",
                  Nil
                )
                .asLeft[Unit]
                .pure[F]
            case Some(value) =>
              describe(cluster, statement).flatMap {
                case Left(error) => error.asLeft[Unit].pure[F]
                case Right((_, Some(binding))) =>
                  Clock[F].realTimeInstant.flatMap(now => tokens.verify(cluster, binding, value, now))
                case _ =>
                  ApplicationError
                    .Invalid("the destructive target could not be verified; plan again", Nil)
                    .asLeft[Unit]
                    .pure[F]
              }
          }
    }

  private def invalid(problem: StatementProblem): KuiError =
    ApplicationError.Invalid(problem.message, Nil)

  /** A cluster that configured no ksqlDB, asked to do something with one.
    *
    * `KUI-UNSUPPORTED`, which is the same classification the read gives it as a section: one fact, one code,
    * whether it arrives as a `not_configured` section or as a status.
    */
  private def notConfigured(cluster: ClusterId): KuiError =
    ApplicationError.Unsupported(
      s"no ksqlDB is configured for cluster '${cluster.value}' (kui.clusters.<n>.ksql.url)"
    )

  /** Configured, and no client built for it. A wiring failure rather than a deployment choice, and it says
    * so: reporting it as "not configured" would hide a KUI defect behind a row that looks deliberately
    * switched off.
    */
  private def notWired(cluster: ClusterId): KuiError =
    ApplicationError.InvalidState(
      s"a ksqlDB is configured for cluster '${cluster.value}' and this process could not build a client " +
        "for it"
    )

}
