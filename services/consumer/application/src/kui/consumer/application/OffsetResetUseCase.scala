package kui.consumer.application

import java.time.Instant

import scala.concurrent.duration.*

import cats.effect.kernel.Temporal
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger

import kui.consumer.domain.*
import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.kernel.{ClusterId, GroupId}
import kui.security.Principal
import kui.security.audit.MutationKind

/** A plan, and the token that authorises applying exactly it. */
final case class PlannedReset(plan: ResetPlan, token: String, expiresAt: Instant)

/** The two halves of an offset reset: work out what would happen, then do exactly that (ADR-045).
  *
  * The split is not ceremony. A form submission carries what the operator typed — "the beginning", "two hours
  * ago", "offset 900 000" — and none of those is a number until it has been resolved against the cluster,
  * clamped into the range each partition holds, and put through KIP-122's rule for a timestamp with no
  * matching record. The operator confirms the resolved offsets, and the apply step writes those and nothing
  * else.
  */
trait OffsetResetUseCase[F[_]] {

  /** Resolve the spec against live offsets and return what would be written.
    *
    * Order: the group exists (by listing, never by describing) → the cluster is not read-only → the group is
    * empty in both senses → the offset window → the planner → the token. A read-only cluster is refused here
    * as well as at apply, so the wizard never renders a plan the operator cannot apply.
    */
  def plan(
      cluster: ClusterId,
      group: GroupId,
      scope: ResetScope,
      spec: ResetSpec
  ): F[Either[KuiError, PlannedReset]]

  /** Apply exactly the plan the token names.
    *
    * Order: verify the token → **re-check the precondition** → the guard (read-only, audit, invalidation) →
    * the write. Nothing stands between the re-check and the write but the write itself: the precondition is
    * checked at plan time and again here because the two-phase flow makes the window between them wider, not
    * narrower, and the broker's own rejection is a third line of defence rather than the first.
    */
  def apply(
      principal: Principal,
      cluster: ClusterId,
      group: GroupId,
      token: String
  ): F[Either[KuiError, ResetPlan]]
}

object OffsetResetUseCase {

  val Operation: String = "kui.consumer.reset"

  /** Five minutes: long enough to read a plan of a hundred partitions, short enough that the cluster it was
    * computed against is still recognisably the same one.
    */
  val TokenTtl: FiniteDuration = 5.minutes

  def make[F[_]: Temporal](
      admin: ClusterId => GroupAdminPort[F],
      guard: MutationGuard[F],
      profiles: ClusterProfileSource[F],
      tokens: PlanToken[F],
      logger: StructuredLogger[F]
  ): OffsetResetUseCase[F] =
    new OffsetResetUseCase[F] {

      private def context(cluster: ClusterId, group: GroupId): Map[String, String] =
        Map(
          "service.name" -> ConsumerService.Id.value,
          "operation" -> Operation,
          "cluster.id" -> cluster.value,
          "group.id" -> group.value
        )

      def plan(
          cluster: ClusterId,
          group: GroupId,
          scope: ResetScope,
          spec: ResetSpec
      ): F[Either[KuiError, PlannedReset]] = {
        val port = admin(cluster)

        (for {
          _ <- requireExists(port, group)
          profile <- EitherTLike(profiles.profileOf(cluster))
          _ <- EitherTLike(
            Temporal[F].pure(
              Either.cond(
                !profile.readOnly,
                (),
                ApplicationError.Refused(
                  ErrorCode.ReadOnly,
                  s"cluster ${profile.displayName} is configured read-only, so its consumer groups cannot be reset"
                ): KuiError
              )
            )
          )
          _ <- requireEmpty(port, group)
          now <- EitherTLike(Temporal[F].realTimeInstant.map(_.asRight[KuiError]))
          window <- EitherTLike(port.offsetWindow(group, scope, ResetPlanner.timestampOf(spec, now)))
          planned <- EitherTLike(
            Temporal[F].pure(
              ResetPlanner.plan(group, scope, spec, window, now).left.map(refusalToError)
            )
          )
          expiresAt = now.plusMillis(TokenTtl.toMillis)
          token <- EitherTLike(tokens.mint(cluster, planned, expiresAt).map(_.asRight[KuiError]))
          _ <- EitherTLike(
            logger
              .info(context(cluster, group))(
                s"planned a ${planned.spec.target.wire} reset over ${planned.partitions.size} partition(s), " +
                  s"${planned.warnings.size} warning(s)"
              )
              .map(_.asRight[KuiError])
          )
        } yield PlannedReset(planned, token, expiresAt)).value
      }

      def apply(
          principal: Principal,
          cluster: ClusterId,
          group: GroupId,
          token: String
      ): F[Either[KuiError, ResetPlan]] = {
        val port = admin(cluster)

        (for {
          now <- EitherTLike(Temporal[F].realTimeInstant.map(_.asRight[KuiError]))
          plan <- EitherTLike(tokens.verify(cluster, group, token, now))
          _ <- requireExists(port, group)
          // The second check. The group can have gained a member since the plan was rendered, and
          // that is precisely the race the two-phase flow widens.
          _ <- requireEmpty(port, group)
          window <- EitherTLike(port.offsetWindow(group, plan.scope, None))
          _ <- EitherTLike(Temporal[F].pure(requireRetainedTargets(plan, window)))
          before = window.committed.view.filterKeys(plan.offsets.contains).toMap
          _ <- EitherTLike(
            guard.guard(
              principal,
              cluster,
              MutationKind.ResetOffsets,
              group.value,
              AuditOffsets.of(before),
              AuditOffsets.of(plan.offsets)
            )(port.applyOffsets(group, plan.offsets))
          )
          // The receipt says what each partition's offset *was*, and this is the only place that
          // knows: the token carries the proposed offsets and nothing else, so a plan read back out
          // of one has no `current` in it. Without this the wizard's receipt renders an em dash in
          // every "from" cell, on the one screen whose entire job is to say what moved where.
        } yield plan.withCurrent(before)).value
      }

      /** Existence is confirmed by listing, never by describing.
        *
        * Describing a group that does not exist answers with a fabricated dead group — the port's documented
        * invariant — so a describe cannot answer this question at all. A reset of a group that is not there
        * would otherwise succeed silently and create it.
        */
      private def requireExists(port: GroupAdminPort[F], group: GroupId): EitherTLike[F, Unit] =
        EitherTLike(
          port.exists(group).map {
            case Right(true) => ().asRight[KuiError]
            case Right(false) =>
              ApplicationError.NotFound("consumer group", group.value, ErrorCode.GroupNotFound).asLeft[Unit]
            case Left(error) => error.asLeft[Unit]
          }
        )

      private def requireEmpty(port: GroupAdminPort[F], group: GroupId): EitherTLike[F, Unit] =
        EitherTLike(
          port.describe(List(group)).map {
            case Left(error) => error.asLeft[Unit]
            case Right(described) =>
              described.get(group) match {
                case None =>
                  ApplicationError
                    .InvalidState(s"consumer group ${group.value} could not be described")
                    .asLeft[Unit]
                case Some(found) if !found.completeness.membersKnown =>
                  ApplicationError
                    .InvalidState(s"consumer group ${group.value} members are unknown")
                    .asLeft[Unit]
                case Some(found) =>
                  found.offsetChangeRefusal match {
                    case None => ().asRight[KuiError]
                    case Some(reason) if found.members.nonEmpty =>
                      ApplicationError.Refused(ErrorCode.GroupNotEmpty, reason).asLeft[Unit]
                    case Some(reason) => ApplicationError.InvalidState(reason).asLeft[Unit]
                  }
              }
          }
        )

      /** Never clamp an approved target again: retention requires a new preview, not a different reset. */
      private def requireRetainedTargets(plan: ResetPlan, window: OffsetWindow): Either[KuiError, Unit] =
        Either.cond(
          plan.offsets.forall { (partition, target) =>
            !window.leaderless.contains(partition) &&
            window.begin.get(partition).exists(_.value <= target.value) &&
            window.end.get(partition).exists(_.value >= target.value)
          },
          (),
          ApplicationError.InvalidState(
            "reset targets are no longer within known retained offsets; preview the reset again"
          )
        )
    }

  /** A planner refusal, in the vocabulary the wire speaks. `KUI-INVALID-STATE` for everything the cluster's
    * own state caused, and validation for what the request got wrong.
    */
  private def refusalToError(refusal: ResetRefusal): KuiError = refusal match {
    case ResetRefusal.Leaderless(_) => ApplicationError.InvalidState(refusal.message)
    case ResetRefusal.NoPartitionsInScope => ApplicationError.Invalid(refusal.message, Nil)
    case ResetRefusal.UnknownPartition(_) => ApplicationError.Invalid(refusal.message, Nil)
    case ResetRefusal.NegativeResult(_) => ApplicationError.Invalid(refusal.message, Nil)
  }

  /** A minimal `EitherT`, so that the ordered sequence above reads as the list of checks it is.
    *
    * `cats.data.EitherT` would do the same job; this avoids adding a dependency on `cats.data` to a module
    * whose only other use of it would be this one function.
    */
  final private case class EitherTLike[F[_]: Temporal, A](value: F[Either[KuiError, A]]) {

    def map[B](f: A => B): EitherTLike[F, B] = EitherTLike(value.map(_.map(f)))

    def flatMap[B](f: A => EitherTLike[F, B]): EitherTLike[F, B] =
      EitherTLike(value.flatMap {
        case Right(a) => f(a).value
        case Left(error) => error.asLeft[B].pure[F]
      })
  }
}
