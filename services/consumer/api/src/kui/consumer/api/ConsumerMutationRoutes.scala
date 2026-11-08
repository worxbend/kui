package kui.consumer.api

import java.time.Instant

import scala.concurrent.duration.FiniteDuration

import cats.effect.kernel.{Async, Clock}
import cats.syntax.all.*
import sttp.tapir.server.ServerEndpoint

import kui.consumer.application.*
import kui.consumer.contract.ConsumerMutationEndpoints
import kui.consumer.contract.dto.*
import kui.consumer.domain.{ResetScope, ResetSpec}
import kui.http.principal.SecuredRoutes
import kui.kernel.error.{ApplicationError, KuiError}
import kui.kernel.{Offset, PartitionId, TopicName, TopicPartition}

/** The four routes that change a cluster, and the one that only says what a change would do.
  *
  * ==The destructive request is the second one (ADR-045)==
  *
  * `planReset` reads live offsets, resolves what the operator asked for against them, and answers with the
  * exact numbers that would be written plus a signed token. `applyReset` takes only that token. There is no
  * route in this file that accepts a specification and writes offsets in one hop, and that is the whole point
  * of the pair: a form submission carries what the operator typed, not what the cluster will do, and only the
  * server can compute the second.
  *
  * ==Read-only and audit (ADR-047)==
  *
  * Neither refusal nor recording is implemented here. `MutationGuard` in the application layer owns both, and
  * every write goes through it: it resolves the cluster's profile, refuses a read-only cluster with
  * `KUI-READ-ONLY` **without touching a Kafka client**, records the attempt either way, and invalidates the
  * snapshot on success. This module contributes nothing to that decision, which is why there is no read-only
  * check written out below — one written here would be a second copy of the rule, and the copy that can
  * disagree.
  *
  * `planReset` refuses on a read-only cluster too, and it is deliberately not a mutation. A wizard that
  * happily renders a plan the operator is not allowed to apply teaches them that the refusal at the end is a
  * bug; the honest moment to say "not on this cluster" is before they compose the change.
  *
  * ==The CSRF header==
  *
  * Every endpoint here carries it and none of them checks it. `KuiEndpoint.mutation` declares the header, so
  * a request without one fails to decode and never reaches this file; binding its *value* to a session is
  * M6's job, because there is no session in M4 to bind it to. Requiring the header from the day the endpoint
  * exists is what stops it from being a breaking change for every client that already shipped.
  */
object ConsumerMutationRoutes {

  def apply[F[_]: Async](
      reset: OffsetResetUseCase[F],
      deleteGroup: DeleteGroupUseCase[F],
      deleteOffsets: DeleteOffsetsUseCase[F],
      snapshots: GroupSnapshots[F],
      secured: ConsumerApi.Securing[F]
  ): List[ServerEndpoint[Any, F]] =
    List(
      planReset(reset, snapshots, secured),
      applyReset(reset, secured),
      deleteGroupRoute(deleteGroup, secured),
      deleteOffsetsRoute(deleteOffsets, secured)
    )

  /** What a reset would do. Changes nothing. */
  private def planReset[F[_]: Async](
      reset: OffsetResetUseCase[F],
      snapshots: GroupSnapshots[F],
      secured: ConsumerApi.Securing[F]
  ): ServerEndpoint[Any, F] =
    secured.withBody(ConsumerMutationEndpoints.planReset)((_, _, _, request) =>
      SecuredRoutes.bodyBytes(request)
    ) { _ => (_, cluster, group, request) =>
      specOf(request) match {
        case Left(error) => error.asLeft[ResetPlanDto].pure[F]
        case Right(spec) =>
          scopeOf(snapshots, cluster, group, request).flatMap(scope =>
            reset.plan(cluster, group, scope, spec).map(_.map(ConsumerMapping.plan))
          )
      }
    }

  /** Apply exactly the plan the token names.
    *
    * It answers with the plan that was applied so the wizard can show what happened without asking again —
    * and so that what it shows is what was written, rather than a second resolution of the same request
    * against a cluster that has moved on. The token is echoed back with an expiry of now because the document
    * is a receipt, not an offer. The signature itself stays valid until its own TTL; apply refuses it once
    * the commits it was planned against have moved, which a successful write does.
    */
  private def applyReset[F[_]: Async](
      reset: OffsetResetUseCase[F],
      secured: ConsumerApi.Securing[F]
  ): ServerEndpoint[Any, F] =
    secured.withBody(ConsumerMutationEndpoints.applyReset)((_, _, _, request) =>
      SecuredRoutes.bodyBytes(request)
    ) { principal => (_, cluster, group, request) =>
      for {
        answer <- reset.apply(principal, cluster, group, request.token)
        now <- Clock[F].realTimeInstant
      } yield answer.map(plan => ConsumerMapping.appliedPlan(plan, request.token, expiryOf(now)))
    }

  /** Delete a group outright. Refused with `KUI-GROUP-NOT-EMPTY` while it still has members. */
  private def deleteGroupRoute[F[_]](
      deleteGroup: DeleteGroupUseCase[F],
      secured: ConsumerApi.Securing[F]
  ): ServerEndpoint[Any, F] =
    secured(ConsumerMutationEndpoints.deleteGroup) { principal => (_, cluster, group) =>
      deleteGroup.delete(principal, cluster, group)
    }

  /** Delete a group's committed offsets for one topic.
    *
    * It answers with the partitions that were removed rather than with an empty body, so that "the group had
    * none" and "they were deleted" stay distinguishable.
    */
  private def deleteOffsetsRoute[F[_]: Async](
      deleteOffsets: DeleteOffsetsUseCase[F],
      secured: ConsumerApi.Securing[F]
  ): ServerEndpoint[Any, F] =
    secured(ConsumerMutationEndpoints.deleteOffsets) { principal => (_, cluster, group, topic) =>
      deleteOffsets
        .delete(principal, cluster, group, topic)
        .map(_.map(ConsumerMapping.deletedOffsets(_, group)))
    }

  // -----------------------------------------------------------------------------------------------

  /** How long an applied plan's receipt claims to be valid for. Zero, because the body is a receipt.
    *
    * This does not mark the signature spent. Apply does that by refusing a token whose bound commits no
    * longer match the group.
    */
  private val AppliedTokenTtl: FiniteDuration = scala.concurrent.duration.Duration.Zero

  private def expiryOf(now: Instant): Instant = now.plusMillis(AppliedTokenTtl.toMillis)

  /** Which partitions the reset covers.
    *
    * An empty `partitions` means "every partition of this topic the group holds an offset for", which the
    * contract documents as the ordinary case — and which nothing below this layer expands, because the port
    * takes an explicit set. Expanding it here rather than letting an empty set through is the difference
    * between the common request working and it being refused with "the reset names no partitions", which is
    * what it did the first time it was called against a real cluster.
    *
    * The set comes from the last snapshot pass, so it is the partitions the group was *known* to hold an
    * offset for up to thirty seconds ago. That staleness is visible rather than hidden: the plan that comes
    * back names every partition that would be written, so an operator who expected a twelfth partition can
    * see that only eleven are listed and re-plan. Resolving it live would mean a describe on the request path
    * for a number the plan is about to re-read anyway.
    */
  private def scopeOf[F[_]: Async](
      snapshots: GroupSnapshots[F],
      cluster: kui.kernel.ClusterId,
      group: kui.kernel.GroupId,
      request: ResetPlanRequest
  ): F[ResetScope] =
    if request.partitions.nonEmpty then
      ResetScope(
        request.topic,
        request.partitions.map(p => TopicPartition(request.topic, PartitionId.unsafe(p))).toSet
      ).pure[F]
    else
      snapshots
        .of(cluster)
        .flatMap {
          case None => Set.empty[TopicPartition].pure[F]
          case Some(cell) =>
            cell.get.map(
              _.value
                .flatMap(_.groups.get(group))
                .toList
                .flatMap(_.subscriptions.filter(_.topic == request.topic))
                .flatMap(_.partitions.map(state => TopicPartition(request.topic, state.partition)))
                .toSet
            )
        }
        .map(ResetScope(request.topic, _))

  /** The wire request as the domain's specification.
    *
    * The mode-specific parameter has already been checked at decode time by `ResetPlanRequest`'s own decoder
    * — `TIMESTAMP` with no timestamp never reaches here — so the `Left` below is unreachable for a request
    * that arrived over HTTP. It is written out anyway rather than thrown, because "unreachable" is a property
    * of today's decoder and a `throw` here would turn a future decoder change into a 500.
    */
  private def specOf(request: ResetPlanRequest): Either[KuiError, ResetSpec] = {
    import kui.kernel.group.ResetTarget

    def missing(field: String): Either[KuiError, ResetSpec] =
      Left(
        ApplicationError.Invalid(
          s"target '${request.target.wire}' requires '$field'; without it there is nothing to reset to",
          Nil
        )
      )

    request.target match {
      case ResetTarget.Earliest => Right(ResetSpec.ToEarliest)
      case ResetTarget.Latest => Right(ResetSpec.ToLatest)
      case ResetTarget.Timestamp =>
        request.timestamp.fold(missing("timestamp"))(at => Right(ResetSpec.ToTimestamp(at)))
      case ResetTarget.ShiftBy => request.shiftBy.fold(missing("shiftBy"))(by => Right(ResetSpec.ShiftBy(by)))
      case ResetTarget.Duration =>
        request.durationMs.fold(missing("durationMs"))(ms =>
          Right(ResetSpec.ByDuration(FiniteDuration(ms, java.util.concurrent.TimeUnit.MILLISECONDS)))
        )
      case ResetTarget.Offset =>
        request.offsets.filter(_.nonEmpty) match {
          case None => missing("offsets")
          case Some(offsets) => offsetsOf(request.topic, offsets)
        }
    }
  }

  /** `{"0": 412}` as `{TopicPartition -> Offset}`.
    *
    * A key that is not a partition number is a validation error and not a silently dropped entry: an operator
    * who typed `"partition-0"` must be told, rather than have that partition quietly left where it was.
    */
  private def offsetsOf(topic: TopicName, raw: Map[String, Long]): Either[KuiError, ResetSpec] =
    raw.toList
      .traverse { (key, value) =>
        key.toIntOption.flatMap(p => PartitionId.from(p).toOption) match {
          case None =>
            Left(
              ApplicationError
                .Invalid(s"'$key' is not a partition number", Nil): KuiError
            )
          case Some(partition) =>
            Offset.from(value) match {
              case Right(offset) => Right(TopicPartition(topic, partition) -> offset)
              case Left(_) =>
                Left(
                  ApplicationError.Invalid(s"$value is not a valid offset for partition $key", Nil): KuiError
                )
            }
        }
      }
      .map(pairs => ResetSpec.ToOffsets(pairs.toMap))
}
