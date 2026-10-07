package kui.message.application.purge

import java.time.Instant

import cats.data.EitherT
import cats.effect.kernel.Temporal
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger

import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.kernel.{ClusterId, TopicName}
import kui.message.application.produce.MutationGuard
import kui.message.domain.ports.{ClusterProfileSource, RecordDeleter}
import kui.message.domain.{PurgePlan, PurgeResult}
import kui.security.Principal
import kui.security.audit.MutationKind

/** A plan, and the token that authorises applying exactly it (ADR-045).
  *
  * Named `PurgeOffer` and not `PlannedPurge` because the domain already has a `PlannedPurge`, which is one
  * *partition* of a plan. Two types with one name in two layers is the drift the layering rules exist to
  * prevent, and here the compiler would only have caught it at the import.
  */
final case class PurgeOffer(plan: PurgePlan, token: String, expiresAt: Instant)

/** Emptying a topic (`MS-008`), in two phases.
  *
  * ==Why this operation, of all of them, is the one ADR-045 was written for==
  *
  * `POST …/purge` says nothing about what it destroys. What it destroys is however many records the topic
  * happens to be holding when the broker is asked, which only the broker knows, which changes while the
  * operator is deciding, and which cannot be recovered afterwards by any means: `deleteRecords` moves a log's
  * low watermark and everything below it is gone. So the first call resolves the numbers and changes nothing,
  * the operator confirms those numbers, and the second call deletes **exactly** up to the offsets the first
  * one named — not a recomputation, which by then would include records that arrived after the operator read
  * the figure and agreed to lose it.
  *
  * ==What it deliberately is not==
  *
  * It is not delete-and-recreate. The reference product empties a topic that way, retrying the create while
  * the deletion is still in flight. That throws away the topic's identity, leaves every consumer group's
  * committed offsets pointed at a log that no longer exists, and races automatic topic creation. This empties
  * the log and leaves the topic, its configuration and its partition count untouched, which is what an
  * operator asking to purge a topic means.
  */
trait PurgeUseCase[F[_]] {

  /** What emptying this topic would destroy. Reads; changes nothing. */
  def plan(cluster: ClusterId, topic: TopicName): F[Either[KuiError, PurgeOffer]]

  /** Delete exactly the records the token names, and answer with what the broker reported per partition.
    *
    * @param principal
    *   who is emptying the topic, so the audit record names them. Verified by the route.
    */
  def apply(
      principal: Principal,
      cluster: ClusterId,
      topic: TopicName,
      token: String
  ): F[Either[KuiError, (PurgePlan, PurgeResult)]]
}

object PurgeUseCase {

  def make[F[_]: Temporal](
      deleter: RecordDeleter[F],
      profiles: ClusterProfileSource[F],
      guard: MutationGuard[F],
      tokens: PurgeToken[F],
      logger: StructuredLogger[F]
  ): PurgeUseCase[F] =
    new PurgeUseCase[F] {

      def plan(cluster: ClusterId, topic: TopicName): F[Either[KuiError, PurgeOffer]] =
        (for {
          // The read-only refusal, made here as well as in the guard, so that a screen never renders a
          // plan the operator is not allowed to apply. It is not a second copy of the policy — both
          // read the same profile flag — and the guard still makes the decision that matters, in the
          // breath before the write.
          _ <- EitherT(writable(cluster))
          identity <- EitherT(deleter.topicId(cluster, topic))
          partitions <- EitherT(deleter.watermarks(cluster, topic))
          currentIdentity <- EitherT(deleter.topicId(cluster, topic))
          _ <- EitherT.fromEither[F](
            Either.cond(
              identity.nonEmpty && identity == currentIdentity,
              (),
              ApplicationError
                .Refused(ErrorCode.InvalidState, "the topic changed while planning; plan again"): KuiError
            )
          )
          policy <- EitherT.liftF(deleter.cleanupPolicy(cluster, topic))
          now <- EitherT.liftF(Temporal[F].realTimeInstant)
          plan = PurgePlan.of(topic, partitions, policy, now)
          expiresAt = now.plus(PurgeToken.Ttl)
          token <- EitherT.liftF(
            tokens.mint(cluster, topic, plan.partitions.filterNot(_.isEmpty), expiresAt, identity)
          )
          _ <- EitherT.liftF(
            logger
              .info(context(cluster, topic))(
                s"planned a purge of ${plan.records} record(s) across ${plan.deletions.size} partition(s)"
              )
          )
        } yield PurgeOffer(plan, token, expiresAt)).value

      def apply(
          principal: Principal,
          cluster: ClusterId,
          topic: TopicName,
          token: String
      ): F[Either[KuiError, (PurgePlan, PurgeResult)]] =
        (for {
          now <- EitherT.liftF(Temporal[F].realTimeInstant)
          identity <- EitherT(deleter.topicId(cluster, topic))
          planned <- EitherT(tokens.verify(cluster, topic, token, now, identity))
          plan = PurgePlan.of(topic, planned, None, now)
          _ <- EitherT.fromEither[F](
            // A token over no partitions is a plan for a topic that was already empty. Applying it would
            // ask Kafka to delete nothing, which is a call that can still fail and can never help.
            Either.cond(
              planned.nonEmpty,
              (),
              ApplicationError.Refused(
                ErrorCode.InvalidState,
                "that purge was planned against a topic with no records in it, so there is nothing to delete"
              ): KuiError
            )
          )
          result <- EitherT(
            guard.guard(
              principal = principal,
              cluster = cluster,
              kind = MutationKind.Purge,
              resource = topic.value,
              // The partitions and the offsets, because "who emptied this topic, and how much went"
              // is the question this trail exists to answer. No record content: an audit log is read
              // by more people than the topic it describes (ADR-023).
              detail = Map(
                "partitions" -> planned.size.toString,
                "records" -> planned.map(_.records).sum.toString,
                "deletedBefore" -> planned
                  .sortBy(_.partition.value)
                  .map(one => s"${one.partition.value}:${one.deleteBefore.value}")
                  .mkString(",")
              )
            )((for {
              current <- EitherT(deleter.topicId(cluster, topic))
              _ <- EitherT.fromEither[F](
                Either.cond(
                  identity.nonEmpty && current == identity,
                  (),
                  ApplicationError
                    .Refused(ErrorCode.InvalidState, "the topic changed; plan the purge again"): KuiError
                )
              )
              deleted <- EitherT(deleter.deleteBefore(cluster, topic, plan.deletions))
            } yield deleted).value)
          )
        } yield (plan, result)).value

      private def context(cluster: ClusterId, topic: TopicName): Map[String, String] =
        Map(
          "service.name" -> MutationGuard.ServiceName,
          "cluster.id" -> cluster.value,
          "topic" -> topic.value
        )

      /** `Right(())` when this cluster may be changed at all. */
      private def writable(cluster: ClusterId): F[Either[KuiError, Unit]] =
        profiles.cluster(cluster).map {
          case Left(error) => error.asLeft[Unit]
          case Right(profile) if profile.readOnly =>
            ApplicationError
              .Refused(
                ErrorCode.ReadOnly,
                s"cluster ${profile.name} is configured read-only, so purge is not accepted"
              )
              .asLeft[Unit]
          case Right(_) => ().asRight[KuiError]
        }
    }
}
