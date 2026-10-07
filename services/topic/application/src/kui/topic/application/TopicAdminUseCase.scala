package kui.topic.application

import java.time.Instant

import cats.data.EitherT
import cats.effect.kernel.Temporal
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger

import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.kernel.{ClusterId, TopicName}
import kui.security.Principal
import kui.topic.domain.*

/** A plan, and the token that authorises applying exactly it (ADR-045). */
final case class Planned[A](plan: A, token: String, expiresAt: Instant)

/** Topic administration: create, configure, grow, delete.
  *
  * ==Two shapes, and which operation gets which==
  *
  * [[create]] and [[alterConfig]] are one call each. Their effect is a function of their request — the
  * partitions, the replication factor and the settings are the numbers the operator typed — so ADR-045 §4
  * gives them nothing to preview that the form does not already say. They still go through [[MutationGuard]],
  * so they are still refused on a read-only cluster before a Kafka client is touched, and still audited.
  *
  * [[planPartitions]]/[[applyPartitions]] and [[planDelete]]/[[applyDelete]] are two calls each, and the
  * second takes **only** a token. Their effect is not a function of their request:
  *
  *   - a partition increase depends on how many partitions there are *now*, and it silently rewrites
  *     key-to-partition routing for every record produced from then on;
  *   - a delete depends on how many records are about to be lost and on whether the cluster's
  *     `auto.create.topics.enable` will bring the topic straight back.
  *
  * Neither of those is readable off the request, and only the server can compute them. There is no method
  * here that takes a target partition count or a topic name and destroys something in one hop.
  *
  * ==Why the plan phase also refuses a read-only cluster==
  *
  * Because a wizard that happily renders a plan the operator is not allowed to apply teaches them that the
  * refusal at the end is a bug. The honest moment to say "not on this cluster" is before they compose the
  * change. The plan is not a mutation and writes no audit record of its own; it borrows the same profile
  * lookup that the guard will use a moment later.
  */
trait TopicAdminUseCase[F[_]] {

  /** Create a topic and answer with what the cluster now reports it to be.
    *
    * Every mutating method here takes the `principal` the route verified, so that the audit record names the
    * person who asked rather than a constant chosen at wiring time. The planning methods do not: a plan
    * changes nothing and writes no audit record.
    */
  def create(principal: Principal, cluster: ClusterId, spec: NewTopicSpec): F[Either[KuiError, CreatedTopic]]

  /** Set and remove entries of a topic's dynamic configuration, and answer with the configuration as it
    * stands afterwards — read back rather than echoed, so a value the broker normalised (`604800000` for
    * `7d`) is the value the screen shows.
    */
  def alterConfig(
      principal: Principal,
      cluster: ClusterId,
      topic: TopicName,
      change: TopicConfigChange
  ): F[Either[KuiError, TopicConfigView]]

  /** What raising the partition count to `target` would do. Changes nothing. */
  def planPartitions(
      cluster: ClusterId,
      topic: TopicName,
      target: Int
  ): F[Either[KuiError, Planned[PartitionPlan]]]

  /** Raise the partition count to exactly what the token names. */
  def applyPartitions(
      principal: Principal,
      cluster: ClusterId,
      topic: TopicName,
      token: String
  ): F[Either[KuiError, PartitionPlan]]

  /** What deleting this topic would destroy, and whether it would come straight back. Changes nothing. */
  def planDelete(cluster: ClusterId, topic: TopicName): F[Either[KuiError, Planned[DeletionPlan]]]

  /** Delete exactly the topic the token names. */
  def applyDelete(
      principal: Principal,
      cluster: ClusterId,
      topic: TopicName,
      token: String
  ): F[Either[KuiError, DeletionPlan]]
}

object TopicAdminUseCase {

  /** The detail a delete token signs when the record count could not be read.
    *
    * A token has to sign *something* for the count, and the count is genuinely unknown. Signing the word
    * rather than a zero is what stops "unknown" and "empty" from becoming the same token — and from the
    * receipt claiming that an unreadable topic held no records.
    */
  val UnknownRecords: String = "unknown"

  def make[F[_]: Temporal](
      admin: TopicAdmin[F],
      writer: TopicWriter[F],
      profiles: ClusterProfiles[F],
      guard: MutationGuard[F],
      tokens: TopicPlanToken[F],
      logger: StructuredLogger[F],
      /** How a `TopicError` reaches the wire. Passed in rather than imported, for the reason
        * [[MutationGuard.make]] gives: `TopicErrors` is the product's single error-to-status table and it
        * lives in the api module, which rule A3 forbids this layer from seeing. Taking it as a parameter is
        * what keeps there being one table rather than a second copy here that can disagree with it.
        */
      toKui: TopicError => KuiError
  ): TopicAdminUseCase[F] =
    new TopicAdminUseCase[F] {

      private def context(cluster: ClusterId, topic: TopicName): Map[String, String] =
        Map(
          "service.name" -> MutationGuard.ServiceName,
          "cluster.id" -> cluster.value,
          "topic" -> topic.value
        )

      // ------------------------------------------------------------------------------------- create

      def create(
          principal: Principal,
          cluster: ClusterId,
          spec: NewTopicSpec
      ): F[Either[KuiError, CreatedTopic]] =
        guard
          .guard(
            principal,
            cluster,
            TopicMutation.Create,
            spec.name.value,
            Map(
              "partitions" -> spec.partitions.fold("broker default")(_.toString),
              "replicationFactor" -> spec.replicationFactor.fold("broker default")(_.toString),
              // The keys and not the values: a configuration value can be a password on a topic that
              // carries one, and an audit log is routinely more widely readable than the thing it
              // describes (ADR-023).
              "config" -> spec.config.keys.toList.sorted.mkString(",")
            )
          )(writer.create(cluster, spec))
          .flatMap {
            case Left(error) => error.asLeft[CreatedTopic].pure[F]
            case Right(_) =>
              // Read it back, so the receipt reports what the *broker* made rather than what was asked
              // for. A topic created with `partitions` absent inherits `num.partitions`, and the operator
              // has no other way to learn what that turned out to be. A failure here costs the two
              // numbers and not the create, which has already happened and must not be reported as
              // failed.
              admin
                .detail(cluster, spec.name)
                .map {
                  case Right(detail) =>
                    CreatedTopic(
                      cluster,
                      spec.name,
                      Some(detail.summary.partitionCount),
                      detail.summary.replicationFactor
                    )
                  case Left(_) => CreatedTopic(cluster, spec.name, spec.partitions, None)
                }
                .map(_.asRight[KuiError])
          }

      // -------------------------------------------------------------------------------- alter config

      def alterConfig(
          principal: Principal,
          cluster: ClusterId,
          topic: TopicName,
          change: TopicConfigChange
      ): F[Either[KuiError, TopicConfigView]] =
        guard
          .guard(
            principal,
            cluster,
            TopicMutation.AlterConfig,
            topic.value,
            Map(
              "set" -> change.set.keys.toList.sorted.mkString(","),
              "remove" -> change.remove.toList.sorted.mkString(",")
            )
          )(writer.alterConfig(cluster, topic, change))
          .flatMap {
            case Left(error) => error.asLeft[TopicConfigView].pure[F]
            case Right(_) =>
              admin.config(cluster, topic).map {
                case Right(view) => view.asRight[KuiError]
                // The change was made. Failing the response now would tell the operator their edit did
                // not happen, which is false and would have them make it a second time.
                case Left(_) => TopicConfigView.Entries(Nil).asRight[KuiError]
              }
          }

      // ---------------------------------------------------------------------------------- partitions

      def planPartitions(
          cluster: ClusterId,
          topic: TopicName,
          target: Int
      ): F[Either[KuiError, Planned[PartitionPlan]]] =
        (for {
          _ <- EitherT(writable(cluster, TopicMutation.IncreasePartitions))
          detail <- EitherT(admin.detail(cluster, topic)).leftMap(toKui)
          now <- EitherT.liftF(Temporal[F].realTimeInstant)
          plan <- EitherT.fromEither[F](
            PartitionPlan.of(topic, detail.summary.partitionCount, target, now)
          )
          expiresAt = now.plus(TopicPlanToken.Ttl)
          token <- EitherT.liftF(
            tokens.mint(cluster, topic, TopicMutation.IncreasePartitions, plan.target.toString, expiresAt)
          )
          _ <- EitherT.liftF(
            logger
              .info(context(cluster, topic))(
                s"planned a partition increase from ${plan.current} to ${plan.target}"
              )
          )
        } yield Planned(plan, token, expiresAt)).value

      def applyPartitions(
          principal: Principal,
          cluster: ClusterId,
          topic: TopicName,
          token: String
      ): F[Either[KuiError, PartitionPlan]] =
        (for {
          now <- EitherT.liftF(Temporal[F].realTimeInstant)
          signed <- EitherT(tokens.verify(cluster, topic, TopicMutation.IncreasePartitions, token, now))
          target <- EitherT.fromEither[F](
            signed.toIntOption.toRight[KuiError](
              ApplicationError.Invalid("this confirmation does not name a partition count", Nil)
            )
          )
          // Re-planned against the cluster as it is *now*, not as it was when the plan was rendered.
          // The count can have moved in the five minutes the token is valid for, and a token that named
          // twelve must not be applied to a topic that has since been grown to sixteen: the re-plan
          // refuses that, because `PartitionPlan.of` requires a strict increase.
          detail <- EitherT(admin.detail(cluster, topic)).leftMap(toKui)
          plan <- EitherT.fromEither[F](
            PartitionPlan.of(topic, detail.summary.partitionCount, target, now)
          )
          _ <- EitherT(
            guard.guard(
              principal,
              cluster,
              TopicMutation.IncreasePartitions,
              topic.value,
              Map("from" -> plan.current.toString, "to" -> plan.target.toString)
            )(writer.increasePartitions(cluster, topic, plan.target))
          )
        } yield plan).value

      // -------------------------------------------------------------------------------------- delete

      def planDelete(cluster: ClusterId, topic: TopicName): F[Either[KuiError, Planned[DeletionPlan]]] =
        (for {
          _ <- EitherT(writable(cluster, TopicMutation.Delete))
          detail <- EitherT(admin.detail(cluster, topic)).leftMap(toKui)
          autoCreate <- EitherT.liftF(writer.autoCreateEnabled(cluster))
          now <- EitherT.liftF(Temporal[F].realTimeInstant)
          plan = DeletionPlan.of(
            topic,
            detail.summary.partitionCount,
            detail.summary.messageCount,
            autoCreate,
            now
          )
          expiresAt = now.plus(TopicPlanToken.Ttl)
          preconditions <- EitherT.fromEither[F](deletePreconditions(detail, plan))
          token <- EitherT.liftF(
            tokens.mint(cluster, topic, TopicMutation.Delete, preconditions, expiresAt)
          )
          _ <- EitherT.liftF(
            logger
              .info(context(cluster, topic))(
                s"planned a delete of ${plan.partitions} partition(s), ${plan.warnings.size} warning(s)"
              )
          )
        } yield Planned(plan, token, expiresAt)).value

      def applyDelete(
          principal: Principal,
          cluster: ClusterId,
          topic: TopicName,
          token: String
      ): F[Either[KuiError, DeletionPlan]] =
        (for {
          now <- EitherT.liftF(Temporal[F].realTimeInstant)
          signed <- EitherT(tokens.verify(cluster, topic, TopicMutation.Delete, token, now))
          detail <- EitherT(admin.detail(cluster, topic)).leftMap(toKui)
          autoCreate <- EitherT.liftF(writer.autoCreateEnabled(cluster))
          plan = DeletionPlan.of(
            topic,
            detail.summary.partitionCount,
            detail.summary.messageCount,
            autoCreate,
            now
          )
          preconditions <- EitherT.fromEither[F](deletePreconditions(detail, plan))
          _ <- EitherT.cond[F](
            preconditions == signed,
            (),
            ApplicationError.InvalidState(
              "topic deletion preconditions changed; preview the deletion again"
            ): KuiError
          )
          _ <- EitherT(
            guard.guard(
              principal,
              cluster,
              TopicMutation.Delete,
              topic.value,
              Map(
                "partitions" -> plan.partitions.toString,
                "records" -> plan.records.fold(UnknownRecords)(_.toString),
                "autoCreateEnabled" -> plan.autoCreateEnabled.fold("unknown")(_.toString)
              )
            )(writer.deleteById(cluster, topic, detail.topicId.getOrElse("")))
          )
        } yield plan).value

      // ------------------------------------------------------------------------------------ plumbing

      /** `Right(())` when this cluster may be changed at all.
        *
        * The same refusal the guard makes, made earlier, so that a plan is never rendered for a change that
        * cannot be applied. It is not a second copy of the *policy* — both read `ClusterRef.readOnly` — and
        * the guard still makes the decision that matters, immediately before the write.
        */
      private def writable(cluster: ClusterId, kind: TopicMutation): F[Either[KuiError, Unit]] =
        profiles.get(cluster).map {
          case None =>
            ApplicationError
              .NotFound("cluster", cluster.value, ErrorCode.ClusterNotFound)
              .asLeft[Unit]
          case Some(profile) if profile.readOnly =>
            ApplicationError
              .Refused(
                ErrorCode.ReadOnly,
                s"cluster ${profile.name} is configured read-only, so ${kind.operation} is not accepted"
              )
              .asLeft[Unit]
          case Some(_) => ().asRight[KuiError]
        }
    }

  /** Bind identity and every retained range, not merely a count that retention can leave unchanged. Old
    * name-only tokens deliberately cannot match this versioned precondition string.
    */
  private def deletePreconditions(detail: TopicDetail, plan: DeletionPlan): Either[KuiError, String] =
    for {
      id <- detail.topicId
        .filter(_.nonEmpty)
        .toRight[KuiError](
          ApplicationError.InvalidState("topic identity is unavailable; deletion cannot be safely confirmed")
        )
      ranges <- detail.partitions.sortBy(_.partition.value).traverse { partition =>
        for {
          begin <- partition.earliestOffset.toRight[KuiError](unknownBounds)
          end <- partition.latestOffset.toRight[KuiError](unknownBounds)
        } yield s"${partition.partition.value}:$begin:$end"
      }
      _ <- Either.cond(
        detail.partitions.nonEmpty && detail.partitions.size == plan.partitions,
        (),
        unknownBounds
      )
    } yield List(
      "delete-v2",
      id,
      plan.partitions.toString,
      plan.records.fold(UnknownRecords)(_.toString),
      plan.autoCreateEnabled.fold(UnknownRecords)(_.toString),
      ranges.mkString(",")
    ).mkString(";")

  private val unknownBounds: KuiError =
    ApplicationError.InvalidState("topic offset bounds are unavailable; preview the deletion again")
}
