package kui.topic.api

import cats.effect.kernel.{Async, Clock}
import cats.syntax.all.*
import sttp.tapir.server.ServerEndpoint

import kui.contracts.Section
import kui.http.principal.SecuredRoutes
import kui.topic.application.{Planned, TopicAdminUseCase}
import kui.topic.contract.TopicAdminEndpoints
import kui.topic.contract.dto.*
import kui.topic.domain.{DeletionPlan, NewTopicSpec, PartitionPlan, TopicConfigChange}

/** The six routes that administer a topic: create it, configure it, grow it, delete it — and the two that
  * only say what growing and deleting would do.
  *
  * ==The destructive request is always the second one (ADR-045)==
  *
  * There is no route in this file that takes a partition count and grows a topic in one hop, and none that
  * takes a topic name and deletes it. The two destructive routes accept a plan token and nothing else, so the
  * change that happens is the change the operator was shown. Create and configure are single calls because
  * their effect *is* their request, which is the test ADR-045 §4 sets rather than a judgement about how
  * frightening each one feels.
  *
  * ==Read-only and audit (ADR-047)==
  *
  * Neither refusal nor recording is implemented here. `MutationGuard` in the application layer owns both, and
  * every write goes through it: it resolves the cluster's profile, refuses a read-only cluster with
  * `KUI-READ-ONLY` **without touching a Kafka client**, records the attempt either way, and asks for the
  * topic snapshot to be re-scraped on success. This module contributes nothing to that decision, which is why
  * there is no read-only check written out below — one written here would be a second copy of the rule, and
  * the copy that can disagree.
  *
  * ==The CSRF header==
  *
  * Every endpoint here carries it and none of them checks it. `KuiEndpoint.mutation` declares the header, so
  * a request without one fails to decode and never reaches this file; binding its *value* to a session is
  * M6's job, because there is no session yet to bind it to.
  */
object TopicAdminRoutes {

  def apply[F[_]: Async](
      admin: TopicAdminUseCase[F],
      secured: TopicApi.Securing[F]
  ): List[ServerEndpoint[Any, F]] =
    List(
      create(admin, secured),
      updateConfig(admin, secured),
      planPartitions(admin, secured),
      increasePartitions(admin, secured),
      planDeletion(admin, secured),
      deleteTopic(admin, secured)
    )

  /** Create a topic, and answer with what the cluster then reports it to be. */
  private def create[F[_]: Async](
      admin: TopicAdminUseCase[F],
      secured: TopicApi.Securing[F]
  ): ServerEndpoint[Any, F] =
    secured.withBody(TopicAdminEndpoints.create)(
      (_, _, request) => SecuredRoutes.bodyBytes(request),
      // The topic is named in the body, so this is the only place its name meets the caller's patterns.
      (_, _, request) => Map(TopicAdminEndpoints.CreateNameField -> List(request.name.value))
    ) { principal => (_, cluster, request) =>
      NewTopicSpec.of(request.name, request.partitions, request.replicationFactor, request.config) match {
        case Left(error) => error.asLeft[CreatedTopicDto].pure[F]
        case Right(spec) =>
          admin
            .create(principal, cluster, spec)
            .map(
              _.map(created =>
                CreatedTopicDto(
                  created.cluster,
                  created.topic,
                  created.partitions,
                  created.replicationFactor
                )
              )
            )
      }
    }

  /** Set and reset entries of a topic's configuration, and answer with the configuration read back. */
  private def updateConfig[F[_]: Async](
      admin: TopicAdminUseCase[F],
      secured: TopicApi.Securing[F]
  ): ServerEndpoint[Any, F] =
    secured.withBody(TopicAdminEndpoints.updateConfig)((_, _, _, request) =>
      SecuredRoutes.bodyBytes(request)
    ) { principal => (_, cluster, topic, request) =>
      TopicConfigChange.of(request.set, request.remove.toSet) match {
        case Left(error) => error.asLeft[TopicConfigResponse].pure[F]
        case Right(change) =>
          for {
            answer <- admin.alterConfig(principal, cluster, topic, change)
            now <- Clock[F].realTimeInstant
          } yield answer.map(view => TopicConfigResponse(Section.Ok(TopicMapping.configView(view), now)))
      }
    }

  /** What raising the partition count would do. Changes nothing. */
  private def planPartitions[F[_]: Async](
      admin: TopicAdminUseCase[F],
      secured: TopicApi.Securing[F]
  ): ServerEndpoint[Any, F] =
    secured.withBody(TopicAdminEndpoints.planPartitions)((_, _, _, request) =>
      SecuredRoutes.bodyBytes(request)
    ) { _ => (_, cluster, topic, request) =>
      admin.planPartitions(cluster, topic, request.partitions).map(_.map(offerPartitions))
    }

  /** Raise the partition count to exactly what the token names. */
  private def increasePartitions[F[_]: Async](
      admin: TopicAdminUseCase[F],
      secured: TopicApi.Securing[F]
  ): ServerEndpoint[Any, F] =
    secured.withBody(TopicAdminEndpoints.increasePartitions)((_, _, _, request) =>
      SecuredRoutes.bodyBytes(request)
    ) { principal => (_, cluster, topic, request) =>
      admin.applyPartitions(principal, cluster, topic, request.token).map(_.map(receipt))
    }

  /** What deleting this topic would destroy. Changes nothing. */
  private def planDeletion[F[_]: Async](
      admin: TopicAdminUseCase[F],
      secured: TopicApi.Securing[F]
  ): ServerEndpoint[Any, F] =
    secured(TopicAdminEndpoints.planDeletion) { _ => (_, cluster, topic) =>
      admin.planDelete(cluster, topic).map(_.map(offerDeletion))
    }

  /** Delete exactly the topic the token names. */
  private def deleteTopic[F[_]: Async](
      admin: TopicAdminUseCase[F],
      secured: TopicApi.Securing[F]
  ): ServerEndpoint[Any, F] =
    secured(TopicAdminEndpoints.deleteTopic) { principal => (_, cluster, topic, token) =>
      admin.applyDelete(principal, cluster, topic, token).map(_.map(receipt))
    }

  // -----------------------------------------------------------------------------------------------

  /** A plan that has not been applied: the numbers, plus the token and when it stops being accepted. */
  private def offerPartitions(planned: Planned[PartitionPlan]): PartitionPlanDto =
    receipt(planned.plan).copy(token = Some(planned.token), expiresAt = Some(planned.expiresAt))

  private def offerDeletion(planned: Planned[DeletionPlan]): DeletionPlanDto =
    receipt(planned.plan).copy(token = Some(planned.token), expiresAt = Some(planned.expiresAt))

  /** A plan that has been applied. No token and no expiry: it has been spent, and a document that still
    * carried one would invite a client to send it again — which the token's own single subject would not
    * refuse, because it names a change that is now already made.
    */
  private def receipt(plan: PartitionPlan): PartitionPlanDto =
    PartitionPlanDto(
      topic = plan.topic,
      current = plan.current,
      target = plan.target,
      warnings = plan.warnings.map(warning => PlanWarningDto(warning.code, warning.message)),
      token = None,
      expiresAt = None,
      computedAt = plan.computedAt
    )

  private def receipt(plan: DeletionPlan): DeletionPlanDto =
    DeletionPlanDto(
      topic = plan.topic,
      partitions = plan.partitions,
      records = plan.records,
      autoCreateEnabled = plan.autoCreateEnabled,
      warnings = plan.warnings.map(warning => PlanWarningDto(warning.code, warning.message)),
      token = None,
      expiresAt = None,
      computedAt = plan.computedAt
    )
}
