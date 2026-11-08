package kui.topic.contract

import sttp.tapir.*
import sttp.tapir.json.circe.jsonBody

import kui.contracts.KernelSchemas.given
import kui.contracts.rbac.{EndpointAuthorization, ResourceRequirement}
import kui.contracts.{ErrorEnvelope, KuiEndpoint}
import kui.kernel.{ClusterId, TopicName}
import kui.security.SignedPrincipal
import kui.security.rbac.{Action, Resource}
import kui.topic.contract.dto.*

/** Everything `kui-topic-service` does that changes a Kafka cluster (M5).
  *
  * `TopicEndpoints` next door serves the reads and states plainly that nothing in it mutates. This is the
  * other file, and it arrives with the safety net rather than before it, which is the ordering
  * `docs/ROADMAP.md` §3 exists to enforce and which ADR-047 turned into a rule.
  *
  * ==Every endpoint here carries the marker and the CSRF header (ADR-047)==
  *
  * `KuiEndpoint.mutation` attaches both. The marker is machine-readable, so M5's read-only enumeration is a
  * policy over an existing classification rather than a hunt through paths, and `TopicAdminEndpointsSuite`
  * asserts that every endpoint in [[all]] carries one. The CSRF header is required from the day the endpoint
  * exists, even though there is no session yet to bind it to, because a header added later has to be added to
  * every client that already shipped.
  *
  * ==Two of the four are two calls, and which two is not a matter of taste (ADR-045)==
  *
  * ADR-045 §4: an operation needs a plan phase when its effect is not a function of its request alone.
  *
  *   - [[create]] and [[updateConfig]] are single calls. The partitions, the replication factor and the
  *     settings are the numbers the operator typed, and the server answers with what it did.
  *   - [[planPartitions]]/[[increasePartitions]] is two, because the effect depends on how many partitions
  *     the topic has *now* and because raising the count rewrites key-to-partition routing for every record
  *     produced afterwards. ADR-045 names this operation explicitly.
  *   - [[planDeletion]]/[[deleteTopic]] is two, because two things the request cannot say decide what
  *     deleting means here: how many records are about to be lost, and whether the cluster's
  *     `auto.create.topics.enable` will recreate the topic seconds later under the broker's defaults. KUI's
  *     own message browser has already been bitten by the second, so this product does not get to treat it as
  *     a footnote.
  *
  * The apply calls take a token and nothing else. A `curl` user cannot skip the preview either, which is
  * ADR-045's stated consequence and the reason the protection lives in the contract rather than in a screen.
  */
object TopicAdminEndpoints {

  export TopicEndpoints.{ClustersSegment, TopicsSegment, ConfigSegment, PartitionsSegment}
  export TopicEndpoints.{ClusterIdParam, TopicNameParam}

  /** The last segment of a plan request. The same word the consumer service's reset wizard uses, because it
    * is the same idea and an operator reading a proxy log should not have to learn two.
    */
  val PlanSegment: String = "plan"

  /** The resource a deletion plan is about. A plan is not a delete, so it cannot be a `DELETE`; and it is not
    * a sub-resource of the topic's partitions or configuration either. `…/deletion/plan` names the thing it
    * actually is: a description of a deletion that has not happened.
    */
  val DeletionSegment: String = "deletion"

  /** Where the confirmation travels on the one apply call that has no body. */
  val TokenParam: String = "token"

  /** The body field that names the topic a create makes, which is what its permission is checked over. */
  val CreateNameField: String = "name"

  /** The operation names, as they appear in the audit record and in the endpoint's marker.
    *
    * `val`s rather than literals at the use site because the same strings are
    * `TopicMutation.<case>.operation` in the application layer, and the api module's suite — the one place
    * that can see both a contract and an application type — asserts the two sets are equal. Two enums in two
    * modules with the same names is exactly the drift build rule A14 exists for, and this is the assertion
    * that catches it across a boundary the rule cannot see.
    */
  val CreateOperation: String = "topic.create"
  val AlterConfigOperation: String = "topic.config.alter"
  val IncreasePartitionsOperation: String = "topic.partitions.increase"
  val DeleteOperation: String = "topic.delete"

  private val topicsBase = "internal" / "v1" / ClustersSegment

  private val clusterIdPath: EndpointInput[ClusterId] =
    path[ClusterId](ClusterIdParam).description("The configured cluster's slug id")

  private val topicNamePath: EndpointInput[TopicName] =
    path[TopicName](TopicNameParam).description("The topic's name, as Kafka spells it")

  private def oneTopic: EndpointInput[(ClusterId, TopicName)] =
    topicsBase / clusterIdPath / TopicsSegment / topicNamePath

  /** Create a topic.
    *
    * A `POST` to the cluster's topic collection, which is what it is. The answer is the topic as the cluster
    * reports it *afterwards*, not an echo of the request: a topic created with the broker's defaults is the
    * common case and this is the operator's only way to learn what those defaults turned out to be.
    */
  val create: Endpoint[
    SignedPrincipal,
    (String, ClusterId, CreateTopicRequest),
    ErrorEnvelope,
    CreatedTopicDto,
    Any
  ] =
    KuiEndpoint
      .mutation(CreateOperation, destructive = false)
      .post
      .in(topicsBase / clusterIdPath / TopicsSegment)
      .in(jsonBody[CreateTopicRequest])
      .out(jsonBody[CreatedTopicDto])
      .name("topic.create")
      .attribute(
        EndpointAuthorization.Key,
        EndpointAuthorization
          .one(
            "topic.create",
            ResourceRequirement.inBody(Resource.Topic, CreateNameField, Action.TopicCreate)
          )
      )
      .summary("Create a topic")
      .description(
        KuiEndpoint.mutationNote(CreateOperation, destructive = false) +
          "An absent partitions or replicationFactor is passed to the broker as absent, so its own " +
          "num.partitions and default.replication.factor apply. A name the cluster already has is refused " +
          "with KUI-INVALID-STATE and 409, which is a different answer from a malformed request."
      )
      .tag("topic")

  /** Set and reset entries of a topic's configuration.
    *
    * `PATCH` and not `PUT`, because the request is a *change* and not a replacement: keys it does not name
    * are left exactly as they are. A `PUT` would promise that the body is the whole configuration, and a
    * client that sent one field would then be silently reverting every other override the topic had — which
    * is what the reference product's endpoint does.
    */
  val updateConfig: Endpoint[
    SignedPrincipal,
    (String, ClusterId, TopicName, UpdateTopicConfigRequest),
    ErrorEnvelope,
    dto.TopicConfigResponse,
    Any
  ] =
    KuiEndpoint
      .mutation(AlterConfigOperation, destructive = false)
      .patch
      .in(oneTopic / ConfigSegment)
      .in(jsonBody[UpdateTopicConfigRequest])
      .out(jsonBody[dto.TopicConfigResponse])
      .name("topic.config.update")
      .attribute(
        EndpointAuthorization.Key,
        EndpointAuthorization.one(
          "topic.config.alter",
          ResourceRequirement.named(Resource.Topic, TopicNameParam, Action.TopicEdit)
        )
      )
      .summary("Set and reset entries of a topic's configuration")
      .description(
        KuiEndpoint.mutationNote(AlterConfigOperation, destructive = false) +
          "Incremental: keys named in neither 'set' nor 'remove' are untouched. A key in 'remove' goes back " +
          "to the broker's default for it, which is not the same as setting it to that default's current " +
          "value. The response is the configuration read back afterwards, so a value the broker normalised " +
          "is the value the caller sees."
      )
      .tag("topic")

  /** What raising the partition count would do. Changes nothing.
    *
    * Marked `destructive = false`, and it genuinely is not a mutation: it reads. It nevertheless refuses on a
    * read-only cluster, which is why it carries the marker at all — a screen that renders a plan the operator
    * is not allowed to apply teaches them that the refusal at the end is a bug.
    */
  val planPartitions: Endpoint[
    SignedPrincipal,
    (String, ClusterId, TopicName, PartitionIncreaseRequest),
    ErrorEnvelope,
    PartitionPlanDto,
    Any
  ] =
    KuiEndpoint
      .mutation(IncreasePartitionsOperation, destructive = false)
      .post
      .in(oneTopic / PartitionsSegment / PlanSegment)
      .in(jsonBody[PartitionIncreaseRequest])
      .out(jsonBody[PartitionPlanDto])
      .name("topic.partitions.plan")
      .attribute(
        EndpointAuthorization.Key,
        EndpointAuthorization.one(
          "topic.partitions.increase",
          ResourceRequirement.named(Resource.Topic, TopicNameParam, Action.TopicEdit)
        )
      )
      .summary("What raising this topic's partition count would do")
      .description(
        KuiEndpoint.mutationNote(IncreasePartitionsOperation, destructive = false) +
          "Reads the current partition count and answers with the change, the warning that key-to-partition " +
          "routing is about to change for every future record, and a token valid for five minutes. A target " +
          "that is not greater than the current count is refused here rather than by the broker a screen " +
          "later. Changes nothing."
      )
      .tag("topic")

  /** Raise the partition count to exactly what the token names.
    *
    * Irreversible in a way that is easy to underestimate: Kafka has no call that removes a partition, and
    * every record produced afterwards under a key that already exists may land on a different partition from
    * the records already stored under it. That is why the operation is classified destructive even though it
    * deletes nothing.
    */
  val increasePartitions: Endpoint[
    SignedPrincipal,
    (String, ClusterId, TopicName, ConfirmRequest),
    ErrorEnvelope,
    PartitionPlanDto,
    Any
  ] =
    KuiEndpoint
      .mutation(IncreasePartitionsOperation, destructive = true)
      .post
      .in(oneTopic / PartitionsSegment)
      .in(jsonBody[ConfirmRequest])
      .out(jsonBody[PartitionPlanDto])
      .name("topic.partitions.increase")
      .attribute(
        EndpointAuthorization.Key,
        EndpointAuthorization.one(
          "topic.partitions.increase",
          ResourceRequirement.named(Resource.Topic, TopicNameParam, Action.TopicEdit)
        )
      )
      .summary("Raise a topic's partition count to what a plan token names")
      .description(
        KuiEndpoint.mutationNote(IncreasePartitionsOperation, destructive = true) +
          "Takes only a plan token. The current count is re-read immediately before the write, so a token " +
          "for twelve partitions cannot be applied to a topic somebody else has already grown to sixteen. " +
          "Kafka has no way to remove a partition afterwards, and key-to-partition routing changes for every " +
          "record produced from then on."
      )
      .tag("topic")

  /** What deleting this topic would destroy, and what would happen next. Changes nothing. */
  val planDeletion: Endpoint[
    SignedPrincipal,
    (String, ClusterId, TopicName),
    ErrorEnvelope,
    DeletionPlanDto,
    Any
  ] =
    KuiEndpoint
      .mutation(DeleteOperation, destructive = false)
      .post
      .in(oneTopic / DeletionSegment / PlanSegment)
      .out(jsonBody[DeletionPlanDto])
      .name("topic.deletion.plan")
      .attribute(
        EndpointAuthorization.Key,
        EndpointAuthorization
          .one("topic.delete", ResourceRequirement.named(Resource.Topic, TopicNameParam, Action.TopicDelete))
      )
      .summary("What deleting this topic would destroy")
      .description(
        KuiEndpoint.mutationNote(DeleteOperation, destructive = false) +
          "Answers with the partition count, the record count where every partition could be counted, " +
          "whether this cluster's auto.create.topics.enable will recreate the topic the moment anything " +
          "names it, and a token valid for five minutes. A record count of null means at least one " +
          "partition could not be counted and is never a sum over the ones that could. Changes nothing."
      )
      .tag("topic")

  /** Delete exactly the topic a plan token names.
    *
    * `DELETE` with the token as a query parameter rather than as a body. A `DELETE` with a request body is
    * unevenly supported by proxies, clients and Tapir's own interpreters, and the confirmation is short and
    * carries no secret of the caller's: it is a signed statement about a plan, useless for anything but this
    * one topic on this one cluster within five minutes.
    */
  val deleteTopic: Endpoint[
    SignedPrincipal,
    (String, ClusterId, TopicName, String),
    ErrorEnvelope,
    DeletionPlanDto,
    Any
  ] =
    KuiEndpoint
      .mutation(DeleteOperation, destructive = true)
      .delete
      .in(oneTopic)
      .in(
        query[String](TokenParam)
          .description("The token the deletion plan answered with. Nothing else is accepted")
      )
      .out(jsonBody[DeletionPlanDto])
      .name("topic.delete")
      .attribute(
        EndpointAuthorization.Key,
        EndpointAuthorization
          .one("topic.delete", ResourceRequirement.named(Resource.Topic, TopicNameParam, Action.TopicDelete))
      )
      .summary("Delete a topic")
      .description(
        KuiEndpoint.mutationNote(DeleteOperation, destructive = true) +
          "Takes only a plan token. Kafka's deleteTopics is asynchronous: it answers when the controller " +
          "has accepted the deletion, and the topic can still appear in a listing for a moment afterwards, " +
          "so a caller must not read 'still listed' as a failure. A cluster with delete.topic.enable=false " +
          "refuses, and the refusal names the setting. The response repeats the plan that was applied, so " +
          "the caller can show what was destroyed without asking about a topic that no longer exists."
      )
      .tag("topic")

  /** Every endpoint this file serves, mutations and their plan phases alike.
    *
    * The gateway derives its public routes from this list, so an endpoint left out of it is an endpoint no
    * browser can reach — which is a failure mode this project has shipped before, as a sidebar of dead links.
    */
  val all: List[AnyEndpoint] =
    List(create, updateConfig, planPartitions, increasePartitions, planDeletion, deleteTopic)

  /** The two endpoints that cannot be undone, read from the marker rather than listed by hand.
    *
    * It is deliberately *not* "the endpoints that change something": create and updateConfig change something
    * too. This is the narrower set the UI treats differently — the ones that reach a screen as a plan an
    * operator has to confirm — and it is derived from the marker so that a seventh endpoint cannot join it by
    * being named like one. Listing them by hand would be a second declaration of the same fact, and the one
    * nothing checks.
    */
  val destructive: List[AnyEndpoint] =
    all.filter(endpoint => endpoint.attribute(KuiEndpoint.MutationKey).exists(_.destructive))
}
