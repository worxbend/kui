package kui.message.contract

import sttp.tapir.*
import sttp.tapir.json.circe.jsonBody

import kui.contracts.KernelSchemas.given
import kui.contracts.rbac.{EndpointAuthorization, ResourceRequirement}
import kui.contracts.{ErrorEnvelope, KuiEndpoint}
import kui.kernel.{ClusterId, TopicName}
import kui.security.SignedPrincipal
import kui.security.rbac.{Action, Resource}

/** Everything this service does that changes a topic: publishing a record, copying a range of records into
  * another one, and emptying one.
  *
  * ==Why they are here and the browse endpoint is not==
  *
  * `MessageEndpoints`, next door, is JVM-only: its response is an event stream, which needs `fs2` and a
  * server-side stream body, and neither links for the browser. These two are ordinary request/response calls
  * with JSON on both sides, so they live in the cross-compiled half — which means the browser's produce form
  * and the gateway's proxy route are both derived from the same value the service serves, rather than from
  * three people's separate ideas of the path. That divergence is the defect this project has shipped before.
  *
  * ==The marker (ADR-047)==
  *
  * Both carry `KuiEndpoint.MutationKey`, so the per-cluster read-only refusal and M5's enumeration have
  * something to key on that is not a naming convention, and both carry the CSRF header from the day they
  * exist rather than from the day there is a session to bind it to — a header added later has to be added to
  * every client that already shipped.
  *
  * ==Which of them carry a plan token, and why (ADR-045)==
  *
  * ADR-045 requires a plan for a destructive operation, and asks a specific question: *what exactly will this
  * do to what is already there?* Publishing and resending have no answer to that question, because they take
  * nothing away. They append. An operator can see the whole of what a produce will do by reading the form
  * they filled in, and the whole of what a resend will do from the range and the destination — and the server
  * answers both with what actually landed, offset by offset, which is a receipt rather than an offer.
  *
  * A purge is the operation on this service that *is* destructive, and it carries a plan. What it destroys is
  * not in its request at all — it is however many records the topic happens to hold when the broker is asked,
  * a number that moves while the operator is deciding and that cannot be recovered afterwards. So `planPurge`
  * resolves the offsets and changes nothing, and `purge` takes only the token that plan was signed with.
  * There is no request in this contract that empties a topic in one hop.
  */
object MessageMutationEndpoints {

  export BrowseAddress.{ClustersSegment, TopicsSegment, MessagesSegment, ClusterIdParam, TopicNameParam}

  /** The last path segment of a resend. The verb is in the path because a resend is not "create a message
    * here" — it names records that already exist somewhere else, and a bare `POST` to the destination's
    * message collection could not say so.
    */
  val ResendSegment: String = "resend"

  /** The body field that names the topic a resend copies into, which is what its produce permission is
    * checked over.
    */
  val ResendDestinationField: String = "toTopic"

  /** The last path segment of a purge, and of the plan that precedes it. A verb in the path for the same
    * reason `resend` is one: `DELETE …/messages` would read as "delete these messages" and name no particular
    * ones, while a purge is one operation over the whole topic with a plan attached.
    */
  val PurgeSegment: String = "purge"
  val PlanSegment: String = "plan"

  /** The operation names, as they appear in the audit record and in the endpoint's marker.
    *
    * `val`s rather than literals at the use site because the same strings are
    * `MutationKind.Produce.operation` and `MutationKind.Resend.operation` in `libs/security-core`, and the
    * api module's suite — the one place that can see both a contract and an application type — asserts the
    * two sets are equal.
    */
  val ProduceOperation: String = "produce"
  val ResendOperation: String = "resend"
  val PurgeOperation: String = "purge"

  private val base = "internal" / "v1" / ClustersSegment

  private val clusterIdPath: EndpointInput[ClusterId] =
    path[ClusterId](ClusterIdParam).description("The configured cluster's slug id")

  private val topicNamePath: EndpointInput[TopicName] =
    path[TopicName](TopicNameParam).description("The topic's name, as Kafka spells it")

  private def messagesOf: EndpointInput[(ClusterId, TopicName)] =
    base / clusterIdPath / TopicsSegment / topicNamePath / MessagesSegment

  /** Publish a record — or `count` copies of it — to a topic.
    *
    * A `POST` to the topic's message collection, which is what it is: a new record appended to a log. The
    * answer is where every copy landed, so the caller can link straight to the record it just wrote.
    */
  val produce: Endpoint[
    SignedPrincipal,
    (String, ClusterId, TopicName, ProduceRequestDto),
    ErrorEnvelope,
    ProduceResultDto,
    Any
  ] =
    KuiEndpoint
      .mutation(ProduceOperation, destructive = false)
      .post
      .in(messagesOf)
      .in(jsonBody[ProduceRequestDto])
      .out(jsonBody[ProduceResultDto])
      .name("message.produce")
      .attribute(
        EndpointAuthorization.Key,
        EndpointAuthorization.one(
          "produce",
          ResourceRequirement.named(Resource.Topic, TopicNameParam, Action.TopicMessagesProduce)
        )
      )
      .summary("Publish a record to a topic")
      .description(
        KuiEndpoint.mutationNote(ProduceOperation, destructive = false) +
          "The key and the value are text, turned into bytes by the named serde — the same serde the " +
          "browser would read them back with. An absent value is a tombstone and not an empty one. " +
          "The answer names the partition, offset and timestamp the broker assigned to every record " +
          "written; a shorter list than the requested count means the batch failed part-way and what " +
          "landed stayed landed. Refused with KUI-READ-ONLY on a read-only cluster, before a producer " +
          "is opened."
      )
      .tag("message")

  /** Copy a range of records into another topic, byte for byte.
    *
    * `destructive = false` for the same reason as `produce`: it appends to the destination and does not touch
    * the source. What it *can* do is append a great deal, which is why the service caps the range and says
    * the number in the refusal rather than starting a copy nobody can stop.
    */
  val resend: Endpoint[
    SignedPrincipal,
    (String, ClusterId, TopicName, ResendRequestDto),
    ErrorEnvelope,
    ResendResultDto,
    Any
  ] =
    KuiEndpoint
      .mutation(ResendOperation, destructive = false)
      .post
      .in(messagesOf / ResendSegment)
      .in(jsonBody[ResendRequestDto])
      .out(jsonBody[ResendResultDto])
      .name("message.resend")
      .attribute(
        EndpointAuthorization.Key,
        EndpointAuthorization(
          "resend",
          List(
            ResourceRequirement.named(Resource.Topic, TopicNameParam, Action.TopicMessagesRead),
            ResourceRequirement.inBody(Resource.Topic, ResendDestinationField, Action.TopicMessagesProduce)
          )
        )
      )
      .summary("Copy a range of records into another topic")
      .description(
        KuiEndpoint.mutationNote(ResendOperation, destructive = false) +
          "The records are re-written byte for byte, headers included, and are never deserialized — so a " +
          "topic KUI cannot decode can still be replayed, which is the case a resend is most often needed " +
          "for. It is not atomic: cancelled or failed halfway it leaves what it already wrote, and the " +
          "answer reports how many records were read and how many were written, which differ whenever " +
          "retention removed part of the source under the copy."
      )
      .tag("message")

  /** What emptying this topic would destroy. Changes nothing.
    *
    * Marked `destructive = false` and it genuinely is not a mutation: it reads two offsets per partition. It
    * carries the marker anyway because it refuses on a read-only cluster, which is deliberate — a screen that
    * renders a plan the operator is not allowed to apply teaches them that the refusal at the end is a bug.
    */
  val planPurge: Endpoint[
    SignedPrincipal,
    (String, ClusterId, TopicName),
    ErrorEnvelope,
    PurgePlanDto,
    Any
  ] =
    KuiEndpoint
      .mutation(PurgeOperation, destructive = false)
      .post
      .in(messagesOf / PurgeSegment / PlanSegment)
      .out(jsonBody[PurgePlanDto])
      .name("message.purge.plan")
      .attribute(
        EndpointAuthorization.Key,
        EndpointAuthorization
          .one("purge", ResourceRequirement.named(Resource.Topic, TopicNameParam, Action.TopicMessagesDelete))
      )
      .summary("What emptying this topic would destroy")
      .description(
        KuiEndpoint.mutationNote(PurgeOperation, destructive = false) +
          "Reads each partition's current start and end offset and answers with how many records would go, " +
          "a warning per thing worth knowing — that committed consumer offsets are not moved, and that a " +
          "compacted topic will very likely have the purge refused by the broker — and a token valid for " +
          "five minutes. Changes nothing."
      )
      .tag("message")

  /** Empty the topic up to exactly the offsets a plan token names.
    *
    * The destructive operation ADR-045 was written for. Kafka's `deleteRecords` moves each partition's low
    * watermark forward and the records below it are gone: no tombstone, no copy and no undo. Taking only a
    * token is what makes the deleted range the range the operator read rather than whatever the topic held by
    * the time they pressed the button.
    */
  val purge: Endpoint[
    SignedPrincipal,
    (String, ClusterId, TopicName, PurgeConfirmRequest),
    ErrorEnvelope,
    PurgeReceiptDto,
    Any
  ] =
    KuiEndpoint
      .mutation(PurgeOperation, destructive = true)
      .post
      .in(messagesOf / PurgeSegment)
      .in(jsonBody[PurgeConfirmRequest])
      .out(jsonBody[PurgeReceiptDto])
      .name("message.purge")
      .attribute(
        EndpointAuthorization.Key,
        EndpointAuthorization
          .one("purge", ResourceRequirement.named(Resource.Topic, TopicNameParam, Action.TopicMessagesDelete))
      )
      .summary("Delete every record a purge plan named")
      .description(
        KuiEndpoint.mutationNote(PurgeOperation, destructive = true) +
          "Takes only a plan token. It deletes up to the offsets the plan resolved and not to the topic's " +
          "end as it stands now, so records produced between the plan and the confirmation survive — they " +
          "are not what the operator agreed to lose. The topic, its configuration and its partition count " +
          "are untouched; this empties the log rather than recreating the topic. The answer carries both " +
          "the plan that was applied and what the broker reported per partition, because after a purge the " +
          "number of records destroyed cannot be read off the cluster at all."
      )
      .tag("message")

  /** Every mutating endpoint this service serves, plus the plan phase that reads.
    *
    * `planPurge` is in this list and deliberately not in `destructive`: it belongs with these because it is
    * part of the same flow and shares their read-only refusal, and it is not destructive because it writes
    * nothing.
    */
  val all: List[AnyEndpoint] = List(produce, resend, planPurge, purge)

  /** The one endpoint here that cannot be undone, read from the marker rather than listed by hand. */
  val destructive: List[AnyEndpoint] =
    all.filter(endpoint => endpoint.attribute(KuiEndpoint.MutationKey).exists(_.destructive))
}
