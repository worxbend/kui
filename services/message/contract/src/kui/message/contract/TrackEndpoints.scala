package kui.message.contract

import io.circe.syntax.*
import io.circe.{Codec, HCursor, Json}
import sttp.tapir.*
import sttp.tapir.json.circe.jsonBody

import kui.contracts.KernelSchemas.given
import kui.contracts.rbac.{EndpointAuthorization, ResourceRequirement}
import kui.contracts.{ErrorEnvelope, KuiEndpoint}
import kui.kernel.ClusterId
import kui.security.SignedPrincipal
import kui.security.rbac.{Action, Resource}

/** What a finished track found (ET-001).
  *
  * ==Why `scanned` is on the answer==
  *
  * Because a track that matched nothing and a track that read nothing are the same screen without it, and
  * they mean opposite things: the first says the value is not in those topics in that window, and the second
  * says the window was empty. A support engineer acts differently on each.
  *
  * ==Why `truncated` is its own field and not an inference==
  *
  * A caller cannot tell "exactly `limit` hits exist" from "the scan stopped at `limit`" by counting, and the
  * difference is whether they have the whole answer. Inferring it from `hits.size == limit` would be wrong in
  * exactly the case that matters.
  *
  * @param hits
  *   the matching records with the topic each came from, in the order they were read: topic by topic, and
  *   within a topic in time order. The screen sorts them by timestamp, which is what a person means by "in
  *   order", and it can only do that because every hit carries its own timestamp
  */
final case class TrackResultDto(hits: List[TrackHitDto], scanned: Long, matched: Long, truncated: Boolean)

object TrackResultDto {

  given Codec[TrackResultDto] = Codec.from(
    (cursor: HCursor) =>
      for {
        hits <- cursor.get[List[TrackHitDto]]("hits")
        scanned <- cursor.get[Long]("scanned")
        matched <- cursor.get[Long]("matched")
        truncated <- cursor.getOrElse[Boolean]("truncated")(false)
      } yield TrackResultDto(hits, scanned, matched, truncated),
    (dto: TrackResultDto) =>
      Json.obj(
        "hits" -> dto.hits.asJson,
        "scanned" -> dto.scanned.asJson,
        "matched" -> dto.matched.asJson,
        "truncated" -> dto.truncated.asJson
      )
  )

  given Schema[TrackResultDto] = Schema
    .derived[TrackResultDto]
    .description("What a track found, how much it read, and whether it stopped early")

  given CanEqual[TrackResultDto, TrackResultDto] = CanEqual.derived
}

/** Following one business event across several topics (ET-001, ADR-029).
  *
  * ==Why a POST, when it reads nothing and changes nothing==
  *
  * Because the request is a document. A track names a list of topics, a match with three fields and a closed
  * window; expressed as query parameters that is a URL nobody can read and several clients would encode
  * differently. It carries no mutation marker and no CSRF header, because it changes nothing — what it is, is
  * a search whose question does not fit in a query string.
  *
  * ==Why it answers all at once==
  *
  * The scan is bounded by its window, its topic list and its hit cap, so the answer has a size the service
  * decided in advance. The streamed variant — results as they are found, for a scan long enough that waiting
  * is unpleasant — is ET-002, and it wants the same second stream relay in the gateway that the browse has;
  * it is not built yet, and this endpoint is not a substitute for it on a wide window.
  */
object TrackEndpoints {

  export BrowseAddress.{ClustersSegment, MessagesSegment, ClusterIdParam}

  /** The last path segment. A verb, because a track is an operation over several topics rather than a
    * collection anybody could `GET`.
    */
  val TrackSegment: String = "track"

  /** The body field that names the topics a track reads, which is what its permission is checked over. */
  val TopicsField: String = "topics"

  private val base = "internal" / "v1" / ClustersSegment

  private val clusterIdPath: EndpointInput[ClusterId] =
    path[ClusterId](ClusterIdParam).description("The configured cluster's slug id")

  val track: Endpoint[SignedPrincipal, (ClusterId, TrackQueryDto), ErrorEnvelope, TrackResultDto, Any] =
    KuiEndpoint.internal.post
      .in(base / clusterIdPath / MessagesSegment / TrackSegment)
      .in(jsonBody[TrackQueryDto])
      .out(jsonBody[TrackResultDto])
      .name("message.track")
      // A track reads the records of every topic its body names, so it needs what a browse of each of them
      // needs. The names are in the body, which the gateway cannot decode: it only checks that the caller
      // may read messages on *some* topic, and the message service — `TrackRoutes` hands the decoded
      // `topics` to `SecuredRoutes.withBody` — refuses the whole track unless every topic is readable.
      .attribute(
        EndpointAuthorization.Key,
        EndpointAuthorization.one(
          "track",
          ResourceRequirement.inBody(Resource.Topic, TopicsField, Action.TopicMessagesRead)
        )
      )
      .summary("Find a value across several topics inside a time window")
      .description(
        "Reads each named topic forwards from the start of the window and answers with the records that " +
          "matched, each carrying the topic it came from. Every bound is mandatory: the window is closed " +
          "and ordered, its width has a ceiling, and the number of hits has a cap — an unbounded scan of a " +
          "production cluster is not a search. `scanned` is what was read, which is what tells a user " +
          "whether 'no hits' means the value is absent or the window was empty; `truncated` says the scan " +
          "stopped at its cap, which a caller cannot work out by counting. Changes nothing."
      )
      .tag("message")

  /** For the gateway's contract map and the merged document. */
  val all: List[AnyEndpoint] = List(track)
}
