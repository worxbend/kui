package kui.message.api

import cats.effect.kernel.Async
import cats.syntax.all.*
import sttp.tapir.server.ServerEndpoint

import kui.http.principal.SecuredRoutes
import kui.kernel.ClusterId
import kui.kernel.browse.PollBudget
import kui.kernel.error.KuiError
import kui.message.application.{TrackEvent, TrackUseCase}
import kui.message.contract.{TrackEndpoints, TrackHitDto, TrackMatchDto, TrackQueryDto, TrackResultDto}
import kui.message.domain.{CorrelationKey, MatchOperator, MatchSource, TrackMatch, TrackQuery}

/** `POST /internal/v1/clusters/{clusterId}/messages/track` (ET-001).
  *
  * ==Where the validation lives==
  *
  * In two places, deliberately, and they check different things. The DTO's own codec refuses a document that
  * is malformed *as a request* — a source that is not one of three words, a header search with no header name
  * — and answers with a decoding failure naming the field. `TrackQuery.of` refuses a request that is
  * well-formed and not a legal scan: an unordered window, one wider than the deployment allows, a pattern
  * that will not compile. Neither could do the other's job: the codec has no configuration and the domain
  * never sees a `Json`.
  *
  * ==Why the stream is collected here==
  *
  * The use case is a stream because a scan produces its hits over time and one of them may be the last thing
  * that happens before a budget runs out. This endpoint answers all at once, so it folds that stream into the
  * counts and the hits the caller asked for — the terminal `Finished` event carries the numbers, so nothing
  * is counted twice. The streamed variant (ET-002) binds the same use case to an event stream and needs a
  * relay in the gateway; it is not built.
  */
object TrackRoutes {

  /** What one track may consume before it gives up.
    *
    * The browse's budget, deliberately: a track *is* a browse, several times over, and giving it a budget of
    * its own would be a second number an operator has to discover the day a scan stops early.
    */
  val DefaultBudget: PollBudget = MessageRoutes.DefaultBudget

  def apply[F[_]: Async](
      track: TrackUseCase[F],
      secured: MessageApi.Securing[F],
      budget: PollBudget = DefaultBudget
  ): List[ServerEndpoint[Any, F]] =
    List(
      secured.withBody(TrackEndpoints.track)(
        (_, request) => SecuredRoutes.bodyBytes(request),
        // Every topic the scan would read, checked before any of them is: one the caller may not read
        // refuses the whole track rather than quietly dropping out of the hits.
        (_, request) => Map(TrackEndpoints.TopicsField -> request.topics.map(_.value))
      ) { _ => (cluster, request) =>
        queryFor(cluster, request) match {
          case Left(error) => error.asLeft[TrackResultDto].pure[F]
          case Right(query) =>
            track
              .track(query, budget)
              .compile
              .toList
              .map(collect)
        }
      }
    )

  /** The events, as the one answer.
    *
    * A failure anywhere in the scan is the answer, even when hits arrived before it: a partial result
    * presented as a complete one is the shape of wrongness this whole feature exists to avoid, and a support
    * engineer who is told "not found" by a scan that actually broke will go and look in the wrong place.
    */
  private[api] def collect(events: List[TrackEvent]): Either[KuiError, TrackResultDto] =
    events.collectFirst { case TrackEvent.Failed(error) => error } match {
      case Some(error) => Left(error)
      case None =>
        val hits = events.collect { case TrackEvent.Hit(hit) => hit }
        val ending = events.collectFirst { case finished: TrackEvent.Finished => finished }

        Right(
          TrackResultDto(
            hits = hits.map(hit => TrackHitDto(hit.topic, MessageMapping.message(hit.record))),
            scanned = ending.fold(0L)(_.read),
            matched = ending.fold(hits.size.toLong)(_.matched),
            truncated = ending.exists(_.truncated)
          )
        )
    }

  /** The request document, as the domain's bounded query. */
  private def queryFor(cluster: ClusterId, request: TrackQueryDto): Either[KuiError, TrackQuery] =
    TrackQuery.of(
      cluster = cluster,
      topics = request.topics,
      from = request.from,
      until = request.to,
      matcher = matcherFor(request.`match`),
      limit = request.limit,
      isolation = None,
      // Correlation is ADR-029's *later* half: it groups hits that belong to one business event, and nothing
      // asks for it yet. It is passed as absent rather than left out of the mapping so that the day the
      // request grows the field, this line is where it lands.
      correlationKey = Option.empty[CorrelationKey]
    )

  /** The wire's three words, as the domain's operators.
    *
    * The domain has five and the wire offers three. That is not an oversight: `NOT_CONTAINS` and `NOT_EQUALS`
    * are expressible in the model and have no control on the screen yet, and publishing an operator no client
    * can produce would be a documented feature nobody can use.
    */
  private def matcherFor(dto: TrackMatchDto): TrackMatch = {
    val source =
      dto.source match {
        case TrackMatchDto.Source.Key => MatchSource.Key
        case TrackMatchDto.Source.Header => MatchSource.Header(dto.header.getOrElse(""))
        // `value` and anything the codec has already refused. The codec is the gate; this is exhaustive
        // because a `match` on a `String` has to be.
        case _ => MatchSource.Value
      }

    val operator =
      dto.operator match {
        case TrackMatchDto.Operator.Equals => MatchOperator.Equals
        case TrackMatchDto.Operator.Matches => MatchOperator.Regex
        case _ => MatchOperator.Contains
      }

    TrackMatch(source, operator, dto.value)
  }
}
