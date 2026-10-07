package kui.cluster.client

import io.circe.Decoder

import kui.cluster.contract.dto.ClusterChangeDto
import kui.http.sse.SseEvent

/** Turning one frame of the cluster service's change stream into something to act on.
  *
  * It is a separate, pure object so that `clusters-stream.sse` — a byte-for-byte capture of what the service
  * writes — can be run through it in a suite with no network, no clock and no effect type. That is the seam
  * half of this module's contract: the producer's bytes, decoded by the consumer's decoder, in one assertion
  * that fails if either side moves.
  */
object ProfileSubscription {

  /** What one stream event means to a consumer.
    *
    * An event this client does not recognise is [[Ignored]] rather than an error, and that is ADR-035's own
    * rule read from the consuming side: a producer is allowed to add an event name, and a consumer that
    * failed the connection on meeting one would go blind to every *other* change as well.
    */
  enum Instruction {
    case Refetch(dto: ClusterChangeDto)
    case Forget(dto: ClusterChangeDto)

    /** A frame that is not a cluster change, or is one this version does not understand. */
    case Ignored(reason: String)
  }

  object Instruction {
    given CanEqual[Instruction, Instruction] = CanEqual.derived
  }

  /** The `event:` name the cluster service writes its changes under.
    *
    * The producing side spells it in `kui.cluster.api.ClusterStreamEndpoint`, which lives in the service's
    * `api` layer and which rule A11 keeps out of this module. `theRecordedStreamParses` is what holds the two
    * spellings together: it reads a recorded capture of the real stream, so a rename on the producing side
    * fails here rather than causing this client to quietly stop hearing changes.
    */
  val EventName: String = kui.cluster.contract.ProfileEndpoints.EventName

  def instructionFor(event: SseEvent): Instruction =
    if event.name != EventName then Instruction.Ignored(s"'${event.name}' is not a cluster change")
    else
      Decoder[ClusterChangeDto].decodeJson(event.data) match {
        case Left(failure) => Instruction.Ignored(s"undecodable cluster change: ${failure.message}")
        case Right(change) =>
          change.change match {
            case ClusterChangeDto.Updated => Instruction.Refetch(change)
            case ClusterChangeDto.Removed => Instruction.Forget(change)
            // A future `"renamed"`: heard, logged, and not acted on. The version it carries will be
            // picked up by the fallback poll, so the worst case is a minute of staleness rather than a
            // client that stopped decoding the stream.
            case other => Instruction.Ignored(s"'$other' is not a change kind this version acts on")
          }
      }
}
