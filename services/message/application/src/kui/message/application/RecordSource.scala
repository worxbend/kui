package kui.message.application

import java.time.Instant

import fs2.Stream

import kui.kernel.browse.PollBudget
import kui.kernel.error.KuiError
import kui.kernel.{Offset, PartitionId}
import kui.message.domain.{BrowseRequest, TimestampType}

/** One Kafka header, still as bytes.
  *
  * Bytes and not text, because rendering a header is a decision — some are UTF-8, some are the big-endian
  * integers Spring's retry machinery writes — and a port that handed text down would have made that decision
  * inside the adapter where no test can see it.
  */
final case class RawHeader(key: String, value: Option[Array[Byte]])

/** One record as it left the broker: nothing decoded, nothing rendered, nothing filtered.
  *
  * `key` and `value` are `Option` because Kafka's are nullable, and the difference matters twice over: a
  * record with no key is ordinary, and a record with a null *value* is a tombstone, which is a fact about the
  * data rather than a missing field.
  *
  * The three sizes are the *serialised* sizes, taken before anything decoded them. They are the only numbers
  * on this record that survive decoding unchanged, and they are what an operator hunting the record that is
  * filling a partition actually needs.
  */
final case class RawRecord(
    partition: PartitionId,
    offset: Offset,
    timestamp: Instant,
    timestampType: TimestampType,
    key: Option[Array[Byte]],
    value: Option[Array[Byte]],
    headers: List[RawHeader],
    keySize: Int,
    valueSize: Int,
    headersSize: Int
)

/** Where records come from, stated so that the use case above it never sees a Kafka type.
  *
  * ==Why the element type is an `Either` and not a failed stream==
  *
  * A browse can fail after it has already delivered records — a broker that goes away mid-poll, a partition
  * that loses its leader — and the user must be told *and* keep the records they already have. A raised
  * exception would give the caller a stream that ended with no explanation, which is precisely the failure
  * ADR-035 exists to remove. So a failure is an element: the stream emits `Left(error)` and then ends, and
  * the layer above turns that into the stream's terminal `error` event.
  *
  * ==What an implementation must promise==
  *
  *   - **it never materialises a topic.** Records are emitted as they are polled, and a backward browse reads
  *     bounded offset windows rather than reading a partition from its beginning and keeping the tail.
  *   - **it is a `Resource` underneath.** The consumer it opens is closed when the stream is finished *or
  *     cancelled*, so a browser tab that goes away closes a Kafka consumer rather than leaking one.
  */
trait RecordSource[F[_]] {

  /** A bounded scan must positively report reaching its captured end. Legacy adapters cannot prove it. */
  def scan(
      request: BrowseRequest,
      budget: PollBudget,
      upperOffsets: Map[PartitionId, Offset] = Map.empty
  ): Stream[F, ScanEvent] =
    browse(request, budget)
      .filter {
        case Right(record) => upperOffsets.get(record.partition).forall(record.offset.value < _.value)
        case Left(_) => true
      }
      .map {
        case Right(record) => ScanEvent.Record(record)
        case Left(error) => ScanEvent.Failed(error)
      } ++ Stream.emit(ScanEvent.Completed(ScanCompletion.Unknown))

  /** The records one browse asks for, in the order the browse wants them: ascending offsets for a forward
    * browse, newest first for a backward one.
    *
    * @param budget
    *   what this browse may consume before it gives up. It is separate from the request because the request
    *   is what the user asked for and the budget is what the deployment allows, and an operator tunes the
    *   second without the first changing meaning.
    */
  def browse(request: BrowseRequest, budget: PollBudget): Stream[F, Either[KuiError, RawRecord]]

  /** The offset every partition assigned to this request actually starts reading from — the same value
    * [[browse]] itself would seek each one to, whatever the seek mode resolves to and whether `partitions`
    * names them or the source has to ask the broker which partitions exist.
    *
    * A browse only needs this once, to seed a starting boundary for a partition that never yields a record
    * before a page's `limit` is reached (otherwise such a partition is silently dropped from every subsequent
    * page's cursor). Kept separate from [[browse]]'s own stream rather than threaded through it, because it
    * is asked for at most once per page — when a continuation cursor is actually being minted — and not once
    * per record.
    */
  def assignedStarts(request: BrowseRequest): F[Either[KuiError, Map[PartitionId, Offset]]]
}

enum ScanCompletion {
  case End, RecordBudget, ByteBudget, Deadline, Unknown
}

enum ScanEvent {
  case Record(record: RawRecord)
  case Failed(error: KuiError)
  case Completed(reason: ScanCompletion)
}
