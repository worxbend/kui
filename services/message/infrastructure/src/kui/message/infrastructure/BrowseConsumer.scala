package kui.message.infrastructure

import scala.concurrent.duration.FiniteDuration

import kui.kernel.error.KuiError
import kui.kernel.{PartitionId, TopicName}
import kui.message.application.RawRecord

/** The handful of consumer operations a browse actually uses, as an interface.
  *
  * ==Why this exists rather than a Kafka consumer passed around==
  *
  * Everything difficult about browsing is arithmetic: which offsets a seek resolves to, how a backward browse
  * walks a partition in windows, when a bounded read knows it has reached the end. None of that is about
  * Kafka, and all of it is wrong in ways only a test can find — an off-by-one at a window boundary duplicates
  * or drops exactly one record per page, which is the kind of defect that survives a demo.
  *
  * With this seam the arithmetic lives in [[KafkaRecordSource]] and is tested against an in-memory log, while
  * [[KafkaBrowseConsumer]] holds the Kafka calls and nothing else. Without it, every one of those tests would
  * need a broker.
  *
  * ==What an implementation promises==
  *
  * Every method is total: a failure comes back as a `Left`, never as a raised exception, because the layer
  * above turns a failure into an event on a stream that has already delivered records (ADR-035). Exceptions
  * are translated here, at the adapter boundary, and nowhere above it.
  */
trait BrowseConsumer[F[_]] {

  /** Every partition of the topic, in whatever order the broker reports them. */
  def partitions(topic: TopicName): F[Either[KuiError, List[PartitionId]]]

  /** The oldest offset each partition still holds — not zero on a topic that retention or compaction has
    * trimmed, which is why it is asked for rather than assumed.
    */
  def beginningOffsets(
      topic: TopicName,
      partitions: List[PartitionId]
  ): F[Either[KuiError, Map[PartitionId, Long]]]

  /** The offset the next record written will get. Under `READ_COMMITTED` this is the last stable offset,
    * which can be well behind the physical end while a transaction is open — a normal condition, not a fault.
    */
  def endOffsets(topic: TopicName, partitions: List[PartitionId]): F[Either[KuiError, Map[PartitionId, Long]]]

  /** The first offset at or after `millis` per partition. A partition with nothing that recent answers
    * `None`, which is a different thing from offset zero and must not be collapsed into it.
    */
  def offsetsForTimes(
      topic: TopicName,
      partitions: List[PartitionId],
      millis: Long
  ): F[Either[KuiError, Map[PartitionId, Option[Long]]]]

  /** Assigns partitions explicitly. KUI never subscribes: a browse must not join a consumer group, because
    * joining one triggers a rebalance in somebody's running application.
    */
  def assign(topic: TopicName, partitions: List[PartitionId]): F[Either[KuiError, Unit]]

  def seek(topic: TopicName, partition: PartitionId, offset: Long): F[Either[KuiError, Unit]]

  /** Positions advance over compacted offsets and control records, unlike delivered records. */
  def positions: F[Either[KuiError, Map[PartitionId, Long]]]

  /** One poll. Returns whatever arrived within `timeout`, which is routinely nothing.
    *
    * The timeout is short by design and the caller loops. A single long poll would be fewer calls and would
    * also be a browse that ignores its own cancellation for the length of that poll — and cancellation
    * arriving promptly is the whole point of the chain from a closed browser tab down to here.
    */
  def poll(timeout: FiniteDuration): F[Either[KuiError, List[RawRecord]]]
}
