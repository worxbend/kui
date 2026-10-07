package kui.message.domain.ports

import kui.kernel.error.KuiError
import kui.kernel.{ClusterId, Offset, PartitionId, TopicName}
import kui.message.domain.{PlannedPurge, PurgeResult}

/** Emptying a topic's partitions (`MS-008`), in the message domain's words.
  *
  * A port of its own rather than four more methods on the browse or produce ports, and for the same reason
  * the topic service keeps its writer separate from its reader: this is the one thing in the message service
  * that takes data away. A reader of the wiring can see at a glance which component was handed that, and a
  * use case that only reads cannot reach a delete by autocomplete.
  *
  * Every method is total: a failure is a `KuiError` on the left, never a raised exception.
  */
trait RecordDeleter[F[_]] {

  /** Kafka's nonzero topic UUID, never the reusable name. Refuse when it cannot be established. */
  def topicId(cluster: ClusterId, topic: TopicName): F[Either[KuiError, String]]

  /** Where each partition's log currently starts and ends.
    *
    * This is the plan phase's whole input, and it is why the operation has a plan at all: the number of
    * records a purge destroys is not in the request, it is in the broker, and it moves while the operator is
    * reading. Missing leaders or unknown offsets refuse the entire plan: a destructive preview must never
    * silently exclude the partitions whose contents could not be measured.
    */
  def watermarks(cluster: ClusterId, topic: TopicName): F[Either[KuiError, List[PlannedPurge]]]

  /** The topic's `cleanup.policy`, when it can be read.
    *
    * `None` means KUI could not read it. It is used for a *warning* — Kafka refuses `deleteRecords` on a
    * topic that is only compacted — so a failure here costs the warning and never the plan.
    */
  def cleanupPolicy(cluster: ClusterId, topic: TopicName): F[Option[String]]

  /** Delete every record below the given offset, per partition.
    *
    * **Irreversible.** Kafka's `deleteRecords` moves the partition's low watermark and the records below it
    * are gone: no tombstone, no copy, no undo.
    *
    * The result reports every partition that was asked for in exactly one of its two maps — purged, with its
    * new low watermark, or skipped, with the broker's reason. That invariant is what stops a partition
    * silently vanishing from a destructive operation's report, and it is the same rule `libs/kafka`'s
    * `BatchResult` states for every batched admin call.
    */
  def deleteBefore(
      cluster: ClusterId,
      topic: TopicName,
      offsets: Map[PartitionId, Offset]
  ): F[Either[KuiError, PurgeResult]]
}
