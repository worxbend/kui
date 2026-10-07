package kui.topic.domain

/** One topic as its detail page shows it: the row the list would have shown, plus the partitions behind it.
  *
  * The summary is not recomputed by the page — it is built from `partitions` by [[TopicDetail.of]], so the
  * indicator strip at the top of the page and the table underneath it cannot disagree. Three consistent
  * statements of one fact ("two partitions offline", "no message count", "leader: offline" on two rows) is
  * what stops an operator from concluding a topic is empty when KUI simply could not read it.
  */
final case class TopicDetail(
    summary: TopicSummary,
    partitions: List[PartitionView],
    /** The `cleanup.policy` the topic is configured with, when the configuration could be read. `None` means
      * KUI was not allowed to read the topic's configuration — never that the topic has no policy, because
      * every topic has one.
      */
    cleanupPolicy: Option[String],
    /** How many log segments the topic's partitions hold. `None` when the broker would not report its log
      * directories, the same refusal as `TopicSummary.sizeBytes`.
      */
    segmentCount: Option[Int],
    /** Kafka's immutable identity, absent when the broker cannot report it. */
    topicId: Option[String] = None
) {
  def name: kui.kernel.TopicName = summary.name
}

object TopicDetail {

  /** Builds a detail from its partitions, deriving every aggregate rather than accepting one.
    *
    * Partitions are sorted by id here and nowhere else: a broker reports them in whatever order its metadata
    * response happened to arrive in, and a table that reshuffles between two refreshes of the same page reads
    * as data changing when nothing has.
    */
  def of(
      name: kui.kernel.TopicName,
      isInternal: Boolean,
      partitions: List[PartitionView],
      cleanupPolicy: Option[String] = None,
      segmentCount: Option[Int] = None,
      topicId: Option[String] = None
  ): TopicDetail = {
    val ordered = partitions.sorted

    TopicDetail(
      // The policy goes into the summary as well as onto the detail, because the row the list shows and the
      // row embedded in this document are the same record and M5 put `cleanupPolicy` on it. Building both
      // from the one argument is what stops the header's tag and the list's column disagreeing about a topic
      // whose configuration was read once.
      summary = TopicSummary.of(name, isInternal, ordered, cleanupPolicy),
      partitions = ordered,
      cleanupPolicy = cleanupPolicy,
      segmentCount = segmentCount,
      topicId = topicId
    )
  }

  given CanEqual[TopicDetail, TopicDetail] = CanEqual.derived
}
