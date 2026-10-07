package kui.topic.domain

import kui.kernel.{ClusterId, TopicName}

/** Everything the topic context needs to *change* a Kafka cluster, in the topic context's words.
  *
  * It is a second port beside [[TopicAdmin]] rather than four more methods on it, and the split is not
  * tidiness. `TopicAdmin` is the port a screen reads through: every method is safe, every method is called on
  * a timer by the background scrape, and a fake of it in a test does nothing an operator would mind. This one
  * is the port that destroys things. Keeping them apart means a use case that only reads cannot reach a
  * delete by autocomplete, and a reader of the wiring can see at a glance which components were handed the
  * ability to change a cluster.
  *
  * ==Nothing here checks whether it is allowed to run==
  *
  * There is no read-only check in this file and there must not be one. ADR-047 puts that decision in
  * `MutationGuard` in the application layer, *before* the Kafka client is reached — a second check here would
  * be a copy of the rule, and a copy is the one that can disagree. An adapter implementing this port may
  * assume it is only called for a mutation that has already been permitted and recorded.
  *
  * Every method is total: a failure is a `TopicError` on the left, never a raised exception, which is the
  * same contract [[TopicAdmin]] carries and which `PortContractSuite` asserts for both.
  */
trait TopicWriter[F[_]] {

  /** Create a topic.
    *
    * `TopicError.AlreadyExists` for `TopicExistsException`, which is a refusal an operator can act on and not
    * a failure. Absent partitions or replication factor are passed to the broker as absent, so it applies its
    * own `num.partitions` and `default.replication.factor` rather than a number KUI invented.
    */
  def create(cluster: ClusterId, spec: NewTopicSpec): F[Either[TopicError, Unit]]

  /** Set and remove entries of a topic's dynamic configuration.
    *
    * Incremental (`incrementalAlterConfigs`, Kafka 2.3 and later): keys the change does not name are left
    * exactly as they are. `research/kafka/admin-capabilities.md` records that the deprecated `alterConfigs`
    * replaces the whole set, which would silently revert every override a caller did not resend.
    */
  def alterConfig(
      cluster: ClusterId,
      topic: TopicName,
      change: TopicConfigChange
  ): F[Either[TopicError, Unit]]

  /** Raise a topic's partition count to `target`.
    *
    * Kafka refuses a target that is not strictly greater and cannot reduce one at all. The refusal is made in
    * [[PartitionPlan.of]] before this is called, so a rejection here is a race — the count changed between
    * plan and apply — and is reported as the broker's own refusal rather than as a KUI bug.
    */
  def increasePartitions(cluster: ClusterId, topic: TopicName, target: Int): F[Either[TopicError, Unit]]

  /** Delete a topic.
    *
    * The call returns when the controller has *accepted* the deletion, not when the log directories are gone:
    * `deleteTopics` is asynchronous and the topic can still be listed for a moment afterwards. Callers must
    * not treat "still in the list" as a failure, and the screen says so rather than looping until it
    * disappears.
    *
    * A cluster with `delete.topic.enable=false` refuses with `TopicError.Rejected`, which names the setting.
    */
  def delete(cluster: ClusterId, topic: TopicName): F[Either[TopicError, Unit]]

  /** Delete only the approved Kafka identity, even if the name was replaced after the final read. */
  def deleteById(cluster: ClusterId, topic: TopicName, topicId: String): F[Either[TopicError, Unit]]

  /** Whether this cluster's brokers will recreate a deleted topic the moment anything names it.
    *
    * `None` means KUI could not read it — the broker refused `describeConfigs` on a broker resource, or the
    * call failed — and that is a third answer the deletion plan reports as "unknown". Reading it as `false`
    * would turn "we did not check" into a promise that the topic stays deleted, and the browse path has
    * already been bitten by exactly this behaviour once.
    *
    * It never fails: a plan that could not be built because a *warning* could not be computed would be a plan
    * that refuses to describe a delete the operator is allowed to make.
    */
  def autoCreateEnabled(cluster: ClusterId): F[Option[Boolean]]
}
