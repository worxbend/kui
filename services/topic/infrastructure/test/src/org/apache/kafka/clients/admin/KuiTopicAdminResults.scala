package org.apache.kafka.clients.admin

import java.util.Map as JMap

import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.internals.KafkaFutureImpl

/** Factories for the admin result types a topic scrape reads and a topic mutation is answered with, whose
  * constructors Kafka keeps package-private or protected.
  *
  * It lives in Kafka's own package for that one reason, and it is the same device the `KuiTopicTestSynonyms`
  * beside it uses. Without it, the behaviour argued at length in `KafkaTopicAdmin.cleanupPolicies`'s scaladoc
  * — one batched `describeConfigs` per two hundred topics, a per-topic failure costing that row's policy, a
  * whole-batch failure costing that batch's policies and not the page — could only be exercised against a
  * broker that had been deliberately misconfigured, which is why none of it was exercised at all.
  *
  * `ListOffsetsResult` is not here: its constructor is public.
  *
  * Test sources only. Nothing shipped is in this package.
  */
object KuiTopicAdminResults {

  def listTopics(listings: JMap[String, TopicListing]): ListTopicsResult =
    new ListTopicsResult(KafkaFuture.completedFuture(listings))

  def describeTopics(values: JMap[String, KafkaFuture[TopicDescription]]): DescribeTopicsResult =
    DescribeTopicsResult.ofTopicNames(values)

  def deleteTopicIds(
      values: JMap[org.apache.kafka.common.Uuid, KafkaFuture[java.lang.Void]]
  ): DeleteTopicsResult =
    DeleteTopicsResult.ofTopicIds(values)

  def describeConfigs(values: JMap[ConfigResource, KafkaFuture[Config]]): DescribeConfigsResult =
    new DescribeConfigsResult(values)

  def describeLogDirs(
      values: JMap[Integer, KafkaFuture[JMap[String, LogDirDescription]]]
  ): DescribeLogDirsResult =
    new DescribeLogDirsResult(values)

  /** `createTopics`, whose constructor is `protected` and whose futures carry a metadata record.
    *
    * The suite that needs it is not interested in the answer at all — it is interested in the `NewTopic` that
    * was handed over, because that is where "absent partitions mean the broker's `num.partitions`" either
    * holds or does not. An answer still has to exist for the adapter to read.
    */
  def createTopics(
      values: JMap[String, KafkaFuture[CreateTopicsResult.TopicMetadataAndConfig]]
  ): CreateTopicsResult =
    new CreateTopicsResult(values)

  /** The metadata a broker returns for a topic it has just created. */
  def created(partitions: Int, replicationFactor: Int): CreateTopicsResult.TopicMetadataAndConfig =
    new CreateTopicsResult.TopicMetadataAndConfig(
      org.apache.kafka.common.Uuid.randomUuid(),
      partitions,
      replicationFactor,
      new Config(java.util.List.of[ConfigEntry]())
    )

  /** `incrementalAlterConfigs`, whose constructor is package-private. */
  def alterConfigs(values: JMap[ConfigResource, KafkaFuture[java.lang.Void]]): AlterConfigsResult =
    new AlterConfigsResult(values)

  /** A `KafkaFuture[Void]` that has completed.
    *
    * `java.lang.Void` has exactly one value and it is `null`; Kafka's own clients complete these futures the
    * same way. It is confined to this test-only file for that reason.
    */
  val completedVoid: KafkaFuture[java.lang.Void] = KafkaFuture.completedFuture(null)

  /** A future that has already failed — how Kafka reports a resource the caller may not describe. */
  def failed[A](failure: Throwable): KafkaFuture[A] = {
    val future = new KafkaFutureImpl[A]()
    val _ = future.completeExceptionally(failure)
    future
  }
}
