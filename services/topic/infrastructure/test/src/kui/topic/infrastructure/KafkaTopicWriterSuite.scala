package kui.topic.infrastructure

import munit.FunSuite
import org.apache.kafka.common.errors.{
  InvalidConfigurationException,
  InvalidRequestException,
  SaslAuthenticationException,
  TimeoutException,
  TopicAuthorizationException,
  UnsupportedVersionException
}

import kui.kernel.TopicName
import kui.topic.domain as dom

/** How a refused write is told apart from an unreachable cluster, and what KUI is allowed to say about it.
  *
  * `KafkaTopicWriter` shipped with no suite of its own: every rule in `writeError` was reachable only through
  * a route that asserts a status code. Three mutations were applied one at a time against
  * `./mill services.topic.__.test`, each leaving it at 298/298 green — making the
  * `UnsupportedVersionException` arm unreachable, appending the exception's own message to a refusal, and
  * turning a configuration `DELETE` into a `SET`. The first two are closed here; the third needs an admin
  * client this module has no stub for and is reported rather than pretended.
  */
final class KafkaTopicWriterSuite extends FunSuite {

  private val orders: Option[TopicName] = Some(TopicName.unsafe("orders.v1"))

  test("brokers too old for a call are said to be too old, and not blamed on the request") {
    /*
     * The ordering rule the file's own comment argues for, and the reason it is fragile: Kafka's
     * `UnsupportedVersionException` is a *subclass* of `InvalidConfigurationException`, so a later editor
     * grouping the two -- or moving the parent's arm above the child's -- changes the answer with nothing
     * to see in the diff. An operator whose brokers are too old for `incrementalAlterConfigs` would then
     * be told to fix a configuration entry that is perfectly valid.
     */
    val old = KafkaTopicWriter.writeError(new UnsupportedVersionException("too old"), orders)
    val refused = KafkaTopicWriter.writeError(new InvalidRequestException("bad key"), orders)
    val invalidConfig = KafkaTopicWriter.writeError(new InvalidConfigurationException("bad value"), orders)

    old match {
      case dom.TopicError.Unreachable(detail, retryable) =>
        assertEquals(detail, "this cluster's brokers are too old for this operation")
        // Not retryable: brokers do not get newer while a dialog is open, and a Try-again button here
        // would be a button that cannot work.
        assertEquals(retryable, false)
      case other => fail(s"an unsupported version must be Unreachable, not $other")
    }

    // The parent class, and the sibling that shares its arm: both are refusals of the request, which is
    // the answer the child must not be given.
    assert(
      refused.isInstanceOf[dom.TopicError.Rejected],
      s"an invalid request must be Rejected, not $refused"
    )
    assert(
      invalidConfig.isInstanceOf[dom.TopicError.Rejected],
      s"an invalid configuration must be Rejected, not $invalidConfig"
    )
    assertNotEquals(old.message, refused.message)
  }

  test("rejected credentials are not reported as a configuration the operator should edit") {
    // `SaslAuthenticationException` extends `AuthenticationException`, which extends
    // `InvalidConfigurationException`. Matching the parent first classifies a bad password as
    // `Rejected` (`KUI-VALIDATION`, 400) and the audit trail records a refusal.
    val error = KafkaTopicWriter.writeError(new SaslAuthenticationException("bad password"), orders)

    error match {
      case dom.TopicError.Rejected(_) =>
        fail(s"authentication must not be Rejected: $error")
      case dom.TopicError.Unreachable(detail, retryable) =>
        assertEquals(detail, "KUI's credentials were rejected")
        assertEquals(retryable, false)
        assert(!error.message.contains("bad password"), error.message)
      case other => fail(s"authentication must be a permanent Unreachable, not $other")
    }
  }

  test("a refusal is said in KUI's own words, never in the broker exception's") {
    /*
     * The rule the class comment states: "a Kafka exception's message routinely carries the bootstrap
     * string and, on some SASL paths, the principal". Appending `getMessage` to the forbidden arm left the
     * suite green, and this is the one seam where a mutation of that kind reaches a screen: `TopicError`
     * is rendered into the error envelope a browser is shown.
     */
    val leaky =
      new TopicAuthorizationException("not authorized for kafka-1.internal:9093 as User:CN=svc-kui")
    val error = KafkaTopicWriter.writeError(leaky, orders)

    assertEquals(error, dom.TopicError.Forbidden("TopicAuthorization"))
    assert(!error.message.contains("kafka-1.internal"), s"the bootstrap address reached the wire: $error")
    assert(!error.message.contains("User:CN=svc-kui"), s"the principal reached the wire: $error")
  }

  test("a timed-out mutation says it may still have been applied, and is retryable") {
    // The other half of the same vocabulary: this one is genuinely about the cluster rather than about the
    // request, and `deleteTopics` can be accepted by the controller and still time out on the way back.
    KafkaTopicWriter.writeError(new TimeoutException("no answer"), orders) match {
      case dom.TopicError.Unreachable(detail, retryable) =>
        assertEquals(detail, "the cluster did not answer in time; the change may still have been applied")
        assertEquals(retryable, true)
      case other => fail(s"a timeout must be a retryable Unreachable, not $other")
    }
  }
}
