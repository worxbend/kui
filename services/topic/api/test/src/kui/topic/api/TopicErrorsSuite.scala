package kui.topic.api

import munit.FunSuite

import kui.contracts.ErrorEnvelope
import kui.kernel.error.ErrorCode
import kui.kernel.{ClusterId, TopicName}
import kui.topic.domain.TopicError

/** The domain-to-wire error table, exhaustively.
  *
  * Two `NotFound` cases produce two different codes, and that is the point of the table rather than an
  * accident of it: "check the topic name" and "check which cluster you are on" are different instructions,
  * and a browser can only give the right one if the code says which happened.
  */
final class TopicErrorsSuite extends FunSuite {

  private val topic = TopicName.unsafe("orders")
  private val cluster = ClusterId.unsafe("prod-eu")

  private def codeAndStatus(error: TopicError): (ErrorCode, Int) = {
    val kui = TopicErrors.toKui(error)
    (kui.code, ErrorEnvelope.statusOf(kui))
  }

  test("the mapping table, one row per domain case") {
    assertEquals(codeAndStatus(TopicError.NotFound(topic)), (ErrorCode.TopicNotFound, 404))
    assertEquals(codeAndStatus(TopicError.ClusterNotFound(cluster)), (ErrorCode.ClusterNotFound, 404))
    assertEquals(codeAndStatus(TopicError.Forbidden("no DESCRIBE")), (ErrorCode.Forbidden, 403))
    assertEquals(
      codeAndStatus(TopicError.Unreachable("leader election", retryable = true)),
      (ErrorCode.Timeout, 408)
    )
    assertEquals(
      codeAndStatus(TopicError.Unreachable("bad truststore", retryable = false)),
      (ErrorCode.UpstreamUnavailable, 503)
    )
  }

  test("every domain case is covered, so a sixth cannot be added silently") {
    // `TopicError` is a sealed enum, so this is the exhaustive list at the moment the suite was written.
    // A new case makes `TopicErrors.toKui` fail to compile, which is the real enforcement; this asserts
    // that none of the five is quietly mapped to the same code as another.
    val codes = List(
      TopicError.NotFound(topic),
      TopicError.ClusterNotFound(cluster),
      TopicError.Forbidden("x"),
      TopicError.Unreachable("x", retryable = true),
      TopicError.Unreachable("x", retryable = false)
    ).map(error => TopicErrors.toKui(error).code)

    assertEquals(codes.distinct.size, codes.size, codes.toString)
  }

  test("the two 404s are different codes, because the remedies differ") {
    assertNotEquals(
      TopicErrors.toKui(TopicError.NotFound(topic)).code,
      TopicErrors.toKui(TopicError.ClusterNotFound(cluster)).code
    )
  }

  test("a retryable failure does not claim the call was given zero milliseconds") {
    // `KafkaTopicWriter.writeError` turns a broker `TimeoutException` into this detail with
    // `retryable = true`. `toKui` keeps HTTP 408 and used to stuff the sentence into
    // `InfrastructureError.Timeout`'s operation with `afterMs = 0`, so the wire message ended
    // "within 0ms".
    val message = TopicErrors
      .toKui(
        TopicError.Unreachable(
          "the cluster did not answer in time; the change may still have been applied",
          retryable = true
        )
      )
      .message

    assert(!message.contains("within 0ms"), message)
    assert(message.contains("may still have been applied"), message)
  }

  test("a retryable failure and a permanent one are different codes") {
    // A screen offers a retry button for one and an explanation for the other. Collapsing the two would
    // make it offer the wrong thing half the time.
    assertNotEquals(
      TopicErrors.toKui(TopicError.Unreachable("x", retryable = true)).code,
      TopicErrors.toKui(TopicError.Unreachable("x", retryable = false)).code
    )
  }

  test("an authorization failure is an application error, so it cannot dim a capability") {
    // ADR-039 §6: only an infrastructure failure of the upstream *service* dims a capability. A cluster
    // that refuses KUI's credentials is not a sign that the topic service is broken, and reporting it as
    // one would take the Topics feature away from every other cluster in the deployment.
    val forbidden = TopicErrors.toKui(TopicError.Forbidden("no DESCRIBE on topic 'orders'"))

    assert(
      forbidden.isInstanceOf[kui.kernel.error.ApplicationError],
      forbidden.getClass.getName
    )
  }

  test("the message a caller reads names the thing, not the internals") {
    assert(
      TopicErrors.toKui(TopicError.NotFound(topic)).message.contains("orders"),
      TopicErrors.toKui(TopicError.NotFound(topic)).message
    )
    assert(
      TopicErrors.toKui(TopicError.ClusterNotFound(cluster)).message.contains("prod-eu"),
      TopicErrors.toKui(TopicError.ClusterNotFound(cluster)).message
    )
  }
}
