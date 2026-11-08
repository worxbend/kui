package kui.kafka

import java.util.concurrent.{CompletionException, ExecutionException}

import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.*
import org.scalacheck.Prop.forAll
import org.scalacheck.{Gen, Prop}

import kui.kafka.KafkaErrorMapper.FailureClass
import kui.kernel.error.{ApplicationError, DomainError, ErrorCode, InfrastructureError, KuiError}
import kui.testkit.{KuiSuite, RedactionAssertions}

/** The mapping table, asserted row by row, plus the three properties that make it trustworthy.
  *
  * The rows matter individually because three separate decisions key on them: whether a failure dims a
  * capability (the `Application` / `Infrastructure` split), what HTTP status a user sees (the `ErrorCode`),
  * and whether one bad key fails a whole batch (`suppressible`).
  */
final class KafkaErrorMapperSuite extends KuiSuite {

  private val operation = "describeConfigs"

  /** One row of the table: the exception, the failure class, the error code it renders as, whether the error
    * is an infrastructure error, and whether a per-key failure of this kind is suppressible.
    */
  final private case class Row(
      failure: Throwable,
      failureClass: FailureClass,
      code: ErrorCode,
      infrastructure: Boolean,
      suppressible: Boolean
  )

  private val table: List[Row] = List(
    Row(new TimeoutException("t"), FailureClass.Reconnect, ErrorCode.Timeout, true, false),
    Row(new SaslAuthenticationException("t"), FailureClass.Reconnect, ErrorCode.UpstreamAuth, true, false),
    Row(new SslAuthenticationException("t"), FailureClass.Reconnect, ErrorCode.UpstreamAuth, true, false),
    Row(
      new BrokerNotAvailableException("t"),
      FailureClass.Reconnect,
      ErrorCode.UpstreamUnavailable,
      true,
      false
    ),
    Row(
      new ClusterAuthorizationException("t"),
      FailureClass.NotAuthorized,
      ErrorCode.Forbidden,
      false,
      true
    ),
    Row(
      new TopicAuthorizationException("t"),
      FailureClass.NotAuthorized,
      ErrorCode.Forbidden,
      false,
      true
    ),
    Row(
      new GroupAuthorizationException("t"),
      FailureClass.NotAuthorized,
      ErrorCode.Forbidden,
      false,
      true
    ),
    Row(
      new DelegationTokenAuthorizationException("t"),
      FailureClass.NotAuthorized,
      ErrorCode.Forbidden,
      false,
      true
    ),
    Row(
      new TransactionalIdAuthorizationException("t"),
      FailureClass.NotAuthorized,
      ErrorCode.Forbidden,
      false,
      true
    ),
    Row(
      new SecurityDisabledException("t"),
      FailureClass.Unsupported,
      ErrorCode.Unsupported,
      false,
      true
    ),
    Row(
      new UnsupportedVersionException("t"),
      FailureClass.Unsupported,
      ErrorCode.Unsupported,
      false,
      true
    ),
    Row(
      new InvalidRequestException("t"),
      FailureClass.Unsupported,
      ErrorCode.Unsupported,
      false,
      true
    ),
    Row(
      new TopicDeletionDisabledException("t"),
      FailureClass.Unsupported,
      ErrorCode.Unsupported,
      false,
      true
    ),
    Row(
      new UnknownTopicOrPartitionException("t"),
      FailureClass.NotFound,
      ErrorCode.TopicNotFound,
      false,
      true
    ),
    Row(new UnknownTopicIdException("t"), FailureClass.NotFound, ErrorCode.TopicNotFound, false, true),
    Row(new LogDirNotFoundException("t"), FailureClass.NotFound, ErrorCode.TopicNotFound, false, true),
    Row(new KafkaStorageException("t"), FailureClass.Request, ErrorCode.InvalidState, false, true),
    Row(
      new InvalidConfigurationException("t"),
      FailureClass.Request,
      ErrorCode.Validation,
      false,
      false
    ),
    Row(new PolicyViolationException("t"), FailureClass.Request, ErrorCode.Validation, false, false),
    Row(new InvalidTopicException("t"), FailureClass.Request, ErrorCode.Validation, false, false),
    Row(new InvalidPartitionsException("t"), FailureClass.Request, ErrorCode.Validation, false, false),
    Row(
      new InvalidReplicationFactorException("t"),
      FailureClass.Request,
      ErrorCode.Validation,
      false,
      false
    ),
    Row(
      new InvalidReplicaAssignmentException("t"),
      FailureClass.Request,
      ErrorCode.Validation,
      false,
      false
    ),
    Row(new TopicExistsException("t"), FailureClass.Request, ErrorCode.InvalidState, false, false),
    Row(new GroupNotEmptyException("t"), FailureClass.Request, ErrorCode.InvalidState, false, false),
    Row(
      new GroupSubscribedToTopicException("t"),
      FailureClass.Request,
      ErrorCode.InvalidState,
      false,
      false
    ),
    Row(
      new ReassignmentInProgressException("t"),
      FailureClass.Request,
      ErrorCode.InvalidState,
      false,
      false
    ),
    Row(
      new NoReassignmentInProgressException("t"),
      FailureClass.Request,
      ErrorCode.InvalidState,
      false,
      false
    ),
    Row(
      new ElectionNotNeededException("t"),
      FailureClass.Request,
      ErrorCode.InvalidState,
      false,
      false
    ),
    Row(new GroupIdNotFoundException("t"), FailureClass.NotFound, ErrorCode.InvalidState, false, true),
    Row(
      new TransactionalIdNotFoundException("t"),
      FailureClass.NotFound,
      ErrorCode.InvalidState,
      false,
      true
    ),
    Row(new UnknownMemberIdException("t"), FailureClass.NotFound, ErrorCode.InvalidState, false, true),
    Row(
      new CoordinatorNotAvailableException("t"),
      FailureClass.Request,
      ErrorCode.UpstreamUnavailable,
      true,
      false
    ),
    Row(
      new NotLeaderOrFollowerException("t"),
      FailureClass.Request,
      ErrorCode.UpstreamUnavailable,
      true,
      false
    ),
    Row(
      new LeaderNotAvailableException("t"),
      FailureClass.Request,
      ErrorCode.UpstreamUnavailable,
      true,
      false
    ),
    Row(
      new NotEnoughReplicasException("t"),
      FailureClass.Request,
      ErrorCode.UpstreamUnavailable,
      true,
      false
    ),
    Row(
      new UnknownServerException("t"),
      FailureClass.Request,
      ErrorCode.UpstreamUnavailable,
      true,
      true
    )
  )

  test("theMappingTable") {
    table.foreach { row =>
      val name = row.failure.getClass.getSimpleName
      val mapped = KafkaErrorMapper.map(operation, row.failure)

      assertEquals(KafkaErrorMapper.classify(row.failure), row.failureClass, name)
      assertEquals(mapped.code, row.code, name)
      assertEquals(mapped.isInstanceOf[InfrastructureError], row.infrastructure, name)
      assertEquals(KafkaErrorMapper.suppressible(row.failure).isDefined, row.suppressible, name)
    }
  }

  test("theTableCoversEveryExceptionTheResearchNames") {
    // A count, so that deleting a row is a visible edit rather than a silent omission.
    assertEquals(table.size, 37)
    assertEquals(table.map(_.failure.getClass).distinct.size, table.size)
  }

  test("theSplitThatDecidesWhetherACapabilityDims") {
    // ADR-039 §6: only an `InfrastructureError` is reported to the capability registry. One user
    // lacking an ACL must not grey the feature out for everybody.
    assert(
      KafkaErrorMapper.map(operation, new TopicAuthorizationException("x")).isInstanceOf[ApplicationError]
    )
    assert(
      KafkaErrorMapper.map("describeCluster", new TimeoutException()).isInstanceOf[InfrastructureError]
    )
  }

  test("reconnectClassMatchesTheInvalidationPredicate") {
    // In both directions, so the mapper and the pool cannot drift apart.
    table.foreach { row =>
      val isReconnect = row.failureClass == FailureClass.Reconnect

      assertEquals(
        AdminInvalidation.isReconnectClass(row.failure),
        isReconnect,
        row.failure.getClass.getSimpleName
      )
    }

    val reconnectRows = table.filter(_.failureClass == FailureClass.Reconnect).map(_.failure.getClass)

    assertEquals(reconnectRows.toSet, AdminInvalidation.reconnectClasses)
  }

  test("everyMappedCodeExistsInTheErrorCodeEnum") {
    table.foreach(row => assertEquals(ErrorCode.fromWire(row.code.wire), Some(row.code), row.code.wire))
  }

  test("aTimeoutNamesTheBoundThatWasInForce") {
    val mapped = KafkaErrorMapper.map("describeLogDirs", new TimeoutException("t"), 60000L)

    assertEquals(mapped.message, "describeLogDirs did not finish within 60000ms")
  }

  /** Every exception class the research names, so that "total" can be asserted rather than hoped. */
  private val everyDocumentedException: Gen[Throwable] = Gen.oneOf(table.map(_.failure))

  property("isTotalOverEveryDocumentedException") {
    forAll(everyDocumentedException) { failure =>
      val mapped: KuiError = KafkaErrorMapper.map(operation, failure)

      assert(mapped.message.nonEmpty)
      Prop.passed
    }
  }

  property("isTotalOverArbitraryThrowables") {
    val exotic: Gen[Throwable] = Gen.oneOf(
      new RuntimeException(),
      new IllegalStateException("state"),
      new InterruptedException("interrupted"),
      new java.util.concurrent.TimeoutException("gave up"),
      new OutOfMemoryError("not really"),
      new CustomApiException("a subclass Kafka does not ship"),
      new CompletionException(new CompletionException(new ExecutionException(new TimeoutException())))
    )

    forAll(exotic) { failure =>
      val mapped = KafkaErrorMapper.map(operation, failure)

      assert(mapped.message.nonEmpty)
      // `classify` and `suppressible` have to be total too: a mapper that answers and a
      // classifier that throws is still a 500.
      val _ = KafkaErrorMapper.classify(failure)
      val _ = KafkaErrorMapper.suppressible(failure)
      Prop.passed
    }
  }

  test("anApiExceptionSubclassKafkaDoesNotShipFallsIntoTheRequestBucket") {
    val custom = new CustomApiException("vendor-specific")

    assertEquals(KafkaErrorMapper.classify(custom), FailureClass.Request)
    assertEquals(KafkaErrorMapper.map(operation, custom).code, ErrorCode.InvalidState)
  }

  test("anInterruptedExceptionIsAnUpstreamFailureAndNotAValidationError") {
    val mapped = KafkaErrorMapper.map(operation, new InterruptedException("interrupted"))

    assertEquals(mapped.code, ErrorCode.UpstreamUnavailable)
    assert(mapped.isInstanceOf[InfrastructureError])
  }

  property("unwrapIsAppliedBeforeClassification") {
    val depths: Gen[Int] = Gen.chooseNum(1, 5)

    forAll(everyDocumentedException, depths) { (failure, depth) =>
      val wrapped = (1 to depth).foldLeft(failure)((inner, index) =>
        if index % 2 == 0 then new CompletionException(inner) else new ExecutionException(inner)
      )

      assertEquals(KafkaErrorMapper.classify(wrapped), KafkaErrorMapper.classify(failure))
      assertEquals(
        KafkaErrorMapper.map(operation, wrapped),
        KafkaErrorMapper.map(operation, failure)
      )
      Prop.passed
    }
  }

  property("noMessageContainsTheOriginalThrowablesText") {
    // Kafka's own text for a SASL failure names the mechanism and the principal, and its text for
    // an invalid configuration can contain the value that was rejected — which is sometimes a
    // password. None of it may reach an HTTP response body.
    val token = "kUiS3cr3t-broker-detail"

    val withToken: Gen[Throwable] = Gen.oneOf(
      new SaslAuthenticationException(s"PLAIN authentication failed for user $token"),
      new InvalidConfigurationException(s"invalid value $token"),
      new TopicAuthorizationException(s"not authorized for $token"),
      new UnknownServerException(token),
      new RuntimeException(token)
    )

    forAll(withToken) { failure =>
      val mapped = KafkaErrorMapper.map(operation, failure)

      RedactionAssertions.assertNoLeak(mapped.message, token)
      KafkaErrorMapper
        .suppressible(failure)
        .foreach(reason => RedactionAssertions.assertNoLeak(reason.message, token))
      Prop.passed
    }
  }

  test("aTimeoutIsNotSuppressible") {
    // A partial result caused by slowness is indistinguishable from a cluster that genuinely has
    // less data, and would be rendered as fact.
    assertEquals(KafkaErrorMapper.suppressible(new TimeoutException("t")), None)
  }

  test("anUnresolvableBootstrapAddressIsUnreachableAndNotAFabricatedHttpStatus") {
    // What a typo in `bootstrapServers` actually produces. `Admin.create` does not let the
    // `ConfigException` out: `KafkaAdminClient.createInternal` wraps it in
    // `KafkaException("Failed to create new KafkaAdminClient", cause)`. A bare `ConfigException`
    // still escapes the `AdminClientConfig` constructor, so both shapes are mapped. Either used to
    // be able to fall through to the catch-all, which reports `Upstream("kafka", 502)` — rendered
    // to an operator as "kafka answered with status 502". Nothing answered.
    val raised = new ConfigException("No resolvable bootstrap urls given in bootstrap.servers")
    val wrapped = new KafkaException("Failed to create new KafkaAdminClient", raised)

    List(raised, wrapped).foreach { failure =>
      KafkaErrorMapper.map("describeCluster", failure) match {
        case InfrastructureError.Unreachable(upstream, message) =>
          assertEquals(upstream, "kafka")
          assert(
            message.contains("bootstrap.servers"),
            s"the client's own message names the setting to fix, and must survive: $message"
          )
        case other => fail(s"an unresolvable bootstrap address is unreachability, not $other")
      }
    }
  }

  test("a client that could not be built keeps the configuration error") {
    // A bad inline keystore or a missing SASL login module is raised as
    // `KafkaClientConfigurationFailure` so this mapper can still see the original code. Falling
    // through to `Upstream("kafka", 502)` throws that code away and dims the capability.
    val invalid = DomainError.InvariantViolation("the truststore is not valid base64")
    val unsupported = ApplicationError.Unsupported("AWS_MSK_IAM")

    assertEquals(
      KafkaErrorMapper.map("describeCluster", KafkaClientConfigurationFailure(invalid)),
      invalid
    )
    assertEquals(
      KafkaErrorMapper.map("describeCluster", KafkaClientConfigurationFailure(unsupported)),
      unsupported
    )
  }

  test("a client construction failure that is not a config exception names its class") {
    // An SSL store or JAAS failure from `Admin.create` arrives as a plain `KafkaException` whose
    // cause is not itself a `ConfigException`. The catch-all called that "kafka answered with
    // status 502" and the cluster log then had no class to look up.
    val failure = new KafkaException(
      "Failed to create new KafkaAdminClient",
      new RuntimeException("keystore password was incorrect")
    )

    KafkaErrorMapper.map("describeCluster", failure) match {
      case InfrastructureError.Unreachable(upstream, cause) =>
        assertEquals(upstream, "kafka")
        assertEquals(cause, "java.lang.RuntimeException")
      case other => fail(s"a construction failure must be unreachable, not $other")
    }
  }
}

/** An `ApiException` subclass Kafka does not ship, standing in for one a future release might. */
final class CustomApiException(message: String) extends ApiException(message)
