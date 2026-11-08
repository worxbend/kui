package kui.gateway.api.client

import java.time.Instant

import scala.concurrent.duration.DurationInt

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*
import io.circe.Json
import io.circe.parser.decode
import munit.CatsEffectSuite
import org.typelevel.otel4s.oteljava.testkit.trace.TracesTestkit

import kui.cluster.contract.ClusterEndpoints
import kui.config.{SafeUrl, UpstreamServiceConfig, UrlPolicy}
import kui.gateway.api.client.ServiceClientFixture as Fixture
import kui.kernel.error.{ApplicationError, ErrorCode, InfrastructureError}
import kui.kernel.{BrokerId, ClusterId, Secret}
import kui.observability.{Correlation, Telemetry}
import kui.security.{JwsPrincipalCodec, PrincipalClaims, PrincipalCodec, RequestDigests, SigningKey}

/** That a call the gateway makes on another service's behalf carries exactly what ADR-020 and
  * `ARCHITECTURE.md` §5 say it carries, and that every failure comes back as a value.
  *
  * The suite is written against the *real* published endpoint of the cluster service rather than a locally
  * invented one. That is the point of GW-002: a route the gateway calls is a value the owning team wrote, so
  * a test that used its own endpoint definition would be testing a copy and would pass on the day the two
  * drifted apart.
  */
final class SttpServiceClientSuite extends CatsEffectSuite {

  /** One real published endpoint of the cluster service, driven end to end through the client.
    *
    * `getCluster` rather than the list, because it has a path parameter: a client that dropped or mangled one
    * would still pass every assertion made against a parameterless route.
    */
  private val getCluster = ClusterEndpoints.getCluster

  private val cluster = ClusterId.unsafe("prod-eu")

  private val clusterBody: Json = Json.obj(
    "cluster" -> Json.obj(
      "id" -> Json.fromString("prod-eu"),
      "name" -> Json.fromString("Production EU"),
      "readOnly" -> Json.fromBoolean(false),
      "bootstrapServers" -> Json.fromString("broker-1.example.com:9093"),
      "security" -> Json.obj(
        "protocol" -> Json.fromString("PLAINTEXT"),
        "mechanism" -> Json.Null,
        "truststoreConfigured" -> Json.fromBoolean(false),
        "keystoreConfigured" -> Json.fromBoolean(false)
      ),
      "summary" -> Json.obj(
        "status" -> Json.fromString("ok"),
        "data" -> Json.obj(
          "kafkaClusterId" -> Json.Null,
          "version" -> Json.Null,
          "controllerId" -> Json.Null,
          "controllerKind" -> Json.fromString("kraft"),
          "brokerCount" -> Json.fromInt(1),
          "onlinePartitionCount" -> Json.Null,
          "offlinePartitionCount" -> Json.Null,
          "underReplicatedPartitionCount" -> Json.Null,
          "totalDiskUsageBytes" -> Json.Null,
          "features" -> Json.arr(),
          "scrapedAt" -> Json.fromString("2026-09-03T10:11:12.000Z")
        ),
        "fetchedAt" -> Json.fromString("2026-09-03T10:11:12.000Z")
      )
    )
  )

  /** An empty but well-formed log-directories answer, for the case that only cares about the query. */
  private val logDirsBody: Json = Json.obj(
    "logDirs" -> Json.obj(
      "status" -> Json.fromString("ok"),
      "data" -> Json.arr(),
      "fetchedAt" -> Json.fromString("2026-09-03T10:11:12.000Z")
    )
  )

  private def envelope(code: ErrorCode, message: String): Json = Json.obj(
    "code" -> Json.fromString(code.wire),
    "message" -> Json.fromString(message),
    "details" -> Json.arr(),
    "correlationId" -> Json.fromString("upstreamcorrelid"),
    "timestamp" -> Json.fromString("2026-09-03T10:11:12.000Z"),
    "retryable" -> Json.fromBoolean(code.retryable)
  )

  /** Reads the claims back out of the in-process token, which renders them as plain JSON. */
  private def claimsOf(token: String): PrincipalClaims =
    decode[PrincipalClaims](token).fold(failure => fail(s"the token is not claims JSON: $failure"), identity)

  test("theAddressRuleTheCallerHoldsIsTheOneEveryRequestIsCheckedAgainst") {
    // `ResilientBackend` re-applies `UpstreamConfig.urlPolicy` to every request and to every redirect, so
    // this is not a start-up setting that has already done its work — it decides, per call, whether the
    // address may be reached at all. Until wave 6 it was left at the type's strict default here, and a
    // gateway an operator had deliberately relaxed with `KUI_ALLOW_PRIVATE_UPSTREAMS=true` accepted a
    // loopback upstream at start-up and then refused every call to it, with no connection ever attempted.
    //
    // Both directions, because the default is a fail-safe: `Strict` when nobody says otherwise is the whole
    // protection against a configured URL turning the gateway into a reader of the link-local metadata
    // address, and a default that drifted to `Dev` would be silent.
    val upstream = UpstreamServiceConfig(
      url = SafeUrl.unsafe("http://127.0.0.1:8081"),
      timeout = 1.second,
      maxConcurrent = kui.kernel.PositiveInt.unsafe(4)
    )
    val service = kui.kernel.ServiceId.unsafe("cluster")

    assertEquals(SttpServiceClient.upstreamConfig(service, upstream).urlPolicy, UrlPolicy.Strict)
    assertEquals(
      SttpServiceClient.upstreamConfig(service, upstream, UrlPolicy.Dev).urlPolicy,
      UrlPolicy.Dev
    )
    // And the two knobs an operator really does set travel with it, unchanged.
    assertEquals(SttpServiceClient.upstreamConfig(service, upstream).callTimeout, 1.second)
    assertEquals(SttpServiceClient.upstreamConfig(service, upstream).name, "cluster")
  }

  test("a signing key that is not active yet is an infrastructure error, not an exception") {
    val future = SigningKey(
      "main",
      Secret(Array.fill[Byte](32)(7)),
      Instant.now.plusSeconds(3600)
    )
    val codec = JwsPrincipalCodec
      .make[IO](NonEmptyList.one(future), "kui-gateway")
      .fold(rejected => fail(rejected.message), identity)

    Fixture.stub(ServiceBehaviour.Ok(clusterBody)).flatMap { stub =>
      kui.testkit.fakes.FakeStructuredLogger[IO].flatMap { logger =>
        SttpServiceClient
          .resource[IO](
            Fixture.Cluster,
            Fixture.config(),
            codec,
            Telemetry.noop[IO],
            logger,
            stub.backend
          )
          .use(client => client.call(getCluster, cluster)(Fixture.context()))
          .map {
            case Left(_: InfrastructureError) => ()
            case other => fail(s"expected an infrastructure error, got $other")
          }
      }
    }
  }

  test("sendsTheFourStandardHeaders") {
    // A recording tracer, because `traceparent` is only propagated when there is a real span to
    // propagate; with the no-op tracer the header would be legitimately absent and the assertion would
    // be testing nothing.
    TracesTestkit.inMemory[IO]().use { traces =>
      for {
        stub <- Fixture.stub(ServiceBehaviour.Ok(clusterBody))
        tracer <- traces.tracerProvider.get("kui.test")
        telemetry = Telemetry
          .fromProviders[IO](traces.tracerProvider, org.typelevel.otel4s.metrics.MeterProvider.noop[IO])
        _ <- kui.testkit.fakes.FakeStructuredLogger[IO].flatMap { logger =>
          SttpServiceClient
            .resource[IO](
              Fixture.Cluster,
              Fixture.config(),
              PrincipalCodec.inProcess[IO],
              telemetry,
              logger,
              stub.backend
            )
            .use(client =>
              tracer
                .span("inbound")
                .surround(
                  client.call(getCluster, cluster)(Fixture.context(Some(ClusterId.unsafe("local"))))
                )
            )
        }
        sent <- stub.sent.map(_.head)
      } yield {
        assert(sent.header("X-Kui-Principal").isDefined, "the signed principal is missing")
        assertEquals(sent.header(Correlation.HeaderName), Some("0123456789abcdef"))
        assertEquals(sent.header(SttpServiceClient.ClusterHeader), Some("local"))
        assert(
          sent.header("traceparent").exists(_.startsWith("00-")),
          s"traceparent is missing or malformed: ${sent.header("traceparent")}"
        )
      }
    }
  }

  test("omitsTheClusterHeaderWhenTheCallIsNotAboutOneCluster") {
    for {
      stub <- Fixture.stub(ServiceBehaviour.Ok(clusterBody))
      _ <- Fixture.client(Fixture.Cluster, stub).use(_.call(getCluster, cluster)(Fixture.context()))
      sent <- stub.sent.map(_.head)
    } yield assertEquals(sent.header(SttpServiceClient.ClusterHeader), None)
  }

  test("signsWithTheTargetServiceAsAudience") {
    for {
      stub <- Fixture.stub(ServiceBehaviour.Ok(clusterBody))
      _ <- Fixture
        .client(Fixture.Topic, stub, Fixture.config("http://topic:8082"))
        .use(_.call(getCluster, cluster)(Fixture.context()))
      sent <- stub.sent.map(_.head)
      claims = claimsOf(sent.header("X-Kui-Principal").getOrElse(fail("no principal header")))
    } yield {
      assertEquals(claims.audience.value, "topic")
      assertEquals(claims.subject.value, "ada")
    }
  }

  test("requestDigestCoversMethodPathAndBody") {
    for {
      stub <- Fixture.stub(ServiceBehaviour.Ok(clusterBody))
      _ <- Fixture.client(Fixture.Cluster, stub).use(_.call(getCluster, cluster)(Fixture.context()))
      sent <- stub.sent.map(_.head)
      claims = claimsOf(sent.header("X-Kui-Principal").getOrElse(fail("no principal header")))
    } yield {
      // The three fields KERN-006's `RequestDigest` is made of, taken from the request that actually
      // went out rather than from anything the gateway happened to have lying around. The receiving
      // service recomputes them from the request line it read, so a token minted for `GET
      // /internal/v1/clusters/prod-eu` cannot be replayed against `DELETE /internal/v1/topics/orders`.
      assertEquals(claims.requestDigest.method, "GET")
      assertEquals(claims.requestDigest.path, "/internal/v1/clusters/prod-eu")
      assertEquals(claims.requestDigest.bodySha256, RequestDigests.sha256Hex(Array.emptyByteArray))
      // A different body is a different digest — asserted on the digest function itself, because no
      // endpoint the cluster service publishes has a request body to vary.
      assertNotEquals(
        RequestDigests.of("POST", "/internal/v1/topics", "a".getBytes("UTF-8")),
        RequestDigests.of("POST", "/internal/v1/topics", "b".getBytes("UTF-8"))
      )
    }
  }

  test("theQueryStringIsDeliberatelyOutsideTheDigest") {
    // Recording a real constraint rather than a preference. `RequestDigest` (KERN-006, ADR-020) is
    // method, path and body — the service on the other side recomputes exactly those three — so two
    // calls that differ only in the query string share a digest. What stops a token from being reused
    // across them is its audience and its 30-second lifetime, not the digest. Widening the digest to
    // cover the query would have to be a change on both sides at once, which is why it is written down
    // here rather than fixed locally.
    for {
      stub <- Fixture.stub(ServiceBehaviour.Ok(logDirsBody))
      _ <- Fixture.client(Fixture.Cluster, stub).use { client =>
        // The same path twice, with and without the broker filter: two calls that differ only in the
        // query string.
        client.call(ClusterEndpoints.logDirs, (cluster, Some(BrokerId.unsafe(1))))(Fixture.context()) >>
          client.call(ClusterEndpoints.logDirs, (cluster, None))(Fixture.context())
      }
      sent <- stub.sent
      first = claimsOf(sent.head.header("X-Kui-Principal").getOrElse(fail("no principal header")))
      second = claimsOf(sent(1).header("X-Kui-Principal").getOrElse(fail("no principal header")))
    } yield {
      assertEquals(first.requestDigest, second.requestDigest)
      assert(sent.head.uri.contains("brokerId=1"), sent.head.uri)
      assert(!sent(1).uri.contains("brokerId"), sent(1).uri)
    }
  }

  test("mapsAnErrorEnvelopeResponseBackToTheOriginalKuiError") {
    for {
      stub <- Fixture.stub(
        ServiceBehaviour.Failure(404, envelope(ErrorCode.TopicNotFound, "topic 'orders' does not exist"))
      )
      result <- Fixture.client(Fixture.Cluster, stub).use(_.call(getCluster, cluster)(Fixture.context()))
    } yield result match {
      case Left(error: ApplicationError.Remote) =>
        assertEquals(error.code, ErrorCode.TopicNotFound)
        // The message the *service* wrote, not one the gateway re-derived: the user needs to read
        // which topic was missing.
        assertEquals(error.message, "topic 'orders' does not exist")
      case other => fail(s"expected an application error carrying the upstream code, got $other")
    }
  }

  test("doesNotDimACapabilityForABusinessError") {
    // The classification half of ADR-039 §6, asserted at the point where it is decided.
    for {
      stub <- Fixture.stub(
        ServiceBehaviour.Failure(403, envelope(ErrorCode.Forbidden, "not your cluster"))
      )
      result <- Fixture.client(Fixture.Cluster, stub).use(_.call(getCluster, cluster)(Fixture.context()))
    } yield assert(
      result.left.exists(_.isInstanceOf[ApplicationError]),
      s"a 403 must stay an application error, got $result"
    )
  }

  test("mapsATransportFailureToInfrastructureError") {
    val cases = List(
      "connection refused" -> ServiceBehaviour.Refused,
      "a 500 with no envelope" -> ServiceBehaviour.Failure(500, Json.obj("oops" -> Json.True)),
      "a timeout" -> ServiceBehaviour.Slow(30.seconds, ServiceBehaviour.Ok(clusterBody))
    )

    cases.traverse_ { (name, behaviour) =>
      for {
        stub <- Fixture.stub(behaviour)
        result <- Fixture
          .client(Fixture.Cluster, stub, Fixture.config(timeout = 200.millis))
          .use(_.call(getCluster, cluster)(Fixture.context()))
      } yield assert(
        result.left.exists(_.isInstanceOf[InfrastructureError]),
        s"$name should be an infrastructure error, got $result"
      )
    }
  }

  test("doesNotForwardTheBrowsersHeaders") {
    // Nothing of the inbound request is available to the client by construction — it is handed an
    // endpoint value and a `CallContext`, and neither can carry a cookie — so this asserts the
    // construction held: no `Cookie`, no `Authorization`, and no second principal header.
    for {
      stub <- Fixture.stub(ServiceBehaviour.Ok(clusterBody))
      _ <- Fixture.client(Fixture.Cluster, stub).use(_.call(getCluster, cluster)(Fixture.context()))
      sent <- stub.sent.map(_.head)
    } yield {
      assertEquals(sent.header("Cookie"), None)
      assertEquals(sent.header("Authorization"), None)
      assertEquals(sent.headers.count(_.is("X-Kui-Principal")), 1)
    }
  }

  test("oneSlowServiceDoesNotDelayAnother") {
    // Independent bulkheads, per PLAN §16.4: the topic service saturating its own concurrency limit
    // must not queue a call to the cluster service behind it.
    for {
      slow <- Fixture.stub(ServiceBehaviour.Slow(2.seconds, ServiceBehaviour.Ok(clusterBody)))
      quick <- Fixture.stub(ServiceBehaviour.Ok(clusterBody))
      outcome <- (
        Fixture.client(Fixture.Topic, slow, Fixture.config("http://topic:8082", maxConcurrent = 1)),
        Fixture.client(Fixture.Cluster, quick)
      ).tupled.use { (slowClient, quickClient) =>
        for {
          blocked <- slowClient.call(getCluster, cluster)(Fixture.context()).start
          fast <- quickClient.call(getCluster, cluster)(Fixture.context()).timeout(500.millis)
          _ <- blocked.cancel
        } yield fast
      }
    } yield assert(outcome.isRight, s"the fast call should have succeeded, got $outcome")
  }

  test("theInProcessClientBoundsACallSoNettyIsNeverTheOneThatAnswers") {
    // The all-in-one deployment builds its client with `over` rather than `resource`, because the service on
    // the far side is an object in this JVM and there is no connection to refuse. What that argument missed
    // is that the object then talks to Kafka, and a `AdminClient` whose broker has vanished takes its own
    // thirty-second budget to give up — while the HTTP server in front gives up after twenty and answers with
    // a bare 503: no body, no error code, no correlation id. Against the running quickstart with the broker
    // stopped, `GET /api/v1/clusters/quickstart/consumer-groups/order-fulfilment` did exactly that, and the
    // screen reported "The server sent something KUI could not read" for a switched-off broker.
    //
    // So `over` takes a call timeout, and a call that overruns it becomes the same `InfrastructureError`
    // the networked shape produces — which is what makes the two deployments answer alike (ADR-005).
    for {
      stub <- Fixture.stub(ServiceBehaviour.Slow(30.seconds, ServiceBehaviour.Ok(clusterBody)))
      client = SttpServiceClient.over[IO](
        Fixture.Cluster,
        "http://cluster.in-process",
        PrincipalCodec.inProcess[IO],
        stub.backend,
        callTimeout = 200.millis
      )
      result <- client.call(getCluster, cluster)(Fixture.context())
    } yield assertEquals(
      result.left.map(_.code),
      Left(ErrorCode.Timeout),
      s"expected a timeout error, got $result"
    )
  }

  test("returnsTheDecodedOutputOnSuccess") {
    for {
      stub <- Fixture.stub(ServiceBehaviour.Ok(clusterBody))
      result <- Fixture.client(Fixture.Cluster, stub).use(_.call(getCluster, cluster)(Fixture.context()))
    } yield assertEquals(result.map(_.cluster.name), Right("Production EU"))
  }

  test("transport errors retain no exception-controlled credential text") {
    val canary = "https://user:service-secret@internal.service"

    SttpServiceClient.transportError(new java.net.ConnectException(canary)) match {
      case InfrastructureError.Unreachable(upstream, cause) =>
        assertEquals(upstream, "upstream")
        assertEquals(cause, "ConnectException")
        assert(!cause.contains("service-secret"), cause)
      case other => fail(s"expected an unreachable upstream, got $other")
    }
  }

  test("aClientBuiltThroughResourceWithNoPolicyRefusesALoopbackUpstream") {
    // `resource`'s `policy` parameter defaults to `UrlPolicy.Strict`, and that default is the SSRF guard's
    // fail-safe: a composition root that says nothing gets the safe answer rather than the convenient one.
    // Until this case existed the default could be changed to `UrlPolicy.Dev` with every gateway suite
    // green, because the only caller that exercised it — `ServiceClientFixture.client` — pointed at
    // `http://cluster:8081`, and `SafeUrl` never resolves a host *name*, so `Strict` and `Dev` decide that
    // address identically. A loopback literal is the cheapest address the two policies disagree about.
    //
    // The refusal is asserted through a real call rather than by reading the parameter back, because the
    // parameter is not where the rule bites: it is threaded into `UpstreamConfig.urlPolicy` and re-applied
    // by `ResilientBackend` to every request and every redirect.
    val loopback = Fixture.config(base = "http://127.0.0.1:8081")

    for {
      stub <- Fixture.stub(ServiceBehaviour.Ok(clusterBody))
      logger <- kui.testkit.fakes.FakeStructuredLogger[IO]
      // `SttpServiceClient.resource` is called here rather than through `ServiceClientFixture.client`,
      // deliberately. The fixture is the only other caller that omits the policy, and a later edit adding
      // an explicit `UrlPolicy.Strict` there would look harmless and would silently re-open this hole. The
      // argument list below is the assertion: there is no `policy` at the end of it.
      refused <- SttpServiceClient
        .resource[IO](
          Fixture.Cluster,
          loopback,
          PrincipalCodec.inProcess[IO],
          Telemetry.noop[IO],
          logger,
          stub.backend
        )
        .use(_.call(getCluster, cluster)(Fixture.context()))
      afterRefusal <- stub.sent
      // The other direction, so that a case which passed because *nothing* can reach the stub would fail:
      // told `Dev` explicitly, the very same address and the very same stub answer.
      allowed <- Fixture
        .clientUnder(Fixture.Cluster, stub, loopback, UrlPolicy.Dev)
        .use(_.call(getCluster, cluster)(Fixture.context()))
      afterAllowed <- stub.sent
    } yield {
      // `message` is the sentence a browser is shown — "cluster could not be reached" — so the reason is
      // read off the typed value's `cause`, which is where `ResilientBackend` puts the violation.
      val cause = refused.left.toOption.collect { case InfrastructureError.Unreachable(_, why) => why }

      assertEquals(refused.left.map(_.code), Left(ErrorCode.UpstreamUnavailable), refused.toString)
      assert(cause.exists(_.contains("refused by the URL policy")), refused.toString)
      assert(
        cause.exists(_.contains("a loopback address is only allowed in development")),
        refused.toString
      )
      // Refused before a connection is attempted: the stub records every request that reaches it, and the
      // policy check runs in front of the transport rather than after a failed dial.
      assertEquals(afterRefusal, Nil, "the refused call still reached the upstream")
      assertEquals(allowed.map(_.cluster.name), Right("Production EU"))
      assertEquals(afterAllowed.size, 1, afterAllowed.toString)
    }
  }
}
