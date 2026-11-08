package kui.gateway.app

import java.net.{InetAddress, ServerSocket, Socket}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import cats.effect.IO
import cats.effect.kernel.{Clock, Ref, Resource}
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import fs2.Stream
import sttp.capabilities.fs2.Fs2Streams
import sttp.tapir.{Endpoint, PublicEndpoint}

import kui.config.{GatewayConfig, ServerConfig, UpstreamServiceConfig, UrlPolicy}
import kui.contracts.ErrorEnvelope
import kui.gateway.application.client.{CallContext, ServiceClient, ServiceClients}
import kui.gateway.application.session.SessionConfig
import kui.gateway.contract.dto.TopicOverviewDto
import kui.http.health.HealthEndpoints
import kui.http.sse.SseEvent
import kui.http.upstream.CircuitEvent
import kui.kernel.error.{InfrastructureError, KuiError}
import kui.kernel.{CorrelationId, Host, Port, PositiveInt, ServiceId}
import kui.observability.Telemetry
import kui.security.{Principal, SignedPrincipal}
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** That the gateway can always be built, whatever the rest of the system is doing.
  *
  * This is the one property of the composition root that has to hold at three in the morning. The gateway is
  * Core tier (PLAN §15): when it is down, the browser has nowhere to go and shows the single full-screen
  * "cannot reach gateway" page, so a gateway that refused to start because some service was unreachable would
  * turn one service's outage into a total blackout — at exactly the moment an operator most needs a working
  * UI to find out what is wrong.
  *
  * AIO-001 extends this suite for the all-in-one process, which wires the same function.
  */
final class GatewayWiringSuite extends KuiIOSuite {

  /** A configuration naming two services that certainly do not answer: port 1 on the loopback interface is
    * privileged and nothing in a test environment is listening on it.
    */
  private val unreachableUpstreams: GatewayServiceConfig = {
    val nowhere = UpstreamServiceConfig(
      url = kui.config.SafeUrl.unsafe("http://127.0.0.1:1"),
      timeout = 1.second,
      maxConcurrent = PositiveInt.unsafe(1)
    )

    GatewayServiceConfig.Default.copy(
      server = ServerConfig(Host.unsafe("localhost"), Port.unsafe(0), "/"),
      gateway = GatewayConfig.Default.copy(
        services = Map(ServiceId.unsafe("cluster") -> nowhere, ServiceId.unsafe("topic") -> nowhere)
      )
    )
  }

  /** The same configuration with every scheduled poll pushed far outside the test.
    *
    * `ReadinessPoller` jitters each service's first poll uniformly across one interval, so with the shipped
    * ten seconds a scheduled poll can land inside a case that is measuring what *acquisition* did. An hour
    * makes acquisition the only thing that could have contacted anybody, which is what the two
    * contacts-nothing-while-wiring cases below need in order to mean one thing rather than two.
    */
  private val idleUpstreams: GatewayServiceConfig =
    unreachableUpstreams.copy(gateway = unreachableUpstreams.gateway.copy(readinessInterval = 1.hour))

  /** A listening socket that counts every connection anybody makes to it.
    *
    * The counting is the point, and it is what the previous shape of this suite did not do. That version
    * bound a socket, wired the gateway, and then called `accept()` once with a 250 ms timeout, reasoning that
    * a connection opened during wiring would still be sitting in the backlog. It is not: a client that
    * connects and then closes — which is what a probe whose call has already failed does — sends a FIN or an
    * RST, and Linux drops such a connection from the accept queue before anybody accepts it. The mutation
    * this suite is named for could therefore be applied with that case still green, which wave 6 measured
    * rather than argued.
    *
    * A thread accepting in a loop from the moment the socket is bound has no such window: a connection is
    * counted when it is accepted, and every accepted socket is held open until release, so nothing the
    * counter has already seen can disappear again.
    */
  final private class CountingListener(val socket: ServerSocket) {
    private val counter = new AtomicInteger(0)
    private val accepted = new ConcurrentLinkedQueue[Socket]()

    private val acceptor: Thread = {
      // The only `// format: off` in the repository, and TD-027 is why. scalafmt's fixed point for
      // this call packs the thread name onto the line that closes the `catch` — `}, "name"` — and
      // scalameta, which is the parser `./mill __.fix` uses, rejects that after a *braceless* `try`
      // even though scalac under `-no-indent` compiles it. The two gates only ever read production
      // sources before this wave, and no production source writes a braceless `try` as a non-final
      // argument, so the disagreement had nowhere to show up. Adding braces does not help:
      // `rewrite.rules = [RedundantBraces]` takes them straight back out. Left in the author's
      // shape, which both tools accept.
      // format: off
      val thread = new Thread(
        () =>
          try
            while !socket.isClosed do {
              val connection = socket.accept()
              accepted.add(connection)
              val _ = counter.incrementAndGet()
            }
          catch {
            // Closing the socket is how this thread is asked to stop, so anything thrown after that is
            // the ordinary shutdown path rather than a failure worth reporting.
            case _: Throwable => ()
          },
        "gateway-wiring-acceptor"
      )
      // format: on
      thread.setDaemon(true)
      thread
    }

    def start(): Unit = acceptor.start()

    def connections: IO[Int] = IO(counter.get())

    /** Waits for the first connection, or gives up and reports what it saw. */
    def awaitConnection(within: FiniteDuration): IO[Int] = {
      def loop: IO[Int] =
        connections.flatMap(seen => if seen > 0 then IO.pure(seen) else IO.sleep(25.milliseconds) *> loop)

      loop.timeoutTo(within, connections)
    }

    def close(): Unit = {
      socket.close()
      accepted.forEach(connection => connection.close())
    }
  }

  private def listener: Resource[IO, CountingListener] =
    Resource.make(IO {
      val counting = new CountingListener(new ServerSocket(0, 64, InetAddress.getLoopbackAddress))
      counting.start()
      counting
    })(counting => IO(counting.close()))

  /** The same two-service configuration, aimed at a port something really is listening on.
    *
    * `UrlPolicy.Dev` is not decoration. The address is `127.0.0.1`, `SafeUrl` treats an address *literal* as
    * loopback, and `ResilientBackend` re-applies the policy to every request — so under the shipped strict
    * default every call here is refused before a socket is opened, which is what
    * `aStrictDeploymentNeverConnectsToALoopbackUpstream` asserts on purpose. A deployment that really does
    * point the gateway at a local process sets `KUI_ALLOW_PRIVATE_UPSTREAMS=true`, and this is that
    * deployment.
    */
  private def pointedAt(
      server: ServerSocket,
      readinessInterval: FiniteDuration = 1.hour,
      policy: UrlPolicy = UrlPolicy.Dev
  ): GatewayServiceConfig = {
    val there = UpstreamServiceConfig(
      url = kui.config.SafeUrl.unsafe(s"http://127.0.0.1:${server.getLocalPort}"),
      timeout = 1.second,
      maxConcurrent = PositiveInt.unsafe(1)
    )

    unreachableUpstreams.copy(
      gateway = unreachableUpstreams.gateway.copy(
        services = Map(ServiceId.unsafe("cluster") -> there, ServiceId.unsafe("topic") -> there),
        readinessInterval = readinessInterval
      ),
      urlPolicy = policy
    )
  }

  /** Builds the gateway and releases it again, which is what a case that only reads the assembled value
    * wants: no poller fiber outlives the assertion.
    */
  private def wire(config: GatewayServiceConfig) = wiring(config).use(IO.pure)

  /** The gateway, held open, for the cases whose subject is what it does *after* it has started. */
  private def wiring(config: GatewayServiceConfig): Resource[IO, GatewayServer[IO]] =
    Resource
      .eval(FakeStructuredLogger[IO])
      .flatMap(logger => GatewayWiring.make[IO](config, Telemetry.noop[IO], logger))

  /** The context a poll travels under: nobody's behalf, one correlation id, no cluster. */
  private val pollContext: CallContext =
    CallContext(Principal.Anonymous, CorrelationId.unsafe("00000000-0000-4000-8000-000000000042"), None)

  /** A client that records every call it is asked to make and reaches nothing.
    *
    * `GatewayWiring.over` is handed its clients and constructs none of its own, so this port is the only way
    * the composition root can reach an upstream at all. A recording of it taken the instant acquisition
    * returns is therefore a complete account of what acquisition asked any upstream for.
    */
  private def recordingClient(calls: Ref[IO, List[String]], id: ServiceId): ServiceClient[IO] =
    new ServiceClient[IO] {
      val service: ServiceId = id
      def circuitStates: Stream[IO, CircuitEvent] = Stream.empty

      private def record[O](what: String): IO[Either[KuiError, O]] =
        calls
          .update(_ :+ s"${id.value} $what")
          .as(Left(InfrastructureError.Unreachable(id.value, "nothing is listening")))

      def call[I, O](endpoint: Endpoint[SignedPrincipal, I, ErrorEnvelope, O, Any], input: I)(
          ctx: CallContext
      ): IO[Either[KuiError, O]] = record(endpoint.info.name.getOrElse("<unnamed>"))

      def callPublic[I, O](endpoint: PublicEndpoint[I, ErrorEnvelope, O, Any], input: I)(
          ctx: CallContext
      ): IO[Either[KuiError, O]] = record(endpoint.info.name.getOrElse("<unnamed>"))

      def stream[I](
          endpoint: Endpoint[SignedPrincipal, I, ErrorEnvelope, Stream[IO, Byte], Fs2Streams[IO]],
          input: I
      )(ctx: CallContext): Stream[IO, SseEvent] = Stream.empty
    }

  test("the sweep fiber removes an expired session before it can evict a live one") {
    // Nothing used to call `SessionStore.sweep`. An absolutely expired session that was still the most
    // recently touched entry kept its slot, and the next create evicted a different live session instead.
    // The fiber is the one `GatewayWiring.over` starts; this case drives that same resource under
    // `TestControl` so the minute between sweeps is not a real minute.
    val config = SessionConfig(
      idleTimeout = 30.minutes,
      absoluteTimeout = 2.minutes,
      maxSessions = 2,
      sweepInterval = 1.minute
    )
    val program = GatewayWiring.sweepingSessions[IO](config).use { store =>
      for {
        start <- Clock[IO].realTimeInstant
        doomed <- store.create(Principal.Anonymous, start)
        _ <- IO.sleep(90.seconds)
        createdLive <- Clock[IO].realTimeInstant
        live <- store.create(Principal.Anonymous, createdLive)
        _ <- IO.sleep(1.second)
        touched <- Clock[IO].realTimeInstant
        // Still inside doomed's absolute lifetime, so this touch makes it the newest entry.
        _ <- store.get(doomed.id, touched)
        // Sweeps fall on the minute. Doomed expires at two minutes; that tick is not strictly after the
        // expiry, and the tick a minute later is. Live was created ninety seconds in, so it is still
        // inside its own absolute lifetime then, and it has not been idle.
        _ <- IO.sleep(90.seconds)
        now <- Clock[IO].realTimeInstant
        _ <- store.create(Principal.Anonymous, now)
        retained <- store.get(live.id, now)
      } yield retained
    }

    TestControl.executeEmbed(program).map { retained =>
      assert(
        retained.isDefined,
        "the expired session was still occupying a slot, so LRU evicted the live one"
      )
    }
  }

  test("theGatewayStartsWithNothingConfigured") {
    wire(GatewayServiceConfig.Default).map { gateway =>
      assert(gateway.routes.nonEmpty, "a gateway with no configuration still serves its health endpoints")
      assert(gateway.interceptors.nonEmpty)
    }
  }

  test("theGatewayStartsWhenEveryUpstreamIsUnreachable") {
    // The gateway is Core tier, so it has to assemble against upstreams that are simply not there. This
    // case says nothing about *when* a connection is made — the upstreams are at 127.0.0.1:1, which refuses
    // instantly, so a composition root that probed every one of them would still return in milliseconds.
    // `theGatewayContactsNoUpstreamWhileWiring` is what pins the timing.
    wire(unreachableUpstreams).map { gateway =>
      assert(gateway.routes.nonEmpty)
      assert(gateway.interceptors.nonEmpty)
    }
  }

  test("theGatewayContactsNoUpstreamWhileWiring") {
    // The rule: nothing in the composition root reaches an upstream during `Resource` acquisition. A
    // gateway that probed every routed service as it assembled would take the slowest upstream's timeout to
    // start and would fail to start at all on the day one of them hangs, turning one optional service's
    // outage into the total blackout the tier model exists to prevent.
    //
    // It is asserted at the port the composition root would have to use. `over` is handed its clients and
    // builds none, so every possible contact with an upstream — a readiness probe, a capability read, a
    // proxied call — passes through `ServiceClient`.
    //
    // Mutation this case is written for, applied and measured: `Resource.eval(registry.attachProbe(
    // trigger.probe))` in `GatewayWiring.over` becomes
    // `Resource.eval(registry.attachProbe(trigger.probe) *> routed.traverse_(trigger.probe))`. Acquisition
    // then records two calls per service — `/health/ready` and `/capabilities` — and this fails.
    Ref.of[IO, List[String]](Nil).flatMap { calls =>
      val services = List(ServiceId.unsafe("cluster"), ServiceId.unsafe("topic"))
      val wired = ServiceClients.of[IO](services.map(recordingClient(calls, _)))

      FakeStructuredLogger[IO].flatMap { logger =>
        GatewayWiring
          .over[IO](idleUpstreams, Telemetry.noop[IO], logger, Resource.pure(wired))
          .use(gateway =>
            for {
              duringAcquire <- calls.get
              // And the recorder is not inert: the same clients, called once each, are seen. A recorder
              // that recorded nothing would satisfy the assertion above whatever the composition root did.
              _ <- wired.all.traverse_(_.callPublic(HealthEndpoints.ready, ())(pollContext))
              afterwards <- calls.get
            } yield {
              assert(gateway.routes.nonEmpty)
              assertEquals(
                duringAcquire,
                Nil,
                "the composition root called an upstream while the gateway was being assembled"
              )
              assertEquals(afterwards.size, services.size, afterwards.toString)
            }
          )
      }
    }
  }

  test("a configured alerts service mounts its public stream relay") {
    Ref.of[IO, List[String]](Nil).flatMap { calls =>
      val wired = ServiceClients.of[IO](List(recordingClient(calls, GatewayWiring.AlertsServiceId)))

      FakeStructuredLogger[IO].flatMap { logger =>
        GatewayWiring
          .over[IO](idleUpstreams, Telemetry.noop[IO], logger, Resource.pure(wired))
          .use(gateway =>
            IO {
              val routes = gateway.routes.filter(_.endpoint.info.name.contains("alerts.stream"))

              assertEquals(routes.size, 1)
              assertEquals(
                routes.head.endpoint.showPathTemplate(showQueryParam = None),
                "/api/v1/clusters/{clusterId}/alerts/stream"
              )
            }
          )
      }
    }
  }

  test("noConnectionReachesAnUpstreamSocketWhileTheGatewayIsBeingAssembled") {
    // The same rule one layer down, at a real socket rather than at the client port — because the port
    // assertion is only as good as the claim that the port is the only way out, and this one holds even if
    // some future composition root opens a connection of its own. The counter is proved able to move by a
    // connection this case makes itself, so `0` is a measurement rather than a broken instrument.
    //
    // The gateway is released before anything is counted, so no scheduled poll can outlive the assertion.
    listener.use { counting =>
      for {
        gateway <- wire(pointedAt(counting.socket))
        // The accept loop runs on its own thread, so a connection made during acquisition is counted a
        // moment after acquisition returns rather than at the same instant.
        _ <- IO.sleep(300.milliseconds)
        duringAcquire <- counting.connections
        _ <- IO(new Socket(InetAddress.getLoopbackAddress, counting.socket.getLocalPort).close())
        _ <- IO.sleep(300.milliseconds)
        afterwards <- counting.connections
      } yield {
        assert(gateway.routes.nonEmpty)
        assertEquals(duringAcquire, 0, "the composition root opened a connection to an upstream while wiring")
        assertEquals(afterwards, 1, "the accept loop did not see a connection that was certainly made")
      }
    }
  }

  test("theReadinessPollerContactsAnUpstreamOnceTheGatewayIsRunning") {
    // The half that makes the two above mean something. A composition root that never contacted an upstream
    // at all would satisfy "nothing was contacted while wiring" perfectly and would leave every capability
    // at `Unknown` for ever, which is the failure GW-003 exists to prevent. Same socket, same wiring, an
    // interval short enough that the first jittered poll lands inside the test: a connection arrives.
    //
    // *An* upstream and not *every* upstream, deliberately. Both configured services point at this one
    // socket and the process shares one HTTP backend, so two polls may legitimately reuse one pooled
    // connection; asserting two would be asserting a property of the connection pool. That each service is
    // polled is `ReadinessPollerSuite`'s subject, over a client it can count calls on.
    listener.use { counting =>
      wiring(pointedAt(counting.socket, readinessInterval = 2.seconds)).use { gateway =>
        counting.awaitConnection(within = 20.seconds).map { seen =>
          assert(gateway.routes.nonEmpty)
          assert(seen > 0, "the readiness poller never contacted the upstream it was configured with")
        }
      }
    }
  }

  test("aStrictDeploymentNeverConnectsToALoopbackUpstream") {
    // The address rule, at the socket, in the direction that protects something. `UrlPolicy.Strict` is the
    // shipped default and refuses loopback, so the poller must not reach this listener at all — not once,
    // not after a redirect. The case beside it is the same wiring under the policy an operator has to set
    // deliberately, so neither of the two can be satisfied by a gateway that simply never calls anybody.
    //
    // This pair is why `GatewayServiceConfig` carries the policy at all. Until wave 6 the client re-derived
    // it as `UpstreamConfig`'s strict default whatever the loader had decided, so a gateway configured with
    // `KUI_ALLOW_PRIVATE_UPSTREAMS=true` accepted `http://localhost:8081` at start-up and then answered
    // `KUI-UPSTREAM-UNAVAILABLE` for every call to it, with no connection ever attempted — measured here,
    // in both directions, before the policy was threaded.
    listener.use { counting =>
      wiring(pointedAt(counting.socket, readinessInterval = 200.milliseconds, policy = UrlPolicy.Strict))
        .use { gateway =>
          for {
            _ <- IO.sleep(3.seconds)
            seen <- counting.connections
          } yield {
            assert(gateway.routes.nonEmpty)
            assertEquals(seen, 0, "a strict deployment opened a connection to a loopback upstream")
          }
        }
    }
  }

  test("readinessDependsOnNoUpstream") {
    // The gateway answers `/health/ready` truthfully with every service down, because "can I serve
    // requests" is a question about this process. Reporting the services' state is the capability
    // registry's job (GW-003), and it dims a feature rather than taking the whole product out of rotation.
    wire(unreachableUpstreams).map { gateway =>
      assertEquals(gateway.readiness.map(_.name), List("process"))
    }
  }

  test("aConfiguredConsumerServiceFillsTheTopicPagesConsumersSection") {
    // The defect this catches was invisible from every layer that had a test. `TopicOverviewUseCase`
    // reports a section with no registered source as `not_configured` — correct, and the answer a fully
    // configured deployment was getting, because this composition root called it without ever passing a
    // source for `consumerGroups`. The consumer service was deployed, its capability read `available`, its
    // own endpoint answered when called directly, and the topic page's Consumers tab said the deployment
    // did not track consumer groups.
    //
    // The assertion is about the *set of sections this build can fill*, which is read from what is wired
    // rather than hard-coded, so it is the one thing that could not have been true by accident.
    val configured = unreachableUpstreams.gateway.services +
      (ServiceId.unsafe("consumer") -> unreachableUpstreams.gateway.services(ServiceId.unsafe("topic")))

    val withConsumers =
      unreachableUpstreams.copy(gateway = unreachableUpstreams.gateway.copy(services = configured))

    wire(withConsumers).map { gateway =>
      assert(
        gateway.fillableTopicSections.contains(TopicOverviewDto.ConsumerGroupsSection),
        s"the topic overview can fill ${gateway.fillableTopicSections}, " +
          "and a consumer service is configured"
      )
    }
  }

  test("aDeploymentWithNoConsumerServiceStillSaysSoRatherThanPretending") {
    // The other half, and the reason `not_configured` exists at all: a build with no consumer service must
    // report the section as one it cannot fill, so the panel says this deployment has no such thing rather
    // than showing a permanent error.
    wire(unreachableUpstreams).map { gateway =>
      assert(!gateway.fillableTopicSections.contains(TopicOverviewDto.ConsumerGroupsSection))
    }
  }

  test("corsIsOffUnlessADeploymentAsksForIt") {
    // Off by default (ADR-019): the shipped deployment serves the shell from this same origin, so there is
    // no cross-origin request to permit, and a permissive default is how a logged-in user's Kafka data
    // becomes readable by any website they happen to visit.
    for {
      plain <- wire(GatewayServiceConfig.Default)
      permissive <- wire(
        GatewayServiceConfig.Default.copy(
          gateway = GatewayConfig.Default.copy(
            cors = kui.config.CorsConfig(enabled = true, origins = List("https://example.com"))
          )
        )
      )
    } yield assertEquals(
      permissive.interceptors.size,
      plain.interceptors.size + 1,
      "enabling CORS adds exactly one interceptor, and disabling it adds none"
    )
  }
}
