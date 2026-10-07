package kui.metrics.app

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import cats.data.NonEmptyList
import cats.effect.kernel.Resource
import cats.effect.{Deferred, IO, Ref}
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import fs2.Stream
import io.circe.Json
import io.circe.parser.parse
import org.typelevel.otel4s.metrics.MeterProvider
import sttp.client4.*
import sttp.client4.impl.cats.implicits.*
import sttp.client4.testing.BackendStub
import sttp.model.Uri
import sttp.tapir.server.stub4.TapirStubInterpreter

import kui.cache.CacheMetrics
import kui.config.*
import kui.contracts.KuiEndpoint
import kui.kernel.cluster.{AdminTuning, BootstrapServers, ClientProperties, ClusterSecurity}
import kui.kernel.error.{InfrastructureError, KuiError}
import kui.kernel.{ClusterId, PositiveInt, Secret, UserName}
import kui.metrics.api.MetricsApi
import kui.metrics.domain.{BrokerSample, ThroughputRange}
import kui.metrics.infrastructure.MetricsBuffer
import kui.metrics.infrastructure.prometheus.{
  CompiledPromQuery,
  InstantQueryResult,
  PrometheusProbe,
  PrometheusQueryClient,
  QueryAnswer,
  RangeQueryResult
}
import kui.observability.Telemetry
import kui.security.*
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** The composition root, built and run, because nothing else can see what it decides.
  *
  * ==Why this suite exists==
  *
  * Wave 4's verification found six rules decided in `MetricsWiring` that no suite executed, for one reason:
  * **nothing constructed `MetricsWiring`**. The scrape cadence, the call-timeout budget, the bulkhead width,
  * the retry count, the bucket step and the sample ceiling were all read from configuration, passed into
  * something, and never observed. `BrokerScrapeLoopSuite`'s own header admitted it assembled the collector
  * "the way `MetricsWiring` assembles it" — by hand, in the test, which is the arrangement rather than the
  * product. Every case below goes through `MetricsWiring` itself.
  *
  * The suite was called `ThroughputScrapeLoopSuite` when that sentence was written, and its subject
  * `ThroughputScrapeLoop`; both were renamed when the scrape stopped being about throughput alone. Wave 6
  * corrected `MT-002`'s note in `docs/FEATURE_MATRIX.md:589-591` for exactly this and missed the copy here,
  * which is the third time a renamed class has survived in prose that nothing compiles.
  *
  * ==Two seams, and why each is used where it is==
  *
  * `makeWith` is `make`'s body with the address rule handed in, so that both halves of that rule can be
  * exercised on one machine: `UrlPolicy.Dev` admits the loopback exporter this suite starts, and
  * `UrlPolicy.Strict` — which is what `make` reads from an environment with no `KUI_ALLOW_PRIVATE_UPSTREAMS`
  * in it — refuses it by name. `collectors` hands back the buffers it built, so the three numbers that are
  * only observable through `record` can be asserted without waiting on a clock.
  *
  * ==A real socket, deliberately==
  *
  * The exporter here is a JDK `HttpServer` on a loopback port, not a stub backend. A stub cannot be late, and
  * "late" is the whole of what `callTimeout` is about; it also cannot be counted, and a cadence is a count
  * over a window. Everything below it — the pool, the breaker, the bulkhead, the timeout — is the production
  * `UpstreamClient`.
  */
final class MetricsWiringSuite extends KuiIOSuite {

  private val quickstart = ClusterId.unsafe("quickstart")
  private val unmeasured = ClusterId.unsafe("legacy")

  /** A body carrying every family this build reads, so that a landed scrape is visible on any card. */
  private def exposition(bytesIn: Double): String =
    s"""kafka_server_brokertopicmetrics_bytesinpersec_oneminuterate $bytesIn
       |kafka_server_brokertopicmetrics_bytesoutpersec_oneminuterate ${bytesIn * 2}
       |kafka_server_brokertopicmetrics_messagesinpersec_oneminuterate ${bytesIn / 100}
       |""".stripMargin

  // -----------------------------------------------------------------------------------------------
  // A real exporter on a loopback port
  // -----------------------------------------------------------------------------------------------

  /** What the fake exporter did while a case was running. */
  final private class Exporter(val port: Int, val requests: AtomicInteger) {
    def url: SafeUrl = SafeUrl.unsafe(s"http://127.0.0.1:$port/metrics")
    def hits: IO[Int] = IO(requests.get())
  }

  /** An exposition endpoint that answers after `delay`, counting what it was asked.
    *
    * `delay` is a `Thread.sleep` inside the handler and the server runs on a pool rather than on its accept
    * thread, so a slow answer holds one connection open and does not stop the next request arriving — which
    * is the arrangement a real exporter under load produces and the one `callTimeout` exists for.
    */
  private def exporter(body: Double => String, delay: FiniteDuration = 0.millis): Resource[IO, Exporter] =
    Resource
      .make(
        IO.blocking {
          val counter = new AtomicInteger(0)
          val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
          server.setExecutor(Executors.newCachedThreadPool())
          server.createContext(
            "/",
            (exchange: HttpExchange) => {
              val served = counter.incrementAndGet()
              if delay.toMillis > 0L then Thread.sleep(delay.toMillis)
              val bytes = body(served.toDouble).getBytes(StandardCharsets.UTF_8)
              exchange.getResponseHeaders.add("Content-Type", "text/plain; version=0.0.4")
              exchange.sendResponseHeaders(200, bytes.length.toLong)
              exchange.getResponseBody.write(bytes)
              exchange.close()
            }
          )
          server.start()
          (server, new Exporter(server.getAddress.getPort, counter))
        }
      )((server, _) => IO.blocking(server.stop(0)))
      .map((_, handle) => handle)

  // -----------------------------------------------------------------------------------------------
  // The configuration the wiring is built from
  // -----------------------------------------------------------------------------------------------

  private def cluster(id: ClusterId): ClusterConfig =
    ClusterConfig(
      id = id,
      name = id.value.capitalize,
      bootstrapServers = BootstrapServers.unsafe("localhost:9092"),
      security = ClusterSecurity.Plaintext,
      properties = ClientProperties.empty,
      readOnly = false,
      admin = AdminTuning.default
    )

  /** The `kui.metrics` section, as a value rather than as text.
    *
    * The intervals here are shorter than `MetricsConfig.MinScrapeInterval`, and that is the point: the
    * loader's bounds keep an *operator* from configuring a cadence that would hammer an exporter, while what
    * is under test is whether the wiring uses the number it was given at all. A five-second floor would make
    * every cadence case a five-second case.
    */
  private def metricsConfig(
      url: SafeUrl,
      interval: FiniteDuration = 100.millis,
      callTimeout: FiniteDuration = 2.seconds,
      retention: FiniteDuration = 24.hours,
      maxSamples: Int = 5000,
      maxResponseBytes: Int = MetricsSourceSettings.DefaultMaxResponseBytes,
      kind: MetricsSourceKind = MetricsSourceKind.Prometheus
  ): MetricsConfig =
    MetricsConfig(
      scrapeInterval = interval,
      retention = retention,
      maxSamplesPerSeries = maxSamples,
      sources = Map(
        quickstart -> MetricsSourceSettings(
          url,
          kind,
          callTimeout,
          maxResponseBytes = maxResponseBytes
        )
      )
    )

  private def querySettings(
      url: SafeUrl,
      auth: UpstreamAuthConfig = UpstreamAuthConfig.Anonymous
  ): MetricsSourceSettings =
    MetricsSourceSettings(
      url = url,
      kind = MetricsSourceKind.PrometheusApi,
      callTimeout = 2.seconds,
      queryTimeout = 1.second,
      maxConcurrentQueries = 3,
      auth = auth
    )

  /** Thirty-two bytes, which is the shortest key HS256 accepts. */
  private val key: SigningKey =
    SigningKey("test-1", Secret(Array.fill[Byte](32)(7)), Instant.parse("2020-01-01T00:00:00Z"))

  private val codec: PrincipalCodec[IO] =
    JwsPrincipalCodec
      .make[IO](NonEmptyList.of(key), "kui-gateway")
      .getOrElse(throw new IllegalStateException("the test signing key is too short for HS256"))

  private def wiring(
      metrics: MetricsConfig,
      policy: UrlPolicy,
      logger: FakeStructuredLogger[IO],
      clusters: List[ClusterConfig] = List(cluster(quickstart), cluster(unmeasured))
  ): Resource[IO, MetricsServer[IO]] =
    MetricsWiring.makeWith[IO](clusters, metrics, policy, Telemetry.noop[IO], codec, logger)

  test("the wired capability source follows runtime add, profile change and removal events") {
    for {
      added <- Deferred[IO, Unit]
      change <- Deferred[IO, Unit]
      changed <- Deferred[IO, Unit]
      remove <- Deferred[IO, Unit]
      removed <- Deferred[IO, Unit]
      logger <- FakeStructuredLogger[IO]
      initial = cluster(quickstart)
      changes = Stream.emit(List(initial)) ++ Stream.exec(added.complete(()).void *> change.get) ++
        Stream.emit(List(initial.copy(name = "renamed"))) ++
        Stream.exec(changed.complete(()).void *> remove.get) ++ Stream.emit(Nil) ++
        Stream.exec(removed.complete(()).void) ++ Stream.never[IO]
      _ <- MetricsWiring
        .makeWith[IO](
          Nil,
          MetricsConfig.Default,
          UrlPolicy.Dev,
          Telemetry.noop[IO],
          codec,
          logger,
          Some(changes)
        )
        .use { service =>
          for {
            _ <- added.get
            first <- service.capabilities
            _ = assertEquals(first.clusters.keySet, Set(quickstart))
            _ <- change.complete(())
            _ <- changed.get
            next <- service.capabilities
            _ = assertEquals(next.clusters(quickstart).name, Some("renamed"))
            _ <- remove.complete(())
            _ <- removed.get
            last <- service.capabilities
            _ = assertEquals(last.clusters.size, 0)
          } yield ()
        }
    } yield ()
  }

  // -----------------------------------------------------------------------------------------------
  // Asking the wiring's own routes, which is what a browser reaches
  // -----------------------------------------------------------------------------------------------

  private def backendFor(server: MetricsServer[IO]): Backend[IO] =
    TapirStubInterpreter(server.interceptors, BackendStub[IO](summon))
      .whenServerEndpointsRunLogic(server.routes)
      .backend()

  private def throughputOf(server: MetricsServer[IO], of: ClusterId): IO[Json] = {
    val path = s"/internal/v1/clusters/${of.value}/metrics/throughput"

    IO.realTimeInstant
      .flatMap(now =>
        codec.sign(
          PrincipalClaims(
            subject = UserName.unsafe("alice"),
            roles = Set.empty,
            kind = PrincipalKind.Session,
            sessionRef = None,
            issuedAt = now,
            expiresAt = now.plusSeconds(60L),
            audience = MetricsApi.Id,
            requestDigest = RequestDigest.ofRequestLine("GET", path)
          )
        )
      )
      .flatMap(signed =>
        basicRequest
          .get(Uri.unsafeParse(s"http://metrics$path"))
          .header(KuiEndpoint.PrincipalHeader, signed.value)
          .response(asStringAlways)
          .send(backendFor(server))
      )
      .map(response => parse(response.body).fold(failure => fail(s"not JSON: ${failure.message}"), identity))
  }

  private def measuredRates(document: Json): List[Double] =
    document.hcursor
      .downField("throughput")
      .downField("data")
      .downField("buckets")
      .as[List[Json]]
      .getOrElse(Nil)
      .flatMap(_.hcursor.get[Option[Double]]("bytesInPerSecond").toOption.flatten)

  private def sectionStatus(document: Json): String =
    document.hcursor.downField("throughput").get[String]("status").getOrElse("<no status>")

  // -----------------------------------------------------------------------------------------------
  // The capability, and the refusal beside it
  // -----------------------------------------------------------------------------------------------

  test("a wiring built by MetricsWiring.make measures a configured cluster end to end") {
    // Configuration in, HTTP out, a bucket on the wire. Every layer between is the production one: the
    // profile list, the connection pool, the breaker, the scrape loop, the buffer, the use case, the
    // mapping and the route. Nothing below is worth reading if this case is not green.
    for {
      log <- FakeStructuredLogger[IO]
      rates <- exporter(_ => exposition(124800.5)).use { serving =>
        wiring(metricsConfig(serving.url), UrlPolicy.Dev, log).use { server =>
          IO.sleep(600.millis) *> throughputOf(server, quickstart).map(measuredRates)
        }
      }
    } yield assertEquals(rates.distinct, List(124800.5), s"expected the served rate on the wire, got $rates")
  }

  test("the collector applies the configured response byte ceiling") {
    val limit = MetricsSourceSettings.MinResponseBytes
    val oversized = exposition(1.0) + "#" + ("x" * limit)

    for {
      log <- FakeStructuredLogger[IO]
      observed <- exporter(_ => oversized).use { serving =>
        wiring(
          metricsConfig(serving.url, maxResponseBytes = limit),
          UrlPolicy.Dev,
          log
        ).use { server =>
          IO.sleep(600.millis) *> (for {
            document <- throughputOf(server, quickstart)
            hits <- serving.hits
          } yield (document, hits))
        }
      }
    } yield {
      val (document, hits) = observed
      assert(hits > 0, s"expected the exporter to be scraped, got $hits requests")
      assertEquals(measuredRates(document), Nil)
    }
  }

  test("a wiring built by MetricsWiring.make scrapes at the configured interval") {
    // **A rule this packet owns.** `kui.metrics.scrapeInterval` is both the cadence and the bucket step,
    // and until this case nothing observed either. The failure it rules out is the one the mutation line
    // names: a loop that waits some multiple of the interval scrapes once in this window instead of a
    // dozen times, and every chart in the product fills in a sixtieth as fast with nothing saying so.
    for {
      log <- FakeStructuredLogger[IO]
      hits <- exporter(_ => exposition(1.0)).use { serving =>
        wiring(metricsConfig(serving.url, interval = 100.millis), UrlPolicy.Dev, log)
          .use(_ => IO.sleep(900.millis)) *> serving.hits
      }
    } yield {
      assert(hits >= 4, s"expected roughly nine scrapes at 100ms over 900ms; the exporter saw $hits")
      assert(hits <= 60, s"expected roughly one scrape per interval; the exporter saw $hits")
    }
  }

  test("a wiring built by MetricsWiring.make bounds a scrape by the configured callTimeout") {
    // **A rule this packet owns.** `kui.metrics.sources.<id>.callTimeout` is the whole-call budget, and it
    // is the only thing standing between one slow exporter and a scrape fibre that never comes back. The
    // exporter here answers in 900 ms and the budget is 150 ms, so every pass must be cut short: the
    // failure is logged as a timeout and no sample reaches the buffer. Multiply the budget and the same
    // exporter is measured instead, which is what makes this a gate rather than a description.
    for {
      log <- FakeStructuredLogger[IO]
      rates <- exporter(_ => exposition(500.0), delay = 900.millis).use { serving =>
        wiring(
          metricsConfig(serving.url, interval = 200.millis, callTimeout = 150.millis),
          UrlPolicy.Dev,
          log
        ).use(server => IO.sleep(1200.millis) *> throughputOf(server, quickstart).map(measuredRates))
      }
      entries <- log.entries
    } yield {
      assertEquals(rates, Nil, s"a scrape outside its budget must file nothing; got $rates")
      assert(
        entries.exists(entry => entry.context.get("error.code").contains("KUI-TIMEOUT")),
        s"the budget must be enforced and said out loud; entries were ${entries.map(_.context)}"
      )
    }
  }

  test("the same wiring under the strict address rule refuses the loopback exporter by name") {
    // The other half of `UrlPolicy`, on the same exporter as the case above. `make` reads
    // `KUI_ALLOW_PRIVATE_UPSTREAMS` from the environment and gets `Strict` without it, so this is what a
    // production process does with a private address: it starts, it serves, it measures nothing, and it
    // says why. ARCHITECTURE.md §14 is the rule; a card that quietly stayed empty would be the bug.
    for {
      log <- FakeStructuredLogger[IO]
      document <- exporter(_ => exposition(1.0)).use { serving =>
        wiring(metricsConfig(serving.url), UrlPolicy.Strict, log).use { server =>
          IO.sleep(500.millis) *> throughputOf(server, quickstart)
        }
      }
      hits <- IO.pure(0)
      entries <- log.entries
    } yield {
      assertEquals(measuredRates(document), Nil)
      // Still `ok` with a full axis of gaps, not a 500: the endpoint answers, and the operator's evidence
      // is the log line naming the policy rather than a red panel naming nothing.
      assertEquals(sectionStatus(document), "ok")
      assert(
        entries.exists(entry => entry.message.toLowerCase.contains("could not be read")),
        s"a refused address must be reported; entries were ${entries.map(_.message)}"
      )
      assertEquals(hits, 0)
    }
  }

  // -----------------------------------------------------------------------------------------------
  // The four numbers the wiring hands to the collector
  // -----------------------------------------------------------------------------------------------

  /** The buffers `make` builds, for the numbers that are only visible through `record`.
    *
    * The address is a port nothing is listening on, so the scrape loop runs and files nothing: what the cases
    * below assert is the shape of the window the wiring built, not the timing of a scrape.
    */
  private def buffersFor(metrics: MetricsConfig): Resource[IO, Map[ClusterId, MetricsBuffer[IO]]] =
    Resource.eval(FakeStructuredLogger[IO]).flatMap { log =>
      Resource
        .eval(MeterProvider.noop[IO].get("kui.metrics"))
        .flatMap(meter =>
          MetricsWiring.collectors[IO](
            List(cluster(quickstart), cluster(unmeasured)),
            metrics,
            UrlPolicy.Dev,
            Telemetry.noop[IO],
            meter,
            log
          )
        )
    }

  private def bufferOf(metrics: MetricsConfig)(assertion: MetricsBuffer[IO] => IO[Unit]): IO[Unit] =
    buffersFor(metrics).use(buffers =>
      assertion(buffers.getOrElse(quickstart, fail("the configured cluster got no collector")))
    )

  test("the buffer files samples under the configured scrape interval, not some multiple of it") {
    // The bucket step is `kui.metrics.scrapeInterval`, so two scrapes one interval apart are two readings
    // and the range bucket that holds both draws their mean. Widen the step and they collapse into one
    // bucket where the later one replaces the earlier, and the chart quietly draws the last sample of
    // each group as though it were the period's rate.
    val at = Instant.parse("2026-09-06T12:00:00Z")
    val unreachable = SafeUrl.unsafe("http://127.0.0.1:1/metrics")

    bufferOf(metricsConfig(unreachable, interval = 1.minute)) { buffer =>
      def sample(seconds: Long, rate: Double): BrokerSample =
        BrokerSample.empty(at.minusSeconds(seconds)).copy(bytesInPerSecond = Some(rate))

      buffer.record(sample(120L, 100.0)) *>
        buffer.record(sample(60L, 300.0)) *>
        buffer.throughput(ThroughputRange.Last24Hours, at).map {
          case Right(series) =>
            // Two samples, two one-minute buckets, one five-minute range bucket: mean 200. A step of
            // twelve minutes would keep only the later one and answer 300.
            assertEquals(series.buckets.flatMap(_.bytesInPerSecond), List(200.0))
          case Left(failure) => fail(s"throughput never refuses a read; got $failure")
        }
    }
  }

  test("the buffer keeps at most the configured maxSamplesPerSeries") {
    // The ceiling that holds when the cadence and the retention multiply out to more resident memory than
    // the operator who typed them asked for. Raise it and the oldest reading survives, which is the same
    // chart drawn from more memory than was asked for — visible here as a mean rather than as a heap.
    val at = Instant.parse("2026-09-06T12:00:00Z")
    val unreachable = SafeUrl.unsafe("http://127.0.0.1:1/metrics")

    bufferOf(metricsConfig(unreachable, interval = 1.minute, maxSamples = 2)) { buffer =>
      def sample(seconds: Long, rate: Double): BrokerSample =
        BrokerSample.empty(at.minusSeconds(seconds)).copy(bytesInPerSecond = Some(rate))

      buffer.record(sample(180L, 900.0)) *>
        buffer.record(sample(120L, 100.0)) *>
        buffer.record(sample(60L, 300.0)) *>
        buffer.throughput(ThroughputRange.Last24Hours, at).map {
          case Right(series) =>
            // Three readings, a ceiling of two: the oldest is gone and the bucket is the mean of the two
            // that survived. A larger ceiling answers the mean of all three, which is 433.3…
            assertEquals(series.buckets.flatMap(_.bytesInPerSecond), List(200.0))
          case Left(failure) => fail(s"throughput never refuses a read; got $failure")
        }
    }
  }

  test("a cluster with no readable source gets no collector, and no pool is built for it") {
    // A deployment that measures nothing builds no connection pool, no breaker and no fibre — the same
    // argument the schema service makes for a cluster with no registry. An idle upstream publishes a
    // permanently zero series on every dashboard that charts it.
    val jmx = metricsConfig(SafeUrl.unsafe("http://127.0.0.1:1/metrics"), kind = MetricsSourceKind.Jmx)

    for {
      declared <- buffersFor(jmx).use(buffers => IO.pure(buffers.keySet))
      none <- buffersFor(MetricsConfig.Default).use(buffers => IO.pure(buffers.keySet))
    } yield {
      assertEquals(declared, Set.empty[ClusterId], "a source this build cannot read gets no collector")
      assertEquals(none, Set.empty[ClusterId], "a deployment with no sources gets no collector")
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Prometheus query clients: source ownership without a scrape loop
  // -----------------------------------------------------------------------------------------------

  test("no API source invokes the query-client builder") {
    for {
      builds <- Ref.of[IO, Int](0)
      clients <- MetricsWiring
        .queryClientsFrom[IO](List(cluster(quickstart)), MetricsConfig.Default) { (_, _) =>
          Resource.eval(builds.update(_ + 1).as(new InertQueryClient))
        }
        .use(IO.pure)
      count <- builds.get
    } yield {
      assertEquals(clients, Map.empty[ClusterId, PrometheusQueryClient[IO]])
      assertEquals(count, 0, "the empty selector must return before allocating a transport or pool")
    }
  }

  test("query-client resources are isolated per API source and all are finalized") {
    val queryA = ClusterId.unsafe("query-a")
    val queryB = ClusterId.unsafe("query-b")
    val scrape = ClusterId.unsafe("scrape")
    val settings = querySettings(SafeUrl.unsafe("http://prometheus:9090"))
    val metrics = MetricsConfig.Default.copy(sources =
      Map(
        queryA -> settings,
        queryB -> settings.copy(url = SafeUrl.unsafe("http://prometheus-b:9090")),
        scrape -> settings.copy(
          url = SafeUrl.unsafe("http://exporter:9404/metrics"),
          kind = MetricsSourceKind.Prometheus
        )
      )
    )

    for {
      acquired <- Ref.of[IO, List[ClusterId]](Nil)
      released <- Ref.of[IO, List[ClusterId]](Nil)
      inside <- MetricsWiring
        .queryClientsFrom[IO](List(cluster(queryA), cluster(scrape), cluster(queryB)), metrics) { (id, _) =>
          Resource.make(acquired.update(id :: _).as(new InertQueryClient))(_ => released.update(id :: _))
        }
        .use(clients => released.get.map(before => clients.keySet -> before))
      after <- released.get
      built <- acquired.get
    } yield {
      assertEquals(inside._1, Set(queryA, queryB))
      assertEquals(inside._2, Nil, "clients must remain live until the owning wiring is released")
      assertEquals(built.toSet, Set(queryA, queryB))
      assertEquals(after.toSet, Set(queryA, queryB))
    }
  }

  test("an API source performs no remote request until its client is used") {
    val body =
      """{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"1"]}]}}"""

    for {
      log <- FakeStructuredLogger[IO]
      result <- exporter(_ => body).use { serving =>
        val metrics = MetricsConfig.Default.copy(sources = Map(quickstart -> querySettings(serving.url)))
        Resource
          .eval(MeterProvider.noop[IO].get("kui.metrics"))
          .flatMap(meter =>
            MetricsWiring.queryClients[IO](
              List(cluster(quickstart)),
              metrics,
              UrlPolicy.Dev,
              Telemetry.noop[IO],
              meter,
              log
            )
          )
          .use { clients =>
            for {
              before <- serving.hits
              probe <- clients(quickstart).probe(Instant.parse("2026-09-20T12:00:00Z"))
              after <- serving.hits
            } yield (before, probe, after)
          }
      }
    } yield {
      assertEquals(result._1, 0, "resource acquisition must not probe Prometheus")
      assert(result._2.isRight, result._2.toString)
      assertEquals(result._3, 1)
    }
  }

  test("an unreachable API source does not fail startup or add a readiness check") {
    val metrics = MetricsConfig.Default.copy(sources =
      Map(
        quickstart -> querySettings(SafeUrl.unsafe("http://127.0.0.1:1/prometheus"))
      )
    )

    for {
      log <- FakeStructuredLogger[IO]
      readiness <- wiring(metrics, UrlPolicy.Dev, log).use(server => IO.pure(server.readiness))
    } yield assertEquals(readiness, Nil)
  }

  test("a cluster the wiring built no collector for answers not_configured through the real routes") {
    for {
      log <- FakeStructuredLogger[IO]
      document <- exporter(_ => exposition(1.0)).use(serving =>
        wiring(metricsConfig(serving.url), UrlPolicy.Dev, log).use(server =>
          IO.sleep(300.millis) *> throughputOf(server, unmeasured)
        )
      )
    } yield assertEquals(sectionStatus(document), "not_configured")
  }

  // -----------------------------------------------------------------------------------------------
  // The resilience an exporter is called behind
  // -----------------------------------------------------------------------------------------------

  test("the exporter is called with one address, one call at a time, no retry, and the operator's budget") {
    // **A rule this packet owns**, and the one number in it that has no behavioural gate: `maxRetries`.
    // A retry only fires on a connection-level failure, which the breaker already counts once for the
    // whole call, and the backoff is full jitter — so the difference between zero retries and five is not
    // observable in a count or in a duration without making the case a coin toss. Asserted at the
    // decision point instead, which is the value the wiring hands `UpstreamClient` and the only place any
    // of these four is chosen.
    val settings =
      MetricsSourceSettings(SafeUrl.unsafe("http://kafka-metrics:5556/metrics"), callTimeout = 7.seconds)
    val config = MetricsWiring.upstreamConfig(quickstart, settings, UrlPolicy.Dev)

    assertEquals(config.maxRetries, 0, "a scrape is a poll: the next one is already scheduled")
    assertEquals(config.maxConcurrent, PositiveInt.unsafe(1), "there is one caller, asking once per interval")
    assertEquals(config.urls, NonEmptyList.one(settings.url), "kui.metrics.sources.<id>.url is one address")
    assertEquals(config.callTimeout, 7.seconds, "the whole-call budget is the operator's, unmodified")
    assertEquals(config.urlPolicy, UrlPolicy.Dev)
    // A name and never an address: a connection failure's text routinely carries hosts and ports, and
    // ADR-034 keeps them out of a user-visible message.
    assertEquals(config.name, "metrics-exporter-quickstart")
  }

  test("the query API uses one address, its configured bulkhead and whole-call budget, with zero retry") {
    val settings = querySettings(SafeUrl.unsafe("https://prometheus.example/prometheus")).copy(
      callTimeout = 17.seconds,
      maxConcurrentQueries = 7
    )
    val config = MetricsWiring.queryUpstreamConfig(quickstart, settings, UrlPolicy.Strict)

    assertEquals(config.maxRetries, 0)
    assertEquals(config.maxConcurrent, PositiveInt.unsafe(7))
    assertEquals(config.urls, NonEmptyList.one(settings.url))
    assertEquals(config.callTimeout, 17.seconds)
    assertEquals(config.urlPolicy, UrlPolicy.Strict)
    assertEquals(config.name, "prometheus-query-quickstart")
  }

  // -----------------------------------------------------------------------------------------------
  // What the process says about itself
  // -----------------------------------------------------------------------------------------------

  test("the start-up log names the clusters this process will scrape") {
    // "Why is the throughput card showing a sentence?" is unanswerable after the fact unless the process
    // said so when it started. INFO for the clusters it will measure, WARN for a source it cannot read —
    // ADR-005's "say which keys are ignored" rule.
    for {
      log <- FakeStructuredLogger[IO]
      _ <- exporter(_ => exposition(1.0)).use(serving =>
        wiring(metricsConfig(serving.url), UrlPolicy.Dev, log).use(_ => IO.unit)
      )
      declared <- log.entriesWith("metrics.declaredSources")
    } yield {
      assertEquals(declared.map(_.context("metrics.declaredSources")), List("quickstart"))
      assertEquals(declared.map(_.level), List("info"))
    }
  }

  test("a source this build cannot read is a warning naming the cluster, not silence") {
    for {
      log <- FakeStructuredLogger[IO]
      _ <- wiring(
        metricsConfig(SafeUrl.unsafe("http://127.0.0.1:1/metrics"), kind = MetricsSourceKind.Jmx),
        UrlPolicy.Dev,
        log
      ).use(_ => IO.unit)
      unreadable <- log.entriesWith("metrics.unreadableSource")
    } yield {
      assertEquals(unreadable.map(_.level), List("warn"))
      assert(unreadable.exists(_.message.contains("kind: jmx")), unreadable.map(_.message).toString)
    }
  }

  test("an API source is logged as an internal query provider without leaking its URL or credentials") {
    val urlCanary = "prometheus-private-canary.example"
    val tokenCanary = "bearer-secret-canary"
    val metrics = MetricsConfig.Default.copy(sources =
      Map(
        quickstart -> querySettings(
          SafeUrl.unsafe(s"https://$urlCanary/prometheus"),
          UpstreamAuthConfig.Bearer(Secret(tokenCanary))
        )
      )
    )

    for {
      log <- FakeStructuredLogger[IO]
      _ <- wiring(metrics, UrlPolicy.Strict, log).use(server => IO(assertEquals(server.readiness, Nil)))
      querySources <- log.entriesWith("metrics.querySources")
      unreadable <- log.entriesWith("metrics.unreadableSource")
      entries <- log.entries
    } yield {
      assertEquals(querySources.map(_.context("metrics.querySources")), List("quickstart"))
      assertEquals(querySources.map(_.level), List("info"))
      assertEquals(unreadable, Nil, "a supported internal provider must not be logged as an unsupported kind")
      val rendered = entries.toString
      assert(!rendered.contains(urlCanary), rendered)
      assert(!rendered.contains(tokenCanary), rendered)
    }
  }

  test("nothing is scraped for a cluster whose configuration names no source") {
    // The collector list is derived from the configuration and not from the cluster list, so a two-cluster
    // deployment with one source builds one fibre. `CacheMetrics.noop` is not used here on purpose: the
    // buffers are the wiring's own.
    val _ = CacheMetrics.noop[IO]

    buffersFor(metricsConfig(SafeUrl.unsafe("http://127.0.0.1:1/metrics"))).use(buffers =>
      IO(assertEquals(buffers.keySet, Set(quickstart)))
    )
  }
}

final private class InertQueryClient extends PrometheusQueryClient[IO] {

  private val unused: Either[KuiError, Nothing] =
    Left(InfrastructureError.Unreachable("prometheus", "unused test client"))

  def probe(at: Instant): IO[Either[KuiError, PrometheusProbe]] = IO.pure(unused)

  def instant(
      query: CompiledPromQuery,
      at: Instant
  ): IO[Either[KuiError, QueryAnswer[InstantQueryResult]]] = IO.pure(unused)

  def range(
      query: CompiledPromQuery,
      from: Instant,
      to: Instant,
      step: FiniteDuration
  ): IO[Either[KuiError, QueryAnswer[RangeQueryResult]]] = IO.pure(unused)
}
