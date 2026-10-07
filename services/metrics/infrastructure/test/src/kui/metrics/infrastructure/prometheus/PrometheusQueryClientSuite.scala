package kui.metrics.infrastructure.prometheus

import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.time.Instant

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import cats.data.NonEmptyList
import cats.effect.kernel.{Ref, Resource}
import cats.effect.testkit.TestControl
import cats.effect.{Deferred, IO}
import cats.syntax.all.*
import munit.CatsEffectSuite
import sttp.client4.Backend
import sttp.client4.httpclient.fs2.HttpClientFs2Backend
import sttp.client4.impl.cats.implicits.*
import sttp.client4.testing.{BackendStub, ResponseStub}
import sttp.model.StatusCode

import kui.config.{MetricsSourceKind, MetricsSourceSettings, SafeUrl, UpstreamAuthConfig, UrlPolicy}
import kui.http.upstream.{UpstreamClient, UpstreamConfig, UpstreamCredentials, UpstreamFailure}
import kui.kernel.error.{ErrorCode, InfrastructureError, KuiError}
import kui.kernel.{ClusterId, PositiveInt, Secret, ServiceId}
import kui.observability.Telemetry
import kui.testkit.fakes.FakeStructuredLogger

final class PrometheusQueryClientSuite extends CatsEffectSuite {

  private val at = Instant.parse("2026-09-20T12:34:56.123456789Z")
  private val query = compiled("consumer.total-lag.v1", "sum(rate(secret_metric{tenant=\"secret\"}[5m]))")

  test("probe is an authenticated bounded form POST below a reverse-proxy prefix") {
    val response = successVector(timestamp = "1789907696.123456789", value = "1")

    PrometheusTestServer.resource(PrometheusTestResponse(200, response)).use { server =>
      val settings = settingsFor(server).copy(
        auth = UpstreamAuthConfig.Basic("kui", Secret("credential-canary"))
      )

      client(settings).use(_.probe(at)).flatMap { result =>
        server.requests.map { requests =>
          assertEquals(result.map(_.sampleCount), Right(1))
          assertEquals(requests.size, 1)
          val request = requests.head
          assertEquals(request.method, "POST")
          assertEquals(request.path, "/prometheus/api/v1/query")
          assertEquals(request.header("Content-Type"), List("application/x-www-form-urlencoded"))
          assert(request.header("Authorization").headOption.exists(_.startsWith("Basic ")))
          assertEquals(
            form(request.body),
            Map(
              "query" -> "vector(1)",
              "time" -> "1789907696.123456789",
              "timeout" -> "1s",
              "limit" -> "1"
            )
          )
        }
      }
    }
  }

  test("instant preserves exact timestamps, finite zero and bounded diagnostics") {
    val response =
      s"""{"status":"success","warnings":["do not expose"],"data":{"resultType":"vector","result":[{"metric":{"topic":"orders"},"value":[1789907696.123456789,"0"]}]}}"""

    PrometheusTestServer.resource(PrometheusTestResponse(200, response)).use { server =>
      val configured = settingsFor(server)
      client(configured).use(_.instant(query, at)).flatMap { result =>
        server.requests.map { requests =>
          val answer = result.fold(error => fail(error.message), identity)
          val sample = answer.result.series.head.sample
          assertEquals(sample.at.instant, at)
          assertEquals(sample.value, SampleValue.finite(0.0).toOption.get)
          assertEquals(answer.diagnostics, QueryDiagnostics(1, 0))
          assertEquals(
            form(requests.head.body),
            Map(
              "query" -> query.expression,
              "time" -> "1789907696.123456789",
              "timeout" -> "1s",
              "limit" -> configured.maxSeriesPerQuery.toString
            )
          )
        }
      }
    }
  }

  test("range validates before I/O and posts exact start/end/step fields") {
    val from = Instant.parse("2026-09-20T12:34:00Z")
    val to = Instant.parse("2026-09-20T12:35:00Z")
    val response = successMatrix("1789907640", "1789907700")

    PrometheusTestServer.resource(PrometheusTestResponse(200, response)).use { server =>
      val configured = settingsFor(server)
      client(configured).use { queries =>
        for {
          invalid <- queries.range(query, from.plusMillis(1), to, 1.minute)
          before <- server.calls
          valid <- queries.range(query, from, to, 1.minute)
          requests <- server.requests
        } yield {
          assertEquals(invalid.left.map(_.code), Left(ErrorCode.Validation))
          assertEquals(before, 0)
          assertEquals(valid.map(_.result.series.head.samples.size), Right(2))
          assertEquals(
            form(requests.head.body),
            Map(
              "query" -> query.expression,
              "start" -> "1789907640",
              "end" -> "1789907700",
              "step" -> "60",
              "timeout" -> "1s",
              "limit" -> configured.maxSeriesPerQuery.toString
            )
          )
        }
      }
    }
  }

  test("redirects are returned as a safe upstream failure and never followed") {
    val redirect = PrometheusTestResponse(
      307,
      "redirect-body-canary",
      headers = Map("Location" -> "/credential-sink")
    )

    PrometheusTestServer.resource(redirect, PrometheusTestResponse(200, successVector())).use { server =>
      val configured = settingsFor(server).copy(
        auth = UpstreamAuthConfig.Basic("kui", Secret("redirect-credential-canary"))
      )
      client(configured).use(_.instant(query, at)).flatMap { result =>
        (server.calls, server.requests).mapN { (calls, requests) =>
          assertEquals(calls, 1)
          assert(requests.head.header("Authorization").headOption.exists(_.startsWith("Basic ")))
          assertEquals(result.left.map(_.code), Left(ErrorCode.UpstreamUnavailable))
          assertSafe(
            result,
            "redirect-body-canary",
            "redirect-credential-canary",
            query.expression,
            server.baseUrl()
          )
        }
      }
    }
  }

  test("Prometheus status outcomes map to stable safe categories without retry") {
    val cases = List(
      400 -> ErrorCode.InvalidState,
      401 -> ErrorCode.UpstreamAuth,
      403 -> ErrorCode.UpstreamAuth,
      404 -> ErrorCode.UpstreamUnavailable,
      422 -> ErrorCode.InvalidState,
      429 -> ErrorCode.UpstreamUnavailable,
      503 -> ErrorCode.Timeout,
      500 -> ErrorCode.UpstreamUnavailable
    )

    cases.foldLeft(IO.unit) { case (tested, (status, expected)) =>
      tested *> PrometheusTestServer
        .resource(PrometheusTestResponse(status, s"response-secret-$status"))
        .use { server =>
          client(settingsFor(server)).use(_.instant(query, at)).flatMap { result =>
            server.calls.map { calls =>
              assertEquals(result.left.map(_.code), Left(expected), s"status $status")
              assertEquals(calls, 1, s"status $status was retried")
              assertSafe(result, s"response-secret-$status", query.expression, server.baseUrl())
            }
          }
        }
    }
  }

  test("2xx error envelopes, malformed results and unsupported result types are safe failures") {
    val cases = List(
      """{"status":"error","errorType":"bad_data","error":"secret-expression"}""" ->
        ErrorCode.InvalidState,
      "<html>proxy secret</html>" -> ErrorCode.UpstreamUnavailable,
      """{"status":"success","data":{"resultType":"scalar","result":[1,"2"]}}""" ->
        ErrorCode.Unsupported
    )

    cases.foldLeft(IO.unit) { case (tested, (body, expected)) =>
      tested *> PrometheusTestServer.resource(PrometheusTestResponse(200, body)).use { server =>
        client(settingsFor(server)).use(_.instant(query, at)).map { result =>
          assertEquals(result.left.map(_.code), Left(expected))
          assertSafe(result, body, query.expression, server.baseUrl())
        }
      }
    }
  }

  test("chunked oversized bodies fail before decode and do not leak their contents") {
    val canary = "oversized-secret-canary"
    val body = canary * 32

    PrometheusTestServer
      .resource(PrometheusTestResponse(200, body, chunked = true))
      .use { server =>
        val configured = settingsFor(server).copy(maxResponseBytes = 64)
        client(configured).use(_.instant(query, at)).map { result =>
          assertEquals(result.left.map(_.code), Left(ErrorCode.UpstreamUnavailable))
          assertSafe(result, canary, query.expression, server.baseUrl())
        }
      }
  }

  test("socket timeout and connection refusal are typed, safe and attempted once") {
    val timeoutCase = PrometheusTestServer
      .resource(PrometheusTestResponse(200, successVector(), delay = 2.seconds))
      .use { server =>
        client(settingsFor(server, callTimeout = 250.millis)).use(_.instant(query, at)).flatMap { result =>
          server.calls.map { calls =>
            assertEquals(result.left.map(_.code), Left(ErrorCode.Timeout))
            assertEquals(calls, 1)
          }
        }
      }

    val refusedSettings = settingsFor("http://127.0.0.1:1/prometheus", 250.millis)
    timeoutCase *> client(refusedSettings).use(_.instant(query, at)).map { result =>
      assertEquals(result.left.map(_.code), Left(ErrorCode.UpstreamUnavailable))
      assertSafe(result, query.expression, refusedSettings.url.value)
    }
  }

  test(
    "resource caches canonical instant keys and records each logical caller but each physical answer once"
  ) {
    val changed = compiled("consumer.total-lag.v1", "sum(other_metric)")
    val body = successVector()
    val program = for {
      calls <- Ref.of[IO, Int](0)
      recorded <- RecordingMetrics.create
      backend = BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest
        .thenRespondF(_ => calls.update(_ + 1).as(ResponseStub.adjust(body, StatusCode.Ok)))
      configured = settingsFor("http://prometheus.example/base", 2.seconds)
      credentials = UpstreamCredentials.static[IO](configured.auth).get
      observed <- queryResource(backend, configured, credentials, recorded).use { client =>
        for {
          first <- client.instant(query, at)
          hit <- client.instant(query, at)
          digestMiss <- client.instant(changed, at)
          count <- calls.get
          events <- recorded.events
        } yield (first, hit, digestMiss, count, events)
      }
    } yield observed

    program.map { case (first, hit, digestMiss, calls, events) =>
      assert(first.isRight)
      assert(hit.isRight)
      assert(digestMiss.isRight)
      assertEquals(calls, 2)
      assertEquals(
        events.collect { case RecordedMetric.Cache(access, freshness) => (access, freshness) },
        Vector(
          QueryCacheAccess.Loaded -> QueryCacheFreshness.Fresh,
          QueryCacheAccess.Hit -> QueryCacheFreshness.Fresh,
          QueryCacheAccess.Loaded -> QueryCacheFreshness.Fresh
        )
      )
      assertEquals(
        events.collect { case RecordedMetric.Logical(outcome) => outcome },
        Vector.fill(3)(QueryOutcome.Success)
      )
      assertEquals(events.count(_.isInstanceOf[RecordedMetric.Response]), 2)
      assertEquals(events.count(_.isInstanceOf[RecordedMetric.Diagnostics]), 2)
    }
  }

  test("range calls use their independent source-local cache and physical telemetry") {
    val from = Instant.parse("2026-09-20T12:34:00Z")
    val to = Instant.parse("2026-09-20T12:35:00Z")
    val program = for {
      calls <- Ref.of[IO, Int](0)
      recorded <- RecordingMetrics.create
      backend = BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest.thenRespondF { _ =>
        calls
          .update(_ + 1)
          .as(
            ResponseStub.adjust(successMatrix("1789907640", "1789907700"), StatusCode.Ok)
          )
      }
      configured = settingsFor("http://prometheus.example/base", 2.seconds)
      credentials = UpstreamCredentials.static[IO](configured.auth).get
      observed <- queryResource(backend, configured, credentials, recorded).use { client =>
        for {
          first <- client.range(query, from, to, 1.minute)
          hit <- client.range(query, from, to, 1.minute)
          count <- calls.get
          events <- recorded.events
        } yield (first, hit, count, events)
      }
    } yield observed

    program.map { case (first, hit, calls, events) =>
      assert(first.isRight)
      assert(hit.isRight)
      assertEquals(calls, 1)
      assertEquals(
        events.collect { case RecordedMetric.Cache(access, freshness) => (access, freshness) },
        Vector(
          QueryCacheAccess.Loaded -> QueryCacheFreshness.Fresh,
          QueryCacheAccess.Hit -> QueryCacheFreshness.Fresh
        )
      )
      assertEquals(
        events.collect { case RecordedMetric.Logical(outcome) => outcome },
        Vector.fill(2)(QueryOutcome.Success)
      )
      assertEquals(events.count(_.isInstanceOf[RecordedMetric.Response]), 1)
      assertEquals(events.count(_.isInstanceOf[RecordedMetric.Diagnostics]), 1)
    }
  }

  test("concurrent callers coalesce one physical answer while retaining one logical outcome each") {
    val program = for {
      started <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      calls <- Ref.of[IO, Int](0)
      recorded <- RecordingMetrics.create
      backend = BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest.thenRespondF { _ =>
        calls.update(_ + 1) *> started.complete(()).void *>
          release.get.as(ResponseStub.adjust(successVector(), StatusCode.Ok))
      }
      configured = settingsFor("http://prometheus.example/base", 2.seconds)
      credentials = UpstreamCredentials.static[IO](configured.auth).get
      observed <- queryResource(backend, configured, credentials, recorded).use { client =>
        for {
          owner <- client.instant(query, at).start
          _ <- started.get
          waiter <- client.instant(query, at).start
          _ <- IO.sleep(1.second)
          _ <- release.complete(())
          answers <- (owner.joinWithNever, waiter.joinWithNever).tupled
          count <- calls.get
          events <- recorded.events
        } yield (answers, count, events)
      }
    } yield observed

    TestControl.executeEmbed(program).map { case ((owner, waiter), calls, events) =>
      assert(owner.isRight)
      assert(waiter.isRight)
      assertEquals(calls, 1)
      assertEquals(
        events.collect { case RecordedMetric.Logical(outcome) => outcome },
        Vector.fill(2)(QueryOutcome.Success)
      )
      assertEquals(
        events.collect { case RecordedMetric.Cache(access, freshness) => (access, freshness) }.toSet,
        Set(
          QueryCacheAccess.Loaded -> QueryCacheFreshness.Fresh,
          QueryCacheAccess.Coalesced -> QueryCacheFreshness.Fresh
        )
      )
      assertEquals(events.count(_.isInstanceOf[RecordedMetric.Response]), 1)
      assertEquals(events.count(_.isInstanceOf[RecordedMetric.Diagnostics]), 1)
    }
  }

  test("a transient refresh failure serves explicitly stale data and remains a successful logical call") {
    val program = for {
      calls <- Ref.of[IO, Int](0)
      recorded <- RecordingMetrics.create
      backend = BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest.thenRespondF { _ =>
        calls.getAndUpdate(_ + 1).map {
          case 0 => ResponseStub.adjust(successVector(), StatusCode.Ok)
          case _ => ResponseStub.adjust("temporary-secret", StatusCode.ServiceUnavailable)
        }
      }
      configured = settingsFor("http://prometheus.example/base", 2.seconds).copy(
        cacheTtl = 1.second,
        staleTtl = 10.seconds
      )
      credentials = UpstreamCredentials.static[IO](configured.auth).get
      observed <- queryResource(backend, configured, credentials, recorded).use { client =>
        for {
          first <- client.instant(query, at)
          _ <- IO.sleep(2.seconds)
          fallback <- client.instant(query, at)
          count <- calls.get
          events <- recorded.events
        } yield (first, fallback, count, events)
      }
    } yield observed

    TestControl.executeEmbed(program).map { case (first, fallback, calls, events) =>
      assert(first.isRight)
      val stale = fallback.fold(error => fail(error.message), identity)
      assert(stale.freshness.isInstanceOf[QueryFreshness.Stale])
      assertEquals(calls, 2)
      assertEquals(
        events.collect { case RecordedMetric.Logical(outcome) => outcome },
        Vector.fill(2)(QueryOutcome.Success)
      )
      assertEquals(
        events.collect { case RecordedMetric.Cache(access, freshness) => (access, freshness) },
        Vector(
          QueryCacheAccess.Loaded -> QueryCacheFreshness.Fresh,
          QueryCacheAccess.Loaded -> QueryCacheFreshness.Stale
        )
      )
      assertEquals(events.count(_.isInstanceOf[RecordedMetric.Response]), 1)
    }
  }

  test(
    "OAuth stale fallback allows only transient transport statuses and rejects contract or auth failures"
  ) {
    val cases = List(
      PrometheusTestResponse(200, "not-json") -> false,
      PrometheusTestResponse(
        200,
        "x" * (UpstreamCredentials.DefaultMaxResponseBytes.toInt + 1),
        chunked = true
      ) -> false,
      PrometheusTestResponse(400, "oauth-rejection-secret") -> false,
      PrometheusTestResponse(401, "oauth-auth-secret") -> false,
      PrometheusTestResponse(403, "oauth-auth-secret") -> false,
      PrometheusTestResponse(429, "oauth-overloaded-secret") -> true,
      PrometheusTestResponse(500, "oauth-unavailable-secret") -> true
    )

    cases.traverse_ { case (failure, staleAllowed) =>
      PrometheusTestServer
        .resource(
          PrometheusTestResponse(200, oauthToken(expiresIn = 1)),
          failure
        )
        .use { tokenServer =>
          PrometheusTestServer.resource(PrometheusTestResponse(200, successVector())).use { queryServer =>
            val configured = oauthSettings(queryServer, tokenServer).copy(
              cacheTtl = 1.millis,
              staleTtl = 10.seconds
            )
            for {
              recorded <- RecordingMetrics.create
              observed <- client(configured, recorded).use { queries =>
                for {
                  seeded <- queries.instant(query, at)
                  _ <- IO.sleep(600.millis)
                  refused <- queries.instant(query, at)
                  events <- recorded.events
                } yield (seeded, refused, events)
              }
              tokenCalls <- tokenServer.calls
              queryCalls <- queryServer.calls
            } yield {
              val (seeded, refused, events) = observed
              assert(seeded.isRight, clue((seeded, tokenCalls, queryCalls)))
              if staleAllowed then {
                val stale = refused.fold(error => fail(error.message), identity)
                assert(stale.freshness.isInstanceOf[QueryFreshness.Stale], clue(stale))
              } else assert(refused.isLeft, clue(refused))
              assertEquals(tokenCalls, 2)
              assertEquals(queryCalls, 1)
              assertEquals(
                events.collect { case RecordedMetric.Logical(outcome) => outcome },
                Vector(
                  QueryOutcome.Success,
                  if staleAllowed then QueryOutcome.Success else QueryOutcome.Failure
                )
              )
              assertEquals(
                events.collect { case RecordedMetric.Cache(access, freshness) => (access, freshness) },
                Vector(
                  QueryCacheAccess.Loaded -> QueryCacheFreshness.Fresh,
                  QueryCacheAccess.Loaded ->
                    (if staleAllowed then QueryCacheFreshness.Stale else QueryCacheFreshness.Fresh)
                )
              )
            }
          }
        }
    }
  }

  test("an OAuth transport failure may serve a seeded answer as explicitly stale") {
    PrometheusTestServer
      .resource(PrometheusTestResponse(200, oauthToken(expiresIn = 1)))
      .use { tokenServer =>
        PrometheusTestServer.resource(PrometheusTestResponse(200, successVector())).use { queryServer =>
          val configured = oauthSettings(queryServer, tokenServer).copy(
            cacheTtl = 1.millis,
            staleTtl = 10.seconds
          )
          client(configured).use { queries =>
            for {
              seeded <- queries.instant(query, at)
              _ <- IO.sleep(600.millis)
              _ <- IO.blocking(tokenServer.server.stop(0))
              fallback <- queries.instant(query, at)
              queryCalls <- queryServer.calls
            } yield {
              assert(seeded.isRight)
              val stale = fallback.fold(error => fail(error.message), identity)
              assert(stale.freshness.isInstanceOf[QueryFreshness.Stale], clue(stale))
              assertEquals(queryCalls, 1)
            }
          }
        }
      }
  }

  test("an OAuth acquisition timeout may serve a seeded answer as explicitly stale") {
    PrometheusTestServer
      .resource(
        PrometheusTestResponse(200, oauthToken(expiresIn = 1)),
        PrometheusTestResponse(200, oauthToken(), delay = 2.seconds)
      )
      .use { tokenServer =>
        PrometheusTestServer.resource(PrometheusTestResponse(200, successVector())).use { queryServer =>
          val configured = oauthSettings(queryServer, tokenServer).copy(
            // Leave enough headroom for the healthy seed request when the full repository suite is
            // competing with Kafka containers and dozens of forked JVMs. The second token response still
            // takes twice this limit, so this remains an acquisition-timeout test rather than a scheduler
            // timing test.
            callTimeout = 1.second,
            cacheTtl = 1.millis,
            staleTtl = 10.seconds
          )
          client(configured).use { queries =>
            for {
              seeded <- queries.instant(query, at)
              _ <- IO.sleep(600.millis)
              fallback <- queries.instant(query, at)
              tokenCalls <- tokenServer.calls
              queryCalls <- queryServer.calls
            } yield {
              assert(seeded.isRight)
              val stale = fallback.fold(error => fail(error.message), identity)
              assert(stale.freshness.isInstanceOf[QueryFreshness.Stale], clue(stale))
              assertEquals(tokenCalls, 2)
              assertEquals(queryCalls, 1)
            }
          }
        }
      }
  }

  test("the response byte limit accepts the exact boundary, rejects one byte over and forbids stale") {
    val limit = 512
    val exact = paddedVector(limit)
    val over = paddedVector(limit + 1)

    PrometheusTestServer
      .resource(
        PrometheusTestResponse(200, exact, chunked = true),
        PrometheusTestResponse(200, over, chunked = true)
      )
      .use { server =>
        val configured = settingsFor(server).copy(
          maxResponseBytes = limit,
          cacheTtl = 1.millis,
          staleTtl = 10.seconds
        )
        for {
          recorded <- RecordingMetrics.create
          observed <- client(configured, recorded).use { queries =>
            for {
              accepted <- queries.instant(query, at)
              _ <- IO.sleep(20.millis)
              rejected <- queries.instant(query, at)
              events <- recorded.events
            } yield (accepted, rejected, events)
          }
          calls <- server.calls
        } yield {
          val (accepted, rejected, events) = observed
          assert(accepted.isRight)
          assertEquals(rejected.left.map(_.code), Left(ErrorCode.UpstreamUnavailable))
          assertEquals(calls, 2)
          assertEquals(
            events.collect { case RecordedMetric.Limit(value) => value },
            Vector(QueryLimit.ResponseBytes)
          )
          assertEquals(events.count(_.isInstanceOf[RecordedMetric.Response]), 1)
          assertEquals(
            events.collect { case RecordedMetric.Logical(outcome) => outcome },
            Vector(QueryOutcome.Success, QueryOutcome.Failure)
          )
        }
      }
  }

  test("repeated response byte violations stay non-stale and cannot open the transport circuit") {
    val limit = 512
    val over = paddedVector(limit + 1)

    PrometheusTestServer
      .resource(
        PrometheusTestResponse(200, successVector()),
        PrometheusTestResponse(200, over, chunked = true)
      )
      .use { server =>
        val configured = settingsFor(server).copy(
          maxResponseBytes = limit,
          cacheTtl = 1.millis,
          staleTtl = 10.seconds
        )
        for {
          recorded <- RecordingMetrics.create
          observed <- client(configured, recorded).use { queries =>
            for {
              seeded <- queries.instant(query, at)
              _ <- IO.sleep(20.millis)
              rejected <- queries.instant(query, at).replicateA(6)
              events <- recorded.events
            } yield (seeded, rejected, events)
          }
          calls <- server.calls
        } yield {
          val (seeded, rejected, events) = observed
          assert(seeded.isRight)
          assert(rejected.forall(_.isLeft), clue(rejected))
          assertEquals(calls, 7)
          assertEquals(
            events.collect { case RecordedMetric.Limit(value) => value },
            Vector.fill(6)(QueryLimit.ResponseBytes)
          )
          assertEquals(
            events.collect { case RecordedMetric.Logical(outcome) => outcome },
            QueryOutcome.Success +: Vector.fill(6)(QueryOutcome.Failure)
          )
        }
      }
  }

  test("the whole call timeout includes credential acquisition and records one timeout outcome") {
    PrometheusTestServer
      .resource(PrometheusTestResponse(200, oauthToken(), delay = 2.seconds))
      .use { tokenServer =>
        PrometheusTestServer.resource(PrometheusTestResponse(200, successVector())).use { queryServer =>
          val configured = oauthSettings(queryServer, tokenServer).copy(callTimeout = 100.millis)
          for {
            recorded <- RecordingMetrics.create
            result <- client(configured, recorded).use(_.instant(query, at))
            tokenCalls <- tokenServer.calls
            queryCalls <- queryServer.calls
            events <- recorded.events
          } yield {
            assertEquals(result.left.map(_.code), Left(ErrorCode.Timeout))
            assertEquals(tokenCalls, 1)
            assertEquals(queryCalls, 0)
            assertEquals(
              events.collect { case RecordedMetric.Logical(outcome) => outcome },
              Vector(QueryOutcome.Timeout)
            )
            assertEquals(
              events.collect { case RecordedMetric.Cache(access, freshness) => (access, freshness) },
              Vector(QueryCacheAccess.Loaded -> QueryCacheFreshness.Fresh)
            )
          }
        }
      }
  }

  test("a circuit refusal records exactly one circuit-open logical outcome") {
    val program = for {
      recorded <- RecordingMetrics.create
      failure = InfrastructureError.CircuitOpen(PrometheusQueryClient.UpstreamName, Instant.EPOCH)
      backend = BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest
        .thenRespondF(_ => IO.raiseError(UpstreamFailure(failure)))
      configured = settingsFor("http://prometheus.example/base", 2.seconds)
      credentials = UpstreamCredentials.static[IO](configured.auth).get
      observed <- queryResource(backend, configured, credentials, recorded).use { client =>
        for {
          result <- client.instant(query, at)
          events <- recorded.events
        } yield (result, events)
      }
    } yield observed

    program.map { case (result, events) =>
      assert(result.left.exists(_.isInstanceOf[InfrastructureError.CircuitOpen]))
      assertEquals(
        events.collect { case RecordedMetric.Logical(outcome) => outcome },
        Vector(QueryOutcome.CircuitOpen)
      )
      assertEquals(
        events.collect { case RecordedMetric.Cache(access, freshness) => (access, freshness) },
        Vector(QueryCacheAccess.Loaded -> QueryCacheFreshness.Fresh)
      )
    }
  }

  test("resource release cancels an in-flight physical load and closes both cache resources") {
    val program = for {
      started <- Deferred[IO, Unit]
      cancelled <- Deferred[IO, Unit]
      recorded <- RecordingMetrics.create
      backend = BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest.thenRespondF { _ =>
        started.complete(()).void *> IO.never.onCancel(cancelled.complete(()).void)
      }
      configured = settingsFor("http://prometheus.example/base", 10.seconds)
      credentials = UpstreamCredentials.static[IO](configured.auth).get
      allocated <- queryResource(backend, configured, credentials, recorded).allocated
      (client, release) = allocated
      caller <- client.instant(query, at).start
      _ <- started.get
      _ <- release
      _ <- cancelled.get
      result <- caller.joinWithNever
      afterRelease <- client.instant(query, at)
    } yield (result, afterRelease)

    TestControl.executeEmbed(program).map { case (result, afterRelease) =>
      assert(result.isLeft)
      assert(afterRelease.isLeft)
    }
  }

  private def client(
      settings: MetricsSourceSettings,
      metrics: PrometheusQueryMetrics[IO] = PrometheusQueryMetrics.noop[IO]
  ): Resource[IO, PrometheusQueryClient[IO]] =
    for {
      transport <- HttpClientFs2Backend.resource[IO]()
      logger <- Resource.eval(FakeStructuredLogger[IO])
      resilient <- UpstreamClient.resource[IO](
        UpstreamConfig(
          name = PrometheusQueryClient.UpstreamName,
          urls = NonEmptyList.one(settings.url),
          callTimeout = settings.callTimeout,
          maxConcurrent = PositiveInt.unsafe(settings.maxConcurrentQueries),
          maxRetries = 0,
          urlPolicy = UrlPolicy.Dev
        ),
        transport,
        Telemetry.noop[IO],
        ServiceId.unsafe("metrics"),
        logger
      )
      // Both loopback fixture endpoints require the same explicit development policy.
      credentials <- UpstreamCredentials.resource[IO](settings.auth, policy = UrlPolicy.Dev)
      query <- PrometheusQueryClient.resource[IO](
        resilient,
        settings,
        credentials,
        ClusterId.unsafe("test"),
        metrics
      )
    } yield query

  private def queryResource(
      backend: Backend[IO],
      settings: MetricsSourceSettings,
      credentials: UpstreamCredentials[IO],
      metrics: PrometheusQueryMetrics[IO]
  ): Resource[IO, PrometheusQueryClient[IO]] =
    for {
      logger <- Resource.eval(FakeStructuredLogger[IO])
      resilient <- UpstreamClient.resource[IO](
        UpstreamConfig(
          name = PrometheusQueryClient.UpstreamName,
          urls = NonEmptyList.one(settings.url),
          callTimeout = settings.callTimeout,
          maxConcurrent = PositiveInt.unsafe(settings.maxConcurrentQueries),
          maxRetries = 0,
          urlPolicy = UrlPolicy.Dev
        ),
        backend,
        Telemetry.noop[IO],
        ServiceId.unsafe("metrics"),
        logger
      )
      query <- PrometheusQueryClient.resource[IO](
        resilient,
        settings,
        credentials,
        ClusterId.unsafe("prod"),
        metrics
      )
    } yield query

  private def settingsFor(
      server: PrometheusTestServer,
      callTimeout: FiniteDuration = 2.seconds
  ): MetricsSourceSettings =
    settingsFor(server.baseUrl(), callTimeout)

  private def settingsFor(raw: String, callTimeout: FiniteDuration): MetricsSourceSettings =
    MetricsSourceSettings(
      url = SafeUrl.from(raw, UrlPolicy.Dev).fold(error => fail(error.message), identity),
      kind = MetricsSourceKind.PrometheusApi,
      callTimeout = callTimeout,
      queryTimeout = 1.second,
      maxConcurrentQueries = 2,
      maxSeriesPerQuery = 7,
      maxPointsPerSeries = 60
    )

  private def oauthSettings(
      queryServer: PrometheusTestServer,
      tokenServer: PrometheusTestServer
  ): MetricsSourceSettings =
    settingsFor(queryServer).copy(
      auth = UpstreamAuthConfig.OAuth(
        SafeUrl
          .from(tokenServer.baseUrl("/oauth/token"), UrlPolicy.Dev)
          .fold(error => fail(error.message), identity),
        "kui",
        Secret("oauth-secret-canary"),
        None
      )
    )

  private def compiled(id: String, expression: String): CompiledPromQuery =
    QueryId
      .from(id)
      .flatMap(CompiledPromQuery.compile(_, expression))
      .fold(problem => fail(problem.toString), identity)

  private def form(body: String): Map[String, String] =
    body
      .split("&")
      .iterator
      .map(_.split("=", 2).toList)
      .collect { case key :: value :: Nil => decode(key) -> decode(value) }
      .toMap

  private def decode(value: String): String =
    URLDecoder.decode(value, StandardCharsets.UTF_8)

  private def successVector(
      labels: String = "{}",
      timestamp: String = "1789907696.123456789",
      value: String = "1"
  ): String =
    s"""{"status":"success","data":{"resultType":"vector","result":[{"metric":$labels,"value":[$timestamp,"$value"]}]}}"""

  private def successMatrix(first: String, second: String): String =
    s"""{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"topic":"orders"},"values":[[$first,"1"],[$second,"2"]]}]}}"""

  private def paddedVector(bytes: Int): String = {
    val empty =
      """{"status":"success","padding":"","data":{"resultType":"vector","result":[{"metric":{},"value":[1789907696.123456789,"1"]}]}}"""
    val padding = bytes - empty.getBytes(StandardCharsets.UTF_8).length
    require(padding >= 0, s"$bytes bytes cannot hold the fixture")
    empty.replace("\"padding\":\"\"", s"\"padding\":\"${"x" * padding}\"")
  }

  private def oauthToken(expiresIn: Int = 3600): String =
    s"""{"access_token":"token-value","expires_in":$expiresIn}"""

  private def assertSafe(result: Either[KuiError, ?], canaries: String*): Unit = {
    val rendered = result.left.toOption.fold("") { error =>
      s"${error.code.wire} ${error.message} ${error.details.mkString} $error"
    }
    canaries.foreach(canary => assert(!rendered.contains(canary), clue(rendered)))
  }

  private enum RecordedMetric {
    case Logical(outcome: QueryOutcome)
    case Response(bytes: Long, series: Long, samples: Long)
    case Cache(access: QueryCacheAccess, freshness: QueryCacheFreshness)
    case Limit(limit: QueryLimit)
    case Diagnostics(value: QueryDiagnostics)
  }

  final private class RecordingMetrics private (state: Ref[IO, Vector[RecordedMetric]])
      extends PrometheusQueryMetrics[IO] {
    def events: IO[Vector[RecordedMetric]] = state.get

    def observe[A](context: PrometheusQueryContext)(fa: IO[A])(classify: A => QueryOutcome): IO[A] =
      fa.flatTap(value => state.update(_ :+ RecordedMetric.Logical(classify(value))))

    def response(
        context: PrometheusQueryContext,
        bytes: Long,
        series: Long,
        samples: Long
    ): IO[Unit] = state.update(_ :+ RecordedMetric.Response(bytes, series, samples))

    def cache(
        context: PrometheusQueryContext,
        access: QueryCacheAccess,
        freshness: QueryCacheFreshness
    ): IO[Unit] = state.update(_ :+ RecordedMetric.Cache(access, freshness))

    def limitRejected(context: PrometheusQueryContext, limit: QueryLimit): IO[Unit] =
      state.update(_ :+ RecordedMetric.Limit(limit))

    def diagnostics(context: PrometheusQueryContext, diagnostics: QueryDiagnostics): IO[Unit] =
      state.update(_ :+ RecordedMetric.Diagnostics(diagnostics))
  }

  private object RecordingMetrics {
    def create: IO[RecordingMetrics] =
      Ref.of[IO, Vector[RecordedMetric]](Vector.empty).map(new RecordingMetrics(_))
  }
}
