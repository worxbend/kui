package kui.ksql.api

import cats.effect.IO
import munit.CatsEffectSuite

import kui.contracts.capability.CapabilityState
import kui.kernel.ClusterId
import kui.kernel.error.{InfrastructureError, KuiError}
import kui.ksql.application.{ClusterKsqlSource, KsqlClient, KsqlProfileView}
import kui.ksql.domain.*
import kui.testkit.fakes.FakeStructuredLogger

/** What the gateway reads about this service, per cluster.
  *
  * The three states call for opposite behaviour in the browser — hide the row, draw it with an explanation,
  * draw it — so every case here is about a *different* state reaching the report rather than about the report
  * having a shape.
  */
final class KsqlCapabilitiesSuite extends CatsEffectSuite {

  private val configured = ClusterId.unsafe("prod-eu")
  private val readOnly = ClusterId.unsafe("prod-us")
  private val bare = ClusterId.unsafe("staging")

  final private class StubClient(answer: Either[KuiError, KsqlObjects]) extends KsqlClient[IO] {
    def objects: IO[Either[KuiError, KsqlObjects]] = IO.pure(answer)
    def destructiveTarget(name: String, kind: KsqlObjectKind): IO[Either[KuiError, Option[KsqlObject]]] =
      objects.map(_.map(_.items.filter(item => item.name == name && item.kind == kind) match {
        case item :: Nil => Some(item)
        case _ => None
      }))
    def execute(statement: KsqlStatement): IO[Either[KuiError, StatementOutcome]] =
      IO.pure(Right(StatementOutcome.Status("done", None)))
    def rows(statement: KsqlStatement): fs2.Stream[IO, Either[KuiError, QueryFrame]] = fs2.Stream.empty
  }

  /** A client that throws, which the port promises never to do. The report has to survive it anyway. */
  private val throwing: KsqlClient[IO] = new KsqlClient[IO] {
    def objects: IO[Either[KuiError, KsqlObjects]] = IO.raiseError(new RuntimeException("boom"))
    def destructiveTarget(name: String, kind: KsqlObjectKind): IO[Either[KuiError, Option[KsqlObject]]] =
      IO.raiseError(new RuntimeException("boom"))
    def execute(statement: KsqlStatement): IO[Either[KuiError, StatementOutcome]] =
      IO.pure(Right(StatementOutcome.Status("done", None)))
    def rows(statement: KsqlStatement): fs2.Stream[IO, Either[KuiError, QueryFrame]] = fs2.Stream.empty
  }

  final private class Source(clients: Map[ClusterId, KsqlClient[IO]]) extends ClusterKsqlSource[IO] {
    private val views = List(
      KsqlProfileView(configured, "Production EU", readOnly = false, configured = true),
      KsqlProfileView(readOnly, "Production US", readOnly = true, configured = true),
      KsqlProfileView(bare, "Staging", readOnly = false, configured = false)
    )

    def profileOf(id: ClusterId): IO[Either[KuiError, KsqlProfileView]] =
      IO.pure(views.find(_.cluster == id).toRight(InfrastructureError.Unreachable("x", "y"): KuiError))
    def all: IO[List[KsqlProfileView]] = IO.pure(views)
    def client(id: ClusterId): IO[Option[KsqlClient[IO]]] = IO.pure(clients.get(id))
  }

  private def report(clients: Map[ClusterId, KsqlClient[IO]]) =
    FakeStructuredLogger[IO].flatMap(logger => KsqlCapabilities.make[IO](new Source(clients), logger).report)

  private val healthy: KsqlClient[IO] = new StubClient(Right(KsqlObjects.empty))

  test("a cluster with no ksqlDB is not_configured, with no features and a sentence naming the key") {
    // `configured = false` is what the gateway folds into `NotConfigured`, and it is what makes the
    // browser hide the row rather than render an error nobody can clear.
    report(Map(configured -> healthy, readOnly -> healthy)).map { rows =>
      val row = rows(bare)

      assertEquals(row.configured, false)
      assertEquals(row.status, CapabilityState.NotConfigured.status)
      assertEquals(row.features, Nil)
      assert(clue(row.reason.getOrElse("")).contains("kui.clusters.<n>.ksql.url"))
    }
  }

  test("a ksqlDB that answered is available with every feature named by its own endpoint") {
    report(Map(configured -> healthy, readOnly -> healthy)).map { rows =>
      val row = rows(configured)

      assertEquals(row.status, CapabilityState.Available.status)
      assertEquals(row.configured, true)
      assertEquals(
        row.features.sorted,
        List("ksql.objects", "ksql.statement.execute", "ksql.statement.plan", "ksql.stream")
      )
    }
  }

  test("a read-only cluster advertises the read and nothing else") {
    // The one place the read-only decision reaches the browser before a request is made. A browser told
    // the feature was there would draw an enabled Run button whose only outcome is a refusal, and a
    // control that changes position between users is worse than a disabled one (§3.7).
    report(Map(configured -> healthy, readOnly -> healthy)).map { rows =>
      assertEquals(rows(readOnly).features, List("ksql.objects"))
      assertEquals(rows(readOnly).status, CapabilityState.Available.status)
    }
  }

  test("the stream is advertised, because it is not in the list the features are derived from") {
    // `KsqlEndpoints.all` cannot hold the push query — its body needs `fs2`, which the browser's half of
    // the contract does not link — so a feature list derived from that list alone would never tell the
    // browser the stream exists, and the screen would never open one.
    assert(KsqlCapabilities.Features.contains("ksql.stream"))
    assert(KsqlCapabilities.WriteFeatures.contains("ksql.stream"))
    assertEquals(KsqlCapabilities.featuresFor(readOnly = true), List("ksql.objects"))
  }

  test("a ksqlDB that did not answer is degraded and keeps its features") {
    // Degraded means "draw the screen and explain itself". A row that also withdrew its features would
    // make the browser hide the tab at the moment the operator most needs to see why it is empty.
    val down = new StubClient(Left(InfrastructureError.Unreachable("ksqldb", "connection refused")))

    report(Map(configured -> down, readOnly -> healthy)).map { rows =>
      val row = rows(configured)

      assertEquals(row.configured, true)
      assertNotEquals(row.status, CapabilityState.Available.status)
      assertNotEquals(row.status, CapabilityState.NotConfigured.status)
      assert(row.features.nonEmpty)
      // The error's own message, which for an unreachable upstream names the upstream and **not** the
      // connection failure's text: that text routinely contains a URL with a password in it, which is
      // why `InfrastructureError.Unreachable` keeps its cause for the log alone.
      assert(clue(row.reason.getOrElse("")).contains("ksqldb"))
    }
  }

  test("a configured cluster with no client is degraded and says it is a wiring failure") {
    // Reporting it as not configured would hide a KUI defect behind a screen that looks deliberately
    // switched off.
    report(Map(readOnly -> healthy)).map { rows =>
      assert(clue(rows(configured).reason.getOrElse("")).contains("could not build a client"))
      assertEquals(rows(configured).configured, true)
    }
  }

  test("a probe that throws is answered rather than propagated, so one cluster cannot blind the rest") {
    // A capability report that fails takes every other cluster's row down with it, at the exact moment the
    // browser needs the report most.
    report(Map(configured -> throwing, readOnly -> healthy)).map { rows =>
      assertEquals(rows.size, 3)
      assertEquals(rows(readOnly).status, CapabilityState.Available.status)
      assert(clue(rows(configured).reason.getOrElse("")).contains("failed unexpectedly"))
    }
  }

  test("every cluster is in the report, including the ones with no ksqlDB") {
    // A cluster missing from the report reads as a service that has never heard of it, which the browser
    // draws as a service being down.
    report(Map(configured -> healthy, readOnly -> healthy)).map(rows =>
      assertEquals(rows.keySet, Set(configured, readOnly, bare))
    )
  }

  test("the capability document names this service and survives a failing report") {
    FakeStructuredLogger[IO].flatMap { logger =>
      val failing = new KsqlCapabilities[IO] {
        def report: IO[Map[ClusterId, kui.contracts.capability.ClusterCapability]] =
          IO.raiseError(new RuntimeException("boom"))
      }

      KsqlApi.capabilityDocument[IO](failing, logger).map { document =>
        assertEquals(document.service, KsqlApi.Id)
        // Answering "nothing is available" rather than a 500: the gateway's registry needs an answer most
        // exactly when things are wrong.
        assertEquals(document.clusters, Map.empty)
      }
    }
  }
}
