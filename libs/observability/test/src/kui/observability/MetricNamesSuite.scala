package kui.observability

import java.nio.file.{Files, Path}

import munit.FunSuite

/** That the metric names in the code are the ones the documentation promises.
  *
  * The expected list below is written out in full rather than derived from `MetricNames.all`. A name that
  * changes has to be changed in two places, and the second place is a test whose diff a reviewer reads.
  *
  * ==What this suite actually reads, because the operator page used to overstate it==
  *
  * Two counterparties, and `ARCHITECTURE.md` is neither of them. The list `expected` holds is *copied from*
  * PLAN §30 and `ARCHITECTURE.md` §13 by hand; nothing here opens either file, so a §13 edit on its own
  * breaks nothing. What is read from disk is `docs/operations/observability.md`'s metric table (since wave
  * 10) and `MetricNames.scala`'s own declarations (since wave 11). `observability.md:76-78` claimed the first
  * sentence as if it were the second; it is corrected there and stated here so the two cannot drift apart
  * again.
  */
final class MetricNamesSuite extends FunSuite {

  /** Copied from the approved metric contracts, in operator-catalogue order. */
  private val expected = List(
    "kui.http.server.duration",
    "kui.upstream.duration",
    "kui.upstream.attempt.duration",
    "kui.upstream.circuit.state",
    "kui.kafka.admin.duration",
    "kui.kafka.consume.records",
    "kui.kafka.consume.bytes",
    "kui.cache.hits",
    "kui.cache.misses",
    "kui.prometheus.query.duration",
    "kui.prometheus.query.requests",
    "kui.prometheus.response.bytes",
    "kui.prometheus.response.series",
    "kui.prometheus.response.samples",
    "kui.prometheus.query.inflight",
    "kui.prometheus.query.cache.access",
    "kui.prometheus.query.coalesced",
    "kui.prometheus.query.limit.rejected",
    "kui.prometheus.query.diagnostics",
    "kui.capability.state",
    // ARCHITECTURE.md §13 additions
    "kui.stream.events",
    "kui.stream.active",
    "kui.cursor.rejected",
    "kui.principal.rejected",
    "kui.config.version",
    "kui.gateway.aggregation.section",
    "kui.cluster.profile.fetch",
    "kui.cluster.profile.subscribed",
    // M3: the serde layer, the smart filters and the masking engine
    "kui.serde.deserialize.failures",
    "kui.serde.autodetected",
    "kui.serde.serialize.failures",
    "kui.serde.registry.built",
    "kui.serde.registry.requests",
    "kui.serde.registry.up",
    "kui.filter.compile",
    "kui.filter.evaluate.duration",
    "kui.filter.errors",
    "kui.masking.applied"
  )

  test("the list matches the approved metric contracts, exactly and in order") {
    assertEquals(MetricNames.all, expected)
  }

  test("no name appears twice") {
    assertEquals(MetricNames.all.distinct, MetricNames.all)
  }

  test("every name is in the kui namespace") {
    assert(MetricNames.all.forall(_.startsWith("kui.")), MetricNames.all.toString)
  }

  test("every name is lowercase and dot-separated, so no dashboard has to guess the spelling") {
    MetricNames.all.foreach { name =>
      assertEquals(name, name.toLowerCase, clue = name)
      assert(name.matches("^[a-z0-9.]+$"), name)
    }
  }

  test("the operator's metric table names exactly the metrics this build declares") {
    // W10-04/F7, closed by W10-A2. `docs/operations/*.md` is what an operator reads to build a dashboard,
    // and not one line of it was read by anything: the verification pass deleted `masking.md` outright and
    // `./scripts/run-tests.sh` stayed green at 4,328 cases. §Metrics of `observability.md` is the half of
    // that prose which is checkable without inventing a language — it is a list of names, and this build
    // holds the same list — so it is the half that gets a gate.
    //
    // Both directions, because they fail differently and both quietly. A name in the table that this build
    // does not emit is a panel that is empty for ever and a query nobody can debug; a name this build emits
    // that the table omits is a number no operator will ever look at. Neither shows up anywhere else: the
    // suite above compares the code with a list inside the same suite, and the document was outside both.
    //
    // Precedent for a test reading a shipped document: `TopicsConfigSuite` asserts its defaults against
    // `docs/operations/configuration.md`'s table for the same reason.
    val table = documentedMetricNames

    assertEquals(
      table.diff(MetricNames.all),
      Nil,
      "docs/operations/observability.md documents a metric this build does not declare in MetricNames"
    )
    assertEquals(
      MetricNames.all.diff(table),
      Nil,
      "this build declares a metric docs/operations/observability.md's table does not name"
    )
  }

  test("the operator table pins every Prometheus-query metric to its bounded attribute set") {
    val common = List("source", "query", "operation")
    val expected = Map(
      MetricNames.PrometheusQueryDuration -> (common :+ "outcome"),
      MetricNames.PrometheusQueryRequests -> (common :+ "outcome"),
      MetricNames.PrometheusResponseBytes -> common,
      MetricNames.PrometheusResponseSeries -> common,
      MetricNames.PrometheusResponseSamples -> common,
      MetricNames.PrometheusQueryInFlight -> common,
      MetricNames.PrometheusQueryCacheAccess -> (common :+ "state"),
      MetricNames.PrometheusQueryCoalesced -> common,
      MetricNames.PrometheusQueryLimitRejected -> (common :+ "limit"),
      MetricNames.PrometheusQueryDiagnostics -> (common :+ "kind")
    )

    val documented = documentedMetricAttributes
    val actual = expected.keysIterator.map(name => name -> documented.getOrElse(name, Nil)).toMap
    assertEquals(actual, expected)
  }

  test("no metric name is declared and then left out of the list every gate reads") {
    // W10-04/F-carry-over, closed here. `kui.gateway.aggregation.section` was declared at
    // `MetricNames.scala:42`, emitted at `TopicOverviewUseCase.scala:230` and absent from `all` — so this
    // process emitted twenty-seven series while every gate that reads `all` pinned twenty-six, and the
    // suite above tolerated the gap through a named exception. A name outside `all` is a name a rename
    // cannot break, which is the one failure this whole file exists to prevent.
    //
    // Read off the declarations rather than off `all`, because reading `all` is what the previous version
    // of this file did and it agrees with whatever it is given. `MetricNames.scala` is the source of
    // truth for what is declared; `all` is a hand-maintained list beside it, and a hand-maintained list
    // is exactly the thing that drifts.
    val declared = declaredMetricNames

    assert(
      declared.sizeIs > 20,
      s"MetricNames.scala read as ${declared.size} declarations: $declared"
    )
    assertEquals(
      declared.diff(MetricNames.all),
      Nil,
      "MetricNames declares a metric name that MetricNames.all does not carry, so nothing pins it"
    )
    assertEquals(
      MetricNames.all.diff(declared),
      Nil,
      "MetricNames.all carries a name that is not declared as a constant above it"
    )
  }

  test("no attribute key appears twice, and none is a metric name by accident") {
    assertEquals(MetricNames.Attr.all.distinct, MetricNames.Attr.all)
    assertEquals(MetricNames.Attr.all.toSet.intersect(MetricNames.all.toSet), Set.empty[String])
  }

  test("an upstream outcome has one spelling, and it round-trips") {
    assertEquals(
      UpstreamOutcome.values.toList.map(_.wire),
      List("success", "client_error", "server_error", "timeout", "circuit_open", "unreachable", "canceled")
    )
    UpstreamOutcome.values.foreach { outcome =>
      assertEquals(UpstreamOutcome.fromWire(outcome.wire), Some(outcome))
    }
  }

  test("a status becomes the outcome an operator would group by") {
    assertEquals(UpstreamOutcome.ofStatus(200), UpstreamOutcome.Success)
    assertEquals(UpstreamOutcome.ofStatus(204), UpstreamOutcome.Success)
    assertEquals(UpstreamOutcome.ofStatus(404), UpstreamOutcome.ClientError)
    assertEquals(UpstreamOutcome.ofStatus(429), UpstreamOutcome.ClientError)
    assertEquals(UpstreamOutcome.ofStatus(500), UpstreamOutcome.ServerError)
    assertEquals(UpstreamOutcome.ofStatus(503), UpstreamOutcome.ServerError)
  }

  /** Every `kui.` metric name declared as a constant in `MetricNames.scala`, read off the source.
    *
    * Anchored on the assignment, `val <Name>: String = "kui...."`, which is the shape of every metric
    * declaration in that file and of nothing else in it: the attribute keys next door are bare words
    * (`"service"`, `"route"`), and the `UpstreamOutcome` wire strings are underscored and un-dotted.
    *
    * READ OVER THE FILE WITH ITS WHITESPACE FLATTENED, AND NOT LINE BY LINE. The line-anchored version of
    * this reader missed any declaration `scalafmt` wraps -- which is the shape the formatter itself produces
    * past `maxColumn = 110`, so it is not a hypothetical. Measured and filed as W11-05/V-2: a constant
    * declared over two lines and left out of `all` left this case, `observability.md`'s table check and
    * `libs.observability.checkFormat` all green at 453/453, which is the exact hole the case exists to close
    * surviving in the shape the formatter forces. A digit in the val name -- the second half of the same miss
    * -- is allowed here for the same reason.
    */
  private def declaredMetricNames: List[String] = {
    val source = Files
      .readString(resolve("libs/observability/src/kui/observability/MetricNames.scala"))
      .replaceAll("\\s+", " ")
    val declaration = "val [A-Za-z0-9]+: String = \"(kui\\.[a-z0-9.]+)\"".r

    declaration.findAllMatchIn(source).map(_.group(1)).toList
  }

  /** Every metric named in the first column of `observability.md`'s metric table, in the order printed.
    *
    * Anchored on a row whose first cell is a backticked `kui.` name, which is the shape of that table and of
    * no other table in the file: the attribute table's first column is an attribute key, and the
    * configuration table two sections down puts its backticked setting in the *second* column.
    */
  private def documentedMetricNames: List[String] = {
    val document = Files.readString(resolve("docs/operations/observability.md"))
    val row = "^\\| `(kui\\.[a-z0-9.]+)` \\|".r

    val names = document.linesIterator.flatMap(line => row.findFirstMatchIn(line).map(_.group(1))).toList

    // A reader that quietly matched nothing would make every assertion above vacuously true, which is the
    // failure mode of every document check ever written.
    assert(names.sizeIs > 20, s"observability.md's metric table read as ${names.size} rows: $names")
    names
  }

  private def documentedMetricAttributes: Map[String, List[String]] = {
    val document = Files.readString(resolve("docs/operations/observability.md"))
    val row = "^\\| `(kui\\.[a-z0-9.]+)` \\| ([^|]+) \\|".r
    val attribute = "`([a-z]+)`".r

    document.linesIterator.flatMap { line =>
      row
        .findFirstMatchIn(line)
        .map(matched => matched.group(1) -> attribute.findAllMatchIn(matched.group(2)).map(_.group(1)).toList)
    }.toMap
  }

  private def resolve(relative: String): Path = {
    val start = Path.of("").toAbsolutePath
    val root = Iterator
      .iterate(Option(start))(_.flatMap(path => Option(path.getParent)))
      .takeWhile(_.isDefined)
      .flatten
      .find(candidate => Files.exists(candidate.resolve("build.mill")))
      .getOrElse(fail(s"no build.mill above $start, so the repository root could not be found"))

    val file = root.resolve(relative)
    if Files.isRegularFile(file) then file
    else fail(s"$relative is read by this suite and does not exist")
  }
}
