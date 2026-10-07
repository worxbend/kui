package kui.filter

import scala.concurrent.duration.*

import cats.effect.{IO, Resource}
import cats.syntax.all.*

import kui.cache.CacheMetrics
import kui.kernel.ClusterId
import kui.kernel.error.{ErrorCode, KuiError}
import kui.testkit.KuiIOSuite

/** What a user is allowed to write, what happens when they write something wrong, and the two properties that
  * make a filter safe to run against a million records.
  */
final class CelFilterEngineSuite extends KuiIOSuite {

  private val cluster: ClusterId = ClusterId.unsafe("prod-eu")

  private val record: FilterableRecord = FilterableRecord(
    partition = 3,
    offset = 41892L,
    timestampMs = 1_700_000_000_000L,
    keyAsText = "order-1",
    valueAsText = """{"status":"FAILED","total":19.5,"name":{"first":"user1"},"tags":["a","b"]}""",
    headers = Map("trace-id" -> "abc", "kafka_delivery-attempt" -> "3")
  )

  /** The production default deadline is 10 milliseconds, which is the right budget for a filter that runs
    * once per record in a browse. It is the wrong budget for a test: the very first CEL evaluation in a JVM
    * pays for class loading and JIT warm-up, and on a machine running the whole build in parallel that alone
    * can exceed ten milliseconds — so the tests about what a filter *means* would intermittently observe
    * `Left(Timeout)` instead of the answer they assert. The tests that are about the deadline itself pass
    * their own `FilterLimits`, so widening the default here does not weaken them.
    */
  private val generous: FilterLimits = FilterLimits.default.copy(evaluationDeadline = 30.seconds)

  private def engine(
      limits: FilterLimits = generous
  ): Resource[IO, MessageFilterPort[IO]] =
    CelFilterEngine.resource[IO](cluster, limits, FilterMetrics.noop[IO], CacheMetrics.noop[IO])

  /** A `KuiError` is a value, not a `Throwable`, so a suite that wants the happy path has to say so. */
  private def orFail[A](fa: IO[Either[KuiError, A]]): IO[A] =
    fa.flatMap(_.fold(error => IO.raiseError(new AssertionError(error.message)), IO.pure))

  private def evaluate(source: String, against: FilterableRecord = record): IO[Either[FilterError, Boolean]] =
    engine().use { port =>
      for {
        id <- orFail(port.register(source))
        predicate <- orFail(port.predicate(id, Some(source)))
        result <- predicate.test(against)
      } yield result
    }

  // ------------------------------------------------------------------ the environment

  test("the environment exposes exactly the documented variables, in both directions") {
    // A variable in the help that does not exist would be a documented feature that fails to compile the
    // first time a user pastes it. A variable that exists and is not documented is a feature nobody can
    // find. Both are failures, so both fail the build.
    // A record whose key and value are both JSON, so that all eight fields are present at once: `key`
    // and `value` are absent by design when the payload is not JSON, which is its own test below.
    val jsonBoth = record.copy(keyAsText = """{"id":1}""")
    val fields = CelEnvironment.recordFields(jsonBoth)
    val declared = scala.jdk.CollectionConverters.SetHasAsScala(fields.keySet()).asScala.toSet
    assertEquals(declared, CelEnvironment.Variables.toSet)
    // And the other direction: nothing is exposed that the help does not list.
    assert(declared.subsetOf(CelEnvironment.Variables.toSet))
  }

  test("every documented variable compiles and evaluates") {
    CelEnvironment.Variables.traverse_ { name =>
      evaluate(s"has(record.$name) || true").map(result => assertEquals(result, Right(true), clue = name))
    }
  }

  test("the metadata fields compare as numbers, not as a type error") {
    // `record.partition == 3` is the first thing anyone writes. If CEL is handed an Integer where it expects
    // its 64-bit `int`, this fails to find an overload at run time — a comparison that looks obviously
    // correct and is rejected, which is a miserable thing to debug from a filter box.
    evaluate("record.partition == 3").assertEquals(Right(true)) >>
      evaluate("record.offset == 41892").assertEquals(Right(true)) >>
      evaluate("record.timestampMs > 1000").assertEquals(Right(true))
  }

  test("JSON values are addressable as dyn, at any depth and through arrays") {
    evaluate("record.value.status == 'FAILED'").assertEquals(Right(true)) >>
      evaluate("record.value.name.first == 'user1'").assertEquals(Right(true)) >>
      evaluate("record.value.tags[0] == 'a'").assertEquals(Right(true)) >>
      evaluate("record.value.total > 19.0").assertEquals(Right(true))
  }

  test("text and headers are addressable too") {
    evaluate("record.keyAsText == 'order-1'").assertEquals(Right(true)) >>
      evaluate("record.valueAsText.contains('FAILED')").assertEquals(Right(true)) >>
      evaluate("record.headers['trace-id'] == 'abc'").assertEquals(Right(true))
  }

  test("a non-JSON value leaves record.value unset while valueAsText stays populated") {
    val plain = record.copy(valueAsText = "not json at all")
    // Unset, not null. A filter reading `record.value.status` against a text topic gets a counted, visible
    // runtime error; with a null it would silently match nothing and the user would blame their data.
    evaluate("record.valueAsText.contains('json')", plain).assertEquals(Right(true)) >>
      evaluate("has(record.value)", plain).assertEquals(Right(false))
  }

  test("a bare JSON number is not exposed as a value either, matching the Json serde's rule") {
    val numeric = record.copy(valueAsText = "123")
    evaluate("has(record.value)", numeric).assertEquals(Right(false))
  }

  test("a JSON value over the node budget is treated as absent, not evaluated regardless") {
    // The producer controls these bytes. A wide object with more entries than the budget allows must not
    // reach the CEL evaluator at all, the same way a non-JSON payload does not — the alternative is a
    // filter on `record.value` that quietly stops erroring only for the record that is trying to hurt it.
    val wide = record.copy(valueAsText = (1 to 20).map(i => s""""f$i":$i""").mkString("{", ",", "}"))
    val tight = generous.copy(maxJsonValueNodes = 5)

    engine(tight).use { port =>
      for {
        id <- orFail(port.register("has(record.value)"))
        predicate <- orFail(port.predicate(id, Some("has(record.value)")))
        result <- predicate.test(wide)
      } yield assertEquals(result, Right(false), "a payload well over the node budget was still parsed")
    } >> // The same payload, under the production default, is nowhere near the budget and still parses.
      evaluate("has(record.value)", wide).assertEquals(Right(true))
  }

  test("a decoded JSON value over the text limit is refused before parsing") {
    val large = record.copy(valueAsText = s"""{"field":"${"x" * 100}"}""")
    val tight = generous.copy(maxJsonValueChars = 32)

    engine(tight).use { port =>
      for {
        id <- orFail(port.register("has(record.value)"))
        predicate <- orFail(port.predicate(id, Some("has(record.value)")))
        result <- predicate.test(large)
      } yield assertEquals(result, Right(false), "an over-limit payload reached the JSON parser")
    } >> evaluate("has(record.value)", large).assertEquals(Right(true))
  }

  test("a JSON value nested deeper than the walk allows is treated as absent, not a stack overflow") {
    // Depth, not width: a producer can make `[[[[...]]]]` arbitrarily deep for very little wire size, which
    // costs the walk one JVM stack frame per level rather than one unit of the node budget above.
    val depth = 5000
    val deeplyNested = record.copy(valueAsText = ("[" * depth) + "1" + ("]" * depth))
    evaluate("has(record.value)", deeplyNested).assertEquals(Right(false))
  }

  test(
    "referencedDynamicFields finds record.key/record.value however a filter spells them, and nothing" +
      " when a filter never mentions either"
  ) {
    // The set this returns decides whether `recordFields` bothers parsing a record's key or value as JSON
    // at all. Under-detecting is the dangerous direction — it would silently make a field a live filter
    // reads disappear — so every shape a filter can use to reach `record.key`/`record.value` is asserted
    // here on its own, not just exercised incidentally by some other test.
    def fieldsOf(source: String): Set[String] =
      CelEnvironment.referencedDynamicFields(CelEnvironment.compiler.compile(source).getAst)

    assertEquals(fieldsOf("record.partition == 0"), Set.empty[String])
    assertEquals(fieldsOf("record.keyAsText == 'x' && record.valueAsText == 'y'"), Set.empty[String])
    assertEquals(fieldsOf("record.key == 'x'"), Set("key"))
    assertEquals(fieldsOf("record.value.status == 'FAILED'"), Set("value"))
    assertEquals(fieldsOf("has(record.value.status)"), Set("value"))
    assertEquals(fieldsOf("record.value.items[0].price > 1.0"), Set("value"))
    assertEquals(fieldsOf("record.value.items.exists(i, i.price > 1.0)"), Set("value"))
    assertEquals(fieldsOf("record[\"value\"].status == 'x'"), Set("value"))
    assertEquals(fieldsOf("record.key == 'a' && record.value.status == 'b'"), Set("key", "value"))
  }

  test("a filter that never mentions record.key/record.value still evaluates correctly end to end") {
    // The optimisation `referencedDynamicFields` enables must not change what any filter answers, only how
    // much work answering it costs. `record` here has a value that would fail to parse as JSON at all, and
    // the filter below still has to see the correct verdict.
    val textOnly = record.copy(valueAsText = "not json at all")
    evaluate("record.partition == 3 && record.keyAsText == 'order-1'", textOnly).assertEquals(Right(true))
  }

  test("pruning preserves membership computed keys and whole-record access") {
    List(
      "'value' in record",
      "record['val' + 'ue'].status == 'FAILED'",
      "size(record) == 7",
      "[record].exists(r, 'value' in r)"
    ).traverse_(source => evaluate(source).assertEquals(Right(true)))
  }

  // ------------------------------------------------------------------ compilation

  test("the three examples from the user-facing help compile") {
    // These are the filters people will paste first, because they are the ones the reference product shows
    // them. If they do not compile here, the feature is not the feature they know.
    val examples = List(
      "record.partition == 0",
      "record.keyAsText.startsWith('order')",
      "has(record.value.status) && record.value.status == 'FAILED'"
    )
    engine().use(port =>
      examples.traverse_(source => port.register(source).map(r => assert(r.isRight, source)))
    )
  }

  test("a compile error carries a position, which is what the editor underlines") {
    engine().use { port =>
      port.register("record.partition ==").map {
        case Right(_) => fail("an incomplete expression must not compile")
        case Left(error) =>
          assertEquals(error.code, ErrorCode.FilterCompile)
          assert(error.details.nonEmpty, "a compile error with no details tells the editor nothing")
          assert(
            error.details.exists(_.restrictions.exists(_.contains("column"))),
            error.details.toString
          )
      }
    }
  }

  test("source over the size limit is rejected before it is parsed") {
    val limits = FilterLimits.default.copy(maxSourceBytes = 64)
    engine(limits).use { port =>
      port.register("true && " * 100 + "true").map { result =>
        assert(result.swap.exists(_.message.contains("limit is 64")), result.toString)
      }
    }
  }

  test("the size limit is exactly the size limit, in both directions") {
    /*
     * Ungated until now: widening the comparison to `bytes > limits.maxSourceBytes * 2` left
     * `./mill libs.filter.test` green, because the case above sends 808 bytes against a limit of 64 — more
     * than twelve times over, so any multiple of the limit short of twelve still refuses it. The boundary
     * itself was asserted in neither direction.
     *
     * Both halves matter, and for different reasons. A limit that is quietly larger than it says is a
     * denial of service the configuration cannot fix: the check exists *before* parsing precisely so that
     * a megabyte of text is refused without being parsed. A limit that is quietly smaller refuses filters
     * an operator has been told are legal.
     */
    val limits = generous.copy(maxSourceBytes = 64)

    // `true && '<padding>' != ''` — a legal CEL boolean whose length is the padding plus sixteen.
    def sourceOf(bytes: Int): String = s"true && '${"x" * (bytes - 16)}' != ''"

    engine(limits).use { port =>
      for {
        exact <- port.register(sourceOf(64))
        over <- port.register(sourceOf(65))
      } yield {
        assertEquals(sourceOf(64).getBytes("UTF-8").length, 64, clue = "the fixture's arithmetic is wrong")
        assert(exact.isRight, s"a source of exactly the limit was refused: $exact")
        assert(
          over.swap.exists(_.message.contains("65 bytes")),
          s"a source one byte over the limit was accepted: $over"
        )
      }
    }
  }

  test("an AST over the node limit is rejected") {
    val limits = FilterLimits.default.copy(maxAstNodes = 5)
    engine(limits).use { port =>
      port.register("record.partition == 0 && record.offset == 1 && record.keyAsText == 'x'").map { result =>
        assert(result.swap.exists(_.message.contains("expression nodes")), result.toString)
      }
    }
  }

  // ------------------------------------------------------------------ evaluation failures

  test("a missing field is a Left, not a thrown exception") {
    evaluate("record.value.nosuchfield == 'x'").map {
      case Left(FilterError.Runtime(_)) => ()
      case other => fail(s"expected a runtime error, got $other")
    }
  }

  test("a non-boolean result is a runtime error, not a truthy value") {
    // `1 + 1` is a perfectly good CEL expression and a nonsensical filter. Calling a non-zero number
    // "matched" is how a user ends up with a filter that appears to work and silently matches everything.
    evaluate("1 + 1").map {
      case Left(FilterError.Runtime(message)) =>
        assert(message.contains("rather than true or false"), message)
      case other => fail(s"expected a runtime error, got $other")
    }
  }

  test("a runtime error names its kind, which is the metric attribute and the stream event's field") {
    assertEquals(FilterError.Runtime("x").kind, "runtime")
    assertEquals(FilterError.Timeout(10L).kind, "timeout")
  }

  test("an evaluation that outruns the deadline is a Timeout, not a hang") {
    // W11-A1: this case used to accept `Right(_)` as well as `Left(Timeout)` — "a trivial program can
    // legitimately finish inside a one-nanosecond window" — which made it an assertion with no failing
    // input at all. Deleting `.timeoutTo(limits.evaluationDeadline, ...)` from `CelFilterEngine` left both
    // `libs.filter.test` (33 cases) and `services.message.__.test` (1,442 targets) green, and with it goes
    // the whole per-record budget: `FilterError.Timeout` becomes unreachable, `consecutiveTimeoutLimit`
    // becomes unreachable, and a browse spends its entire deadline inside one user's expression.
    //
    // The repair is to stop asking one evaluation to be slow and to ask a *population* instead. A
    // one-nanosecond deadline cannot be met fifty times in a row by anything that has to cross the
    // scheduler; a deadline that is not applied is met every time. Both directions are asserted, so a
    // mutation that makes everything time out fails here too.
    val instant = FilterLimits.default.copy(evaluationDeadline = 1.nanosecond)
    val attempts = 50

    def runAll(limits: FilterLimits): IO[List[Either[FilterError, Boolean]]] =
      engine(limits).use { port =>
        for {
          id <- orFail(port.register("record.partition == 3"))
          predicate <- orFail(port.predicate(id, None))
          results <- List.fill(attempts)(()).traverse(_ => predicate.test(record))
        } yield results
      }

    for {
      impatient <- runAll(instant)
      patient <- runAll(generous)
    } yield {
      val timedOut = impatient.collect { case Left(FilterError.Timeout(afterMs)) => afterMs }
      assert(
        timedOut.nonEmpty,
        s"no evaluation of $attempts hit a one-nanosecond deadline, so no deadline is being applied"
      )
      // The reported figure is the deadline itself, in milliseconds, because that is what reaches the
      // browse's `done` event and the user's screen.
      assert(timedOut.forall(_ == 0L), timedOut.toString)
      // Nothing escapes as an exception, and nothing times out when there is time.
      assertEquals(patient, List.fill(attempts)(Right(true)), "a generous deadline still refused a record")
    }
  }

  test("cancelling a browse cancels the evaluation in flight rather than waiting it out") {
    // The rule is one word in `CelFilterEngine.program`: `Sync[F].interruptible`, not `Sync[F].blocking`.
    // The comment beside it argues the case — twenty thousand records queued at ten milliseconds each is
    // more than three minutes of work nobody is waiting for any more — and until this case existed,
    // rewriting that word to `blocking` left all 548 of `libs.filter.test` green (W11-A1, wave 11).
    //
    // The difference is only observable when the evaluation is *in* an interruptible region and the work
    // it is doing answers `Thread.interrupt`. So the record carries a header map whose iteration parks,
    // which puts the stall exactly where a real CEL evaluation spends its time: inside `eval`, inside the
    // region the rule is about. `blocking` is uncancelable, so the cancel would have to wait out the whole
    // stall; `interruptible` interrupts the thread and the cancel completes at once.
    val stall = 20.seconds
    val cancelBudget = 5.seconds
    val started = new java.util.concurrent.CountDownLatch(1)
    val interrupted = new java.util.concurrent.atomic.AtomicBoolean(false)
    val stalling = record.copy(headers = new StallingHeaders(started, stall, interrupted))

    engine().use { port =>
      for {
        id <- orFail(port.register("size(record.headers) > 0"))
        predicate <- orFail(port.predicate(id, None))
        fiber <- predicate.test(stalling).start
        // Not a sleep: the latch is counted down by the stalling map itself, so the cancel below is
        // guaranteed to arrive while the evaluation is running rather than before it starts.
        _ <- IO.interruptible(started.await())
        before <- IO.monotonic
        _ <- fiber.cancel
        after <- IO.monotonic
      } yield {
        val took = after - before
        assert(
          took < cancelBudget,
          s"cancelling took $took while the evaluation had ${stall} left to run, so the evaluation is " +
            "not running in an interruptible region"
        )
        assert(
          interrupted.get(),
          "the evaluation was never interrupted, so cancelling a browse does not reach the CEL program"
        )
      }
    }
  }

  /** A header map whose every read parks until it is interrupted, announcing that it has started.
    *
    * `FilterableRecord.headers` is a `Map[String, String]`, which is a trait, and `CelEnvironment.activation`
    * is built *inside* the interruptible region — so a map that stalls on iteration stalls the CEL evaluation
    * itself and nothing else. That is what makes the case above a statement about the product's region and
    * not about the test's own arrangement.
    */
  final private class StallingHeaders(
      started: java.util.concurrent.CountDownLatch,
      held: FiniteDuration,
      interrupted: java.util.concurrent.atomic.AtomicBoolean
  ) extends Map[String, String] {

    private def stall(): Unit = {
      started.countDown()
      try Thread.sleep(held.toMillis)
      catch {
        case _: InterruptedException =>
          interrupted.set(true)
          Thread.currentThread().interrupt()
      }
    }

    def get(key: String): Option[String] = { stall(); None }
    def iterator: Iterator[(String, String)] = { stall(); Iterator.empty }
    def removed(key: String): Map[String, String] = this
    def updated[V1 >: String](key: String, value: V1): Map[String, V1] = Map(key -> value)
  }

  // ------------------------------------------------------------------ identity and caching

  test("ids are a pure function of the source, so two engines agree") {
    // The property Kafbat's per-process salt broke: an id minted by one replica has to mean the same filter
    // on every other one, or a load balancer silently breaks the feature.
    val source = "record.partition == 0"
    assertEquals(FilterId.of(source).value.length, 16)
    assertEquals(FilterId.of(source), FilterId.of(source))
    assertNotEquals(FilterId.of(source), FilterId.of("record.partition == 1"))
    (
      engine().use(port => orFail(port.register(source))),
      engine().use(port => orFail(port.register(source)))
    ).tupled
      .map((first, second) => assertEquals(first, second))
  }

  test("an id is sixteen lowercase hex characters and reads back") {
    val id = FilterId.of("record.offset > 0")
    assertEquals(FilterId.fromString(id.value), Some(id))
    assertEquals(FilterId.fromString("NOTHEX0000000000"), None)
    assertEquals(FilterId.fromString("abc"), None)
  }

  test("a replica that has never seen an id compiles it from the source it was sent with") {
    val source = "record.partition == 3"
    val id = FilterId.of(source)
    // A fresh engine: nothing in its cache, exactly like a pod that started thirty seconds ago.
    engine()
      .use(port => orFail(port.predicate(id, Some(source))).flatMap(_.test(record)))
      .assertEquals(Right(true))
  }

  test("an unknown id with no source is refused rather than silently matching everything") {
    engine().use(_.predicate(FilterId.of("never registered"), None)).map(result => assert(result.isLeft))
  }

  test("a source that does not hash to the id it was sent with is refused") {
    // Compiling it anyway would mean the browser and the server disagree about which filter is running,
    // which is the one outcome worse than refusing.
    engine()
      .use(_.predicate(FilterId.of("record.partition == 0"), Some("record.partition == 1")))
      .map(result => assert(result.swap.exists(_.message.contains("does not match"))))
  }

  test("a mismatched source is refused even when the requested id is already cached") {
    val cachedSource = "record.partition == 3"
    val mismatchedSource = "record.partition == 1"

    engine().use { port =>
      for {
        cachedId <- orFail(port.register(cachedSource))
        result <- port.predicate(cachedId, Some(mismatchedSource))
      } yield assert(
        result.swap.exists(_.message.contains("does not match")),
        s"a warm cache bypassed source/id validation: $result"
      )
    }
  }

  // ------------------------------------------------------------------ the test endpoint

  test("the test endpoint answers about one synthetic record without touching Kafka") {
    engine().use(_.test("record.value.status == 'FAILED'", record)).assertEquals(Right(true))
  }

  test("the test endpoint reports a runtime error rather than swallowing it") {
    // During a browse a runtime error is counted and the record excluded. On the test endpoint the error
    // *is* the answer the user came for.
    engine().use(_.test("record.value.nosuchfield == 'x'", record)).map(result => assert(result.isLeft))
  }

  test("the test endpoint reports a compile error with its code") {
    engine().use(_.test("record.partition ==", record)).map { result =>
      assertEquals(result.swap.toOption.map(_.code), Some(ErrorCode.FilterCompile))
    }
  }
}
