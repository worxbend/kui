package kui.filter

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*

import cats.effect.syntax.all.*
import cats.effect.{Async, Resource, Sync}
import cats.syntax.all.*
import dev.cel.common.CelAbstractSyntaxTree
import dev.cel.common.navigation.CelNavigableAst
import dev.cel.runtime.CelRuntime

import kui.cache.{BoundedCache, CacheMetrics}
import kui.kernel.ClusterId
import kui.kernel.error.{ApplicationError, ErrorCode, FieldError, KuiError}

/** The identity of a filter: `sha256(source)`, first 16 hexadecimal characters.
  *
  * **No process salt.** Kafbat salts this per process, which means the id a browser is handed by one replica
  * is meaningless to every other one, and a filter registered before a rolling restart stops existing. A
  * content hash means every replica independently agrees on the id of the same source, registering the same
  * filter twice is free, and a load balancer is allowed to do its job.
  *
  * Sixteen hex characters is 64 bits. A collision needs about four billion distinct filters in one cache of
  * ten thousand entries, and the consequence of one would be a user's filter evaluating as somebody else's —
  * which is why the source is re-sent alongside the id (`filterSource`, ADR-017) and a cache entry is only
  * trusted when it exists; a miss recompiles rather than failing.
  */
opaque type FilterId = String

object FilterId {

  private val Length: Int = 16

  def of(source: String): FilterId = {
    val digest = MessageDigest.getInstance("SHA-256").digest(source.getBytes(StandardCharsets.UTF_8))
    digest.take(Length / 2).map(byte => f"${byte & 0xff}%02x").mkString
  }

  /** Reads an id back from a request. `None` for anything that is not sixteen lowercase hex characters —
    * which is a request KUI never minted, so recompiling from the source is the only honest answer.
    */
  def fromString(raw: String): Option[FilterId] =
    Option
      .when(raw.length == Length && raw.forall(c => (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')))(raw)

  extension (id: FilterId) def value: String = id

  given CanEqual[FilterId, FilterId] = CanEqual.derived
  given Ordering[FilterId] = Ordering.String
}

/** The limits a filter is held to. Configuration, not constants: an operator whose users write longer
  * predicates than KUI's author imagined should be able to say so.
  */
final case class FilterLimits(
    maxSourceBytes: Int,
    maxAstNodes: Int,
    evaluationDeadline: FiniteDuration,
    cacheSize: Long,
    cacheTtl: FiniteDuration,
    /** After this many consecutive timeouts the browse ends with `done{reason:"filter"}` rather than spending
      * its whole deadline on a program that cannot finish. Consecutive, not total: a filter that times out on
      * one record in a thousand is slow, and a filter that times out on every record is broken.
      */
    consecutiveTimeoutLimit: Int,
    /** The node/depth budget `CelEnvironment.asDynamic` walks a record's decoded key or value under, so that
      * a producer-controlled JSON payload cannot spend unbounded CPU converting `record.key`/`record.value`
      * regardless of whether `evaluationDeadline`'s `Thread.interrupt()` is honored (see `CelEnvironment`'s
      * `## The limits`).
      */
    maxJsonValueNodes: Int,
    /** Refuses producer-controlled decoded JSON before Circe parses it. The node limit above only applies
      * after parsing and therefore cannot bound the parser's initial allocation by itself.
      */
    maxJsonValueChars: Int,
    /** Total iterations a comprehension may perform, nested loops included. CEL does not observe the
      * interrupt `evaluationDeadline` requests, and `exists` short-circuits only on true, so this is the
      * bound on a filter whose predicate never matches. A negative value disables the check.
      */
    maxComprehensionIterations: Int = FilterLimits.DefaultMaxComprehensionIterations
)

object FilterLimits {

  /** CEL's comprehension counter, shared across every nested `exists`/`all`/`map`/`filter`. */
  val DefaultMaxComprehensionIterations: Int = 10_000

  /** The defaults of the M3 plan's limits table, and of `kui.message.filter.*`. */
  val default: FilterLimits = FilterLimits(
    maxSourceBytes = 8 * 1024,
    maxAstNodes = 1000,
    evaluationDeadline = scala.concurrent.duration.DurationInt(10).millis,
    cacheSize = 10000L,
    cacheTtl = scala.concurrent.duration.DurationInt(1).hour,
    consecutiveTimeoutLimit = 100,
    maxJsonValueNodes = CelEnvironment.DefaultMaxJsonNodes,
    maxJsonValueChars = CelEnvironment.DefaultMaxJsonChars,
    maxComprehensionIterations = DefaultMaxComprehensionIterations
  )

  given CanEqual[FilterLimits, FilterLimits] = CanEqual.derived
}

/** Compile a filter, cache it, evaluate it. */
trait MessageFilterPort[F[_]] {

  /** Compiles and caches. The id is a pure function of the source, so registering the same filter twice is
    * free and idempotent.
    */
  def register(source: String): F[Either[KuiError, FilterId]]

  /** The predicate for an id.
    *
    * `source` is accepted alongside the id (ADR-017) so that a replica which has never seen this id — a new
    * pod, or simply the other one behind the load balancer — compiles it on demand instead of telling the
    * user their filter has expired.
    */
  def predicate(id: FilterId, source: Option[String]): F[Either[KuiError, MessagePredicate[F]]]

  /** The test endpoint's pure function: compile this source and run it against one synthetic record, without
    * touching Kafka. It is how the filter editor tells a user their expression is wrong before they start a
    * browse that returns nothing.
    */
  def test(source: String, sample: FilterableRecord): F[Either[KuiError, Boolean]]
}

/** CEL, behind `MessageFilterPort`.
  *
  * `dev.cel` types appear in this file and in `CelEnvironment` and nowhere else in KUI (rule A12). That is
  * what makes DEVPLAN's R-5 fallback cheap: if CEL ever has to be dropped, what is lost is one implementation
  * of `MessagePredicate`, not the shape of the browse pipeline.
  */
object CelFilterEngine {

  /** @param cluster
    *   whose filters these are, for the cache metric's attribute. The compiled-program cache is per cluster
    *   because a filter is written against one cluster's data and because an operator reading cache
    *   statistics is asking about one cluster at a time.
    */
  def resource[F[_]: Async](
      cluster: ClusterId,
      limits: FilterLimits,
      metrics: FilterMetrics[F],
      cacheMetrics: CacheMetrics[F]
  ): Resource[F, MessageFilterPort[F]] =
    for {
      cache <- BoundedCache.make[F, String, CelAbstractSyntaxTree](
        "filter.programs",
        cluster,
        limits.cacheSize,
        Some(limits.cacheTtl),
        cacheMetrics
      )
      // The compiler and the runtime are built once. Both are documented as thread-safe and immutable once
      // built, and building a compiler per request would put the standard environment's construction —
      // which is not cheap — on the path of every record.
      runtime <- Resource.eval(
        Sync[F].delay(CelEnvironment.runtime(limits.maxComprehensionIterations))
      )
      compiler <- Resource.eval(Sync[F].delay(CelEnvironment.compiler))
      // Warmed once, at startup, and this is not a micro-optimisation. CEL's first compile and first
      // evaluation in a JVM initialise the standard function dispatcher and load a good deal of class
      // data, and measured cold that costs more than the ten-millisecond per-record deadline. Without
      // this line the *first* records of the *first* filtered browse after a restart time out, which
      // reaches a user as "the filter mysteriously dropped some rows" and reaches an operator as a
      // timeout counter that is non-zero for no reason anyone can reproduce.
      _ <- Resource.eval(warm(compiler, runtime))
    } yield new Impl[F](limits, metrics, cache, compiler, runtime)

  /** Compiles and evaluates one trivial program, so that nothing a user writes pays for the cold path. */
  private def warm[F[_]: Sync](
      compiler: dev.cel.compiler.CelCompiler,
      runtime: CelRuntime
  ): F[Unit] =
    Sync[F]
      .delay {
        val ast = compiler.compile("record.partition == 0").getAst
        val _ = runtime.createProgram(ast).eval(CelEnvironment.activation(WarmUpRecord))
      }
      // A failed warm-up is not a reason to refuse to start: the engine still works, the first
      // evaluation is merely slow. Anything genuinely broken shows up on the first real compile.
      .handleError(_ => ())

  private val WarmUpRecord: FilterableRecord =
    FilterableRecord(0, 0L, 0L, "", "", Map.empty)

  /** Compilation, as a pure function, so that the size and complexity rules are testable without an effect.
    *
    * The order matters. Size is checked **before** parsing, because parsing a megabyte of text to discover it
    * is too long is the denial of service the limit exists to prevent. Node count is checked after parsing
    * and before caching, because it can only be known from the AST and because caching an AST that will
    * always be rejected wastes an entry.
    */
  private[filter] def compile(
      compiler: dev.cel.compiler.CelCompiler,
      limits: FilterLimits,
      source: String
  ): Either[KuiError, CelAbstractSyntaxTree] = {
    val bytes = source.getBytes(StandardCharsets.UTF_8).length
    if bytes > limits.maxSourceBytes then
      Left(
        ApplicationError.Invalid(
          s"the filter is $bytes bytes and the limit is ${limits.maxSourceBytes}",
          List(FieldError.of("filterSource", s"at most ${limits.maxSourceBytes} bytes"))
        )
      )
    else
      validated(compiler, source).flatMap { ast =>
        val nodes = CelNavigableAst.fromAst(ast).getRoot.allNodes().count()
        if nodes > limits.maxAstNodes then
          Left(
            ApplicationError.Invalid(
              s"the filter has $nodes expression nodes and the limit is ${limits.maxAstNodes}",
              List(FieldError.of("filterSource", s"at most ${limits.maxAstNodes} expression nodes"))
            )
          )
        else Right(ast)
      }
  }

  /** CEL's own compile errors, turned into `KUI-FILTER-COMPILE` with the position kept.
    *
    * The position is the whole point: it is what the editor underlines. An error that says only "syntax
    * error" sends the user to re-read a line they have already read three times.
    */
  private def validated(
      compiler: dev.cel.compiler.CelCompiler,
      source: String
  ): Either[KuiError, CelAbstractSyntaxTree] = {
    val result = compiler.compile(source)
    if result.hasError then {
      val issues = result.getErrors.asScala.toList
      Left(
        ApplicationError.FilterCompile(
          issues.headOption.fold("the filter could not be compiled")(_.getMessage),
          issues.map { issue =>
            val at = issue.getSourceLocation
            FieldError(
              Some("filterSource"),
              List(s"line ${at.getLine}, column ${at.getColumn}: ${issue.getMessage}")
            )
          }
        )
      )
    } else
      Either
        .catchNonFatal(result.getAst)
        .leftMap(t =>
          ApplicationError.FilterCompile(
            Option(t.getMessage).getOrElse("the filter could not be compiled"),
            Nil
          )
        )
  }

  final private class Impl[F[_]: Async](
      limits: FilterLimits,
      metrics: FilterMetrics[F],
      cache: BoundedCache[F, String, CelAbstractSyntaxTree],
      compiler: dev.cel.compiler.CelCompiler,
      runtime: CelRuntime
  ) extends MessageFilterPort[F] {

    def register(source: String): F[Either[KuiError, FilterId]] =
      compiled(source).map(_.map(_ => FilterId.of(source)))

    def predicate(id: FilterId, source: Option[String]): F[Either[KuiError, MessagePredicate[F]]] =
      source match {
        case Some(text) if FilterId.of(text) != id =>
          // Check this before consulting the cache. Otherwise a source/id mismatch is rejected on a cold
          // replica but silently accepted on a warm one, so the same request runs different filters based
          // only on which pod receives it.
          Sync[F].pure(
            Left(
              ApplicationError.Invalid(
                "the filter source does not match the filter id it was sent with",
                List(FieldError.of("filterId", "must be sha256(filterSource) truncated to 16 characters"))
              )
            )
          )
        case verifiedSource =>
          cache.get(id.value).flatMap {
            case Some(ast) => Sync[F].pure(Right(program(ast)))
            case None =>
              verifiedSource match {
                case Some(text) => compiled(text).map(_.map(program))
                case None =>
                  Sync[F].pure(
                    Left(
                      ApplicationError.NotFound("filter", id.value, ErrorCode.Validation)
                    )
                  )
              }
          }
      }

    def test(source: String, sample: FilterableRecord): F[Either[KuiError, Boolean]] =
      compiled(source).flatMap {
        case Left(error) => Sync[F].pure(Left(error))
        case Right(ast) =>
          program(ast).test(sample).map {
            case Right(matched) => Right(matched)
            // On the test endpoint a runtime error *is* the answer the user needs, so it is reported rather
            // than counted and swallowed the way it is during a browse.
            case Left(failure) => Left(ApplicationError.Invalid(failure.describe, Nil))
          }
      }

    /** The AST for a source, from the cache or freshly compiled, counted either way. */
    private def compiled(source: String): F[Either[KuiError, CelAbstractSyntaxTree]] = {
      val key = FilterId.of(source).value
      cache.get(key).flatMap {
        case Some(ast) => Sync[F].pure(Right(ast))
        case None =>
          Sync[F].delay(compile(compiler, limits, source)).flatMap {
            case Left(error) => metrics.compiled("failure").as(Left(error))
            case Right(ast) => cache.put(key, ast) >> metrics.compiled("success").as(Right(ast))
          }
      }
    }

    /** One compiled AST, as a predicate.
      *
      * The `Program` is built **once here**, not once per record. That is not an optimisation, it is the
      * difference between working and not: building a program costs several milliseconds on a cold JIT, and
      * the per-record deadline is ten. Doing it inside `test` made every first evaluation of every browse
      * time out — which the suite caught, and which would otherwise have shipped as "the filter mysteriously
      * drops the first few records".
      *
      * A `Program` is immutable and safe to evaluate from many fibers, so one per predicate is also one per
      * browse rather than one per record.
      */
    private def program(ast: CelAbstractSyntaxTree): MessagePredicate[F] = new MessagePredicate[F] {

      private val prepared: Either[FilterError, CelRuntime.Program] =
        Either
          .catchNonFatal(runtime.createProgram(ast))
          .leftMap(t => FilterError.Runtime(Option(t.getMessage).getOrElse(t.getClass.getSimpleName)))

      /** Computed once per compiled program rather than once per record, so a filter that never mentions
        * `record.key`/`record.value` — `record.partition == 0` being the obvious one — does not pay to parse
        * and convert either payload as JSON on every record of the browse. Falls back to converting both, the
        * pre-existing behaviour, if inspecting the AST itself ever fails; a failure here is a reason to do
        * the extra work, never a reason to skip a field a filter might actually need.
        */
      private val neededDynamicFields: Set[String] =
        Either
          .catchNonFatal(CelEnvironment.referencedDynamicFields(ast))
          .getOrElse(CelEnvironment.AllDynamicFields)

      def test(record: FilterableRecord): F[Either[FilterError, Boolean]] = {
        val evaluate = Sync[F]
          .interruptible {
            // `interruptible`, not `blocking`: cancelling a browse has to cancel the evaluation in flight
            // rather than wait out its deadline. With twenty thousand records queued behind it, ten
            // milliseconds each is more than three minutes of work nobody is waiting for any more.
            //
            // `interruptible` only requests `Thread.interrupt()`; it is best-effort, not a wall-clock
            // guarantee, because CEL's own evaluation and the JSON conversion inside `activation` are not
            // obligated to check for it. `maxJsonValueNodes` bounds the JSON half of this call, and
            // `maxComprehensionIterations` bounds the CEL half (see `CelEnvironment`'s `## The limits`).
            // The AST node limit does not: a nested `exists` of a few hundred bytes is still millions of
            // iterations. Crossing the iteration budget throws, and `handleError` below reports it as
            // `FilterError.Runtime`.
            prepared.map(
              _.eval(
                CelEnvironment.activation(
                  record,
                  limits.maxJsonValueNodes,
                  limits.maxJsonValueChars,
                  neededDynamicFields
                )
              )
            )
          }
          .map(_.flatMap(asBoolean))
          .handleError(t =>
            Left(FilterError.Runtime(Option(t.getMessage).getOrElse(t.getClass.getSimpleName)))
          )

        evaluate
          .timeoutTo(
            limits.evaluationDeadline,
            Sync[F].pure(Left(FilterError.Timeout(limits.evaluationDeadline.toMillis)))
          )
          .flatTap {
            case Left(error) => metrics.errored(error.kind)
            case Right(_) => Sync[F].unit
          }
      }
    }

    /** A CEL program that returns something other than a boolean is a runtime error, not a truthy value.
      *
      * `1 + 1` is a perfectly good CEL expression and a nonsensical filter. Treating a non-empty string or a
      * non-zero number as "matched" is how a user ends up with a filter that appears to work and silently
      * matches everything.
      */
    private def asBoolean(result: Object): Either[FilterError, Boolean] =
      result match {
        case boolean: java.lang.Boolean => Right(boolean.booleanValue())
        case other =>
          Left(
            FilterError.Runtime(
              s"the filter returned ${Option(other).fold("null")(_.getClass.getSimpleName)} rather than true or false"
            )
          )
      }
  }
}
