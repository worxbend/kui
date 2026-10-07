package kui.observability

import java.util.concurrent.TimeoutException

import cats.effect.kernel.{Async, Outcome}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.Histogram
import org.typelevel.otel4s.trace.{SpanKind, Tracer}
import sttp.capabilities.Effect
import sttp.client4.wrappers.DelegateBackend
import sttp.client4.{Backend, GenericRequest, Response}

/** The client-side half of the observability standard: one span and one measurement per call to another
  * system.
  *
  * A KUI process is mostly a caller. When a page is slow, the first question is always "was it us, or was it
  * the thing we called", and answering it needs the outbound call to be a span of its own inside the
  * request's span, with a duration recorded under an outcome an operator can group by.
  *
  * ==Why the outcome matters more than the status==
  *
  * `outcome` distinguishes success, client error, server error, timeout, circuit open, unreachable and
  * external cancellation because these lead to different actions. A dashboard grouped by HTTP status cannot
  * distinguish "the upstream refused the connection" from "we gave up waiting", and those have different
  * causes and different fixes.
  */
object UpstreamInstrumentation {

  /** Wraps a backend so every call it makes is traced and measured.
    *
    * `traceparent` is injected into the outgoing request, which is what joins KUI's span to the upstream's
    * own trace when the upstream is also instrumented — the difference between seeing "the call took 4
    * seconds" and seeing what the other system spent those 4 seconds on.
    *
    * @param upstream
    *   the label for this upstream in metrics and errors, e.g. `schema-registry`. A name, never a URL: a URL
    *   contains a host and sometimes a port, and a label whose values multiply is how a metrics backend runs
    *   out of memory.
    */
  def wrap[F[_]: Async](
      backend: Backend[F],
      telemetry: Telemetry[F],
      serviceName: String,
      upstream: String,
      classify: Either[Throwable, Int] => UpstreamOutcome = outcomeOf
  ): F[Backend[F]] =
    for {
      tracer <- telemetry.tracer(s"kui.${KuiInterceptors.contextOf(serviceName)}.upstream")
      meter <- telemetry.meter(s"kui.${KuiInterceptors.contextOf(serviceName)}.upstream")
      duration <- meter
        .histogram[Double](MetricNames.UpstreamDuration)
        .withUnit("s")
        .withDescription("How long a call to another system took, and how it ended")
        .create
    } yield new InstrumentedBackend[F](backend, tracer, duration, serviceName, upstream, classify)

  /** Attempt measurements are separate from logical calls and do not create extra logical spans. */
  def measureAttempts[F[_]: Async](
      backend: Backend[F],
      telemetry: Telemetry[F],
      serviceName: String,
      upstream: String
  ): F[Backend[F]] =
    telemetry.meter(s"kui.${KuiInterceptors.contextOf(serviceName)}.upstream").flatMap { meter =>
      meter
        .histogram[Double](MetricNames.UpstreamAttemptDuration)
        .withUnit("s")
        .withDescription("Duration of individual transport attempts, excluding retry backoff")
        .create
        .map { histogram =>
          new DelegateBackend[F, Any](backend) with Backend[F] {
            override def send[T](request: GenericRequest[T, Any & Effect[F]]): F[Response[T]] =
              Async[F].monotonic.flatMap { started =>
                backend.send(request).guaranteeCase { exit =>
                  for {
                    outcome <- classifyExit(exit, outcomeOf)
                    ended <- Async[F].monotonic
                    _ <- record(
                      histogram,
                      serviceName,
                      upstream,
                      outcome,
                      (ended - started).toNanos.toDouble / 1e9
                    )
                  } yield ()
                }
              }
          }
        }
    }

  private def classifyExit[F[_]: Async, T](
      exit: Outcome[F, Throwable, Response[T]],
      classify: Either[Throwable, Int] => UpstreamOutcome
  ): F[UpstreamOutcome] = exit match {
    case Outcome.Succeeded(value) => value.map(response => classify(Right(response.code.code)))
    case Outcome.Errored(error) => Async[F].pure(classify(Left(error)))
    case Outcome.Canceled() => Async[F].pure(UpstreamOutcome.Canceled)
  }

  /** How a call ended, from what came back.
    *
    * Exposed so that `libs/http`'s upstream client can report the outcomes only it knows about — a call the
    * circuit breaker refused, or one the bulkhead turned away — under the same labels.
    */
  def outcomeOf(result: Either[Throwable, Int]): UpstreamOutcome =
    result match {
      case Right(status) => UpstreamOutcome.ofStatus(status)
      case Left(_: TimeoutException) => UpstreamOutcome.Timeout
      case Left(_) => UpstreamOutcome.Unreachable
    }

  /** Records one call's duration under its outcome. Callers that never reached the network — a circuit that
    * was open — use this directly, so an open circuit is visible in the same metric as a slow one rather than
    * as an absence of data.
    */
  def record[F[_]](
      histogram: Histogram[F, Double],
      serviceName: String,
      upstream: String,
      outcome: UpstreamOutcome,
      seconds: Double
  ): F[Unit] =
    histogram.record(
      seconds,
      Attribute(MetricNames.Attr.Service, serviceName),
      Attribute(MetricNames.Attr.Upstream, upstream),
      Attribute(MetricNames.Attr.Outcome, outcome.wire)
    )

  final private class InstrumentedBackend[F[_]: Async](
      delegate: Backend[F],
      tracer: Tracer[F],
      duration: Histogram[F, Double],
      serviceName: String,
      upstream: String,
      classify: Either[Throwable, Int] => UpstreamOutcome
  ) extends DelegateBackend[F, Any](delegate)
      with Backend[F] {

    override def send[T](request: GenericRequest[T, Any & Effect[F]]): F[Response[T]] = {
      val name = s"${request.method.method} $upstream"

      tracer
        .spanBuilder(name)
        // `Client`, not the default `Internal`: a trace backend uses the kind to lay a trace out,
        // and a call that leaves the process drawn as an internal step makes the picture wrong.
        .withSpanKind(SpanKind.Client)
        .build
        .use { span =>
          for {
            startedAt <- Async[F].monotonic
            propagated <- withTraceparent(request, span)
            result <- delegate.send(propagated).guaranteeCase { exit =>
              for {
                outcome <- classifyExit(exit, classify)
                endedAt <- Async[F].monotonic
                _ <- record(
                  duration,
                  serviceName,
                  upstream,
                  outcome,
                  (endedAt - startedAt).toNanos.toDouble / 1e9
                )
                _ <- span.addAttributes(
                  Attribute(MetricNames.Attr.Upstream, upstream),
                  Attribute(MetricNames.Attr.Outcome, outcome.wire)
                )
              } yield ()
            }
          } yield result
        }
    }

    /** Adds `traceparent` for this span, unless the caller already set one.
      *
      * The header is written from the span context by hand rather than through a propagator, because the W3C
      * format is four fixed fields and a two-line encoding here is easier to read — and to be sure of — than
      * plumbing a `TextMapPropagator` through an sttp request.
      */
    private def withTraceparent[T](
        request: GenericRequest[T, Any & Effect[F]],
        span: org.typelevel.otel4s.trace.Span[F]
    ): F[GenericRequest[T, Any & Effect[F]]] =
      Async[F].pure {
        val context = span.context
        if !context.isValid || request.headers.exists(_.is(TraceparentHeader)) then request
        else {
          val flags = if context.isSampled then "01" else "00"
          val value = s"00-${context.traceIdHex}-${context.spanIdHex}-$flags"
          request.header(TraceparentHeader, value)
        }
      }
  }

  /** The W3C Trace Context header. Lowercase, as the specification requires. */
  val TraceparentHeader: String = "traceparent"
}
