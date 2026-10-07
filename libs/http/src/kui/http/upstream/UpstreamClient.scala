package kui.http.upstream

import java.util.Locale
import java.util.concurrent.TimeoutException

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Resource}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.Stream
import org.typelevel.log4cats.StructuredLogger
import sttp.capabilities.{Effect, StreamMaxLengthExceededException}
import sttp.client4.wrappers.DelegateBackend
import sttp.client4.{Backend, GenericRequest, Response}

import kui.config.{SafeUrl, UrlPolicy}
import kui.kernel.error.{ErrorCode, InfrastructureError, KuiError}
import kui.kernel.{PositiveInt, ServiceId}
import kui.observability.{MetricNames, Telemetry, UpstreamInstrumentation, UpstreamOutcome}

/** Everything KUI needs to know about how to call one other system.
  *
  * @param name
  *   the label used in metrics and in errors, e.g. `schema-registry`. A name, never a URL.
  * @param urls
  *   the addresses of that system, in preference order
  * @param callTimeout
  *   the whole-call budget, including retries. A caller may treat every call as bounded by it.
  * @param maxConcurrent
  *   the bulkhead
  * @param maxRetries
  *   how many extra attempts an idempotent call gets
  * @param failureThreshold
  *   consecutive failures that open the circuit
  * @param resetTimeout
  *   how long the circuit stays open
  * @param failoverGrace
  *   how long an address that refused a connection is set aside
  * @param retryableStatuses
  *   statuses the caller says are safe to repeat. Empty here; Kafka Connect's 409 "rebalance in progress" is
  *   the first entry, and it arrives with Connect support in M7.
  */
final case class UpstreamConfig(
    name: String,
    urls: NonEmptyList[SafeUrl],
    callTimeout: FiniteDuration = 10.seconds,
    maxConcurrent: PositiveInt = PositiveInt.unsafe(32),
    maxRetries: Int = 2,
    failureThreshold: PositiveInt = PositiveInt.unsafe(5),
    resetTimeout: FiniteDuration = 30.seconds,
    failoverGrace: FiniteDuration = 5.seconds,
    retryableStatuses: Set[Int] = Set.empty,
    retryBase: FiniteDuration = 100.milliseconds,
    urlPolicy: UrlPolicy = UrlPolicy.Strict
)

/** A backend that will not let one broken upstream take a KUI process down.
  *
  * This is the mechanical half of the product's central promise: one slow or dead dependency degrades one
  * part of the UI and nothing else. Every call it makes is bounded, every failure it produces is a typed
  * `KuiError`, and a caller may therefore treat an upstream call the way it treats a local one — as something
  * that finishes.
  */
trait UpstreamClient[F[_]] {

  /** Hand this to any Tapir sttp client interpreter. */
  def backend: Backend[F]

  /** Circuit transitions, for the capability registry. */
  def circuitStates: Stream[F, CircuitEvent]

  def currentState: F[CircuitState]
}

/** The exception a caller unwraps to get back the typed error.
  *
  * An sttp backend can only fail with a `Throwable`, so the `KuiError` travels inside one. Nothing else in
  * KUI should ever construct this: it exists so that the error crossing the backend boundary is the same
  * value on both sides rather than something reconstructed from a message.
  */
final case class UpstreamFailure(error: KuiError) extends Exception(error.message)

object UpstreamClient {

  /** Builds the client. Order per call, and each layer is there for a different failure:
    *
    *   1. **URL policy** — the selected failover URL must be one this deployment may call. Production
    *      transports must come from [[HttpTls.resource]] with this same policy: that factory enforces the
    *      numeric socket address, which cannot be secured by a separate preflight DNS lookup;
    *   2. **bulkhead** — fail fast rather than let a slow upstream consume every thread;
    *   3. **circuit breaker** — stop calling something that is plainly down, and find out when it is back;
    *   4. **failover and retry** — try the next address when one refuses a connection, and repeat an
    *      idempotent call with full-jitter backoff;
    *   5. **timeout** — around all attempts, inside breaker protection so deadline expiry counts as failure;
    *   6. **instrumentation** — one span and one measurement per call.
    *
    * The nesting is what makes the timeout meaningful: it is outside the retries, so `callTimeout` bounds the
    * *call*, not each attempt. A caller told "at most ten seconds" gets ten seconds, not ten seconds times
    * three.
    */
  def resource[F[_]: Async](
      config: UpstreamConfig,
      underlying: Backend[F],
      telemetry: Telemetry[F],
      serviceName: ServiceId,
      logger: StructuredLogger[F]
  ): Resource[F, UpstreamClient[F]] =
    for {
      breaker <- Resource.eval(
        CircuitBreaker.make[F](config.name, config.failureThreshold, config.resetTimeout)
      )
      bulkhead <- Resource.eval(Bulkhead.make[F](config.name, config.maxConcurrent))
      failover <- Resource.eval(Failover.make[F](config.urls, config.failoverGrace))
      attempts <- Resource.eval(
        UpstreamInstrumentation.measureAttempts[F](underlying, telemetry, serviceName.value, config.name)
      )
      instrumented <- Resource.eval(
        UpstreamInstrumentation.wrap[F](
          new ResilientBackend[F](attempts, config, breaker, bulkhead, failover),
          telemetry,
          serviceName.value,
          config.name,
          result =>
            result match {
              case Left(UpstreamFailure(_: InfrastructureError.Timeout)) => UpstreamOutcome.Timeout
              case Left(UpstreamFailure(_: InfrastructureError.CircuitOpen)) => UpstreamOutcome.CircuitOpen
              case other => UpstreamInstrumentation.outcomeOf(other)
            }
        )
      )
      _ <- logTransitions[F](breaker, config.name, logger)
    } yield new UpstreamClient[F] {
      val backend: Backend[F] = instrumented

      def circuitStates: Stream[F, CircuitEvent] = breaker.events
      def currentState: F[CircuitState] = breaker.state
    }

  /** One INFO line per transition, and **no line per failed call**.
    *
    * A dead upstream that logs on every attempt floods the log at exactly the moment an operator needs to
    * read it — this is the flooding footgun the reference implementation has. A transition is rare, is the
    * thing worth knowing, and carries the last error that caused it.
    *
    * ==Subscribed before the client exists==
    *
    * `breaker.subscribed` and not `breaker.events`. The subscription is registered while this `Resource` is
    * being acquired, which is before `resource` yields the client, so there is no window in which a caller
    * can make a request through a breaker nothing is listening to. Running `events` in a background fiber
    * instead left exactly that window open: an upstream that is already down when KUI starts trips its
    * circuit during start-up, the event is published to no subscribers, and the line an operator needs is
    * never written. Nothing looks wrong — the requests fail correctly — the log is simply silent about why.
    */
  private def logTransitions[F[_]: Async](
      breaker: CircuitBreaker[F],
      upstream: String,
      logger: StructuredLogger[F]
  ): Resource[F, Unit] =
    breaker.subscribed.flatMap(transitions => drainInto(transitions, upstream, logger))

  private def drainInto[F[_]: Async](
      transitions: fs2.Stream[F, CircuitEvent],
      upstream: String,
      logger: StructuredLogger[F]
  ): Resource[F, Unit] =
    transitions
      .evalMap { event =>
        val state = event.state.toString.toLowerCase(Locale.ROOT)
        logger.info(
          Map(
            MetricNames.Attr.Upstream -> upstream,
            MetricNames.Attr.State -> state
          ) ++ event.lastError.map("error.last" -> _)
        )(s"circuit for $upstream is now $state")
      }
      .compile
      .drain
      .background
      .void

  /** Turns whatever went wrong into the `KuiError` the error table in HTTP-003 promises.
    *
    * The upstream's response **body is discarded** (ADR-034): it may carry another system's internal detail
    * or its credentials, and echoing it into a KUI error would put both in a place a user can read.
    */
  def errorFor(config: UpstreamConfig, outcome: Either[Throwable, Int]): Option[KuiError] =
    outcome match {
      case Right(status) if status == 401 || status == 403 =>
        Some(InfrastructureError.AuthFailed(config.name))
      case Right(status) if status >= 400 => Some(InfrastructureError.Upstream(config.name, status))
      case Right(_) => None

      case Left(CircuitOpenException(upstream, since)) =>
        Some(InfrastructureError.CircuitOpen(upstream, since))

      case Left(BulkheadFullException(upstream)) =>
        // A bulkhead rejection is a timeout of zero milliseconds: nothing was waited for, and the
        // caller is being told the same thing a timeout tells it — this did not happen in time.
        Some(InfrastructureError.Timeout(s"$upstream (bulkhead full)", 0))

      case Left(_: TimeoutException) =>
        Some(InfrastructureError.Timeout(config.name, config.callTimeout.toMillis))

      case Left(UpstreamFailure(error)) => Some(error)

      case Left(error) if isResponseLimitCause(error) =>
        Some(responseLimitFailure(config.name))

      case Left(error) =>
        Some(
          InfrastructureError.Unreachable(
            config.name,
            safeFailureCause(error)
          )
        )
    }

  /** A stable transport diagnostic that cannot copy credentials, hosts, or query strings from an exception.
    *
    * Exception messages are controlled by HTTP clients and the operating system and commonly include the
    * address being dialled. Configured addresses may contain sensitive user-info, so only the exception's
    * static class name is retained for logs.
    */
  private[kui] def safeFailureCause(error: Throwable): String =
    Option(error.getClass.getSimpleName).filter(_.nonEmpty).getOrElse("transport failure")

  private[upstream] def isResponseLimitCause(error: Throwable): Boolean = {
    @annotation.tailrec
    def loop(current: Throwable, remaining: Int): Boolean =
      if current.isInstanceOf[StreamMaxLengthExceededException] then true
      else if remaining <= 0 then false
      else
        Option(current.getCause) match {
          case Some(cause) if cause ne current => loop(cause, remaining - 1)
          case _ => false
        }

    loop(error, 16)
  }

  /** Stable classification for a streamed response that crossed its configured byte ceiling.
    *
    * The marker is private and attached only while translating the typed transport failure. Callers can
    * therefore distinguish this case without interpreting user-visible error text, while the long-standing
    * [[UpstreamFailure]] apply/unapply API remains unchanged.
    */
  def isResponseLimitFailure(error: Throwable): Boolean =
    error match {
      case failure: UpstreamFailure => Option(failure.getCause).contains(ResponseLimitMarker)
      case _ => false
    }

  private def responseLimitFailure(upstream: String): KuiError =
    InfrastructureError.Remote(
      ErrorCode.UpstreamUnavailable,
      s"$upstream$ResponseLimitMessageSuffix",
      Nil
    )

  private val ResponseLimitMessageSuffix = " response exceeded its configured size limit"
  private object ResponseLimitMarker extends RuntimeException("response_limit")

  /** From this status upwards the upstream is telling us it is broken, not that we are.
    *
    * The circuit counts these as failures: something that answers `503` to everything is as down as something
    * that refuses connections, and a breaker that only noticed exceptions would never open for it.
    */
  private val ServerErrorFrom: Int = 500

  // ---------------------------------------------------------------------------------------------

  final private class ResilientBackend[F[_]: Async](
      delegate: Backend[F],
      config: UpstreamConfig,
      breaker: CircuitBreaker[F],
      bulkhead: Bulkhead[F],
      failover: Failover[F]
  ) extends DelegateBackend[F, Any](delegate)
      with Backend[F] {

    override def send[T](request: GenericRequest[T, Any & Effect[F]]): F[Response[T]] = {
      val bounded = attempt(request, 0).timeoutTo(
        config.callTimeout,
        Async[F].raiseError[Response[T]](
          new TimeoutException(s"${config.name} did not answer within ${config.callTimeout}")
        )
      )
      val call = bulkhead.protect(
        breaker
          .protectClassified(bounded)(response => response.code.code < ServerErrorFrom)(error =>
            !isResponseLimitCause(error)
          )
      )

      call.handleErrorWith(error => Async[F].raiseError(translate(error)))
    }

    /** One attempt over the healthy addresses, then a retry if the outcome allows it. */
    private def attempt[T](request: GenericRequest[T, Any & Effect[F]], retries: Int): F[Response[T]] =
      failover.candidates
        .flatMap(sendToFirstReachable(request, _))
        .attempt
        .flatMap {
          case Right(response) =>
            val outcome = Right(response.code.code)
            if RetryPolicy.shouldRetry(request.method, outcome, config.retryableStatuses) &&
              retries < config.maxRetries
            then waitThen(retries) *> attempt(request, retries + 1)
            else Async[F].pure(response)

          case Left(error) =>
            val retryable = RetryPolicy.shouldRetry(request.method, Left(error), config.retryableStatuses)
            // The circuit's own refusal is never retried: it exists precisely to stop calls, and a
            // retry loop around it would defeat it entirely.
            val isRefusal =
              error.isInstanceOf[CircuitOpenException] || error.isInstanceOf[BulkheadFullException]

            val isResponseLimit = isResponseLimitCause(error)

            if retryable && !isRefusal && !isResponseLimit && retries < config.maxRetries then
              waitThen(retries) *> attempt(request, retries + 1)
            else Async[F].raiseError(error)
        }

    private def waitThen(retries: Int): F[Unit] =
      Async[F].sleep(RetryPolicy.backoff(retries, config.retryBase))

    /** Tries each address in turn, rotating past the ones that will not connect.
      *
      * Only a connection-level failure on a replay-safe request (or one known not to have transmitted) moves
      * on to the next address. An address that answered `500` is reachable and is answering; asking the next
      * machine the same question would give the same answer and would hide from the operator that the cluster
      * is unwell rather than unreachable. A URL-policy refusal does not fail over either — the next address
      * would be refused for the same reason.
      *
      * When every address refuses a connection the last failure propagates, and `errorFor` turns it into
      * `Unreachable`.
      */
    private def sendToFirstReachable[T](
        request: GenericRequest[T, Any & Effect[F]],
        urls: NonEmptyList[SafeUrl]
    ): F[Response[T]] =
      urls.tail.foldLeft(sendTo(request, urls.head)) { (previous, url) =>
        previous.handleErrorWith {
          case error
              if Failover.isConnectionFailure(error) &&
                (RetryPolicy.isIdempotent(request.method) || Failover.isBeforeTransmission(error)) =>
            sendTo(request, url)
          case other => Async[F].raiseError(other)
        }
      }

    private def sendTo[T](
        request: GenericRequest[T, Any & Effect[F]],
        url: SafeUrl
    ): F[Response[T]] = {
      val rebased = Failover.rebase(request.uri, url)

      SafeUrl.from(rebased.toString, config.urlPolicy) match {
        case Left(violation) =>
          // A redirect, or a caller, that would take KUI somewhere it may not go. This is a policy
          // violation and not a transport failure, so it is not retried and not failed over: the
          // next address would be refused for the same reason.
          Async[F].raiseError(
            UpstreamFailure(
              InfrastructureError.Unreachable(config.name, s"refused by the URL policy: ${violation.message}")
            )
          )

        case Right(_) =>
          delegate
            // Redirect handling is above the socket transport. Only explicitly configured failover
            // endpoints may receive credentials; HttpTls independently disables redirects as well.
            .send(request.method(request.method, rebased).followRedirects(false))
            .attempt
            .flatMap {
              case Right(response) => failover.markHealthy(url).as(response)
              case Left(error) if Failover.isConnectionFailure(error) =>
                failover.markFailed(url) *> Async[F].raiseError(error)
              case Left(error) => Async[F].raiseError(error)
            }
      }
    }

    /** Every failure leaves this backend as an [[UpstreamFailure]], so a caller has one thing to match on
      * rather than a mixture of transport exceptions and KUI errors.
      */
    private def translate(error: Throwable): Throwable =
      if isResponseLimitCause(error) then markedResponseLimitFailure(config.name)
      else errorFor(config, Left(error)).fold(error)(UpstreamFailure.apply)

    private def markedResponseLimitFailure(upstream: String): UpstreamFailure = {
      val failure = UpstreamFailure(responseLimitFailure(upstream))
      val _ = failure.initCause(ResponseLimitMarker)
      failure
    }
  }
}
