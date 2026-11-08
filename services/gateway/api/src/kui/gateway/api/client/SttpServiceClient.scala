package kui.gateway.api.client

import scala.concurrent.duration.FiniteDuration

import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Clock, Resource}
import cats.syntax.all.*
import fs2.Stream
import org.typelevel.log4cats.StructuredLogger
import sttp.capabilities.fs2.Fs2Streams
import sttp.client4.*
import sttp.model.Header
import sttp.tapir.client.sttp4.SttpClientInterpreter
import sttp.tapir.client.sttp4.stream.StreamSttpClientInterpreter
import sttp.tapir.{DecodeResult, Endpoint, PublicEndpoint}

import kui.config.{UpstreamServiceConfig, UrlPolicy}
import kui.contracts.ErrorEnvelope
import kui.gateway.application.client.{CallContext, ServiceClient}
import kui.http.sse.{SseEvent, SseWire}
import kui.http.upstream.{CircuitEvent, UpstreamClient, UpstreamConfig}
import kui.kernel.ServiceId
import kui.kernel.error.{FieldError, InfrastructureError, KuiError}
import kui.observability.{Correlation, Telemetry}
import kui.security.{PrincipalClaims, PrincipalCodec, RequestDigests, SignedPrincipal}

/** The distributed implementation of `ServiceClient`: a Tapir client interpreter over the resilient backend
  * of ADR-037.
  *
  * ==How one call is assembled==
  *
  *   1. The endpoint value is interpreted into an sttp request against this service's base URL, with a
  *      placeholder principal, purely so that the method, the final path and the encoded body are known.
  *   2. Those three are hashed into a `RequestDigest` and signed into a token whose audience is *this*
  *      service (ADR-020). A token minted for the topic service is therefore useless against the schema
  *      service, and a token for `GET /topics` is useless for `DELETE /topics/x`.
  *   3. The request is interpreted a second time with the real token, and the correlation id and the optional
  *      cluster id are added as headers.
  *   4. It is sent through the `UpstreamClient` backend, which applies this service's own bulkhead, circuit
  *      breaker, retry policy and timeout, and which adds `traceparent` from the current span.
  *
  * Interpreting twice looks wasteful and is the only honest way to do it: the signature covers the request,
  * so the request has to exist before it can be signed, and the token is part of the request. The cost is
  * building an in-memory request object, not a network round trip.
  *
  * ==Why nothing of the browser's request is forwarded==
  *
  * The outbound request is built from the endpoint value and the `CallContext` alone. No inbound header is
  * copied. A browser `Cookie` or `Authorization` therefore cannot reach a service, which is the point of
  * ADR-020: services trust the signed principal and nothing else, so a header the browser controls must never
  * be in a position to be mistaken for one the gateway vouched for.
  */
object SttpServiceClient {

  /** How long a minted principal token is valid. Seconds, because it is spent immediately on the call it was
    * minted for; the only thing it has to survive is clock skew between the two processes.
    */
  val TokenLifetimeSeconds: Long = 30L

  val ClusterHeader: String = "X-Kui-Cluster-Id"

  /** Builds a client for one service.
    *
    * `underlying` is the raw transport, supplied rather than constructed here so that a suite can hand in a
    * stub backend and exercise the whole assembly — signing, headers, error mapping — without a socket. The
    * composition root passes the process-wide HTTP backend.
    *
    * @param policy
    *   the deployment's outbound address rule, which `ResilientBackend` re-applies to every request and to
    *   every redirect. It is a parameter rather than the default because the two halves have to be the same
    *   decision: the configuration loader already accepted this URL under the operator's
    *   `KUI_ALLOW_PRIVATE_UPSTREAMS`, and a per-call check that disagrees with the loader refuses at request
    *   time what start-up accepted. Defaulted to `Strict` so that a caller who says nothing gets the safe
    *   answer rather than the convenient one.
    */
  def resource[F[_]: Async](
      service: ServiceId,
      config: UpstreamServiceConfig,
      principals: PrincipalCodec[F],
      telemetry: Telemetry[F],
      logger: StructuredLogger[F],
      underlying: StreamBackend[F, Fs2Streams[F]],
      policy: UrlPolicy = UrlPolicy.Strict
  ): Resource[F, ServiceClient[F]] =
    UpstreamClient
      .resource[F](upstreamConfig(service, config, policy), underlying, telemetry, service, logger)
      .map(upstream =>
        new Impl[F](
          service,
          config.url.value,
          principals,
          calls = upstream.backend,
          streaming = underlying,
          circuits = upstream.circuitStates,
          // `UpstreamClient` already bounds the call, retries inside that bound and trips a breaker on it.
          // A second timeout here would only be a slower copy of the first.
          callTimeout = None
        )
      )

  /** The same client over a backend that needs no resilience wrapper.
    *
    * `resource` above puts every request/response call inside a bulkhead, a circuit breaker, a retry policy
    * and a call timeout, because on the far side of those calls is a network and another machine. When the
    * far side is an object in this JVM — the all-in-one deployment of ADR-005 — none of those protections has
    * anything to protect against: there is no connection to refuse, no socket to hang, and no second process
    * whose slowness could exhaust this one's threads independently of its own work. Wrapping an in-memory
    * call in a circuit breaker would only add a way for the process to stop calling itself.
    *
    * Everything above the transport is unchanged, and that is the point of the method existing at all. The
    * signing, the header decoration, the response decoding and the error mapping are the very same lines, so
    * a response cannot differ between the two deployment shapes — which is exactly what ADR-005 says must be
    * impossible.
    *
    * @param baseUrl
    *   the address requests are built against. In the in-process shape nothing dials it, but the path it
    *   produces is what gets hashed into the request digest, so it still has to be a well-formed URL.
    * ==One protection this shape still needs, and why==
    *
    * A bulkhead and a circuit breaker guard against another *process*; a timeout does not. The object on the
    * far side of this call is in this JVM, but what it does is talk to Kafka, and a Kafka `AdminClient` whose
    * broker has vanished takes its own `apiTimeout` — thirty seconds by default — to give up. The HTTP server
    * in front of all of this gives up after twenty, and when Netty is the one that gives up first the client
    * gets a bare `503` with no body, no error code and no correlation id, and the browser reports "the server
    * sent something KUI could not read" for what is only a broker being switched off.
    *
    * So this shape keeps the call timeout and drops the rest, which is a smaller claim than the one this
    * method used to make. Nothing about resilience is being simulated: the point is that KUI, and not Netty,
    * is what answers.
    *
    * @param backend
    *   where a built request is sent. For the all-in-one that is Tapir's stub interpreter over the service's
    *   own routes and interceptors; for a suite it can be any stub at all.
    * @param callTimeout
    *   how long one request/response call may take before it becomes `KUI-UPSTREAM-TIMEOUT`. It must be
    *   shorter than the HTTP server's own response timeout, or the server answers first and the answer is not
    *   KUI's. Streaming is not bounded by it, for the reason `stream` records.
    */
  def over[F[_]: Async](
      service: ServiceId,
      baseUrl: String,
      principals: PrincipalCodec[F],
      backend: StreamBackend[F, Fs2Streams[F]],
      callTimeout: FiniteDuration
  ): ServiceClient[F] =
    new Impl[F](
      service,
      baseUrl,
      principals,
      calls = backend,
      streaming = backend,
      circuits = Stream.empty,
      callTimeout = Some(callTimeout)
    )

  /** The gateway's per-service upstream policy, derived from the two knobs an operator sets.
    *
    * Everything else is `UpstreamConfig`'s default, which is deliberate: an operator configures a URL, a
    * timeout and a concurrency cap per service, and the resilience behaviour behind those is one decision
    * (ADR-037) rather than eleven sets of tuning knobs nobody will keep consistent.
    *
    * The address rule is the exception, and it is threaded rather than defaulted for a reason that was
    * measured rather than argued. `UpstreamConfig.urlPolicy` defaults to `Strict`, and `ResilientBackend`
    * applies it to every request; leaving the default in place here meant that a gateway configured with
    * `KUI_ALLOW_PRIVATE_UPSTREAMS=true` and a loopback or ClusterIP upstream *loaded* that address happily —
    * `Main.loadConfig` uses `UrlPolicy.fromEnv` — and then refused every call to it as
    * `KUI-UPSTREAM-UNAVAILABLE` with no connection ever attempted. The distributed Compose stack is not
    * affected, because `SafeUrl` never resolves a host *name* and `kui-cluster` is a name; a developer
    * running the gateway against `http://localhost:8081`, which is the case the environment variable exists
    * for, was. `MetricsWiring` and `SchemaWiring` already thread the policy the same way.
    */
  def upstreamConfig(
      service: ServiceId,
      config: UpstreamServiceConfig,
      policy: UrlPolicy = UrlPolicy.Strict
  ): UpstreamConfig =
    UpstreamConfig(
      name = service.value,
      urls = NonEmptyList.one(config.url),
      callTimeout = config.timeout,
      maxConcurrent = config.maxConcurrent,
      urlPolicy = policy
    )

  /** Turns an upstream's error response back into the `KuiError` the service raised.
    *
    * This is what keeps a business failure a business failure across a process boundary. A missing topic is a
    * 404 `KUI-TOPIC-NOT-FOUND` on the wire; if the gateway decoded it as "the upstream answered 404, so it is
    * `InfrastructureError.Upstream`", then the browser would see a generic upstream error and — worse — the
    * capability registry would dim the topic feature because a user typed a name that does not exist (ADR-039
    * §6). So the envelope's own code wins, and only a response that carries no recognisable envelope falls
    * back to a transport verdict.
    */
  def errorOf(service: ServiceId, status: Int, envelope: ErrorEnvelope): KuiError =
    ErrorEnvelope.codeOf(envelope) match {
      case Some(code) =>
        KuiError.remote(
          code,
          envelope.message,
          envelope.details.map(detail => FieldError(detail.field, detail.restrictions))
        )
      case None => InfrastructureError.Upstream(service.value, status)
    }

  final private class Impl[F[_]: Async](
      val service: ServiceId,
      baseUrl: String,
      principals: PrincipalCodec[F],
      calls: Backend[F],
      streaming: StreamBackend[F, Fs2Streams[F]],
      circuits: Stream[F, CircuitEvent],
      callTimeout: Option[FiniteDuration]
  ) extends ServiceClient[F] {

    private val baseUri: Option[sttp.model.Uri] = uri"$baseUrl".some

    private val interpreter: SttpClientInterpreter = SttpClientInterpreter()
    private val streamInterpreter: StreamSttpClientInterpreter = StreamSttpClientInterpreter()

    def circuitStates: Stream[F, CircuitEvent] = circuits

    def call[I, O](endpoint: Endpoint[SignedPrincipal, I, ErrorEnvelope, O, Any], input: I)(
        ctx: CallContext
    ): F[Either[KuiError, O]] = {
      def build(token: SignedPrincipal): Request[DecodeResult[Either[ErrorEnvelope, O]]] =
        interpreter.toSecureRequest(endpoint, baseUri).apply(token).apply(input)

      signed(build(Placeholder), ctx).flatMap {
        case Left(error) => Async[F].pure(Left(error))
        case Right(token) =>
          send(decorate(build(token), ctx)).map {
            case Left(error) => Left(error)
            case Right(response) => decoded(response.code.code, response.body)
          }
      }
    }

    private def decoded[O](
        status: Int,
        body: DecodeResult[Either[ErrorEnvelope, O]]
    ): Either[KuiError, O] =
      body match {
        case DecodeResult.Value(Right(output)) => Right(output)
        case DecodeResult.Value(Left(envelope)) => Left(errorOf(service, status, envelope))
        case failure: DecodeResult.Failure =>
          Left(
            InfrastructureError.Unreachable(service.value, s"the response could not be decoded: $failure")
          )
      }

    /** Streaming deliberately bypasses the resilience wrapper that `call` goes through.
      *
      * ADR-037's budget is written for request/response calls: a 10-second call timeout, a bulkhead slot held
      * for the duration of the call, and a circuit breaker that counts a call as failed when it does not
      * finish. A Server-Sent Events stream is meant to stay open for hours, so putting it inside that budget
      * would kill every stream after ten seconds, hold a bulkhead slot per connected browser, and trip the
      * breaker for the request/response traffic that shares it.
      *
      * The protection a stream actually needs is different in kind — the heartbeat and the bounded buffer
      * that `kui.http.sse.Sse.stream` applies on the way out, and the client's own reconnect — so the stream
      * goes out on the plain transport and is protected there instead.
      */
    def callPublic[I, O](endpoint: PublicEndpoint[I, ErrorEnvelope, O, Any], input: I)(
        ctx: CallContext
    ): F[Either[KuiError, O]] =
      send(decorate(interpreter.toRequest(endpoint, baseUri).apply(input), ctx)).map {
        case Left(error) => Left(error)
        case Right(response) => decoded(response.code.code, response.body)
      }

    def stream[I](
        endpoint: Endpoint[SignedPrincipal, I, ErrorEnvelope, Stream[F, Byte], Fs2Streams[F]],
        input: I
    )(ctx: CallContext): Stream[F, SseEvent] = {
      def build(
          token: SignedPrincipal
      ): StreamRequest[DecodeResult[Either[ErrorEnvelope, Stream[F, Byte]]], Fs2Streams[F]] =
        streamInterpreter
          .toSecureRequest[SignedPrincipal, I, ErrorEnvelope, Stream[F, Byte], Fs2Streams[F]](
            endpoint,
            baseUri
          )
          .apply(token)
          .apply(input)

      Stream
        .eval(signed(build(Placeholder), ctx))
        .flatMap {
          case Left(error) => Stream.raiseError[F](UpstreamRejection(error))
          case Right(token) =>
            Stream
              .eval(decorateStream(build(token), ctx).send(streaming))
              .flatMap { response =>
                response.body match {
                  case DecodeResult.Value(Right(bytes)) => bytes.through(SseWire.parse)
                  case DecodeResult.Value(Left(envelope)) =>
                    Stream.raiseError[F](
                      UpstreamRejection(errorOf(service, response.code.code, envelope))
                    )
                  case failure: DecodeResult.Failure =>
                    Stream.raiseError[F](
                      UpstreamRejection(
                        InfrastructureError.Unreachable(
                          service.value,
                          s"the stream could not be opened: $failure"
                        )
                      )
                    )
                }
              }
        }
    }

    /** Mints a token whose audience is this service and whose digest covers this exact request.
      *
      * A signing failure is a typed infrastructure error, not an exception. `sign` raises when no configured
      * key is active yet; leaving that to escape `call` turns every proxied request into an untyped 500 and
      * skips the capability signal. The cause kept here is the exception's class name, the same rule as a
      * transport failure, so it cannot carry a host or a credential.
      */
    private def signed(
        request: GenericRequest[?, ?],
        ctx: CallContext
    ): F[Either[KuiError, SignedPrincipal]] =
      Clock[F].realTimeInstant
        .flatMap { now =>
          principals
            .sign(
              PrincipalClaims(
                subject = ctx.principal.name,
                roles = ctx.principal.roles,
                kind = ctx.principal.kind,
                sessionRef = None,
                issuedAt = now,
                expiresAt = now.plusSeconds(TokenLifetimeSeconds),
                audience = service,
                requestDigest = RequestDigests.of(
                  request.method.method,
                  pathOf(request),
                  bodyBytes(request)
                )
              )
            )
            .attempt
        }
        .map(
          _.leftMap(failure =>
            InfrastructureError.Unreachable(service.value, UpstreamClient.safeFailureCause(failure))
          )
        )

    private def decorate[O](request: Request[O], ctx: CallContext): Request[O] =
      request.headers(edgeHeaders(ctx)*)

    private def decorateStream[O](
        request: StreamRequest[O, Fs2Streams[F]],
        ctx: CallContext
    ): StreamRequest[O, Fs2Streams[F]] =
      request.headers(edgeHeaders(ctx)*)

    private def edgeHeaders(ctx: CallContext): List[Header] =
      Header(Correlation.HeaderName, ctx.correlationId.value) ::
        ctx.cluster.map(cluster => Header(ClusterHeader, cluster.value)).toList

    /** Sends, and gives up.
      *
      * `callTimeout` is `None` for the networked shape, where [[UpstreamClient]] has already applied one
      * further out, and is set for the in-process shape, where nothing else does. Either way a request that
      * does not finish becomes `InfrastructureError.Timeout` — the same error, the same envelope, the same
      * `503` and the same capability signal — which is what ADR-005 requires of the two deployments.
      */
    private def send[O](request: Request[O]): F[Either[KuiError, Response[O]]] = {
      val sent = request.send(calls)

      callTimeout
        .fold(sent)(bound =>
          Async[F].timeoutTo(
            sent,
            bound,
            Async[F].raiseError[Response[O]](
              kui.http.upstream.UpstreamFailure(InfrastructureError.Timeout(service.value, bound.toMillis))
            )
          )
        )
        .attempt
        .map(_.leftMap(transportError))
    }
  }

  /** The token used only to shape the request that is about to be signed. It never leaves the process: the
    * request carrying it is discarded once the digest has been taken from it.
    */
  private val Placeholder: SignedPrincipal = SignedPrincipal.unsafe("unsigned")

  /** Wraps a typed error so it can travel through an `fs2.Stream`, which has no error channel of its own.
    * `ContractRouting` (GW-006) unwraps it again.
    */
  final case class UpstreamRejection(error: KuiError) extends Exception(error.message)

  /** The path exactly as it goes on the wire, which is what the service will hash on the other side. */
  def pathOf(request: GenericRequest[?, ?]): String =
    request.uri.path.mkString("/", "/", "")

  /** The encoded body, for the digest. A body sttp cannot show us as bytes is hashed as empty rather than
    * guessed at: the receiving service hashes what it actually read, so a guess would fail the check on every
    * such request instead of failing loudly here. KUI's contracts encode JSON to a string, so this fallback
    * is unreachable today and exists so that adding a file upload later fails visibly.
    */
  def bodyBytes(request: GenericRequest[?, ?]): Array[Byte] =
    request.body match {
      case StringBody(value, encoding, _) => value.getBytes(encoding)
      case ByteArrayBody(value, _) => value
      case _ => Array.emptyByteArray
    }

  /** Everything that reached us as a thrown exception rather than as a response. */
  def transportError(error: Throwable): KuiError =
    error match {
      case kui.http.upstream.UpstreamFailure(typed) => typed
      case UpstreamRejection(typed) => typed
      case other =>
        InfrastructureError.Unreachable(
          "upstream",
          UpstreamClient.safeFailureCause(other)
        )
    }
}
