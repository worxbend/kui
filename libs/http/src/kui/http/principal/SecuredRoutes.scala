package kui.http.principal

import java.nio.charset.StandardCharsets

import cats.effect.kernel.{Async, Clock, Sync}
import cats.syntax.all.*
import io.circe.syntax.*
import io.circe.{Encoder, Printer}
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.otel4s.metrics.Counter
import sttp.capabilities.fs2.Fs2Streams
import sttp.model.StatusCode
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.{extractFromRequest, statusCode, Endpoint, EndpointInput}

import kui.contracts.ErrorEnvelope
import kui.kernel.ServiceId
import kui.kernel.error.KuiError
import kui.observability.Correlation
import kui.security.{Principal, PrincipalCodec, RequestDigest, RequestDigests, SignedPrincipal}

/** Binding a `/internal/v1` endpoint to the code behind it, with the gateway's signature checked first.
  *
  * ==Why this is one class in `libs/http` and not one per service==
  *
  * Every service's `api` module does the same four things to every endpoint it publishes: read the request
  * line and the correlation id off the request, verify the signed principal against them (ADR-020), decide
  * the HTTP status from the failure with `ErrorEnvelope.statusOf` (ADR-034), and render the envelope. Written
  * per service, those four things are four copies of an authentication check, and rule A11 forbids one
  * service from seeing another's `api` module — so the copies could not even be compared, only re-derived.
  * `PrincipalInterceptor` next door was hoisted here for exactly that reason, and this is the other half of
  * the same job.
  *
  * ==The bodied case (ADR-020 Amendment 1)==
  *
  * [[apply]] verifies in Tapir's *security* stage, which runs before the request body is decoded: an
  * unauthenticated caller is refused without this service parsing a byte of what they sent.
  *
  * That is not possible for an endpoint that carries a body. ADR-020 binds a token to one call by hashing the
  * method, the path **and the body**; the gateway signs the bytes it is about to send, so a service that
  * hashed only the request line would refuse every bodied call as `request_mismatch` — which is exactly what
  * happened the first time the offset-reset wizard ran against a real cluster. Tapir gives the security stage
  * a `ServerRequest`, and a `ServerRequest` does not expose the raw bytes; there is no `extractFromRequest`
  * that can read them.
  *
  * So [[withBody]] verifies one stage later, inside the endpoint's own logic, where the decoded input is in
  * hand, and reconstructs the signed bytes by re-encoding that input with the very codec the gateway encoded
  * it with. The reconstruction is exact rather than approximate: the gateway does not forward a caller's
  * bytes, it re-encodes the decoded value through this same contract (`SttpServiceClient`), and both sides
  * print with circe's `Printer.noSpaces` over a hand-written field order (ADR-007). [[bodyBytes]] is that
  * printer, and it is shared for the same reason this class is — two printers is two hashes.
  *
  * The cost is stated plainly: the body of an unauthenticated request is decoded before the token is checked.
  * It is bounded — a few hundred bytes of JSON through a codec that refuses anything it does not recognise —
  * and it buys back the binding that stops a token minted for one call from being replayed with a different
  * body, which is the property that actually matters on a mutating endpoint.
  *
  * ==Authentication, then authorization==
  *
  * Verifying the signature answers "who is this", and answering it is not the same as deciding what they may
  * do. [[guard]] is the second question, run after the principal is known and before the endpoint's own logic
  * — so a caller who reaches this service directly, bypassing the gateway entirely, is refused here on the
  * same rule the gateway would have applied. A service that trusted its caller would be open to anything that
  * can reach its port.
  *
  * @param service
  *   this service's id, which is the `aud` claim a token must have been minted for. A token for `topic` is
  *   refused by `message`, and that is the check this parameter exists for.
  * @param guard
  *   this deployment's permission rules. The four-argument constructor allows everything, so the gateway is
  *   the only RBAC check. Metrics and identity use that constructor. Every other domain service's wiring
  *   passes a real guard.
  */
final class SecuredRoutes[F[_]: Async](
    principals: PrincipalCodec[F],
    service: ServiceId,
    rejections: Counter[F, Long],
    logger: StructuredLogger[F],
    guard: RbacGuard[F]
) {

  /** The same, with no permission rules: the gateway is the only RBAC check.
    *
    * Metrics and identity use this constructor. Their routes are still signed. It is a constructor rather
    * than a default argument so that `new SecuredRoutes(...)` with four arguments is a visible choice
    * somebody can grep for.
    */
  def this(
      principals: PrincipalCodec[F],
      service: ServiceId,
      rejections: Counter[F, Long],
      logger: StructuredLogger[F]
  ) = this(principals, service, rejections, logger, RbacGuard.allowAll[F])

  import SecuredRoutes.Failure

  /** Binds an endpoint with no request body. Verification happens before anything is decoded. */
  def apply[I, O](endpoint: Endpoint[SignedPrincipal, I, ErrorEnvelope, O, Any])(
      logic: Principal => I => F[Either[KuiError, O]]
  ): ServerEndpoint[Any, F] =
    endpoint
      .securityIn(SecuredRoutes.requestContext)
      .errorOut(statusCode)
      .serverSecurityLogic[(Principal, RequestContext), F] { case (token, ctx) =>
        PrincipalVerification
          .secured[F, Failure](principals, service, logger, rejections)(SecuredRoutes.failure[F])(token, ctx)
      }
      .serverLogic { case (principal, ctx) =>
        input =>
          authorized(endpoint, principal, ctx) {
            logic(principal)(input)
          }.flatMap {
            case Right(value) => value.asRight[Failure].pure[F]
            case Left(error) => SecuredRoutes.failure[F](error, ctx).map(_.asLeft[O])
          }
      }

  /** The same as [[apply]], for a route whose output is a stream.
    *
    * Tapir's `ServerEndpoint` is invariant in its capability parameter once a streaming body is involved, so
    * this cannot be the same method as [[apply]] however similar the body reads. The logic is handed the
    * [[RequestContext]] as well as the principal, because a stream that fails *after* its status line has
    * gone renders the failure into the stream itself and needs the correlation id to do it (ADR-035).
    *
    * A streaming endpoint carries no request body, so its digest is the request line — which is what ADR-020
    * says for streams and what [[requestContext]] already produces.
    */
  def stream[I, O](endpoint: Endpoint[SignedPrincipal, I, ErrorEnvelope, O, Fs2Streams[F]])(
      logic: Principal => RequestContext => I => F[Either[KuiError, O]]
  ): ServerEndpoint[Fs2Streams[F], F] =
    endpoint
      .securityIn(SecuredRoutes.requestContext)
      .errorOut(statusCode)
      .serverSecurityLogic[(Principal, RequestContext), F] { case (token, ctx) =>
        PrincipalVerification
          .secured[F, Failure](principals, service, logger, rejections)(SecuredRoutes.failure[F])(token, ctx)
      }
      .serverLogic { case (principal, ctx) =>
        input =>
          authorized(endpoint, principal, ctx) {
            logic(principal)(ctx)(input)
          }.flatMap {
            case Right(value) => value.asRight[Failure].pure[F]
            case Left(error) => SecuredRoutes.failure[F](error, ctx).map(_.asLeft[O])
          }
      }

  /** Binds an endpoint that carries a request body, hashing that body into the digest the token is checked
    * against (ADR-020 Amendment 1).
    *
    * @param bodyOf
    *   the decoded input as the bytes the gateway signed. It is nearly always [[SecuredRoutes.bodyBytes]]
    *   over the field of the input tuple that the endpoint declared as `jsonBody`; it is a parameter rather
    *   than a `given` because only the route knows which field of a wide input tuple the body is.
    */
  def withBody[I, O](endpoint: Endpoint[SignedPrincipal, I, ErrorEnvelope, O, Any])(
      bodyOf: I => Array[Byte]
  )(logic: Principal => I => F[Either[KuiError, O]]): ServerEndpoint[Any, F] =
    endpoint
      .securityIn(SecuredRoutes.requestContext)
      .errorOut(statusCode)
      // Carries the token through unverified. Nothing downstream can forget to check it: the only thing
      // that can be done with the pair below is to verify it, which either yields a `Principal` or
      // renders the refusal.
      .serverSecurityLogic[(SignedPrincipal, RequestContext), F] { case (token, ctx) =>
        (token, ctx).asRight[Failure].pure[F]
      }
      .serverLogic { case (token, ctx) =>
        input =>
          val bound = ctx.copy(
            digest = RequestDigests.of(ctx.digest.method, ctx.digest.path, bodyOf(input))
          )

          PrincipalVerification.verify[F](principals, service, logger, rejections)(token, bound).flatMap {
            case Left(error) => SecuredRoutes.failure[F](error, bound).map(_.asLeft[O])
            case Right(principal) =>
              authorized(endpoint, principal, bound) {
                logic(principal)(input)
              }.flatMap {
                case Right(value) => value.asRight[Failure].pure[F]
                case Left(error) => SecuredRoutes.failure[F](error, bound).map(_.asLeft[O])
              }
          }
      }

  /** The permission check, run between "who is this" and "do the work".
    *
    * It short-circuits: the endpoint's logic is a by-name parameter and is never evaluated for a refused
    * call, so a denied request costs this service exactly one decision over data it already had.
    */
  private def authorized[O](endpoint: Endpoint[?, ?, ?, ?, ?], principal: Principal, ctx: RequestContext)(
      logic: => F[Either[KuiError, O]]
  ): F[Either[KuiError, O]] =
    guard.authorize(principal, endpoint, ctx.digest.path).flatMap {
      case Right(_) => logic
      case Left(refusal) => refusal.asLeft[O].pure[F]
    }
}

object SecuredRoutes {

  /** The two halves of an error response: the body the contract fixes and the status the code decides. */
  type Failure = (ErrorEnvelope, StatusCode)

  /** Reads what verification and error reporting need off the request, once.
    *
    * The digest here covers the request *line* only. A bodied endpoint completes it in
    * [[SecuredRoutes .withBody]], where the decoded input is available; a body-less one is complete as it
    * stands, and `RequestDigest.ofRequestLine` says so with a written-down constant rather than with an empty
    * string somebody could change.
    */
  val requestContext: EndpointInput[RequestContext] =
    extractFromRequest[RequestContext](request =>
      RequestContext(
        RequestDigest.ofRequestLine(request.method.method, request.uri.path.mkString("/", "/", "")),
        request.header(Correlation.HeaderName).flatMap(Correlation.accept)
      )
    )

  /** One failure, as the status and the envelope every KUI error response uses (ADR-034). */
  def failure[F[_]: Sync](error: KuiError, ctx: RequestContext): F[Failure] =
    for {
      correlationId <- ctx.correlationId.fold(Correlation.newRandom[F])(_.pure[F])
      now <- Clock[F].realTimeInstant
    } yield (
      ErrorEnvelope.of(error, correlationId, now),
      StatusCode(ErrorEnvelope.statusOf(error))
    )

  /** A request body as the exact bytes the gateway hashed.
    *
    * `Printer.noSpaces` and the contract's own encoder, which is what `jsonBody` uses on both sides of the
    * hop. A different printer here — pretty-printing, dropping nulls, sorting keys — produces a different
    * hash and refuses every call with a 401 naming nothing, so this must not be "tidied".
    */
  def bodyBytes[A: Encoder](value: A): Array[Byte] =
    Printer.noSpaces.print(value.asJson).getBytes(StandardCharsets.UTF_8)
}
