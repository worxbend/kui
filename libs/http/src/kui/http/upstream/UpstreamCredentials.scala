package kui.http.upstream

import java.nio.charset.StandardCharsets
import java.util.Base64

import scala.concurrent.duration.{DurationInt, DurationLong, FiniteDuration}

import cats.effect.Deferred
import cats.effect.kernel.{Async, Clock, Outcome, Ref, Resource}
import cats.effect.std.Supervisor
import cats.effect.syntax.all.*
import cats.syntax.all.*
import io.circe.parser
import sttp.client4.*
import sttp.model.{HeaderNames, Uri}

import kui.config.{HttpTlsConfig, UpstreamAuthConfig, UrlPolicy}
import kui.kernel.Secret
import kui.kernel.error.{ErrorCode, InfrastructureError, KuiError}

/** Credentials applied to one upstream HTTP request.
  *
  * The configured value is authoritative: Basic and bearer authentication replace every caller-supplied
  * `Authorization` header, so a request can never leave with two competing credentials. Implementations
  * render only [[describe]], never the header value they hold.
  */
sealed trait UpstreamCredentials[F[_]] {
  def authenticate[T](request: Request[T]): F[Either[UpstreamCredentials.Failure, Request[T]]]

  def authenticateStream[T, S](
      request: StreamRequest[T, S]
  ): F[Either[UpstreamCredentials.Failure, StreamRequest[T, S]]]

  /** Safe for logs and diagnostics. */
  def describe: String

  final override def toString: String = s"UpstreamCredentials($describe)"
}

object UpstreamCredentials {

  sealed trait Failure extends Product with Serializable {
    def message: String
  }

  object Failure {
    final case class TimedOut(after: FiniteDuration) extends Failure {
      val message: String = s"the OAuth token endpoint did not answer within $after"
    }

    final case class Rejected(statusCode: Int) extends Failure {
      val message: String = s"the OAuth token endpoint refused the request with HTTP $statusCode"
    }

    final case class ResponseTooLarge(limitBytes: Long) extends Failure {
      val message: String = s"the OAuth token response exceeded the $limitBytes-byte limit"
    }

    final case class Malformed(reason: MalformedReason) extends Failure {
      val message: String = s"the OAuth token response was malformed: ${reason.message}"
    }

    case object Transport extends Failure {
      val message: String = "the OAuth token endpoint failed at the transport boundary"
    }
  }

  enum MalformedReason {
    case InvalidJson
    case MissingToken
    case EmptyToken
    case InvalidToken
    case MissingExpiry
    case InvalidExpiry

    def message: String = this match {
      case InvalidJson => "invalid JSON"
      case MissingToken => "missing access_token"
      case EmptyToken => "empty access_token"
      case InvalidToken => "access_token is not a valid bearer token"
      case MissingExpiry => "missing expires_in"
      case InvalidExpiry => "expires_in must be a positive, bounded whole number of seconds"
    }
  }

  final case class Settings(
      requestTimeout: FiniteDuration = DefaultRequestTimeout,
      maxResponseBytes: Long = DefaultMaxResponseBytes,
      refreshBefore: FiniteDuration = DefaultRefreshBefore
  ) {
    require(requestTimeout > 0.seconds, "the OAuth request timeout must be positive")
    require(maxResponseBytes > 0L, "the OAuth response limit must be positive")
    require(refreshBefore >= 0.seconds, "the OAuth refresh margin must not be negative")
  }

  val DefaultRequestTimeout: FiniteDuration = 5.seconds
  val DefaultMaxResponseBytes: Long = 64L * 1024L
  val DefaultRefreshBefore: FiniteDuration = 30.seconds
  val MaxTokenLifetime: FiniteDuration = 365.days

  def anonymous[F[_]: Async]: UpstreamCredentials[F] = new Anonymous[F]

  def basic[F[_]: Async](username: String, password: Secret[String]): UpstreamCredentials[F] = {
    val encoded = Base64.getEncoder.encodeToString(
      s"$username:${password.value}".getBytes(StandardCharsets.UTF_8)
    )
    new HeaderCredentials[F](s"Basic $encoded", "basic")
  }

  def bearer[F[_]: Async](token: Secret[String]): UpstreamCredentials[F] =
    new HeaderCredentials[F](s"Bearer ${token.value}", "bearer")

  /** Build credentials for the static cases in [[UpstreamAuthConfig]].
    *
    * OAuth is deliberately `None` here because it owns an effectful token client; [[resource]] constructs
    * every mechanism behind one uniform interface.
    */
  def static[F[_]: Async](config: UpstreamAuthConfig): Option[UpstreamCredentials[F]] =
    config match {
      case UpstreamAuthConfig.Anonymous => Some(anonymous[F])
      case UpstreamAuthConfig.Basic(username, password) => Some(basic[F](username, password))
      case UpstreamAuthConfig.Bearer(token) => Some(bearer[F](token))
      case _: UpstreamAuthConfig.OAuth => None
    }

  /** Translate a credential-source failure without exposing response bodies, credentials, or endpoint URLs.
    */
  def toKuiError(upstream: String, failure: Failure): KuiError =
    failure match {
      case Failure.Rejected(status) if status == 401 || status == 403 =>
        InfrastructureError.AuthFailed(upstream)
      case Failure.Rejected(status) => InfrastructureError.Upstream(upstream, status)
      case Failure.TimedOut(_) | Failure.Transport =>
        InfrastructureError.Unreachable(upstream, failure.message)
      case Failure.ResponseTooLarge(_) | Failure.Malformed(_) =>
        InfrastructureError.Remote(ErrorCode.UpstreamAuth, s"$upstream: ${failure.message}", Nil)
    }

  /** Credentials using a source-owned token transport with the JVM's default trust configuration. */
  def resource[F[_]: Async](
      config: UpstreamAuthConfig,
      settings: Settings = Settings(),
      policy: UrlPolicy = UrlPolicy.Strict
  ): Resource[F, UpstreamCredentials[F]] =
    static[F](config) match {
      case Some(credentials) => Resource.pure(credentials)
      case None =>
        config match {
          case oauth: UpstreamAuthConfig.OAuth =>
            HttpTls.resource[F](HttpTlsConfig.Default, policy).flatMap(withBackend(oauth, _, settings))
          case _ => Resource.eval(Async[F].raiseError(new IllegalStateException("unreachable auth case")))
        }
    }

  /** Test seam for the token endpoint transport. Production uses [[resource]] and JVM trust. */
  private[kui] def withBackend[F[_]: Async](
      config: UpstreamAuthConfig.OAuth,
      backend: Backend[F],
      settings: Settings = Settings()
  ): Resource[F, UpstreamCredentials[F]] =
    for {
      state <- Resource.eval(Ref.of[F, OAuthState[F]](OAuthState(None, None)))
      supervisor <- Supervisor[F]
    } yield new OAuthCredentials[F](config, backend, settings, state, supervisor)

  final private class Anonymous[F[_]: Async] extends UpstreamCredentials[F] {
    def authenticate[T](request: Request[T]): F[Either[Failure, Request[T]]] =
      request
        .withHeaders(request.headers.filterNot(_.is(HeaderNames.Authorization)))
        .asRight[Failure]
        .pure[F]

    def authenticateStream[T, S](
        request: StreamRequest[T, S]
    ): F[Either[Failure, StreamRequest[T, S]]] =
      request
        .withHeaders(request.headers.filterNot(_.is(HeaderNames.Authorization)))
        .asRight[Failure]
        .pure[F]

    val describe: String = UpstreamAuthConfig.Anonymous.describe
  }

  final private class HeaderCredentials[F[_]: Async](value: String, val describe: String)
      extends UpstreamCredentials[F] {
    def authenticate[T](request: Request[T]): F[Either[Failure, Request[T]]] =
      request
        .header(HeaderNames.Authorization, value, DuplicateHeaderBehavior.Replace)
        .asRight[Failure]
        .pure[F]

    def authenticateStream[T, S](
        request: StreamRequest[T, S]
    ): F[Either[Failure, StreamRequest[T, S]]] =
      request
        .header(HeaderNames.Authorization, value, DuplicateHeaderBehavior.Replace)
        .asRight[Failure]
        .pure[F]
  }

  final private case class CachedToken(token: Secret[String], refreshAt: FiniteDuration)
  final private case class OAuthState[F[_]](
      cached: Option[CachedToken],
      refreshing: Option[Deferred[F, Either[Failure, CachedToken]]]
  )

  final private class OAuthCredentials[F[_]: Async](
      config: UpstreamAuthConfig.OAuth,
      backend: Backend[F],
      settings: Settings,
      state: Ref[F, OAuthState[F]],
      supervisor: Supervisor[F]
  ) extends UpstreamCredentials[F] {

    val describe: String = OAuthDescription

    def authenticate[T](request: Request[T]): F[Either[Failure, Request[T]]] =
      token.map(
        _.map(value =>
          request.header(
            HeaderNames.Authorization,
            s"Bearer ${value.value}",
            DuplicateHeaderBehavior.Replace
          )
        )
      )

    def authenticateStream[T, S](
        request: StreamRequest[T, S]
    ): F[Either[Failure, StreamRequest[T, S]]] =
      token.map(
        _.map(value =>
          request.header(
            HeaderNames.Authorization,
            s"Bearer ${value.value}",
            DuplicateHeaderBehavior.Replace
          )
        )
      )

    private def token: F[Either[Failure, Secret[String]]] =
      for {
        now <- Clock[F].monotonic
        candidate <- Deferred[F, Either[Failure, CachedToken]]
        decision <- state.modify { current =>
          current.cached.filter(_.refreshAt > now) match {
            case Some(cached) => (current, Left(Right(cached)))
            case None =>
              current.refreshing match {
                case Some(running) => (current, Left(Left(running)))
                case None => (current.copy(refreshing = Some(candidate)), Right(candidate))
              }
          }
        }
        result <- decision match {
          case Left(Right(cached)) => cached.asRight[Failure].pure[F]
          case Left(Left(running)) => running.get
          case Right(mine) => supervisor.supervise(refresh(mine)).void >> mine.get
        }
      } yield result.map(_.token)

    private def refresh(gate: Deferred[F, Either[Failure, CachedToken]]): F[Unit] =
      fetch.guaranteeCase {
        case Outcome.Succeeded(result) => result.flatMap(complete(gate, _))
        case Outcome.Errored(_) | Outcome.Canceled() => complete(gate, Left(Failure.Transport))
      }.void

    private def complete(
        gate: Deferred[F, Either[Failure, CachedToken]],
        result: Either[Failure, CachedToken]
    ): F[Unit] =
      state.update(current => OAuthState(result.toOption.orElse(current.cached), None)) >>
        gate.complete(result).void

    private def fetch: F[Either[Failure, CachedToken]] = {
      val form = Map("grant_type" -> "client_credentials") ++ config.scope.map("scope" -> _).toMap
      val sent = basicRequest
        .post(endpoint)
        .auth
        .basic(config.clientId, config.clientSecret.value)
        .body(form)
        .followRedirects(false)
        .readTimeout(settings.requestTimeout)
        // The JVM backend enforces this while receiving the stream. ResponseAsInputStream is not supported
        // by HttpClientFs2Backend, so the shared OAuth transport must cap before `asStringAlways` instead of
        // delegating to BoundedResponse's InputStream reader.
        .maxResponseBodyLength(settings.maxResponseBytes)
        .response(asStringAlways)

      sent
        .send(backend)
        .attempt
        .flatMap {
          case Left(error) if UpstreamClient.isResponseLimitCause(error) =>
            Failure.ResponseTooLarge(settings.maxResponseBytes).asLeft[CachedToken].pure[F]
          case Left(_) =>
            Failure.Transport.asLeft[CachedToken].pure[F]
          // BackendStub does not enforce request options, and a non-JVM backend may only expose a
          // materialized body. Keep a defensive byte check here while production's JVM backend rejects the
          // stream before allocation.
          case Right(response)
              if response.body.getBytes(StandardCharsets.UTF_8).length.toLong > settings.maxResponseBytes =>
            Failure.ResponseTooLarge(settings.maxResponseBytes).asLeft[CachedToken].pure[F]
          case Right(response) if !response.code.isSuccess =>
            Failure.Rejected(response.code.code).asLeft[CachedToken].pure[F]
          case Right(response) => parse(response.body)
        }
        .timeoutTo(
          settings.requestTimeout,
          Failure.TimedOut(settings.requestTimeout).asLeft[CachedToken].pure[F]
        )
    }

    private def parse(body: String): F[Either[Failure, CachedToken]] =
      Clock[F].monotonic.map { now =>
        parser.parse(body).leftMap(_ => Failure.Malformed(MalformedReason.InvalidJson)).flatMap { json =>
          val cursor = json.hcursor
          val token = cursor
            .get[String]("access_token")
            .leftMap(_ => Failure.Malformed(MalformedReason.MissingToken))
            .flatMap(value =>
              if value.trim.isEmpty then Failure.Malformed(MalformedReason.EmptyToken).asLeft
              else if !BearerTokenPattern.matcher(value).matches() then
                Failure.Malformed(MalformedReason.InvalidToken).asLeft
              else Secret(value).asRight
            )
          val expiryCursor = cursor.downField("expires_in")
          val expires = expiryCursor.focus match {
            case None => Failure.Malformed(MalformedReason.MissingExpiry).asLeft
            case Some(value) =>
              value.asNumber.flatMap(_.toLong) match {
                case Some(seconds) if seconds > 0L && seconds <= MaxTokenLifetime.toSeconds =>
                  seconds.seconds.asRight
                case _ => Failure.Malformed(MalformedReason.InvalidExpiry).asLeft
              }
          }

          (token, expires).mapN { (value, lifetime) =>
            val margin = settings.refreshBefore.min(lifetime / 2L)
            CachedToken(value, now + lifetime - margin)
          }
        }
      }

    private val endpoint: Uri = Uri.parse(config.tokenEndpoint.value).toOption.get
  }

  private val OAuthDescription = "oauth client credentials"
  private val BearerTokenPattern = java.util.regex.Pattern.compile("[A-Za-z0-9._~+/-]+={0,}")
}
