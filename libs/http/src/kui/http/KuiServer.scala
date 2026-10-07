package kui.http

import java.net.BindException

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import io.netty.handler.codec.http.{HttpDecoderConfig, HttpServerCodec}
import org.typelevel.log4cats.StructuredLogger
import sttp.capabilities.fs2.Fs2Streams
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.interceptor.Interceptor
import sttp.tapir.server.netty.NettyConfig
import sttp.tapir.server.netty.cats.{NettyCatsServer, NettyCatsServerOptions}

import kui.config.ServerConfig

/** The HTTP server every KUI process runs.
  *
  * It is one function rather than something each `app` assembles for itself, because "how this process serves
  * HTTP" is a decision the product makes once: the same error body, the same base path handling, the same
  * interceptor order, the same behaviour when the port is taken. An operator who has learned one KUI service
  * has learned all of them, and that only stays true if there is one place where it is decided.
  *
  * The server is `tapir-netty-server-cats` (ADR-003). The long-lived streaming that KUI depends on was
  * measured on exactly this stack before it was committed to: a connection held open past ten minutes carried
  * 612 events with no drift, each flushed individually, and the producing fiber was cancelled 8 ms after the
  * client went away.
  */
object KuiServer {

  /** How long a stopping server waits for in-flight requests. See `resource`'s `gracefulShutdown`. */
  val DefaultGracefulShutdown: FiniteDuration = 10.seconds

  /** An 8 KiB cursor needs additional space for the route, prefix, query options and HTTP framing. Keep this
    * envelope aligned with deployment/frontend/entrypoint.sh and any outer proxy.
    */
  val MaxRequestLineBytes: Int = 16 * 1024

  /** How long a request may take before Netty stops waiting for the handler and closes the connection.
    *
    * Set here rather than inherited from Tapir's default, because it is one half of a pair and the pair has
    * to be read together. Netty's answer to a handler that overruns is a bare `503`: no body, no `KUI-...`
    * code, no correlation id and no `X-Kui-Correlation-Id` header — the one failure shape KUI never otherwise
    * produces, and one a browser can only report as its own inability to read the answer.
    *
    * So nothing KUI serves may be allowed to reach it. The other half of the pair is the gateway's
    * per-service call timeout (`UpstreamServiceConfig.DefaultTimeout`, ten seconds, applied in both
    * deployment shapes by `SttpServiceClient`), which is what actually bounds a request that is waiting on a
    * Kafka broker that has gone away. Raise a service's timeout above this number and the server starts
    * answering first.
    *
    * Thirty rather than Tapir's own twenty, so that the ten-second call bound has room to be exceeded and
    * still lose the race. Two browser requests for the same unreachable cluster queue behind one Kafka
    * client: the second waits for the first to give up and then times out itself, and one measured against
    * the quickstart with its broker stopped took 20.008 seconds end to end. At Tapir's default that is a coin
    * toss between KUI's answer and Netty's.
    *
    * It does not bound a Server-Sent Events stream: the response begins immediately and the timeout is on
    * beginning it, not on finishing it.
    */
  val DefaultResponseTimeout: FiniteDuration = 30.seconds

  /** Where the server actually ended up listening.
    *
    * The port is worth returning rather than assuming: a test binds port `0` and asks the operating system to
    * choose, and this is how it learns which one it got.
    */
  final case class ServerBinding(host: String, port: Int, basePath: String)

  object ServerBinding {
    given CanEqual[ServerBinding, ServerBinding] = CanEqual.derived
  }

  /** Starts the server, and stops it when the resource closes.
    *
    * The endpoints are prefixed with the configured base path here, so no contract and no service has to know
    * it exists.
    *
    * @param endpoints
    *   the routes to serve, in the order they should be matched
    * @param interceptors
    *   the cross-cutting concerns, outermost first — correlation, tracing, metrics, then the error handling
    *   from [[ErrorInterceptor]]. They are supplied rather than assembled here so a composition root can add
    *   its own (authentication, in the gateway) without this module knowing about them.
    * @param gracefulShutdown
    *   how long a stopping server waits for the requests already in flight to finish. Ten seconds in
    *   production, so a rolling deployment does not cut anyone off mid-response; suites set it to a few
    *   milliseconds, because a suite that starts and stops thirty servers would otherwise spend five minutes
    *   waiting for connections that are already closed.
    */
  def resource[F[_]: Async](
      config: ServerConfig,
      endpoints: List[ServerEndpoint[Fs2Streams[F], F]],
      interceptors: List[Interceptor[F]],
      logger: StructuredLogger[F],
      gracefulShutdown: FiniteDuration = DefaultGracefulShutdown
  ): Resource[F, ServerBinding] = {
    val basePath = BasePath.normalize(config.basePath)

    for {
      dispatcher <- Dispatcher.parallel[F]
      routes = BasePath.prefixAll(basePath, endpoints)
      options = NettyCatsServerOptions.default[F](dispatcher).copy(interceptors = interceptors)
      server = NettyCatsServer[F](
        options,
        NettyConfig.default
          .withGracefulShutdownTimeout(gracefulShutdown)
          .copy(requestTimeout = Some(DefaultResponseTimeout))
          .initPipeline { config => (pipeline, handler) =>
            // Retain Tapir's TLS, streaming and compression handlers; customize only the decoder limit.
            NettyConfig.defaultInitPipeline(config)(pipeline, handler)
            val codecName = pipeline.context(classOf[HttpServerCodec]).name()
            val _ = pipeline.replace(
              classOf[HttpServerCodec],
              codecName,
              new HttpServerCodec(new HttpDecoderConfig().setMaxInitialLineLength(MaxRequestLineBytes))
            )
            ()
          }
      )
        .host(config.host.value)
        .port(config.port.value)
        .addEndpoints(routes)
      binding <- Resource.make(start(server, config, logger))(b => Async[F].defer(b.stop()))
      _ <- Resource.eval(
        logger.info(
          Map("host" -> binding.hostName, "port" -> binding.port.toString, "basePath" -> basePath)
        )(s"listening on http://${binding.hostName}:${binding.port}${basePath}")
      )
    } yield ServerBinding(binding.hostName, binding.port, basePath)
  }

  /** Starts, and turns "the port is in use" into a message that says which port.
    *
    * The process then exits non-zero. It never retries on another port: a server that quietly moved to 8081
    * is a server whose health check passes, whose logs look normal, and which nothing can reach — far worse
    * to diagnose than a process that refused to start and said why.
    */
  private def start[F[_]: Async](
      server: NettyCatsServer[F],
      config: ServerConfig,
      logger: StructuredLogger[F]
  ): F[sttp.tapir.server.netty.cats.NettyCatsServerBinding[F]] =
    server.start().handleErrorWith {
      case bind: BindException =>
        val message =
          s"cannot bind ${config.host.value}:${config.port.value} — the port is already in use. " +
            "KUI does not fall back to another port; free this one or configure kui.server.port."
        logger.error(bind)(message) *> Async[F].raiseError(new BindException(message))
      case other => Async[F].raiseError(other)
    }
}
