package kui.metrics.app

import cats.effect.{ExitCode, IO, IOApp}

import kui.http.ServiceMain
import kui.metrics.api.MetricsApi

/** The metrics service process.
  *
  * `IO` appears here and nowhere else in the service (ADR-010). The startup sequence is [[ServiceMain]]'s,
  * shared with every other service process; what is left here is the part that is about metrics.
  *
  * ==It starts even when nothing is configured, and most deployments are in that state==
  *
  * Standalone deployments name `kui.clusterProfiles.url` (or `kui.gateway.services.cluster.url`) for the
  * authoritative, live cluster list. A deployment that names no `kui.metrics.sources` entry still starts this
  * process, still serves its routes, and reports every cluster as `not_configured` — which is the fact the
  * dashboard needs in order to keep its written "not measured" sentences rather than draw five empty axes. A
  * process that exited because it had nothing to measure would restart-loop in every deployment there is.
  *
  * ==Why there is no URL policy parameter here==
  *
  * `MetricsWiring.make` reads `KUI_ALLOW_PRIVATE_UPSTREAMS` for itself, so that this process and the
  * all-in-one cannot apply two different address rules to the same configuration file. Passing one down from
  * here would be a second opinion about a security control, which is exactly what `UrlPolicy`'s own scaladoc
  * argues against.
  */
object Main extends IOApp {

  def run(args: List[String]): IO[ExitCode] =
    ServiceMain.run(
      MetricsApi.ServiceName,
      args,
      (config, telemetry, principals, logger) =>
        MetricsWiring
          .standalone[IO](config, telemetry, principals, logger)
          .map(service => ServiceMain.Serving(service.routes, service.interceptors))
    )
}
