package kui.alerts.app

import cats.effect.{ExitCode, IO, IOApp}

import kui.alerts.api.AlertsApi
import kui.http.ServiceMain

/** The alerts service process.
  *
  * `IO` appears here and nowhere else in the service (ADR-010). The startup sequence is [[ServiceMain]]'s,
  * shared with every other service process; what is left here is the part that is about alerts.
  *
  * ==It starts even when nothing is configured==
  *
  * An empty authoritative profile list still starts this process and serves empty feeds. Standalone
  * deployments must name `kui.clusterProfiles.url` (or `kui.gateway.services.cluster.url`); local
  * `kui.clusters[]` is not a substitute for that live source. A temporarily unreachable profile service is
  * handled by the shared client's last-known-state and reconnect policy.
  *
  * ==Why `kui.alerts` has no on-switch to read here==
  *
  * Every rule reads a fact this product already measures, so alerting is on for every deployment and the only
  * question is where the lines are drawn — `AlertsConfig`'s own scaladoc, and the reason there is no
  * `enabled` key for this file to consult.
  */
object Main extends IOApp {

  def run(args: List[String]): IO[ExitCode] =
    ServiceMain.run(
      AlertsApi.ServiceName,
      args,
      (config, telemetry, principals, logger) =>
        AlertsWiring
          .standalone[IO](config, telemetry, principals, logger)
          .map(service => ServiceMain.Serving(service.routes, service.interceptors))
    )
}
