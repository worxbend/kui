package kui.topic.app

import cats.effect.{ExitCode, IO, IOApp}

import kui.http.ServiceMain
import kui.topic.api.TopicApi

/** The topic service process.
  *
  * `IO` appears here and nowhere else in the service (ADR-010). Every layer beneath is written against an
  * abstract `F[_]`, which is not an aesthetic preference: it is what lets a suite run the same code on a
  * deterministic clock, and what lets the all-in-one deployment run this same wiring inside a different
  * process.
  *
  * The startup sequence — configuration, log format, principal codec, telemetry, wiring, listener, drain — is
  * [[ServiceMain]]'s and is shared with every other service process, so what is left here is only the part
  * that is about topics: which settings this service reads and what it builds from them.
  *
  * The settings are four, and the two that are not obvious are worth saying out loud:
  *
  *   - `kui.topics.refreshInterval` and not `kui.consumers.refreshInterval`. Sweeping a cluster's topics and
  *     describing its consumer groups are different costs against different broker paths, and one knob for
  *     both would mean tuning the cheap one by the expensive one.
  *   - `kui.streaming.cursorKey`, which signs the plan tokens that confirm a topic deletion or a partition
  *     increase (ADR-045). It is the same secret ADR-026 already makes an operator configure for streaming
  *     cursors: one secret and one rotation procedure, with this service's use kept apart from the consumer
  *     service's by the operation name inside the payload. Running more than one replica without configuring
  *     it means a confirmation minted by one replica is refused by the other, and `TopicWiring` says so in
  *     the log rather than failing quietly.
  *   - `kui.clusterProfiles.url`, or else `kui.gateway.services.cluster.url`. Either one is the cluster list.
  *     With neither, the process still starts and serves the static `kui.clusters[]` list, and the log says
  *     that clusters created in the cluster service will not appear here.
  */
object Main extends IOApp {

  def run(args: List[String]): IO[ExitCode] =
    ServiceMain.run(
      TopicApi.ServiceName,
      args,
      (config, telemetry, principals, logger) =>
        TopicWiring
          .standalone[IO](config, telemetry, principals, logger)
          .map(service => ServiceMain.Serving(service.routes, service.interceptors))
    )
}
