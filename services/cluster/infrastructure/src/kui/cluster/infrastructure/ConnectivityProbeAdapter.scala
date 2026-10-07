package kui.cluster.infrastructure

import scala.concurrent.duration.*

import cats.effect.kernel.{Async, Resource}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger

import kui.cluster.domain.{ClusterProfile, Connectivity, ConnectivityProbe}
import kui.kafka.admin as adm
import kui.kernel.error.{ErrorCode, KuiError}

/** "Can KUI talk to this cluster, and if not, what kind of no?"
  *
  * One `describeCluster` under a short bound of its own. Short is the whole point: the dashboard asks this
  * about every configured cluster, and the milestone's exit criterion is that a dead cluster does not make
  * the healthy ones slow. The admin client's own `default.api.timeout.ms` is a minute and bounds a *useful*
  * request; a probe that took a minute to say "down" would make the dashboard exactly as slow as the dead
  * cluster.
  *
  * `describeCluster` rather than a TCP connect (which proves nothing about SASL or TLS), rather than
  * `listTopics` (which needs authorization a probe should not require), and rather than a produce (which
  * writes). It exercises the whole connection path — DNS, TCP, the TLS handshake, the SASL handshake, a
  * metadata request — which is the entire set of things a misconfiguration breaks.
  */
final class ConnectivityProbeAdapter[F[_]: Async](
    admin: Resource[F, adm.ClusterAdmin[F]],
    logger: StructuredLogger[F]
) extends ConnectivityProbe[F] {

  def probe(profile: ClusterProfile): F[Connectivity] = {
    val bound = ConnectivityProbeAdapter.timeoutFor(profile)

    admin
      .use(_.describeCluster(ClusterProfileConnection.of(profile)))
      .timeoutTo(bound, Async[F].pure(Left(ConnectivityProbeAdapter.timedOut(bound))))
      .attempt
      .flatMap {
        case Right(Right(_)) => Async[F].pure(Connectivity.Reachable)
        case Right(Left(error)) => refused(profile, bound, error)
        // An exception this far out is a defect in `libs/kafka`, not an answer about the cluster; it is
        // still not allowed to take a dashboard row down, so it is recorded and reported as unreachable.
        case Left(failure) =>
          logger
            .warn(
              s"the connectivity probe for cluster ${profile.id.value} raised " +
                failure.getClass.getName
            )
            .as(Connectivity.Unreachable(ConnectivityProbeAdapter.CouldNotConnect))
      }
  }

  private def refused(
      profile: ClusterProfile,
      bound: FiniteDuration,
      error: KuiError
  ): F[Connectivity] =
    logger
      .info(s"cluster ${profile.id.value} is not reachable: ${error.code.wire}")
      .as(ConnectivityProbeAdapter.verdictOf(error, bound))
}

object ConnectivityProbeAdapter {

  /** The probe's own bound, and the reason it is not the admin client's.
    *
    * `AdminTuning` has no probe-specific field yet; CFGOP-002 owns that configuration key and this module
    * must not add one. Until it exists the bound is the smaller of the cluster's own request timeout and five
    * seconds, so that a cluster deliberately configured to answer quickly is not slowed down to five seconds
    * and a cluster configured with a long timeout does not lend it to the probe.
    */
  val DefaultProbeTimeout: FiniteDuration = 5.seconds

  def timeoutFor(profile: ClusterProfile): FiniteDuration =
    profile.admin.requestTimeout.min(DefaultProbeTimeout)

  // The fixed sentences. `detail` is display text: a raw Kafka message can carry the bootstrap string and,
  // on some SASL paths, the principal, so nothing derived from an exception is ever interpolated here. The
  // only substitution in the whole set is the probe's own bound, which is KUI's number and not the
  // cluster's.
  val CouldNotConnect: String = "KUI could not open a connection to this cluster"
  val CredentialsRejected: String = "the cluster rejected KUI's credentials"
  val NotAuthorized: String = "the cluster accepted KUI but refused the request"

  def timedOutDetail(bound: FiniteDuration): String =
    s"the cluster did not answer within ${bound.toSeconds} seconds"

  /** The classification, derived from the `KuiError` the admin port already produced.
    *
    * The Kafka exception hierarchy is examined in exactly one place in KUI — `KafkaErrorMapper` — and this is
    * not it. Keeping the judgement here on the error *code* is what stops a second, drifting copy of that
    * table from existing.
    */
  def verdictOf(error: KuiError, bound: FiniteDuration): Connectivity = error.code match {
    case ErrorCode.UpstreamAuth => Connectivity.AuthenticationFailed(CredentialsRejected)
    case ErrorCode.Timeout => Connectivity.Unreachable(timedOutDetail(bound))
    // A cluster that answered and refused the request is reachable. Reporting it as unreachable would send
    // an operator to the network when the answer is an ACL, and would grey out a row that is working.
    case ErrorCode.Forbidden => Connectivity.AuthenticationFailed(NotAuthorized)
    case _ => Connectivity.Unreachable(CouldNotConnect)
  }

  private def timedOut(bound: FiniteDuration): KuiError =
    kui.kernel.error.InfrastructureError.Timeout("describeCluster", bound.toMillis)
}
