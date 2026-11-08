package kui.kafka.auth

import cats.effect.{Async, Resource}
import cats.syntax.all.*
import fs2.io.file.Files
import org.typelevel.log4cats.Logger

import kui.kernel.cluster.{ClientProperties, ClusterConnection}
import kui.kernel.error.{DomainError, FieldError, KuiError}

/** Check, materialize, render — the one entry point every client factory in `libs/kafka` uses.
  *
  * It is a `Resource` because the properties are only valid while the materialized files exist. A
  * `ClientProperties` value that outlived its truststore would name a path that is no longer there, and the
  * SSL error a Kafka client then produces names the file rather than the mistake.
  *
  * The order is check, then materialize, then render, and it is deliberate: a deployment that configured AWS
  * MSK IAM without the library fails before a private key has been written to any filesystem.
  */
object ConnectionProperties {

  def resource[F[_]: {Async, Files}](
      connection: ClusterConnection,
      purpose: ClientPurpose,
      clientId: String,
      log: Option[Logger[F]] = None
  ): Resource[F, Either[KuiError, ClientProperties]] =
    Resource
      .eval(checkClasspath(connection))
      .flatMap {
        case Left(error) => Resource.pure[F, Either[KuiError, ClientProperties]](Left(error))
        case Right(()) =>
          KeyStoreMaterializer
            .resource[F](connection)
            .evalMap {
              case Left(error) =>
                Async[F].pure(Left(error): Either[KuiError, ClientProperties])
              case Right(materialized) =>
                report(connection, materialized, log).map { _ =>
                  renderWith(connection, purpose, clientId, materialized) match {
                    case right @ Right(properties) =>
                      // After render, so an override that replaced the token endpoint is what gets
                      // listed. Kafka reads the list when the client is constructed, which is after
                      // this resource yields.
                      OAuthBearerAllowList.permit(properties)
                      right
                    case left => left
                  }
                }
            }
      }

  /** Every SASL mechanism the connection uses has its classes on this classpath. */
  private def checkClasspath[F[_]: Async](
      connection: ClusterConnection
  ): F[Either[KuiError, Unit]] =
    connection.security.saslMechanism.fold(Async[F].pure(Right(())))(
      CloudHandlers.check[F](connection.id, _)
    )

  private def renderWith(
      connection: ClusterConnection,
      purpose: ClientPurpose,
      clientId: String,
      materialized: Map[ClientPropertyRenderer.StoreRole, String]
  ): Either[KuiError, ClientProperties] =
    ClientPropertyRenderer
      .render(connection, purpose, clientId, materialized)
      .left
      // Every bad field, not the first: this error reaches CFGOP-001's accumulator, and the
      // milestone's exit criteria require a startup message that names all of them at once.
      .map(errors =>
        DomainError.InvariantViolation(
          errors.toList.map(_.message).mkString("; "),
          errors.toList.map(FieldError.fromValidation)
        )
      )

  /** The two lines an operator needs when a handshake fails, and nothing else.
    *
    * The paths are safe to print — they are directories KUI created — and they are the first thing to look at
    * when a Kafka client reports a truststore it could not read. The warning is here because turning
    * certificate hostname checking off is a decision that should appear in the log of every process that made
    * it, not only in the configuration file of the person who made it.
    */
  private def report[F[_]: Async](
      connection: ClusterConnection,
      materialized: Map[ClientPropertyRenderer.StoreRole, String],
      log: Option[Logger[F]]
  ): F[Unit] =
    log.fold(Async[F].unit) { logger =>
      val materializations = materialized.toList.sortBy(_._1.toString).traverse_ { (role, path) =>
        logger.info(
          s"materialized ${role.toString.toLowerCase(java.util.Locale.ROOT)} for cluster " +
            s"${connection.id.value} at $path"
        )
      }

      val hostnameWarning =
        if connection.security.tlsConfig.exists(!_.verifyHostname) then
          logger.warn(s"hostname verification is disabled for cluster ${connection.id.value}")
        else Async[F].unit

      materializations >> hostnameWarning
    }
}
