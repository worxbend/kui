package kui.http.upstream

import java.time.Instant

import scala.concurrent.duration.FiniteDuration

import cats.data.NonEmptyList
import cats.effect.kernel.{Clock, Ref, Temporal}
import cats.syntax.all.*
import sttp.model.Uri

import kui.config.SafeUrl

/** Rotating between several addresses for the same upstream.
  *
  * A schema registry or a Connect cluster is often several machines behind one logical name, and an operator
  * lists them all. When one refuses a connection, KUI should try the next rather than reporting the upstream
  * as down — but it should also stop hammering the one that just failed.
  *
  * Hence the grace period: a URL that failed to *connect* is set aside for a few seconds, and the next call
  * starts from the next one. Only a connection-level failure counts. A URL that answered 500 is reachable and
  * is answering; trying the next one would just ask the same question of a machine that will give the same
  * answer, and would hide from the operator that the cluster is unwell rather than unreachable.
  */
trait Failover[F[_]] {

  /** The addresses to try, in order, skipping those inside their grace period.
    *
    * When every URL is inside its grace period the full list is returned anyway: refusing to try at all would
    * turn a transient wobble into a hard outage that only time could clear.
    */
  def candidates: F[NonEmptyList[SafeUrl]]

  /** Records that this URL could not be connected to, setting it aside for the grace period. */
  def markFailed(url: SafeUrl): F[Unit]

  /** Records that this URL answered, clearing any grace period on it. */
  def markHealthy(url: SafeUrl): F[Unit]

  /** The URLs currently set aside. */
  def failed: F[Set[SafeUrl]]
}

object Failover {

  def make[F[_]: Temporal](urls: NonEmptyList[SafeUrl], grace: FiniteDuration): F[Failover[F]] =
    Ref.of[F, Map[SafeUrl, Instant]](Map.empty).map { state =>
      new Failover[F] {

        def candidates: F[NonEmptyList[SafeUrl]] =
          for {
            now <- Clock[F].realTimeInstant
            setAside <- state.get
          } yield {
            val stillFailed = setAside.filter((_, until) => until.isAfter(now)).keySet
            val healthy = urls.filterNot(stillFailed.contains)

            NonEmptyList.fromList(healthy).getOrElse(urls)
          }

        def markFailed(url: SafeUrl): F[Unit] =
          Clock[F].realTimeInstant
            .flatMap(now => state.update(_.updated(url, now.plusMillis(grace.toMillis))))

        def markHealthy(url: SafeUrl): F[Unit] = state.update(_ - url)

        def failed: F[Set[SafeUrl]] =
          for {
            now <- Clock[F].realTimeInstant
            setAside <- state.get
          } yield setAside.filter((_, until) => until.isAfter(now)).keySet
      }
    }

  /** Points a request at one of the upstream's base addresses.
    *
    * The caller — usually a Tapir client interpreter — builds a request against *some* base URL and knows
    * nothing about failover. This replaces the scheme and authority with the chosen one and prefixes the base
    * URL's own path, so a base of `https://host/api` and a request path of `/subjects` becomes
    * `https://host/api/subjects`.
    */
  def rebase(uri: Uri, base: SafeUrl): Uri =
    Uri.parse(base.value) match {
      case Left(_) => uri
      case Right(baseUri) =>
        val prefix = baseUri.path.filter(_.nonEmpty)
        uri
          .scheme(baseUri.scheme.getOrElse("http"))
          .host(baseUri.host.getOrElse(""))
          .port(baseUri.port)
          .withPath(prefix ++ uri.path.filter(_.nonEmpty))
    }

  /** These failures occur during address resolution or connection establishment, before HTTP transmission. A
    * generic SocketException is deliberately excluded: it can be a reset after a committed mutation.
    */
  def isBeforeTransmission(error: Throwable): Boolean = {
    @annotation.tailrec
    def loop(current: Throwable, remaining: Int): Boolean = current match {
      case _: java.net.ConnectException | _: java.net.UnknownHostException |
          _: java.net.NoRouteToHostException | _: java.nio.channels.UnresolvedAddressException =>
        true
      case _ if remaining > 0 =>
        Option(current.getCause) match {
          case Some(cause) if cause ne current => loop(cause, remaining - 1)
          case _ => false
        }
      case _ => false
    }
    loop(error, 16)
  }

  /** Connectivity failures mark a destination unhealthy; only replay-safe calls may repeat after a reset. */
  def isConnectionFailure(error: Throwable): Boolean =
    error match {
      case _: java.net.ConnectException => true
      case _: java.net.UnknownHostException => true
      case _: java.net.NoRouteToHostException => true
      case _: java.net.SocketException => true
      case _: java.nio.channels.UnresolvedAddressException => true
      case _ => Option(error.getCause).exists(isConnectionFailure)
    }
}
