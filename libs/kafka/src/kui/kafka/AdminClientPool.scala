package kui.kafka

import scala.concurrent.duration.*

import cats.effect.std.Semaphore
import cats.effect.{Async, Ref, Resource}
import cats.syntax.all.*
import fs2.io.file.Files
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig}
import org.typelevel.log4cats.Logger

import kui.kafka.auth.{ClientPurpose, ConnectionProperties}
import kui.kernel.ClusterId
import kui.kernel.cluster.{ClientProperties, ClusterConnection}
import kui.kernel.error.KuiError

/** One admin client per cluster: created on first use, shared by every caller, and replaced when a failure
  * says the old one is finished.
  *
  * The pool is a resource cache, not a data cache (ADR-016): it holds no cluster data, so it needs no TTL and
  * no invalidation policy beyond "the connection broke" and "the profile changed".
  */
trait AdminClientPool[F[_]] {

  /** Runs one admin call.
    *
    * `operation` is the metric and log label — `describeCluster`, `describeLogDirs` — and must come from a
    * short closed set. It becomes a metric attribute, and an attribute whose values multiply with the request
    * is how a metrics backend runs out of memory.
    *
    * Failures arrive as `Throwable`: this layer does not map them. KAFKA-005 puts the mapping on top, and
    * `ClusterAdmin` (KAFKA-007) is the first thing a caller outside this module sees.
    */
  def run[A](connection: ClusterConnection, operation: String)(call: Admin => F[A]): F[A]

  /** Closes the current client for this cluster, if any; the next `run` builds a new one.
    *
    * Idempotent and safe to call concurrently.
    */
  def invalidate(id: ClusterId): F[Unit]

  /** Closes and forgets a cluster entirely — for a profile that was removed from configuration, or whose
    * connection details changed, since the old client is bound to the old bootstrap list and the old
    * credentials.
    */
  def evict(id: ClusterId): F[Unit]
}

/** A configuration problem surfaced through an effect that can only carry a `Throwable`.
  *
  * `run` has to fail with something, and the thing that went wrong is a `KuiError` — a bad keystore, a
  * missing login module. Wrapping it rather than flattening it to a message means KAFKA-005's mapper, and the
  * service above it, can still see the original error and its code.
  */
final case class KafkaClientConfigurationFailure(error: KuiError)
    extends RuntimeException(error.message)
    with scala.util.control.NoStackTrace

object AdminClientPool {

  /** How long `close` may take before the pool gives up on a broker that has stopped answering.
    *
    * Bounded on purpose: an unbounded close is how a process that is being shut down hangs, and a shutdown
    * that hangs is one an orchestrator eventually kills, losing whatever else was still draining.
    */
  private val CloseTimeout: FiniteDuration = 5.seconds

  /** What the pool holds for one cluster.
    *
    * `generation` is what makes invalidation idempotent under concurrency: `run` remembers the generation it
    * used and invalidates *that* one, so two requests failing at once on the same dead client replace it once
    * rather than twice. That is the difference between a reconnect and a reconnect storm.
    *
    * `release` closes the client *and* deletes the keystore files that were materialized for it, in that
    * order, which is why it is stored rather than recomputed.
    */
  final private case class Entry[F[_]](
      client: Admin,
      clientId: ClientId,
      generation: Long,
      release: F[Unit]
  )

  /** How a client is built. A parameter so that the pool's own behaviour — sharing, generations,
    * invalidation, measurement — can be tested without a broker, which is a different question from whether
    * `Admin.create` works and is answered by KAFKA-007 against a real one.
    */
  type Factory[F[_]] = (ClusterConnection, ClientId, ClientProperties) => Resource[F, Admin]

  def resource[F[_]: {Async, Files}](
      metrics: AdminMetrics[F],
      log: Option[Logger[F]] = None
  ): Resource[F, AdminClientPool[F]] =
    resourceWith(metrics, defaultFactory[F], log)

  def resourceWith[F[_]: {Async, Files}](
      metrics: AdminMetrics[F],
      factory: Factory[F],
      log: Option[Logger[F]] = None
  ): Resource[F, AdminClientPool[F]] =
    Resource.make(
      for {
        entries <- Ref.of[F, Map[ClusterId, Entry[F]]](Map.empty)
        gates <- Ref.of[F, Map[ClusterId, Semaphore[F]]](Map.empty)
        gateLock <- Semaphore[F](1)
        generations <- Ref.of[F, Long](0L)
      } yield new Impl[F](entries, gates, gateLock, generations, metrics, factory, log)
    )(_.closeAll)

  /** `Admin.create` parses the properties and starts a network thread, so it is blocking work and belongs on
    * the blocking pool rather than on a compute worker.
    */
  private def defaultFactory[F[_]: Async]: Factory[F] =
    (_, _, properties) =>
      Resource.make(Async[F].blocking(Admin.create(toJava(properties))))(client =>
        Async[F].blocking(client.close(java.time.Duration.ofMillis(CloseTimeout.toMillis)))
      )

  private def toJava(properties: ClientProperties): java.util.Map[String, Object] = {
    val map = new java.util.HashMap[String, Object]()
    properties.unsafeValues.foreach((key, value) => map.put(key, value))
    map
  }

  /** The timeouts every KUI admin client runs under.
    *
    * Applied *after* the renderer and *before* the operator's overrides would be — except that the overrides
    * already won inside `ConnectionProperties`, so they are re-applied here on top. A cluster whose
    * `properties` set `request.timeout.ms` means it, and `AdminTuning` is the default rather than the law.
    */
  private def withAdminSettings(
      rendered: ClientProperties,
      connection: ClusterConnection,
      clientId: ClientId
  ): ClientProperties = {
    import kui.kernel.cluster.PropertyValue

    val tuned = ClientProperties.fromMap(
      Map(
        AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG ->
          PropertyValue.Plain(connection.admin.requestTimeout.toMillis.toString),
        AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG ->
          PropertyValue.Plain(connection.admin.apiTimeout.toMillis.toString),
        AdminClientConfig.CLIENT_ID_CONFIG -> PropertyValue.Plain(clientId.value)
      )
    )

    rendered ++ tuned ++ connection.overrides
  }

  final private class Impl[F[_]: {Async, Files}](
      entries: Ref[F, Map[ClusterId, Entry[F]]],
      gates: Ref[F, Map[ClusterId, Semaphore[F]]],
      gateLock: Semaphore[F],
      generations: Ref[F, Long],
      metrics: AdminMetrics[F],
      factory: Factory[F],
      log: Option[Logger[F]]
  ) extends AdminClientPool[F] {

    def run[A](connection: ClusterConnection, operation: String)(call: Admin => F[A]): F[A] =
      for {
        entry <- entryFor(connection)
        result <- metrics
          .timed(connection.id, operation)(call(entry.client))
          .onError(failure =>
            if AdminInvalidation.isReconnectClass(failure) then
              // The failed call still fails, with its original error. Retrying here would double
              // every timeout and hide the failure from the metric; the caller's own policy —
              // `SnapshotCell` keeps serving the previous value — is where retrying belongs.
              invalidateGeneration(connection.id, entry.generation, failure)
            else Async[F].unit
          )
      } yield result

    def invalidate(id: ClusterId): F[Unit] =
      entries.get.flatMap(_.get(id).fold(Async[F].unit)(entry => remove(id, entry.generation).void))

    def evict(id: ClusterId): F[Unit] =
      gateFor(id)
        .flatMap(
          _.permit.use { _ =>
            entries
              .modify(current => (current - id, current.get(id)))
              .flatMap(_.fold(Async[F].unit)(_.release))
          }
        ) >> logged(_.debug(s"admin client for cluster ${id.value} evicted"))

    /** Closes every client the pool still holds. Runs on `Resource` release, including on the cancellation
      * path, which is the only thing standing between a cancelled startup and a process that keeps a Kafka
      * network thread alive for ever.
      *
      * Calls still pending here are abandoned first. Everything that ran through the pool was acquired inside
      * its scope and has been released already, so nobody can receive their answers, and `close`'s grace
      * period would only wait for them: against a broker that never answers that is the whole
      * [[CloseTimeout]], once per pool, on every shutdown.
      */
    def closeAll: F[Unit] =
      entries
        .getAndSet(Map.empty)
        .flatMap(
          _.values.toList
            .traverse_(entry => (abandonPendingCalls(entry.client).attempt >> entry.release).attempt.void)
        )

    /** Moves the client's hard-shutdown deadline to now, which aborts its pending calls.
      *
      * Not `Duration.ZERO`: Kafka joins its I/O thread for the same duration, and a zero join waits without a
      * limit. The release that follows still waits up to [[CloseTimeout]] for that thread to exit.
      */
    private def abandonPendingCalls(client: Admin): F[Unit] =
      Async[F].blocking(client.close(java.time.Duration.ofMillis(1L)))

    // -------------------------------------------------------------- creation

    private def entryFor(connection: ClusterConnection): F[Entry[F]] =
      entries.get.map(_.get(connection.id)).flatMap {
        case Some(entry) => entry.pure[F]
        case None =>
          gateFor(connection.id).flatMap { gate =>
            gate.permit.use { _ =>
              // Re-read inside the gate: ten concurrent first calls all miss above, and exactly one
              // of them should create a client.
              entries.get.map(_.get(connection.id)).flatMap {
                case Some(entry) => entry.pure[F]
                case None => create(connection)
              }
            }
          }
      }

    private def gateFor(id: ClusterId): F[Semaphore[F]] =
      // Keep one gate per id for the pool's lifetime. Removing it during eviction would let a call that
      // already holds the old gate create in parallel with a later call holding a newly allocated gate.
      gates.get.map(_.get(id)).flatMap {
        case Some(gate) => gate.pure[F]
        case None =>
          gateLock.permit.use { _ =>
            gates.get.map(_.get(id)).flatMap {
              case Some(gate) => gate.pure[F]
              case None => Semaphore[F](1).flatTap(gate => gates.update(_ + (id -> gate)))
            }
          }
      }

    private def create(connection: ClusterConnection): F[Entry[F]] =
      for {
        clientId <- ClientId.next[F](ClientPurpose.Admin, connection.id)
        // Uncancellable from the moment the keystore files exist to the moment the entry is in the
        // map: a cancellation in between would leave a client with a live network thread and a
        // directory of private keys that nothing holds a finalizer for.
        entry <- Async[F].uncancelable { _ =>
          for {
            allocated <- ConnectionProperties
              .resource[F](connection, ClientPurpose.Admin, clientId.value, log)
              .allocated
            (rendered, releaseProperties) = allocated
            entry <- rendered match {
              case Left(error) =>
                releaseProperties >> Async[F]
                  .raiseError[Entry[F]](KafkaClientConfigurationFailure(error))
              case Right(properties) =>
                openClient(connection, clientId, properties, releaseProperties)
            }
          } yield entry
        }
      } yield entry

    private def openClient(
        connection: ClusterConnection,
        clientId: ClientId,
        properties: ClientProperties,
        releaseProperties: F[Unit]
    ): F[Entry[F]] =
      for {
        generation <- generations.updateAndGet(_ + 1L)
        allocated <- factory(
          connection,
          clientId,
          withAdminSettings(properties, connection, clientId)
        ).allocated.onError(_ => releaseProperties)
        (client, releaseClient) = allocated
        // Order matters: the client has to stop using the keystore before the keystore is deleted.
        entry = Entry[F](
          client,
          clientId,
          generation,
          releaseClient.attempt.void >> releaseProperties.attempt.void
        )
        _ <- entries.update(_ + (connection.id -> entry))
        _ <- logged(
          _.info(
            s"admin client ${clientId.value} created for cluster ${connection.id.value} " +
              s"at ${connection.bootstrapServers.value}"
          )
        )
      } yield entry

    // -------------------------------------------------------------- invalidation

    /** Removes the entry only if it is still the generation the caller saw.
      *
      * Without the check, two calls failing simultaneously on one dead client would each remove "the" entry —
      * the second removing the replacement the first had just created.
      */
    private def invalidateGeneration(
        id: ClusterId,
        generation: Long,
        reason: Throwable
    ): F[Unit] =
      remove(id, generation).flatMap {
        case false => Async[F].unit
        case true =>
          logged(
            _.info(
              s"admin client for cluster ${id.value} (generation $generation) invalidated after " +
                KafkaFutures.unwrap(reason).getClass.getName
            )
          )
      }

    private def remove(id: ClusterId, generation: Long): F[Boolean] =
      entries
        .modify { current =>
          current.get(id) match {
            case Some(entry) if entry.generation == generation => (current - id, Some(entry))
            case _ => (current, None)
          }
        }
        .flatMap {
          case Some(entry) => entry.release.attempt.as(true)
          case None => false.pure[F]
        }

    private def logged(write: Logger[F] => F[Unit]): F[Unit] =
      log.fold(Async[F].unit)(write)
  }

}
