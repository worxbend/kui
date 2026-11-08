package kui.serde.confluent

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.concurrent.duration.*

import cats.effect.syntax.all.*
import cats.effect.{Async, Clock, Resource}
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger
import sttp.client4.Backend

import kui.cache.{BoundedCache, CacheMetrics}
import kui.http.upstream.UpstreamClient
import kui.kernel.ServiceId
import kui.observability.Telemetry
import kui.serde.{Serde, SerdeDescription, SerdeFactory, SerdeName, SerdeProfile}

/** The `SerdeFactory` seam of `libs/serde`, filled in.
  *
  * `ClusterSerdes.SerdeFactory` returns `Resource[F, Either[String, Serde[F]]]`, and that signature is the
  * entire contract this file has to honour. A registry that is unreachable when a cluster is built must not
  * stop the cluster being built: it must produce a `Left` carrying a sentence, so that `ClusterSerdes` shows
  * the picker a disabled `SchemaRegistry` row explaining itself (ADR-032), and so that asking for the serde
  * by name returns `KUI-SERDE-UNAVAILABLE` rather than silently falling through to `Fallback`.
  *
  * ==Why the probe==
  *
  * `create` does not merely build a client; it asks the registry one question and waits for the answer. That
  * is what turns "configured" into "working", and it is the difference between a picker that tells an
  * operator their registry URL has a typo in it and one that looks fine until the first record fails to
  * decode. The cost is one HTTP call per cluster per profile version, bounded by the upstream's own timeout.
  * A failure is not kept for the life of the process: [[recheck]] asks again once [[RecheckAfter]] has
  * passed, and a registry that was down at startup becomes the working serde without a restart.
  *
  * ==What the probe asks==
  *
  * The latest version of a subject that is not expected to exist. A registry that is up answers 404, which is
  * a successful call; a registry that is down, misconfigured or rejecting KUI's credentials fails, and the
  * failure is what the disabled row shows. Asking for something absent rather than listing every subject is
  * deliberate: a registry with forty thousand subjects should not be made to enumerate them so that KUI can
  * find out it is awake.
  */
object SchemaRegistrySerdeFactory {

  /** The subject the probe asks about. Nothing may ever register it; the 404 *is* the healthy answer. */
  val ProbeSubject: String = "kui-schema-registry-probe-subject"

  /** How long a failed probe is trusted.
    *
    * Short enough that a registry which was restarting when KUI started is usable again without restarting
    * KUI, and long enough that a registry which is still down is not asked on every record of a browse.
    */
  val RecheckAfter: FiniteDuration = 2.seconds

  private val Description: String =
    "Reads and writes payloads carrying a Confluent Schema Registry header, fetching each record's schema " +
      "from the registry by the id in the record."

  /** @param underlying
    *   the raw HTTP transport. Supplied rather than constructed so that a suite can hand in a stub backend
    *   and exercise the whole assembly - probe, caches, decode - without a socket, exactly as
    *   `SttpServiceClient` does.
    */
  def apply[F[_]: Async](
      config: SchemaRegistryConfig,
      underlying: Backend[F],
      telemetry: Telemetry[F],
      service: ServiceId,
      logger: StructuredLogger[F],
      metrics: CacheMetrics[F]
  ): SerdeFactory[F] = new Impl[F](config, underlying, telemetry, service, logger, metrics)

  final private class Impl[F[_]: Async](
      config: SchemaRegistryConfig,
      underlying: Backend[F],
      telemetry: Telemetry[F],
      service: ServiceId,
      logger: StructuredLogger[F],
      metrics: CacheMetrics[F]
  ) extends SerdeFactory[F] {

    val name: SerdeName = SerdeName.SchemaRegistry

    val describe: SerdeDescription = SerdeDescription(name, Description, coveredByIntegrationTest = false)

    def create(profile: SerdeProfile): Resource[F, Either[String, Serde[F]]] =
      for {
        upstream <- UpstreamClient.resource[F](config.upstream, underlying, telemetry, service, logger)
        // The first URL only decides how the path is joined; which host a request actually goes to is the
        // resilient backend's decision, because failover may send it to the second address (CL-008).
        direct = SchemaRegistry.http[F](upstream.backend, config.urls.head, config.auth)
        cached <- CachingSchemaRegistry.resource[F](direct, config, profile.cluster, metrics)
        parsed <- BoundedCache.make[F, java.lang.Integer, ParsedSchema](
          "serde.registry.parsed",
          profile.cluster,
          config.schemaCacheSize,
          ttl = None,
          metrics
        )
        serde = SchemaRegistrySerde[F](cached, parsed)
        reachable <- Resource.eval(probe(cached))
        _ <- Resource.eval(remember(cached, serde, reachable.isRight))
      } yield reachable.as(serde)

    /** The client and the serde `create` built, kept even when the probe failed so a later [[recheck]] can
      * return the same serde without opening a second client.
      */
    private val open = new AtomicReference[Option[OpenRegistry]](None)

    /** Stops two recovers from probing at once. The one that loses keeps the answer already published. */
    private val rechecking = new AtomicBoolean(false)

    final private case class OpenRegistry(
        registry: SchemaRegistry[F],
        serde: Serde[F],
        checkedAt: FiniteDuration,
        up: Boolean
    )

    private def remember(registry: SchemaRegistry[F], serde: Serde[F], up: Boolean): F[Unit] =
      Clock[F].monotonic.flatMap { now =>
        Async[F].delay(open.set(Some(OpenRegistry(registry, serde, now, up))))
      }

    def recheck(profile: SerdeProfile): F[Option[Either[String, Serde[F]]]] =
      Async[F].delay(open.get).flatMap {
        case Some(current) if !current.up => retry(current)
        case _ => none[Either[String, Serde[F]]].pure[F]
      }

    private def retry(current: OpenRegistry): F[Option[Either[String, Serde[F]]]] =
      Clock[F].monotonic.flatMap { now =>
        if now - current.checkedAt < RecheckAfter || !rechecking.compareAndSet(false, true) then
          none[Either[String, Serde[F]]].pure[F]
        else
          probe(current.registry)
            .flatMap { result =>
              Clock[F].monotonic.flatMap { finished =>
                Async[F].delay {
                  open.set(Some(current.copy(checkedAt = finished, up = result.isRight)))
                  Some(result.as(current.serde))
                }
              }
            }
            .guarantee(Async[F].delay(rechecking.set(false)))
      }

    /** `Right` when the registry answered anything at all, `Left` with a sentence when it did not.
      *
      * A 404 for a subject nobody registered is an answer and therefore a success — `latestForSubject`
      * returns `Right(None)` for it. Only a transport failure, a timeout, an authentication rejection or a
      * 5xx becomes a `Left`, and the sentence is the `KuiError`'s own display message, which is already
      * written for a person and already free of credentials and stack traces.
      *
      * It is used *unadorned*. This used to prefix it with "the schema registry could not be reached: ",
      * which produced "the schema registry could not be reached: schema-registry could not be reached" on the
      * two screens that show it, and would have been wrong outright for the failures that are not
      * unreachability. Both call sites — the picker's disabled row and the note on a record — already say
      * which serde this is about, so the reason only has to say what went wrong.
      */
    private def probe(registry: SchemaRegistry[F]): F[Either[String, Unit]] =
      registry
        .latestForSubject(ProbeSubject)
        .map(_.void.leftMap(_.message))
  }
}
