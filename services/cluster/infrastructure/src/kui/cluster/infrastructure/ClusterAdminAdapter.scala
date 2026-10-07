package kui.cluster.infrastructure

import cats.data.NonEmptyList
import cats.effect.kernel.Async
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.trace.{SpanKind, Tracer}

import kui.cluster.domain as dom
import kui.cluster.domain.{ClusterAdmin as ClusterAdminPort, ClusterProfile}
import kui.kafka.admin as adm
import kui.kernel.BrokerId
import kui.kernel.error.{ErrorCode, InfrastructureError, KuiError}
import kui.observability.{MetricNames, Telemetry}

/** The cluster domain's `ClusterAdmin` port, satisfied by `libs/kafka`'s.
  *
  * The two traits share a name in different packages, which is unavoidable and harmless as long as every file
  * that sees both aliases one of them on import, as above. The domain port is the one this class implements;
  * the `libs/kafka` one is the one it calls.
  *
  * The port speaks `ClusterProfile`; `libs/kafka` speaks the pure `kui.kernel.cluster` connection value. The
  * projection between them is `ClusterProfileConnection.of`, reached through `ClusterAdminClients` so that a
  * profile whose version moved evicts the client built from the old one on the way past.
  *
  * ==What this class guarantees==
  *
  * Failure is typed and total. Every method returns a value; none raises. An exception escaping `libs/kafka`
  * — which `KafkaErrorMapper` should already have caught — is caught here as a last resort and becomes
  * `InfrastructureError.Unreachable(upstream = "kafka:<clusterId>", cause = <class name>)`. The class name
  * and never the message: a Kafka exception's message routinely carries the bootstrap string, and on some
  * SASL paths the principal.
  *
  * It does not degrade, cache or fall back. Staleness is the snapshot's job (CLDOM-005 over `SnapshotCell`),
  * not the adapter's, and an adapter with a fallback is a second cache with no staleness contract.
  */
final class ClusterAdminAdapter[F[_]: Async](
    admin: adm.ClusterAdmin[F],
    sweeper: KafkaPartitionSweeper[F],
    clients: ClusterAdminClients[F],
    tracer: Tracer[F],
    logger: StructuredLogger[F]
) extends ClusterAdminPort[F] {

  import ClusterAdminAdapter.Operations

  def describeCluster(profile: ClusterProfile): F[Either[KuiError, dom.ClusterDescription]] =
    traced(profile, Operations.DescribeCluster) { connection =>
      // The domain's `ClusterDescription` carries the controller *mode*, which `describeCluster` alone
      // cannot tell you: a KRaft cluster and a ZooKeeper cluster describe themselves identically. The
      // quorum call is the only thing that distinguishes them, and its failure costs the mode and not the
      // description — which is why it is folded into `ControllerMode.Unknown` rather than a second thing
      // that can fail the call. The two calls are independent, so they run concurrently rather than paying
      // two sequential broker round trips on every 30-second refresh.
      Async[F].both(admin.describeCluster(connection), admin.describeQuorum(connection)).map {
        case (described, quorum) =>
          described.flatMap(raw => KafkaToDomain.description(raw, KafkaToDomain.controllerMode(quorum)))
      }
    }

  def detectVersion(profile: ClusterProfile): F[Either[KuiError, Option[dom.KafkaVersion]]] =
    traced(profile, Operations.DetectVersion) { connection =>
      admin.version(connection).map(_.map(KafkaToDomain.version))
    }

  def describeQuorum(profile: ClusterProfile): F[Either[KuiError, Option[dom.QuorumInfo]]] =
    traced(profile, Operations.DescribeQuorum) { connection =>
      admin.describeQuorum(connection).map(_.flatMap(_.traverse(KafkaToDomain.quorum)))
    }

  /** A cluster that refuses this call is a `Left`, never `Right(Nil)`.
    *
    * This is the one place where KUI deliberately does the opposite of the reference product. Swallowing
    * `InvalidRequestException` (MSK Serverless) or `ClusterAuthorizationException` to an empty list shows a
    * table of no settings, which reads as "this broker has no configuration" — a statement that is never
    * true. `Left(ApplicationError.Unsupported)` lets the screen say "this cluster does not expose broker
    * configuration", and per ADR-039 §6 an `ApplicationError` does not dim a capability, so saying so costs
    * the rest of the page nothing.
    */
  def brokerConfigs(
      profile: ClusterProfile,
      broker: BrokerId,
      docs: Boolean
  ): F[Either[KuiError, List[dom.ConfigEntry]]] =
    traced(profile, Operations.BrokerConfigs) { connection =>
      admin
        .brokerConfigs(connection, broker, docs)
        .map(_.map(_.map(KafkaToDomain.configEntry).sorted))
        .flatTap(downgradeNoted(profile, "broker configs", _))
    }

  def describeLogDirs(
      profile: ClusterProfile,
      brokers: NonEmptyList[BrokerId]
  ): F[Either[KuiError, dom.PartialResult[BrokerId, List[dom.LogDir]]]] = {
    val requested = brokers.toList.toSet

    traced(profile, Operations.DescribeLogDirs, Attribute("kui.kafka.broker.count", requested.size.toLong)) {
      connection =>
        admin
          .describeLogDirs(connection, requested)
          .map(_.map(batch => KafkaToDomain.logDirsByBroker(requested, batch)))
          .flatTap(downgradeNoted(profile, "log dirs", _))
    }
  }

  /** Traced and invalidated exactly like the calls that go through `libs/kafka`, which is why it goes through
    * the same wrapper rather than calling the sweeper directly: a `describeTopics` that times out is as good
    * a reason to rebuild the client as a `describeCluster` that does, and a sweep that is not in the trace is
    * a minute of a slow refresh that a trace cannot account for.
    */
  def sweepPartitions(profile: ClusterProfile): F[Either[KuiError, dom.TopicSweep]] =
    traced(profile, Operations.SweepPartitions) { connection =>
      sweeper.sweep(connection).flatTap(downgradeNoted(profile, "the partition sweep", _))
    }

  /** Never fails, by the port's own contract: "the probe failed" is already a third answer the type carries.
    *
    * An unreachable cluster produces `ClusterFeatures.unprobed`, every feature `unknown`, which reads
    * correctly as "KUI has established nothing here yet" — and not as "this cluster cannot do any of these
    * things", which is the lie a two-set model would have to tell.
    */
  def capabilities(profile: ClusterProfile): F[dom.ClusterFeatures] =
    clients
      .withConnection(profile)(connection => admin.capabilities(connection))
      .map(KafkaToDomain.features)
      .handleErrorWith { failure =>
        Async[F].realTimeInstant.flatMap { now =>
          logger
            .warn(
              s"the capability probe for cluster ${profile.id.value} did not complete: " +
                failure.getClass.getName
            )
            .as(dom.ClusterFeatures.unprobed(now))
        }
      }

  // ------------------------------------------------------------------ the wrapper

  /** One span, one failure log, one invalidation decision, for every method.
    *
    * The duration histogram is deliberately *not* recorded here. `libs/kafka`'s `AdminMetrics` already times
    * every call inside `AdminClientPool.run` under `kui.kafka.admin.duration` with the same
    * `{cluster, operation, outcome}` attributes; recording it a second time here would double every count and
    * halve every rate on the dashboard those attributes were chosen for.
    */
  private def traced[A](
      profile: ClusterProfile,
      operation: String,
      extra: Attribute[?]*
  )(call: kui.kernel.cluster.ClusterConnection => F[Either[KuiError, A]]): F[Either[KuiError, A]] =
    tracer
      .spanBuilder(s"kui.cluster.admin.$operation")
      // `Client`: the call leaves the process. Drawn as an internal step, a trace of a slow dashboard would
      // not show that the time was spent waiting for a broker.
      .withSpanKind(SpanKind.Client)
      .build
      .use { span =>
        for {
          _ <- span.addAttributes(
            Attribute(MetricNames.Attr.Cluster, profile.id.value),
            Attribute("kui.kafka.operation", operation)
          )
          _ <- if extra.isEmpty then Async[F].unit else span.addAttributes(extra*)
          outcome <- clients
            .withConnection(profile)(connection =>
              call(connection).attempt
                .map(flatten[A](profile))
                .flatTap(_.fold(reportFailure(profile, operation), _ => Async[F].unit))
            )
            .attempt
            .map(flatten[A](profile))
          _ <- span.addAttributes(
            Attribute(MetricNames.Attr.Outcome, outcome.fold(_.code.wire, _ => "ok"))
          )

        } yield outcome
      }

  /** An exception that escaped `libs/kafka` is a defect, so it is caught here and named as one.
    *
    * `getClass.getName` and never `getMessage`: the message is the field that carries the bootstrap string.
    */
  private def flatten[A](profile: ClusterProfile)(
      attempted: Either[Throwable, Either[KuiError, A]]
  ): Either[KuiError, A] =
    attempted match {
      case Right(result) => result
      case Left(failure) =>
        Left(InfrastructureError.Unreachable(s"kafka:${profile.id.value}", failure.getClass.getName))
    }

  /** Logs the failure once, and rebuilds the client when — and only when — the connection is what broke.
    *
    * The line carries the exception's class name as well as the error code whenever [[flatten]] produced it,
    * because the code on its own is not enough to act on. `KUI-UPSTREAM-UNAVAILABLE` is what a broker that is
    * switched off produces, and it is also what a Kafka client that could not be *constructed* produces — a
    * truststore that will not open, a login module that is not on the classpath. Those need opposite
    * remedies, and the only thing that separated them was a string this method used to throw away: a secured
    * cluster whose truststore password was wrong sat on the dashboard reading "unavailable" with nothing
    * anywhere naming the store.
    *
    * The class name and not the message, for the reason [[flatten]] gives.
    */
  private def reportFailure(profile: ClusterProfile, operation: String)(error: KuiError): F[Unit] =
    logger.warn(
      s"cluster ${profile.id.value} could not answer $operation: ${error.code.wire}${causeOf(error)}"
    ) >> {
      if ReconnectPolicy.shouldInvalidate(error) then
        logger.warn(
          s"kafka admin client invalidated for cluster ${profile.id.value} after ${error.code.wire}"
        ) >> clients.invalidate(profile.id)
      else Async[F].unit
    }

  /** ` (org.apache.kafka.common.KafkaException)`, or nothing at all when the failure named no exception. */
  private def causeOf(error: KuiError): String = error match {
    case InfrastructureError.Unreachable(_, cause) if cause.nonEmpty => s" ($cause)"
    case _ => ""
  }

  /** A managed service that answers "I do not offer that" is worth one line, at the cadence of the snapshot
    * rather than of a request, so that an operator can see a section is permanently empty by design.
    */
  private def downgradeNoted(
      profile: ClusterProfile,
      section: String,
      result: Either[KuiError, ?]
  ): F[Unit] =
    result match {
      case Left(error) if ClusterAdminAdapter.DowngradeCodes.contains(error.code) =>
        logger.warn(s"$section unavailable on cluster ${profile.id.value}: ${error.code.wire}")
      case _ => Async[F].unit
    }
}

object ClusterAdminAdapter {

  /** Builds the adapter with its tracer resolved once.
    *
    * A `Telemetry.tracer` call per admin call would be a lookup on the hot path of a 30-second refresh loop
    * across every configured cluster, for a value that never changes.
    */
  def create[F[_]: Async](
      admin: adm.ClusterAdmin[F],
      sweeper: KafkaPartitionSweeper[F],
      clients: ClusterAdminClients[F],
      telemetry: Telemetry[F],
      logger: StructuredLogger[F]
  ): F[ClusterAdminAdapter[F]] =
    telemetry
      .tracer("kui.cluster.admin")
      .map(tracer => new ClusterAdminAdapter[F](admin, sweeper, clients, tracer, logger))

  /** The operation names that appear in the span name and in the `operation` metric attribute. They are
    * constants because a dashboard is built on them.
    */
  object Operations {
    val DescribeCluster: String = "describeCluster"
    val DetectVersion: String = "detectVersion"
    val DescribeQuorum: String = "describeQuorum"
    val BrokerConfigs: String = "brokerConfigs"
    val DescribeLogDirs: String = "describeLogDirs"
    val SweepPartitions: String = "sweepPartitions"
    val Capabilities: String = "capabilities"
  }

  /** The codes that mean "this cluster does not offer this", as opposed to "this failed".
    *
    * They are `ApplicationError` codes on purpose: ADR-039 §6 dims a capability only for an
    * `InfrastructureError`, so a managed service permanently refusing broker configuration greys out one
    * panel and never the sidebar.
    */
  val DowngradeCodes: Set[ErrorCode] = Set(ErrorCode.Unsupported, ErrorCode.Forbidden)
}
