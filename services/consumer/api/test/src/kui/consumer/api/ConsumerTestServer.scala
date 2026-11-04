package kui.consumer.api

import java.time.Instant

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.Resource
import org.typelevel.otel4s.metrics.Counter
import org.typelevel.otel4s.oteljava.testkit.OtelJavaTestkit
import sttp.client4.Backend
import sttp.client4.impl.cats.implicits.*
import sttp.client4.testing.BackendStub
import sttp.tapir.server.stub4.TapirStubInterpreter

import kui.cache.{Snapshot, SnapshotCell, SnapshotStatus}
import kui.consumer.application.*
import kui.consumer.domain.*
import kui.contracts.capability.ClusterCapability
import kui.http.principal.{PrincipalVerification, RbacGuard}
import kui.kernel.error.{ApplicationError, ErrorCode, InfrastructureError, KuiError}
import kui.kernel.group.{GroupProtocol, GroupState}
import kui.kernel.{ClusterId, GroupId, Page, RoleName, Secret, ServiceId, TopicName, UserName}
import kui.observability.Telemetry
import kui.security.rbac.{ClusterFlags, RbacPolicy}
import kui.security.{Principal, *}
import kui.testkit.fakes.FakeStructuredLogger

/** The consumer service, assembled the way `services/consumer/app` assembles it, with no socket.
  *
  * The same shape as `TopicTestServer` and for the same reasons: Tapir's stub interpreter runs the real
  * routes through the real interceptors, so the route order, the principal check and the error envelope are
  * all under test, and nothing here waits for a port to bind.
  *
  * The principal codec is real. A fake one would verify a token by agreeing with itself, which would pass
  * just as happily against a service that had stopped checking signatures — and the two things this module's
  * suites exist to pin are both about verification and routing rather than about consumer groups.
  */
final case class ConsumerTestServer(
    backend: Backend[IO],
    logger: FakeStructuredLogger[IO],
    principals: PrincipalCodec[IO]
)

object ConsumerTestServer {

  val Cluster: ClusterId = ClusterId.unsafe("prod")
  val At: Instant = Instant.parse("2026-09-04T09:00:00Z")

  def path(rest: String): String = s"/internal/v1/clusters/${Cluster.value}$rest"

  def uri(rest: String): String = s"http://consumer$rest"

  private val KeyMaterial: Array[Byte] = Array.fill[Byte](32)(7)

  private val Key: SigningKey = SigningKey("test-1", Secret(KeyMaterial), Instant.EPOCH)

  val Issuer: String = "kui-gateway"

  def codec: PrincipalCodec[IO] =
    JwsPrincipalCodec
      .make[IO](NonEmptyList.of(Key), Issuer)
      .getOrElse(throw new IllegalStateException("the test signing key is too short for HS256"))

  /** A token bound to one call, the way the gateway binds one (ADR-020).
    *
    * `body` is the exact bytes the caller is about to send. It defaults to empty, which is right for every
    * read; a bodied request has to pass the same bytes it puts on the wire, and a suite that passes different
    * ones is exercising the mismatch rather than the happy path.
    */
  def token(
      requestPath: String,
      method: String = "GET",
      body: Array[Byte] = Array.emptyByteArray,
      audience: ServiceId = ConsumerApi.Id,
      validFor: FiniteDuration = 60.seconds,
      roles: Set[RoleName] = Set.empty
  ): IO[SignedPrincipal] =
    IO.realTimeInstant.flatMap(now =>
      codec.sign(
        PrincipalClaims(
          subject = UserName.unsafe("alice"),
          roles = roles,
          kind = PrincipalKind.Session,
          sessionRef = None,
          issuedAt = now,
          expiresAt = now.plusSeconds(validFor.toSeconds),
          audience = audience,
          requestDigest = RequestDigests.of(method, requestPath.takeWhile(_ != '?'), body)
        )
      )
    )

  // -----------------------------------------------------------------------------------------------
  // The stubs
  // -----------------------------------------------------------------------------------------------

  private def notThisCluster[A](cluster: ClusterId): Option[Either[KuiError, A]] =
    Option.when(cluster != Cluster)(
      Left(ApplicationError.NotFound("cluster", cluster.value, ErrorCode.ClusterNotFound))
    )

  final class StubList(view: GroupListView) extends GroupListUseCase[IO] {
    def list(cluster: ClusterId, query: GroupQuery): IO[Either[KuiError, GroupListView]] =
      IO.pure(notThisCluster[GroupListView](cluster).getOrElse(Right(view)))
  }

  /** Records every group id it was asked about.
    *
    * That record is the whole point of `ConsumerRoutesSuite`: a request for `/consumer-groups/lag` that
    * reaches this stub reached the *detail* route, and the group it names is the literal string "lag".
    */
  final class StubDetail(
      view: GroupDetailView,
      val asked: cats.effect.kernel.Ref[IO, List[GroupId]]
  ) extends GroupDetailUseCase[IO] {
    def detail(cluster: ClusterId, group: GroupId): IO[Either[KuiError, GroupDetailView]] =
      asked.update(_ :+ group) *>
        IO.pure(notThisCluster[GroupDetailView](cluster).getOrElse(Right(view)))
  }

  final class StubLag(view: LagPollView) extends LagPollUseCase[IO] {
    def poll(
        cluster: ClusterId,
        groups: Set[GroupId],
        since: Option[String]
    ): IO[Either[KuiError, LagPollView]] =
      IO.pure(notThisCluster[LagPollView](cluster).getOrElse(Right(view)))
  }

  final class StubForTopic(view: TopicConsumersView) extends GroupsForTopicUseCase[IO] {
    def forTopic(cluster: ClusterId, topic: TopicName): IO[Either[KuiError, TopicConsumersView]] =
      IO.pure(notThisCluster[TopicConsumersView](cluster).getOrElse(Right(view)))
  }

  /** One empty snapshot for [[Cluster]], which is all the mutation routes read it for. */
  final class StubSnapshots extends GroupSnapshots[IO] {

    private val cell: SnapshotCell[IO, GroupSnapshot] = new SnapshotCell[IO, GroupSnapshot] {
      private val snapshot = Snapshot(Some(GroupSnapshot.empty(At)), SnapshotStatus.Online, Some(At))
      def get: IO[Snapshot[GroupSnapshot]] = IO.pure(snapshot)
      def refresh: IO[Snapshot[GroupSnapshot]] = IO.pure(snapshot)
      def invalidate: IO[Snapshot[GroupSnapshot]] = IO.pure(snapshot)
      def updates: fs2.Stream[IO, Snapshot[GroupSnapshot]] = fs2.Stream.emit(snapshot)
    }

    def of(cluster: ClusterId): IO[Option[SnapshotCell[IO, GroupSnapshot]]] =
      IO.pure(Option.when(cluster == Cluster)(cell))
    def all: IO[List[(ClusterId, SnapshotCell[IO, GroupSnapshot])]] = IO.pure(List(Cluster -> cell))
    def previousOf(cluster: ClusterId): IO[Option[GroupSnapshot]] = IO.pure(None)
    def requestRefresh(cluster: ClusterId): IO[Boolean] = IO.pure(cluster == Cluster)
    def invalidate(cluster: ClusterId, reason: String): IO[Unit] = IO.unit
  }

  /** Records what the reset was asked to plan and to apply, and answers with a fixed plan. */
  final class StubReset(
      val planned: cats.effect.kernel.Ref[IO, List[(GroupId, ResetSpec)]],
      val applied: cats.effect.kernel.Ref[IO, List[(GroupId, String)]]
  ) extends OffsetResetUseCase[IO] {

    def plan(
        cluster: ClusterId,
        group: GroupId,
        scope: ResetScope,
        spec: ResetSpec
    ): IO[Either[KuiError, PlannedReset]] =
      planned
        .update(_ :+ (group -> spec))
        .as(
          Right(PlannedReset(emptyPlan(group, scope), "a-plan-token", At.plusSeconds(300)))
        )

    def apply(
        principal: Principal,
        cluster: ClusterId,
        group: GroupId,
        token: String
    ): IO[Either[KuiError, ResetPlan]] =
      applied.update(_ :+ (group -> token)).as(Right(emptyPlan(group, ResetScope(Topic, Set.empty))))
  }

  final class StubDeleteGroup extends DeleteGroupUseCase[IO] {
    def delete(principal: Principal, cluster: ClusterId, group: GroupId): IO[Either[KuiError, Unit]] =
      IO.pure(Right(()))
  }

  final class StubDeleteOffsets extends DeleteOffsetsUseCase[IO] {
    def delete(
        principal: Principal,
        cluster: ClusterId,
        group: GroupId,
        topic: TopicName
    ): IO[Either[KuiError, DeletedOffsets]] = IO.pure(Right(DeletedOffsets(topic, Set.empty)))
  }

  final class StubCapabilities extends ConsumerCapabilities[IO] {
    def report: IO[Map[ClusterId, ClusterCapability]] = IO.pure(Map.empty)
  }

  // -----------------------------------------------------------------------------------------------
  // Fixtures
  // -----------------------------------------------------------------------------------------------

  val Topic: TopicName = TopicName.unsafe("orders.v1")
  val Group: GroupId = GroupId.unsafe("order-fulfilment")

  /** The group id a request for `/consumer-groups/lag` decodes to if the detail route claims that path. */
  val LagAsAGroupId: GroupId = GroupId.unsafe("lag")

  def emptyPlan(group: GroupId, scope: ResetScope): ResetPlan =
    ResetPlan(
      group = group,
      scope = scope,
      spec = ResetSpec.ToEarliest,
      partitions = Nil,
      warnings = Nil,
      computedAt = At
    )

  def summary(id: GroupId, lag: Option[Long]): GroupSummary =
    GroupSummary(
      groupId = id,
      state = GroupState.Stable,
      protocol = GroupProtocol.Consumer,
      isSimple = false,
      memberCount = 1,
      topicCount = 1,
      partitionCount = 6,
      coordinator = None,
      totalLag = lag,
      excludedPartitions = 0,
      pace = None,
      completeness = GroupCompleteness.Complete
    )

  val Fresh: SnapshotFreshness = SnapshotFreshness.Fresh(At)

  def listView(groups: List[GroupSummary]): GroupListView =
    GroupListView(
      page =
        Page(groups, page = 1, pageSize = 25, totalItems = Some(groups.size.toLong), nextPageToken = None),
      freshness = Fresh,
      incompleteCoordinators = 0,
      stateCounts = Map.empty,
      notes = Nil
    )

  def detailView(group: GroupId): GroupDetailView =
    GroupDetailView(
      group = ConsumerGroup(
        groupId = group,
        state = GroupState.Dead,
        protocol = GroupProtocol.Consumer,
        isSimple = false,
        partitionAssignor = "range",
        members = Nil,
        coordinator = None,
        subscriptions = Nil,
        completeness = GroupCompleteness.Complete,
        observedAt = At
      ),
      topics = Nil,
      total = LagMath.LagTotal.Empty,
      assignments = AssignmentFreshness.Current,
      freshness = Fresh,
      computedAt = At
    )

  def lagView(changed: List[LagUpdate]): LagPollView =
    LagPollView(
      changed = changed,
      gone = Nil,
      token = LagToken.of(Cluster, 7L),
      nextPollMs = 30_000L,
      full = true
    )

  val Timeout: KuiError = InfrastructureError.Timeout("describeConsumerGroups", 30_000L)

  // -----------------------------------------------------------------------------------------------
  // The server
  // -----------------------------------------------------------------------------------------------

  /** Everything the routes are built from, so a suite can assert against the stubs it programmed. */
  final case class Stubs(
      detail: StubDetail,
      reset: StubReset
  )

  def resource(
      groups: List[GroupSummary] = Nil,
      lag: List[LagUpdate] = Nil,
      freshness: SnapshotFreshness = Fresh,
      // What this service allows on its own account, independently of whatever the gateway decided. It
      // defaults to allowing everything so that a suite about consumer groups is about consumer groups;
      // the suite that is about the guard passes a real policy.
      rbac: RbacPolicy = RbacPolicy.Disabled,
      clusterFlags: Map[ClusterId, ClusterFlags] = Map.empty
  ): Resource[IO, (ConsumerTestServer, Stubs)] =
    OtelJavaTestkit.inMemory[IO]().evalMap { testkit =>
      for {
        logger <- FakeStructuredLogger[IO]
        rejections <- rejectionCounter(testkit)
        asked <- cats.effect.kernel.Ref.of[IO, List[GroupId]](Nil)
        planned <- cats.effect.kernel.Ref.of[IO, List[(GroupId, ResetSpec)]](Nil)
        applied <- cats.effect.kernel.Ref.of[IO, List[(GroupId, String)]](Nil)
        telemetry = Telemetry.fromProviders(testkit.tracerProvider, testkit.meterProvider)
        interceptors <- ConsumerApi.interceptors[IO](telemetry, rejections, logger)
      } yield {
        val catsMonadError: sttp.monad.MonadError[IO] = summon
        val detail = new StubDetail(detailView(Group), asked)
        val reset = new StubReset(planned, applied)

        val routes = ConsumerApi.routes[IO](
          new StubList(listView(groups).copy(freshness = freshness)),
          detail,
          new StubLag(lagView(lag)),
          new StubForTopic(TopicConsumersView(Topic, Nil, Fresh, At)),
          new StubSnapshots,
          reset,
          new StubDeleteGroup,
          new StubDeleteOffsets,
          Nil,
          new StubCapabilities,
          codec,
          rejections,
          logger,
          RbacGuard.fromPolicy[IO](
            rbac,
            cluster => clusterFlags.getOrElse(cluster, ClusterFlags.Writable),
            logger
          )
        )

        (
          ConsumerTestServer(
            TapirStubInterpreter(interceptors, BackendStub[IO](catsMonadError))
              .whenServerEndpointsRunLogic(routes)
              .backend(),
            logger,
            codec
          ),
          Stubs(detail, reset)
        )
      }
    }

  def rejectionCounter(testkit: OtelJavaTestkit[IO]): IO[Counter[IO, Long]] =
    testkit.meterProvider.get("kui.consumer").flatMap(PrincipalVerification.rejectionCounter[IO])
}
