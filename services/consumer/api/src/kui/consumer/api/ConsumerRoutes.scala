package kui.consumer.api

import cats.effect.kernel.Async
import cats.syntax.all.*
import sttp.tapir.server.ServerEndpoint

import kui.consumer.application.*
import kui.consumer.contract.dto.*
import kui.consumer.contract.{ConsumerEndpoints, GroupListParams}
import kui.http.principal.RbacGuard
import kui.kernel.SortOrder
import kui.security.rbac.{Action, Resource, ResourceAccess}

/** The four read endpoints, bound to use cases.
  *
  * One rule shapes every route in this file, and it is the same one the topic explorer settled on:
  *
  *   - a request that names something KUI has never heard of **fails**: an unknown cluster id is
  *     `404 KUI-CLUSTER-NOT-FOUND`;
  *   - a request that names something real which could not be read **succeeds**, with the answer carrying the
  *     reason it is thin — `stale`, an `incomplete` note, a `null` lag.
  *
  * An unknown *group* is deliberately in neither category. Describing a group that does not exist answers
  * with a fabricated dead group (`GroupAdmin.describeGroups`), so `detail` answers 200 with an empty group in
  * state `DEAD` and a stale bookmark lands on an empty page rather than an error. Where existence really
  * matters — the mutations in [[ConsumerMutationRoutes]] — it is confirmed by listing instead.
  *
  * ==This layer never computes a number==
  *
  * The page arrives from `GroupListUseCase` already filtered, searched, sorted and cut; the lag totals arrive
  * from `LagMath`. Nothing here slices, sorts, counts or subtracts. `ConsumerMapping` renames fields and that
  * is all it does.
  */
object ConsumerRoutes {

  def apply[F[_]: Async](
      list: GroupListUseCase[F],
      detail: GroupDetailUseCase[F],
      lag: LagPollUseCase[F],
      forTopic: GroupsForTopicUseCase[F],
      snapshots: GroupSnapshots[F],
      secured: ConsumerApi.Securing[F],
      guard: RbacGuard[F]
  ): List[ServerEndpoint[Any, F]] =
    // `lagPoll` before `groupDetail`, for the reason `ConsumerEndpoints.all` records at length: both match
    // `/consumer-groups/lag`, and the detail route answers it with a fabricated dead group rather than an
    // error, so getting this order wrong produces a well-formed 200 and a lag column that silently stops.
    List(
      listGroups(list, secured, guard),
      lagPoll(lag, secured, guard),
      groupDetail(detail, secured),
      topicConsumers(forTopic, snapshots, secured)
    )

  /** One page of a cluster's consumer groups, cut out of the 30-second snapshot.
    *
    * Served from the snapshot rather than from a live describe because describing four thousand groups on the
    * request path would make this the slowest screen in KUI and would hammer the coordinators once per page
    * view. A group whose lag could not be computed reports `null`, never `0`.
    */
  private def listGroups[F[_]: Async](
      list: GroupListUseCase[F],
      secured: ConsumerApi.Securing[F],
      guard: RbacGuard[F]
  ): ServerEndpoint[Any, F] =
    secured(ConsumerEndpoints.list) { principal => (cluster, params) =>
      visibleGroups(guard, principal, cluster).flatMap { allow =>
        list
          .list(cluster, queryOf(params), allow)
          .map(_.map { view =>
            // The freshness travels with the rows rather than being dropped here. Before it did, a cluster
            // that had stopped answering was a bare 200 carrying the lag figures of the last successful
            // scrape, and the browser had no way to tell that from a caught-up cluster.
            GroupsResponse(
              groups = ConsumerSections.of(view.freshness, ConsumerMapping.page(view.page)),
              incompleteCoordinators = view.incompleteCoordinators
            )
          })
      }
    }

  /** One group, whole. A deep link has to work without fetching the list first. */
  private def groupDetail[F[_]: Async](
      detail: GroupDetailUseCase[F],
      secured: ConsumerApi.Securing[F]
  ): ServerEndpoint[Any, F] =
    secured(ConsumerEndpoints.detail) { _ => (cluster, group) =>
      detail.detail(cluster, group).map(_.map(ConsumerMapping.detail))
    }

  /** What changed since a token.
    *
    * An absent, unparseable, foreign or expired token is answered with a full payload and a fresh token,
    * never with an error: a client that has been asleep must be able to resynchronise without special-casing
    * an error code, and a polling hint is not worth a failure mode. That decision lives in the use case; this
    * route only carries `full` across so the browser knows whether to merge the rows or replace them.
    */
  private def lagPoll[F[_]: Async](
      lag: LagPollUseCase[F],
      secured: ConsumerApi.Securing[F],
      guard: RbacGuard[F]
  ): ServerEndpoint[Any, F] =
    secured(ConsumerEndpoints.lag) { principal => (cluster, groups, since) =>
      visibleGroups(guard, principal, cluster).flatMap { allow =>
        lag
          .poll(cluster, groups, since, allow)
          .map(
            _.map(view =>
              LagDeltaDto(
                changed = view.changed.map(ConsumerMapping.lagUpdate),
                gone = view.gone,
                token = LagToken.value(view.token),
                nextPollMs = view.nextPollMs,
                full = view.full
              )
            )
          )
      }
    }

  /** `CONSUMER VIEW` on the group id. Hidden ids are dropped from the page, the state counts and the lag
    * delta, including names that only appear because they disappeared.
    */
  private def visibleGroups[F[_]: cats.Functor](
      guard: RbacGuard[F],
      principal: kui.security.Principal,
      cluster: kui.kernel.ClusterId
  ): F[kui.kernel.GroupId => Boolean] =
    guard
      .visibility(
        principal,
        cluster,
        name => ResourceAccess.named(Resource.ConsumerGroup, name, Action.ConsumerGroupView)
      )
      .map(allow => id => allow(id.value))

  /** Every consumer group that reads one topic — the topic page's Consumers tab.
    *
    * The gateway calls this while assembling the topic overview; the browser never does, which is what keeps
    * `ui-topics` free of any knowledge of this service (DEVPLAN §10 D13).
    *
    * A topic nobody consumes is an empty list of rows and a 200, not a 404: "no consumer groups" is a common
    * and healthy answer.
    */
  private def topicConsumers[F[_]: Async](
      forTopic: GroupsForTopicUseCase[F],
      snapshots: GroupSnapshots[F],
      secured: ConsumerApi.Securing[F]
  ): ServerEndpoint[Any, F] =
    secured(ConsumerEndpoints.forTopic) { _ => (cluster, topic) =>
      for {
        answer <- forTopic.forTopic(cluster, topic)
        // The per-topic lag needs the whole group, and the use case's view carries only the row. The
        // groups come from the same cell the row did; a refresh landing between the two reads makes a
        // group present in one and absent from the other, and the row then reports a `null` topic lag
        // rather than a number from a different pass.
        cell <- snapshots.of(cluster)
        groups <- cell.fold(Map.empty[kui.kernel.GroupId, kui.consumer.domain.ConsumerGroup].pure[F])(
          _.get.map(_.value.fold(Map.empty)(_.groups))
        )
      } yield answer.map(view => ConsumerMapping.topicConsumers(view, groups.get))
    }

  /** The wire query as the use case's, which is where the two `GroupSortField` enums meet.
    *
    * `pageSize` is carried across unbounded on purpose: `GroupQuery.normalise` clamps it and says what it
    * changed. Refusing an oversized page here with a 400 would make every caller write the clamping the
    * server can do once.
    */
  private def queryOf(params: GroupListParams): GroupQuery =
    GroupQuery(
      states = params.states,
      search = params.q,
      sort = ConsumerMapping.sortField(params.sort),
      descending = params.direction == SortOrder.Desc,
      page = params.page,
      pageSize = params.pageSize
    )
}
