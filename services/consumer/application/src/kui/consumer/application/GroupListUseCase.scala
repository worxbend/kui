package kui.consumer.application

import cats.effect.kernel.Temporal
import cats.syntax.all.*

import kui.consumer.domain.GroupSummary
import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.kernel.group.GroupState
import kui.kernel.search.{NameIndex, SearchMode}
import kui.kernel.{ClusterId, GroupId, Page, PageRequest, PageSize, PositiveInt}

/** What a group list may be sorted by. */
enum GroupSortField(val wire: String) {
  case Id extends GroupSortField("id")
  case Members extends GroupSortField("members")
  case Topics extends GroupSortField("topics")
  case Lag extends GroupSortField("lag")
  case State extends GroupSortField("state")
}

object GroupSortField {

  val All: List[GroupSortField] = values.toList

  def from(wire: String): Option[GroupSortField] = All.find(_.wire == wire)

  given CanEqual[GroupSortField, GroupSortField] = CanEqual.derived
}

/** One request for a page of groups. */
final case class GroupQuery(
    states: Set[GroupState],
    search: Option[String],
    sort: GroupSortField,
    descending: Boolean,
    page: Int,
    pageSize: Int
)

object GroupQuery {

  val MaxPageSize: Int = 200

  val Default: GroupQuery =
    GroupQuery(Set.empty, None, GroupSortField.Id, descending = false, page = 1, pageSize = 25)

  /** Bounds rather than rejects, and says what it changed.
    *
    * A client asking for five thousand rows gets two hundred and a note, not a 400: the request is
    * understandable, and answering it with an error helps nobody. A page beyond the last is an empty page
    * with the right `totalItems`, which is `Page.of`'s rule and not this one's.
    */
  def normalise(raw: GroupQuery): (GroupQuery, List[String]) = {
    val boundedSize =
      if raw.pageSize < 1 then 1
      else if raw.pageSize > MaxPageSize then MaxPageSize
      else raw.pageSize

    val boundedPage = math.max(1, raw.page)
    val trimmed = raw.search.map(_.trim).filter(_.nonEmpty)

    val notes = List(
      Option.when(boundedSize != raw.pageSize)(
        s"pageSize ${raw.pageSize} was clamped to $boundedSize"
      ),
      Option.when(boundedPage != raw.page)(s"page ${raw.page} was clamped to $boundedPage")
    ).flatten

    (raw.copy(pageSize = boundedSize, page = boundedPage, search = trimmed), notes)
  }

  given CanEqual[GroupQuery, GroupQuery] = CanEqual.derived
}

/** A page of groups, and everything the screen needs to say how much of the truth it is showing. */
final case class GroupListView(
    page: Page[GroupSummary],
    freshness: SnapshotFreshness,
    /** How many coordinators did not answer the pass this page was cut from. Non-zero means the list may be
      * short, and the screen says so rather than silently showing fewer groups.
      */
    incompleteCoordinators: Int,
    /** Every state present among the groups this caller may see, computed **before** the state filter, so a
      * filter chip can show how many visible groups filtering it in would reveal. A hidden group is not a
      * count.
      */
    stateCounts: Map[GroupState, Int],
    /** What `normalise` changed about the request. */
    notes: List[String]
)

trait GroupListUseCase[F[_]] {

  /** Never fails for a configured cluster.
    *
    * `Left(NotFound)` only for a cluster id this service is not serving. A configured cluster that cannot be
    * reached is a `Right` whose freshness says `Unavailable`, because "this cluster is not configured" and
    * "this cluster is not answering" are different screens and only one of them is the operator's mistake.
    */
  def list(
      cluster: ClusterId,
      query: GroupQuery,
      visible: GroupId => Boolean = _ => true
  ): F[Either[KuiError, GroupListView]]
}

object GroupListUseCase {

  val Operation: String = "kui.consumer.list"

  def make[F[_]: Temporal](snapshots: GroupSnapshots[F]): GroupListUseCase[F] =
    new GroupListUseCase[F] {

      def list(
          cluster: ClusterId,
          query: GroupQuery,
          visible: GroupId => Boolean
      ): F[Either[KuiError, GroupListView]] =
        snapshots.of(cluster).flatMap {
          case None =>
            ApplicationError
              .NotFound("cluster", cluster.value, ErrorCode.ClusterNotFound)
              .asLeft[GroupListView]
              .pure[F]

          case Some(cell) =>
            cell.get.map { snapshot =>
              val (normalised, notes) = GroupQuery.normalise(query)
              val freshness = SnapshotFreshness.of(
                snapshot,
                kui.kernel.error.InfrastructureError
                  .Unreachable("kafka", "no group listing has been loaded yet")
              )
              val held = snapshot.value.getOrElse(
                GroupSnapshot.empty(snapshot.scrapedAt.getOrElse(java.time.Instant.EPOCH))
              )
              // Before the state counts and before paging. A hidden group that survived either would
              // publish its existence as a chip count or as `totalItems`.
              val permitted = held.summaries.filter(row => visible(row.groupId))

              GroupListView(
                page = applyQuery(permitted, normalised, held.index),
                freshness = freshness,
                incompleteCoordinators = held.incompleteCoordinators,
                stateCounts = permitted.groupBy(_.state).view.mapValues(_.size).toMap,
                notes = notes
              ).asRight[KuiError]
            }
        }
    }

  /** Filter, then search, then sort, then page — in that order, and pure so the order can be asserted
    * directly rather than through six effectful scenarios.
    *
    * The order matters for one reason above the others: `Page.of` counts what it is given, so every filter
    * has to have run before it. The reference product counts before filtering, and its page count is wrong
    * whenever a filter removes anything.
    */
  def applyQuery(
      rows: Vector[GroupSummary],
      query: GroupQuery,
      index: NameIndex,
      visible: GroupId => Boolean = _ => true
  ): Page[GroupSummary] = {
    val permitted = rows.filter(row => visible(row.groupId))
    val byState =
      if query.states.isEmpty then permitted else permitted.filter(row => query.states.contains(row.state))

    val searched = query.search match {
      case None => byState
      case Some(needle) =>
        val matching = index.search(needle, SearchMode.Plain).toSet
        byState.filter(row => matching.contains(row.groupId.value))
    }

    val sorted = sortRows(searched, query)

    Page.of(
      sorted.toList,
      PageRequest(PositiveInt.unsafe(query.page), PageSize.unsafe(query.pageSize))
    )
  }

  /** Every sort is stable and ties break on the group id, so page 2 of two identical requests holds the same
    * rows and paging does not silently duplicate or skip one.
    */
  private def sortRows(rows: Vector[GroupSummary], query: GroupQuery): Vector[GroupSummary] = {
    val byId = rows.sortBy(_.groupId.value)

    val ordered = query.sort match {
      case GroupSortField.Id => byId
      case GroupSortField.Members => stable(byId, query.descending)(_.memberCount)
      case GroupSortField.Topics => stable(byId, query.descending)(_.topicCount)
      case GroupSortField.State => stable(byId, query.descending)(row => stateRank(row.state))
      case GroupSortField.Lag =>
        // A group with no computable lag sorts last in *both* directions. It is not a zero, so it
        // must not lead an ascending sort, and it is not the largest, so it must not lead a
        // descending one. "Unknown last" is the only ordering that is not a lie in one direction.
        val (known, unknown) = byId.partition(_.totalLag.isDefined)
        stable(known, query.descending)(_.totalLag.getOrElse(0L)) ++ unknown
    }

    if query.sort == GroupSortField.Id && query.descending then ordered.reverse else ordered
  }

  private def stable[A, B](rows: Vector[A], descending: Boolean)(key: A => B)(using
      order: Ordering[B]
  ): Vector[A] =
    // `sortBy` is stable, and `rows` arrives sorted by group id, so ties keep id order in *both*
    // directions. Reversing the sorted vector instead would reverse the tie-break too, and page 2 of
    // a descending list would hold different rows from page 2 of the same request a moment later.
    rows.sortBy(key)(using if descending then order.reverse else order)

  /** Operational severity, not the alphabet.
    *
    * A rebalancing group is what an operator is looking for; a dead one is what they are not. Alphabetical
    * order puts `COMPLETING_REBALANCE` above `DEAD` for no reason anybody can act on.
    */
  private def stateRank(state: GroupState): Int = state match {
    case GroupState.PreparingRebalance => 0
    case GroupState.CompletingRebalance => 1
    case GroupState.Stable => 2
    case GroupState.Empty => 3
    case GroupState.Dead => 4
    case GroupState.Unknown => 5
  }
}
