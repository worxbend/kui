package kui.consumer.application

import java.nio.charset.StandardCharsets
import java.util.Base64

import scala.concurrent.duration.FiniteDuration

import cats.effect.kernel.Temporal
import cats.syntax.all.*

import kui.consumer.domain.GroupSummary
import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.kernel.group.GroupState
import kui.kernel.{ClusterId, GroupId}

/** Opaque to the client, meaningful to the server.
  *
  * `<clusterId>:<snapshotVersion>`, base64url'd. Not signed: it carries no authority, and the worst a forged
  * one can do is make the server send a full payload. That is deliberate — a token that needed the cursor key
  * would need key distribution for what is only a polling hint.
  *
  * A server-issued token rather than the browser's own `lastUpdate` timestamp, which is what the reference
  * product sends back. A client's clock is not a version: skew in one direction silently drops updates and in
  * the other replays them, and neither is visible to anyone (DEVPLAN §10 D9).
  */
opaque type LagToken = String

object LagToken {

  def of(cluster: ClusterId, version: Long): LagToken =
    Base64.getUrlEncoder.withoutPadding
      .encodeToString(s"${cluster.value}:$version".getBytes(StandardCharsets.UTF_8))

  def parse(raw: String): Option[(ClusterId, Long)] =
    scala.util
      .Try(new String(Base64.getUrlDecoder.decode(raw), StandardCharsets.UTF_8))
      .toOption
      .flatMap { decoded =>
        decoded.lastIndexOf(':') match {
          case -1 => None
          case at =>
            val (id, version) = (decoded.take(at), decoded.drop(at + 1))
            version.toLongOption.flatMap(v => ClusterId.from(id).toOption.map(_ -> v))
        }
      }

  def unsafe(raw: String): LagToken = raw

  extension (token: LagToken) def value: String = token

  given CanEqual[LagToken, LagToken] = CanEqual.derived
}

final case class LagUpdate(
    groupId: GroupId,
    totalLag: Option[Long],
    pace: Option[Double],
    state: GroupState,
    memberCount: Int
)

object LagUpdate {

  def of(summary: GroupSummary): LagUpdate =
    LagUpdate(summary.groupId, summary.totalLag, summary.pace, summary.state, summary.memberCount)

  given CanEqual[LagUpdate, LagUpdate] = CanEqual.derived
}

final case class LagPollView(
    changed: List[LagUpdate],
    /** Groups the caller asked about that are no longer on the cluster.
      *
      * Named, so the row is removed rather than frozen at its last value. The reference product freezes them,
      * which is why a deleted group lingers on its screen showing the lag it had when it died.
      */
    gone: List[GroupId],
    token: LagToken,
    /** How long the client should wait before asking again — the snapshot interval normally, and the
      * capability's `suggestedPollIntervalMs` when this service is degraded.
      */
    nextPollMs: Long,
    /** True when everything was sent because the token could not be used. The client resets its trend history
      * rather than drawing a line across a gap it cannot see.
      */
    full: Boolean
)

trait LagPollUseCase[F[_]] {

  /** `groups` empty means every group in the snapshot.
    *
    * An absent, unparseable, foreign or too-old token is answered with a full payload and a fresh token —
    * never with an error. A client that has been asleep must be able to resynchronise without special-casing
    * an error code, and a polling hint is not worth a failure mode.
    */
  def poll(
      cluster: ClusterId,
      groups: Set[GroupId],
      since: Option[String]
  ): F[Either[KuiError, LagPollView]]
}

object LagPollUseCase {

  val Operation: String = "kui.consumer.lag"

  def make[F[_]: Temporal](
      snapshots: GroupSnapshots[F],
      refreshInterval: FiniteDuration,
      degradedHint: F[Option[Long]]
  ): LagPollUseCase[F] =
    new LagPollUseCase[F] {

      def poll(
          cluster: ClusterId,
          groups: Set[GroupId],
          since: Option[String]
      ): F[Either[KuiError, LagPollView]] =
        snapshots.of(cluster).flatMap {
          case None =>
            ApplicationError
              .NotFound("cluster", cluster.value, ErrorCode.ClusterNotFound)
              .asLeft[LagPollView]
              .pure[F]

          case Some(cell) =>
            for {
              snapshot <- cell.get
              previous <- snapshots.previousOf(cluster)
              hint <- degradedHint
              current = snapshot.value.getOrElse(GroupSnapshot.empty(java.time.Instant.EPOCH))
              parsed = since.flatMap(LagToken.parse)
              // `previous` is written before the cell publishes (`GroupSnapshots.load`). A poll can
              // therefore see the new previous beside the snapshot that is still current, and those
              // two are the same version. Diffing a snapshot against itself answers `changed = []`,
              // and the client treats that as "nothing moved" forever. The token is usable only when
              // previous really is the pass before the one on screen; otherwise answer in full.
              aligned = previous.exists(_.version == current.version - 1L)
              // A token from another cluster, or one more than a pass old, is answered in full: the
              // one previous snapshot is all that is kept, and a client that has been away longer is
              // better served by everything than by a delta computed against a gap.
              usable =
                aligned && parsed.exists((id, version) => id == cluster && version == current.version - 1L)
              caughtUp = parsed.exists((id, version) => id == cluster && version == current.version)
              (changed, gone) =
                if caughtUp then (Nil, Nil)
                else if usable then diff(previous, current, groups)
                else diff(None, current, groups)
            } yield LagPollView(
              changed = changed,
              gone = gone,
              token = LagToken.of(cluster, current.version),
              nextPollMs = hint.getOrElse(refreshInterval.toMillis),
              full = !usable && !caughtUp
            ).asRight[KuiError]
        }
    }

  /** The delta between two passes, restricted to the groups the caller asked about.
    *
    * Pure, so the rule is asserted directly rather than through a snapshot rig. A group is "changed" when any
    * of the four numbers on its row moved; a group present before and absent now is `gone`, which is what
    * lets a client remove the row instead of leaving it frozen.
    */
  def diff(
      previous: Option[GroupSnapshot],
      current: GroupSnapshot,
      groups: Set[GroupId]
  ): (List[LagUpdate], List[GroupId]) = {
    def wanted(id: GroupId): Boolean = groups.isEmpty || groups.contains(id)

    val now =
      current.summaries.filter(row => wanted(row.groupId)).map(row => row.groupId -> LagUpdate.of(row)).toMap

    previous match {
      case None => (now.values.toList.sortBy(_.groupId.value), Nil)
      case Some(before) =>
        val was = before.summaries
          .filter(row => wanted(row.groupId))
          .map(row => row.groupId -> LagUpdate.of(row))
          .toMap

        val changed = now.collect { case (id, update) if !was.get(id).contains(update) => update }
        val gone = was.keySet.diff(now.keySet)

        (changed.toList.sortBy(_.groupId.value), gone.toList.sortBy(_.value))
    }
  }
}
