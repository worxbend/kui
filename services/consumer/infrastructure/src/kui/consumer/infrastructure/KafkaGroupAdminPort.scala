package kui.consumer.infrastructure

import java.time.Instant

import cats.effect.kernel.Async
import cats.effect.kernel.implicits.parallelForGenSpawn
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger

import kui.consumer.domain.*
import kui.kafka.admin.{
  CommittedOffset,
  GroupAdmin,
  GroupDescription,
  GroupListing,
  GroupMember as KafkaGroupMember,
  OffsetLookup
}
import kui.kafka.{BatchResult, SkipReason}
import kui.kernel.cluster.ClusterConnection
import kui.kernel.error.KuiError
import kui.kernel.group.GroupState
import kui.kernel.{GroupId, Offset, TopicPartition}

/** The consumer domain's port, over `libs/kafka`'s `GroupAdmin` and `OffsetLookup`.
  *
  * This is where the two vocabularies meet, and it is the only place they do: above it nothing knows what an
  * `AdminClient` is, and below it nothing knows what a `ConsumerGroup` is.
  *
  * The composition rule that matters is DEVPLAN §10 D7: a group's committed offsets and the end offsets its
  * lag is computed against come from **one pass**. Fetching them from two would produce a lag between a
  * commit read at one moment and a log end read at another — a number describing a cluster that never
  * existed, and one that looks perfectly plausible.
  */
object KafkaGroupAdminPort {

  def make[F[_]: Async](
      admin: GroupAdmin[F],
      offsets: OffsetLookup[F],
      connection: ClusterConnection,
      logger: StructuredLogger[F]
  ): GroupAdminPort[F] = new Impl[F](admin, offsets, connection, logger)

  final private class Impl[F[_]: Async](
      admin: GroupAdmin[F],
      offsets: OffsetLookup[F],
      connection: ClusterConnection,
      logger: StructuredLogger[F]
  ) extends GroupAdminPort[F] {

    private val context: Map[String, String] =
      Map("component" -> GroupAdmin.Component, "cluster.id" -> connection.id.value)

    def list(states: Set[GroupState]): F[Either[KuiError, GroupListingPage]] =
      admin
        .listGroups(connection, states)
        .map(_.map { listed =>
          GroupListingPage(
            groups = listed.groups.map(summaryOf),
            incompleteCoordinators = listed.coordinatorFailures.size
          )
        })

    /** Existence by listing.
      *
      * Never by describing: `GroupAdmin.describeGroups` normalises an unknown group into a fabricated dead
      * one, so a describe cannot tell "not there" from "there and finished". This is the check every offset
      * operation makes first.
      */
    def exists(id: GroupId): F[Either[KuiError, Boolean]] =
      admin.listGroups(connection, Set.empty).map(_.map(_.groups.exists(_.groupId == id)))

    /** Describe, then commits, then log ends — one pass, in that order, because each needs the one before it.
      *
      * Only the describe is required. Without commits every partition is `NoCommit`-shaped and the
      * completeness record says the offsets are unknown; without log ends the lag is `None` everywhere and it
      * says that instead. Neither degradation is an error, because on a cluster where KUI may describe a
      * group but not read its topics, both are the permanent steady state.
      */
    def describe(ids: List[GroupId]): F[Either[KuiError, Map[GroupId, ConsumerGroup]]] =
      (
        admin.describeGroups(connection, ids, includeAuthorizedOperations = false),
        admin.committedOffsets(connection, ids, partitions = None, requireStable = false)
      ).parTupled.flatMap {
        case (Left(error), _) => error.asLeft[Map[GroupId, ConsumerGroup]].pure[F]
        case (Right(described), _) if ids.size == 1 && requireResult(described, ids.head).isLeft =>
          requireResult(described, ids.head).map(_ => Map.empty[GroupId, ConsumerGroup]).pure[F]
        case (Right(described), committed) =>
          val commitsByGroup = committed.toOption.map(_.values).getOrElse(Map.empty)
          for {
            _ <- committed.left.toOption.traverse_(error =>
              logger.debug(context ++ Map("error.code" -> error.code.wire))(
                s"committed offsets are not available: ${error.message}"
              )
            )
            wanted = partitionsOf(described, commitsByGroup)
            endsAndBegins <-
              if wanted.isEmpty then (noEnds, noEnds).pure[F]
              else
                (
                  offsets.endOffsets(connection, wanted),
                  offsets.beginningOffsets(connection, wanted)
                ).parTupled
            (ends, begins) = endsAndBegins
            _ <- ends.left.toOption.traverse_(error =>
              logger.debug(context ++ Map("error.code" -> error.code.wire))(
                s"log end offsets are not available: ${error.message}"
              )
            )
            now <- Async[F].realTimeInstant
          } yield described.values
            .map { (id, description) =>
              id -> groupOf(
                description,
                commitsByGroup.getOrElse(id, Nil),
                committedKnown = committed.exists(requireResult(_, id).isRight),
                ends = ends,
                begins = begins,
                at = now
              )
            }
            .asRight[KuiError]
      }

    /** Everything the planner needs, gathered in one pass (DEVPLAN §10 D7).
      *
      * The leaderless set is read first and carried through, so that the planner can refuse before it plans
      * rather than the write discovering it — a partial reset leaves a group where nobody asked for it.
      */
    def offsetWindow(
        group: GroupId,
        scope: ResetScope,
        at: Option[Instant]
    ): F[Either[KuiError, OffsetWindow]] = {
      val partitions = scope.partitions

      (for {
        offline <- eitherT(offsets.leaderless(connection, partitions))
        askable = partitions.diff(offline)
        begin <- eitherT(
          offsets.beginningOffsets(connection, askable).map(_.flatMap(requireResults(_, askable)))
        )
        end <- eitherT(offsets.endOffsets(connection, askable).map(_.flatMap(requireResults(_, askable))))
        committed <- eitherT(
          admin
            .committedOffsets(connection, List(group), Some(partitions), requireStable = true)
            .map(_.flatMap(requireResult(_, group)))
        )
        timestamps <- at match {
          case None => eitherT(Map.empty[TopicPartition, Option[Offset]].asRight[KuiError].pure[F])
          case Some(instant) =>
            eitherT(
              offsets
                .offsetsForTimes(connection, askable.map(_ -> instant.toEpochMilli).toMap)
                .map(_.flatMap(requireResults(_, askable)))
            )
        }
      } yield OffsetWindow(
        begin = begin,
        end = end,
        committed = committed
          .map(offset => offset.partition -> offset.offset)
          .toMap,
        atTimestamp = timestamps,
        leaderless = offline
      )).value
    }

    def applyOffsets(group: GroupId, offsets: Map[TopicPartition, Offset]): F[Either[KuiError, Unit]] =
      admin.alterOffsets(connection, group, offsets)

    def deleteOffsets(group: GroupId, partitions: Set[TopicPartition]): F[Either[KuiError, Unit]] =
      admin.deleteOffsets(connection, group, partitions)

    def deleteGroup(id: GroupId): F[Either[KuiError, Unit]] =
      admin.deleteGroups(connection, List(id)).map {
        case Left(error) => error.asLeft[Unit]
        case Right(result) =>
          result.get(id) match {
            case Right(_) => ().asRight[KuiError]
            // A per-group skip in a batch of one is that group's failure, and it is reported as one
            // rather than as an empty success.
            case Left(reason) => skipToError(reason).asLeft[Unit]
          }
      }

    // ---------------------------------------------------------------- mapping

    private val noEnds: Either[KuiError, BatchResult[TopicPartition, Offset]] =
      BatchResult.empty[TopicPartition, Offset].asRight[KuiError]

    private def partitionsOf(
        described: BatchResult[GroupId, GroupDescription],
        commits: Map[GroupId, List[CommittedOffset]]
    ): Set[TopicPartition] = {
      val assigned = described.values.values.flatMap(_.members.flatMap(_.assignment.partitions)).toSet
      val committed = commits.values.flatten.map(_.partition).toSet

      // The union: a partition a member holds but has never committed on still has a row, and a
      // partition with a commit but no member still has its lag. Either one alone loses half a page.
      assigned ++ committed
    }

    private def summaryOf(listing: GroupListing): GroupSummary =
      GroupSummary(
        groupId = listing.groupId,
        state = listing.state,
        protocol = listing.protocol,
        isSimple = listing.isSimple,
        memberCount = 0,
        topicCount = 0,
        partitionCount = 0,
        coordinator = None,
        totalLag = None,
        pace = None,
        // A listing knows the group exists and nothing else. Saying so is what stops a row built
        // from a listing alone from rendering as a group with no members and no lag.
        completeness = GroupCompleteness.Complete.withoutMembers.withoutCommittedOffsets.withoutEndOffsets
      )

    private def groupOf(
        description: GroupDescription,
        commits: List[CommittedOffset],
        committedKnown: Boolean,
        ends: Either[KuiError, BatchResult[TopicPartition, Offset]],
        begins: Either[KuiError, BatchResult[TopicPartition, Offset]],
        at: Instant
    ): ConsumerGroup = {
      val endOffsets = ends.map(_.values).getOrElse(Map.empty)
      val beginOffsets = begins.map(_.values).getOrElse(Map.empty)
      val skipped = ends.map(_.skipped).getOrElse(Map.empty)
      val committed = commits.map(offset => offset.partition -> offset.offset).toMap
      val holder = description.members.flatMap(member => member.assignment.partitions.map(_ -> member)).toMap

      val partitions = (committed.keySet ++ holder.keySet).toList.sorted

      val states = partitions.map { partition =>
        val member = holder.get(partition)
        val lag =
          if skipped.get(partition).contains(SkipReason.NoLeader) then PartitionLag.noLeader
          else LagMath.lagOf(committed.get(partition), beginOffsets.get(partition), endOffsets.get(partition))

        partition -> PartitionState(
          partition = partition.partition,
          committed = committed.get(partition),
          begin = beginOffsets.get(partition),
          end = endOffsets.get(partition),
          memberId = member.map(_.memberId),
          host = member.map(_.host),
          lag = lag
        )
      }

      val completeness = skipped.foldLeft(
        GroupCompleteness.Complete
          .copy(committedOffsetsKnown = committedKnown, endOffsetsKnown = ends.isRight)
      ) { (acc, entry) =>
        val (partition, reason) = entry
        acc.excluding(partition, reason.message)
      }

      ConsumerGroup(
        groupId = description.groupId,
        state = description.state,
        protocol = description.protocol,
        isSimple = description.isSimple,
        partitionAssignor = description.partitionAssignor,
        members = description.members.map(memberOf),
        coordinator = description.coordinator.map(c => GroupCoordinatorRef(c.id, c.host, c.port)),
        subscriptions = states
          .groupBy(_._1.topic)
          .toList
          .map((topic, rows) => TopicSubscription(topic, rows.map(_._2).sortBy(_.partition.value)))
          .sortBy(_.topic.value),
        completeness = completeness,
        observedAt = at
      )
    }

    private def memberOf(member: KafkaGroupMember): GroupMember =
      GroupMember(
        memberId = member.memberId,
        groupInstanceId = member.groupInstanceId,
        clientId = member.clientId,
        host = member.host,
        partitions = member.assignment.partitions,
        targetPartitions = member.targetAssignment.map(_.partitions)
      )

    /** A missing batch key is a failed lookup, not a successful absent offset. */
    private def requireResults[K, V](batch: BatchResult[K, V], keys: Set[K]): Either[KuiError, Map[K, V]] =
      keys.toList.traverse(key => requireResult(batch, key).map(key -> _)).map(_.toMap)

    private def requireResult[K, V](batch: BatchResult[K, V], key: K): Either[KuiError, V] =
      batch.skipped.get(key) match {
        case Some(reason) => Left(skipToError(reason))
        case None => batch.get(key).left.map(skipToError)
      }

    private def skipToError(reason: SkipReason): KuiError = reason match {
      case SkipReason.NotAuthorized(detail) => kui.kernel.error.ApplicationError.Forbidden(detail)
      case SkipReason.NotFound(detail) =>
        kui.kernel.error.ApplicationError
          .NotFound("consumer group", detail, kui.kernel.error.ErrorCode.GroupNotFound)
      case SkipReason.Unsupported(feature) => kui.kernel.error.ApplicationError.Unsupported(feature)
      case SkipReason.NoLeader => kui.kernel.error.ApplicationError.InvalidState(reason.message)
      case SkipReason.Failed(code, detail) => kui.kernel.error.ApplicationError.Refused(code, detail)
    }

    /** A three-line `EitherT`, for the same reason the reset use case has one: the window is a sequence of
      * calls where any one can fail, and a `for` over `Either` inside `F` reads as that sequence.
      */
    private def eitherT[A](fa: F[Either[KuiError, A]]): EitherStep[A] = EitherStep(fa)

    final private case class EitherStep[A](value: F[Either[KuiError, A]]) {
      def map[B](f: A => B): EitherStep[B] = EitherStep(value.map(_.map(f)))
      def flatMap[B](f: A => EitherStep[B]): EitherStep[B] =
        EitherStep(value.flatMap {
          case Right(a) => f(a).value
          case Left(error) => error.asLeft[B].pure[F]
        })
    }
  }
}
