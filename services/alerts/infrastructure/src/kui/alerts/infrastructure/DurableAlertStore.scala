package kui.alerts.infrastructure

import java.time.Instant

import scala.concurrent.duration.FiniteDuration

import cats.effect.Async
import cats.syntax.all.*
import fs2.Stream
import io.circe.syntax.*
import io.circe.{Codec, Decoder, DecodingFailure, Encoder, HCursor, Json}
import org.typelevel.log4cats.StructuredLogger

import kui.alerts.application.{AlertFeed, AlertStore}
import kui.alerts.domain.*
import kui.config.store.*
import kui.kernel.error.{ErrorCode, InfrastructureError, KuiError}
import kui.kernel.{ClusterId, GroupId}
import kui.security.Principal

/** Durable alert history backed by the deployment's compacted `__kui_config` topic.
  *
  * One record per cluster keeps the event projection, rule state and last evaluation together, so a replica
  * can never observe an event list from one pass with rule state from another. Per-principal read watermarks
  * share [[UserStateStore]] records with appearance settings; its field-preserving optimistic update keeps a
  * theme click and a bell click from overwriting one another.
  *
  * The projection is bounded both when it is written and when untrusted metadata is decoded. A malformed or
  * oversized record fails closed with `KUI-STORE-ENVELOPE`; it is never treated as an empty feed, because
  * doing that would reopen continuing conditions and relight every reader's bell.
  */
final class DurableAlertStore[F[_]: Async] private (
    store: ConfigStore[F],
    retention: FiniteDuration,
    logger: StructuredLogger[F]
) extends AlertStore[F] {

  import DurableAlertStore.*

  private val users = UserStateStore[F](store)

  def feed(
      cluster: ClusterId,
      principal: Principal,
      limit: Int,
      markRead: Option[Instant]
  ): F[AlertFeed] =
    for {
      held <- read(cluster).flatMap(lift)
      user <- users.get(cluster, principal).flatMap(lift)
      effectiveMarker <- markRead match {
        case None => user.alertsReadAt.pure[F]
        case Some(at) => users.markAlertsRead(cluster, principal, at).flatMap(lift).map(Some(_))
      }
      ordered = held.events.sorted
      unread = ordered.count(event => effectiveMarker.forall(event.openedAt.isAfter))
    } yield AlertFeed(
      events = ordered.take(limit.max(0)).toList,
      total = ordered.size,
      openCount = ordered.count(_.isOpen),
      openByRule = ordered.filter(_.isOpen).groupBy(_.key.rule).view.mapValues(_.size).toMap,
      unreadCount = unread,
      lastReadAt = effectiveMarker,
      evaluatedAt = held.evaluatedAt,
      reports = held.reports
    )

  def acknowledge(
      cluster: ClusterId,
      event: AlertEventId,
      at: Instant,
      by: String
  ): F[Either[KuiError, AlertEvent]] = {
    def attempt(remaining: Int): F[Either[KuiError, AlertEvent]] =
      readVersioned(cluster).flatMap {
        case Left(error) => error.asLeft[AlertEvent].pure[F]
        case Right((version, held)) =>
          held.events.indexWhere(_.id == event) match {
            case -1 => InMemoryAlertStore.notOpen(event).asLeft[AlertEvent].pure[F]
            case index if !held.events(index).isOpen =>
              InMemoryAlertStore.alreadyClosed(event).asLeft[AlertEvent].pure[F]
            case index =>
              val closed = held
                .events(index)
                .resolvedBy(
                  at,
                  AlertResolutionKind.Acknowledged,
                  Some(by.take(MaxActorLength))
                )
              val next = held.copy(events = held.events.updated(index, closed))

              put(cluster, next, version).flatMap {
                case Right(_) => closed.asRight[KuiError].pure[F]
                case Left(error) if error.code == ErrorCode.ConfigVersionConflict && remaining > 1 =>
                  attempt(remaining - 1)
                case Left(error) => error.asLeft[AlertEvent].pure[F]
              }
          }
      }

    attempt(MaxWriteAttempts)
  }

  def record(cluster: ClusterId, evaluation: Evaluation, at: Instant): F[Unit] = {
    def attempt(remaining: Int): F[Either[KuiError, Unit]] =
      readVersioned(cluster).flatMap {
        case Left(error) => error.asLeft[Unit].pure[F]
        case Right((_, held)) if held.evaluatedAt.exists(current => !current.isBefore(at)) =>
          // More than one alerts replica may evaluate the same cluster. A delayed pass must not move the
          // projection's clock or an event's last-seen time backwards after a newer replica has committed.
          ().asRight[KuiError].pure[F]
        case Right((version, held)) =>
          val resolved = evaluation.resolved.toSet
          val refreshed = evaluation.refreshed.map(event => event.id -> event).toMap
          val updated = held.events.map { event =>
            if !event.isOpen then event
            else if resolved.contains(event.id) then event.resolvedBy(at, AlertResolutionKind.Cleared, None)
            else
              refreshed
                .get(event.id)
                .fold(event)(latest =>
                  event.copy(
                    severity = latest.severity,
                    title = latest.title,
                    detail = latest.detail,
                    lastSeenAt = at
                  )
                )
          }
          // Two replicas can both decide that one condition is new before either write becomes visible. Their
          // opening instants (and therefore ids) differ, so id-only deduplication would leave two open rows.
          // Keep the earliest event identity and the latest observation for each still-open condition.
          val byId = coalesceOpenEvents(updated ++ evaluation.opened).map(event => event.id -> event).toMap
          val next = AlertProjection(
            events = bounded(byId.values.toVector, at),
            ruleState = boundRuleState(evaluation.state),
            evaluatedAt = Some(at),
            reports = evaluation.reports.take(AlertRule.All.size)
          )

          put(cluster, next, version).flatMap {
            case Right(_) => ().asRight[KuiError].pure[F]
            case Left(error) if error.code == ErrorCode.ConfigVersionConflict && remaining > 1 =>
              attempt(remaining - 1)
            case Left(error) => error.asLeft[Unit].pure[F]
          }
      }

    attempt(MaxWriteAttempts).flatMap(lift)
  }

  def ruleState(cluster: ClusterId): F[AlertRuleState] =
    read(cluster).flatMap(lift).map(_.ruleState)

  /** Store changes include this process's own read-back and writes from every other replica. */
  def changes: Stream[F, ClusterId] =
    store.changes.flatMap {
      case change @ (StoreChange.Upserted(_) | StoreChange.Deleted(_, _, _)) =>
        Stream.fromOption[F](change.keyOption.flatMap(clusterFrom))
      case StoreChange.Desynchronized(missed) =>
        Stream.eval(
          logger.warn(s"alert metadata subscriber lost $missed changes; reloading alert clusters")
        ) >> Stream.evalSeq(
          store
            .list(StoreSection.Alerts)
            .map(_.flatMap(record => clusterFrom(record.key)))
        )
    }

  private def read(cluster: ClusterId): F[Either[KuiError, AlertProjection]] =
    readVersioned(cluster).map(_.map(_._2))

  private def readVersioned(
      cluster: ClusterId
  ): F[Either[KuiError, (Option[Long], AlertProjection)]] = {
    val key = keyOf(cluster)
    store.get(key).map {
      case None => Right(None -> AlertProjection.empty)
      case Some(record) =>
        Decoder[AlertProjection]
          .decodeJson(record.payload)
          .left
          .map(failure => malformed(key, failure.message))
          .map(projection => Some(record.version) -> projection)
    }
  }

  private def put(
      cluster: ClusterId,
      projection: AlertProjection,
      version: Option[Long]
  ): F[Either[KuiError, StoreRecord]] =
    store.put(keyOf(cluster), projection.asJson, version, s"kui-alerts/${cluster.value}")

  private def lift[A](result: Either[KuiError, A]): F[A] =
    result match {
      case Right(value) => value.pure[F]
      case Left(error) =>
        logger.error(s"${error.code.wire}: durable alert metadata operation failed: ${error.message}") *>
          Async[F].raiseError[A](DurableAlertStoreFailure(error))
    }

  private def bounded(events: Vector[AlertEvent], at: Instant): Vector[AlertEvent] = {
    val cutoff = at.minusMillis(retention.toMillis)
    events.filterNot(_.openedAt.isBefore(cutoff)).sorted.take(InMemoryAlertStore.MaxEventsPerCluster)
  }
}

object DurableAlertStore {
  val CurrentFormatVersion: Int = 1
  val MaxWriteAttempts: Int = 4
  val MaxRebalancingGroups: Int = 2000

  private val MaxSubjectLength = 2048
  private val MaxTitleLength = 1024
  private val MaxDetailLength = 4096
  private val MaxActorLength = 512
  private val MaxErrorMessageLength = 2048

  def apply[F[_]: Async](
      store: ConfigStore[F],
      retention: FiniteDuration,
      logger: StructuredLogger[F]
  ): DurableAlertStore[F] = new DurableAlertStore(store, retention, logger)

  final private case class AlertProjection(
      events: Vector[AlertEvent],
      ruleState: AlertRuleState,
      evaluatedAt: Option[Instant],
      reports: List[RuleReport]
  )

  private object AlertProjection {
    val empty: AlertProjection = AlertProjection(Vector.empty, AlertRuleState.empty, None, Nil)

    given Codec[AlertProjection] = Codec.from(
      Decoder.instance { cursor =>
        for {
          version <- cursor.get[Int]("formatVersion")
          _ <- ensure(
            version == CurrentFormatVersion,
            cursor,
            s"unsupported alert formatVersion $version (expected $CurrentFormatVersion)"
          )
          events <- cursor.get[Vector[AlertEvent]]("events")
          _ <- ensure(
            events.size <= InMemoryAlertStore.MaxEventsPerCluster,
            cursor,
            s"events contains ${events.size} entries; maximum is ${InMemoryAlertStore.MaxEventsPerCluster}"
          )
          state <- cursor.get[AlertRuleState]("ruleState")
          evaluatedAt <- cursor.get[Option[Instant]]("evaluatedAt")
          reports <- cursor.get[List[RuleReport]]("reports")
          _ <- ensure(
            reports.size <= AlertRule.All.size,
            cursor,
            s"reports contains ${reports.size} entries; maximum is ${AlertRule.All.size}"
          )
        } yield AlertProjection(events, state, evaluatedAt, reports)
      },
      Encoder.instance { projection =>
        Json.obj(
          "formatVersion" -> Json.fromInt(CurrentFormatVersion),
          "events" -> projection.events.asJson,
          "ruleState" -> projection.ruleState.asJson,
          "evaluatedAt" -> projection.evaluatedAt.asJson,
          "reports" -> projection.reports.asJson
        )
      }
    )
  }

  given Codec[AlertEvent] = Codec.from(
    Decoder.instance { cursor =>
      for {
        rawId <- boundedString(cursor, "id", AlertEventId.MaxLength, allowEmpty = false)
        id <- AlertEventId
          .from(rawId)
          .toRight(DecodingFailure("id is not a valid alert event id", cursor.history))
        rule <- enumValue(cursor, "rule", AlertRule.fromWire, "alert rule")
        subject <- boundedString(cursor, "subject", MaxSubjectLength, allowEmpty = true)
        severity <- enumValue(cursor, "severity", AlertSeverity.fromWire, "alert severity")
        openedAt <- cursor.get[Instant]("openedAt")
        lastSeenAt <- cursor.get[Instant]("lastSeenAt")
        title <- boundedString(cursor, "title", MaxTitleLength, allowEmpty = false)
        detail <- boundedString(cursor, "detail", MaxDetailLength, allowEmpty = true)
        resolution <- cursor.get[Option[AlertResolution]]("resolution")(using
          Decoder.decodeOption(using alertResolutionCodec)
        )
        key = AlertKey(rule, subject)
        _ <- ensure(
          id == AlertEventId.of(key, openedAt),
          cursor,
          "id does not match the event key and openedAt"
        )
        _ <- ensure(!lastSeenAt.isBefore(openedAt), cursor, "lastSeenAt is before openedAt")
      } yield AlertEvent(id, key, severity, openedAt, lastSeenAt, title, detail, resolution)
    },
    Encoder.instance { (event: AlertEvent) =>
      Json.obj(
        "id" -> Json.fromString(event.id.value),
        "rule" -> Json.fromString(event.key.rule.wire),
        "subject" -> Json.fromString(event.key.subject),
        "severity" -> Json.fromString(event.severity.wire),
        "openedAt" -> event.openedAt.asJson,
        "lastSeenAt" -> event.lastSeenAt.asJson,
        "title" -> Json.fromString(event.title),
        "detail" -> Json.fromString(event.detail),
        "resolution" -> Encoder.encodeOption(using alertResolutionCodec).apply(event.resolution)
      )
    }
  )

  given alertResolutionCodec: Codec[AlertResolution] = Codec.from(
    Decoder.instance { cursor =>
      for {
        at <- cursor.get[Instant]("at")
        kind <- enumValue(cursor, "kind", AlertResolutionKind.fromWire, "resolution kind")
        by <- cursor.get[Option[String]]("by")
        _ <- ensure(by.forall(_.length <= MaxActorLength), cursor, s"by exceeds $MaxActorLength characters")
      } yield AlertResolution(at, kind, by)
    },
    Encoder.instance(resolution =>
      Json.obj(
        "at" -> resolution.at.asJson,
        "kind" -> Json.fromString(resolution.kind.wire),
        "by" -> resolution.by.asJson
      )
    )
  )

  given Codec[AlertRuleState] = Codec.from(
    Decoder.instance { cursor =>
      cursor.get[Map[String, Instant]]("rebalancingSince").flatMap { raw =>
        for {
          _ <- ensure(
            raw.size <= MaxRebalancingGroups,
            cursor,
            s"rebalancingSince contains ${raw.size} entries; maximum is $MaxRebalancingGroups"
          )
          entries <- raw.toList.traverse { (name, at) =>
            GroupId
              .from(name)
              .left
              .map(_ => DecodingFailure("rebalancingSince contains an invalid group id", cursor.history))
              .map(_ -> at)
          }
        } yield AlertRuleState(entries.toMap)
      }
    },
    Encoder.instance(state =>
      Json.obj(
        "rebalancingSince" -> state.rebalancingSince.map((group, at) => group.value -> at).asJson
      )
    )
  )

  given Codec[RuleReport] = Codec.from(
    Decoder.instance { cursor =>
      for {
        rule <- enumValue(cursor, "rule", AlertRule.fromWire, "alert rule")
        outcome <- cursor.get[RuleOutcome]("outcome")(using ruleOutcomeCodec)
      } yield RuleReport(rule, outcome)
    },
    Encoder.instance(report =>
      Json.obj(
        "rule" -> Json.fromString(report.rule.wire),
        "outcome" -> ruleOutcomeCodec(report.outcome)
      )
    )
  )

  given ruleOutcomeCodec: Codec[RuleOutcome] = Codec.from(
    Decoder.instance { cursor =>
      cursor.get[String]("kind").flatMap {
        case "evaluated" =>
          cursor.get[Int]("unmeasuredSubjects").flatMap { count =>
            ensure(count >= 0, cursor, "unmeasuredSubjects must be non-negative")
              .as(RuleOutcome.Evaluated(count))
          }
        case "not-evaluated" =>
          for {
            rawCode <- cursor.get[String]("code")
            code <- ErrorCode
              .fromWire(rawCode)
              .toRight(DecodingFailure(s"unknown error code '$rawCode'", cursor.history))
            message <- boundedString(cursor, "message", MaxErrorMessageLength, allowEmpty = false)
          } yield RuleOutcome.NotEvaluated(InfrastructureError.Remote(code, message, Nil))
        case other => Left(DecodingFailure(s"unknown rule outcome '$other'", cursor.history))
      }
    },
    Encoder.instance {
      case RuleOutcome.Evaluated(count) =>
        Json.obj(
          "kind" -> Json.fromString("evaluated"),
          "unmeasuredSubjects" -> Json.fromInt(count)
        )
      case RuleOutcome.NotEvaluated(error) =>
        Json.obj(
          "kind" -> Json.fromString("not-evaluated"),
          "code" -> Json.fromString(error.code.wire),
          "message" -> Json.fromString(error.message.take(MaxErrorMessageLength))
        )
    }
  )

  private def keyOf(cluster: ClusterId): StoreKey = StoreKey(StoreSection.Alerts, cluster.value)

  private def clusterFrom(key: StoreKey): Option[ClusterId] =
    Option.when(key.section == StoreSection.Alerts)(key.id).flatMap(ClusterId.from(_).toOption)

  private def malformed(key: StoreKey, why: String): KuiError =
    StoreError.toKuiError(StoreError.MalformedRecord(key.render, s"alert payload: $why"))

  private def boundRuleState(state: AlertRuleState): AlertRuleState =
    AlertRuleState(state.rebalancingSince.toList.sortBy(_._1).take(MaxRebalancingGroups).toMap)

  private def coalesceOpenEvents(events: Vector[AlertEvent]): Vector[AlertEvent] = {
    val (open, closed) = events.partition(_.isOpen)
    val onePerCondition = open.groupBy(_.key).valuesIterator.map { duplicates =>
      val earliest = duplicates.reduceLeft { (left, right) =>
        if left.openedAt.isBefore(right.openedAt) ||
          (left.openedAt == right.openedAt && left.id.value <= right.id.value)
        then left
        else right
      }
      val latest = duplicates.reduceLeft { (left, right) =>
        if left.lastSeenAt.isAfter(right.lastSeenAt) then left else right
      }
      earliest.copy(
        lastSeenAt = latest.lastSeenAt,
        severity = latest.severity,
        title = latest.title,
        detail = latest.detail
      )
    }

    closed ++ onePerCondition
  }

  private def boundedString(
      cursor: HCursor,
      field: String,
      maximum: Int,
      allowEmpty: Boolean
  ): Decoder.Result[String] =
    cursor.get[String](field).flatMap { value =>
      ensure(
        value.length <= maximum && (allowEmpty || value.nonEmpty),
        cursor,
        s"$field must be ${if allowEmpty then "at most" else "between 1 and"} $maximum characters"
      ).as(value)
    }

  private def enumValue[A](
      cursor: HCursor,
      field: String,
      decode: String => Option[A],
      label: String
  ): Decoder.Result[A] =
    cursor
      .get[String](field)
      .flatMap(raw => decode(raw).toRight(DecodingFailure(s"$field is not a known $label", cursor.history)))

  private def ensure(condition: Boolean, cursor: HCursor, message: String): Decoder.Result[Unit] =
    Either.cond(condition, (), DecodingFailure(message, cursor.history))
}

/** Preserves the named KUI error for logs while crossing an `F[A]` port whose shape predates durability. */
final private class DurableAlertStoreFailure(val error: KuiError) extends RuntimeException(error.message)
