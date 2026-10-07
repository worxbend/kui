package kui.message.application

import scala.concurrent.duration.FiniteDuration

import cats.effect.kernel.{Ref, Temporal}
import cats.syntax.all.*
import fs2.Stream

import kui.kernel.TopicName
import kui.kernel.browse.{Direction, PollBudget, SeekMode}
import kui.kernel.error.KuiError
import kui.kernel.serde.Target
import kui.message.domain.ports.{ClusterProfileSource, SerdeSource}
import kui.message.domain.{
  BrowseLimits,
  BrowseRequest,
  DecodeError,
  DecodedRecord,
  PreparedMatch,
  TrackHit,
  TrackQuery
}

/** What a track has to say. */
enum TrackEvent {

  /** A record that matched, and the topic it was in. */
  case Hit(hit: TrackHit)

  /** How much has been read so far, so that a scan which matches nothing does not look like a scan that is
    * stuck. It is the same distinction the browse's `consumed` event makes, and for the same reason: reading
    * a million records and matching none of them is the ordinary case for a track, and without a number on
    * screen it is indistinguishable from an empty cluster.
    */
  case Scanned(topic: TopicName, read: Long, matched: Long)

  /** The scan finished. `truncated` is true unless every topic positively reached its captured end and the
    * hit limit was not reached. Budget exhaustion and unavailable topics must not look like a complete
    * search.
    */
  case Finished(read: Long, matched: Long, truncated: Boolean)

  /** The scan stopped because something broke. It carries the ordinary `KuiError` so that the layer above
    * renders it with the code it already knows.
    */
  case Failed(error: KuiError)
}

object TrackEvent {
  given CanEqual[TrackEvent, TrackEvent] = CanEqual.derived
}

/** Following one business event across several topics (ET-001, ADR-029).
  *
  * ==What it is==
  *
  * "Where did order 4711 go?" — the question a support engineer actually has, and the one thing a console
  * consumer cannot answer, because the answer spans six topics and they have to be read together and put in
  * time order. Kouncil's defining second feature, and the reason its users keep it.
  *
  * ==Why it is a scan and not an index==
  *
  * Because KUI holds no index and building one would be a database. So a track is a bounded read of each
  * named topic over a closed time window, and every bound is mandatory: `TrackQuery` refuses an unordered
  * window, a window wider than the deployment allows, and a query with no topics — because an unbounded scan
  * of a production cluster is not a search, it is an outage with a progress bar.
  *
  * ==Why there is no join here==
  *
  * ADR-029 is explicit: a track adds the topic to each hit and stops. Correlating hits into one event — the
  * graph, the causal order — is M9's work, and building it here on the assumption that this was it would be
  * the expensive mistake. The hits come back in the order they were read, and the screen sorts them by
  * timestamp, which is what a person means by "in order".
  *
  * ==Why the topics are read one at a time==
  *
  * A track over six topics is six Kafka consumers. Read together they are six times the load on a cluster
  * somebody is already investigating an incident on, and the wall-clock saving is smaller than it looks
  * because the budget bounds the whole scan either way. Sequential also gives the deterministic partial
  * result that makes the `limit` ending meaningful: the hits are "the first `limit` in this topic order", not
  * "whichever six consumers happened to win".
  */
trait TrackUseCase[F[_]] {
  def track(query: TrackQuery, budget: PollBudget): Stream[F, TrackEvent]
}

object TrackUseCase {

  /** How many records go by between two `Scanned` events. The same number the browse uses, for the same
    * reason: often enough that a long scan visibly moves, rarely enough that it does not spend its bandwidth
    * reporting on itself.
    */
  val ProgressEvery: Long = 500L

  def make[F[_]: Temporal](
      clusters: ClusterProfileSource[F],
      serdes: SerdeSource[F],
      source: RecordSource[F],
      masking: RecordMasking[F],
      limits: BrowseLimits = BrowseLimits.Default
  ): TrackUseCase[F] =
    new TrackUseCase[F] {

      def track(query: TrackQuery, budget: PollBudget): Stream[F, TrackEvent] =
        Stream.eval(clusters.cluster(query.cluster)).flatMap {
          case Left(error) => Stream.emit(TrackEvent.Failed(error))
          case Right(_) =>
            Stream.eval((Ref.of[F, Progress](Progress.empty), Temporal[F].monotonic).tupled).flatMap {
              case (progress, startedAt) =>
                scanning(query, budget, progress, startedAt)
                  .interruptAfter(budget.deadline) ++ ending(query, progress)
            }
        }

      /** Every named topic, in the order the caller named them, until the limit is reached. */
      private def scanning(
          query: TrackQuery,
          budget: PollBudget,
          progress: Ref[F, Progress],
          startedAt: FiniteDuration
      ): Stream[F, TrackEvent] = {
        val prepared = PreparedMatch.of(query.matcher)

        Stream
          .emits(query.topics.toList)
          .flatMap(topic => topicScan(query, topic, prepared, budget, progress, startedAt))
          // The scan stops as soon as the limit is reached. It also stops *between* topics — `topicScan`
          // checks before it opens anything — because without that a track whose first topic filled the
          // limit would still open a consumer on the other five and read them to no purpose.
          .through(stopAtLimit(query, progress))
      }

      private def topicScan(
          query: TrackQuery,
          topic: TopicName,
          prepared: PreparedMatch,
          budget: PollBudget,
          progress: Ref[F, Progress],
          startedAt: FiniteDuration
      ): Stream[F, TrackEvent] =
        requestFor(query, topic) match {
          // A topic name that will not make a legal browse is reported and skipped rather than failing the
          // whole track: five topics' worth of answers is a better answer than none.
          case Left(error) => Stream.emit(TrackEvent.Failed(error))
          case Right(request) =>
            Stream.eval((progress.get, Temporal[F].monotonic).tupled).flatMap { case (before, now) =>
              val remaining = budget.consume(before.read.toInt, before.bytes, now - startedAt)
              if before.matched >= query.limit.toLong || remaining.isExhausted then Stream.empty
              else
                // Once per topic, not once per record, and not once per track: masking rules are scoped
                // by topic pattern, so a track over six topics has six answers and each is constant for
                // the whole of its topic's scan.
                Stream.eval(masking.forTopic(query.cluster, topic)).flatMap { mask =>
                  source
                    .scan(request, remaining)
                    .takeThrough {
                      case ScanEvent.Failed(_) => false
                      case _ => true
                    }
                    .evalMap {
                      case ScanEvent.Failed(error) => (TrackEvent.Failed(error): TrackEvent).some.pure[F]
                      case ScanEvent.Completed(ScanCompletion.End) =>
                        progress
                          .update(now => now.copy(completedTopics = now.completedTopics + 1))
                          .as(Option.empty[TrackEvent])
                      case ScanEvent.Completed(_) => Option.empty[TrackEvent].pure[F]
                      case ScanEvent.Record(raw) => consider(query, topic, prepared, progress, raw, mask)
                    }
                    .unNone
                }
            }
        }

      /** One record: decode it, count it, and say so if it matched or if enough have gone by.
        *
        * The window's far end is checked here rather than by the browse, because a browse has no notion of
        * "until": it seeks to a timestamp and reads forwards. Both endpoints are checked for every record:
        * CreateTime timestamps can regress at later offsets, so neither endpoint is a stopping condition.
        */
      private def consider(
          query: TrackQuery,
          topic: TopicName,
          prepared: PreparedMatch,
          progress: Ref[F, Progress],
          raw: RawRecord,
          mask: RecordMask
      ): F[Option[TrackEvent]] =
        if raw.timestamp.isBefore(query.from) || raw.timestamp.isAfter(query.until) then
          progress.update(_.saw(raw, false)).as(Option.empty[TrackEvent])
        else
          decode(query, topic, raw, mask).flatMap { record =>
            val matched = prepared.matches(record)

            progress.updateAndGet(_.saw(raw, matched)).map { now =>
              if matched && now.matched <= query.limit.toLong then
                Some(TrackEvent.Hit(TrackHit(topic, record)))
              else if now.read % ProgressEvery == 0L then
                Some(TrackEvent.Scanned(topic, now.read, now.matched))
              else None
            }
          }

      /** Ends the stream once the limit has been reached, whichever topic reached it. */
      private def stopAtLimit(
          query: TrackQuery,
          progress: Ref[F, Progress]
      ): Stream[F, TrackEvent] => Stream[F, TrackEvent] =
        _.evalMap(event => progress.get.map(now => (event, now.matched >= query.limit.toLong)))
          .takeThrough((_, reached) => !reached)
          .map(_._1)

      private def ending(query: TrackQuery, progress: Ref[F, Progress]): Stream[F, TrackEvent] =
        Stream.eval(progress.get).map { now =>
          TrackEvent.Finished(
            now.read,
            now.matched,
            truncated = now.matched >= query.limit.toLong || now.completedTopics < query.topics.size
          )
        }

      /** One topic's slice of the track, as an ordinary browse.
        *
        * A forward read from the window's start, which is what makes the hits come back in time order per
        * topic and is why `AtTimestamp` exists. The limit is the *record* ceiling rather than the hit
        * ceiling: a scan may legitimately read every record in the window and match one of them.
        */
      private def requestFor(query: TrackQuery, topic: TopicName): Either[KuiError, BrowseRequest] =
        BrowseRequest.of(
          cluster = query.cluster,
          topic = topic,
          seek = SeekMode.AtTimestamp(query.from.toEpochMilli),
          direction = Some(Direction.Forward),
          partitions = None,
          limit = Some(limits.max),
          isolation = Some(query.isolation),
          keySerde = None,
          valueSerde = None,
          stringFilter = None,
          filter = None,
          live = false,
          limits = limits
        )

      /** The same decoding a browse does, so that what a track matches on is what the screen would show.
        *
        * Matching on the decoded text and not the raw bytes is the point: a person searching for `order-4711`
        * means the characters they can read, and a search of the bytes would miss it on every topic whose
        * values are not plain text — which is most of them.
        *
        * The mask is applied here for the same reason and with the same consequence as in the browse
        * (`RecordMasking`): a track matches on what the screen would show, so a masked field is not
        * trackable. A track that matched on the unmasked text would be a search over exactly the values the
        * rule hides, and it would return the record while drawing a row of asterisks — which tells the
        * searcher what they were looking for without ever printing it.
        */
      private def decode(
          query: TrackQuery,
          topic: TopicName,
          raw: RawRecord,
          mask: RecordMask
      ): F[DecodedRecord] =
        for {
          key <- serdes.decode(query.cluster, topic, Target.Key, None, raw.key)
          value <- serdes.decode(query.cluster, topic, Target.Value, None, raw.value)
        } yield mask(
          DecodedRecord(
            partition = raw.partition,
            offset = raw.offset,
            timestamp = raw.timestamp,
            timestampType = raw.timestampType,
            key = key._1,
            value = value._1,
            headers = raw.headers.map(BrowseUseCase.render),
            keySize = raw.keySize,
            valueSize = raw.valueSize,
            headersSize = raw.headersSize,
            decodeErrors = List(
              key._2.map(DecodeError(Target.Key, key._1.serde, _)),
              value._2.map(DecodeError(Target.Value, value._1.serde, _))
            ).flatten
          )
        )
    }

  /** How much has been read and how much has matched, across every topic in the scan. */
  final private case class Progress(read: Long, matched: Long, bytes: Long, completedTopics: Int) {
    def saw(record: RawRecord, hit: Boolean): Progress = {
      val size = math.max(0, record.keySize).toLong + math.max(0, record.valueSize).toLong +
        math.max(0, record.headersSize).toLong
      copy(
        read = read + 1L,
        matched = if hit then matched + 1L else matched,
        bytes = if size > Long.MaxValue - bytes then Long.MaxValue else bytes + size
      )
    }
  }

  private object Progress {
    val empty: Progress = Progress(0L, 0L, 0L, 0)
  }
}
