package kui.message.application

import java.nio.charset.StandardCharsets
import java.util.Locale

import scala.concurrent.duration.FiniteDuration

import cats.data.NonEmptySet
import cats.effect.kernel.{Clock, Concurrent, Ref}
import cats.syntax.all.*
import fs2.{Chunk, Stream}

import kui.kernel.browse.{Direction, PollBudget, SeekMode}
import kui.kernel.error.KuiError
import kui.kernel.serde.Target
import kui.kernel.{ClusterId, Offset, PartitionId, TopicName}
import kui.message.application.cursor.{BrowseCursor, CursorCodec}
import kui.message.domain.ports.{
  ClusterProfileSource,
  CompiledFilter,
  FilterSource,
  FilterVerdict,
  SerdeSource
}
import kui.message.domain.{BrowseLimits, BrowseRequest, DecodeError, DecodedRecord, FilterRef, RenderedHeader}

/** Why a browse stopped. */
enum BrowseEnd {

  /** The caller's `limit` was reached and there is more where that came from. */
  case Limit

  /** The raw record, byte, or time budget was spent before the requested match limit was reached. */
  case Budget

  /** Every selected partition was read to its end. Asking again returns nothing new. */
  case Exhausted
}

object BrowseEnd {
  given CanEqual[BrowseEnd, BrowseEnd] = CanEqual.derived
}

/** One thing a browse has to say, in the vocabulary of this layer rather than of the wire.
  *
  * These are not SSE events and they deliberately do not know that SSE exists: rule A3 keeps `libs/http` and
  * the contract out of this module, and `services/message/api` is the one place that maps a [[BrowseEvent]]
  * onto a frame (ADR-033). The mapping is one `match`, and having it in one place is what lets the use case
  * be tested by reading a list.
  */
enum BrowseEvent {

  /** What the browse is doing, in words, for the status line of a stream that has not produced a record yet.
    * A browse that said nothing until its first record is indistinguishable from a hung one.
    */
  case Phase(name: String)

  case Record(record: DecodedRecord)

  /** Progress. `read` counts records taken from Kafka, `delivered` counts the ones that survived the filter;
    * the gap between them is the number that tells a user their filter is doing something.
    */
  case Consumed(
      bytes: Long,
      read: Long,
      delivered: Long,
      filterErrors: Long,
      elapsed: FiniteDuration,
      budget: PollBudget
  )

  /** The browse ended on purpose. `cursor` is the signed continuation of ADR-026, and it is `None` whenever
    * asking again would be pointless.
    */
  case Finished(reason: BrowseEnd, cursor: Option[String])

  /** The browse ended because something broke. It carries the ordinary `KuiError`, so the layer above renders
    * it with the code it already knows rather than with a second error shape (ADR-034).
    */
  case Failed(error: KuiError)
}

object BrowseEvent {
  given CanEqual[BrowseEvent, BrowseEvent] = CanEqual.derived
}

/** Browsing a topic: resolve the cluster, read records, decode them, filter them, and account for what was
  * spent.
  *
  * ==The rule that shapes the whole file==
  *
  * **Decoding never fails a browse.** A record no serde can read is delivered through the fallback with the
  * failure attached to it, and the stream carries on (ADR-035). The quickstart seeds `audit.log.raw` with
  * deliberately non-JSON payloads for exactly this reason: a browser asked to show that topic must show its
  * bytes, not an error page, and must certainly not stop at the first line.
  *
  * Nothing here materialises a topic. Records arrive one at a time from [[RecordSource]] and leave one at a
  * time; the only state kept is a handful of counters and the first and last offset seen per partition, which
  * is what the continuation cursor is built from.
  */
trait BrowseUseCase[F[_]] {
  def browse(request: BrowseRequest, budget: PollBudget): Stream[F, BrowseEvent]

  /** The browse that continues where a finished one stopped (ADR-026).
    *
    * A browse ends by emitting a signed cursor naming, per partition, the offset the next page starts at.
    * Handing that cursor back is what "load more" is: the client does not compute the next offsets, and
    * cannot, because forward and backward boundaries are different numbers for the same place and getting
    * that arithmetic wrong duplicates or skips exactly one record per page.
    *
    * It lives on the use case rather than in the API layer because the cursor is verified and decoded by the
    * `CursorCodec` this object already holds. A route that decoded one itself would need the signing key,
    * which is precisely the thing the API layer must not have.
    *
    * `stringFilter` is *not* carried by the cursor and is taken again here. The cursor names a position, a
    * direction, a page size, the serdes and the saved filter — the things that decide which records exist in
    * the next page. A plain substring is applied to the records after they are decoded, so it can be changed
    * between pages without invalidating the position, and a client that narrows its filter while paging gets
    * what it asked for rather than a rejected cursor.
    */
  def resume(
      cluster: ClusterId,
      topic: TopicName,
      cursor: String,
      stringFilter: Option[String],
      limits: BrowseLimits,
      filterSource: Option[String] = None
  ): F[Either[KuiError, BrowseRequest]]
}

object BrowseUseCase {

  /** How many delivered records go by between two `consumed` events.
    *
    * Often enough that a long filtered scan visibly moves, rarely enough that a fast browse does not spend
    * its bandwidth on progress reports about itself.
    */
  val ProgressEvery: Int = 25

  /** How long a minted cursor stays usable. `BrowseCursor.DefaultTtlSeconds`, as a duration. */
  val CursorTtl: FiniteDuration = FiniteDuration(BrowseCursor.DefaultTtlSeconds, "seconds")

  def make[F[_]: {Concurrent, Clock}](
      clusters: ClusterProfileSource[F],
      serdes: SerdeSource[F],
      source: RecordSource[F],
      cursors: CursorCodec[F],
      filters: FilterSource[F],
      masking: RecordMasking[F]
  ): BrowseUseCase[F] =
    new BrowseUseCase[F] {

      def resume(
          cluster: ClusterId,
          topic: TopicName,
          cursor: String,
          stringFilter: Option[String],
          limits: BrowseLimits,
          filterSource: Option[String]
      ): F[Either[KuiError, BrowseRequest]] =
        Clock[F].realTimeInstant
          .flatMap(now => cursors.decode(cursor, (cluster, topic), now))
          .map(_.flatMap(decoded => requestOf(decoded, stringFilter, filterSource, limits)))

      /** The cursor, as the browse it describes.
        *
        * Every partition resumes at its own offset, which is what `AtOffsets` is for and why the seek grammar
        * keeps a per-partition form the reference product dropped: a continuation that could only express one
        * offset for every partition could not express what a cursor already means.
        *
        * The partition subset is the cursor's own key set rather than "all of them". A partition added to the
        * topic since the first page must not appear halfway through a paged read with no start position of
        * its own — it would arrive from wherever the consumer happened to land.
        */
      private def requestOf(
          cursor: BrowseCursor,
          stringFilter: Option[String],
          filterSource: Option[String],
          limits: BrowseLimits
      ): Either[KuiError, BrowseRequest] =
        BrowseRequest.of(
          cluster = cursor.cluster,
          topic = cursor.topic,
          seek = SeekMode.AtOffsets(cursor.perPartitionNext),
          direction = Some(cursor.direction),
          partitions = Some(cursor.perPartitionNext.keySet),
          limit = Some(cursor.limit),
          isolation = Some(cursor.isolation),
          keySerde = cursor.keySerde,
          valueSerde = cursor.valueSerde,
          stringFilter = stringFilter,
          filter = cursor.filterId.flatMap(id => FilterRef.of(id, filterSource).toOption),
          // A continuation is never a tail: `live` and a start position are mutually exclusive, and a cursor
          // is nothing but a start position.
          live = false,
          limits = limits
        )

      def browse(request: BrowseRequest, budget: PollBudget): Stream[F, BrowseEvent] =
        Stream.emit(BrowseEvent.Phase(ResolvingCluster)) ++
          Stream.eval(clusters.cluster(request.cluster)).flatMap {
            case Left(error) => Stream.emit(BrowseEvent.Failed(error))
            // The smart filter is compiled *before* any Kafka client is opened, and a compile failure ends
            // the request rather than the stream. An expression with a typo in it must not open a consumer
            // and read a million records to tell the user nothing matched.
            case Right(_) =>
              Stream.eval(predicateFor(request)).flatMap {
                case Left(error) => Stream.emit(BrowseEvent.Failed(error))
                case Right(predicate) =>
                  // The mask is resolved once for the whole browse, before the first record is read
                  // (ADR-023, DM-001). Once, because which rules reach this topic cannot change
                  // mid-stream and re-deciding per record would re-walk the rule list a million times
                  // on a browse of a million records; before, because `kui.masking.applied` counts
                  // browses on which masking applied, and a browse that delivered nothing still had
                  // its rules in force.
                  Stream
                    .eval(masking.forTopic(request.cluster, request.topic))
                    .flatMap(mask => reading(request, budget, predicate, mask))
              }
          }

      /** The compiled smart filter, or the one that lets everything through.
        *
        * "No filter" is a predicate rather than a branch around the filtering code, so that the filtered and
        * unfiltered paths cannot drift apart — and they have drifted in every product where one of them was
        * an `if`.
        */
      private def predicateFor(request: BrowseRequest): F[Either[KuiError, Option[CompiledFilter[F]]]] =
        request.filter match {
          case None => Option.empty[CompiledFilter[F]].asRight[KuiError].pure[F]
          case Some(reference) => filters.compile(request.cluster, reference).map(_.map(Some(_)))
        }

      private def reading(
          request: BrowseRequest,
          budget: PollBudget,
          filter: Option[CompiledFilter[F]],
          mask: RecordMask
      ): Stream[F, BrowseEvent] =
        Stream.emit(BrowseEvent.Phase(ReadingRecords)) ++
          Stream
            .eval((Clock[F].monotonic, Ref.of[F, State](State.empty)).tupled)
            .flatMap { case (startedAt, state) =>
              records(request, budget, state, filter, mask, startedAt) ++ ending(
                request,
                budget,
                state,
                startedAt
              )
            }

      /** The record events, and the progress events between them. */
      private def records(
          request: BrowseRequest,
          budget: PollBudget,
          state: Ref[F, State],
          filter: Option[CompiledFilter[F]],
          mask: RecordMask,
          startedAt: FiniteDuration
      ): Stream[F, BrowseEvent] =
        source
          .browse(request, budget)
          // The first failure ends the stream, and is kept: `ending` turns it into the terminal `error`
          // event. `takeThrough` rather than `takeWhile` because the failing element is the one carrying
          // the reason.
          .takeThrough(_.isRight)
          .evalMap {
            case Left(error) => state.update(_.copy(failure = Some(error))).as(Step.stop)
            case Right(raw) => deliver(request, budget, state, raw, filter, mask, startedAt)
          }
          .takeThrough(_.more)
          .flatMap(step => Stream.chunk(step.events))

      /** One record: decode it, account for it, and decide whether it is shown. */
      private def deliver(
          request: BrowseRequest,
          budget: PollBudget,
          state: Ref[F, State],
          raw: RawRecord,
          filter: Option[CompiledFilter[F]],
          mask: RecordMask,
          startedAt: FiniteDuration
      ): F[Step] =
        for {
          record <- decode(request, raw, mask)
          // Both filters, in the cheap-first order: the substring is a `contains` over text already in
          // hand, and the expression is a program. A record the substring rejected is never handed to the
          // engine, which is what keeps a smart filter's cost proportional to what it is asked about.
          verdict <-
            if matches(request, record) then verdictOf(filter, record)
            else FilterVerdict.DidNotMatch.pure[F]
          matched = verdict == FilterVerdict.Matched
          next <- state.updateAndGet(_.saw(raw, matched, failed(verdict)))
          progress <-
            if next.read % ProgressEvery.toLong == 0L then
              Clock[F].monotonic.map(now =>
                Chunk.singleton(
                  BrowseEvent
                    .Consumed(
                      next.bytes,
                      next.read,
                      next.delivered,
                      next.filterErrors,
                      now - startedAt,
                      budget
                    )
                )
              )
            else Chunk.empty[BrowseEvent].pure[F]
        } yield Step(
          events = (if matched then Chunk.singleton(BrowseEvent.Record(record)) else Chunk.empty) ++ progress,
          // A tail has no total. `limit` is a page size, and a page is a thing a bounded browse has; a
          // browse that is still open after an hour has delivered whatever was written in that hour and
          // is not finished. The bound on a tail is on the *screen* — `BrowseSession.MaxRows` keeps the
          // newest five hundred rows and drops the rest — because that is where an unbounded stream can
          // be bounded without deciding on the user's behalf that they have watched enough.
          more = request.live || next.delivered < request.limit.toLong
        )

      /** The terminal events: either the failure that stopped the browse, or the accounting and the cursor.
        */
      private def ending(
          request: BrowseRequest,
          budget: PollBudget,
          state: Ref[F, State],
          startedAt: FiniteDuration
      ): Stream[F, BrowseEvent] =
        Stream.eval((state.get, Clock[F].monotonic).tupled).flatMap { case (finalState, now) =>
          finalState.failure match {
            case Some(error) => Stream.emit(BrowseEvent.Failed(error))
            case None =>
              val elapsed = now - startedAt
              val remaining = budget.consume(
                records = math.min(finalState.read, Int.MaxValue.toLong).toInt,
                bytes = finalState.bytes,
                elapsed = elapsed
              )
              val reason =
                if finalState.delivered >= request.limit.toLong then BrowseEnd.Limit
                else if remaining.isExhausted then BrowseEnd.Budget
                else BrowseEnd.Exhausted

              Stream.emit(
                BrowseEvent.Consumed(
                  finalState.bytes,
                  finalState.read,
                  finalState.delivered,
                  finalState.filterErrors,
                  elapsed,
                  budget
                )
              ) ++ Stream.eval(cursorFor(request, finalState, reason)).map(BrowseEvent.Finished(reason, _))
          }
        }

      /** A continuation, but only where continuing means anything.
        *
        * A browse that reached the end of every partition has nothing to continue, and handing the browser a
        * cursor there would put a "load more" button under a screen that can only ever answer "nothing". A
        * cursor that fails to sign is dropped rather than raised: the page the user is looking at is correct,
        * and the honest consequence is a missing button, not a failed browse.
        */
      private def cursorFor(
          request: BrowseRequest,
          state: State,
          reason: BrowseEnd
      ): F[Option[String]] =
        if (reason != BrowseEnd.Limit && reason != BrowseEnd.Budget) || state.read == 0L then
          Option.empty[String].pure[F]
        else
          Clock[F].realTimeInstant.flatMap { now =>
            request.direction match {
              case Direction.Forward =>
                withUnseenPartitions(request, state.last, start => Offset.unsafe(start.value - 1L))
                  .flatMap(last => cursors.encode(BrowseCursor.afterForward(request, last, now, CursorTtl)))
                  .map(_.toOption)
              case Direction.Backward =>
                withUnseenPartitions(request, state.last, identity)
                  .flatMap(oldest =>
                    cursors.encode(BrowseCursor.beforeBackward(request, oldest, now, CursorTtl))
                  )
                  .map(_.toOption)
            }
          }

      /** `seen`, with an entry added for every assigned partition that never yielded a raw record.
        *
        * A partition can be starved by an uneven poll before the global `limit` is reached; without this it
        * is silently missing from `seen` and then from every `perPartitionNext` built from it, which drops it
        * from every subsequent page (`partitions = Some(cursor.perPartitionNext.keySet)` in `requestOf`).
        * Only the gap is filled — a partition already in `seen` keeps the boundary it actually observed.
        */
      private def withUnseenPartitions(
          request: BrowseRequest,
          seen: Map[PartitionId, Offset],
          fallback: Offset => Offset
      ): F[Map[PartitionId, Offset]] =
        resolvedStarts(request).map {
          case None => seen
          case Some(starts) =>
            starts.foldLeft(seen) { case (acc, (partition, start)) =>
              if acc.contains(partition) then acc else acc.updated(partition, fallback(start))
            }
        }

      /** The offset every assigned partition actually starts from.
        *
        * `AtOffsets`/`AtOffset` name one for every partition already, so the answer is resolved right here
        * with no I/O. `Beginning`, `Latest` and `AtTimestamp` resolve to a per-partition offset only once a
        * broker is asked — and so does a request that names its own `partitions` under one of those modes,
        * since which offset each of them starts at is still unknown above [[RecordSource]] — so both fall
        * back to [[RecordSource.assignedStarts]], the same resolution `browse` itself would seek to. A
        * failure there is treated the same as "unknown": the page already shown is correct, and the honest
        * consequence of not being able to ask is a starved partition staying omitted, not a guessed offset.
        */
      private def resolvedStarts(request: BrowseRequest): F[Option[Map[PartitionId, Offset]]] =
        request.partitions.flatMap(startOffsets(request.seek, _)) match {
          case resolved @ Some(_) => resolved.pure[F]
          case None => source.assignedStarts(request).map(_.toOption)
        }

      /** The offset each named partition actually starts from, without asking anything — only possible when
        * the seek already names one per partition.
        */
      private def startOffsets(
          seek: SeekMode,
          assigned: NonEmptySet[PartitionId]
      ): Option[Map[PartitionId, Offset]] =
        seek match {
          case SeekMode.AtOffsets(perPartition) =>
            Some(assigned.toSortedSet.toList.flatMap(p => perPartition.get(p).map(p -> _)).toMap)
          case SeekMode.AtOffset(offset) =>
            Some(assigned.toSortedSet.toList.map(_ -> offset).toMap)
          case SeekMode.Beginning | SeekMode.Latest | SeekMode.AtTimestamp(_) => None
        }

      /** The smart filter's answer about one record, or `Matched` when there is no smart filter.
        *
        * A record the filter could not decide about is **excluded** and counted, never delivered and never
        * fatal (ADR-017). Excluded because a filter is a narrowing and a user who asked for failures does not
        * want successes when the predicate breaks; counted because a filter that errors on every record would
        * otherwise look exactly like a filter that matches nothing.
        */
      private def verdictOf(filter: Option[CompiledFilter[F]], record: DecodedRecord): F[FilterVerdict] =
        filter.fold(FilterVerdict.Matched.pure[F])(_.test(record))

      /** Both halves of a record, read by a serde and then masked.
        *
        * The mask is applied here and nowhere else, which is what makes ADR-023's "before any DTO leaves the
        * service" true for every consumer of this stream at once — the record event, the string filter, the
        * smart filter and the cursor all see the same masked value, because there is only one. A mask applied
        * at the API layer instead would leave the two filters reading the original, which turns a filter into
        * a way of asking questions about a field nobody is allowed to see.
        */
      private def decode(request: BrowseRequest, raw: RawRecord, mask: RecordMask): F[DecodedRecord] =
        for {
          key <- serdes.decode(request.cluster, request.topic, Target.Key, request.keySerde, raw.key)
          value <- serdes.decode(request.cluster, request.topic, Target.Value, request.valueSerde, raw.value)
        } yield mask(
          DecodedRecord(
            partition = raw.partition,
            offset = raw.offset,
            timestamp = raw.timestamp,
            timestampType = raw.timestampType,
            key = key._1,
            value = value._1,
            headers = raw.headers.map(render),
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

  /** The plain-substring filter, applied after decoding and case-insensitively.
    *
    * After decoding and not before, because a user typing `order-4711` is looking for the text they can see
    * on the screen; a search of the raw bytes would miss it on every topic whose values are not plain text,
    * which is most of them.
    */
  def matches(request: BrowseRequest, record: DecodedRecord): Boolean =
    request.stringFilter match {
      case None => true
      case Some(needle) =>
        val wanted = needle.toLowerCase(Locale.ROOT)
        record.key.text.toLowerCase(Locale.ROOT).contains(wanted) ||
        record.value.text.toLowerCase(Locale.ROOT).contains(wanted) ||
        record.headers.exists(header =>
          header.key.toLowerCase(Locale.ROOT).contains(wanted) ||
            header.value.toLowerCase(Locale.ROOT).contains(wanted)
        )
    }

  /** A header's bytes as text.
    *
    * UTF-8 with replacement, never an exception: a header nobody can read is still a header worth showing
    * beside the record, and a browse that failed on one would fail on every record of a topic whose producer
    * writes a binary trace id.
    */
  def render(header: RawHeader): RenderedHeader =
    RenderedHeader(header.key, header.value.fold("")(new String(_, StandardCharsets.UTF_8)))

  val ResolvingCluster: String = "resolving the cluster"
  val ReadingRecords: String = "reading records from Kafka"

  /** One record's worth of output, and whether the browse wants another. */
  final private case class Step(events: Chunk[BrowseEvent], more: Boolean)

  private object Step {

    /** What a failure produces: no events of its own — `ending` writes the terminal one — and no appetite for
      * more.
      */
    val stop: Step = Step(Chunk.empty, more = false)
  }

  /** Everything a browse remembers, which is deliberately not the records themselves.
    *
    * `last` is the final raw boundary offset processed per partition, and is what a continuation cursor is
    * built from: a forward browse resumes after it, while a backward browse uses it as the next half-open
    * window's upper bound.
    */
  /** True when the filter answered neither way about this record. */
  private def failed(verdict: FilterVerdict): Boolean = verdict match {
    case FilterVerdict.Failed(_) => true
    case FilterVerdict.Matched | FilterVerdict.DidNotMatch => false
  }

  final private case class State(
      read: Long,
      bytes: Long,
      delivered: Long,
      filterErrors: Long,
      last: Map[PartitionId, Offset],
      failure: Option[KuiError]
  ) {

    def saw(raw: RawRecord, matched: Boolean, filterFailed: Boolean): State =
      copy(
        read = read + 1L,
        bytes = bytes + raw.keySize.toLong + raw.valueSize.toLong + raw.headersSize.toLong,
        delivered = if matched then delivered + 1L else delivered,
        filterErrors = if filterFailed then filterErrors + 1L else filterErrors,
        last = last.updated(raw.partition, raw.offset)
      )
  }

  private object State {
    val empty: State = State(0L, 0L, 0L, 0L, Map.empty, None)
  }
}
