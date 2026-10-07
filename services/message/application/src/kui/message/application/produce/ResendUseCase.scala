package kui.message.application.produce

import cats.effect.kernel.Async
import cats.syntax.all.*

import kui.kernel.browse.{Direction, PollBudget, SeekMode}
import kui.kernel.error.{ApplicationError, FieldError, KuiError}
import kui.message.application.{RawRecord, RecordSource, ScanCompletion, ScanEvent}
import kui.message.domain.*
import kui.security.Principal
import kui.security.audit.MutationKind

/** How much one resend may copy, and why there is a ceiling at all.
  *
  * A resend is a read and a series of writes, and both halves are unbounded in principle: an operator can ask
  * for a range of ten million offsets as easily as for ten. The ceiling turns "KUI became unresponsive" into
  * "KUI said no and told me the number", which is the same argument `PollBudget` makes for a browse — except
  * that this one also protects the *destination* topic, which a browse never touches.
  *
  * Configuration rather than a constant (`kui.message.resend.maxRecords`): ten thousand is a sensible
  * afternoon's dead-letter replay and a poor answer for somebody rebuilding a topic.
  */
final case class ResendLimits(maxRecords: Long)

object ResendLimits {
  val Default: ResendLimits = ResendLimits(maxRecords = 10000L)
  given CanEqual[ResendLimits, ResendLimits] = CanEqual.derived
}

/** Copying a range of records from one topic into another, byte for byte (MP-003).
  *
  * This is the feature Kouncil has and the other reference products do not, and the reason an operator wants
  * it is nearly always the same: a dead-letter topic holds the records that failed, the bug behind them has
  * been fixed, and those records need to go back where they came from.
  *
  * ==Nothing on this path is ever deserialized==
  *
  * Not as an optimisation. Three things follow from it, and all three are the point:
  *
  *   - a topic KUI cannot decode is still copyable, which is the case a resend is most often needed for;
  *   - the destination gets the producer's original bytes, so a consumer downstream cannot tell a replayed
  *     record from the first one — which is what makes the replay a replay rather than a new record;
  *   - masking (ADR-023) is *structurally* impossible here rather than merely omitted. A masked value written
  *     into the destination topic would be a corruption that outlives every screen that showed it.
  *
  * ==A resend is not atomic, and the type says so==
  *
  * It is a read and a series of produces. Cancelled or failed halfway it leaves what it already wrote, and
  * [[ResendResult]] reports how far it got — `read`, `produced`, and a failure per source offset. A caller
  * that assumes otherwise will write a retry that duplicates records, so the result is shaped to make the
  * assumption impossible to hold: there is no `Boolean` anywhere in it.
  */
trait ResendUseCase[F[_]] {

  /** @param principal
    *   who is copying the records, so the audit record names them. Verified by the route.
    */
  def resend(principal: Principal, request: ResendRequest): F[Either[KuiError, ResendResult]]
}

object ResendUseCase {

  def make[F[_]: Async](
      producers: RecordProducers[F],
      records: RecordSource[F],
      guard: MutationGuard[F],
      budget: PollBudget,
      limits: ResendLimits = ResendLimits.Default
  ): ResendUseCase[F] =
    new ResendUseCase[F] {

      def resend(principal: Principal, request: ResendRequest): F[Either[KuiError, ResendResult]] =
        guard.guard(
          principal = principal,
          cluster = request.cluster,
          kind = MutationKind.Resend,
          resource = s"${request.source.topic.value}:${request.source.partition.value}",
          detail = Map(
            "destination" -> request.destination.topic.value,
            "from" -> request.source.offsets.from.value.toString,
            "until" -> request.source.offsets.until.value.toString,
            "keepHeaders" -> request.keepHeaders.toString
          )
        ) {
          within(request, limits) match {
            case Left(error) => error.asLeft[ResendResult].pure[F]
            case Right(()) =>
              producers.forCluster(request.cluster).use {
                case Left(error) => error.asLeft[ResendResult].pure[F]
                case Right(producer) => copy(producer, request)
              }
          }
        }

      /** Read the window, then write what came back.
        *
        * The destination's partition count is checked *first*, before a single source record is read, because
        * reading a million records and only then discovering there is nowhere to put them is the expensive
        * way to fail.
        */
      private def copy(
          producer: RecordProducer[F],
          request: ResendRequest
      ): F[Either[KuiError, ResendResult]] =
        producer.partitionCount(request.destination.topic).flatMap {
          case Left(error) => error.asLeft[ResendResult].pure[F]
          case Right(partitions) =>
            ProduceUseCase.partitionWithin(
              request.destination.partition,
              partitions,
              request.destination.topic
            ) match {
              case Left(error) => error.asLeft[ResendResult].pure[F]
              case Right(_) => read(request).flatMap(_.flatTraverse(write(producer, request, _)))
            }
        }

      /** The source window, as raw records.
        *
        * It goes through the same [[RecordSource]] a browse uses, deliberately. Seeking to an offset, walking
        * a partition and stopping at the end of a window is arithmetic that took a milestone to get right,
        * and a second implementation of it here would be a second set of off-by-ones — the kind that copies
        * one record twice or drops the last one, which on a replay is a defect nobody notices until the
        * duplicate reaches production.
        *
        * The records are filtered to the requested range afterwards anyway: `limit` bounds how many come
        * back, not which, and retention moving underneath the read is a normal condition rather than a fault.
        */
      private def read(request: ResendRequest): F[Either[KuiError, List[RawRecord]]] =
        BrowseRequest.of(
          cluster = request.cluster,
          topic = request.source.topic,
          seek = SeekMode.AtOffsets(Map(request.source.partition -> request.source.offsets.from)),
          direction = Some(Direction.Forward),
          partitions = Some(Set(request.source.partition)),
          limit = Some(request.source.offsets.size.toInt),
          isolation = None,
          keySerde = None,
          valueSerde = None,
          stringFilter = None,
          filter = None,
          live = false,
          limits = BrowseLimits(request.source.offsets.size.toInt, request.source.offsets.size.toInt)
        ) match {
          case Left(error) => error.asLeft[List[RawRecord]].pure[F]
          case Right(browse) =>
            records
              .scan(browse, budget, Map(request.source.partition -> request.source.offsets.until))
              .compile
              .toList
              .map(collected =>
                collected.collectFirst { case ScanEvent.Failed(error) => error } match {
                  // A failure part-way through the read is the whole request's failure: copying the first
                  // half of a range without saying so would leave the destination in a state the operator
                  // did not ask for and cannot see.
                  case Some(error) => error.asLeft[List[RawRecord]]
                  case None if !collected.exists {
                        case ScanEvent.Completed(ScanCompletion.End) => true
                        case _ => false
                      } =>
                    ApplicationError
                      .Refused(
                        kui.kernel.error.ErrorCode.InvalidState,
                        "the source range was not completely read; no records were written; use a smaller range"
                      )
                      .asLeft[List[RawRecord]]
                  case None =>
                    collected
                      .collect { case ScanEvent.Record(record) => record }
                      .filter(record => request.source.offsets.contains(record.offset))
                      .asRight[KuiError]
                }
              )
        }

      private def write(
          producer: RecordProducer[F],
          request: ResendRequest,
          source: List[RawRecord]
      ): F[Either[KuiError, ResendResult]] =
        producer
          .send(source.map(asProduced(request, _)))
          .map(_.map { outcomes =>
            ResendResult(
              read = source.length.toLong,
              produced = outcomes.count(_.isRight).toLong,
              failures = source.zip(outcomes).collect { case (record, Left(error)) =>
                ResendFailure(record.offset, error)
              }
            )
          })

      /** One source record as one destination record. The bytes are the same objects, not copies of a decoded
        * form: `key`, `value` and every header value travel through untouched.
        */
      private def asProduced(request: ResendRequest, record: RawRecord): RawProducerRecord =
        RawProducerRecord(
          topic = request.destination.topic,
          partition = request.destination.partition,
          key = record.key,
          value = record.value,
          // Dropping the headers is a real choice an operator makes: a replayed record often carries the
          // retry counters and dead-letter stamps of the machinery that failed it, and feeding those back
          // in is how a record loops forever.
          headers = if request.keepHeaders then record.headers else Nil,
          // Only a `CreateTime` source has a timestamp worth replaying — it is the moment the event
          // actually happened. A `LogAppendTime` (or unknown) source's timestamp is the broker's own
          // bookkeeping from the first write, and stamping the copy with it would misrepresent when the
          // replay happened rather than preserve anything.
          timestamp = Option.when(record.timestampType == TimestampType.CreateTime)(record.timestamp)
        )
    }

  /** Whether the range is one this deployment is willing to copy in a single request. */
  def within(request: ResendRequest, limits: ResendLimits): Either[KuiError, Unit] =
    if request.source.offsets.size > limits.maxRecords then
      Left(
        ApplicationError.Invalid(
          s"a resend may copy at most ${limits.maxRecords} records at a time, and this range holds " +
            s"${request.source.offsets.size}; copy it in several ranges",
          List(FieldError.of("ranges", s"a range of at most ${limits.maxRecords} offsets"))
        )
      )
    else Right(())
}
