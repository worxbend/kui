package kui.filter

/** A record, as a filter sees it.
  *
  * Deliberately neither the wire DTO nor `libs/kafka`'s `RawRecord`. Taking the DTO would make the browse
  * pipeline serialise before it filters; taking `RawRecord` would make a filter run on undecoded bytes and
  * would put a Kafka dependency on this module. This is the decoded middle, which is the only place a
  * predicate over `record.value.status` can be evaluated at all.
  *
  * Everything is already a `String` because that is what a serde produces (`DeserializeResult.text`). Where a
  * value happens to be JSON, the CEL environment additionally exposes it parsed, so both
  * `record.valueAsText.contains('x')` and `record.value.status == 'FAILED'` work on the same record.
  */
final case class FilterableRecord(
    partition: Int,
    offset: Long,
    timestampMs: Long,
    keyAsText: String,
    valueAsText: String,
    headers: Map[String, String]
)

object FilterableRecord {
  given CanEqual[FilterableRecord, FilterableRecord] = CanEqual.derived
}

/** Why a filter could not decide about a record.
  *
  * Both cases are per-record and neither is fatal. A record a filter could not decide about is **excluded**
  * and the error is counted — excluded rather than included, because a filter is a narrowing and a user who
  * asked for failures does not want successes when the predicate breaks. The count reaches the screen
  * (`consumed.filterErrors`), so a filter that fails on every record is visible rather than silent.
  */
enum FilterError {

  /** The program ran and did not produce a boolean: a missing field, a type error, a non-boolean result, or a
    * comprehension that crossed `FilterLimits.maxComprehensionIterations`.
    */
  case Runtime(message: String)

  /** The program did not finish inside its per-record deadline. */
  case Timeout(afterMs: Long)

  /** One sentence, safe to show a user beside the record count it explains. */
  def describe: String = this match {
    case Runtime(text) => text
    case Timeout(afterMs) => s"the filter did not finish within ${afterMs}ms"
  }

  /** The `kind` attribute of `kui.filter.errors`, and the field name the stream event uses. */
  def kind: String = this match {
    case Runtime(_) => "runtime"
    case Timeout(_) => "timeout"
  }
}

object FilterError {
  given CanEqual[FilterError, FilterError] = CanEqual.derived
}

/** The one predicate shape the browse pipeline knows.
  *
  * CEL is an instance of it; so is the string filter; so is [[MessagePredicate.always]]. That is what makes
  * DEVPLAN's R-5 fallback — "if CEL cannot be shipped, MS-006 ships and MS-007 moves to M5" — a change of
  * which instance is constructed rather than a redesign of the pipeline.
  */
trait MessagePredicate[F[_]] {
  def test(record: FilterableRecord): F[Either[FilterError, Boolean]]
}

object MessagePredicate {

  /** The no-filter case, as a predicate rather than as a branch.
    *
    * The browse pipeline runs this when the user asked for nothing, instead of skipping the filter step. One
    * code path means the filtered and unfiltered behaviours cannot drift apart — and the two *have* drifted
    * in every product where "no filter" was an `if` around the filtering code.
    */
  def always[F[_]](using F: cats.Applicative[F]): MessagePredicate[F] = new MessagePredicate[F] {
    def test(record: FilterableRecord): F[Either[FilterError, Boolean]] = F.pure(Right(true))
  }
}
