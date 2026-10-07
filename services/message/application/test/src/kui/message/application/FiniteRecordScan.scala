package kui.message.application

import cats.effect.IO
import fs2.Stream

import kui.kernel.browse.PollBudget

/** A finite test log with the same explicit budget/end contract as the broker adapter. */
object FiniteRecordScan {
  def apply(records: List[RawRecord], budget: PollBudget): Stream[IO, ScanEvent] = {
    def loop(rest: List[RawRecord], remaining: PollBudget): Stream[IO, ScanEvent] =
      rest match {
        case Nil => Stream.emit(ScanEvent.Completed(ScanCompletion.End))
        case _ if remaining.isExhausted =>
          Stream.emit(
            ScanEvent.Completed(
              if remaining.recordsLeft == 0 then ScanCompletion.RecordBudget else ScanCompletion.ByteBudget
            )
          )
        case record :: tail =>
          val bytes = math.max(0, record.keySize).toLong + math.max(0, record.valueSize).toLong +
            math.max(0, record.headersSize).toLong
          Stream.emit(ScanEvent.Record(record)) ++ loop(tail, remaining.consume(1, bytes))
      }
    loop(records, budget)
  }
}
