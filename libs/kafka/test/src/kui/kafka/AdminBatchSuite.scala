package kui.kafka

import scala.concurrent.duration.*

import cats.effect.testkit.TestControl
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import org.apache.kafka.common.errors.{TimeoutException, TopicAuthorizationException}

import kui.kernel.cluster.AdminTuning
import kui.kernel.error.ErrorCode
import kui.testkit.KuiIOSuite

/** The batching rules, including the two that are only visible in time.
  *
  * `parallelismIsBounded` and `theBatchTakesAsLongAsItsSlowestWave` are asserted with `TestControl` against
  * virtual time, so a change from `parTraverseN` to `parTraverse` fails the suite rather than merely making
  * production slower and a broker angrier.
  */
final class AdminBatchSuite extends KuiIOSuite {

  private val operation = "describeConfigs"

  test("chunkSizesComeFromTuning") {
    val defaults = AdminTuning.default

    assertEquals(AdminBatch.topicChunk(defaults), 200)
    assertEquals(AdminBatch.partitionChunk(defaults), 200)
    assertEquals(AdminBatch.groupChunk(defaults), 50)

    // A second tuning, to prove the numbers are read rather than hard-coded here.
    val tuned = defaults.copy(topicChunkSize = 7, partitionChunkSize = 9, groupChunkSize = 11)

    assertEquals(AdminBatch.topicChunk(tuned), 7)
    assertEquals(AdminBatch.partitionChunk(tuned), 9)
    assertEquals(AdminBatch.groupChunk(tuned), 11)
  }

  test("chunksArePartitionedInTheOrderTheyArrived") {
    assertEquals(AdminBatch.chunks((1 to 450).toList, 200).map(_.size), List(200, 200, 50))
    assertEquals(AdminBatch.chunks(List.empty[Int], 200), Nil)
  }

  test("aFailedChunkDoesNotFailTheBatchWhenTheFailureIsSuppressible") {
    AdminBatch
      .chunked[IO, Int, String](List(1, 2, 3, 4), chunkSize = 2, parallelism = 2, operation) {
        case List(1, 2) => IO.pure(Map(1 -> "a", 2 -> "b"))
        case _ => IO.raiseError(new TopicAuthorizationException("no DESCRIBE"))
      }
      .map { result =>
        assertEquals(result.values.keySet, Set(1, 2))
        assertEquals(result.skipped.keySet, Set(3, 4))
        assertEquals(result.requested, Set(1, 2, 3, 4))
        assert(result.skipped.values.forall {
          case SkipReason.NotAuthorized(_) => true
          case _ => false
        })
      }
  }

  test("aFailedChunkDoesNotFailTheBatchWhenTheFailureIsNotSuppressible") {
    // A timeout is not suppressible, so the keys still come back — but as `Failed`, carrying the
    // code, so that a caller can tell a timed-out batch from a genuinely small one.
    AdminBatch
      .chunked[IO, Int, String](List(1, 2), chunkSize = 1, parallelism = 2, operation) {
        case List(1) => IO.pure(Map(1 -> "a"))
        case _ => IO.raiseError(new TimeoutException("timed out"))
      }
      .map { result =>
        assertEquals(result.values.keySet, Set(1))
        assertEquals(
          result.skipped.get(2),
          Some(SkipReason.Failed(ErrorCode.Timeout, "describeConfigs did not finish"))
        )
        assert(!result.isComplete)
      }
  }

  test("aKeyTheBrokerDidNotMentionIsSkippedWithAReasonRatherThanDropped") {
    // `describeConfigs` on a partly-authorized cluster answers about fewer keys than it was asked
    // about. "The key is missing" without a reason is exactly what `BatchResult` exists to prevent.
    AdminBatch
      .chunked[IO, Int, String](List(1, 2, 3), chunkSize = 3, parallelism = 1, operation)(_ =>
        IO.pure(Map(1 -> "a"))
      )
      .map { result =>
        assertEquals(result.values.keySet, Set(1))
        assertEquals(result.requested, Set(1, 2, 3))
        assert(result.skipped.contains(2) && result.skipped.contains(3))
      }
  }

  test("parallelismIsBounded") {
    val chunkDuration = 1.second

    val program = for {
      running <- Ref.of[IO, Int](0)
      peak <- Ref.of[IO, Int](0)
      _ <- AdminBatch.chunked[IO, Int, String]((1 to 8).toList, 1, parallelism = 4, operation) { chunk =>
        running.updateAndGet(_ + 1).flatMap(now => peak.update(_.max(now))) >>
          IO.sleep(chunkDuration) >>
          running.update(_ - 1).as(chunk.map(k => k -> s"v$k").toMap)
      }
      observed <- peak.get
    } yield observed

    TestControl.executeEmbed(program.timed).map { (elapsed, peak) =>
      assertEquals(peak, 4, "more than four chunks were in flight at once")
      // Eight one-second chunks, four at a time: two waves, and not eight seconds or one.
      assertEquals(elapsed, 2.seconds)
    }
  }

  test("mergeOrderIsDeterministic") {
    // The same batch, once with the chunks finishing in order and once in reverse. A result that
    // depended on scheduling could not be asserted against a golden file or reproduced from a bug
    // report.
    def run(delayOf: Int => FiniteDuration): IO[BatchResult[Int, String]] =
      AdminBatch.chunked[IO, Int, String]((1 to 6).toList, 2, parallelism = 6, operation) { chunk =>
        IO.sleep(delayOf(chunk.head)).as(chunk.map(k => k -> s"v$k").toMap)
      }

    for {
      forward <- TestControl.executeEmbed(run(key => (key * 10).millis))
      backward <- TestControl.executeEmbed(run(key => ((10 - key) * 10).millis))
    } yield {
      assertEquals(forward, backward)
      assertEquals(forward.values.size, 6)
      assert(forward.isComplete)
    }
  }

  test("emptyInputCallsNothing") {
    // Every caller eventually passes an empty list — an unauthorized user, a cluster with no log
    // directories — and a request with an empty argument list can only waste a round trip.
    for {
      chunkedResult <- AdminBatch.chunked[IO, Int, String](Nil, 200, 4, operation)(_ =>
        IO.raiseError(new AssertionError("the broker was called for an empty key list"))
      )
      perBrokerResult <- AdminBatch.perBroker[IO, Int, String](Nil, 4, operation)(_ =>
        IO.raiseError(new AssertionError("the broker was called for an empty key list"))
      )
      perKeyResult <- AdminBatch.perKey[IO, Int, String](Map.empty, 4, operation)
    } yield {
      assertEquals(chunkedResult, BatchResult.empty[Int, String])
      assertEquals(perBrokerResult, BatchResult.empty[Int, String])
      assertEquals(perKeyResult, BatchResult.empty[Int, String])
    }
  }

  test("cancellationCancelsInFlightChunksAndNeverStartsTheRest") {
    // A cancelled refresh must leave the previous snapshot in place, not replace it with half a
    // cluster — so a cancelled batch produces no result at all.
    val program = for {
      started <- Ref.of[IO, Int](0)
      fiber <- AdminBatch
        .chunked[IO, Int, String]((1 to 8).toList, 1, parallelism = 2, operation) { chunk =>
          started.update(_ + 1) >> IO.sleep(1.second).as(chunk.map(k => k -> "v").toMap)
        }
        .start
      _ <- IO.sleep(500.millis)
      _ <- fiber.cancel
      outcome <- fiber.join
      count <- started.get
    } yield (count, outcome.isCanceled)

    TestControl.executeEmbed(program).map { (started, cancelled) =>
      assert(cancelled, "the batch completed instead of being cancelled")
      assertEquals(started, 2, "chunks beyond the parallelism bound were started")
    }
  }

  test("perKeyIsolatesAKeyFailure") {
    val effects: Map[Int, IO[String]] = Map(
      1 -> IO.pure("a"),
      2 -> IO.raiseError(new TopicAuthorizationException("no DESCRIBE")),
      3 -> IO.pure("c")
    )

    AdminBatch.perKey[IO, Int, String](effects, parallelism = 2, operation).map { result =>
      assertEquals(result.values, Map(1 -> "a", 3 -> "c"))
      assertEquals(result.skipped.keySet, Set(2))
      assertEquals(result.requested, Set(1, 2, 3))
    }
  }

  test("perBrokerIsolatesABrokerFailure") {
    AdminBatch
      .perBroker[IO, Int, String](List(1, 2, 3), parallelism = 3, "describeLogDirs") {
        case 2 => IO.raiseError(new org.apache.kafka.common.errors.KafkaStorageException("offline"))
        case broker => IO.pure(s"dirs-$broker")
      }
      .map { result =>
        assertEquals(result.values.keySet, Set(1, 3))
        assertEquals(
          result.skipped.get(2),
          Some(SkipReason.Failed(ErrorCode.InvalidState, "the log directory is offline"))
        )
      }
  }

  test("perBrokerIsBounded") {
    val program = for {
      running <- Ref.of[IO, Int](0)
      peak <- Ref.of[IO, Int](0)
      _ <- AdminBatch.perBroker[IO, Int, String]((1 to 9).toList, 3, "describeLogDirs") { broker =>
        running.updateAndGet(_ + 1).flatMap(now => peak.update(_.max(now))) >>
          IO.sleep(1.second) >>
          running.update(_ - 1).as(s"dirs-$broker")
      }
      observed <- peak.get
    } yield observed

    TestControl.executeEmbed(program.timed).map { (elapsed, peak) =>
      assertEquals(peak, 3)
      assertEquals(elapsed, 3.seconds)
    }
  }

  test("duplicateKeysAreReportedNotDeduplicated") {
    assertEquals(AdminBatch.rejectDuplicates(List(1, 2, 3)), Right(List(1, 2, 3)))
    assert(AdminBatch.rejectDuplicates(List(1, 2, 2, 3)).left.exists(_.contains("2")))
  }

}
