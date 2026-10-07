package kui.message.infrastructure

import scala.concurrent.duration.DurationInt

import kui.kernel.PartitionId
import kui.testkit.KuiIOSuite

final class FakeBrowseConsumerSuite extends KuiIOSuite {
  private val p0 = PartitionId.unsafe(0)
  private val p1 = PartitionId.unsafe(1)
  private val topic = FakeBrowseConsumer.Topic

  test("positions contain only the current assignment after reassignment") {
    for {
      consumer <- FakeBrowseConsumer.of(
        Map(FakeBrowseConsumer.partition(0, 3), FakeBrowseConsumer.partition(1, 3))
      )
      _ <- consumer.assign(topic, List(p0, p1))
      _ <- consumer.seek(topic, p0, 1L)
      _ <- consumer.seek(topic, p1, 2L)
      _ <- consumer.assign(topic, List(p1))
      positions <- consumer.positions
    } yield assertEquals(positions.toOption.get, Map(p1 -> 2L))
  }

  test("seeking into a hole returns the next visible record and advances past it") {
    for {
      consumer <- FakeBrowseConsumer.of(Map(p0 -> Vector(0L, 2L).map(FakeBrowseConsumer.record(0, _))))
      _ <- consumer.assign(topic, List(p0))
      _ <- consumer.seek(topic, p0, 1L)
      before <- consumer.positions
      batch <- consumer.poll(1.milli)
      after <- consumer.positions
      empty <- consumer.poll(1.milli)
      exhausted <- consumer.positions
    } yield {
      assertEquals(before.toOption.get, Map(p0 -> 1L))
      assertEquals(batch.toOption.get.map(_.offset.value), List(2L))
      assertEquals(after.toOption.get, Map(p0 -> 3L))
      assertEquals(empty.toOption.get, Nil)
      assertEquals(exhausted.toOption.get, Map(p0 -> 3L))
    }
  }

  test("batch caps never advance positions over visible undelivered records") {
    for {
      consumer <- FakeBrowseConsumer.of(
        Map(
          p0 -> Vector(0L, 2L, 4L).map(FakeBrowseConsumer.record(0, _)),
          FakeBrowseConsumer.partition(1, 1)
        ),
        maxPollRecords = 2
      )
      _ <- consumer.assign(topic, List(p0, p1))
      _ <- consumer.seek(topic, p0, 0L)
      _ <- consumer.seek(topic, p1, 0L)
      first <- consumer.poll(1.milli)
      positions <- consumer.positions
      second <- consumer.poll(1.milli)
      end <- consumer.positions
    } yield {
      assertEquals(first.toOption.get.map(r => (r.partition, r.offset.value)), List((p0, 0L), (p0, 2L)))
      assertEquals(positions.toOption.get, Map(p0 -> 3L, p1 -> 0L))
      assertEquals(second.toOption.get.map(r => (r.partition, r.offset.value)), List((p0, 4L), (p1, 0L)))
      assertEquals(end.toOption.get, Map(p0 -> 5L, p1 -> 1L))
    }
  }

  test("retained bounds are independent of visible records including wholly invisible partitions") {
    for {
      consumer <- FakeBrowseConsumer.of(
        Map(p0 -> Vector(7L, 9L).map(FakeBrowseConsumer.record(0, _))),
        maxPollRecords = 10,
        retainedBounds = Map(p0 -> (5L, 12L), p1 -> (20L, 25L))
      )
      ids <- consumer.partitions(topic)
      begin <- consumer.beginningOffsets(topic, List(p0, p1))
      end <- consumer.endOffsets(topic, List(p0, p1))
      _ <- consumer.assign(topic, List(p0, p1))
      _ <- consumer.seek(topic, p0, 5L)
      _ <- consumer.seek(topic, p1, 20L)
      batch <- consumer.poll(1.milli)
      positions <- consumer.positions
      empty <- consumer.poll(1.milli)
    } yield {
      assertEquals(ids.toOption.get, List(p0, p1))
      assertEquals(begin.toOption.get, Map(p0 -> 5L, p1 -> 20L))
      assertEquals(end.toOption.get, Map(p0 -> 12L, p1 -> 25L))
      assertEquals(batch.toOption.get.map(_.offset.value), List(7L, 9L))
      assertEquals(positions.toOption.get, end.toOption.get)
      assertEquals(empty.toOption.get, Nil)
    }
  }
}
