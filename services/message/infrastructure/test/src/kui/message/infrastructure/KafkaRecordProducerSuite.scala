package kui.message.infrastructure

import cats.effect.IO
import cats.effect.kernel.Ref

import kui.testkit.KuiIOSuite

final class KafkaRecordProducerSuite extends KuiIOSuite {
  test("a thrown send constructor and a failed acknowledgement keep every outcome") {
    KafkaRecordProducer
      .dispatch[IO, Int, Int](List(1, 2, 3, 4)) { number =>
        if number == 2 then throw new IllegalArgumentException("send construction failed")
        else if number == 3 then IO.pure(IO.raiseError(new IllegalStateException("acknowledgement failed")))
        else IO.pure(IO.pure(number))
      }
      .map(outcomes => assertEquals(outcomes.map(_.toOption), List(Some(1), None, None, Some(4))))
  }

  test("a dispatch failure retains earlier acknowledgements and later outcomes") {
    for {
      acknowledged <- Ref.of[IO, List[Int]](Nil)
      outcomes <- KafkaRecordProducer.dispatch[IO, Int, Int](List(1, 2, 3)) { number =>
        if number == 2 then IO.raiseError(new IllegalArgumentException("dispatch failed"))
        else IO.pure(acknowledged.update(_ :+ number).as(number))
      }
      seen <- acknowledged.get
    } yield {
      assertEquals(outcomes.map(_.toOption), List(Some(1), None, Some(3)))
      assertEquals(seen, List(1, 3))
    }
  }
}
