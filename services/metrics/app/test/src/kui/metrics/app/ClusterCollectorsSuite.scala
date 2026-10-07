package kui.metrics.app

import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all.*
import fs2.Stream

import kui.config.ClusterConfig
import kui.kernel.ClusterId
import kui.kernel.cluster.{AdminTuning, BootstrapServers, ClientProperties, ClusterSecurity}
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

final class ClusterCollectorsSuite extends KuiIOSuite {
  private def config(name: String) = ClusterConfig(
    ClusterId.unsafe(name),
    name,
    BootstrapServers.unsafe("localhost:9092"),
    ClusterSecurity.Plaintext,
    ClientProperties.empty,
    false,
    AdminTuning.default
  )

  test("runtime additions, changes and removals release only their own collectors") {
    for {
      events <- Ref.of[IO, List[String]](Nil)
      build = (c: ClusterConfig) =>
        Resource.make(events.update(_ :+ s"open:${c.name}").as(c.name))(name =>
          events.update(_ :+ s"close:$name")
        )
      _ <- ClusterCollectors.resource[IO, String](List(config("a"), config("b")))(build).use { live =>
        for {
          _ <- live.reconcile(List(config("a").copy(name = "changed"), config("b"), config("c")))
          snapshot <- live.current
          _ = assertEquals(snapshot.values.map(_._2).toSet, Set("changed", "b", "c"))
          _ <- live.reconcile(List(config("b")))
          remaining <- live.current
          _ = assertEquals(remaining.keySet, Set(config("b").id))
          log <- events.get
          _ = assertEquals(log.count(_ == "open:b"), 1)
          _ = assertEquals(log.count(_ == "close:b"), 0)
          _ = assertEquals(log.count(_ == "close:a"), 1)
          _ = assertEquals(log.count(_ == "close:changed"), 1)
          _ = assertEquals(log.count(_ == "close:c"), 1)
        } yield ()
      }
      log <- events.get
    } yield assertEquals(log.count(_ == "close:b"), 1)
  }

  test("cancelled acquisition releases partial resources and never publishes the new profile") {
    for {
      entered <- Deferred[IO, Unit]
      released <- Ref.of[IO, Int](0)
      build = (_: ClusterConfig) =>
        Resource.make(IO.unit)(_ => released.update(_ + 1)) *>
          Resource.eval(entered.complete(()).void *> IO.never[String])
      _ <- ClusterCollectors.resource[IO, String](Nil)(build).use { live =>
        for {
          fiber <- live.reconcile(List(config("a"))).start
          _ <- entered.get
          _ <- fiber.cancel
          snapshot <- live.current
          _ = assertEquals(snapshot.size, 0)
        } yield ()
      }
      count <- released.get
    } yield assertEquals(count, 1)
  }

  test("shutdown cancels the subscription and an in-flight acquisition before draining collectors") {
    for {
      acquiring <- Deferred[IO, Unit]
      subscribed <- Ref.of[IO, Boolean](false)
      closed <- Ref.of[IO, Set[String]](Set.empty)
      logger <- FakeStructuredLogger[IO]
      changes = Stream.bracket(subscribed.set(true))(_ => subscribed.set(false)) >>
        Stream.emit(List(config("a"), config("b"))) ++ Stream.never[IO]
      build = (c: ClusterConfig) =>
        Resource
          .make(IO.pure(c.name))(name => closed.update(_ + name))
          .evalTap(name => if name == "b" then acquiring.complete(()).void *> IO.never else IO.unit)
      _ <- (for {
        live <- ClusterCollectors.resource[IO, String](List(config("a")))(build)
        _ <- live.watch(changes, logger)
      } yield live).use(_ => acquiring.get)
      released <- closed.get
      listening <- subscribed.get
    } yield {
      assertEquals(released, Set("a", "b"))
      assertEquals(listening, false)
    }
  }
}
