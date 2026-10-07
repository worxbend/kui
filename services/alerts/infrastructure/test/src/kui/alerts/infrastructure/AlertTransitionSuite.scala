package kui.alerts.infrastructure

import java.time.Instant

import scala.concurrent.duration.DurationInt

import cats.effect.{IO, Resource}

import kui.alerts.application.AlertStore
import kui.alerts.domain.*
import kui.cache.CacheMetrics
import kui.kernel.{BrokerId, ClusterId, UserName}
import kui.security.{Principal, PrincipalKind}
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

final class AlertTransitionSuite extends KuiIOSuite {
  private val cluster = ClusterId.unsafe("prod")
  private val at = Instant.parse("2026-09-21T12:00:00Z")
  private val reader = Principal(UserName.unsafe("ada"), Set.empty, PrincipalKind.Session)
  private val limits = AlertLimits(1, 1, 5.minutes, 80, 90)
  private def facts(percent: Int): ClusterFacts = ClusterFacts(
    FactReading.Read(PartitionFacts(0, 0, true)),
    FactReading.Read(Set.empty),
    FactReading.Read(List(LogDirectoryFact(BrokerId.unsafe(1), "/disk", Some(percent))))
  )
  private val stores: List[(String, Resource[IO, AlertStore[IO]])] = List(
    "memory" -> InMemoryAlertStore.resource[IO](7.days, CacheMetrics.noop[IO]).map(identity[AlertStore[IO]]),
    "durable" -> Resource.eval(for {
      metadata <- TestConfigStore.create
      logger <- FakeStructuredLogger[IO]
    } yield DurableAlertStore[IO](metadata, 7.days, logger): AlertStore[IO])
  )

  stores.foreach { (name, resource) =>
    test(s"$name: a warning escalates with new details under the same event identity") {
      resource.use { store =>
        val first = AlertRules.evaluate(at, limits, facts(82), Nil, AlertRuleState.empty)
        val next = AlertRules.evaluate(at.plusSeconds(60), limits, facts(95), first.opened, first.state)
        for {
          _ <- store.record(cluster, first, at)
          _ <- store.record(cluster, next, at.plusSeconds(60))
          feed <- store.feed(cluster, reader, 10, None)
        } yield {
          assertEquals(feed.events.map(_.id), first.opened.map(_.id))
          assertEquals(feed.events.head.openedAt, at)
          assertEquals(feed.events.head.lastSeenAt, at.plusSeconds(60))
          assertEquals(feed.events.head.severity, AlertSeverity.Critical)
          assert(feed.events.head.title.contains("95%"))
          assert(feed.events.head.detail.contains("90%"))
        }
      }
    }

    List(10, 95).foreach { percent =>
      test(s"$name: evaluation computed before acknowledgement cannot overwrite it ($percent percent)") {
        resource.use { store =>
          val first = AlertRules.evaluate(at, limits, facts(82), Nil, AlertRuleState.empty)
          val delayed =
            AlertRules.evaluate(at.plusSeconds(60), limits, facts(percent), first.opened, first.state)
          for {
            _ <- store.record(cluster, first, at)
            acknowledged <- store.acknowledge(cluster, first.opened.head.id, at.plusSeconds(30), "ada")
            _ <- store.record(cluster, delayed, at.plusSeconds(60))
            feed <- store.feed(cluster, reader, 10, None)
          } yield assertEquals(feed.events, acknowledged.toOption.toList)
        }
      }
    }
  }
}
