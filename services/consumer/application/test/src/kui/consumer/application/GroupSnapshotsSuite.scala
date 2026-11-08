package kui.consumer.application

import scala.concurrent.duration.*

import cats.effect.IO

import kui.consumer.domain.GroupListingPage
import kui.consumer.domain.fixtures.GroupFixtures
import kui.kernel.GroupId
import kui.kernel.error.InfrastructureError
import kui.kernel.group.GroupState
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** The refresh pass, and what it does when only half the cluster answers.
  *
  * The rule under test throughout: only the listing is required. A cluster where KUI may list groups but not
  * describe them still has a page to render, and that page says which half is missing rather than showing an
  * error or, worse, an empty list.
  */
final class GroupSnapshotsSuite extends KuiIOSuite {

  private val orders = ConsumerRig.group("orders")
  private val audit = ConsumerRig.group("audit", state = GroupState.Empty, committed = 10L)

  test("a pass lists, describes, and computes lag") {
    for {
      logger <- FakeStructuredLogger[IO]
      port <- ConsumerRig.port(
        ConsumerRig.PortState.Empty.copy(
          listing = Right(ConsumerRig.listingOf(List(orders, audit))),
          described = Right(Map(orders.groupId -> orders, audit.groupId -> audit))
        )
      )
      result <- GroupSnapshots.refreshOne[IO](port, None, logger, ConsumerRig.Cluster)
    } yield result match {
      case Right(snapshot) =>
        assertEquals(snapshot.version, 1L)
        assertEquals(snapshot.summaries.map(_.groupId.value).sorted, Vector("audit", "orders"))
        assertEquals(snapshot.summaries.find(_.groupId == orders.groupId).flatMap(_.totalLag), Some(10L))
      case Left(error) => fail(s"the pass failed: $error")
    }
  }

  test("a failed listing fails the whole pass, because an empty list is a different fact") {
    for {
      logger <- FakeStructuredLogger[IO]
      port <- ConsumerRig.port(
        ConsumerRig.PortState.Empty.copy(listing = Left(InfrastructureError.Unreachable("kafka", "down")))
      )
      result <- GroupSnapshots.refreshOne[IO](port, None, logger, ConsumerRig.Cluster)
    } yield assert(result.isLeft)
  }

  test("a describe KUI is not allowed to make leaves the rows on screen, marked incomplete") {
    for {
      logger <- FakeStructuredLogger[IO]
      port <- ConsumerRig.port(
        ConsumerRig.PortState.Empty.copy(
          listing = Right(ConsumerRig.listingOf(List(orders))),
          described = Left(kui.kernel.error.ApplicationError.Forbidden("no DESCRIBE on groups"))
        )
      )
      result <- GroupSnapshots.refreshOne[IO](port, None, logger, ConsumerRig.Cluster)
    } yield result match {
      case Right(snapshot) =>
        assertEquals(snapshot.summaries.size, 1)
        val row = snapshot.summaries.head
        assert(!row.completeness.membersKnown)
        assert(!row.completeness.committedOffsetsKnown)
      case Left(error) => fail(s"an undescribable cluster lost its group list: $error")
    }
  }

  test("a coordinator that did not answer is carried into the snapshot rather than hidden") {
    for {
      logger <- FakeStructuredLogger[IO]
      port <- ConsumerRig.port(
        ConsumerRig.PortState.Empty.copy(
          listing = Right(GroupListingPage(List(orders.summary), incompleteCoordinators = 2)),
          described = Right(Map(orders.groupId -> orders))
        )
      )
      result <- GroupSnapshots.refreshOne[IO](port, None, logger, ConsumerRig.Cluster)
    } yield assertEquals(result.map(_.incompleteCoordinators), Right(2))
  }

  test("a held partition's first commit does not become a pace") {
    // P1 is assigned and has never committed. Its first commit is the whole log in one interval.
    // The partition set the pace guard compares has to be the partitions that were summed, or that
    // first commit passes the "same set" check and renders as a spike.
    val held = GroupFixtures.group(
      id = "orders",
      state = GroupState.Stable,
      members = List(GroupFixtures.member("m-1", Set(0, 1))),
      partitions = List(
        GroupFixtures.state(0, Some(100L), end = Some(100L), memberId = Some("m-1")),
        GroupFixtures.state(1, None, end = Some(5000000L), memberId = Some("m-1"))
      )
    )
    val firstCommit = GroupFixtures.group(
      id = "orders",
      state = GroupState.Stable,
      members = List(GroupFixtures.member("m-1", Set(0, 1))),
      partitions = List(
        GroupFixtures.state(0, Some(100L), end = Some(100L), memberId = Some("m-1")),
        GroupFixtures.state(1, Some(5000000L), end = Some(5000000L), memberId = Some("m-1"))
      )
    )

    for {
      logger <- FakeStructuredLogger[IO]
      port <- ConsumerRig.port(
        ConsumerRig.PortState.Empty.copy(
          listing = Right(ConsumerRig.listingOf(List(held))),
          described = Right(Map(held.groupId -> held))
        )
      )
      first <- GroupSnapshots.refreshOne[IO](port, None, logger, ConsumerRig.Cluster)
      _ <- IO.sleep(1.second)
      _ <- port.state.update(_.copy(described = Right(Map(firstCommit.groupId -> firstCommit))))
      second <- GroupSnapshots.refreshOne[IO](port, first.toOption, logger, ConsumerRig.Cluster)
    } yield assertEquals(second.map(_.summaries.head.pace), Right(None))
  }

  test("pace is None on the first pass and a rate on the second") {
    val moved = ConsumerRig.group("orders", committed = 190L, end = 200L)

    for {
      logger <- FakeStructuredLogger[IO]
      port <- ConsumerRig.port(
        ConsumerRig.PortState.Empty.copy(
          listing = Right(ConsumerRig.listingOf(List(orders))),
          described = Right(Map(orders.groupId -> orders))
        )
      )
      first <- GroupSnapshots.refreshOne[IO](port, None, logger, ConsumerRig.Cluster)
      _ <- IO.sleep(1.second)
      _ <- port.state.update(_.copy(described = Right(Map(moved.groupId -> moved))))
      second <- GroupSnapshots.refreshOne[IO](port, first.toOption, logger, ConsumerRig.Cluster)
    } yield {
      assertEquals(first.map(_.summaries.head.pace), Right(None))
      assert(second.exists(_.summaries.head.pace.exists(_ > 0.0)), "a second pass reported no pace")
      assertEquals(second.map(_.version), Right(2L))
    }
  }

  test("the cells are released with the resource, so no refresh fiber outlives it") {
    for {
      profiles <- ConsumerRig.profiles()
      port <- ConsumerRig.port(
        ConsumerRig.PortState.Empty.copy(listing = Right(ConsumerRig.listingOf(List(orders))))
      )
      held <- ConsumerRig.snapshots(port, profiles, 50.millis).use { snapshots =>
        snapshots.of(ConsumerRig.Cluster).map(_.isDefined)
      }
      // Nothing to assert about the fiber directly; what matters is that `use` returned, which it
      // cannot do while a supervised refresh loop is still being awaited.
      describesAfter <- port.state.get.map(_.describes)
      _ <- IO.sleep(200.millis)
      describesLater <- port.state.get.map(_.describes)
    } yield {
      assert(held)
      assertEquals(describesLater, describesAfter, clue = "a refresh fiber outlived the resource")
    }
  }

  test("a snapshot answers which groups consume a topic without another call") {
    for {
      logger <- FakeStructuredLogger[IO]
      port <- ConsumerRig.port(
        ConsumerRig.PortState.Empty.copy(
          listing = Right(ConsumerRig.listingOf(List(orders))),
          described = Right(Map(orders.groupId -> orders))
        )
      )
      result <- GroupSnapshots.refreshOne[IO](port, None, logger, ConsumerRig.Cluster)
    } yield {
      assertEquals(result.map(_.groupsOf(GroupFixtures.Orders).map(_.groupId)), Right(List(orders.groupId)))
      assertEquals(result.map(_.groupsOf(kui.kernel.TopicName.unsafe("other"))), Right(List.empty))
      assertEquals(
        result.map(_.index.search("ord", kui.kernel.search.SearchMode.Plain)),
        Right(List("orders"))
      )
    }
  }

  test("an unknown group id is not in the snapshot rather than being invented") {
    for {
      logger <- FakeStructuredLogger[IO]
      port <- ConsumerRig.port(
        ConsumerRig.PortState.Empty.copy(listing = Right(ConsumerRig.listingOf(List(orders))))
      )
      result <- GroupSnapshots.refreshOne[IO](port, None, logger, ConsumerRig.Cluster)
    } yield assertEquals(result.map(_.groups.get(GroupId.unsafe("ghost"))), Right(None))
  }
}
