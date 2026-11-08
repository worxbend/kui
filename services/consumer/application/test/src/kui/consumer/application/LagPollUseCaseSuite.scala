package kui.consumer.application

import munit.FunSuite

import kui.kernel.group.GroupState
import kui.kernel.{ClusterId, GroupId}

/** The delta, and the token that makes it safe.
  *
  * The reference product sends the browser's own `lastUpdate` timestamp back to the server. A client's clock
  * is not a version: skew in one direction drops updates and in the other replays them, and neither failure
  * is visible to anyone. Here the token is server-issued and carries the snapshot version it was cut from.
  */
final class LagPollUseCaseSuite extends FunSuite {

  private def snapshot(version: Long, groups: List[(String, Long)]): GroupSnapshot =
    GroupSnapshot(
      version = version,
      summaries =
        groups.map((id, committed) => ConsumerRig.group(id, committed = committed).summary).toVector,
      groups = Map.empty,
      paceSamples = Map.empty,
      incompleteCoordinators = 0,
      takenAt = ConsumerRig.At
    )

  test("with no previous snapshot, everything is sent") {
    val (changed, gone) = LagPollUseCase.diff(None, snapshot(1L, List("a" -> 90L, "b" -> 80L)), Set.empty)

    assertEquals(changed.map(_.groupId.value), List("a", "b"))
    assertEquals(gone, Nil)
  }

  test("two identical passes produce no updates at all") {
    val before = snapshot(1L, List("a" -> 90L))
    val after = snapshot(2L, List("a" -> 90L))

    assertEquals(LagPollUseCase.diff(Some(before), after, Set.empty), (Nil, Nil))
  }

  test("only the group that moved is sent") {
    val before = snapshot(1L, List("a" -> 90L, "b" -> 80L))
    val after = snapshot(2L, List("a" -> 95L, "b" -> 80L))

    val (changed, _) = LagPollUseCase.diff(Some(before), after, Set.empty)
    assertEquals(changed.map(_.groupId.value), List("a"))
  }

  test("a group that is gone is named, so the row is removed rather than frozen") {
    val before = snapshot(1L, List("a" -> 90L, "b" -> 80L))
    val after = snapshot(2L, List("a" -> 90L))

    val (_, gone) = LagPollUseCase.diff(Some(before), after, Set.empty)
    assertEquals(gone.map(_.value), List("b"))
  }

  test("a hidden group is absent from changed and from gone") {
    val before = snapshot(1L, List("order-a" -> 1L, "billing-x" -> 9L, "order-old" -> 2L))
    val after = snapshot(2L, List("order-a" -> 3L, "billing-x" -> 8L))

    val (changed, gone) = LagPollUseCase.diff(
      Some(before),
      after,
      Set.empty,
      visible = id => id.value.startsWith("order")
    )

    assertEquals(changed.map(_.groupId.value), List("order-a"))
    assertEquals(gone.map(_.value), List("order-old"))
  }

  test("the caller's group filter restricts both halves") {
    val before = snapshot(1L, List("a" -> 90L, "b" -> 80L))
    val after = snapshot(2L, List("a" -> 95L))

    val (changed, gone) = LagPollUseCase.diff(Some(before), after, Set(GroupId.unsafe("a")))
    assertEquals(changed.map(_.groupId.value), List("a"))
    assertEquals(gone, Nil)
  }

  test("a state change is a change even when the lag did not move") {
    val before = snapshot(1L, List("a" -> 90L))
    val stopped = before.copy(
      version = 2L,
      summaries = before.summaries.map(_.copy(state = GroupState.Empty, memberCount = 0))
    )

    val (changed, _) = LagPollUseCase.diff(Some(before), stopped, Set.empty)
    assertEquals(changed.map(_.state), List(GroupState.Empty))
  }

  test("a token round-trips its cluster and version, and a foreign one does not parse into ours") {
    val token = LagToken.of(ConsumerRig.Cluster, 7L)

    assertEquals(LagToken.parse(token.value), Some((ConsumerRig.Cluster, 7L)))
    assertEquals(LagToken.parse("not-a-token"), None)
    assertNotEquals(
      LagToken.parse(LagToken.of(ClusterId.unsafe("staging"), 7L).value),
      Some((ConsumerRig.Cluster, 7L))
    )
  }
}
