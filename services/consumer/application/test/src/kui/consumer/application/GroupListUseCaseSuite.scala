package kui.consumer.application

import munit.FunSuite

import kui.consumer.domain.GroupSummary
import kui.kernel.group.GroupState
import kui.kernel.search.NameIndex

/** Filter, search, sort, page — the order, and the two orderings that are lies if they are got wrong. */
final class GroupListUseCaseSuite extends FunSuite {

  private def row(
      id: String,
      state: GroupState = GroupState.Stable,
      lag: Option[Long] = Some(10L),
      members: Int,
      topics: Int = 1
  ): GroupSummary =
    ConsumerRig
      .group(id, state = state, members = math.max(1, members))
      .summary
      .copy(totalLag = lag, memberCount = members, topicCount = topics)

  private val rows: Vector[GroupSummary] = Vector(
    row("orders", GroupState.Stable, Some(30L), members = 3),
    row("audit", GroupState.Empty, None, members = 0),
    row("billing", GroupState.PreparingRebalance, Some(5L), members = 2),
    row("shipping", GroupState.Dead, Some(100L), members = 0)
  )

  private val index: NameIndex = NameIndex.of(rows.map(_.groupId.value).toList)

  private def ids(query: GroupQuery): List[String] =
    GroupListUseCase.applyQuery(rows, query, index).items.map(_.groupId.value)

  test("no filter and no search returns every row, ordered by id") {
    assertEquals(ids(GroupQuery.Default), List("audit", "billing", "orders", "shipping"))
  }

  test("the state filter narrows the rows") {
    assertEquals(ids(GroupQuery.Default.copy(states = Set(GroupState.Empty))), List("audit"))
  }

  test("search is a substring match over the group id") {
    assertEquals(ids(GroupQuery.Default.copy(search = Some("ill"))), List("billing"))
    assertEquals(ids(GroupQuery.Default.copy(search = Some("ORDERS"))), List("orders"))
  }

  test("a hidden group is excluded before paging, so it cannot occupy the page or the total") {
    // Alphabetical order puts `audit` first. Filtering after `Page.of` would hand back that row and a
    // total of four; the visible group is `orders` and the total is one.
    val page = GroupListUseCase.applyQuery(
      rows,
      GroupQuery.Default.copy(pageSize = 1),
      index,
      visible = id => id.value.startsWith("order")
    )

    assertEquals(page.totalItems, Some(1L))
    assertEquals(page.items.map(_.groupId.value), List("orders"))
  }

  test("a total counts what is left after filtering, not before it") {
    val page =
      GroupListUseCase.applyQuery(rows, GroupQuery.Default.copy(states = Set(GroupState.Empty)), index)
    assertEquals(page.totalItems, Some(1L))
  }

  test("a group with no computable lag sorts last in both directions") {
    val ascending = ids(GroupQuery.Default.copy(sort = GroupSortField.Lag))
    val descending = ids(GroupQuery.Default.copy(sort = GroupSortField.Lag, descending = true))

    assertEquals(ascending, List("billing", "orders", "shipping", "audit"))
    assertEquals(descending, List("shipping", "orders", "billing", "audit"))
  }

  test("state sorts by what an operator is looking for, not by the alphabet") {
    assertEquals(
      ids(GroupQuery.Default.copy(sort = GroupSortField.State)),
      List("billing", "orders", "audit", "shipping")
    )
  }

  test("ties break on the group id in both directions, so paging is deterministic") {
    val tied = Vector(row("b", members = 1), row("a", members = 1), row("c", members = 1))
    val ascending = GroupListUseCase.applyQuery(
      tied,
      GroupQuery.Default.copy(sort = GroupSortField.Members),
      NameIndex.of(Nil)
    )
    val descending = GroupListUseCase.applyQuery(
      tied,
      GroupQuery.Default.copy(sort = GroupSortField.Members, descending = true),
      NameIndex.of(Nil)
    )

    assertEquals(ascending.items.map(_.groupId.value), List("a", "b", "c"))
    assertEquals(descending.items.map(_.groupId.value), List("a", "b", "c"))
  }

  test("paging cuts the sorted list and reports the full total") {
    val page = GroupListUseCase.applyQuery(rows, GroupQuery.Default.copy(page = 2, pageSize = 2), index)

    assertEquals(page.items.map(_.groupId.value), List("orders", "shipping"))
    assertEquals(page.totalItems, Some(4L))
  }

  test("a page past the end is empty and still reports the total") {
    val page = GroupListUseCase.applyQuery(rows, GroupQuery.Default.copy(page = 9, pageSize = 2), index)

    assertEquals(page.items, Nil)
    assertEquals(page.totalItems, Some(4L))
  }

  test("an absurd page size is clamped and reported rather than refused") {
    val (normalised, notes) = GroupQuery.normalise(GroupQuery.Default.copy(pageSize = 5000, page = 0))

    assertEquals(normalised.pageSize, GroupQuery.MaxPageSize)
    assertEquals(normalised.page, 1)
    assertEquals(notes.size, 2)
  }

  test("a blank search is no search at all") {
    val (normalised, _) = GroupQuery.normalise(GroupQuery.Default.copy(search = Some("   ")))
    assertEquals(normalised.search, None)
  }
}
