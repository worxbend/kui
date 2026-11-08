package kui.topic.domain

import java.time.Instant

import kui.kernel.TopicName
import kui.kernel.search.NameIndex

/** Every topic of one cluster, at one instant, with the index the list searches.
  *
  * ==Why the index is part of the value==
  *
  * A search index built per request over ten thousand names would be rebuilt for every keystroke of every
  * user. Built here, it is built once per scrape and thrown away with the snapshot it belongs to, which is
  * the whole of ADR-038's "in memory first" position. Making it a field rather than something the list
  * pipeline constructs also removes the way that design goes wrong: an index built from a different list from
  * the rows would make search quietly return topics the list cannot show, or hide topics it can. The
  * constructor is private and [[TopicSnapshot.of]] builds both halves from one input, so they cannot drift.
  *
  * @param incomplete
  *   topics the scrape listed and could not describe, with the reason, so the list can say "9 998 of 10 000
  *   topics; 2 could not be read" instead of quietly showing fewer. It is disjoint from `topics`: the adapter
  *   keys it by `listings.keySet.diff(described.keySet)`, so a topic here has no row at all, which is exactly
  *   why it needs naming. [[TopicSnapshot.names]] and `TopicStatistics.topicCount` both rest on that
  *   disjointness — they concatenate the two halves rather than merging them, and a topic counted twice would
  *   inflate the cluster total the topics screen prints above its table.
  */
final case class TopicSnapshot private (
    topics: Vector[TopicSummary],
    index: NameIndex,
    scrapedAt: Instant,
    incomplete: Map[TopicName, String]
) {

  /** The rows by name. A `lazy val` because the detail path looks a topic up by name on every request and the
    * list path never does, so neither one should pay for the other.
    */
  lazy val byName: Map[TopicName, TopicSummary] = topics.map(row => row.name -> row).toMap

  def get(name: TopicName): Option[TopicSummary] = byName.get(name)

  def size: Int = topics.size

  /** How many topics the scrape could not fully read. Rendered beside the total, never subtracted from it. */
  def incompleteCount: Int = incomplete.size

  /** Every topic name this scrape learned of, sorted, including the ones it could not describe.
    *
    * The names index is deliberately *more* complete than the list: a topic KUI may see and may not describe
    * has no row, but it exists, and a drawer tree built from names that quietly omitted it would tell an
    * operator the topic is gone. `incomplete` is keyed by names that were listed and not described, so the
    * two halves are disjoint by construction and this is a concatenation rather than a merge.
    */
  lazy val names: Vector[TopicName] = (topics.map(_.name) ++ incomplete.keys).sortBy(_.value)

  /** The topics `visible` allows, described rows and incomplete names alike.
    *
    * The search index stays the one the scrape built. The list intersects search hits with the rows that
    * remain, so a hidden name in the index cannot come back. Rebuilding through [[TopicSnapshot.of]] would
    * difference produce rates against this snapshot as if it were the previous scrape and wipe them.
    */
  def retaining(visible: TopicName => Boolean): TopicSnapshot =
    copy(
      topics = topics.filter(row => visible(row.name)),
      incomplete = incomplete.filter((name, _) => visible(name))
    )
}

object TopicSnapshot {

  /** Builds a snapshot, and the index over exactly the names it holds.
    *
    * `topics` is a `Vector` and not a `List` because the list pipeline slices it by index for every page of
    * every request over a ten-thousand-element collection, and a `List` makes that a walk from the head each
    * time for no reason at all.
    *
    * The order given here is the order the index resolves ties in, so a caller that hands over an already
    * sorted vector gets deterministic search results and a stable later sort over them.
    */
  def of(
      topics: Vector[TopicSummary],
      scrapedAt: Instant,
      incomplete: Map[TopicName, String] = Map.empty,
      previous: Option[TopicSnapshot] = None
  ): TopicSnapshot =
    new TopicSnapshot(
      topics = withRates(topics, scrapedAt, previous),
      index = NameIndex.of(topics.map(_.name.value).toList),
      scrapedAt = scrapedAt,
      incomplete = incomplete
    )

  /** Fills each row's produce rate by differencing it against the same topic in the previous scrape.
    *
    * This is the only place two consecutive scrapes are both in scope, which is why the rate is written here
    * rather than by whatever built the rows. The adapter that reads a broker sees one cluster at one instant;
    * `TopicSummary.of` sees one topic's partitions. Neither can subtract, and a rate computed anywhere else
    * would need the previous scrape threaded through both of them.
    *
    * A topic the previous scrape did not hold — created since, or unreadable then — has no sample to subtract
    * from and keeps its `None`, which is [[ProduceRate.of]]'s first refusal reached by the ordinary route
    * rather than by a special case here.
    */
  private def withRates(
      topics: Vector[TopicSummary],
      at: Instant,
      previous: Option[TopicSnapshot]
  ): Vector[TopicSummary] =
    previous match {
      case None => topics
      case Some(before) =>
        val takenAt = before.scrapedAt
        topics.map(row =>
          row.copy(produceRate = ProduceRate.of(before.get(row.name).map(_.sample(takenAt)), row.sample(at)))
        )
    }

  /** An empty snapshot, for a cluster whose first scrape has not produced anything yet. */
  def empty(scrapedAt: Instant): TopicSnapshot = of(Vector.empty, scrapedAt)
}
