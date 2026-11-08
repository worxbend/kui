package kui.config.store

import java.time.Instant

import io.circe.Json
import org.scalacheck.{Gen, Prop}

import kui.testkit.KuiSuite

/** Where the store's design is actually tested.
  *
  * The version rule — a record is applied only when its version is exactly the next one for a key the state
  * already holds, and a key the state has never seen is taken at the version compaction left behind — is what
  * makes concurrent writes from several KUI replicas safe with no lock anywhere. Every replica folds the same
  * ordered partition through this same function, so every replica reaches the same answer about who won a
  * race without any of them talking to each other. The integration suite proves that end to end against a
  * broker; this suite proves it as a property, one layer down, where a counter-example is readable.
  */
final class StoreStateSuite extends KuiSuite {

  private val key = StoreKey(StoreSection.Cluster, "prod-eu")
  private val other = StoreKey(StoreSection.Cluster, "aardvark")
  private val at = Instant.parse("2026-09-03T10:00:00Z")

  private def record(
      k: StoreKey,
      version: Long,
      by: String = "replica-a",
      deleted: Boolean = false
  ): StoreRecord =
    StoreRecord(1, k, version, at, by, deleted, Json.obj("writer" -> Json.fromString(by)))

  private def fold(state: StoreState, entries: List[(StoreRecord, Long)]): StoreState =
    entries.foldLeft(state)((current, entry) => current.apply(entry._1, entry._2)._1)

  test("acceptsTheNextVersion") {
    val (afterFirst, first) = StoreState.empty.apply(record(key, 1L), 0L)
    assertEquals(first, StoreApplied.Accepted(StoreChange.Upserted(record(key, 1L))))
    assertEquals(afterFirst.get(key).map(_.version), Some(1L))
    val (_, second) = afterFirst.apply(record(key, 2L), 1L)
    assert(second.isInstanceOf[StoreApplied.Accepted])
  }

  test("ignoresAStaleVersion") {
    // The lost-race rule. Two replicas both read version 2 and both produced version 3; the one whose
    // record landed second in the partition lost, and every replica reaches that conclusion alone.
    val state = fold(
      StoreState.empty,
      List(record(key, 1L) -> 0L, record(key, 2L) -> 1L, record(key, 3L, "replica-a") -> 2L)
    )
    val (after, applied) = state.apply(record(key, 3L, "replica-b"), 3L)
    assertEquals(applied, StoreApplied.Ignored(key, 3L, 4L))
    assertEquals(after.get(key).flatMap(_.payload.hcursor.get[String]("writer").toOption), Some("replica-a"))
  }

  test("ignoresAFutureVersion") {
    // A gap means records were lost or the log was edited by hand. Accepting it would let a writer skip
    // the conflict check entirely by inventing a large version number.
    val state = fold(StoreState.empty, List(record(key, 1L) -> 0L, record(key, 2L) -> 1L))
    assertEquals(state.apply(record(key, 9L), 2L)._2, StoreApplied.Ignored(key, 9L, 3L))
  }

  test("aKeyFirstSeenPastVersionOneIsAcceptedAtThatVersion") {
    // `__kui_config` is compacted: once the cleaner has run, a key edited twice survives only as its
    // version-3 record. A replica replaying that log must hold the key at version 3, as every replica that
    // watched versions 1 and 2 go by does, or the key vanishes and the replicas stop agreeing on versions.
    val state = fold(StoreState.empty, List(record(key, 3L) -> 10L))
    assertEquals(state.get(key).map(_.version), Some(3L))
    assertEquals(state.nextVersion(key), 4L)
    assert(state.apply(record(key, 4L), 11L)._2.isInstanceOf[StoreApplied.Accepted])
    // The exact-next rule still holds for a key the state already has.
    assertEquals(state.apply(record(key, 3L, "replica-b"), 12L)._2, StoreApplied.Ignored(key, 3L, 4L))
  }

  test("aCompactedTombstoneKeepsTheVersionCounting") {
    val state = fold(StoreState.empty, List(record(key, 4L, deleted = true) -> 0L))
    assertEquals(state.get(key), None)
    assertEquals(state.nextVersion(key), 5L)
  }

  test("aVersionBelowOneIsNeverABaseline") {
    assertEquals(StoreState.empty.apply(record(key, 0L), 0L)._2, StoreApplied.Ignored(key, 0L, 1L))
  }

  property("aCompactedLogFoldsToTheSameStateAsTheWholeLog") {
    // Compaction keeps the newest record of each key, at its original offset. However much of the log the
    // cleaner has reached, a replica replaying what is left must hold every key at the version a replica
    // that read the whole log holds it at. The writes here are the ones KUI makes: each the next version of
    // its key, some of them logical deletes.
    val keys = List(key, other, StoreKey(StoreSection.Settings, "global"))
    val writes: Gen[List[(StoreKey, Boolean)]] =
      Gen.listOf(Gen.zip(Gen.oneOf(keys), Gen.frequency(4 -> false, 1 -> true)))
    val logs: Gen[(List[(StoreRecord, Long)], Int)] = writes.flatMap { ops =>
      val log = ops
        .foldLeft((Map.empty[StoreKey, Long], Vector.empty[(StoreRecord, Long)])) {
          case ((versions, acc), (k, deleted)) =>
            val version = versions.getOrElse(k, 0L) + 1L
            (versions.updated(k, version), acc :+ (record(k, version, deleted = deleted) -> acc.size.toLong))
        }
        ._2
        .toList
      Gen.choose(0, log.size).map(cleaned => (log, cleaned))
    }
    Prop.forAll(logs) { (log, cleaned) =>
      val (head, tail) = log.splitAt(cleaned)
      val survivors = head.filterNot((kept, offset) =>
        log.exists((later, laterOffset) =>
          later.key == kept.key && laterOffset > offset && laterOffset < cleaned
        )
      )
      fold(StoreState.empty, survivors ++ tail).records == fold(StoreState.empty, log).records
    }
  }

  test("aPhysicalTombstoneForgetsAKeyTheStateHolds") {
    // A `null` value is what compaction later reduces to nothing at all, so a replica replaying the log after
    // the cleaner has run never sees the key. Every replica has to reach that same state: forget the key, so
    // that the next create is version 1 everywhere.
    val state = fold(StoreState.empty, List(record(key, 1L) -> 0L, record(key, 2L) -> 1L))
    val (after, applied) = state.erase(key, 2L)
    assertEquals(applied, StoreApplied.Accepted(StoreChange.Deleted(key, 2L, Instant.EPOCH)))
    assertEquals(after.get(key), None)
    assertEquals(after.nextVersion(key), 1L)
    assertEquals(after.lastAppliedOffset, 2L)
    assert(after.apply(record(key, 1L), 3L)._2.isInstanceOf[StoreApplied.Accepted])
  }

  property("twoWritersOnOneKeyConvergeWhicheverOrderIsReplayed") {
    // Every replica reads the same partition, so "any interleaving" means any single log order — and for
    // one fixed order, every replica must reach an identical state. This is the milestone's "both
    // converge on the winner's record" criterion as a property.
    val logs: Gen[List[(StoreRecord, Long)]] =
      Gen.listOfN(8, Gen.oneOf("replica-a", "replica-b")).flatMap { writers =>
        Gen.listOfN(8, Gen.choose(1L, 5L)).map { versions =>
          writers.zip(versions).zipWithIndex.map { case ((writer, version), index) =>
            record(key, version, writer) -> index.toLong
          }
        }
      }
    Prop.forAll(logs) { log =>
      val replicaOne = fold(StoreState.empty, log)
      val replicaTwo = fold(StoreState.empty, log)
      replicaOne == replicaTwo && replicaOne.get(key).forall(_.version == replicaOne.nextVersion(key) - 1L)
    }
  }

  test("tombstoneRemovesTheKeyAndKeepsTheVersion") {
    val state = fold(StoreState.empty, List(record(key, 1L) -> 0L, record(key, 2L, deleted = true) -> 1L))
    assertEquals(state.get(key), None)
    // The version keeps counting. If a re-create restarted at 1 it would collide with the version-1
    // record still sitting in the log, and one of them would be silently ignored.
    assertEquals(state.nextVersion(key), 3L)
    assert(state.apply(record(key, 3L), 2L)._2.isInstanceOf[StoreApplied.Accepted])
  }

  test("listSkipsTombstonesAndSortsByKey") {
    val state = fold(
      StoreState.empty,
      List(record(key, 1L) -> 0L, record(other, 1L) -> 1L, StoreRecord.tombstone(key, 2L, "x", at) -> 2L)
    )
    assertEquals(state.list(StoreSection.Cluster).map(_.key.render), List("cluster/aardvark"))
    assertEquals(state.list(StoreSection.Settings), Nil)
  }

  test("unreadableRecordDoesNotAdvanceTheVersion") {
    // KUI cannot know what version an unreadable record put the key at, and guessing would make the next
    // legitimate write look like a lost race.
    val state = fold(StoreState.empty, List(record(key, 1L) -> 0L))
    val (after, applied) = state.markUnreadable(key, 1L, "the field 'password' could not be decrypted")
    assertEquals(applied, StoreApplied.Unreadable(key, "the field 'password' could not be decrypted"))
    assertEquals(after.nextVersion(key), 2L)
    assertEquals(after.unreadableKeys, List(key))
    // A later readable record for the same key clears the mark.
    assertEquals(after.apply(record(key, 2L), 2L)._1.unreadableKeys, Nil)
  }

  property("lastAppliedOffsetIsMonotonic") {
    Prop.forAll(Gen.listOf(Gen.choose(0L, 1000L))) { offsets =>
      val states =
        offsets.scanLeft(StoreState.empty)((state, offset) => state.apply(record(key, 1L), offset)._1)
      states.map(_.lastAppliedOffset).sliding(2).forall {
        case List(before, after) => after >= before
        case _ => true
      }
    }
  }

  property("applyIsPureAndTotal") {
    val anyRecord: Gen[StoreRecord] = for {
      section <- Gen.oneOf(StoreSection.Cluster, StoreSection.Settings, StoreSection.Other("connect"))
      id <- Gen.oneOf("a", "prod-eu", "x9")
      version <- Gen.choose(-5L, 20L)
      deleted <- Gen.oneOf(true, false)
    } yield StoreRecord(1, StoreKey(section, id), version, at, "gen", deleted, Json.obj())

    Prop.forAll(Gen.listOf(anyRecord)) { records =>
      val finalState = records.zipWithIndex.foldLeft(StoreState.empty) { case (state, (record, index)) =>
        state.apply(record, index.toLong)._1
      }
      // Total: nothing threw, and every stored record is at the version the state agrees it is at.
      finalState.records.forall((k, r) => finalState.nextVersion(k) == r.version + 1L)
    }
  }

  test("outcomeIsRecordedPerOffset") {
    // A writer cannot learn whether it won by comparing the map to what it wrote: by the time it looks, a
    // third writer may have moved the key on again. It asks what the log did with *its own offset*, which
    // is a fact rather than an inference — so the outcome has to be retrievable by offset.
    val state =
      fold(StoreState.empty, List(record(key, 1L, "replica-a") -> 40L, record(key, 1L, "replica-b") -> 41L))
    state.outcomeAt(40L) match {
      case Some(StoreApplied.Accepted(StoreChange.Upserted(applied))) =>
        assertEquals(applied.updatedBy, "replica-a")
      case other => fail(s"offset 40 should have been accepted, got $other")
    }
    assertEquals(state.outcomeAt(41L), Some(StoreApplied.Ignored(key, 1L, 2L)))
    assertEquals(state.outcomeAt(999L), None)
  }

  test("anUnreadableRecordsOutcomeIsAlsoRetrievable") {
    val (state, _) = StoreState.empty.markUnreadable(key, 7L, "it is not valid JSON")
    assertEquals(state.outcomeAt(7L), Some(StoreApplied.Unreadable(key, "it is not valid JSON")))
  }

  test("outcomeRingBufferIsBounded") {
    // Unbounded history would grow for the life of the process, and its only reader is a writer asking
    // about an offset it produced seconds ago.
    val log = (0 until 2000).toList.map(i => record(StoreKey(StoreSection.Cluster, s"c$i"), 1L) -> i.toLong)
    val state = fold(StoreState.empty, log)
    assertEquals(state.outcomes.size, StoreState.OutcomeWindow)
    // The most recent outcome is never the one dropped.
    assert(state.outcomeAt(1999L).isDefined, "the newest outcome must survive")
    assertEquals(state.outcomeAt(0L), None)
  }

  test("theEmptyStateReportsNoLog") {
    assertEquals(StoreState.empty.lastAppliedOffset, -1L)
    assertEquals(StoreState.empty.nextVersion(key), 1L)
    assertEquals(StoreState.empty.get(key), None)
  }
}
