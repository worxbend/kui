package kui.config.store

/** What happened when one record from the log was folded into the state. */
enum StoreApplied {

  case Accepted(change: StoreChange)

  /** A writer whose base version was stale. Normal, and worth seeing in a log. */
  case Ignored(key: StoreKey, recordVersion: Long, expectedVersion: Long)

  /** A record whose payload could not be decoded or decrypted. That payload is not served; every other key
    * is.
    */
  case Unreadable(key: StoreKey, reason: String)
}

object StoreApplied {
  given CanEqual[StoreApplied, StoreApplied] = CanEqual.derived
}

/** The in-memory projection of `__kui_config`, and the rule that decides what the log means.
  *
  * Pure and free of any platform on purpose. Every interesting decision in the store — which of two racing
  * writers won, whether a record is a lost race, what the next version is — lives here and is testable
  * without a broker. What is left in the Kafka adapter is plumbing.
  */
final case class StoreState(
    records: Map[StoreKey, StoreRecord],
    lastAppliedOffset: Long,
    unreadable: Map[StoreKey, String],
    outcomes: Vector[(Long, StoreApplied)],
    ignoredGaps: Int = 0,
    // Envelope versions whose payload could not be decrypted. Reads skip them; the version still counts.
    concealed: Map[StoreKey, Long] = Map.empty
) {

  /** What the follower did with the record at one offset.
    *
    * A writer needs this and cannot get by with the resulting state. Two replicas both produce version 3 of
    * one key; the partition orders them; one is accepted and one is ignored. The loser has to learn that it
    * lost, and it cannot find out by comparing the map to what it wrote — by the time it looks, a third
    * writer may have moved the key on again. So it asks what happened to *its own offset*, which is a fact
    * about the log and not a guess about the present.
    */
  def outcomeAt(offset: Long): Option[StoreApplied] =
    outcomes.collectFirst { case (recorded, applied) if recorded == offset => applied }

  /** The live record for a key. `None` for a key that was never written and for one that was deleted: a
    * tombstone is kept in `records` so that versions keep counting, but it is not a value anybody reads.
    */
  def get(key: StoreKey): Option[StoreRecord] = records.get(key).filterNot(_.deleted)

  def list(section: StoreSection): List[StoreRecord] =
    records.toList
      .filter((key, record) => key.section == section && !record.deleted)
      .sortBy((key, _) => key.render)
      .map(_._2)

  /** The version the next accepted write to this key must carry.
    *
    * A concealed envelope counts. Its payload is hidden, but the version in that envelope is the one the log
    * established, and a write that pretends otherwise is a lost race on every replica that could read it.
    */
  def nextVersion(key: StoreKey): Long = versionHeld(key).fold(1L)(_ + 1L)

  /** The version of an envelope whose payload this replica could not decrypt, if that is all it holds. */
  def concealedVersion(key: StoreKey): Option[Long] = concealed.get(key)

  /** The version established for this key: a stored record, or a concealed envelope when the payload is not
    * readable. A tombstone is a stored record, so deletion keeps the version too.
    */
  private def versionHeld(key: StoreKey): Option[Long] =
    records.get(key).map(_.version).orElse(concealed.get(key))

  /** Folds one decoded record in.
    *
    * **The version rule, which is the heart of the design.** For a key this state holds, a record is accepted
    * only when its version is exactly the next one. Anything else is ignored.
    *
    * A *stale* version is a writer that lost a race: two replicas both read version 2, both produced version
    * 3, and the one whose record landed second in the partition lost. A *future* version is a gap, which
    * means records were lost or the log was edited by hand — and accepting it would let a writer skip the
    * conflict check entirely by inventing a large version number.
    *
    * **A key this state has never seen is accepted at whatever version it carries**, from 1 up. The topic is
    * compacted, and compaction keeps only the newest record of each key: a cluster created and edited twice
    * survives as its version-3 record alone. Demanding version 1 there would drop every key edited more than
    * once from a replica that replays after the cleaner has run, and leave it counting versions from 1 while
    * every replica that watched the edits go by counts from 4. Taking the first record seen as the baseline
    * gives the replaying replica the state the others hold, because the record compaction kept is the one
    * they accepted last — unless the key's last record was a writer that lost a race, which compaction keeps
    * in place of the winner because it sits at the later offset.
    *
    * Every replica applies this rule to the same ordered log, so every replica agrees on who won without any
    * of them talking to each other. That is what makes "the partition, not a lock, is the serialization
    * point" (ADR-042 §3) true rather than aspirational — and it is why the version rule, not the pre-write
    * check, is the correctness guarantee.
    */
  def apply(record: StoreRecord, offset: Long): (StoreState, StoreApplied) = {
    val expected = nextVersion(record.key)
    val advanced = copy(lastAppliedOffset = math.max(lastAppliedOffset, offset))
    if !isNext(record.key, record.version) then {
      val ignored = StoreApplied.Ignored(record.key, record.version, expected)
      // A version ahead of the one this replica holds is a gap: records were lost, or the log was edited.
      // A stale version is only a lost race, and the key is still the record that won. A gap is not. Leaving
      // it out of `unreadable` makes the key look absent, and a reader that treats absent as empty revives a
      // configured password or reopens an alert feed while health still says Healthy.
      val ahead = versionHeld(record.key).exists(_ + 1L < record.version)
      val marked =
        if ahead then
          advanced.copy(
            unreadable = advanced.unreadable.updated(
              record.key,
              s"version gap: log has v${record.version}, expected v$expected"
            ),
            ignoredGaps = advanced.ignoredGaps + 1
          )
        else advanced
      (marked.withOutcome(offset, ignored), ignored)
    } else {
      val change =
        if record.deleted then StoreChange.Deleted(record.key, record.version, record.updatedAt)
        else StoreChange.Upserted(record)
      val applied = StoreApplied.Accepted(change)
      (
        advanced
          .copy(
            records = advanced.records.updated(record.key, record),
            unreadable = advanced.unreadable - record.key,
            concealed = advanced.concealed - record.key
          )
          .withOutcome(offset, applied),
        applied
      )
    }
  }

  /** Whether a record at `version` is the next one for its key: exactly one past the record held, or — for a
    * key not held at all, which is what a compacted log looks like — any real version.
    */
  private def isNext(key: StoreKey, version: Long): Boolean =
    versionHeld(key) match {
      case Some(held) => version == held + 1L
      case None => version >= 1L
    }

  /** Folds a physical tombstone: a `null` value, which KUI never writes but an operator or another tool may.
    *
    * The key is forgotten, not deleted at a version, because forgetting it is what compaction does: the
    * cleaner removes every earlier record of the key and, once `delete.retention.ms` has passed, the `null`
    * itself. A replica that replays the log after that finds no trace of the key and starts it again at
    * version 1, so every replica that saw the `null` go by has to reach the same state, or the next create is
    * version 1 on one replica and an ignored record on another.
    */
  def erase(key: StoreKey, offset: Long): (StoreState, StoreApplied) = {
    val applied =
      StoreApplied.Accepted(
        StoreChange.Deleted(key, versionHeld(key).getOrElse(0L), java.time.Instant.EPOCH)
      )
    (
      copy(
        records = records - key,
        concealed = concealed - key,
        lastAppliedOffset = math.max(lastAppliedOffset, offset),
        unreadable = unreadable - key
      ).withOutcome(offset, applied),
      applied
    )
  }

  /** Records that one entry of the log could not be read.
    *
    * When the envelope parsed, `knownVersion` is the version it carried. The payload stays hidden — `get` and
    * `list` must not keep serving the record this one superseded — and the version counts, so the next
    * readable record applies and a write based on the old version conflicts. Guessing a version that was not
    * in the envelope would make the next legitimate write look like a lost race, so a record that did not
    * parse leaves the version where it was.
    *
    * An envelope older than the version already held is a lost race, not a new fact. It does not rewind the
    * key and it does not hide the newer record.
    */
  def markUnreadable(
      key: StoreKey,
      offset: Long,
      reason: String,
      knownVersion: Option[Long] = None
  ): (StoreState, StoreApplied) = {
    val advanced = copy(lastAppliedOffset = math.max(lastAppliedOffset, offset))
    knownVersion match {
      case Some(version) if versionHeld(key).exists(_ >= version) =>
        val ignored = StoreApplied.Ignored(key, version, nextVersion(key))
        (advanced.withOutcome(offset, ignored), ignored)
      case Some(version) =>
        val applied = StoreApplied.Unreadable(key, reason)
        val hidden = advanced.copy(
          records = records - key,
          concealed = concealed.updated(key, version),
          unreadable = unreadable.updated(key, reason)
        )
        (hidden.withOutcome(offset, applied), applied)
      case None =>
        val applied = StoreApplied.Unreadable(key, reason)
        val marked = advanced.copy(unreadable = unreadable.updated(key, reason))
        (marked.withOutcome(offset, applied), applied)
    }
  }

  def unreadableKeys: List[StoreKey] = unreadable.keys.toList.sortBy(_.render)

  /** Appends one outcome, dropping the oldest once the window is full.
    *
    * Bounded because it must be: an unbounded history would grow for the life of the process, and its only
    * reader is a writer asking about an offset it produced seconds ago.
    */
  private def withOutcome(offset: Long, applied: StoreApplied): StoreState = {
    val appended = outcomes :+ (offset -> applied)
    copy(outcomes =
      if appended.size > StoreState.OutcomeWindow then appended.drop(appended.size - StoreState.OutcomeWindow)
      else appended
    )
  }
}

object StoreState {

  /** How many per-offset outcomes are kept for writers to look their own write up in.
    *
    * At one metadata write a second — far more than any real deployment produces — this is seventeen minutes
    * of history, which is orders of magnitude more than the seconds a writer waits. It is a window rather
    * than a map because the alternative grows for ever.
    */
  val OutcomeWindow: Int = 1024

  /** Offset `-1` means "nothing applied yet", which is also what a store with no log reports. */
  val empty: StoreState = StoreState(Map.empty, -1L, Map.empty, Vector.empty)

  given CanEqual[StoreState, StoreState] = CanEqual.derived
}
