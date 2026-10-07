package kui.message.contract

import java.time.Instant

import io.circe.syntax.*
import io.circe.{Codec, DecodingFailure, HCursor, Json}
import sttp.tapir.Schema

import kui.contracts.ErrorEnvelope.given
import kui.contracts.KernelCodecs.given
import kui.contracts.KernelSchemas.given
import kui.contracts.message.HeaderDto
import kui.kernel.{Offset, PartitionId, TopicName}

/** A record to write (MP-001, MP-002).
  *
  * ==Why key and value are strings and not bytes==
  *
  * Because a person types them. The serde named in `keySerde` / `valueSerde` turns the text into bytes, which
  * is the same journey in reverse that [[kui.contracts.message.DecodedPayloadDto]] describes — and it means
  * the produce form and the message list agree about what a serde is. A base64 field would let a caller write
  * bytes no serde would have produced, which is a way to poison a topic that no UI should offer.
  *
  * @param partition
  *   `None` means "let Kafka choose", which is what a producer without a partition does: hash the key, or
  *   round-robin when there is no key. Naming a partition explicitly is the unusual case and looks unusual
  * @param key
  *   `None` is a record with no key, which is not the same as a record with an empty key. Kafka partitions
  *   the two differently, and compaction treats them differently
  * @param value
  *   `None` is a **tombstone** — the record that tells a compacted topic to forget this key. It is spelled as
  *   an absent value rather than a `tombstone: true` flag because that is exactly what it is on the wire, and
  *   a flag would let a caller send both a value and a tombstone
  * @param count
  *   how many copies to write, for filling a topic while testing. It defaults to one and is capped by the
  *   service; a field rather than a client loop because a hundred round trips to write a hundred identical
  *   records is a hundred chances to fail halfway
  */
final case class ProduceRequestDto(
    partition: Option[PartitionId],
    key: Option[String],
    value: Option[String],
    headers: List[HeaderDto],
    keySerde: Option[String],
    valueSerde: Option[String],
    count: Int
)

object ProduceRequestDto {

  given Codec[ProduceRequestDto] = Codec.from(
    (cursor: HCursor) =>
      for {
        partition <- cursor.get[Option[PartitionId]]("partition")
        key <- cursor.get[Option[String]]("key")
        value <- cursor.get[Option[String]]("value")
        headers <- cursor.getOrElse[List[HeaderDto]]("headers")(Nil)
        keySerde <- cursor.get[Option[String]]("keySerde")
        valueSerde <- cursor.get[Option[String]]("valueSerde")
        count <- cursor.getOrElse[Int]("count")(1)
      } yield ProduceRequestDto(partition, key, value, headers, keySerde, valueSerde, count),
    (dto: ProduceRequestDto) =>
      Json.obj(
        "partition" -> dto.partition.asJson,
        "key" -> dto.key.asJson,
        "value" -> dto.value.asJson,
        "headers" -> dto.headers.asJson,
        "keySerde" -> dto.keySerde.asJson,
        "valueSerde" -> dto.valueSerde.asJson,
        "count" -> dto.count.asJson
      )
  )

  given Schema[ProduceRequestDto] = Schema
    .derived[ProduceRequestDto]
    .description("A record to write; an absent value is a tombstone, not an empty one")

  given CanEqual[ProduceRequestDto, ProduceRequestDto] = CanEqual.derived
}

/** Where one written record landed.
  *
  * The broker's acknowledgement, passed through. It is what lets the UI link straight to the record that was
  * just written, and what lets a caller that wrote a hundred copies know that all hundred are there.
  */
final case class ProducedRecordDto(partition: PartitionId, offset: Offset, timestamp: Instant)

object ProducedRecordDto {

  given Codec[ProducedRecordDto] = Codec.from(
    (cursor: HCursor) =>
      for {
        partition <- cursor.get[PartitionId]("partition")
        offset <- cursor.get[Offset]("offset")
        timestamp <- cursor.get[Instant]("timestamp")
      } yield ProducedRecordDto(partition, offset, timestamp),
    (dto: ProducedRecordDto) =>
      Json.obj(
        "partition" -> dto.partition.asJson,
        "offset" -> dto.offset.asJson,
        "timestamp" -> dto.timestamp.asJson
      )
  )

  given Schema[ProducedRecordDto] = Schema
    .derived[ProducedRecordDto]
    .description("The broker's acknowledgement of one written record")

  given CanEqual[ProducedRecordDto, ProducedRecordDto] = CanEqual.derived
}

/** Every record one produce request wrote.
  *
  * A list and not a count, even when `count` was one, because a caller that wants the count has `.length` and
  * a caller that wants the offsets cannot get them back from a number.
  */
final case class ProduceResultDto(records: List[ProducedRecordDto])

object ProduceResultDto {

  given Codec[ProduceResultDto] = Codec.from(
    (cursor: HCursor) => cursor.get[List[ProducedRecordDto]]("records").map(ProduceResultDto.apply),
    (dto: ProduceResultDto) => Json.obj("records" -> dto.records.asJson)
  )

  given Schema[ProduceResultDto] =
    Schema.derived[ProduceResultDto].description("Where every record this request wrote landed")

  given CanEqual[ProduceResultDto, ProduceResultDto] = CanEqual.derived
}

/** A half-open window `[from, until)` of one partition, on the wire.
  *
  * Half-open, and said so, because every off-by-one in this area is a disagreement about whether the last
  * offset is included. It is not, anywhere: `until` is the offset of the first record **not** in the window,
  * which is the same convention Kafka's own end offsets use.
  */
final case class OffsetRangeDto(partition: PartitionId, from: Offset, until: Offset)

object OffsetRangeDto {

  given Codec[OffsetRangeDto] = Codec.from(
    (cursor: HCursor) =>
      for {
        partition <- cursor.get[PartitionId]("partition")
        from <- cursor.get[Offset]("from")
        until <- cursor.get[Offset]("until")
        // A range that runs backwards is a caller's mistake and there is no sensible reading of it. Rejecting
        // it in the decoder makes it a 400 naming the field, rather than a zero-record resend that looks like
        // success.
        checked <-
          if until.value < from.value then
            Left(DecodingFailure(s"until (${until.value}) is before from (${from.value})", cursor.history))
          else Right(OffsetRangeDto(partition, from, until))
      } yield checked,
    (dto: OffsetRangeDto) =>
      Json.obj(
        "partition" -> dto.partition.asJson,
        "from" -> dto.from.asJson,
        "until" -> dto.until.asJson
      )
  )

  given Schema[OffsetRangeDto] =
    Schema.derived[OffsetRangeDto].description("A half-open offset window [from, until) of one partition")

  given CanEqual[OffsetRangeDto, OffsetRangeDto] = CanEqual.derived
}

/** Copy a range of records into another topic (MP-003).
  *
  * The records are re-written byte for byte, headers included. There is no transform, no re-serialisation and
  * no option to add or drop a header: a resend that changed the bytes would produce a record the original
  * producer never wrote, and the operator replaying a dead-letter queue would have no way to know it.
  *
  * @param toTopic
  *   the destination. It may be the topic being read — replaying a topic into itself is a real operation —
  *   which is why the service, not this document, is what refuses a resend that would never terminate
  * @param ranges
  *   which records, per partition. Empty is rejected by the service rather than treated as "everything": a
  *   resend of a whole topic must be asked for explicitly, one range at a time
  */
final case class ResendRequestDto(toTopic: TopicName, ranges: List[OffsetRangeDto])

object ResendRequestDto {

  given Codec[ResendRequestDto] = Codec.from(
    (cursor: HCursor) =>
      for {
        toTopic <- cursor.get[TopicName]("toTopic")
        ranges <- cursor.get[List[OffsetRangeDto]]("ranges")
      } yield ResendRequestDto(toTopic, ranges),
    (dto: ResendRequestDto) =>
      Json.obj(
        "toTopic" -> dto.toTopic.asJson,
        "ranges" -> dto.ranges.asJson
      )
  )

  given Schema[ResendRequestDto] =
    Schema.derived[ResendRequestDto].description("Copy offset ranges into another topic, byte for byte")

  given CanEqual[ResendRequestDto, ResendRequestDto] = CanEqual.derived
}

/** A failed write identified by its source partition and offset, for selective retry. */
final case class ResendFailureDto(partition: PartitionId, offset: Offset, code: String, error: String)

object ResendFailureDto {
  given Codec[ResendFailureDto] = Codec.from(
    (c: HCursor) =>
      for {
        partition <- c.get[PartitionId]("partition")
        offset <- c.get[Offset]("offset")
        code <- c.get[String]("code")
        error <- c.get[String]("error")
      } yield ResendFailureDto(partition, offset, code, error),
    (v: ResendFailureDto) =>
      Json.obj(
        "partition" -> v.partition.asJson,
        "offset" -> v.offset.asJson,
        "code" -> v.code.asJson,
        "error" -> v.error.asJson
      )
  )
  given Schema[ResendFailureDto] = Schema.derived[ResendFailureDto]
}

final case class ResendRangeFailureDto(range: OffsetRangeDto, code: String, error: String)

object ResendRangeFailureDto {
  given Codec[ResendRangeFailureDto] = Codec.from(
    (c: HCursor) =>
      for {
        range <- c.get[OffsetRangeDto]("range")
        code <- c.get[String]("code")
        error <- c.get[String]("error")
      } yield ResendRangeFailureDto(range, code, error),
    (v: ResendRangeFailureDto) =>
      Json.obj("range" -> v.range.asJson, "code" -> v.code.asJson, "error" -> v.error.asJson)
  )
  given Schema[ResendRangeFailureDto] = Schema.derived[ResendRangeFailureDto]
}

/** Successful counts alongside individual write failures and source ranges that could not be copied. */
final case class ResendResultDto(
    toTopic: TopicName,
    read: Long,
    written: Long,
    failures: List[ResendFailureDto] = Nil,
    rangeFailures: List[ResendRangeFailureDto] = Nil
)

object ResendResultDto {

  given Codec[ResendResultDto] = Codec.from(
    (cursor: HCursor) =>
      for {
        toTopic <- cursor.get[TopicName]("toTopic")
        read <- cursor.get[Long]("read")
        written <- cursor.get[Long]("written")
        failures <- cursor.getOrElse[List[ResendFailureDto]]("failures")(Nil)
        rangeFailures <- cursor.getOrElse[List[ResendRangeFailureDto]]("rangeFailures")(Nil)
      } yield ResendResultDto(toTopic, read, written, failures, rangeFailures),
    (dto: ResendResultDto) =>
      Json.obj(
        "toTopic" -> dto.toTopic.asJson,
        "read" -> dto.read.asJson,
        "written" -> dto.written.asJson,
        "failures" -> dto.failures.asJson,
        "rangeFailures" -> dto.rangeFailures.asJson
      )
  )

  given Schema[ResendResultDto] = Schema
    .derived[ResendResultDto]
    .description("A resend's tally, failed source records and ranges; retry only the reported failures")

  given CanEqual[ResendResultDto, ResendResultDto] = CanEqual.derived
}

/** What a purge deleted, per partition (MS-008).
  *
  * Kafka's `deleteRecords` does not delete records; it moves each partition's *low watermark* forward, and
  * the broker removes whole segments below it when it next gets around to it. `deletedBefore` is that new
  * watermark: every offset below it is gone, and the number is reported per partition because the operation
  * can succeed on some partitions and fail on others.
  */
final case class PurgedPartitionDto(partition: PartitionId, deletedBefore: Offset)

object PurgedPartitionDto {

  given Codec[PurgedPartitionDto] = Codec.from(
    (cursor: HCursor) =>
      for {
        partition <- cursor.get[PartitionId]("partition")
        deletedBefore <- cursor.get[Offset]("deletedBefore")
      } yield PurgedPartitionDto(partition, deletedBefore),
    (dto: PurgedPartitionDto) =>
      Json.obj(
        "partition" -> dto.partition.asJson,
        "deletedBefore" -> dto.deletedBefore.asJson
      )
  )

  given Schema[PurgedPartitionDto] =
    Schema.derived[PurgedPartitionDto].description("One partition's new low watermark after a purge")

  given CanEqual[PurgedPartitionDto, PurgedPartitionDto] = CanEqual.derived
}

/** The result of a purge.
  *
  * `failed` is a list of partitions the broker refused, with its reason, rather than an exception: a purge
  * that worked on seven partitions out of eight has done something, and answering with an error would tell
  * the operator nothing about which seven.
  */
final case class PurgeResultDto(purged: List[PurgedPartitionDto], failed: List[PurgeFailureDto])

object PurgeResultDto {

  given Codec[PurgeResultDto] = Codec.from(
    (cursor: HCursor) =>
      for {
        purged <- cursor.get[List[PurgedPartitionDto]]("purged")
        failed <- cursor.getOrElse[List[PurgeFailureDto]]("failed")(Nil)
      } yield PurgeResultDto(purged, failed),
    (dto: PurgeResultDto) =>
      Json.obj(
        "purged" -> dto.purged.asJson,
        "failed" -> dto.failed.asJson
      )
  )

  given Schema[PurgeResultDto] = Schema
    .derived[PurgeResultDto]
    .description("What a purge deleted, and which partitions refused it")

  given CanEqual[PurgeResultDto, PurgeResultDto] = CanEqual.derived
}

/** One partition the broker would not purge, and what it said. */
final case class PurgeFailureDto(partition: PartitionId, reason: String)

object PurgeFailureDto {

  given Codec[PurgeFailureDto] = Codec.from(
    (cursor: HCursor) =>
      for {
        partition <- cursor.get[PartitionId]("partition")
        reason <- cursor.get[String]("reason")
      } yield PurgeFailureDto(partition, reason),
    (dto: PurgeFailureDto) =>
      Json.obj(
        "partition" -> dto.partition.asJson,
        "reason" -> dto.reason.asJson
      )
  )

  given Schema[PurgeFailureDto] =
    Schema.derived[PurgeFailureDto].description("A partition the broker refused to purge, and why")

  given CanEqual[PurgeFailureDto, PurgeFailureDto] = CanEqual.derived
}
