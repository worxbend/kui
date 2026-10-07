package kui.ksql.contract.dto

import java.time.Instant

import io.circe.syntax.*
import io.circe.{Codec, HCursor, Json}
import sttp.tapir.Schema as TapirSchema

import kui.contracts.Section

/** One thing ksqlDB named, flattened for the browser.
  *
  * ==Why the wire is flat where the domain is four cases==
  *
  * `kui.ksql.domain.KsqlObject` has a case per kind, each carrying the facts that kind has. The wire does
  * not, and the reason is `SCREENS-V4.md` §3.16: the left pane is **one list** whose rows differ by a glyph.
  * A document with four arrays would make the browser merge and order them, which is a second implementation
  * of a rule this service already applies — and the first time the two orderings disagree, the row an
  * operator clicks is not the row they meant.
  *
  * So every row has a `kind` and a `name`, and the rest is optional **by kind, not by luck**. Which fields
  * belong to which kind is stated here and asserted by `KsqlResponsesSuite`, so "a table with no topic" fails
  * a case rather than reaching a screen:
  *
  *   - `stream` — `topic` always, `format` when the server said one;
  *   - `table` — `topic` always, `format` when the server said one, `windowed` always;
  *   - `query` — `statement` always, `sinks` possibly empty (a transient query writes nowhere);
  *   - `topic` — `partitions` and `replication` always.
  *
  * @param format
  *   the value format the server reported. `null` means the server did not say, which older ksqlDB releases
  *   genuinely do not — it is not `JSON`, and a screen must draw the absence rather than a default that would
  *   tell an operator their Avro stream is JSON.
  */
final case class KsqlObjectDto(
    kind: String,
    name: String,
    topic: Option[String],
    format: Option[String],
    windowed: Option[Boolean],
    sinks: List[String],
    statement: Option[String],
    partitions: Option[Int],
    replication: Option[Int]
)

object KsqlObjectDto {

  given Codec[KsqlObjectDto] = Codec.from(
    (cursor: HCursor) =>
      for {
        kind <- cursor.get[String]("kind")
        name <- cursor.get[String]("name")
        topic <- cursor.getOrElse[Option[String]]("topic")(None)
        format <- cursor.getOrElse[Option[String]]("format")(None)
        windowed <- cursor.getOrElse[Option[Boolean]]("windowed")(None)
        sinks <- cursor.getOrElse[List[String]]("sinks")(Nil)
        statement <- cursor.getOrElse[Option[String]]("statement")(None)
        partitions <- cursor.getOrElse[Option[Int]]("partitions")(None)
        replication <- cursor.getOrElse[Option[Int]]("replication")(None)
      } yield KsqlObjectDto(
        kind,
        name,
        topic,
        format,
        windowed,
        sinks,
        statement,
        partitions,
        replication
      ),
    (item: KsqlObjectDto) =>
      Json.obj(
        "kind" -> item.kind.asJson,
        "name" -> item.name.asJson,
        "topic" -> item.topic.asJson,
        "format" -> item.format.asJson,
        "windowed" -> item.windowed.asJson,
        "sinks" -> item.sinks.asJson,
        "statement" -> item.statement.asJson,
        "partitions" -> item.partitions.asJson,
        "replication" -> item.replication.asJson
      )
  )

  given TapirSchema[KsqlObjectDto] = TapirSchema
    .derived[KsqlObjectDto]
    .description(
      "One ksqlDB object. `kind` is one of stream, table, query, topic and decides which of the " +
        "remaining fields are present; a null `format` means the server did not report one"
    )

  given CanEqual[KsqlObjectDto, KsqlObjectDto] = CanEqual.derived
}

/** Everything one ksqlDB cluster named.
  *
  * @param unreadable
  *   the rows the server returned and KUI could not describe, in the order it returned them. They are on the
  *   wire rather than dropped because a row missing from a list is indistinguishable from a row that is not
  *   there — `ConnectorsDto.unreadable`'s argument, and the same live case: a `SHOW STREAMS` answer with a
  *   field this build has never seen is a real answer about a real stream.
  * @param truncated
  *   how many objects were dropped to keep the answer bounded, and **zero is a measured zero here**: it is
  *   the count of what was cut, not an unknown. A ksqlDB cluster naming more than `KsqlObjects.MaxObjects` of
  *   them is the case this exists for, and a screen that said nothing would leave an operator concluding a
  *   stream does not exist.
  */
final case class KsqlObjectsDto(items: List[KsqlObjectDto], unreadable: List[String], truncated: Int)

object KsqlObjectsDto {

  given Codec[KsqlObjectsDto] = Codec.from(
    (cursor: HCursor) =>
      for {
        items <- cursor.getOrElse[List[KsqlObjectDto]]("items")(Nil)
        unreadable <- cursor.getOrElse[List[String]]("unreadable")(Nil)
        truncated <- cursor.getOrElse[Int]("truncated")(0)
      } yield KsqlObjectsDto(items, unreadable, truncated),
    (objects: KsqlObjectsDto) =>
      Json.obj(
        "items" -> objects.items.asJson,
        "unreadable" -> objects.unreadable.asJson,
        "truncated" -> objects.truncated.asJson
      )
  )

  given TapirSchema[KsqlObjectsDto] = TapirSchema
    .derived[KsqlObjectsDto]
    .description(
      "The streams, tables, queries and topics of one ksqlDB cluster, ordered by kind and then name. " +
        "`unreadable` names the rows the server returned and KUI could not describe; `truncated` counts " +
        "the objects dropped to keep the answer bounded"
    )

  given CanEqual[KsqlObjectsDto, KsqlObjectsDto] = CanEqual.derived
}

/** The object listing endpoint's whole answer.
  *
  * The section is `not_configured` for a cluster with no `kui.clusters.<n>.ksql.url` — answered with a
  * **200**, because a deployment that never intended to run ksqlDB is not a broken one, and ADR-032's rule is
  * that the browser hides the row rather than drawing a red panel nobody can clear.
  */
final case class KsqlObjectsResponse(objects: Section[KsqlObjectsDto])

object KsqlObjectsResponse {

  given Codec[KsqlObjectsResponse] = Codec.from(
    (cursor: HCursor) => cursor.get[Section[KsqlObjectsDto]]("objects").map(KsqlObjectsResponse.apply),
    (response: KsqlObjectsResponse) => Json.obj("objects" -> response.objects.asJson)
  )

  given TapirSchema[KsqlObjectsResponse] = TapirSchema
    .derived[KsqlObjectsResponse]
    .description(
      "What this cluster's ksqlDB named. `not_configured` with a 200 when this cluster has no ksqlDB"
    )

  given CanEqual[KsqlObjectsResponse, KsqlObjectsResponse] = CanEqual.derived
}

/** What a caller sends to run or to plan a statement.
  *
  * ==Why the statement travels on the apply call, where ADR-045 says only a token should==
  *
  * ADR-045's apply endpoints take a token and nothing else, so that the change applied cannot differ from the
  * change that was shown. A statement editor cannot work that way — the statement *is* the request, and there
  * is no second phase for the ordinary `CREATE STREAM` — so this DTO carries both, and the protection is
  * moved rather than dropped: the token is minted over the **canonical statement text**, and
  * `KsqlPlanToken.verify` refuses a token whose statement is not character-for-character the one being
  * applied. Substituting a different statement therefore fails, which is the property the two-phase flow
  * exists for. ADR-055 §7 records the deviation and this reason.
  *
  * @param token
  *   the confirmation a destructive statement needs, from `…/ksql/statements/plan`. `null` for every
  *   statement that is not destructive, and a non-null token on one of those is ignored rather than refused:
  *   a client that always sends the token it last received is doing nothing wrong.
  */
final case class StatementRequestDto(statement: String, token: Option[String])

object StatementRequestDto {

  given Codec[StatementRequestDto] = Codec.from(
    (cursor: HCursor) =>
      for {
        statement <- cursor.get[String]("statement")
        token <- cursor.getOrElse[Option[String]]("token")(None)
      } yield StatementRequestDto(statement, token),
    (request: StatementRequestDto) =>
      Json.obj("statement" -> request.statement.asJson, "token" -> request.token.asJson)
  )

  given TapirSchema[StatementRequestDto] = TapirSchema
    .derived[StatementRequestDto]
    .description(
      "One ksqlDB statement, and the plan token confirming it when it is destructive. One statement " +
        "per request: this service classifies what it is given in order to decide whether it needs a " +
        "confirmation, and a batch has no single classification"
    )

  given CanEqual[StatementRequestDto, StatementRequestDto] = CanEqual.derived
}

/** What a statement that finished produced.
  *
  * ==The two answers, and why they share one document==
  *
  * ksqlDB answers a pull query with **rows** and a DDL or DML statement with a **status**. Both are on this
  * type, discriminated by [[outcome]], because a client's next action is the same in both cases — draw what
  * came back — and two documents would mean two decoders and two renderings of the same success.
  *
  * What is *not* shared is emptiness. `outcome = "rows"` with an empty `rows` list is a query that ran and
  * matched nothing, which is a measured emptiness a screen should say out loud; a statement that returns no
  * rows at all is `outcome = "status"` and has no `rows` field to misread. ADR-055 §4.
  *
  * @param shape
  *   what the statement was: `pull_query` or `statement`. Never `push_query` — a push query cannot answer
  *   here, and the endpoint refuses it with a sentence naming the stream address instead.
  * @param rows
  *   every row, each a list of column values in [[columns]]' order. Rendered text and not typed JSON because
  *   a ksqlDB row's schema is whatever the statement selected, and a browser drawing a table draws strings;
  *   ADR-055 §4 records what it would cost to send the types as well. A cell is `null` when the value is a
  *   **SQL NULL**, which is not the same fact as the four characters `null` and must not be drawn as them.
  * @param message
  *   the server's own sentence for a status answer — *"Stream created and running"* — verbatim, never one KUI
  *   composed. `null` on a rows answer.
  */
final case class StatementResultDto(
    statement: String,
    shape: String,
    outcome: String,
    columns: List[String],
    rows: List[List[Option[String]]],
    message: Option[String],
    entity: Option[String],
    executedAt: Instant
)

object StatementResultDto {

  given Codec[StatementResultDto] = Codec.from(
    (cursor: HCursor) =>
      for {
        statement <- cursor.get[String]("statement")
        shape <- cursor.get[String]("shape")
        outcome <- cursor.get[String]("outcome")
        columns <- cursor.getOrElse[List[String]]("columns")(Nil)
        rows <- cursor.getOrElse[List[List[Option[String]]]]("rows")(Nil)
        message <- cursor.getOrElse[Option[String]]("message")(None)
        entity <- cursor.getOrElse[Option[String]]("entity")(None)
        executedAt <- cursor.get[Instant]("executedAt")
      } yield StatementResultDto(statement, shape, outcome, columns, rows, message, entity, executedAt),
    (result: StatementResultDto) =>
      Json.obj(
        "statement" -> result.statement.asJson,
        "shape" -> result.shape.asJson,
        "outcome" -> result.outcome.asJson,
        "columns" -> result.columns.asJson,
        "rows" -> result.rows.asJson,
        "message" -> result.message.asJson,
        "entity" -> result.entity.asJson,
        "executedAt" -> result.executedAt.asJson
      )
  )

  given TapirSchema[StatementResultDto] = TapirSchema
    .derived[StatementResultDto]
    .description(
      "The statement result. `outcome` is 'rows' for a completed pull query, 'status' for a completed " +
        "statement, or 'pending' for a command accepted but not yet completed. `message` is the server's " +
        "own sentence and `entity` carries the command ID when supplied. Do not retry a pending command."
    )

  given CanEqual[StatementResultDto, StatementResultDto] = CanEqual.derived
}

/** What running this statement would do, and the token that confirms it (ADR-045).
  *
  * Every statement can be planned, not only the destructive ones, and that is deliberate: a screen that only
  * knew how to plan the dangerous ones would have to decide which those are, which is the decision this
  * service exists to make. A harmless statement plans to `destructive = false`, no warnings and **no token**,
  * and the editor can go straight on to running it.
  *
  * @param deletesTopic
  *   whether this statement deletes the Kafka topic behind the object it drops. It is the one thing ksqlDB's
  *   language can do that destroys records, and it is the whole reason this endpoint exists.
  * @param warnings
  *   sentences for the person about to confirm, in the order they should be read. Text rather than codes:
  *   they are read once, by a human, in a modal, and a code would need a lookup table in the browser that
  *   nothing keeps in step with this list.
  * @param token
  *   `null` when nothing needs confirming. A token is valid for `KsqlPlanToken.Ttl` and for exactly this
  *   cluster and this statement text.
  */
final case class StatementPlanDto(
    statement: String,
    shape: String,
    destructive: Boolean,
    deletesTopic: Boolean,
    warnings: List[String],
    token: Option[String],
    expiresAt: Option[Instant],
    computedAt: Instant
)

object StatementPlanDto {

  given Codec[StatementPlanDto] = Codec.from(
    (cursor: HCursor) =>
      for {
        statement <- cursor.get[String]("statement")
        shape <- cursor.get[String]("shape")
        destructive <- cursor.get[Boolean]("destructive")
        deletesTopic <- cursor.get[Boolean]("deletesTopic")
        warnings <- cursor.getOrElse[List[String]]("warnings")(Nil)
        token <- cursor.getOrElse[Option[String]]("token")(None)
        expiresAt <- cursor.getOrElse[Option[Instant]]("expiresAt")(None)
        computedAt <- cursor.get[Instant]("computedAt")
      } yield StatementPlanDto(
        statement,
        shape,
        destructive,
        deletesTopic,
        warnings,
        token,
        expiresAt,
        computedAt
      ),
    (plan: StatementPlanDto) =>
      Json.obj(
        "statement" -> plan.statement.asJson,
        "shape" -> plan.shape.asJson,
        "destructive" -> plan.destructive.asJson,
        "deletesTopic" -> plan.deletesTopic.asJson,
        "warnings" -> plan.warnings.asJson,
        "token" -> plan.token.asJson,
        "expiresAt" -> plan.expiresAt.asJson,
        "computedAt" -> plan.computedAt.asJson
      )
  )

  given TapirSchema[StatementPlanDto] = TapirSchema
    .derived[StatementPlanDto]
    .description(
      "What running this statement would do. A destructive statement carries a token valid for five " +
        "minutes and for this statement's exact text; a harmless one carries none and needs none"
    )

  given CanEqual[StatementPlanDto, StatementPlanDto] = CanEqual.derived
}

/** The push query's first frame: the columns, once.
  *
  * Sent under `SseEventName.Phase`, which is the shared name every KUI stream uses for "here is what is
  * coming". It arrives as soon as the server accepts the query rather than when the first record does, which
  * is what makes a push query over an idle topic a stream that has visibly started rather than an open socket
  * that has said nothing (house rule 16, ADR-055 §5).
  */
final case class QueryHeaderDto(columns: List[String])

object QueryHeaderDto {

  given Codec[QueryHeaderDto] = Codec.from(
    (cursor: HCursor) => cursor.getOrElse[List[String]]("columns")(Nil).map(QueryHeaderDto.apply),
    (header: QueryHeaderDto) => Json.obj("columns" -> header.columns.asJson)
  )

  given TapirSchema[QueryHeaderDto] =
    TapirSchema.derived[QueryHeaderDto].description("A push query's column names, sent once, first")

  given CanEqual[QueryHeaderDto, QueryHeaderDto] = CanEqual.derived
}

/** One row of a push query.
  *
  * The column names are not repeated: they were sent once in [[QueryHeaderDto]], and a stream that emitted
  * them on every row would send a schema a thousand times to describe a thousand rows.
  *
  * A `null` entry is a SQL NULL. It is `null` rather than an empty string or the word "null" because a cell
  * with no value and a cell whose value is the text `null` are different facts, and drawing them the same way
  * is the class of invented measurement this product exists to refuse.
  */
final case class QueryRowDto(values: List[Option[String]])

object QueryRowDto {

  /** The SSE event name a consumer registers its listener under.
    *
    * It is `SseEventName.Row` and not a literal here, which is the whole point of this line: `SseEventName`
    * had no entry for the alerts wire and that wire mirrors its name by eye. The browser's constant is
    * compared to `ksql-stream-frame.json`'s `event` field, and `GoldenFilesSuite` pins this end of it —
    * ADR-055 §5.
    */
  val EventName: String = kui.contracts.sse.SseEventName.Row

  given Codec[QueryRowDto] = Codec.from(
    (cursor: HCursor) => cursor.getOrElse[List[Option[String]]]("values")(Nil).map(QueryRowDto.apply),
    (row: QueryRowDto) => Json.obj("values" -> row.values.asJson)
  )

  given TapirSchema[QueryRowDto] = TapirSchema
    .derived[QueryRowDto]
    .description(
      "One row of a push query, in the column order the header declared. A null entry is a SQL NULL"
    )

  given CanEqual[QueryRowDto, QueryRowDto] = CanEqual.derived
}
