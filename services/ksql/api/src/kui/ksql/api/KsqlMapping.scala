package kui.ksql.api

import java.time.Instant

import kui.contracts.Section
import kui.kernel.error.KuiError
import kui.ksql.application.{ExecutedStatement, KsqlListing, StatementPlan}
import kui.ksql.contract.dto.*
import kui.ksql.domain.*

/** Application types to wire types, and the one place the two vocabularies are allowed to meet (ADR-033).
  *
  * Rule A3 keeps `Section` out of the application layer, so every decision about *which* section the answer
  * carries is made here, and each of them is a decision about honesty rather than about shape:
  *
  *   - a cluster with no ksqlDB carries `not_configured`, with a 200. ADR-032's rule is that the browser
  *     hides the row rather than drawing a red panel nobody can clear;
  *   - a server that answered carries `ok` with what it said, timestamped with the read;
  *   - a server that did not carries `Section.fromEither`'s fold, which is the same fold every other section
  *     in the product uses, so a ksqlDB that is down, slow, refusing KUI's credentials or behind an open
  *     breaker is described in the vocabulary the browser already renders.
  *
  * There is no `STARTING` special case here, where `services/connect` has one for a rebalancing worker. A
  * ksqlDB that is still building its metastore answers its requests with a 5xx and the ordinary fold is the
  * honest description of that; inventing a "starting" section for it would be KUI claiming to know the
  * difference between a server that is starting and one that is broken, which from the outside it does not.
  */
object KsqlMapping {

  /** The whole object listing. */
  def objects(listing: KsqlListing, at: Instant): KsqlObjectsResponse =
    KsqlObjectsResponse(
      listing match {
        case KsqlListing.NotConfigured => Section.NotConfigured
        case KsqlListing.Answered(Right(found)) => Section.Ok(objectsDto(found), at)
        case KsqlListing.Answered(Left(error)) => section(error, at)
      }
    )

  /** A failure, as the section the browser renders. Named rather than inlined so that a case can assert this
    * function rather than a copy of it.
    */
  def section(error: KuiError, at: Instant): Section[KsqlObjectsDto] = Section.fromEither(Left(error), at)

  def objectsDto(objects: KsqlObjects): KsqlObjectsDto =
    KsqlObjectsDto(
      items = objects.items.map(objectDto),
      unreadable = objects.unreadable,
      truncated = objects.truncated
    )

  /** One object, flattened.
    *
    * The flattening is where the domain's four cases become one row shape, and every optional field below is
    * `None` **because that kind of object does not have that fact**, never because it was not read: a query
    * has no topic, a topic has no format. `KsqlResponsesSuite` asserts the four shapes so that the sentence
    * in `KsqlObjectDto`'s scaladoc is held by a case.
    */
  def objectDto(item: KsqlObject): KsqlObjectDto =
    item match {
      case KsqlObject.Stream(name, topic, format) =>
        empty(item.kind, name).copy(topic = Some(topic), format = format)
      case KsqlObject.Table(name, topic, format, windowed) =>
        empty(item.kind, name).copy(topic = Some(topic), format = format, windowed = Some(windowed))
      case KsqlObject.Query(id, sinks, statement) =>
        empty(item.kind, id).copy(sinks = sinks, statement = Some(statement))
      case KsqlObject.Topic(name, partitions, replication) =>
        empty(item.kind, name).copy(partitions = Some(partitions), replication = Some(replication))
    }

  private def empty(kind: KsqlObjectKind, name: String): KsqlObjectDto =
    KsqlObjectDto(
      kind = kind.wire,
      name = name,
      topic = None,
      format = None,
      windowed = None,
      sinks = Nil,
      statement = None,
      partitions = None,
      replication = None
    )

  /** What a finished statement answers.
    *
    * The discriminator comes off `StatementOutcome` rather than being decided here, so the wire's `outcome`
    * and the domain's two cases cannot come apart — and `columns`/`rows` are empty on a status answer while
    * `message` is absent on a rows answer, which is what makes "no rows" and "this statement returns no rows"
    * two different documents (ADR-055 §4).
    */
  def result(executed: ExecutedStatement): StatementResultDto =
    executed.outcome match {
      case StatementOutcome.Rows(columns, rows) =>
        StatementResultDto(
          statement = executed.statement.canonical,
          shape = executed.statement.shape.wire,
          outcome = executed.outcome.wire,
          columns = columns,
          rows = rows,
          message = None,
          entity = None,
          executedAt = executed.executedAt
        )
      case StatementOutcome.Status(message, entity) =>
        StatementResultDto(
          statement = executed.statement.canonical,
          shape = executed.statement.shape.wire,
          outcome = executed.outcome.wire,
          columns = Nil,
          rows = Nil,
          message = Some(message),
          entity = entity,
          executedAt = executed.executedAt
        )
      case StatementOutcome.Pending(message, entity) =>
        StatementResultDto(
          statement = executed.statement.canonical,
          shape = executed.statement.shape.wire,
          outcome = executed.outcome.wire,
          columns = Nil,
          rows = Nil,
          message = Some(message),
          entity = entity,
          executedAt = executed.executedAt
        )
    }

  def plan(plan: StatementPlan): StatementPlanDto =
    StatementPlanDto(
      statement = plan.statement.canonical,
      shape = plan.statement.shape.wire,
      destructive = plan.statement.destructive,
      // The same flag today, and two fields on purpose. `destructive` is "a confirmation is required" and
      // `deletesTopic` is "records outside ksqlDB are destroyed"; they coincide because `DROP … DELETE
      // TOPIC` is currently the only statement in ksqlDB's language that does the second. A browser that
      // drew one warning from one boolean would draw the wrong warning the day a second destructive
      // statement arrives that deletes nothing. ADR-055 §7.
      deletesTopic = plan.statement.destructive,
      warnings = plan.warnings,
      token = plan.token,
      expiresAt = plan.expiresAt,
      computedAt = plan.computedAt
    )

  def header(columns: List[String]): QueryHeaderDto = QueryHeaderDto(columns)

  def row(row: QueryRow): QueryRowDto = QueryRowDto(row.values)
}
