package kui.ksql.infrastructure

import scala.concurrent.duration.FiniteDuration

import cats.effect.kernel.Async
import cats.syntax.all.*
import fs2.Stream
import io.circe.{parser, Json}
import sttp.capabilities.fs2.Fs2Streams
import sttp.client4.*
import sttp.model.{StatusCode, Uri}

import kui.config.SafeUrl
import kui.http.upstream.{BoundedResponse, UpstreamClient, UpstreamFailure}
import kui.kernel.error.{ApplicationError, ErrorCode, InfrastructureError, KuiError}
import kui.ksql.application.KsqlClient
import kui.ksql.domain.*

/** ksqlDB's REST API, as KUI's own client.
  *
  * ==Which API this assumes==
  *
  * ksqlDB **0.14 and later**, which is Confluent Platform 6.1 and later, over HTTP/1.1. Two endpoints are
  * used and no others:
  *
  *   - `POST /ksql` — everything that finishes and is not a query: `SHOW`, `CREATE`, `DROP`, `TERMINATE`,
  *     `INSERT`. It answers a JSON array of entities, one per statement sent.
  *   - `POST /query` — the queries. It answers a JSON array whose first element is a `header` carrying the
  *     result schema and whose remaining elements are `row` objects, **chunked as they are produced**. For a
  *     pull query the array completes; for a push query it does not, which is the whole reason this client
  *     reads it as a stream of lines rather than as a document.
  *
  * `/query-stream` — the newer, better endpoint — is deliberately not used: it requires HTTP/2, and the
  * transport this process shares with every other upstream is HTTP/1.1. ADR-055 §2 records that and names
  * what changing it would cost.
  *
  * ==Two roots, and they are not the same, which is a trap this file exists to make visible==
  *
  * [[callRoot]] has the configured **path stripped**, because the requests built from it go through the
  * resilient backend, whose `Failover.rebase` replaces the scheme and authority and *prefixes the base URL's
  * own path*. A request built against the full configured URL therefore has that path applied twice, which
  * for a ksqlDB behind an ingress at `/ksql` produces `/ksql/ksql/ksql` and a 404 that looks exactly like an
  * address that is not a ksqlDB. `RegistryHttp` cost that service a milestone by getting this wrong.
  *
  * [[streamRoot]] keeps the path, because the push query does **not** go through the resilient backend and
  * nothing rebases it. It cannot go through that backend: a `StreamRequest` needs a `StreamBackend`, and — as
  * important — a bulkhead permit held for the length of a push query is a permit held for minutes, which
  * would let three open queries starve every other call to the same server. So a push query has the timeout
  * and the cancellation chain and it does not have the breaker; ADR-055 §5 states that trade with its cost.
  *
  * ==Nothing here throws==
  *
  * Every method answers a `KuiError` on the left, including the stream, whose left is its terminal element. A
  * server that is down, slow, starting up or rejecting KUI's credentials arrives as a value, because the
  * caller has to keep the rest of the product working while this one upstream is broken.
  */
final class KsqlHttp[F[_]: Async](
    calls: Backend[F],
    streaming: StreamBackend[F, Fs2Streams[F]],
    baseUrl: SafeUrl,
    streamTimeout: FiniteDuration,
    credentials: KsqlCredentials[F]
) extends KsqlClient[F] {

  import KsqlHttp.*

  /** The address the pooled, broker-protected calls are built against: **path stripped**. */
  private val callRoot: Uri =
    Uri.parse(baseUrl.value).getOrElse(uri"http://ksqldb.invalid").withWholePath("")

  /** The address the push query is built against: **path kept**, because nothing rebases it. */
  private val streamRoot: Uri = Uri.parse(baseUrl.value).getOrElse(uri"http://ksqldb.invalid")

  def objects: F[Either[KuiError, KsqlObjects]] =
    statement(ListingStatements).map(_.flatMap(entities => Right(objectsFrom(entities))))

  def execute(statement: KsqlStatement): F[Either[KuiError, StatementOutcome]] =
    if statement.push then
      // The port's contract says a push query never reaches here. If one does it is a defect in the
      // caller, and the adapter says so rather than opening an unbounded response and buffering it into
      // a document that will never be complete.
      ApplicationError
        .InvalidState(
          "a push query reached the ksqlDB statement adapter; it is answered by the stream endpoint"
        )
        .asLeft[StatementOutcome]
        .pure[F]
    else if statement.query then pullQuery(statement)
    else this.statement(statement.canonical).map(_.flatMap(commandOutcome))

  /** The push query, as frames.
    *
    * `interruptAfter` is `kui.clusters.<n>.ksql.streamTimeout` and is the reason that key exists: a push
    * query does not finish, so a browser tab somebody left open would otherwise hold a query running on the
    * server for as long as the process lives. The interruption produces a normal end of stream, which the
    * `api` layer turns into ADR-035's `done` with reason `budget` — a different sentence from the `exhausted`
    * that [[QueryFrame.Ended]] carries, because "your five minutes are up" and "the query finished" are
    * different things to tell somebody watching a live topic.
    */
  def rows(statement: KsqlStatement): Stream[F, Either[KuiError, QueryFrame]] =
    Stream
      .eval(
        credentials.authenticateStream(
          basicRequest
            .post(streamRoot.addPath(QueryPath))
            .body(payloadFor(statement.canonical))
            .contentType(MediaType)
            .header("Accept", MediaType)
            .response(asStreamAlwaysUnsafe(Fs2Streams[F]))
        )
      )
      .flatMap {
        case Left(error) => Stream.emit(error.asLeft[QueryFrame])
        case Right(request) =>
          Stream
            .eval(request.send(streaming))
            .flatMap { response =>
              if response.code.isSuccess then
                response.body
                  .through(fs2.text.utf8.decode)
                  .through(fs2.text.lines)
                  .map(frameOf)
                  .unNone
              else
                // A failure status on a stream arrives before any frame, so it is the whole answer.
                // `asStreamAlwaysUnsafe` has already handed the body over as a stream, so it is drained
                // into a string in order to recover ksqlDB's own sentence about what was wrong.
                Stream
                  .eval(response.body.through(fs2.text.utf8.decode).compile.string)
                  .map(body => errorFrom(response.code, body).asLeft[QueryFrame])
            }
            .handleErrorWith(failure => Stream.emit(thrown(failure).asLeft[QueryFrame]))
            .interruptAfter(streamTimeout)
      }

  // -----------------------------------------------------------------------------------------------
  // POST /ksql
  // -----------------------------------------------------------------------------------------------

  /** One or more statements through `/ksql`, answered as the entities ksqlDB rendered. */
  private def statement(text: String): F[Either[KuiError, List[Json]]] =
    send(
      basicRequest
        .post(callRoot.addPath(KsqlPath))
        .body(payloadFor(text))
        .contentType(MediaType)
    ).map(_.flatMap(body => entitiesOf(body)))

  /** A pull query through `/query`, read to the end because a pull query has one.
    *
    * Unlike the push query above, this does not stream — a pull query's array completes, so it is read whole
    * and parsed as a document. "Whole" still has a ceiling: [[KsqlHttp.MaxPullQueryResponseBytes]] bounds how
    * much of ksqlDB's answer this call will hold in memory, the same way a request body is bounded elsewhere
    * (`CompatibilityCheckUseCase.MaxDefinitionBytes`), because a `SELECT * FROM <large topic>` with no
    * `LIMIT` would otherwise let one request buffer an unbounded amount of ksqlDB's JSON into this process's
    * heap — a process every other cluster's schema, connect and cluster calls share.
    */
  private def pullQuery(statement: KsqlStatement): F[Either[KuiError, StatementOutcome]] =
    send(
      basicRequest
        .post(callRoot.addPath(QueryPath))
        .body(payloadFor(statement.canonical))
        .contentType(MediaType),
      maxResponseBytes = Some(KsqlHttp.MaxPullQueryResponseBytes)
    ).map(_.flatMap { body =>
      parser.parse(body).left.map(_ => malformed("its query answer is not JSON")).flatMap { json =>
        json.asArray.map(_.toList).getOrElse(List(json)).flatMap(decodeFrame).sequence.map { frames =>
          val columns = frames.collectFirst { case QueryFrame.Header(names) => names }.getOrElse(Nil)
          val rows = frames.collect { case QueryFrame.Row(row) => row.values }
          StatementOutcome.Rows(columns, rows)
        }
      }
    })

  private def payloadFor(text: String): String =
    Json.obj("ksql" -> Json.fromString(text), "streamsProperties" -> Json.obj()).noSpaces

  /** Authenticate, send, and turn everything that is not a usable response into a typed error.
    *
    * The server's response body is **read** on a failure and reduced to its own `message` field, which is
    * where ksqlDB writes the sentence it wrote for a human — `line 1:8: mismatched input 'FROM'` or
    * `Cannot drop ORDERS. The following queries read from this source: [...]`. ADR-034 forbids echoing an
    * upstream body wholesale; one field it wrote for this purpose is the difference between "ksqlDB said no,
    * and here is why" and a status code.
    */
  private def send(
      request: Request[Either[String, String]],
      maxResponseBytes: Option[Long] = None
  ): F[Either[KuiError, String]] = {
    val responseAs = maxResponseBytes.fold(asStringAlways)(BoundedResponse.asString)

    credentials.authenticate(request.header("Accept", MediaType).response(responseAs)).flatMap {
      case Left(error) => error.asLeft[String].pure[F]
      case Right(authenticated) =>
        authenticated
          .send(calls)
          .map(response =>
            if response.code.isSuccess then Right(response.body)
            else Left(errorFrom(response.code, response.body))
          )
          .recover {
            // The resilient backend carries its typed error inside this one exception rather than losing
            // it in a message. A response past `maxResponseBytes` is reported the same way a malformed one
            // is, because both are "ksqlDB answered something this call cannot use", not an unreachable
            // upstream. Anything else is genuinely unexpected and is reported as an upstream that did not
            // produce a response, which is the honest description.
            case UpstreamFailure(error) => Left(error)
            case BoundedResponse.LimitExceeded(limit) =>
              Left(malformed(s"its answer was larger than the $limit-byte limit KUI enforces on this call"))
            case failure: Exception => Left(thrown(failure))
          }
    }
  }

  private def upstreamName: String = UpstreamName

  private def thrown(failure: Throwable): KuiError =
    failure match {
      case UpstreamFailure(error) => error
      case other =>
        InfrastructureError.Unreachable(
          upstreamName,
          UpstreamClient.safeFailureCause(other)
        )
    }

  /** A failure status, classified.
    *
    * The `400` arm is the one with a rule attached: a ksqlDB `400` is almost always **the operator's own
    * statement being wrong**, and reporting it as an upstream failure would tell them their ksqlDB is broken
    * when what happened is that they typed `SELCT`. It reaches the browser as `KUI-VALIDATION` carrying
    * ksqlDB's own sentence, which is the one thing they can act on.
    *
    * `ErrorCode.UpstreamKsql` — declared in wave 1 and unused until now — is what a ksqlDB that answered
    * something else gets. It is not widened and no new code is added (house rule 3).
    */
  private def errorFrom(status: StatusCode, body: String): KuiError = {
    val detail = parser
      .parse(body)
      .toOption
      .flatMap(json =>
        json.hcursor
          .get[String]("message")
          .toOption
          .orElse(json.hcursor.downField("error").get[String]("message").toOption)
      )
      .map(_.trim)
      .filter(_.nonEmpty)

    if status == StatusCode.BadRequest then
      ApplicationError.Invalid(
        detail.getOrElse("the ksqlDB cluster refused this statement and gave no reason"),
        Nil
      )
    else if status == StatusCode.Unauthorized || status == StatusCode.Forbidden then
      InfrastructureError.AuthFailed(upstreamName)
    else if status == StatusCode.NotFound then
      // A 404 on `/ksql` is not an absence: that path exists on every ksqlDB server there has ever been.
      // It is an address that is not a ksqlDB — most often a proxy or an ingress pointed at the wrong
      // service — and saying so is what an operator can act on.
      InfrastructureError.Remote(
        ErrorCode.UpstreamUnavailable,
        "the address configured for this cluster's ksqlDB answered 404 for POST /ksql, so it does not " +
          "look like a ksqlDB server",
        Nil
      )
    else
      detail match {
        case None => InfrastructureError.Upstream(upstreamName, status.code)
        case Some(message) =>
          InfrastructureError.Remote(
            ErrorCode.UpstreamKsql,
            s"the ksqlDB cluster answered ${status.code}: $message",
            Nil
          )
      }
  }

  private def malformed(why: String): KuiError =
    InfrastructureError.Remote(
      ErrorCode.UpstreamKsql,
      s"the ksqlDB cluster answered something KUI could not understand: $why",
      Nil
    )

  private def entitiesOf(body: String): Either[KuiError, List[Json]] =
    parser
      .parse(body)
      .left
      .map(_ => malformed("it is not JSON"))
      .flatMap(json =>
        json.asArray
          .map(_.toList)
          .toRight(malformed("its answer is not the array of entities the /ksql endpoint returns"))
      )
}

object KsqlHttp {

  /** The name this upstream is known by in errors and metrics. A name, never a URL. */
  val UpstreamName: String = "ksqldb"

  val KsqlPath: String = "ksql"
  val QueryPath: String = "query"

  /** The most bytes a single pull-query answer may occupy before this call gives up on it.
    *
    * A pull query completes and is read whole (see [[KsqlHttp.pullQuery]]), unlike a push query, which
    * streams. Without a ceiling, an operator-written `SELECT` with no `LIMIT` over a large topic would let
    * one request hold an unbounded slice of ksqlDB's JSON in this process's heap — the same process that
    * answers every other cluster's schema, connect and cluster calls.
    */
  val MaxPullQueryResponseBytes: Long = 16L * 1024 * 1024

  /** ksqlDB's own media type, sent and accepted.
    *
    * The versioned one rather than `application/json`, because it is what pins the *response* shape: a server
    * that later changes its default representation answers this one unchanged, and a client that asked for
    * plain JSON would silently start receiving something else.
    */
  val MediaType: String = "application/vnd.ksql.v1+json"

  /** The four listings, in one request.
    *
    * One round trip rather than four, because §3.16's pane is one pane and `ConnectEndpoints`' argument
    * applies: four requests for one screen is how four panels come to disagree. ksqlDB runs them in order and
    * answers one entity per statement, so the order here is the order they are read back in.
    */
  val ListingStatements: String = "SHOW STREAMS; SHOW TABLES; SHOW QUERIES; SHOW TOPICS;"

  /** What a name looks like when the server returned a row and did not name it.
    *
    * A sentence rather than an empty string, because this lands in `unreadable`, which is drawn as a list of
    * names on a screen — and an empty row there says nothing at all.
    */
  def unnamed(kind: String): String = s"(a $kind the ksqlDB cluster did not name)"

  /** Every entity of a listing response, turned into objects.
    *
    * A row KUI cannot describe goes to `unreadable` rather than being dropped, and an entity whose `@type`
    * this build has never seen is **ignored**: it is a listing this client did not ask for, which a future
    * ksqlDB could add to the answer, and counting it as unreadable would put a permanent false row on the
    * screen of every deployment that upgraded.
    */
  def objectsFrom(entities: List[Json]): KsqlObjects = {
    val read = entities.flatMap(entityOf)

    KsqlObjects.of(
      items = read.collect { case Right(item) => item },
      unreadable = read.collect { case Left(name) => name }
    )
  }

  private def entityOf(entity: Json): List[Either[String, KsqlObject]] = {
    val cursor = entity.hcursor
    val kind = cursor.get[String]("@type").toOption.getOrElse("")

    def rows(field: String): List[Json] =
      cursor.downField(field).focus.flatMap(_.asArray).map(_.toList).getOrElse(Nil)

    kind match {
      case "streams" => rows("streams").map(streamOf)
      case "tables" => rows("tables").map(tableOf)
      case "queries" => rows("queries").map(queryOf)
      case "kafka_topics" | "kafka_topics_extended" => rows("topics").map(topicOf)
      case _ => Nil
    }
  }

  private def streamOf(row: Json): Either[String, KsqlObject] = {
    val cursor = row.hcursor
    val name = cursor.get[String]("name").toOption.map(_.trim).filter(_.nonEmpty)

    (name, cursor.get[String]("topic").toOption.map(_.trim).filter(_.nonEmpty)) match {
      case (Some(named), Some(topic)) => Right(KsqlObject.Stream(named, topic, format(cursor)))
      case (named, _) => Left(named.getOrElse(unnamed("stream")))
    }
  }

  private def tableOf(row: Json): Either[String, KsqlObject] = {
    val cursor = row.hcursor
    val name = cursor.get[String]("name").toOption.map(_.trim).filter(_.nonEmpty)

    (name, cursor.get[String]("topic").toOption.map(_.trim).filter(_.nonEmpty)) match {
      case (Some(named), Some(topic)) =>
        // `isWindowed` is absent on ksqlDB releases before 0.10 and on some compatible servers. A table
        // KUI cannot tell is windowed is reported as not windowed, and that is the only defaulted fact on
        // this wire: the alternative is dropping the row, and a table missing from a list looks like a
        // table that is not there. ADR-055 §3 records it.
        Right(
          KsqlObject.Table(named, topic, format(cursor), cursor.get[Boolean]("isWindowed").getOrElse(false))
        )
      case (named, _) => Left(named.getOrElse(unnamed("table")))
    }
  }

  private def queryOf(row: Json): Either[String, KsqlObject] = {
    val cursor = row.hcursor
    val id = cursor
      .get[String]("id")
      .toOption
      .orElse(cursor.downField("id").get[String]("id").toOption)
      .map(_.trim)
      .filter(_.nonEmpty)

    (id, cursor.get[String]("queryString").toOption.map(_.trim).filter(_.nonEmpty)) match {
      case (Some(identifier), Some(text)) =>
        Right(
          KsqlObject.Query(
            identifier,
            cursor.get[List[String]]("sinks").getOrElse(Nil),
            text
          )
        )
      case (identifier, _) => Left(identifier.getOrElse(unnamed("query")))
    }
  }

  /** `{"name": "orders", "replicaInfo": [3, 3, 3]}` — one entry per partition, each the replica count.
    *
    * So the partition count is the array's length and the replication factor is its first entry. A topic
    * whose partitions have different replica counts — which happens mid-reassignment — reports the first,
    * because a replication factor is a property of the topic and this is the only one ksqlDB publishes;
    * `services/topic` is where a per-partition view belongs and it already has one.
    */
  private def topicOf(row: Json): Either[String, KsqlObject] = {
    val cursor = row.hcursor
    val name = cursor.get[String]("name").toOption.map(_.trim).filter(_.nonEmpty)
    val replicas = cursor.get[List[Int]]("replicaInfo").toOption.filter(_.nonEmpty)

    (name, replicas) match {
      case (Some(named), Some(counts)) => Right(KsqlObject.Topic(named, counts.size, counts.head))
      case (named, _) => Left(named.getOrElse(unnamed("topic")))
    }
  }

  private def format(cursor: io.circe.HCursor): Option[String] =
    cursor
      .get[String]("valueFormat")
      .toOption
      .orElse(cursor.get[String]("format").toOption)
      .map(_.trim)
      .filter(_.nonEmpty)

  /** What a statement that was not a query produced.
    *
    * ksqlDB answers `{"@type":"currentStatus","commandStatus":{"status":…,"message":…}}` for everything it
    * applies, and the message is its own sentence — *"Stream created and running"* — which is what the screen
    * shows. Anything else is an entity this client did not ask for, and the honest answer is to name the
    * document rather than to invent a sentence about what happened.
    */
  def statusFrom(entities: List[Json]): StatementOutcome =
    entities.lastOption match {
      case None =>
        StatementOutcome.Status("the ksqlDB cluster accepted the statement and said nothing about it", None)
      case Some(entity) =>
        val cursor = entity.hcursor
        val kind = cursor.get[String]("@type").toOption

        cursor.downField("commandStatus").get[String]("message").toOption match {
          case Some(message) =>
            StatementOutcome.Status(message, cursor.get[String]("commandId").toOption.orElse(kind))
          case None =>
            cursor.get[String]("message").toOption match {
              case Some(message) => StatementOutcome.Status(message, kind)
              case None =>
                StatementOutcome.Status(
                  s"the ksqlDB cluster answered a ${kind.getOrElse("document")} rather than a status; " +
                    "the object list is where its contents are drawn",
                  kind
                )
            }
        }
    }

  /** HTTP acceptance is not command completion. Unknown command states fail closed.
    *
    * The supported baseline's status API documents TERMINATED as a query started by the command and
    * subsequently stopped, not a rejected command. Keep its message/ID as an applied statement's status:
    * https://github.com/confluentinc/ksql/blob/v0.14.0-ksqldb/docs/developer-guide/ksqldb-rest-api/status-endpoint.md
    * RUNNING is also declared in that release's CommandStatus.Status, but the REST guide does not promise
    * completion for it. Preserve it conservatively as pending (audit Unknown), never an upstream failure:
    * https://github.com/confluentinc/ksql/blob/v0.14.0-ksqldb/ksqldb-rest-model/src/main/java/io/confluent/ksql/rest/entity/CommandStatus.java
    */
  def commandOutcome(entities: List[Json]): Either[KuiError, StatementOutcome] = {
    val decoded = entities.traverse { entity =>
      val cursor = entity.hcursor
      val command = cursor.downField("commandStatus")
      val id = cursor.get[String]("commandId").toOption
      val message = command.get[String]("message").getOrElse("the command has not reported a result")
      // The versioned /ksql examples omit @type. Field presence (even null/malformed) is enough to
      // require command state; a listing discriminator must not bypass that requirement.
      if command.succeeded || cursor.downField("commandId").succeeded ||
        cursor.downField("commandSequenceNumber").succeeded ||
        cursor.get[String]("@type").toOption.contains("currentStatus")
      then
        command.get[String]("status").toOption match {
          case Some("SUCCESS" | "TERMINATED") => Right(StatementOutcome.Status(message, id))
          case Some("QUEUED" | "PARSING" | "EXECUTING" | "RUNNING") =>
            Right(StatementOutcome.Pending(message, id))
          case Some("ERROR") => Left(InfrastructureError.Remote(ErrorCode.UpstreamKsql, message, Nil))
          case _ =>
            Left(
              InfrastructureError.Remote(
                ErrorCode.UpstreamKsql,
                "ksqlDB returned an unknown command status",
                Nil
              )
            )
        }
      else
        errorIn(entity) match {
          case Some(error) => Left(InfrastructureError.Remote(ErrorCode.UpstreamKsql, error, Nil))
          case None => Right(statusFrom(List(entity)))
        }
    }
    decoded.map(results =>
      results
        .collectFirst { case pending: StatementOutcome.Pending => pending }
        .orElse(results.lastOption)
        .getOrElse(StatementOutcome.Pending("ksqlDB accepted the request without a completion status", None))
    )
  }

  /** One line of a chunked `/query` response, as a frame.
    *
    * The response is a JSON array written incrementally, so each line is one element with the array's
    * punctuation around it: a leading `[` on the first, a trailing `,` on all but the last, a trailing `]` on
    * the last. Stripping those is what makes each line independently parseable, which is the whole reason
    * this is read line by line rather than decoded as a document — a document has to be complete, and a push
    * query's never is.
    */
  def frameOf(line: String): Option[Either[KuiError, QueryFrame]] = {
    val trimmed = line.trim.stripPrefix("[").stripSuffix(",").stripSuffix("]").trim

    if trimmed.isEmpty then None
    else
      parser.parse(trimmed) match {
        case Left(_) =>
          Some(
            Left(
              InfrastructureError.Remote(
                ErrorCode.UpstreamKsql,
                "the ksqlDB cluster sent a query frame KUI could not parse",
                Nil
              )
            )
          )
        case Right(json) => decodeFrame(json)
      }
  }

  private def decodeFrame(json: Json): Option[Either[KuiError, QueryFrame]] =
    json.hcursor.get[String]("errorMessage").toOption.orElse(errorIn(json)) match {
      case Some(message) => Some(Left(InfrastructureError.Remote(ErrorCode.UpstreamKsql, message, Nil)))
      case None => frameOfJson(json).map(Right(_))
    }

  private def errorIn(json: Json): Option[String] =
    json.hcursor
      .downField("errorMessage")
      .get[String]("message")
      .toOption
      .orElse(
        json.hcursor
          .get[String]("@type")
          .toOption
          .filter(_.endsWith("error"))
          .flatMap(_ => json.hcursor.get[String]("message").toOption)
      )

  /** `{"header":{…}}`, `{"row":{"columns":[…]}}` or `{"finalMessage":…}`; anything else is skipped. */
  def frameOfJson(json: Json): Option[QueryFrame] = {
    val cursor = json.hcursor

    cursor.downField("header").get[String]("schema").toOption match {
      case Some(schema) => Some(QueryFrame.Header(columnsOf(schema)))
      case None =>
        cursor.downField("row").downField("columns").focus.flatMap(_.asArray) match {
          case Some(columns) => Some(QueryFrame.Row(QueryRow(columns.toList.map(cellOf))))
          case None =>
            // `finalMessage` is ksqlDB saying the query is over on its own terms: a `LIMIT` was reached,
            // or the server is shutting down. It is `exhausted` and not `budget`, which is the difference
            // ADR-035's `done` reason carries.
            cursor.get[String]("finalMessage").toOption.map(_ => QueryFrame.Ended(exhausted = true))
        }
    }
  }

  /** One cell, rendered.
    *
    * A JSON `null` is a **SQL NULL** and becomes `None`, which reaches the browser as `null` rather than as
    * the four characters `null` — a cell with no value and a cell holding the text "null" are different
    * facts. A string is its own text, and anything else — a number, a boolean, a nested array or struct — is
    * its compact JSON, because ksqlDB's own CLI prints those the same way and a table drawing them needs a
    * string.
    */
  def cellOf(value: Json): Option[String] =
    if value.isNull then None else Some(value.asString.getOrElse(value.noSpaces))

  /** The column names out of a ksqlDB schema string.
    *
    * `` `K` STRING KEY, `V` ARRAY<STRUCT<`A` INT, `B` INT>> `` is one column pair, and a naive split on `,`
    * would make five columns out of two. So the split is depth-aware over `<>` and `()` and ignores commas
    * inside a quoted identifier, and each segment's name is its first backticked token.
    */
  def columnsOf(schema: String): List[String] = {
    val scanned =
      schema.foldLeft((List.empty[String], new StringBuilder, 0, false)) {
        case ((segments, current, depth, quoted), character) =>
          if character == '`' then (segments, current.append(character), depth, !quoted)
          else if quoted then (segments, current.append(character), depth, quoted)
          else if character == '<' || character == '(' then
            (segments, current.append(character), depth + 1, quoted)
          else if character == '>' || character == ')' then
            (segments, current.append(character), depth - 1, quoted)
          else if character == ',' && depth == 0 then
            (segments :+ current.toString, new StringBuilder, depth, quoted)
          else (segments, current.append(character), depth, quoted)
      }

    (scanned._1 :+ scanned._2.toString).map(_.trim).filter(_.nonEmpty).map(nameOf)
  }

  private val Backticked = """`([^`]+)`""".r

  private def nameOf(segment: String): String =
    Backticked.findFirstMatchIn(segment).map(_.group(1)).getOrElse(segment.takeWhile(!_.isWhitespace))
}
