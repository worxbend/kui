package kui.connect.infrastructure

import cats.effect.kernel.Async
import cats.effect.syntax.all.*
import cats.syntax.all.*
import io.circe.{parser, Json}
import sttp.client4.*
import sttp.model.{StatusCode, Uri}

import kui.config.SafeUrl
import kui.connect.domain.*
import kui.http.upstream.{UpstreamClient, UpstreamFailure}
import kui.kernel.error.{ApplicationError, ErrorCode, InfrastructureError, KuiError}
import kui.kernel.{ConnectName, ConnectorName, TaskId}

/** The Kafka Connect REST API, as KUI's own client.
  *
  * ==Which API this assumes, and what it does when the worker is older==
  *
  * `GET /connectors?expand=status&expand=info` — one request for every connector, its type and every task's
  * state — arrived in Kafka **2.3** (KIP-465) and is what this client asks for first. A worker that predates
  * it answers the plain array of names it has answered since 0.10, and that is not an error: the client
  * notices the shape and falls back to one `GET /connectors/{name}/status` per connector. The fallback is a
  * request per connector, which is why it is the fallback and not the default.
  *
  * There is no version negotiation and no `GET /` probe to decide between them, because the shape of the
  * answer is the negotiation: it is one round trip either way and it cannot go stale.
  *
  * ==Nothing here throws==
  *
  * Every method answers a `KuiError` on the left. A worker that is down, slow, rebalancing or rejecting KUI's
  * credentials arrives as a value, because the caller is a use case that has to keep the rest of the product
  * working while this one upstream is broken.
  *
  * ==A rebalancing worker is not a broken one==
  *
  * Connect answers `409` while its workers are agreeing on an assignment — the documented "Cannot complete
  * request momentarily due to stale configuration" — and it does so on reads as well as on writes. That is
  * KUI's `ErrorCode.ConnectRebalancing`, declared in wave 1 and unused until now: **retryable**, an
  * `ApplicationError` rather than an `InfrastructureError`, so ADR-039 §6 keeps it from dimming the connect
  * capability for everybody. A rebalance lasts seconds and clears itself; a capability dimmed by one is a red
  * badge an operator cannot clear and learns to ignore, which is the failure mode that ADR-039 §6 exists to
  * prevent. `ConnectMapping.section` is the other half of this rule and `ConnectCapabilities.probe` is the
  * third.
  */
final class ConnectHttp[F[_]: Async](
    backend: Backend[F],
    baseUrl: SafeUrl,
    val connect: ConnectName,
    credentials: ConnectCredentials[F]
) extends ConnectWorkerPort[F] {

  import ConnectHttp.*

  /** How many `GET /connectors/{name}/status` calls the pre-2.3 fallback keeps in flight at once.
    *
    * Half of `ConnectWiring.MaxConcurrentPerWorker` (8), deliberately not equal to it: that limit is the
    * bulkhead for *everything* KUI sends this one worker, and a connector-list request that filled it would
    * queue every other screen's call to the same worker behind itself. `SubjectListUseCase.MaxConcurrentRows`
    * uses the identical halving for the same reason against a registry. It cannot be read from here — `app`
    * depends on `infrastructure`, not the other way round — so the number is restated rather than shared.
    */
  private val MaxConcurrentPerConnector: Int = 4

  /** The address every request below is built against, with the configured **path stripped**.
    *
    * `RegistryHttp`'s reason, which cost that service a milestone: only the path and query of each request
    * are built from this, and `Failover.rebase` replaces the scheme and authority and *prefixes the base
    * URL's own path*. A request built against the full configured URL therefore has that path applied twice,
    * which for a Connect cluster behind an ingress at `/connect` produces `/connect/connect/connectors` and a
    * 404 that looks exactly like an address that is not a Connect worker.
    */
  private val root: Uri =
    Uri.parse(baseUrl.value).getOrElse(uri"http://kafka-connect.invalid").withWholePath("")

  def connectors: F[Either[KuiError, ConnectorFacts]] =
    get(root.addPath(ConnectorsPath).addParam("expand", "status").addParam("expand", "info")).flatMap {
      case Left(error) => error.asLeft[ConnectorFacts].pure[F]
      case Right(body) =>
        parser.parse(body) match {
          case Left(_) => malformed("it is not JSON").asLeft[ConnectorFacts].pure[F]

          // The expanded shape: an object keyed by connector name. Read without a further request.
          case Right(json) if json.isObject =>
            expanded(json).pure[F]

          // The pre-2.3 shape: a bare array of names. One status request per connector, and a connector
          // whose status does not arrive is `unreadable` rather than absent.
          case Right(json) if json.isArray =>
            json.as[List[String]] match {
              case Left(_) => malformed("its connector list is not an array of names").asLeft.pure[F]
              case Right(names) => perConnector(names)
            }

          case Right(_) =>
            malformed("it is neither a connector list nor an expanded connector document")
              .asLeft[ConnectorFacts]
              .pure[F]
        }
    }

  def operate(connector: ConnectorName, operation: ConnectorOperation): F[Either[KuiError, Unit]] = {
    val base = root.addPath(ConnectorsPath, connector.value)

    operation match {
      // PUT, and empty-bodied, which is what the API documents for both. `basicRequest` sends no body
      // unless one is set, and a Connect worker rejects a `Content-Type` with no content.
      case ConnectorOperation.Pause =>
        send(basicRequest.put(base.addPath("pause")), Some(connector)).map(_.void)
      case ConnectorOperation.Resume =>
        send(basicRequest.put(base.addPath("resume")), Some(connector)).map(_.void)
      case ConnectorOperation.Restart =>
        get(root).flatMap {
          case Left(error) => error.asLeft[Unit].pure[F]
          case Right(body) =>
            // KIP-745 (Kafka 3.0), not KIP-465's 2.3 expanded listing. Older workers silently ignore
            // includeTasks, so HTTP success alone cannot negotiate this capability.
            val major = parser
              .parse(body)
              .toOption
              .flatMap(_.hcursor.get[String]("version").toOption)
              .flatMap(_.takeWhile(_.isDigit).toIntOption)
            if major.exists(_ >= 3) then
              send(
                basicRequest.post(
                  base
                    .addPath("restart")
                    .addParam("includeTasks", "true")
                    .addParam("onlyFailed", "false")
                ),
                Some(connector)
              ).map(_.void)
            else restartLegacy(connector, base)
        }
    }
  }

  private def restartLegacy(connector: ConnectorName, base: Uri): F[Either[KuiError, Unit]] =
    get(base.addPath("status")).flatMap {
      case Left(error) => error.asLeft[Unit].pure[F]
      case Right(body) =>
        val ids = for {
          json <- parser.parse(body).toOption
          tasks <- json.hcursor.get[List[Json]]("tasks").toOption
          ids <- tasks.traverse(_.hcursor.get[Int]("id").toOption.filter(_ >= 0))
        } yield ids.distinct.sorted
        ids match {
          case None =>
            malformed("the task identities needed for a legacy restart could not be read")
              .asLeft[Unit]
              .pure[F]
          case Some(tasks) =>
            send(basicRequest.post(base.addPath("restart")), Some(connector)).flatMap {
              case Left(error) => error.asLeft[Unit].pure[F]
              case Right(_) =>
                tasks.foldLeft(().asRight[KuiError].pure[F]) { (previous, task) =>
                  previous.flatMap {
                    case Left(error) => error.asLeft[Unit].pure[F]
                    case Right(_) =>
                      send(
                        basicRequest.post(base.addPath("tasks", task.toString, "restart")),
                        Some(connector)
                      ).map(_.void)
                  }
                }
            }
        }
    }

  /** `{"orders-sink": {"status": {...}, "info": {...}}}`, which is one connector per key. */
  private def expanded(json: Json): Either[KuiError, ConnectorFacts] =
    json.asObject.toRight(malformed("its expanded connector document is not an object")).map { document =>
      val read = document.toList.map((name, entry) =>
        connectorFrom(name, entry.hcursor.downField("status").focus, entry.hcursor.downField("info").focus)
          .toRight(name)
      )

      ConnectorFacts(
        connectors = read.collect { case Right(connector) => connector }.sortBy(_.name.value),
        unreadable = read.collect { case Left(name) => name }.sorted
      )
    }

  /** The fallback: one status request per name, for a worker that cannot expand.
    *
    * A status request that fails takes its connector to `unreadable` and does **not** fail the call. That is
    * the difference this list exists to carry: one worker of a Connect cluster being wedged loses the status
    * of the connectors it owns, and the other rows are real answers that a screen should draw.
    *
    * Bounded, not sequential: `MaxConcurrentPerConnector` requests in flight at once, not one after another.
    * A legacy or partially-unhealthy worker with many connectors would otherwise turn one connector-list
    * request into N sequential round trips — each up to the worker's own `callTimeout` — before the screen
    * could render anything at all.
    */
  private def perConnector(names: List[String]): F[Either[KuiError, ConnectorFacts]] =
    names.sorted
      .parTraverseN(MaxConcurrentPerConnector)(name =>
        get(root.addPath(ConnectorsPath, name, "status")).map {
          case Left(_) => name.asLeft[Connector]
          case Right(body) =>
            parser.parse(body).toOption.flatMap(json => connectorFrom(name, Some(json), None)).toRight(name)
        }
      )
      .map(read =>
        ConnectorFacts(
          connectors = read.collect { case Right(connector) => connector },
          unreadable = read.collect { case Left(name) => name }
        ).asRight[KuiError]
      )

  /** One connector, from whichever of the two documents the worker sent.
    *
    * `None` means this connector is unreadable: no status at all, a name KUI's own validation refuses, or a
    * task numbered with something that is not a task id. Every one of those is "the worker named it and KUI
    * cannot describe it", which is a row on the screen rather than a silent omission.
    */
  private def connectorFrom(name: String, status: Option[Json], info: Option[Json]): Option[Connector] =
    for {
      connectorName <- ConnectorName.from(name).toOption
      document <- status
      cursor = document.hcursor
      connectorNode = cursor.downField("connector")
      state <- connectorNode.get[String]("state").toOption
      tasks <- tasksFrom(cursor.downField("tasks").focus)
    } yield Connector(
      connect = connect,
      name = connectorName,
      // `type` is on the status document from Kafka 2.0 and on the info document always; every release
      // before that sends neither, and `ConnectorKind.fromWorker` answers "the worker did not say"
      // rather than guessing a direction the icon would then point the wrong way.
      kind = ConnectorKind.fromWorker(
        cursor
          .get[String]("type")
          .toOption
          .orElse(info.flatMap(_.hcursor.get[String]("type").toOption))
      ),
      state = ConnectorState(state),
      workerId = text(connectorNode.get[String]("worker_id").toOption),
      trace = text(connectorNode.get[String]("trace").toOption),
      tasks = tasks
    )

  /** Every task of one connector, or `None` if any of them could not be read.
    *
    * All or nothing per connector, deliberately. A connector drawn with three of its four tasks would report
    * `3/3 tasks` over a cluster with four, and a task quietly dropped is the one that was failing.
    */
  private def tasksFrom(node: Option[Json]): Option[List[ConnectorTask]] =
    node match {
      // A connector with no `tasks` key at all is a connector with no tasks, which a `PAUSED` or freshly
      // created connector genuinely is. It is not the same as a task list KUI could not read.
      case None => Some(Nil)
      case Some(json) =>
        json.asArray.map(_.toList).flatMap { entries =>
          entries
            .traverse { entry =>
              val cursor = entry.hcursor
              for {
                raw <- cursor.get[Int]("id").toOption
                id <- TaskId.from(raw).toOption
                state <- cursor.get[String]("state").toOption
              } yield ConnectorTask(
                id = id,
                state = ConnectorState(state),
                workerId = text(cursor.get[String]("worker_id").toOption),
                trace = text(cursor.get[String]("trace").toOption)
              )
            }
            .map(_.sortBy(_.id.value))
        }
    }

  /** A field the worker sent as an empty or blank string is the same as one it did not send.
    *
    * Connect writes `"trace": ""` on a task that has not failed, and an empty trace rendered as a reason
    * would put an empty sentence under a healthy connector.
    */
  private def text(raw: Option[String]): Option[String] = raw.map(_.trim).filter(_.nonEmpty)

  private def get(uri: Uri): F[Either[KuiError, String]] =
    send(basicRequest.get(uri), None)

  /** Authenticate, send, and turn everything that is not a usable response into a typed error.
    *
    * The worker's response body is **read** on a failure and reduced to its own `message` field, which is
    * where Connect writes the sentence it wrote for a human — "Connector orders-sink not found" or the
    * rebalance notice. ADR-034 forbids echoing an upstream body wholesale; one field it wrote for this
    * purpose is the difference between "the Connect cluster said no" and a status code.
    */
  private def send(
      request: Request[Either[String, String]],
      connector: Option[ConnectorName]
  ): F[Either[KuiError, String]] =
    credentials.authenticate(request.header("Accept", "application/json").response(asStringAlways)).flatMap {
      case Left(error) => error.asLeft[String].pure[F]
      case Right(authenticated) =>
        authenticated
          .send(backend)
          .map(response =>
            if response.code.isSuccess then Right(response.body)
            else Left(errorFrom(response.code, response.body, connector))
          )
          .recover {
            // The resilient backend carries its typed error inside this one exception rather than losing
            // it in a message. Anything else is genuinely unexpected and is reported as an upstream that
            // did not produce a response, which is the honest description.
            case UpstreamFailure(error) => Left(error)
            case failure: Exception =>
              Left(InfrastructureError.Unreachable(upstreamName, UpstreamClient.safeFailureCause(failure)))
          }
    }

  /** A failure status, classified.
    *
    * The `409` arm is the one with a rule attached and it is stated in this class's header: it is a
    * rebalance, it is transient, it is `ErrorCode.ConnectRebalancing` and it is an `ApplicationError` so that
    * it cannot dim a capability. The others are the ordinary fold: credentials rejected, connector gone,
    * anything else an upstream failure carrying the worker's own sentence.
    */
  private def errorFrom(status: StatusCode, body: String, connector: Option[ConnectorName]): KuiError = {
    val detail = parser
      .parse(body)
      .toOption
      .flatMap(_.hcursor.get[String]("message").toOption)
      .map(_.trim)
      .filter(_.nonEmpty)

    if status == StatusCode.Conflict then
      ApplicationError.Refused(
        ErrorCode.ConnectRebalancing,
        s"the Kafka Connect cluster '${connect.value}' is rebalancing and cannot answer yet" +
          detail.fold("")(message => s": $message")
      )
    else if status == StatusCode.Unauthorized || status == StatusCode.Forbidden then
      InfrastructureError.AuthFailed(upstreamName)
    else if status == StatusCode.NotFound then
      connector match {
        // There is no `KUI-CONNECTOR-NOT-FOUND` in the shipped vocabulary and this wave may not add an
        // error code (house rule 3). The two shapes available are a 409 naming the connector and a 501
        // saying this deployment cannot do it at all; the second would be false, so it is the first, and
        // it is the same code the alerts service answers for an event id that names nothing. ADR-054 §8.
        case Some(name) =>
          ApplicationError.Conflict(
            s"connector '${name.value}' does not exist on the Kafka Connect cluster '${connect.value}'"
          )
        // A 404 on a *list* is not an absence: `/connectors` exists on every Connect worker there has ever
        // been. It is an address that is not a Connect worker — most often a proxy or an ingress pointed
        // at the wrong service — and saying so is what an operator can act on.
        case None =>
          InfrastructureError.Remote(
            ErrorCode.UpstreamUnavailable,
            s"the address configured for the Kafka Connect cluster '${connect.value}' answered 404 for " +
              "GET /connectors, so it does not look like a Kafka Connect worker",
            Nil
          )
      }
    else
      detail match {
        case None => InfrastructureError.Upstream(upstreamName, status.code)
        case Some(message) =>
          InfrastructureError.Remote(
            ErrorCode.UpstreamUnavailable,
            s"the Kafka Connect cluster '${connect.value}' answered ${status.code}: $message",
            Nil
          )
      }
  }

  private def malformed(why: String): KuiError =
    InfrastructureError.Remote(
      ErrorCode.UpstreamUnavailable,
      s"the Kafka Connect cluster '${connect.value}' answered something KUI could not understand: $why",
      Nil
    )

  /** The name this upstream is known by in errors and metrics: `kafka-connect.<name>`, never a URL. A
    * deployment with two Connect clusters must be able to see which of them is failing.
    */
  private def upstreamName: String = ConnectHttp.upstreamName(connect)
}

object ConnectHttp {

  /** The name this kind of upstream is known by, before a Connect cluster's name is added to it. */
  val UpstreamName: String = "kafka-connect"

  /** The one path segment every request in this file starts from. */
  val ConnectorsPath: String = "connectors"

  /** `kafka-connect.payments`, which is what appears in a metric label and in an error a screen may show. */
  def upstreamName(connect: ConnectName): String = s"$UpstreamName.${connect.value}"
}
