package kui.serde.confluent

import java.util.Locale

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import cats.data.NonEmptyList
import cats.effect.Async
import cats.syntax.all.*
import io.circe.parser
import sttp.client4.*
import sttp.model.{StatusCode, Uri}

import kui.config.{SafeUrl, UrlPolicy}
import kui.http.upstream.{UpstreamClient, UpstreamConfig, UpstreamFailure}
import kui.kernel.PositiveInt
import kui.kernel.error.{ApplicationError, ErrorCode, InfrastructureError, KuiError}
import kui.serde.SchemaDescription

/** Which of the three registry schema languages a schema is written in.
  *
  * The registry spells these `AVRO`, `JSON` and `PROTOBUF`, and omits the field entirely when the answer is
  * Avro — the format predates the other two. The omission is handled here, once, rather than at each call
  * site, because a missing field that silently means something specific is exactly the kind of thing that is
  * handled correctly in two places out of three.
  */
enum SchemaType {
  case Avro, Json, Protobuf

  /** The registry's own spelling, kept so that an operator comparing KUI's screen with their registry sees
    * the same word (`SchemaDescription.schemaType`).
    */
  def label: String = this match {
    case Avro => "AVRO"
    case Json => "JSON"
    case Protobuf => "PROTOBUF"
  }
}

object SchemaType {

  def fromRegistry(raw: Option[String]): Either[String, SchemaType] =
    raw.map(_.trim.toUpperCase(Locale.ROOT)) match {
      case None | Some("") | Some("AVRO") => Right(Avro)
      case Some("JSON") => Right(Json)
      case Some("PROTOBUF") => Right(Protobuf)
      case Some(other) =>
        Left(
          s"the registry described this schema as '$other', which is not one of the three types KUI " +
            "knows (AVRO, JSON, PROTOBUF)"
        )
    }

  given CanEqual[SchemaType, SchemaType] = CanEqual.derived
}

/** One schema, as the registry holds it.
  *
  * `definition` is the schema text verbatim — the Avro JSON, the JSON Schema document, the `.proto` source —
  * and not a parsed form. Parsing belongs to whichever codec is about to use it, and keeping the text is what
  * lets the "view schema" panel show an operator the same characters their registry shows them.
  */
final case class RegistrySchema(id: Int, schemaType: SchemaType, definition: String) {

  def describe(jsonSchema: Option[String]): SchemaDescription =
    SchemaDescription(schemaType.label, definition, jsonSchema)
}

object RegistrySchema {
  given CanEqual[RegistrySchema, RegistrySchema] = CanEqual.derived
}

/** How KUI proves to a registry that it is allowed to ask.
  *
  * Basic and bearer, never both: a registry configured with two credentials is a registry whose operator does
  * not know which one is in use, and the one that silently loses is the one they will change when the other
  * expires (ADR-014).
  */
enum SchemaRegistryAuth {
  case Anonymous
  case Basic(user: String, password: String)
  case Bearer(token: String)
}

object SchemaRegistryAuth {
  given CanEqual[SchemaRegistryAuth, SchemaRegistryAuth] = CanEqual.derived
}

/** Everything KUI needs in order to talk to one cluster's registry.
  *
  * @param urls
  *   in preference order. More than one is a registry cluster, and `libs/http`'s `Failover` is what makes the
  *   second address useful (CL-008).
  * @param schemaCacheSize
  *   how many schemas to hold by id. A schema id is immutable, so a cached schema can never be wrong, only
  *   unused — which is why this cache has a size and no expiry.
  * @param urlPolicy
  *   the same address restriction `libs/http` applies to every other upstream (`ARCHITECTURE.md` ss14). It
  *   has to be a field rather than the default, because a schema registry is very often the one upstream that
  *   legitimately lives on `localhost` or on a private network — the quickstart's does — and
  *   `UrlPolicy.Strict` refuses both. The operator's `KUI_ALLOW_PRIVATE_UPSTREAMS` decides it; passing
  *   `Strict` unconditionally here would mean the relaxation they switched on had no effect.
  * @param subjectCacheTtl
  *   how long the *latest* version of a subject may be reused. This one must expire: registering a new
  *   version is how a schema evolves, and a KUI that cached the latest version forever would keep producing
  *   against a schema the topic has moved on from.
  * @param schemaMissTtl
  *   how long a schema id the registry reported missing may be treated as missing without asking again. A 404
  *   is an answer, unlike a registry that cannot be reached, but an id that 404s today can be allocated
  *   tomorrow, so the answer must expire.
  */
final case class SchemaRegistryConfig(
    urls: NonEmptyList[SafeUrl],
    auth: SchemaRegistryAuth = SchemaRegistryAuth.Anonymous,
    callTimeout: FiniteDuration = 10.seconds,
    urlPolicy: UrlPolicy = UrlPolicy.Strict,
    schemaCacheSize: Long = 1000L,
    subjectCacheTtl: FiniteDuration = 30.seconds,
    schemaMissTtl: FiniteDuration = 30.seconds
) {

  /** The registry as `libs/http` sees it: an upstream with a name, a bulkhead, a breaker and a failover list.
    *
    * The registry gets KUI's resilience stack rather than its own for the reason `Failover`'s own comment
    * gives — it was written with this upstream in mind. A registry that stops answering must degrade the
    * serde picker and nothing else.
    */
  def upstream: UpstreamConfig =
    UpstreamConfig(
      name = SchemaRegistry.UpstreamName,
      urls = urls,
      callTimeout = callTimeout,
      // Reads only, and every one of them idempotent: fetching a schema by id twice returns the same schema.
      maxRetries = 2,
      maxConcurrent = PositiveInt.unsafe(16),
      urlPolicy = urlPolicy
    )
}

/** The registry, reduced to the three questions decoding and encoding actually ask.
  *
  * Deliberately not the whole REST surface. ADR-014 splits the registry in two: *management* — subjects,
  * versions, compatibility, modes, contexts — belongs to the future `kui-schema-service`, which exposes it as
  * a screen; *wire format* belongs here, and needs only enough of the API to turn a schema id into a schema
  * and a topic into its current schema. Putting the management calls here as well would give KUI two registry
  * clients that both drift, and the one nobody is looking at would drift further.
  */
trait SchemaRegistry[F[_]] {

  /** The schema a record was written with. The id comes out of the record's own header. */
  def schemaById(id: Int): F[Either[KuiError, RegistrySchema]]

  /** The schema a record would be written with now, for one subject.
    *
    * `None` rather than an error when the subject does not exist: a topic with no registered schema is an
    * ordinary topic, not a broken one, and it is how `canSerialize` answers "no" without inventing a failure.
    */
  def latestForSubject(subject: String): F[Either[KuiError, Option[RegistrySchema]]]
}

object SchemaRegistry {

  /** The name this upstream is known by in metrics, spans and errors. A name, never a URL. */
  val UpstreamName: String = "schema-registry"

  /** The default `TopicNameStrategy` subject: the topic, a dash, then `key` or `value`.
    *
    * The other two Confluent strategies (`RecordNameStrategy`, `TopicRecordNameStrategy`) need the record's
    * own type name, which is inside the payload KUI has not decoded yet, so they cannot be applied on the
    * read path at all. They are a produce-time option and belong with the produce form, not here.
    */
  def subjectFor(topic: String, targetLabel: String): String = s"$topic-$targetLabel"

  /** The registry over HTTP.
    *
    * @param backend
    *   the *resilient* backend from `UpstreamClient`, not a raw one. Everything about timeouts, retries,
    *   failover and the circuit breaker is already in it, which is why nothing in this file mentions any of
    *   them.
    * @param baseUrl
    *   the registry's address. Only its *origin* — scheme, host, port — is used to build a request, and any
    *   path it carries is deliberately ignored here: `UpstreamClient` rebases every request onto the address
    *   it chose and prefixes that address's own path (`Failover.rebase`), so a registry published under a
    *   prefix such as `http://registry:8080/apis/ccompat/v7` would otherwise get the prefix twice and answer
    *   404 to every lookup. Which host a request actually goes to is the backend's decision in any case,
    *   because failover may send it to the second address.
    */
  def http[F[_]: Async](backend: Backend[F], baseUrl: SafeUrl, auth: SchemaRegistryAuth): SchemaRegistry[F] =
    new Http[F](backend, baseUrl, auth)

  final private class Http[F[_]: Async](backend: Backend[F], baseUrl: SafeUrl, auth: SchemaRegistryAuth)
      extends SchemaRegistry[F] {

    /** The origin of the configured address, with its path dropped for the reason given above. */
    private val root: Uri =
      Uri
        .parse(baseUrl.value)
        .getOrElse(uri"http://schema-registry.invalid")
        .withPath(Nil)

    def schemaById(id: Int): F[Either[KuiError, RegistrySchema]] =
      get(root.addPath("schemas", "ids", id.toString)).map(_.flatMap {
        case None =>
          Left(
            ApplicationError.NotFound("schema", id.toString, ErrorCode.SchemaNotFound): KuiError
          )
        case Some(body) => parseSchema(body, Some(id))
      })

    def latestForSubject(subject: String): F[Either[KuiError, Option[RegistrySchema]]] =
      get(root.addPath("subjects", subject, "versions", "latest")).map(_.flatMap {
        case None => Right(None)
        case Some(body) => parseSchema(body, None).map(Some(_))
      })

    /** One GET. `None` is a 404, which both callers treat as an answer rather than a failure.
      *
      * The two "not found" cases are not the same thing and both arrive as 404: a subject that has never been
      * registered, and a schema id that does not exist. Which of them a 404 means is the caller's business,
      * so this returns the absence and says nothing about what it means.
      */
    private def get(uri: Uri): F[Either[KuiError, Option[String]]] =
      authenticated(basicRequest.get(uri).response(asStringAlways))
        .send(backend)
        .map { response =>
          if response.code == StatusCode.NotFound then Right(None)
          else if response.code.isSuccess then Right(Some(response.body))
          else if response.code == StatusCode.Unauthorized || response.code == StatusCode.Forbidden then
            Left(InfrastructureError.AuthFailed(UpstreamName))
          else Left(InfrastructureError.Upstream(UpstreamName, response.code.code))
        }
        .recover {
          // The resilient backend carries its typed error inside this one exception rather than losing it in
          // a message. Anything else really is unexpected and is reported as an unreachable upstream, which
          // is the honest description of "the call did not produce a response".
          case UpstreamFailure(error) => Left(error)
          case failure: Exception =>
            Left(InfrastructureError.Unreachable(UpstreamName, UpstreamClient.safeFailureCause(failure)))
        }

    private def authenticated(request: Request[String]): Request[String] =
      auth match {
        case SchemaRegistryAuth.Anonymous => request
        case SchemaRegistryAuth.Basic(user, password) => request.auth.basic(user, password)
        case SchemaRegistryAuth.Bearer(token) => request.auth.bearer(token)
      }

    /** The registry's `{schema, schemaType, id}` object.
      *
      * `expectedId` is supplied on the by-id path because the response to `GET /schemas/ids/{id}` does not
      * repeat the id, and the caller already knows it. On the by-subject path the response does carry one and
      * it is the only place the id can come from.
      */
    private def parseSchema(body: String, expectedId: Option[Int]): Either[KuiError, RegistrySchema] =
      parser.parse(body).left.map(_ => malformed("it is not JSON")).flatMap { json =>
        val cursor = json.hcursor
        for {
          definition <- cursor
            .get[String]("schema")
            .left
            .map(_ => malformed("it has no 'schema' field holding the schema text"))
          rawType = cursor.get[String]("schemaType").toOption
          schemaType <- SchemaType.fromRegistry(rawType).left.map(malformed)
          id <- expectedId
            .orElse(cursor.get[Int]("id").toOption)
            .toRight(malformed("it has no 'id' field"))
        } yield RegistrySchema(id, schemaType, definition)
      }

    /** The registry answered, and the answer was not one KUI can use.
      *
      * `Remote` and not `Unreachable`: `Unreachable.message` deliberately names only the upstream, because a
      * connection failure's text routinely contains hosts and credentials, so the explanation would be
      * dropped on the floor. This failure's text is written here from the shape of the response and contains
      * nothing the registry sent, so it is safe to show — and it is the only thing that tells an operator
      * their "registry" is in fact a proxy's error page.
      */
    private def malformed(why: String): KuiError =
      InfrastructureError.Remote(
        ErrorCode.UpstreamUnavailable,
        s"the schema registry's answer could not be understood: $why",
        Nil
      )
  }
}
