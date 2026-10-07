package kui.schema.infrastructure

import cats.effect.kernel.Async
import cats.syntax.all.*
import io.circe.{parser, Decoder, HCursor, Json}
import sttp.client4.*
import sttp.model.{MediaType, Method, StatusCode, Uri}

import kui.config.SafeUrl
import kui.http.upstream.{UpstreamClient, UpstreamFailure}
import kui.kernel.error.{ApplicationError, ErrorCode, FieldError, InfrastructureError, KuiError}
import kui.kernel.{SchemaId, Subject}
import kui.schema.domain.*

/** The Schema Registry's REST API, as KUI's own client (ADR-014).
  *
  * ==Why KUI writes this rather than wrapping Confluent's client==
  *
  * Confluent's `kafka-schema-registry-client` brings its own HTTP stack, its own retry loop and its own
  * cache. KUI already has all three, and they are not interchangeable: the retry has to be the one with the
  * circuit breaker and the bulkhead in front of it (ADR-037), or a registry that has stopped answering
  * consumes KUI's threads instead of being cut off; the cache has to be the one that reports the metrics
  * ADR-016 requires of every cache in the product. Once those are KUI's, what is left of the vendor client is
  * a URL builder and a JSON decoder — which is this file.
  *
  * ==What it is spoken to==
  *
  * The API is Confluent's, but the products that speak it are not all Confluent's: Apicurio, Karapace and Red
  * Hat's registry all implement it, with small differences. Two of those differences are handled here rather
  * than assumed away:
  *
  *   - the media type. `application/vnd.schemaregistry.v1+json` is the documented one; several
  *     implementations only ever send `application/json`, so both are accepted and the vendor type is what
  *     KUI *sends*.
  *   - `schemaType` is absent on an Avro schema, because Avro predates the other two formats.
  *
  * ==Nothing here throws==
  *
  * Every method answers a `KuiError` on the left. A registry that is down, slow, rejecting KUI's credentials
  * or returning a proxy's HTML error page all arrive as values, because the caller is a use case that has to
  * keep the rest of the product working while this one upstream is broken.
  */
final class RegistryHttp[F[_]: Async](
    backend: Backend[F],
    baseUrl: SafeUrl,
    credentials: RegistryCredentials[F]
) extends SchemaRegistryPort[F] {

  import RegistryHttp.*

  /** The address every request below is built against, with the configured **path stripped**.
    *
    * Only the path and the query of each request are built from this. Which host it actually goes to is the
    * resilient backend's decision, because failover may send it to the second configured address — and
    * `Failover.rebase` does that by replacing the scheme and authority and *prefixing the base URL's own
    * path*, so that a base of `https://host/api` and a request path of `/subjects` becomes
    * `https://host/api/subjects`.
    *
    * Which means a request built against the full configured URL has that path applied twice. A registry
    * mounted at a sub-path — Apicurio serves the Confluent-compatible API at `/apis/ccompat/v7`, and it is
    * the registry the quickstart runs — produced `/apis/ccompat/v7/apis/ccompat/v7/subjects`, answered 404,
    * and every schema screen reported "the configured address does not look like a Schema Registry". The
    * message was accurate about the symptom and pointed at the operator's configuration, which was right.
    *
    * So the path is dropped here and `rebase` puts it back exactly once. The scheme and host are kept only so
    * that a logged or failed URI reads sensibly; they are replaced before the request is sent.
    */
  private val root: Uri =
    Uri.parse(baseUrl.value).getOrElse(uri"http://schema-registry.invalid").withWholePath("")

  def subjects: F[Either[KuiError, List[Subject]]] =
    get(root.addPath("subjects")).map(_.flatMap {
      // A registry with no subjects answers `[]`; a 404 here is not "no subjects", it is a URL that is not
      // a Schema Registry at all — most often a proxy or an ingress pointed at the wrong service.
      case None => Left(notARegistry("GET /subjects answered 404"))
      case Some(body) =>
        decode[List[String]](body, "a JSON array of subject names")
          .map(_.map(Subject.unsafe))
    })

  /** Three requests, which is the fewest the Confluent API allows for one list row.
    *
    * Each of the three is the only source of its fact. The version *list* is the only source of a count that
    * survives a soft-deleted version — the latest version number is not the number of versions, and a
    * registry where v2 was deleted answers `latest` with v3 over a list of two. The latest version's own
    * document is the only source of `schemaType`. `/config/{subject}` is the only source of a level.
    *
    * They are sent one after another rather than all at once because this call is already multiplied by the
    * page size, and the bulkhead in front of the registry is deliberately narrow: the concurrency that
    * matters is the caller's across rows, not this method's across three requests for one row.
    *
    * A subject the list named a moment ago and the version call cannot find is `None` and not an error. A
    * registry is written to while somebody is reading it, and the other two requests are skipped because
    * there is nothing left to ask them about.
    */
  def summary(subject: Subject): F[Either[KuiError, Option[SubjectSummary]]] =
    versions(subject).flatMap {
      case Left(error) => error.asLeft[Option[SubjectSummary]].pure[F]
      case Right(None) => none[SubjectSummary].asRight[KuiError].pure[F]
      case Right(Some(numbers)) =>
        (schema(subject, VersionSelector.Latest), subjectCompatibility(subject)).tupled.map {
          case (Left(error), _) => error.asLeft[Option[SubjectSummary]]
          case (_, Left(error)) => error.asLeft[Option[SubjectSummary]]
          case (Right(latest), Right(ownLevel)) =>
            SubjectSummary(
              subject = subject,
              // Absent when the latest version went away between the two requests. The count still
              // stands: it was measured, and a row with a count and no format says exactly that.
              format = latest.map(_.format),
              versionCount = Some(numbers.size),
              compatibility = ownLevel.map(SubjectCompatibility.own)
            ).some.asRight[KuiError]
        }
    }

  def versions(subject: Subject): F[Either[KuiError, Option[List[SchemaVersion]]]] =
    get(root.addPath("subjects", subject.value, "versions")).map(_.flatMap {
      case None => Right(None)
      case Some(body) =>
        decode[List[Int]](body, "a JSON array of version numbers").map(numbers =>
          Some(numbers.filter(_ >= 1).sorted.map(SchemaVersion.unsafe))
        )
    })

  def schema(subject: Subject, version: VersionSelector): F[Either[KuiError, Option[RegisteredSchema]]] =
    get(root.addPath("subjects", subject.value, "versions", version.path)).map(_.flatMap {
      case None => Right(None)
      case Some(body) => parseSchema(body, subject).map(Some(_))
    })

  def globalCompatibility: F[Either[KuiError, CompatibilityLevel]] =
    get(root.addPath("config")).map(_.flatMap {
      // No global level has ever been set, so the registry's own default is in force. That is a fact, not
      // an absence: `BACKWARD` is what the registry will apply to the next registration.
      case None => Right(CompatibilityLevel.RegistryDefault)
      case Some(body) => parseLevel(body).map(_.getOrElse(CompatibilityLevel.RegistryDefault))
    })

  def subjectCompatibility(subject: Subject): F[Either[KuiError, Option[CompatibilityLevel]]] =
    // A 404 here means "this subject has no level of its own", which is the overwhelmingly common case and
    // is why it is `None` rather than an error. It is also, unhelpfully, what a registry answers for a
    // subject that does not exist at all; the two are told apart by the caller, which has already looked
    // the subject up, rather than by a second request from here.
    get(root.addPath("config", subject.value)).map(_.flatMap {
      case None => Right(None)
      case Some(body) => parseLevel(body)
    })

  /** Two requests, because the Confluent API answers registration with an id and no version.
    *
    * `POST /subjects/{subject}/versions` returns `{"id": N}` — that is the entire documented response, and
    * the compatibility layers of Apicurio and Karapace copy it. So the version number is asked for
    * separately, with `POST /subjects/{subject}`, which is the registry's "which version is *this* schema"
    * lookup. That call is used rather than `GET .../versions/latest` on purpose: latest is a race — somebody
    * else's registration between the two calls would give this operator back a version that is not theirs —
    * whereas the lookup is about the exact document that was just sent and cannot answer about another one.
    *
    * The lookup's failure does not fail the registration. The schema is in the registry by then, and
    * answering `Left` would tell an operator to do it again; the version travels as `None` instead and the
    * screen says the number could not be read. See [[kui.schema.domain.RegisteredVersion]].
    *
    * The rejection path is the one that matters most here. `errorFrom` turns the registry's 409 or 422 into
    * `KUI-VALIDATION` carrying the registry's own sentence in `details`, keyed to the `definition` field,
    * because that sentence — "Schema being registered is incompatible with an earlier schema" and the reason
    * it gives — is the only part of the answer an operator can act on.
    */
  def register(subject: Subject, proposed: ProposedSchema): F[Either[KuiError, RegisteredVersion]] = {
    val body = schemaBody(proposed)

    post(root.addPath("subjects", subject.value, "versions"), body, RegisteredField).flatMap {
      // A 404 on registration is not an absence: the subject is *created* by this call, so there is nothing
      // that could be missing. It is the same misconfigured address `subjects` reports.
      case Left(error) => error.asLeft[RegisteredVersion].pure[F]
      case Right(None) =>
        notARegistry(s"POST /subjects/${subject.value}/versions answered 404")
          .asLeft[RegisteredVersion]
          .pure[F]
      case Right(Some(answer)) =>
        parseId(answer) match {
          case Left(error) => error.asLeft[RegisteredVersion].pure[F]
          case Right(id) =>
            lookupVersion(subject, body).map(version =>
              RegisteredVersion(subject, id, version).asRight[KuiError]
            )
        }
    }
  }

  /** Which version the schema just registered became, or `None` when the registry would not say.
    *
    * Every failure here is swallowed into `None` — deliberately, and this is the only place in this file that
    * swallows one. The registration has already succeeded; there is no failure left to report that would not
    * be a lie about what happened to the operator's schema.
    */
  private def lookupVersion(subject: Subject, body: Json): F[Option[SchemaVersion]] =
    post(root.addPath("subjects", subject.value), body).map {
      case Right(Some(answer)) =>
        parser
          .parse(answer)
          .toOption
          .flatMap(_.hcursor.get[Int]("version").toOption)
          .flatMap(SchemaVersion.from(_).toOption)
      case _ => None
    }

  private def parseId(body: String): Either[KuiError, SchemaId] =
    parser.parse(body).left.map(_ => malformed("it is not JSON")).flatMap { json =>
      json.hcursor
        .get[Int]("id")
        .left
        .map(_ => malformed("it has no 'id' field naming the schema it stored"))
        .flatMap(SchemaId.from(_).left.map(error => malformed(error.message)))
    }

  def setGlobalCompatibility(level: CompatibilityLevel): F[Either[KuiError, Unit]] =
    put(root.addPath("config"), levelBody(level)).map(_.void)

  def setSubjectCompatibility(subject: Subject, level: CompatibilityLevel): F[Either[KuiError, Unit]] =
    put(root.addPath("config", subject.value), levelBody(level)).map(_.void)

  def checkCompatibility(
      subject: Subject,
      version: VersionSelector,
      proposed: ProposedSchema
  ): F[Either[KuiError, Option[CompatibilityVerdict]]] = {
    val uri = root
      .addPath("compatibility", "subjects", subject.value, "versions", version.path)
      // Without this the registry answers a bare `{"is_compatible": false}` and the operator is told
      // "no" with no reason, which is the least useful possible answer to "why will this not register".
      .addParam("verbose", "true")

    post(uri, schemaBody(proposed)).map(_.flatMap {
      case None => Right(None)
      case Some(body) => parseVerdict(body).map(Some(_))
    })
  }

  // -----------------------------------------------------------------------------------------------
  // The three verbs
  // -----------------------------------------------------------------------------------------------

  /** One GET. `None` is a 404, which every caller treats as an answer rather than as a failure — though not
    * all of them treat it as the *same* answer, which is why this says nothing about what it means.
    */
  private def get(uri: Uri): F[Either[KuiError, Option[String]]] =
    send(basicRequest.get(uri))

  private def put(uri: Uri, body: Json): F[Either[KuiError, String]] =
    send(basicRequest.put(uri).body(body.noSpaces).contentType(VendorMediaType)).map(_.flatMap {
      case Some(answer) => Right(answer)
      case None => Left(InfrastructureError.Upstream(UpstreamName, StatusCode.NotFound.code))
    })

  /** @param rejectedField
    *   which request field a refusal belongs beside, when the registry refuses this call. It is a parameter
    *   rather than a constant because the same status from the same registry means different things on
    *   different calls: a 422 on `PUT /config` is about the level somebody typed, and a 409 on
    *   `POST /subjects/.../versions` is about the schema document. A refusal with no field lands on the
    *   request as a whole, which is what [[kui.kernel.error.FieldError]] models with a `None` field.
    */
  private def post(
      uri: Uri,
      body: Json,
      rejectedField: Option[String] = None
  ): F[Either[KuiError, Option[String]]] =
    send(basicRequest.post(uri).body(body.noSpaces).contentType(VendorMediaType), rejectedField)

  /** Authenticate, send, and turn everything that is not a usable response into a typed error.
    *
    * The registry's response body is **read** on a failure and then thrown away except for its `error_code`
    * and its own message. ADR-034 forbids echoing an upstream body wholesale — it may carry another system's
    * internals or credentials — but the registry's error message is written for exactly this purpose and is
    * the difference between "the registry said no" and "the registry said this schema drops a field that has
    * no default".
    */
  private def send(
      request: Request[Either[String, String]],
      rejectedField: Option[String] = None
  ): F[Either[KuiError, Option[String]]] =
    credentials.authenticate(request.header("Accept", AcceptHeader).response(asStringAlways)).flatMap {
      case Left(error) => error.asLeft[Option[String]].pure[F]
      case Right(authenticated) =>
        authenticated
          .send(backend)
          .map { response =>
            if response.code.isSuccess then Right(Some(response.body))
            else if response.code == StatusCode.NotFound then Right(None)
            else if response.code == StatusCode.Unauthorized || response.code == StatusCode.Forbidden then
              Left(InfrastructureError.AuthFailed(UpstreamName))
            else Left(errorFrom(response.code, response.body, rejectedField, request.method))
          }
          .recover {
            // The resilient backend carries its typed error inside this one exception rather than losing
            // it in a message. Anything else is genuinely unexpected and is reported as an upstream that
            // did not produce a response, which is the honest description.
            case UpstreamFailure(error) => Left(error)
            case failure: Exception =>
              Left(InfrastructureError.Unreachable(UpstreamName, UpstreamClient.safeFailureCause(failure)))
          }
    }

  // -----------------------------------------------------------------------------------------------
  // Reading what came back
  // -----------------------------------------------------------------------------------------------

  private def decode[A: Decoder](body: String, expected: String): Either[KuiError, A] =
    parser.decode[A](body).left.map(_ => malformed(s"it is not $expected"))

  private def parseSchema(body: String, subject: Subject): Either[KuiError, RegisteredSchema] =
    parser.parse(body).left.map(_ => malformed("it is not JSON")).flatMap { json =>
      val cursor = json.hcursor
      for {
        definition <- cursor
          .get[String]("schema")
          .left
          .map(_ => malformed("it has no 'schema' field holding the schema text"))
        version <- cursor
          .get[Int]("version")
          .left
          .map(_ => malformed("it has no 'version' field"))
          .flatMap(SchemaVersion.from(_).left.map(error => malformed(error.message)))
        id <- cursor
          .get[Int]("id")
          .left
          .map(_ => malformed("it has no 'id' field"))
          .flatMap(SchemaId.from(_).left.map(error => malformed(error.message)))
        // References are optional, and a registry that omits them is not broken — most schemas have none.
        // A malformed reference list is treated as none rather than failing the whole schema: the schema
        // text is what the panel is for, and refusing to show it because a dependency list was odd would
        // be a worse answer than showing it with an empty list.
        references = cursor.get[List[ReferenceJson]]("references").toOption.getOrElse(Nil)
      } yield RegisteredSchema(
        subject = cursor.get[String]("subject").toOption.map(Subject.unsafe).getOrElse(subject),
        version = version,
        id = id,
        format = SchemaFormat.fromRegistry(cursor.get[String]("schemaType").toOption),
        definition = definition,
        references = references.flatMap(_.toDomain)
      )
    }

  /** `{"compatibilityLevel": "BACKWARD"}`, with the two spellings registries actually send.
    *
    * `None` when the object is there and names no level, which some registries do for a subject whose
    * override has been deleted.
    */
  private def parseLevel(body: String): Either[KuiError, Option[CompatibilityLevel]] =
    parser.parse(body).left.map(_ => malformed("it is not JSON")).flatMap { json =>
      val cursor = json.hcursor
      cursor
        .get[String]("compatibilityLevel")
        .orElse(cursor.get[String]("compatibility"))
        .toOption match {
        case None => Right(None)
        case Some(raw) =>
          CompatibilityLevel
            .fromWire(raw)
            .toRight(
              malformed(
                s"it names the compatibility level '$raw', which is not one of " +
                  CompatibilityLevel.values.map(_.wire).mkString(", ")
              )
            )
            .map(Some(_))
      }
    }

  private def parseVerdict(body: String): Either[KuiError, CompatibilityVerdict] =
    parser.parse(body).left.map(_ => malformed("it is not JSON")).flatMap { json =>
      val cursor = json.hcursor
      cursor
        .get[Boolean]("is_compatible")
        .left
        .map(_ => malformed("it has no 'is_compatible' field"))
        .map(compatible =>
          CompatibilityVerdict(compatible, cursor.get[List[String]]("messages").toOption.getOrElse(Nil))
        )
    }

  private def levelBody(level: CompatibilityLevel): Json =
    Json.obj("compatibility" -> Json.fromString(level.wire))

  private def schemaBody(proposed: ProposedSchema): Json =
    Json.obj(
      "schema" -> Json.fromString(proposed.definition),
      "schemaType" -> Json.fromString(proposed.format.label),
      "references" -> Json.arr(
        proposed.references.map(reference =>
          Json.obj(
            "name" -> Json.fromString(reference.name),
            "subject" -> Json.fromString(reference.subject.value),
            "version" -> Json.fromInt(reference.version.value)
          )
        )*
      )
    )

  /** A non-404 failure status, with the registry's own error code and message when it sent one.
    *
    * ==Which statuses become `KUI-VALIDATION`, and on which calls==
    *
    * `400` and `422` say the registry read what KUI sent and would not have it. `422` with `error_code` 42203
    * is the one worth naming: it is what a registry answers when the compatibility level in a `PUT /config`
    * is not one it knows, and turning it into a `KUI-VALIDATION` failure puts the message beside the field
    * rather than in a red banner about an upstream.
    *
    * `409` is the registration's own refusal — "incompatible with an earlier schema for this subject" — and
    * it belongs in the same branch for the same reason. It is folded in **only for a call that sent a body**,
    * which here means every method except `GET`. A `409` answering `GET /subjects`, `GET …/versions`,
    * `GET …/versions/{v}` or `GET /config` cannot be about anything the caller typed, because those calls
    * carry nothing; reporting one as a `400 KUI-VALIDATION` would put a form error on a screen that has no
    * form and hide a registry mid-election, mid-migration or behind a confused proxy. Those arrive as
    * `KUI-UPSTREAM-UNAVAILABLE`, which is what a caller can act on.
    *
    * ==What `details` carries==
    *
    * One [[kui.kernel.error.FieldError]] when the registry sent a `message`, and an empty array when it sent
    * nothing usable. Its `field` is `rejectedField` — `definition` on a registration, and `null` on the two
    * `PUT /config` calls, because a level that the registry rejected is the whole request rather than one
    * input of a form. A `null` field is the shape ADR-034 gives "this refusal is about the request", and a
    * browser reading `details[0]` has to keep working when it is absent.
    */
  private def errorFrom(
      status: StatusCode,
      body: String,
      rejectedField: Option[String],
      method: Method
  ): KuiError = {
    val detail = parser
      .parse(body)
      .toOption
      .flatMap(_.hcursor.get[String]("message").toOption)
      .map(_.trim)
      .filter(_.nonEmpty)

    if status == StatusCode.UnprocessableEntity || status == StatusCode.BadRequest ||
      (status == StatusCode.Conflict && method != Method.GET)
    then
      ApplicationError.Invalid(
        detail.fold(s"the schema registry refused the request (HTTP ${status.code})")(message =>
          s"the schema registry refused the request: $message"
        ),
        // The registry's own sentence, in `details` as well as in the message, because that is where a form
        // reads the text it puts beside the field somebody typed. ADR-034 allows this and not the body it
        // came in: one field the registry writes for humans, and nothing else it sent.
        detail.toList.map(message => FieldError(rejectedField, List(message)))
      )
    else
      detail match {
        case None => InfrastructureError.Upstream(UpstreamName, status.code)
        case Some(message) =>
          InfrastructureError.Remote(
            ErrorCode.UpstreamUnavailable,
            s"the schema registry answered ${status.code}: $message",
            Nil
          )
      }
  }

  private def malformed(why: String): KuiError =
    InfrastructureError.Remote(
      ErrorCode.UpstreamUnavailable,
      s"the schema registry's answer could not be understood: $why",
      Nil
    )

  /** A 404 where a registry must answer something.
    *
    * Separate from [[malformed]] so that the message can say the thing an operator can act on: the address
    * they configured is answering, and it is not a Schema Registry.
    */
  private def notARegistry(what: String): KuiError =
    InfrastructureError.Remote(
      ErrorCode.UpstreamUnavailable,
      s"the configured address does not look like a Schema Registry: $what",
      Nil
    )
}

object RegistryHttp {

  /** The name this upstream is known by in metrics, spans and errors. A name, never a URL. */
  val UpstreamName: String = "schema-registry"

  /** The request field a registration refusal belongs beside: the schema text the operator pasted.
    *
    * It is the browser's field name and not the registry's, because the `details` array is read by a form
    * that has to decide which of its inputs to mark. `RegisterSchemaRequest.definition` is that input.
    */
  val RegisteredField: Option[String] = Some("definition")

  /** What KUI sends as `Content-Type`: the documented vendor type. */
  val VendorMediaType: MediaType =
    MediaType.unsafeParse("application/vnd.schemaregistry.v1+json")

  /** What KUI accepts: the vendor type first, then plain JSON, because several non-Confluent registries only
    * ever produce the latter and a strict `Accept` gets a 406 from them.
    */
  val AcceptHeader: String =
    "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json"

  /** One entry of a schema's `references` array, exactly as the registry sends it. */
  final private[infrastructure] case class ReferenceJson(name: String, subject: String, version: Int) {

    /** `None` for an entry KUI cannot make sense of, which is dropped rather than failing the schema. */
    def toDomain: Option[SchemaReference] =
      SchemaVersion
        .from(version)
        .toOption
        .map(valid => SchemaReference(name, Subject.unsafe(subject), valid))
  }

  private[infrastructure] object ReferenceJson {
    given Decoder[ReferenceJson] = (cursor: HCursor) =>
      for {
        name <- cursor.get[String]("name")
        subject <- cursor.get[String]("subject")
        version <- cursor.get[Int]("version")
      } yield ReferenceJson(name, subject, version)
  }
}
