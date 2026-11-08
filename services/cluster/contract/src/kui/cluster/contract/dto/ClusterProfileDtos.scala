package kui.cluster.contract.dto

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant

import io.circe.syntax.*
import io.circe.{Codec, HCursor, Json}
import sttp.tapir.Schema

import kui.cluster.contract.dto.ClusterConnectionCodecs.given
import kui.contracts.ErrorEnvelope.given
import kui.contracts.KernelCodecs.given
import kui.contracts.KernelSchemas.given
import kui.kernel.ClusterId
import kui.kernel.cluster.{
  AdminTuning,
  BootstrapServers,
  ClientProperties,
  ClusterConnection,
  ClusterSecurity
}

/** A cluster's resolved connection settings, for another KUI service.
  *
  * ==Why this document carries credentials==
  *
  * It is served only on `/internal/v1`, only to a caller carrying a signed principal (ADR-020), and it is the
  * mechanism ADR-036 names for exactly this purpose: non-owner services "keep receiving the resolved
  * `ClusterProfile` over the internal contract", and "keystore bytes travel inside the signed inter-service
  * channel". ADR-046 is the decision, taken at M2's first consumer, and it closes `ARCHITECTURE.md` §14's
  * open question.
  *
  * The alternative, considered and rejected, was for every Kafka-facing service to become a metadata-store
  * client holding `kui.store.encryptionKey`. That would put the one key whose loss makes every stored secret
  * unrecoverable into four processes instead of one, and would give write-capable credentials for
  * `__kui_config` to three services that must never write it.
  *
  * Every credential inside `security` and `properties` is a `Secret`, so `toString`, log interpolation and
  * span attributes render `***`. The only encoder in KUI that can write one out is `ClusterConnectionCodecs`,
  * which this document uses and nothing on `/api/v1` does; `kui.cluster.api.SecretLeakSuite` asserts that as
  * a fact about every declared endpoint rather than as a convention.
  *
  * ==What is deliberately absent==
  *
  * Keystore *bytes* stored in `__kui_files` are not here yet. ADR-036 says they travel on this channel and
  * they will, but nothing in M2 configures such a keystore, and shipping a field no producer ever fills is
  * the permanently-`null` field CLAPI-001 already ruled against. `StoreSource.Inline` exists in the shape
  * above, so the field arrives with its first producer and not before.
  *
  * @param version
  *   the store version this profile was resolved at. It stays on the body because the header is an HTTP
  *   concern that an in-process client (the all-in-one build) does not have, and the consumer's rebuild
  *   decision must work identically in both deployment shapes. It is **not** the `ETag`: a static profile is
  *   always version 0, so a version tag would still match after this process restarted with different
  *   settings
  * @param updatedAt
  *   when the cluster service resolved it, for a consumer's own staleness reporting. It is not part of the
  *   `ETag` — the clock moves on every answer, and a validator that included it could never be `304`
  */
final case class ClusterProfileDto(
    id: ClusterId,
    name: String,
    version: Long,
    readOnly: Boolean,
    bootstrapServers: BootstrapServers,
    security: ClusterSecurity,
    properties: ClientProperties,
    admin: AdminTuning,
    updatedAt: Instant
)

object ClusterProfileDto {

  /** A strong `ETag` for this profile, quoted, never `W/`-prefixed.
    *
    * SHA-256 over the canonical document, truncated to sixteen bytes. It covers the version **and** the
    * resolved connection, credentials included, and nothing else: `updatedAt` is the clock at which this
    * process rendered the answer, so folding it in would make every response a new validator. A static
    * profile's version is always 0, including after a restart that changed its bootstrap or its password; the
    * bytes are what tell those two profiles apart.
    *
    * One function so that the header the server writes and the tag a client falls back to cannot be written
    * twice and drift. Comparison is byte equality after the quotes are trimmed.
    */
  def etagOf(dto: ClusterProfileDto): String = {
    val document = canonical(
      Json.obj(
        "admin" -> dto.admin.asJson,
        "bootstrapServers" -> dto.bootstrapServers.asJson,
        "id" -> dto.id.asJson,
        "name" -> dto.name.asJson,
        "properties" -> dto.properties.asJson,
        "readOnly" -> dto.readOnly.asJson,
        "security" -> dto.security.asJson,
        "version" -> dto.version.asJson
      )
    )
    val digest =
      MessageDigest.getInstance("SHA-256").digest(document.noSpaces.getBytes(StandardCharsets.UTF_8))

    digest.take(16).map(byte => f"${byte & 0xff}%02x").mkString("\"", "", "\"")
  }

  /** Object keys in one order, recursively, so a `Map` that happens to iterate differently still hashes the
    * same. Arrays keep their order: a bootstrap list is not a set.
    */
  private def canonical(json: Json): Json =
    json.arrayOrObject(
      json,
      array => Json.fromValues(array.map(canonical)),
      obj => Json.fromFields(obj.toList.sortBy(_._1).map { case (key, value) => key -> canonical(value) })
    )

  given Codec[ClusterProfileDto] = Codec.from(
    (cursor: HCursor) =>
      for {
        id <- cursor.get[ClusterId]("id")
        name <- cursor.get[String]("name")
        version <- cursor.get[Long]("version")
        readOnly <- cursor.getOrElse[Boolean]("readOnly")(false)
        bootstrapServers <- cursor.get[BootstrapServers]("bootstrapServers")
        security <- cursor.get[ClusterSecurity]("security")
        properties <- cursor.getOrElse[ClientProperties]("properties")(ClientProperties.empty)
        admin <- cursor.get[AdminTuning]("admin")
        updatedAt <- cursor.get[Instant]("updatedAt")
      } yield ClusterProfileDto(
        id,
        name,
        version,
        readOnly,
        bootstrapServers,
        security,
        properties,
        admin,
        updatedAt
      ),
    (dto: ClusterProfileDto) =>
      Json.obj(
        "id" -> dto.id.asJson,
        "name" -> dto.name.asJson,
        "version" -> dto.version.asJson,
        "readOnly" -> dto.readOnly.asJson,
        "bootstrapServers" -> dto.bootstrapServers.asJson,
        "security" -> dto.security.asJson,
        "properties" -> dto.properties.asJson,
        "admin" -> dto.admin.asJson,
        "updatedAt" -> dto.updatedAt.asJson
      )
  )

  given Schema[ClusterProfileDto] = Schema
    .derived[ClusterProfileDto]
    .description("A cluster's resolved connection settings, credentials included. Internal channel only")

  given CanEqual[ClusterProfileDto, ClusterProfileDto] = CanEqual.derived

  /** Everything a Kafka client is built from, in the one shape `libs/kafka` takes.
    *
    * It lives here, on the producing side's own type, so that the reconstruction happens once. A consumer
    * that assembled a `ClusterConnection` itself would be the second place the field order and the defaults
    * are decided, which is the shape of defect ADR-046 exists to prevent.
    */
  def connectionOf(dto: ClusterProfileDto): ClusterConnection =
    ClusterConnection(dto.id, dto.bootstrapServers, dto.security, dto.properties, dto.admin)
}

/** One change notification.
  *
  * It carries no profile on purpose. A consumer that sees a version it does not hold fetches the profile,
  * which keeps the stream to four fields, keeps credentials off every subscriber's socket, and means a
  * dropped frame costs one fetch rather than leaving a service talking to a cluster with stale settings.
  *
  * @param change
  *   `"updated"` or `"removed"`. A removal is a first-class event, never inferred from silence: a cluster an
  *   operator deleted must make every consumer drop its clients, and "I have heard nothing" is
  *   indistinguishable from a healthy quiet cluster. A string rather than an enum, so a consumer meeting a
  *   future kind renders it instead of failing to decode the frame
  */
final case class ClusterChangeDto(id: ClusterId, version: Long, change: String, at: Instant)

object ClusterChangeDto {

  val Updated: String = "updated"
  val Removed: String = "removed"

  given Codec[ClusterChangeDto] = Codec.from(
    (cursor: HCursor) =>
      for {
        id <- cursor.get[ClusterId]("id")
        version <- cursor.get[Long]("version")
        change <- cursor.get[String]("change")
        at <- cursor.get[Instant]("at")
      } yield ClusterChangeDto(id, version, change, at),
    (dto: ClusterChangeDto) =>
      Json.obj(
        "id" -> dto.id.asJson,
        "version" -> dto.version.asJson,
        "change" -> dto.change.asJson,
        "at" -> dto.at.asJson
      )
  )

  given Schema[ClusterChangeDto] =
    Schema.derived[ClusterChangeDto].description("A cluster's profile changed or the cluster was removed")

  given CanEqual[ClusterChangeDto, ClusterChangeDto] = CanEqual.derived
}

/** What `GET .../profile` answered with: the profile, or "you already have it".
  *
  * A type rather than an `Option[ClusterProfileDto]` because the two outcomes carry different HTTP status
  * codes and a 304 is not a missing profile — it is the strongest possible statement that the caller's copy
  * is current. Modelling it as an absence would make every consumer write the same three-line reconstruction.
  */
enum ProfileResult {

  /** 200: here is the profile, and here is the tag to send back next time. */
  case Current(etag: String, profile: ClusterProfileDto)

  /** 304: what you hold is current. No body, by definition of the status code. */
  case NotModified(etag: String)

  /** The tag, whichever outcome this is. Not called `etag`: that is already the name of both cases' field,
    * and a member of the same name on the enum itself would be an override of them.
    */
  def entityTag: String = this match {
    case Current(tag, _) => tag
    case NotModified(tag) => tag
  }
}

object ProfileResult {

  /** The only way a 200 should be built: the tag is derived from the profile, so the two cannot disagree. */
  def current(profile: ClusterProfileDto): ProfileResult =
    Current(ClusterProfileDto.etagOf(profile), profile)

  /** The only way a 304 should be built, for the same reason. */
  def notModified(etag: String): ProfileResult = NotModified(etag)

  /** Whether a caller's `If-None-Match` header means "I already have the profile this tag names".
    *
    * Quotes are trimmed before comparing, because a proxy may or may not preserve them, and `*` means "send
    * it regardless" — the wildcard is about preconditions on writes, and on a read the useful reading is
    * "unconditional". The comparison is against the content tag, not the version: a version-shaped tag such
    * as `"0"` is a previous process's validator and must miss.
    */
  def isCurrent(ifNoneMatch: Option[String], etag: String): Boolean =
    ifNoneMatch.map(_.trim) match {
      case None => false
      case Some("*") => false
      case Some(tag) =>
        val held = tag.stripPrefix("W/").replace("\"", "")
        held.nonEmpty && held == etag.stripPrefix("W/").replace("\"", "")
    }

  given CanEqual[ProfileResult, ProfileResult] = CanEqual.derived
}
