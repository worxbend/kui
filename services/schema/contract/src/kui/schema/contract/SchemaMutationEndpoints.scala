package kui.schema.contract

import sttp.tapir.*
import sttp.tapir.json.circe.jsonBody

import kui.contracts.KernelSchemas.given
import kui.contracts.rbac.{EndpointAuthorization, ResourceRequirement}
import kui.contracts.{ErrorEnvelope, KuiEndpoint}
import kui.kernel.{ClusterId, Subject}
import kui.schema.contract.dto.*
import kui.schema.contract.dto.CompatibilityCheckDto.given
import kui.schema.contract.dto.CompatibilityCheckRequest.given
import kui.schema.contract.dto.CompatibilityDto.given
import kui.schema.contract.dto.RegisterSchemaRequest.given
import kui.schema.contract.dto.RegisteredVersionDto.given
import kui.schema.contract.dto.SetCompatibilityRequest.given
import kui.security.SignedPrincipal
import kui.security.rbac.{Action, Resource}

/** The endpoints that carry a request body: three that change something, and one that changes nothing.
  *
  * ==Why the compatibility check is in this file and is not a mutation==
  *
  * `checkCompatibility` posts a schema and gets a verdict. Nothing is registered, nothing is stored, and
  * running it twice does exactly what running it once does — so it carries **no** `MutationKey` marker, is
  * not enumerated by the read-only policy, and is answered on a read-only cluster like any other read.
  *
  * That asymmetry is deliberate and is the opposite of the choice the consumer service's reset *plan* makes.
  * A reset plan is refused on a read-only cluster because it is the first step of a wizard whose last step
  * cannot be allowed, and letting somebody compose a change they may not apply teaches them the refusal is a
  * bug. A compatibility check is not the first step of anything KUI does: it answers "would this schema be
  * accepted", which is a question a read-only operator is entitled to an answer to, and refusing it would
  * push them to ask the registry directly instead.
  *
  * It lives here because it has a body, and a bodied endpoint is verified differently (ADR-020 Amendment 1) —
  * which is a property of the request shape, not of whether it mutates.
  *
  * ==The three writes==
  *
  * All three carry `KuiEndpoint.MutationKey` and the CSRF header, and all three are marked
  * `destructive = false`. Setting a compatibility level loses no data and can be set back; registering a
  * schema adds a version and removes nothing, and the registry's own compatibility check is what stands
  * between a registration and a subject its consumers can no longer read. They are still mutations with real
  * consequences — lowering a level to `NONE` removes that check for every subject following it — which is why
  * each is refused on a read-only cluster.
  *
  * `registerVersion` is a POST onto the same path `versions` is a GET on. That is one path with two methods
  * in the published document rather than a new path, which is the shape the two topic collections already
  * have.
  */
object SchemaMutationEndpoints {

  val ClustersSegment: String = SchemaEndpoints.ClustersSegment
  val SchemasSegment: String = SchemaEndpoints.SchemasSegment
  val SubjectsSegment: String = SchemaEndpoints.SubjectsSegment
  val VersionsSegment: String = SchemaEndpoints.VersionsSegment
  val CompatibilitySegment: String = SchemaEndpoints.CompatibilitySegment

  /** The operation names. They are the same strings as the audit vocabulary's `MutationKind.operation`, and
    * the api module's suite — the only place that can see both a contract and an application type — asserts
    * that they are.
    */
  val SetGlobalCompatibilityOperation: String = "schema.compatibility.global.set"
  val SetSubjectCompatibilityOperation: String = "schema.compatibility.subject.set"

  /** Registration's operation name, also used by the structured mutation audit record. */
  val RegisterVersionOperation: String = "schema.subject.version.register"

  private val clustersBase = "internal" / "v1" / ClustersSegment

  private val clusterIdPath: EndpointInput[ClusterId] =
    path[ClusterId](SchemaEndpoints.ClusterIdParam).description("The configured cluster's slug id")

  private val subjectPath: EndpointInput[Subject] =
    path[Subject](SchemaEndpoints.SubjectParam).description("The subject, as the registry knows it")

  private val versionPath: EndpointInput[String] =
    path[String](SchemaEndpoints.VersionParam)
      .description(
        s"The version to check against: a number, or '${SchemaEndpoints.LatestVersion}' for the current one"
      )

  /** Set the level every subject without its own follows. */
  val setGlobalCompatibility: Endpoint[
    SignedPrincipal,
    (String, ClusterId, SetCompatibilityRequest),
    ErrorEnvelope,
    CompatibilityDto,
    Any
  ] =
    KuiEndpoint
      .mutation(SetGlobalCompatibilityOperation, destructive = false)
      .put
      .in(clustersBase / clusterIdPath / SchemasSegment / CompatibilitySegment)
      .in(jsonBody[SetCompatibilityRequest])
      .out(jsonBody[CompatibilityDto])
      .attribute(
        EndpointAuthorization.Key,
        EndpointAuthorization.one(
          "schema.compatibility.global.set",
          ResourceRequirement.unnamed(Resource.Schema, Action.SchemaModifyGlobalCompatibility)
        )
      )
      .name("schema.compatibility.global.set")
      .summary("Set the registry-wide compatibility level")
      .description(
        KuiEndpoint.mutationNote(SetGlobalCompatibilityOperation, destructive = false) +
          "It applies to every subject that has no level of its own, so lowering it to NONE removes the " +
          "compatibility check for all of them at once. Refused on a read-only cluster, and audited either " +
          "way with the level that was in force before the change."
      )
      .tag("schema")

  /** Set one subject's own level, overriding the global one from now on. */
  val setSubjectCompatibility: Endpoint[
    SignedPrincipal,
    (String, ClusterId, Subject, SetCompatibilityRequest),
    ErrorEnvelope,
    CompatibilityDto,
    Any
  ] =
    KuiEndpoint
      .mutation(SetSubjectCompatibilityOperation, destructive = false)
      .put
      .in(
        clustersBase / clusterIdPath / SchemasSegment / SubjectsSegment / subjectPath / CompatibilitySegment
      )
      .in(jsonBody[SetCompatibilityRequest])
      .out(jsonBody[CompatibilityDto])
      .attribute(
        EndpointAuthorization.Key,
        EndpointAuthorization.one(
          "schema.compatibility.subject.set",
          ResourceRequirement.named(Resource.Schema, SchemaEndpoints.SubjectParam, Action.SchemaEdit)
        )
      )
      .name("schema.compatibility.subject.set")
      .summary("Set one subject's compatibility level")
      .description(
        KuiEndpoint.mutationNote(SetSubjectCompatibilityOperation, destructive = false) +
          "The subject stops following the global level from this point on, which the answer reports as " +
          "inheritedFromGlobal: false. Refused on a read-only cluster."
      )
      .tag("schema")

  /** Register a schema under a subject, creating the subject if it is new.
    *
    * The one endpoint in this service that adds to a registry's contents. It is a POST onto the version
    * *collection* — the same path `SchemaEndpoints.versions` reads — because that is what it appends to, and
    * because it is also the registry's own shape.
    *
    * `Action.SchemaCreate` was declared with the rest of the vocabulary and used by nothing until now. It is
    * altering, so ADR-047's read-only rule refuses it on a read-only cluster without a second rule being
    * written, and `RbacLawsSuite` already covers its implication of `SchemaView`.
    */
  val registerVersion: Endpoint[
    SignedPrincipal,
    (String, ClusterId, Subject, RegisterSchemaRequest),
    ErrorEnvelope,
    RegisteredVersionDto,
    Any
  ] =
    KuiEndpoint
      .mutation(RegisterVersionOperation, destructive = false)
      .post
      .in(clustersBase / clusterIdPath / SchemasSegment / SubjectsSegment / subjectPath / VersionsSegment)
      .in(jsonBody[RegisterSchemaRequest])
      .out(jsonBody[RegisteredVersionDto])
      .attribute(
        EndpointAuthorization.Key,
        EndpointAuthorization.one(
          RegisterVersionOperation,
          ResourceRequirement.named(Resource.Schema, SchemaEndpoints.SubjectParam, Action.SchemaCreate)
        )
      )
      .name(RegisterVersionOperation)
      .summary("Register a schema under this subject")
      .description(
        // Deliberately not `KuiEndpoint.mutationNote`. Its non-destructive text is "This call changes
        // nothing", which is true of a compatibility level that can be set back and false of a
        // registration: it adds a version, and a registry has no way to remove one except a soft delete.
        // The operation name is spelled the same way so the generated reference still reads as one family.
        s"Mutation ($RegisterVersionOperation). This call adds a version to the subject and cannot be " +
          "undone from KUI. " +
          "The registry decides whether the schema is accepted; a rejection comes back as 400 " +
          "KUI-VALIDATION carrying the registry's own explanation in details[0], because that sentence " +
          "names the field that broke the rule and is the only part of the answer anybody can act on. " +
          "Registering a schema that is already registered under this subject is what the registry calls " +
          "idempotent: it answers the existing id and version and adds no version. The answer's version is " +
          "absent when the registry stored the schema and then would not say which version it became, " +
          "which is a registration that succeeded and a number KUI does not know."
      )
      .tag("schema")

  /** Would the registry accept this schema for this subject? Changes nothing. */
  val checkCompatibility: Endpoint[
    SignedPrincipal,
    (ClusterId, Subject, String, CompatibilityCheckRequest),
    ErrorEnvelope,
    CompatibilityCheckDto,
    Any
  ] =
    KuiEndpoint.internal.post
      .in(
        clustersBase / clusterIdPath / SchemasSegment / SubjectsSegment / subjectPath /
          VersionsSegment / versionPath / CompatibilitySegment
      )
      .in(jsonBody[CompatibilityCheckRequest])
      .out(jsonBody[CompatibilityCheckDto])
      .attribute(
        EndpointAuthorization.Key,
        EndpointAuthorization.one(
          "schema.compatibility.check",
          ResourceRequirement.named(Resource.Schema, SchemaEndpoints.SubjectParam, Action.SchemaView)
        )
      )
      .name("schema.compatibility.check")
      .summary("Whether a proposed schema would be accepted for this subject")
      .description(
        "Registers nothing. The check runs inside the registry, because the verdict that matters is the " +
          "one the registry will give when the schema is really registered; KUI does not reimplement the " +
          "compatibility rules of three schema languages. Answered on a read-only cluster like any other " +
          "read."
      )
      .tag("schema")

  /** Every bodied endpoint this service serves.
    *
    * The three writes and the check are one list because the gateway proxies them identically — it rewrites
    * the prefix and forwards the inputs, and the mutation marker is read by policy rather than by routing.
    * `ServiceContracts.byService` reads this value, so a route for the registration appears at the gateway
    * with no gateway change at all.
    */
  val all: List[AnyEndpoint] =
    List(setGlobalCompatibility, setSubjectCompatibility, registerVersion, checkCompatibility)
}
