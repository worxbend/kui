package kui.schema.api

import cats.effect.IO

import kui.contracts.capability.CapabilityState
import kui.contracts.rbac.EndpointAuthorization
import kui.kernel.error.InfrastructureError
import kui.kernel.{ClusterId, SchemaId, Subject}
import kui.schema.application.{ClusterRegistries, RegisterSchemaUseCase, RegistryProfile}
import kui.schema.contract.{SchemaEndpoints, SchemaMutationEndpoints}
import kui.schema.domain.*
import kui.security.audit.MutationKind
import kui.security.rbac.Action
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** The capability report, which is how a deployment with no Schema Registry stays usable.
  *
  * The distinction these tests defend is the whole reason this service is written the way it is: "no registry
  * configured" hides the feature, "the registry is down" shows it degraded with a reason, and the two must
  * never be reported as the same thing. Reporting the first as the second puts a permanently red panel on a
  * deployment where nothing is wrong — which is how people learn to ignore red panels.
  */
final class SchemaCapabilitiesSuite extends KuiIOSuite {

  private val configured = ClusterId.unsafe("has-registry")
  private val bare = ClusterId.unsafe("no-registry")

  /** A port that either answers or refuses, and does nothing else. */
  private def port(failure: Option[kui.kernel.error.KuiError]): SchemaRegistryPort[IO] =
    new SchemaRegistryPort[IO] {
      private def answer[A](value: A) = IO.pure(failure.toLeft(value))
      def subjects = answer(List.empty[Subject])
      def summary(subject: Subject) = answer(Option.empty[SubjectSummary])
      def versions(subject: Subject) = answer(None)
      def schema(subject: Subject, version: VersionSelector) = answer(None)
      def globalCompatibility = answer(CompatibilityLevel.Backward)
      def subjectCompatibility(subject: Subject) = answer(None)
      def register(subject: Subject, proposed: ProposedSchema) =
        answer(RegisteredVersion(subject, SchemaId.unsafe(1), Some(SchemaVersion.unsafe(1))))
      def setGlobalCompatibility(level: CompatibilityLevel) = answer(())
      def setSubjectCompatibility(subject: Subject, level: CompatibilityLevel) = answer(())
      def checkCompatibility(subject: Subject, version: VersionSelector, proposed: ProposedSchema) =
        answer(None)
    }

  private def registries(ports: Map[ClusterId, SchemaRegistryPort[IO]]): ClusterRegistries[IO] =
    new ClusterRegistries[IO] {
      val profiles = List(
        RegistryProfile(configured, "Has a registry", hasRegistry = true, readOnly = false),
        RegistryProfile(bare, "Has none", hasRegistry = false, readOnly = false)
      )
      def all = IO.pure(profiles)
      def profile(cluster: ClusterId) = IO.pure(profiles.find(_.cluster == cluster))
      def registry(cluster: ClusterId) = IO.pure(ports.get(cluster))
    }

  private def report(ports: Map[ClusterId, SchemaRegistryPort[IO]]) =
    FakeStructuredLogger[IO].flatMap(logger => SchemaCapabilities.make[IO](registries(ports), logger).report)

  test("a cluster with no registry is not_configured, not degraded, and says which key to set") {
    report(Map(configured -> port(None))).map { capabilities =>
      val row = capabilities(bare)

      assertEquals(row.configured, false)
      assertEquals(row.status, CapabilityState.NotConfigured.status)
      assert(clue(row.reason).exists(_.contains("schemaRegistry.url")))
    }
  }

  test("a reachable registry is available and carries the cluster's display name") {
    report(Map(configured -> port(None))).map { capabilities =>
      val row = capabilities(configured)

      assertEquals(row.status, CapabilityState.Available.status)
      assertEquals(row.configured, true)
      assertEquals(row.name, Some("Has a registry"))
    }
  }

  test("a configured registry that does not answer is degraded and stays configured") {
    val down = InfrastructureError.Unreachable("schema-registry", "connection refused")

    report(Map(configured -> port(Some(down)))).map { capabilities =>
      val row = capabilities(configured)

      // `configured = true` is the load-bearing half: it is what stops the gateway folding this into
      // NotConfigured and hiding a feature that exists and is merely broken.
      assertEquals(row.configured, true)
      assertEquals(row.status, "degraded")
      // The reason is the error's own display message, which names the upstream and deliberately not
      // the cause: `InfrastructureError.Unreachable` drops the connection failure's text because that
      // text routinely contains a URL with a password in it. The screen therefore says which upstream
      // is unreachable, and the detail stays in the log.
      assert(clue(row.reason).exists(_.contains("schema-registry")))
    }
  }

  test("every configured cluster appears, so a registry outage never removes a row from the switcher") {
    report(Map(configured -> port(None))).map(capabilities =>
      assertEquals(capabilities.keySet, Set(configured, bare))
    )
  }

  test("a capability report that fails answers an empty document rather than a 500") {
    // The gateway's registry needs an answer most exactly when things are wrong: a capability document
    // that fails takes every cluster's row down with it, and the browser then cannot tell "the schema
    // service is unreachable" from "this cluster has no registry". `SchemaApi.capabilityDocument`'s
    // recovery can be deleted with every other case in this service green.
    val exploding = new ClusterRegistries[IO] {
      def all = IO.raiseError(new RuntimeException("the cluster list could not be built"))
      def profile(cluster: ClusterId) = IO.pure(None)
      def registry(cluster: ClusterId) = IO.pure(None)
    }

    for {
      logger <- FakeStructuredLogger[IO]
      document <- SchemaApi.capabilityDocument[IO](SchemaCapabilities.make[IO](exploding, logger), logger)
      entries <- logger.entries
    } yield {
      assertEquals(document.service, SchemaApi.Id)
      assertEquals(document.clusters, Map.empty)
      // And it says so, because a document reporting no clusters and a deployment with no clusters look
      // identical from the outside and only one of them is a failure.
      assertEquals(entries.map(_.level), List("error"))
    }
  }

  test("a port that throws is reported as degraded rather than failing the whole report") {
    val throwing = new SchemaRegistryPort[IO] {
      private def boom[A]: IO[Either[kui.kernel.error.KuiError, A]] =
        IO.raiseError(new RuntimeException("a defect below the port"))
      def subjects = boom
      def summary(subject: Subject) = boom
      def versions(subject: Subject) = boom
      def schema(subject: Subject, version: VersionSelector) = boom
      def globalCompatibility = boom
      def subjectCompatibility(subject: Subject) = boom
      def register(subject: Subject, proposed: ProposedSchema) = boom
      def setGlobalCompatibility(level: CompatibilityLevel) = boom
      def setSubjectCompatibility(subject: Subject, level: CompatibilityLevel) = boom
      def checkCompatibility(subject: Subject, version: VersionSelector, proposed: ProposedSchema) = boom
    }

    report(Map(configured -> throwing)).map { capabilities =>
      assertEquals(capabilities(configured).status, "degraded")
      // The other cluster's row survives, which is the point: one broken registry must not take the
      // report down at the moment the browser needs it most.
      assertEquals(capabilities(bare).status, CapabilityState.NotConfigured.status)
    }
  }
}

/** The seam between the contract and the audit vocabulary, which no other module can see at once. */
final class SchemaEndpointClassificationSuite extends munit.FunSuite {

  test("every endpoint this service publishes is classified as a mutation or explicitly as a read") {
    val published = SchemaEndpoints.all ++ SchemaMutationEndpoints.all

    val mutations = published.filter(kui.contracts.KuiEndpoint.isMutation)
    val reads = published.filterNot(kui.contracts.KuiEndpoint.isMutation)

    assertEquals(
      mutations.flatMap(_.info.name).toSet,
      Set(
        "schema.compatibility.global.set",
        "schema.compatibility.subject.set",
        "schema.subject.version.register"
      )
    )

    // The compatibility check carries a body and is deliberately *not* a mutation: it registers nothing.
    // If that ever changes, this assertion is what says so.
    assert(reads.flatMap(_.info.name).contains("schema.compatibility.check"))
  }

  test("the contract's operation names are the audit vocabulary's, exactly") {
    val fromContract = Set(
      SchemaMutationEndpoints.SetGlobalCompatibilityOperation,
      SchemaMutationEndpoints.SetSubjectCompatibilityOperation
    )

    val fromAudit =
      Set(MutationKind.SetGlobalCompatibility.operation, MutationKind.SetSubjectCompatibility.operation)

    // Two spellings of one operation is how an audit trail comes to have two vocabularies, and this is
    // the only place in the build that can see both.
    assertEquals(fromContract, fromAudit)
  }

  test("the registration operation matches the structured audit vocabulary") {
    // The endpoint and its audit record must name the same mutation.
    assertEquals(
      SchemaMutationEndpoints.RegisterVersionOperation,
      RegisterSchemaUseCase.Operation
    )

    assert(
      MutationKind.values.map(_.operation).contains(SchemaMutationEndpoints.RegisterVersionOperation),
      "schema registration must have a structured audit kind"
    )
  }

  test("every mutating endpoint declares itself non-destructive: a level can be set back") {
    val markers = SchemaMutationEndpoints.all.flatMap(_.attribute(kui.contracts.KuiEndpoint.MutationKey))

    assertEquals(markers.map(_.destructive).toSet, Set(false))
  }

  test("every published endpoint declares the action it needs, and no write needs only a view") {
    // The declaration *is* the rule: the gateway and this service are two enforcement points over one
    // value, and `EndpointDecision.decide` reads it. `Action.SchemaModifyGlobalCompatibility` on the
    // registry-wide write and `Action.SchemaEdit` on the subject one could each be replaced with
    // `Action.SchemaView` with every case in this service green — and a role granted read access to
    // schemas would then be able to set the whole registry's compatibility level to NONE.
    def actionsOf(endpoint: sttp.tapir.AnyEndpoint): Set[Action] =
      EndpointAuthorization
        .of(endpoint)
        .toSet
        .flatMap(_.requirements.flatMap(_.actions.toList).toSet)

    val declared = (SchemaEndpoints.all ++ SchemaMutationEndpoints.all)
      .flatMap(endpoint => endpoint.info.name.map(_ -> actionsOf(endpoint)))
      .toMap

    assertEquals(
      declared.get("schema.compatibility.global.set"),
      Some(Set[Action](Action.SchemaModifyGlobalCompatibility))
    )
    assertEquals(declared.get("schema.compatibility.subject.set"), Some(Set[Action](Action.SchemaEdit)))
    assertEquals(declared.get("schema.subject.version.register"), Some(Set[Action](Action.SchemaCreate)))

    // And the reads, so that "no write needs only a view" is measured against something rather than
    // asserted: the three reads that declare a requirement declare exactly the viewing one.
    assertEquals(declared.get("schema.versions"), Some(Set[Action](Action.SchemaView)))
    assertEquals(declared.get("schema.version"), Some(Set[Action](Action.SchemaView)))
    assertEquals(declared.get("schema.compatibility.check"), Some(Set[Action](Action.SchemaView)))

    val writeActions = List(
      "schema.compatibility.global.set",
      "schema.compatibility.subject.set",
      "schema.subject.version.register"
    ).flatMap(declared.getOrElse(_, Set.empty[Action]))

    assert(
      !writeActions.contains(Action.SchemaView),
      "a write is declared as needing nothing more than read access to schemas"
    )
  }
}
