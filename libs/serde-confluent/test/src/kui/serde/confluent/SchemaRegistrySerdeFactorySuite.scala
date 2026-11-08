package kui.serde.confluent

import scala.concurrent.duration.*

import cats.data.NonEmptyList
import cats.effect.testkit.TestControl
import cats.effect.{IO, Ref}
import sttp.client4.Backend
import sttp.client4.impl.cats.implicits.*
import sttp.client4.testing.{BackendStub, ResponseStub, StubBody}
import sttp.model.StatusCode

import kui.cache.CacheMetrics
import kui.config.{SafeUrl, UrlPolicy}
import kui.kernel.{ClusterId, ServiceId, TopicName}
import kui.observability.Telemetry
import kui.serde.{ClusterSerdes, SerdeName, SerdeProfile, SerdeUse, Target}
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** The seam itself: what `ClusterSerdes` gets when the registry is up, and what it gets when it is not.
  *
  * This is the suite that matters most, because the failure it guards against is silent. A factory that threw
  * on an unreachable registry would take a cluster down at startup; one that returned nothing would leave an
  * operator who configured Avro staring at a picker with no Avro in it and no way to tell a typo from an
  * outage (ADR-032). Neither of those looks like a bug until somebody is on call.
  */
final class SchemaRegistrySerdeFactorySuite extends KuiIOSuite {

  private val cluster: ClusterId = ClusterId.unsafe("prod-eu")
  private val topic: TopicName = TopicName.unsafe("orders-v2")
  private val profile: SerdeProfile = SerdeProfile.unconfigured(cluster, version = 1L)

  private def url(raw: String): SafeUrl =
    SafeUrl.from(raw, UrlPolicy.Dev).fold(error => fail(error.message), identity)

  // `UrlPolicy.Dev` for the same reason the stub listens on loopback: `Strict` refuses `localhost`
  // outright, so a strict config would fail on the address before any of this file's subject matter -
  // the probe, the disabled row, the refusal - had a chance to happen.
  private val config: SchemaRegistryConfig =
    SchemaRegistryConfig(urls = NonEmptyList.one(url("http://localhost:8081")), urlPolicy = UrlPolicy.Dev)

  private def factory(backend: Backend[IO]) =
    for {
      logger <- FakeStructuredLogger[IO]
    } yield SchemaRegistrySerdeFactory[IO](
      config,
      backend,
      Telemetry.noop[IO],
      ServiceId.unsafe("message"),
      logger,
      CacheMetrics.noop[IO]
    )

  private def answering(status: StatusCode, body: String): Backend[IO] =
    BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest
      .thenRespondF(_ => IO.pure(ResponseStub.adjust(body, status): sttp.client4.Response[StubBody]))

  private val refusing: Backend[IO] =
    BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest
      .thenRespondF(_ =>
        IO.raiseError[sttp.client4.Response[StubBody]](new java.net.ConnectException("refused"))
      )

  test("a registry that answers the probe with a 404 is a working registry") {
    // 404 for a subject nobody registered is the *healthy* answer: the registry replied.
    factory(answering(StatusCode.NotFound, "")).flatMap { built =>
      built.create(profile).use(result => IO(assert(result.isRight, result)))
    }
  }

  test("a registry that cannot be reached yields a reason, not an exception and not an absence") {
    factory(refusing).flatMap { built =>
      built.create(profile).use {
        case Left(reason) => IO(assert(reason.contains("could not be reached"), reason))
        case Right(_) => IO(fail("a refused connection must not produce a working serde"))
      }
    }
  }

  test("a registry that rejects KUI's credentials also yields a reason") {
    factory(answering(StatusCode.Unauthorized, "")).flatMap { built =>
      built.create(profile).use {
        case Left(reason) => IO(assert(reason.nonEmpty, reason))
        case Right(_) => IO(fail("a rejected credential must not produce a working serde"))
      }
    }
  }

  test("an unreachable registry becomes a disabled picker row, not a missing one") {
    factory(refusing).flatMap { built =>
      ClusterSerdes
        .resource[IO](profile, List(built))
        .use(_.suggest(topic, Target.Value, SerdeUse.Deserialize))
        .map { rows =>
          val registryRow = rows.find(_.name == SerdeName.SchemaRegistry)
          assert(registryRow.isDefined, rows.map(_.name))
          assertEquals(registryRow.map(_.available), Some(false))
          assert(registryRow.flatMap(_.unavailableReason).exists(_.nonEmpty))
        }
    }
  }

  test("asking for the serde by name while its registry is down is KUI-SERDE-UNAVAILABLE, never Fallback") {
    // The live defect this module closes: before it existed, the picker offered `SchemaRegistry` and
    // resolution quietly handed back `Fallback`, so a user saw Base64 and no explanation.
    factory(refusing).flatMap { built =>
      ClusterSerdes
        .resource[IO](profile, List(built))
        .use(_.resolve(topic, Target.Value, Some(SerdeName.SchemaRegistry)))
        .map {
          case Left(error) => assertEquals(error.code.wire, "KUI-SERDE-UNAVAILABLE")
          case Right(serde) => fail(s"expected a refusal, got ${serde.name}")
        }
    }
  }

  test("a registry that was down at startup is used once it answers") {
    // Review R2 finding h-serde-untrusted-decode-3. The probe used to run once, inside `create`, and a 503
    // at that moment left SchemaRegistry out of `all` until the process was restarted. A later 404 — the
    // healthy answer for the probe subject — has to make `resolve` a working serde, and that serde has to
    // decode a framed record.
    val schemaId = 7
    val schema = AvroPayload.parse("\"string\"").fold(why => fail(why), identity)
    val body = AvroPayload.encode(schema, "\"hello\"").fold(why => fail(why), identity)
    val framed = WireFormat.frame(schemaId, body)
    val schemaDocument = """{"schema":"\"string\"","schemaType":"AVRO"}"""

    val program = for {
      probes <- Ref.of[IO, Int](0)
      backend = BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest.thenRespondF { request =>
        val path = "/" + request.uri.path.mkString("/")
        if path.startsWith("/schemas/ids/") then
          IO.pure(ResponseStub.adjust(schemaDocument, StatusCode.Ok): sttp.client4.Response[StubBody])
        else
          probes.modify { n =>
            val status = if n == 0 then StatusCode.ServiceUnavailable else StatusCode.NotFound
            (n + 1, ResponseStub.adjust("", status): sttp.client4.Response[StubBody])
          }
      }
      built <- factory(backend)
      outcome <- ClusterSerdes.resource[IO](profile, List(built)).use { serdes =>
        for {
          before <- serdes.resolve(topic, Target.Value, Some(SerdeName.SchemaRegistry))
          _ <- IO.sleep(SchemaRegistrySerdeFactory.RecheckAfter + 1.millis)
          after <- serdes.resolve(topic, Target.Value, Some(SerdeName.SchemaRegistry))
          decoded <- after match {
            case Left(error) => IO.pure(Left(error.message))
            case Right(serde) =>
              serde
                .deserializer(topic, Target.Value)
                .flatMap(_.deserialize(Nil, framed))
                .map(_.left.map(_.cause).map(_.text))
          }
        } yield (before, decoded)
      }
    } yield outcome

    TestControl.executeEmbed(program).map { (before, decoded) =>
      assertEquals(before.left.map(_.code.wire), Left("KUI-SERDE-UNAVAILABLE"))
      assertEquals(decoded, Right("\"hello\""))
    }
  }
}
