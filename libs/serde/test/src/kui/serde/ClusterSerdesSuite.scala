package kui.serde

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*

import kui.kernel.error.ErrorCode
import kui.kernel.{ClusterId, TopicName}
import kui.serde.SerdeResolution.{PatternRule, Rules}
import kui.testkit.KuiIOSuite

/** The picker's rows, the per-cluster lifetime, and what happens when a configured serde cannot work. */
final class ClusterSerdesSuite extends KuiIOSuite {

  private val cluster: ClusterId = ClusterId.unsafe("prod-eu")
  private val topic: TopicName = TopicName.unsafe("orders-v2")

  /** A stand-in for the Schema-Registry serde: a `Resource` whose release is observable, which either builds
    * or reports why it could not.
    */
  private def factory(
      built: Either[String, Unit],
      closed: Option[Ref[IO, Boolean]] = None
  ): SerdeFactory[IO] = new SerdeFactory[IO] {
    val name: SerdeName = SerdeName.SchemaRegistry
    val describe: SerdeDescription = SerdeDescription(name, "reads Avro through a Schema Registry", false)

    def create(profile: SerdeProfile): Resource[IO, Either[String, Serde[IO]]] =
      Resource
        .make(IO.unit)(_ => closed.traverse_(_.set(true)))
        .as(built.map(_ => Registry))

    def recheck(profile: SerdeProfile): IO[Option[Either[String, Serde[IO]]]] = IO.pure(None)
  }

  /** A serde that stands in for the registry-backed one. It can read but not write, which is the real
    * asymmetry: a subject that exists can be read from and a subject that does not cannot be written to.
    */
  private object Registry extends SimpleSerde[IO] {
    val name: SerdeName = SerdeName.SchemaRegistry
    val summary: String = "reads Avro through a Schema Registry"
    def decode(headers: List[RawHeader], bytes: Array[Byte]): Either[DeserializeFailure, DeserializeResult] =
      Right(DeserializeResult.json("""{"decoded":true}"""))
    def encode(input: String, headers: List[RawHeader]): Either[SerializeFailure, Array[Byte]] =
      encodeFailure("not registered")
    override def canSerialize(t: TopicName, target: Target): IO[Boolean] = IO.pure(false)
    def claims(sample: Array[Byte]): Boolean = false
  }

  private val rules = Rules(
    patterns = List(PatternRule("orders.*".r, SerdeName.SchemaRegistry, Target.Value)),
    defaultKey = None,
    defaultValue = None
  )

  private def serdes(
      factories: List[SerdeFactory[IO]] = Nil,
      profileRules: Rules = Rules.empty
  ): Resource[IO, ClusterSerdes[IO]] =
    ClusterSerdes.resource[IO](SerdeProfile(cluster, 1L, profileRules, Map.empty), factories)

  test("the built-ins are there with no configuration at all") {
    serdes().use { cs =>
      IO {
        assertEquals(cs.all.map(_.name), kui.serde.builtin.BuiltinSerdes.names)
        assertEquals(cs.fallback.name, SerdeName.Fallback)
        assert(!cs.all.map(_.name).contains(SerdeName.Fallback), "the fallback is not a candidate")
      }
    }
  }

  test("a configured serde sorts ahead of the built-ins") {
    // Someone went to the trouble of configuring it, so it is more likely to be what they want than a
    // primitive that is always present.
    serdes(List(factory(Right(())))).use { cs =>
      IO(assertEquals(cs.all.map(_.name).head, SerdeName.SchemaRegistry))
    }
  }

  test("a configured serde that could not be built is named, with its reason") {
    // The fact resolution throws away. Without it a browse of `orders-v2` renders the registry's payloads
    // through the fallback and reports "the payload is not valid UTF-8", which is a description of the
    // fallback's own attempt and points the reader at their data instead of at their registry.
    serdes(List(factory(Left("the registry could not be reached"))), rules).use { cs =>
      IO {
        assertEquals(
          cs.unavailableChoice(topic, Target.Value),
          Some((SerdeName.SchemaRegistry, "the registry could not be reached"))
        )
      }
    }
  }

  test("a configured serde that built fine is not reported as unavailable") {
    serdes(List(factory(Right(()))), rules).use { cs =>
      IO(assertEquals(cs.unavailableChoice(topic, Target.Value), None))
    }
  }

  test("nothing configured for the topic is not a degradation and is not reported") {
    // `String` is the floor of resolution, not a fall-back from something the operator asked for, so
    // there is nothing here to tell anybody about.
    serdes(List(factory(Left("the registry could not be reached"))), rules).use { cs =>
      IO {
        assertEquals(cs.unavailableChoice(TopicName.unsafe("audit-log"), Target.Value), None)
        assertEquals(cs.unavailableChoice(topic, Target.Key), None)
      }
    }
  }

  test("resolve follows the configured pattern") {
    serdes(List(factory(Right(()))), rules).use { cs =>
      cs.resolve(topic, Target.Value, None)
        .map(r => assertEquals(r.map(_.name), Right(SerdeName.SchemaRegistry)))
    }
  }

  test("an explicit choice wins") {
    serdes(List(factory(Right(()))), rules).use { cs =>
      cs.resolve(topic, Target.Value, Some(SerdeName.Hex))
        .map(r => assertEquals(r.map(_.name), Right(SerdeName.Hex)))
    }
  }

  test("suggest marks at most one row preferred, and marks the one resolution would pick") {
    serdes(List(factory(Right(()))), rules).use { cs =>
      cs.suggest(topic, Target.Value, SerdeUse.Deserialize).map { rows =>
        assertEquals(rows.count(_.preferred), 1)
        assertEquals(rows.find(_.preferred).map(_.name), Some(SerdeName.SchemaRegistry))
      }
    }
  }

  test("suggest offers only serdes that can do the thing being asked about") {
    // The stand-in registry serde reads and does not write. Offering it on the produce form would be
    // offering a choice that fails on submit.
    serdes(List(factory(Right(())))).use { cs =>
      for {
        reading <- cs.suggest(topic, Target.Value, SerdeUse.Deserialize)
        writing <- cs.suggest(topic, Target.Value, SerdeUse.Serialize)
      } yield {
        assert(reading.exists(_.name == SerdeName.SchemaRegistry))
        assert(!writing.exists(_.name == SerdeName.SchemaRegistry))
      }
    }
  }

  test("a serde whose backing service is down is listed disabled with a reason, not omitted") {
    // A user who configured Avro and finds Avro simply missing has no way to tell a typo from an outage
    // (ADR-032).
    serdes(List(factory(Left("the schema registry at https://sr:8081 did not answer"))), rules).use { cs =>
      cs.suggest(topic, Target.Value, SerdeUse.Deserialize).map { rows =>
        val row = rows.find(_.name == SerdeName.SchemaRegistry)
        assert(row.isDefined, "the unavailable serde must still be listed")
        assertEquals(row.map(_.available), Some(false))
        assert(row.flatMap(_.unavailableReason).exists(_.contains("did not answer")))
        assertEquals(row.map(_.preferred), Some(false))
      }
    }
  }

  test("an unavailable serde is never chosen implicitly: the browse continues on the fallback path") {
    serdes(List(factory(Left("registry unreachable"))), rules).use { cs =>
      cs.resolve(topic, Target.Value, None).map { resolved =>
        assertEquals(resolved.map(_.name), Right(SerdeName.String))
      }
    }
  }

  test("asking for an unavailable serde by name is KUI-SERDE-UNAVAILABLE, before any stream starts") {
    serdes(List(factory(Left("registry unreachable"))), rules).use { cs =>
      cs.resolve(topic, Target.Value, Some(SerdeName.SchemaRegistry)).map { resolved =>
        assertEquals(resolved.swap.toOption.map(_.code), Some(ErrorCode.SerdeUnavailable))
        assert(resolved.swap.exists(_.message.contains("registry unreachable")))
      }
    }
  }

  test("asking for a serde this cluster never had is a different, non-infrastructure error") {
    // The distinction is load-bearing: ADR-039 reports only infrastructure failures to the capability
    // registry, so a user typing a bad serde name must not dim the message feature for everybody else.
    serdes().use { cs =>
      cs.resolve(topic, Target.Value, Some(SerdeName.SchemaRegistry)).map { resolved =>
        assertEquals(resolved.swap.toOption.map(_.code), Some(ErrorCode.Unsupported))
      }
    }
  }

  test("the resource is closed when it is released, which is what a profile change does") {
    // Without this, a rotated Schema Registry credential is never picked up and every profile edit leaks
    // an HTTP client (ADR-016).
    for {
      closed <- Ref.of[IO, Boolean](false)
      _ <- serdes(List(factory(Right(()), Some(closed)))).use(_ => IO.unit)
      _ <- closed.get.assertEquals(true)
    } yield ()
  }

  test("the registry logs and counts one build per cluster version") {
    for {
      builds <- Ref.of[IO, List[SerdeProfile]](Nil)
      metrics = new SerdeRegistryMetrics[IO] {
        def registryBuilt(profile: SerdeProfile): IO[Unit] = builds.update(_ :+ profile)
      }
      registry = SerdeRegistry[IO](Nil, metrics)
      profile = SerdeProfile.unconfigured(cluster, 7L)
      _ <- registry.forCluster(profile).use(cs => IO(assert(cs.all.nonEmpty)))
      recorded <- builds.get
      _ <- IO(assertEquals(recorded.map(_.version), List(7L)))
    } yield ()
  }
}
