package kui.message.infrastructure

import cats.effect.IO
import cats.effect.kernel.Resource

import kui.kernel.serde.{SerdeName, Target}
import kui.kernel.{ClusterId, TopicName}
import kui.serde.{ClusterSerdes, Serde, SerdeDescription, SerdeFactory, SerdeProfile, SerdeResolution}
import kui.testkit.KuiIOSuite

/** Which sentence a record's marker carries when a decode did not go the way the operator configured.
  *
  * `ClusterSerdeSource` shipped with no suite: it is reachable only through a browse, and a browse suite
  * asserts records rather than markers. Two mutations were applied one at a time against
  * `./mill services.message.__.test`, each leaving it at 204/204 green — reversing the precedence of the
  * skipped-serde note over the fallback's own complaint, and dropping the `requested.isDefined` guard that
  * keeps the note off a decode the caller explicitly asked for. Both are about one sentence on one row, and
  * that sentence is the only thing that tells an operator whether to go and look at their registry or at
  * their data.
  *
  * The serdes here are built through `ClusterSerdes.resource` from a factory that fails the way a
  * schema-registry serde fails when the registry is down, so the arrangement is the product's own rather than
  * a hand-written stand-in for it.
  */
final class ClusterSerdeSourceSuite extends KuiIOSuite {

  private val cluster: ClusterId = ClusterId.unsafe("prod")
  private val topic: TopicName = TopicName.unsafe("orders.v1")
  private val registry: SerdeName = SerdeName.unsafe("SchemaRegistry")

  private val Unreachable: String = "the schema registry could not be reached"

  /** Bytes no serde claims and the cluster's configured default cannot read: a lone UTF-8 continuation byte.
    * Auto-detection asks each serde whether the bytes are its own before configuration is consulted, so a
    * payload every serde recognises would never reach the fall-through this case is about.
    */
  private val NotUtf8: Array[Byte] = Array(0xc3.toByte, 0x28.toByte)

  /** A configured serde that cannot be built, which is what a registry being down looks like from here. */
  private val brokenFactory: SerdeFactory[IO] = new SerdeFactory[IO] {
    def name: SerdeName = registry
    def describe: SerdeDescription =
      SerdeDescription(registry, "A registry that is not answering", coveredByIntegrationTest = false)
    def create(profile: SerdeProfile): Resource[IO, Either[String, Serde[IO]]] =
      Resource.pure(Left(Unreachable))

    def recheck(profile: SerdeProfile): IO[Option[Either[String, Serde[IO]]]] = IO.pure(None)
  }

  /** `orders.v1`'s values are configured to use the registry serde; the cluster default is `Json`.
    *
    * Both matter: the pattern is what `unavailableChoice` reports, and the default is what resolution falls
    * through to once the pattern's serde turns out not to be usable.
    */
  private val profile: SerdeProfile = SerdeProfile(
    cluster = cluster,
    version = 1L,
    rules = SerdeResolution.Rules(
      patterns = List(SerdeResolution.PatternRule("orders\\..*".r, registry, Target.Value)),
      defaultKey = None,
      defaultValue = Some(SerdeName.Json)
    ),
    properties = Map.empty
  )

  private def sourceOver(use: ClusterSerdeSource[IO] => IO[Unit]): IO[Unit] =
    ClusterSerdes
      .resource[IO](profile, List(brokenFactory))
      .use(serdes => use(new ClusterSerdeSource[IO](Map(cluster -> serdes))))

  test("the serde that could not be used is what the row says, not what the fallback complained about") {
    /*
     * The precedence the file argues for in a paragraph: "the SchemaRegistry serde is configured for this
     * topic and could not be used - the registry could not be reached" names the thing an operator can
     * fix, while the decode failure underneath it describes the consequence and points at the data. Both
     * are present on this record — the configured serde is unusable *and* the serde resolution fell
     * through to cannot read these bytes — and only the first is worth showing.
     */
    sourceOver { source =>
      source
        .decode(cluster, topic, Target.Value, requested = None, bytes = Some(NotUtf8))
        .map { (decoded, marker) =>
          val expected =
            s"the SchemaRegistry serde is configured for this topic and could not be used: $Unreachable"

          assertEquals(marker, Some(expected))
          // The record is still shown: a decode never fails a browse, which is the promise this whole
          // class exists to keep.
          assertEquals(decoded.text.isEmpty, false)
        }
    }
  }

  test("a caller who named a serde is not also told about the one they overrode") {
    /*
     * The `requested.isDefined` guard. An explicit choice that cannot be honoured is refused by `resolve`
     * with its own message, so adding the configured-serde note as well says the same thing twice — and
     * when the explicit choice *worked*, as here, the note is not merely redundant but wrong: it reports
     * a problem with a serde this record was deliberately not decoded with.
     */
    sourceOver { source =>
      source
        .decode(
          cluster,
          topic,
          Target.Value,
          requested = Some(SerdeName.Json),
          bytes = Some("""{"id":"7"}""".getBytes("UTF-8"))
        )
        .map { (decoded, marker) =>
          assertEquals(marker, None)
          assertEquals(decoded.serde, SerdeName.Json)
        }
    }
  }
}
