package kui.serde

import kui.kernel.TopicName
import kui.testkit.KuiSuite

/** The last line of defence `SimpleSerde` adds around every pure codec, built-in or not. */
final class SimpleSerdeSuite extends KuiSuite {

  private val topic: TopicName = TopicName.unsafe("orders")

  /** A codec whose recursion has no floor: the shape of any decoder handed a payload nested deeper than the
    * thread's stack, reduced to the one property that matters here. `Id`, so that it runs on this thread.
    */
  private object Bottomless extends SimpleSerde[cats.Id] {
    val name: SerdeName = SerdeName.unsafe("Bottomless")
    val summary: String = "recurses until the stack runs out"

    private def descend(depth: Int): Int = if depth == Int.MaxValue then 0 else descend(depth + 1) + 1

    def decode(headers: List[RawHeader], bytes: Array[Byte]): Either[DeserializeFailure, DeserializeResult] =
      Right(DeserializeResult.text(descend(bytes.length).toString))

    def encode(input: String, headers: List[RawHeader]): Either[SerializeFailure, Array[Byte]] =
      Right(Array.fill(descend(input.length))(0.toByte))

    def claims(sample: Array[Byte]): Boolean = false
  }

  /** Whatever the call threw, fatal errors included, so that a regression fails as an assertion. */
  private def caught[A](body: => A): Either[Throwable, A] =
    try Right(body)
    catch { case thrown: Throwable => Left(thrown) }

  test("a pure codec that overflows the stack while decoding yields a failure on the record") {
    // Cats Effect treats StackOverflowError as fatal: escaping `Deserializers.attempt`'s `Sync.defer` it
    // never reaches `handleError`, and the runtime halts the process. Review R2 finding
    // h-serde-untrusted-decode-1.
    val outcome = caught(Bottomless.deserializer(topic, Target.Value).deserialize(Nil, Array[Byte](1)))
    outcome match {
      case Left(thrown) => fail(s"decoding threw $thrown instead of returning a Left")
      case Right(decoded) =>
        assert(decoded.left.exists(failure => failure.serde == Bottomless.name), decoded)
        assert(decoded.left.exists(_.cause.contains("too deeply nested")), decoded)
    }
  }

  test("a pure codec that overflows the stack while encoding refuses the produce instead") {
    val outcome = caught(Bottomless.serializer(topic, Target.Value, Map.empty).serialize("x", Nil))
    outcome match {
      case Left(thrown) => fail(s"encoding threw $thrown instead of returning a Left")
      case Right(written) => assert(written.left.exists(_.cause.contains("too deeply nested")), written)
    }
  }
}
