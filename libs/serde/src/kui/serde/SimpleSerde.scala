package kui.serde

import cats.Applicative
import cats.syntax.all.*

import kui.kernel.TopicName

/** The shape every serde in this module has: pure, stateless, and the same for every topic.
  *
  * `Serde[F]`'s eight methods exist for the Schema-Registry case, where answering "can you read this topic?"
  * is a network call. For a serde that turns four bytes into an integer, all eight collapse into two pure
  * functions, and writing them out nine times would be nine chances to get `canSerialize` subtly wrong in one
  * of them.
  *
  * Subclasses provide [[decode]] and [[encode]]; everything else has a default that is right for a pure
  * codec. A serde that genuinely differs — one that is read-only, say — overrides the one method it differs
  * in.
  */
abstract class SimpleSerde[F[_]](using F: Applicative[F]) extends Serde[F] with SampleDetector {

  /** Bytes to text. Total: returns `Left` rather than throwing, though `Deserializers.attempt` catches a
    * throw as well.
    */
  def decode(headers: List[RawHeader], bytes: Array[Byte]): Either[DeserializeFailure, DeserializeResult]

  /** Text to bytes, for the produce form. */
  def encode(input: String, headers: List[RawHeader]): Either[SerializeFailure, Array[Byte]]

  /** The sentence the picker and `docs/operations/serdes.md` show. */
  def summary: String

  /** Whether this serde is exercised against a real broker or registry, not only in unit tests. */
  def integrationTested: Boolean = false

  final def describe: SerdeDescription = SerdeDescription(name, summary, integrationTested)

  def canDeserialize(topic: TopicName, target: Target): F[Boolean] = true.pure[F]
  def canSerialize(topic: TopicName, target: Target): F[Boolean] = true.pure[F]

  /** Topic-level preference, which for a pure codec is never a property of the topic: nothing about the name
    * `orders-v2` says its values are integers. Sample-level preference is [[SampleDetector.claims]], and it
    * is what auto-detection actually uses.
    */
  def preferable(topic: TopicName, target: Target): F[Boolean] = false.pure[F]

  def schema(topic: TopicName, target: Target): F[Option[SchemaDescription]] = none[SchemaDescription].pure[F]

  def parameters(topic: TopicName, target: Target): F[List[SerdeParameter]] =
    List.empty[SerdeParameter].pure[F]

  final def deserializer(topic: TopicName, target: Target): F[Deserializer[F]] =
    (new Deserializer[F] {
      val serde: SerdeName = name
      def deserialize(
          headers: List[RawHeader],
          bytes: Array[Byte]
      ): F[Either[DeserializeFailure, DeserializeResult]] =
        unlessOverflowing(decode(headers, bytes))(failure(Overflowed)).pure[F]
    }: Deserializer[F]).pure[F]

  final def serializer(topic: TopicName, target: Target, params: Map[String, String]): F[Serializer[F]] =
    (new Serializer[F] {
      val serde: SerdeName = name
      def serialize(input: String, headers: List[RawHeader]): F[Either[SerializeFailure, Array[Byte]]] =
        unlessOverflowing(encode(input, headers))(encodeFailure(Overflowed)).pure[F]
    }: Serializer[F]).pure[F]

  /** The one failure a pure codec can raise that nothing above it can turn into a `Left`.
    *
    * A `StackOverflowError` is a `VirtualMachineError`. `Deserializers.attempt` runs a decode inside
    * `Sync.defer(...).handleError(...)`, and Cats Effect never hands a fatal error to `handleError`: it shuts
    * its runtime down and the process halts. A codec that recurses over a payload's structure overflows on a
    * payload nested deeply enough, so this catches exactly that error, here, where the codec has already
    * unwound and nothing is half-done: a pure function over bytes has no state to leave inconsistent. The
    * built-in codecs bound their depth themselves (`JsonNesting`); this is for any codec that does not.
    */
  private def unlessOverflowing[A](body: => A)(overflowed: => A): A =
    try body
    catch { case _: StackOverflowError => overflowed }

  private val Overflowed: String =
    "this payload is too deeply nested for this serde: processing it exhausted the stack"

  /** Convenience for the common failure, so that every serde's `Left` names itself the same way. */
  final protected def failure(cause: String): Either[DeserializeFailure, DeserializeResult] =
    Left(DeserializeFailure(name, cause))

  final protected def encodeFailure(cause: String): Either[SerializeFailure, Array[Byte]] =
    Left(SerializeFailure(name, cause))
}

/** A serde that can look at a payload and say whether the payload is its own.
  *
  * A separate capability rather than a ninth method on `Serde[F]`, because `Serde[F]`'s `preferable` asks
  * about a *topic* and auto-detection asks about *bytes*. Widening `preferable` to take a sample would put an
  * `Array[Byte]` in the signature that the Schema-Registry serde — the one serde for which the topic-level
  * question is the meaningful one — has no use for.
  *
  * A serde that is not a detector is never auto-detected. That is deliberate for `Base64` and `Hex`, which
  * can render literally any bytes and so would win every detection if they were allowed to compete.
  */
trait SampleDetector {

  /** True when these bytes are, on their face, this serde's format. Must be a pure function of the bytes: a
    * picker whose order changes between two refreshes of the same page is a picker nobody trusts.
    */
  def claims(sample: Array[Byte]): Boolean
}
