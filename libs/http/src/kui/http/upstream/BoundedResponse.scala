package kui.http.upstream

import java.nio.charset.StandardCharsets

import scala.util.control.NoStackTrace

import sttp.client4.*

/** Response handlers that keep a strict byte ceiling on an upstream body.
  *
  * The production transport is sttp's Http4s backend, which cannot hand a body over as a
  * `java.io.InputStream`. The ceiling is therefore the request option that backend enforces while it receives
  * the stream, set by the `Request` overload of `asString`, together with a check on the string a backend
  * that ignores the option materialises anyway. A declared `Content-Length` above the ceiling fails before
  * that string is trusted, and an absent or understated length still cannot slip a larger body through.
  */
object BoundedResponse {

  /** A UTF-8 string response whose encoded body may occupy no more than `maxBytes`. */
  def asString(maxBytes: Long): ResponseAs[String] = {
    require(maxBytes >= 0L, "a response byte limit must not be negative")

    fromMetadata(
      asStringAlways.map(rejectIfOver(maxBytes)),
      ConditionalResponseAs(
        _.contentLength.exists(_ > maxBytes),
        ignore.map[String](_ => fail(maxBytes))
      )
    )
  }

  /** [[asString(Long)]] on `request`, with the ceiling also set as `maxResponseBodyLength`.
    *
    * Http4s applies that option through `Fs2Streams.limitBytes` and stops reading an endless body. A
    * description alone cannot: the backend only limits a stream it was told to limit.
    */
  def asString[T](request: Request[T], maxBytes: Long): Request[String] =
    request.maxResponseBodyLength(maxBytes).response(asString(maxBytes))

  private def rejectIfOver(maxBytes: Long)(body: String): String = {
    val size = body.getBytes(StandardCharsets.UTF_8).length.toLong
    if size > maxBytes then fail(maxBytes) else body
  }

  final case class LimitExceeded(limitBytes: Long)
      extends RuntimeException(s"response exceeded $limitBytes bytes")
      with NoStackTrace

  /** sttp's `ResponseAs` callback communicates decoder failure by throwing. The exception is caught by the
    * effectful backend and contains only the configured numeric limit.
    */
  private def fail(limitBytes: Long): Nothing =
    // scalafix:off DisableSyntax.throw
    throw LimitExceeded(limitBytes)
    // scalafix:on DisableSyntax.throw
}
