package kui.security.masking

import cats.data.NonEmptyList
import munit.FunSuite

import kui.kernel.TopicName
import kui.kernel.serde.{PayloadKind, Target}

/** [[MaskingEngine.maskPayload]]: the entry point a service calls, and the two choices it makes for it.
  *
  * It exists because `services/message` holds a `Decoded(text, kind, …)` and no circe — ADR-041 rule A3
  * forbids its use cases JSON — so the choice between [[MaskingEngine.maskJson]] and
  * [[MaskingEngine.maskText]] has to be made inside this module or be made twice outside it. Both of the
  * choices below are ones a caller would plausibly make differently, which is why each has a case.
  */
final class MaskingPayloadSuite extends FunSuite {

  private val orders: TopicName = TopicName.unsafe("orders")

  private val stars: MaskingKind = MaskingKind.Mask("*", KeepEnds.none)

  private def onField(kind: MaskingKind, name: String): MaskingRule =
    MaskingRule(kind, Some(NonEmptyList.of(name)), None, None, None)

  private val wholeValue: MaskingRule = MaskingRule(stars, None, None, None, None)

  private def mask(rules: List[MaskingRule], kind: PayloadKind, text: String): String =
    MaskingEngine.maskPayload(rules, orders, Target.Value, kind, text)

  test("a JSON payload takes the document path: the named field is masked and the rest is not") {
    assertEquals(
      mask(List(onField(stars, "card")), PayloadKind.Json, """{"id":7,"card":"4111"}"""),
      """{"id":7,"card":"****"}"""
    )
  }

  test("a text payload takes the string path and is not re-quoted") {
    assertEquals(mask(List(wholeValue), PayloadKind.Text, "4111"), "****")
  }

  // THE CHOICE A CALLER WOULD GET WRONG. Parsing first and treating whatever parses as JSON is the obvious
  // implementation, and it is wrong for every text topic whose records happen to be numbers or booleans:
  // `42` is a valid JSON document, so the document path would run `maskLeaf` over it and `noSpaces` would
  // put the result back with quotes round it. The serde already said what it produced; this asserts that
  // `maskPayload` believes it rather than re-deciding.
  test("a text payload that happens to be valid JSON is still masked as text, quotes and all") {
    assertEquals(mask(List(wholeValue), PayloadKind.Text, "42"), "**")
    assertEquals(mask(List(wholeValue), PayloadKind.Json, "42"), "\"**\"")
  }

  // THE OTHER CHOICE, AND THE ONE THAT MATTERS MOST. A payload labelled JSON that will not parse must not
  // come back untouched: that is the single outcome masking may never have, because it publishes in full
  // the field the rule exists to hide. Masking too much is recoverable; masking nothing is not.
  test("a payload labelled JSON that will not parse is masked as text rather than left alone") {
    val truncated = """{"card":"4111"""

    assertEquals(mask(List(wholeValue), PayloadKind.Json, truncated), "*" * truncated.length)
  }

  test("a field rule over a payload labelled JSON that will not parse masks nothing, and says so here") {
    // The honest limit of the fallback above: `maskText` applies the *first matching rule's string form*,
    // and a field rule's string form is the whole value. So a `fields: [card]` rule over an unparseable
    // document masks the entire text rather than one field — which is more than the operator asked for
    // and never less, which is the side of the line this has to fall on.
    assertEquals(
      mask(List(onField(stars, "card")), PayloadKind.Json, """{"card":"4111"""),
      "*" * """{"card":"4111""".length
    )
  }

  // CLOSING A HOLE FOUND BY MUTATION WHILE WIRING DM-001, AND IT IS A FAIL-OPEN.
  // `maskString`'s `case MaskingKind.Remove => ""` had no case anywhere: changing it to `=> text` left
  // `./mill --no-daemon -k libs.securityCore.jvm.test + libs.config.test + services.message.__.test` at
  // 708/708 green over 59 suites. Every `remove` fixture in `MaskingEngineSuite` goes through `maskJson`,
  // where `maskLeaf` answers `None` and never reaches this function, and `maskHeaders` drops a removed
  // header before calling it. The one path that does reach it is a whole-value `remove` over a **non-JSON**
  // payload -- which is the quickstart's own `audit.log.raw` -- and under the mutation that payload comes
  // back in full. A rule that says "delete this" and returns the data is the worst outcome a masking
  // engine has.
  test("a whole-value remove on a text payload yields nothing, and never the payload") {
    val removeEverything = MaskingRule(MaskingKind.Remove, None, None, None, None)

    assertEquals(mask(List(removeEverything), PayloadKind.Text, "4111111111111111"), "")
    assertEquals(MaskingEngine.maskText(List(removeEverything), orders, Target.Value, "secret"), "")
  }

  // A document labelled JSON is re-parsed, walked and re-printed here, all three recursively. One nested a few
  // thousand levels deep overflowed the stack, and StackOverflowError is fatal to Cats Effect: the process
  // halted. The serdes refuse such documents now, but this is the last walker before the screen and must not
  // depend on every serde, including a third-party one, having done so. Review R2 finding
  // h-serde-untrusted-decode-1.
  test("a payload labelled JSON nested past the limit is masked as text instead of overflowing the stack") {
    val depth = 3000
    val deep = ("""{"card":""" * depth) + "\"4111\"" + ("}" * depth)
    val outcome = new java.util.concurrent.atomic.AtomicReference[Either[Throwable, String]]()
    // A 256 KiB stack, so that the overflow does not depend on how far the JIT has compiled the walk.
    val thread = new Thread(
      null,
      () =>
        outcome.set(
          try Right(mask(List(onField(stars, "card")), PayloadKind.Json, deep))
          catch { case thrown: Throwable => Left(thrown) }
        ),
      "masking-small-stack",
      256L * 1024L
    )
    thread.start()
    thread.join()

    assertEquals(outcome.get().left.map(_.toString), Right("*" * deep.length))
  }

  test("no rule in scope leaves both forms exactly as they arrived") {
    val keyScoped = MaskingRule(stars, None, None, Some("orders".r), None)

    assertEquals(mask(List(keyScoped), PayloadKind.Json, """{"card":"4111"}"""), """{"card":"4111"}""")
    assertEquals(mask(List(keyScoped), PayloadKind.Text, "4111"), "4111")
  }
}
