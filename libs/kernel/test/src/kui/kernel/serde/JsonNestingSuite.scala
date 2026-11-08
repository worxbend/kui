package kui.kernel.serde

import munit.FunSuite

/** The depth scan the serdes and the masking engine share: exact on JSON, blind to brackets in strings, and
  * flat enough to measure a document no recursive walk could.
  */
final class JsonNestingSuite extends FunSuite {

  test("depth counts objects and arrays together, and the limit itself is allowed") {
    assert(!JsonNesting.exceeds("""{"a":[{"b":[]}]}""", limit = 4))
    assert(JsonNesting.exceeds("""{"a":[{"b":[]}]}""", limit = 3))
    assert(!JsonNesting.exceeds("""[] {} [[]]""", limit = 2))
  }

  test("a scalar or an empty text has no depth at all") {
    assert(!JsonNesting.exceeds("", limit = 0))
    assert(!JsonNesting.exceeds("42", limit = 0))
    assert(JsonNesting.exceeds("[]", limit = 0))
  }

  test("brackets inside strings do not count, and an escaped quote does not end the string") {
    assert(!JsonNesting.exceeds("""{"a":"[[[[{{{{"}""", limit = 1))
    assert(!JsonNesting.exceeds("""{"a":"say \"[[[[\" twice"}""", limit = 1))
    assert(!JsonNesting.exceeds("""{"a":"ends in a backslash \\","b":1}""", limit = 1))
    assert(JsonNesting.exceeds("""{"a":"ends in a backslash \\","b":[1]}""", limit = 1))
  }

  test("a document far deeper than the stack could walk is measured without recursing") {
    val depth = 1000000
    val deep = ("[" * depth) + ("]" * depth)
    assert(JsonNesting.exceeds(deep, JsonNesting.MaxDepth))
    assert(!JsonNesting.exceeds(deep, depth))
  }
}
