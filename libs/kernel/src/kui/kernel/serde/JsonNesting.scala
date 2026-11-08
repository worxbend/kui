package kui.kernel.serde

import scala.annotation.tailrec

/** How deeply a decoded payload may nest, and a way of measuring it that cannot itself run out of stack.
  *
  * Everything KUI decodes is walked recursively at some point afterwards: circe's printer, the masking walk,
  * the Avro and Protobuf readers, the JSON Schema validator. A recursive walk costs at least one JVM stack
  * frame per level, and a producer can nest a document thousands of levels deep in a few kilobytes — `{"a":`
  * three thousand times is 15 KB. The overflow that follows is not an ordinary failure: `StackOverflowError`
  * is a `VirtualMachineError`, `NonFatal` does not match it, and Cats Effect answers it by shutting the
  * runtime down and halting the process. One record could stop the message service every time anybody looked
  * at it.
  *
  * So depth is bounded where a payload is decoded or encoded, before anything walks it, and a deeper one is a
  * typed failure the record carries to the screen like any other decode that did not work. The constant lives
  * in the kernel because the serdes that produce JSON and the masking that re-reads it must agree on it: a
  * document one side lets through has to be one the other can walk.
  *
  * 256 is far beyond anything a person or a schema produces (real payloads rarely pass twenty levels) and
  * shallow enough that walking it uses a small fraction of a default 1 MiB thread stack even before the JIT
  * has compiled the walker. `libs/filter` bounds its own walk at 500, so anything a serde lets through is
  * also something a filter can read.
  */
object JsonNesting {

  /** The deepest a decoded document's objects and arrays may nest. */
  val MaxDepth: Int = 256

  /** Whether JSON text nests objects and arrays more than `limit` levels deep.
    *
    * A scan over characters with a counter, not a walk over a parsed tree, so that measuring the depth is not
    * itself the recursion it guards against. Brackets inside string literals are not counted, and escapes
    * inside them are honoured. On valid JSON the answer is exact; on text that is not JSON it is an upper
    * bound on how deep a parser could get before rejecting it, which is all a guard needs.
    */
  def exceeds(text: String, limit: Int): Boolean = {
    @tailrec
    def scan(index: Int, depth: Int, inString: Boolean, escaped: Boolean): Boolean =
      if index >= text.length then false
      else {
        val character = text.charAt(index)
        if inString then
          if escaped then scan(index + 1, depth, inString = true, escaped = false)
          else if character == '\\' then scan(index + 1, depth, inString = true, escaped = true)
          else scan(index + 1, depth, inString = character != '"', escaped = false)
        else
          character match {
            case '"' => scan(index + 1, depth, inString = true, escaped = false)
            case '{' | '[' =>
              if depth + 1 > limit then true
              else scan(index + 1, depth + 1, inString = false, escaped = false)
            case '}' | ']' => scan(index + 1, depth - 1, inString = false, escaped = false)
            case _ => scan(index + 1, depth, inString = false, escaped = false)
          }
      }

    scan(0, 0, inString = false, escaped = false)
  }
}
