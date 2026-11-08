package kui.security.masking

import io.circe.{parser, Json, JsonObject}

import kui.kernel.TopicName
import kui.kernel.serde.{JsonNesting, PayloadKind, Target}

/** The masking rules, applied. Pure functions over `Json` and `String`, with no effect and no failure path.
  *
  * Every function here is total and the identity is a legal result, which is why none of them returns an
  * `Either`. The one dangerous case — a rule that matches nothing because of a typo — cannot be caught here
  * at all; it is caught at startup, where an unusable regex is a configuration error
  * (`kui.clusters[].masking[]`, `MaskingConfig`). `docs/operations/masking.md` is the operator's guide to
  * writing one; it exists as of wave 10, and the sentence that stood here for nine waves — saying the file
  * had never existed — is what it replaces.
  *
  * ## The application order
  *
  * A **JSON** value gets **every** matching rule, in configuration order. Two rules on the same field
  * compose, which is what lets an operator write "replace this field" and "mask everything else" and get
  * both.
  *
  * A **non-JSON** value gets the **first** matching rule's string form only. There is no meaningful way to
  * compose "remove" and "keep the last four characters" over a payload with no structure, and applying rules
  * in sequence to flat text produces results that depend on rule order in ways nobody can predict from
  * reading the configuration.
  *
  * This is Kafbat's order, kept so that a migrating operator's policies behave the way their old tool
  * behaved.
  *
  * ## Where it runs
  *
  * After deserialization and before any DTO leaves the service, including `originalValue` (ADR-023). This
  * object does not decide that — the browse use case does — but the placement is the entire point: a masked
  * field that is still readable in one response shape is not masked.
  */
object MaskingEngine {

  /** Whether any rule could apply to this topic and target.
    *
    * The fast path for the overwhelmingly common case of no rules at all. Without it every record would be
    * re-parsed and re-walked to discover there was nothing to do, which is the difference between masking
    * costing nothing on an unmasked topic and costing a JSON round trip per record.
    */
  def applies(rules: List[MaskingRule], topic: TopicName, target: Target): Boolean =
    rules.exists(scopeMatches(_, topic, target))

  /** All matching rules, in configuration order, applied to a JSON document. */
  def maskJson(rules: List[MaskingRule], topic: TopicName, target: Target, json: Json): Json =
    rules.filter(scopeMatches(_, topic, target)).foldLeft(json)((document, rule) => apply(rule, document))

  /** The first matching rule's string form, for a payload that is not JSON. */
  def maskText(rules: List[MaskingRule], topic: TopicName, target: Target, text: String): String =
    rules.find(scopeMatches(_, topic, target)).fold(text)(rule => maskString(rule.kind, text))

  /** One decoded payload's text, masked, whichever of the two forms the serde said it is in.
    *
    * This is the entry point a *service* calls, and it exists so that a caller does not have to hold circe in
    * order to mask a record. `services/message` sees a `Decoded(text, kind, …)` and nothing else; making it
    * parse the text itself would put the parse, the fallback below and the re-serialisation in a layer that
    * ADR-041 rule A3 forbids JSON to, and would put a second copy of the choice between [[maskJson]] and
    * [[maskText]] next to every caller.
    *
    * ==Why the kind decides, and not a parse attempt==
    *
    * Trying `parse` first and treating anything that succeeds as JSON is wrong for a payload the serde
    * already called text: `42` and `true` are valid JSON documents, so a plain-text topic whose records
    * happen to be numbers would take the document path, and a whole-value rule would then render `42` as
    * `"**"` — quotes and all — where the text path renders `**`. The serde has already decided what it
    * produced; re-deciding here is how two layers come to disagree about the same record.
    *
    * ==The fallback, which is the part that matters==
    *
    * A payload labelled JSON that will not parse still gets the **text** form rather than being returned
    * untouched. That case is reachable — a serde reporting a kind it did not verify, a truncated document —
    * and the alternative is publishing in full the field the rule exists to hide, which is the one outcome
    * masking may never have. Masking too much is recoverable; masking nothing is not.
    */
  def maskPayload(
      rules: List[MaskingRule],
      topic: TopicName,
      target: Target,
      kind: PayloadKind,
      text: String
  ): String =
    kind match {
      case PayloadKind.Text => maskText(rules, topic, target, text)
      // Deeper than any serde emits is treated exactly like "will not parse", and for the same reason: the
      // walk in `maskJson` and the printer after it recurse once per level, and a `StackOverflowError` is
      // fatal to Cats Effect, so a document nested a few thousand levels deep would halt the service rather
      // than mask one record. The serdes refuse such documents themselves; this is the last walk before the
      // screen and must not rely on every serde, a third-party one included, having done so.
      case PayloadKind.Json if JsonNesting.exceeds(text, JsonNesting.MaxDepth) =>
        maskText(rules, topic, target, text)
      case PayloadKind.Json =>
        parser.parse(text) match {
          case Right(json) => maskJson(rules, topic, target, json).noSpaces
          case Left(_) => maskText(rules, topic, target, text)
        }
    }

  /** Header values, masked by header name against the same field rules.
    *
    * Header names are matched the way field names are, because a header called `authorization` is exactly as
    * sensitive as a field called `authorization` and an operator should not have to say so twice. A rule
    * scoped to `topicKeysPattern` does not apply: headers belong to the record, not to its key or its value,
    * so only value-scoped and unscoped rules reach them.
    */
  def maskHeaders(
      rules: List[MaskingRule],
      topic: TopicName,
      headers: Map[String, String]
  ): Map[String, String] = {
    val applicable = rules.filter(scopeMatches(_, topic, Target.Value))
    if applicable.isEmpty then headers
    else
      headers.flatMap { (name, value) =>
        applicable.filter(rule => fieldMatches(rule, name)) match {
          case Nil => Some(name -> value)
          case matched =>
            // `Remove` on a header drops the header entirely, matching what it does to an object key.
            if matched.exists(_.kind == MaskingKind.Remove) then None
            else Some(name -> matched.foldLeft(value)((current, rule) => maskString(rule.kind, current)))
        }
      }
  }

  // -------------------------------------------------------------------------------------------
  // scope
  // -------------------------------------------------------------------------------------------

  /** Which topics and which half of a record a rule reaches.
    *
    * The two patterns behave as a pair, not independently:
    *
    *   - **Neither set** — the rule applies to keys and values of every topic. That is what makes "mask
    *     `cardNumber` wherever it appears" writable in one line, which matters because the topics a field
    *     appears in are exactly the ones the operator has not thought of yet.
    *   - **One set** — the rule applies to that half only, on the topics the pattern names. An operator who
    *     wrote `topicKeysPattern` and nothing else meant the keys; reading the absent `topicValuesPattern` as
    *     "every value everywhere" would mask far more than they asked for, and masking too much is a silent,
    *     hard-to-notice kind of wrong.
    *   - **Both set** — each half is scoped by its own pattern.
    */
  private def scopeMatches(rule: MaskingRule, topic: TopicName, target: Target): Boolean = {
    val unscoped = rule.topicKeysPattern.isEmpty && rule.topicValuesPattern.isEmpty
    val pattern = target match {
      case Target.Key => rule.topicKeysPattern
      case Target.Value => rule.topicValuesPattern
    }
    unscoped || pattern.exists(regex => regex.matches(topic.value))
  }

  /** Whether a rule applies to a field of this name.
    *
    * A rule with neither `fields` nor `fieldsNamePattern` matches nothing *by name* — it applies to the whole
    * value instead, which [[apply]] handles before it ever walks into the document.
    */
  private def fieldMatches(rule: MaskingRule, name: String): Boolean =
    (rule.fields, rule.fieldsNamePattern) match {
      case (Some(names), _) => names.exists(_ == name)
      case (None, Some(regex)) => regex.matches(name)
      case (None, None) => false
    }

  private def isWholeValueRule(rule: MaskingRule): Boolean =
    rule.fields.isEmpty && rule.fieldsNamePattern.isEmpty

  // -------------------------------------------------------------------------------------------
  // the walk
  // -------------------------------------------------------------------------------------------

  private def apply(rule: MaskingRule, json: Json): Json =
    if isWholeValueRule(rule) then maskLeaf(rule.kind, json).getOrElse(Json.Null)
    else walk(rule, json)

  /** Objects and arrays, to any depth.
    *
    * Recursion terminates without a depth counter because `Json` is a finite tree: circe has no way to
    * express a cycle, so there is no cycle to guard against.
    */
  private def walk(rule: MaskingRule, json: Json): Json =
    json.fold(
      jsonNull = json,
      jsonBoolean = _ => json,
      jsonNumber = _ => json,
      jsonString = _ => json,
      jsonArray = values => Json.fromValues(values.map(walk(rule, _))),
      jsonObject = obj => Json.fromJsonObject(walkObject(rule, obj))
    )

  private def walkObject(rule: MaskingRule, obj: JsonObject): JsonObject =
    JsonObject.fromIterable(
      obj.toList.flatMap { (name, value) =>
        if fieldMatches(rule, name) then maskLeaf(rule.kind, value).map(name -> _)
        else List(name -> walk(rule, value))
      }
    )

  /** One matched value.
    *
    * `None` means "this entry disappears": the key is deleted from its object, or the element is dropped from
    * its array. Every other kind produces a value, and a masked or replaced value is always a JSON string —
    * masking a number and keeping it a number would either change its magnitude or fail to hide it.
    */
  private def maskLeaf(kind: MaskingKind, value: Json): Option[Json] =
    kind match {
      case MaskingKind.Remove => None
      case MaskingKind.Replace(replacement) => Some(Json.fromString(replacement))
      case mask @ MaskingKind.Mask(_, _) =>
        // A matched object or array is masked leaf by leaf rather than flattened to its JSON text: replacing
        // `{"a":"secret"}` with `*************` would tell the reader how long the document was, and would
        // turn an object into a string in a way that breaks the table view's flattener two layers away.
        Some(
          value.fold(
            jsonNull = value,
            jsonBoolean = flag => Json.fromString(maskString(mask, flag.toString)),
            jsonNumber = number => Json.fromString(maskString(mask, number.toString)),
            jsonString = text => Json.fromString(maskString(mask, text)),
            jsonArray = values => Json.fromValues(values.map(v => maskLeaf(mask, v).getOrElse(Json.Null))),
            jsonObject =
              obj => Json.fromJsonObject(obj.mapValues(v => maskLeaf(mask, v).getOrElse(Json.Null)))
          )
        )
    }

  /** The string form of a rule, which is what a non-JSON payload and a header value get.
    *
    * Characters, not bytes, and code points, not `Char`s. Masking half of an emoji produces invalid text,
    * which then fails JSON encoding two layers away — in the response, long after anyone could connect the
    * failure to the rule that caused it.
    */
  private[masking] def maskString(kind: MaskingKind, text: String): String =
    kind match {
      case MaskingKind.Remove => ""
      case MaskingKind.Replace(replacement) => replacement
      case MaskingKind.Mask(chars, keep) => maskKeepingEnds(text, chars, keep)
    }

  /** A mask whose kept ends leave nothing to mask masks the **whole** value.
    *
    * This is the fail-safe direction, and it is the direction this file argues for everywhere else:
    * [[maskPayload]]'s own rule is that *masking too much is recoverable and masking nothing is not*. The
    * branch used to return `text` — the input, in full, from a rule whose author wrote it to hide the input.
    *
    * It was reachable from a configuration file, which is why it is repaired here and not only at the loader.
    * `MaskingConfig.MaxKeep` bounds each end at 20 and the bound was enforced per end, so
    * `keep: {prefix: 20, suffix: 20}` loaded and returned a sixteen-digit card number untouched; the loader
    * now refuses that pair as well. But `KeepEnds` is a plain pair of `Int`s that any caller can build —
    * `MaskingRule.onFields(MaskingKind.Mask("*", KeepEnds(8, 8)), "pin")` in code reaches this function
    * without passing a loader at all — and a four-character `last4` under an honest `keep: {suffix: 4}`
    * reaches it on every record. The engine is where the guarantee has to hold.
    *
    * The result is still never longer than the input: every kept end is dropped and one replacement code
    * point is written per input code point.
    */
  private def maskKeepingEnds(text: String, chars: String, keep: KeepEnds): String = {
    val points: Vector[Int] = codePoints(text)
    val total = points.length
    val prefix = keep.prefix.max(0).min(total)
    // If the two kept ends would overlap, the suffix yields: keeping more than the input has would mean
    // returning the input unmasked, and a rule that silently does nothing is worse than one that masks
    // more than its author intended.
    val suffix = keep.suffix.max(0).min(total - prefix)
    val maskedCount = total - prefix - suffix

    if maskedCount <= 0 then replacementFor(chars, total)
    else
      new String(points.take(prefix).flatMap(Character.toChars).toArray) +
        replacementFor(chars, maskedCount) +
        new String(points.takeRight(suffix).flatMap(Character.toChars).toArray)
  }

  /** `count` replacement code points, cycling through `chars`.
    *
    * Cycling is Kafbat's behaviour, and it is one replacement code point per masked input code point — never
    * more — which is what keeps the result no longer than the input.
    */
  private def replacementFor(chars: String, count: Int): String =
    if chars.isEmpty then "*" * count
    else {
      val cycle = codePoints(chars)
      (0 until count)
        .map(index => new String(Character.toChars(cycle(index % cycle.length))))
        .mkString
    }

  /** Code points, not `Char`s. A `Char` is half of an emoji, and half of an emoji is invalid text.
    *
    * Walked with `unfold` rather than `String.codePoints()`. That method returns a
    * `java.util.stream.IntStream` and Scala.js has no `java.util.stream` at all: this module is
    * cross-compiled, so the obvious one-liner compiles perfectly on the JVM and fails at *link* time for the
    * browser build — which is only discovered by running the JS suite, not by compiling. `codePointAt` and
    * `charCount` exist on both platforms.
    */
  private def codePoints(text: String): Vector[Int] =
    Vector.unfold(0) { index =>
      Option.when(index < text.length) {
        val point = text.codePointAt(index)
        (point, index + Character.charCount(point))
      }
    }
}
