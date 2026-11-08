package kui.kernel.error

import java.time.Instant

import kui.kernel.ValidationError

/** One field of a request and everything that is wrong with it.
  *
  * `field` is optional because a validation failure is sometimes about the request as a whole rather than
  * about one named field. `restrictions` is a list rather than one string because a single field can break
  * more than one rule at a time, and telling a user about one rule per round trip is how a form becomes
  * hostile.
  */
final case class FieldError(field: Option[String], restrictions: List[String])

object FieldError {

  def of(field: String, restriction: String): FieldError =
    FieldError(Some(field), List(restriction))

  /** The validation failures of `libs/kernel`'s smart constructors, in the shape the wire wants. */
  def fromValidation(error: ValidationError): FieldError =
    FieldError(Some(error.fieldName), List(error.message))

  given CanEqual[FieldError, FieldError] = CanEqual.derived
}

/** Every failure KUI returns, as a value.
  *
  * Business code returns `F[Either[KuiError, A]]` and never throws: an exception that crosses a layer
  * boundary carries a stack trace, a class name and often an upstream response body, none of which belong in
  * an HTTP response, and all of which end up there by accident the first time nobody catches it. Adapters
  * translate the exceptions their libraries throw into these cases at the boundary (ADR-034); genuinely fatal
  * errors still propagate.
  *
  * The three branches say where a failure came from, which is what decides who can fix it:
  *
  *   - [[DomainError]] — a business rule was broken. The request was understood and refused.
  *   - [[ApplicationError]] — the use case cannot proceed: the thing is missing, the caller is not allowed,
  *     the state is wrong.
  *   - [[InfrastructureError]] — something KUI depends on failed. The user did nothing wrong.
  *
  * **`message` is display text.** It must never contain a stack trace, an upstream response body, a JAAS
  * configuration string, a URL with credentials, or the contents of a `Secret`. That is why
  * `InfrastructureError.Upstream` carries a status code and no body: a field that does not exist cannot be
  * logged by mistake.
  */
sealed trait KuiError {

  /** The stable code an operator searches for and the browser switches on. */
  def code: ErrorCode

  /** One sentence, safe to show a user. */
  def message: String

  /** Per-field detail, populated for validation failures and empty for everything else. */
  def details: List[FieldError] = Nil
}

/** A business rule was broken. Services add their own cases in their `domain` modules; the kernel only
  * carries the one that every service needs, because every service validates input.
  */
sealed trait DomainError extends KuiError

object DomainError {

  /** A rule that the domain, not the transport, defines: an offset range that runs backwards, a replication
    * factor above the broker count, a reset spec that names a partition the group does not own.
    */
  final case class InvariantViolation(rule: String, fields: List[FieldError] = Nil) extends DomainError {
    val code: ErrorCode = ErrorCode.Validation
    val message: String = rule
    override val details: List[FieldError] = fields
  }

  /** Lifts a smart constructor's rejection into the error hierarchy, so that a value object's rule and a use
    * case's rule reach the wire through the same path.
    */
  def fromValidation(error: ValidationError): DomainError =
    InvariantViolation(error.message, List(FieldError.fromValidation(error)))
}

/** The use case cannot proceed. Nothing is broken; the request cannot be carried out as asked. */
sealed trait ApplicationError extends KuiError

object ApplicationError {

  /** Something that was addressed by id is not there. The code is a parameter because "not found" has one
    * code per kind of thing (`KUI-TOPIC-NOT-FOUND`, `KUI-CLUSTER-NOT-FOUND`, …) and the caller knows which
    * kind it was looking for.
    */
  final case class NotFound(what: String, id: String, code: ErrorCode) extends ApplicationError {
    val message: String = s"$what '$id' does not exist"
  }

  /** The request collides with the current state — a concurrent change, a duplicate name. */
  final case class Conflict(message: String) extends ApplicationError {
    val code: ErrorCode = ErrorCode.InvalidState
  }

  final case class Forbidden(message: String) extends ApplicationError {
    val code: ErrorCode = ErrorCode.Forbidden
  }

  final case class Unauthenticated(message: String) extends ApplicationError {
    val code: ErrorCode = ErrorCode.Unauthenticated
  }

  /** This deployment, cluster or upstream cannot do the thing at all — an older broker, a cluster with no
    * schema registry configured. Distinct from `Forbidden`, which is about permission.
    */
  final case class Unsupported(feature: String) extends ApplicationError {
    val code: ErrorCode = ErrorCode.Unsupported
    val message: String = s"$feature is not supported here"
  }

  final case class InvalidState(message: String) extends ApplicationError {
    val code: ErrorCode = ErrorCode.InvalidState
  }

  /** A refusal whose *code* the caller chooses, the way [[NotFound]] already lets it choose one.
    *
    * `InvalidState` covers "the target is not in a state where this is allowed", which is true of a dozen
    * refusals and actionable for none of them. Some refusals have an obvious remedy and deserve a code the UI
    * can switch on to say what it is: `KUI-GROUP-NOT-EMPTY` means "stop the consumers and try again", and a
    * screen that renders that sentence is worth more than one that renders "invalid state".
    *
    * The code is a parameter rather than a case per refusal because the alternative is a case in the kernel
    * for every adapter's vocabulary, and the kernel does not know what a consumer group is.
    */
  final case class Refused(code: ErrorCode, message: String) extends ApplicationError

  /** A smart-filter expression the user typed could not be compiled (ADR-017).
    *
    * It is separate from [[Invalid]] because it carries its own code, `KUI-FILTER-COMPILE`, which the browser
    * switches on to underline the expression in the editor rather than to show a form error. `fields` carries
    * one entry per compile issue, each naming a line and a column: an error that says only "syntax error"
    * sends the user back to re-read a line they have already read three times.
    *
    * It lives in the kernel rather than in `libs/filter` because `ApplicationError` is sealed, and it is
    * sealed so that `ErrorEnvelope.statusOf` can be exhaustive.
    */
  final case class FilterCompile(message: String, fields: List[FieldError]) extends ApplicationError {
    val code: ErrorCode = ErrorCode.FilterCompile
    override val details: List[FieldError] = fields
  }

  /** The request itself is malformed. `fields` becomes the `details` array of the envelope. */
  final case class Invalid(message: String, fields: List[FieldError]) extends ApplicationError {
    val code: ErrorCode = ErrorCode.Validation
    override val details: List[FieldError] = fields
  }

  /** A business failure another KUI process already classified, carried across the boundary verbatim.
    *
    * The gateway proxies calls to services and has to hand the caller back what the service said. Every other
    * case here computes its own `message` from its fields, which is right when this process is the one that
    * noticed the problem and wrong when it is not: re-deriving "topic 'orders' does not exist" from a code
    * alone is impossible, and picking the nearest constructor loses the topic name the user needs to read.
    *
    * It is an `ApplicationError` and not an `InfrastructureError` because that split is what the capability
    * registry keys on (ADR-039 §6): a business failure must never dim a capability, whichever process decided
    * it was one.
    */
  final case class Remote(code: ErrorCode, message: String, fields: List[FieldError])
      extends ApplicationError {
    override val details: List[FieldError] = fields
  }
}

/** Something KUI depends on failed: a broker, a schema registry, a Connect cluster, ksqlDB. */
sealed trait InfrastructureError extends KuiError

object InfrastructureError {

  /** The upstream could not be reached at all. `cause` is kept for the log; `message` names only the
    * upstream, because a connection failure's text routinely contains hosts, ports and credentials.
    */
  final case class Unreachable(upstream: String, cause: String) extends InfrastructureError {
    val code: ErrorCode = ErrorCode.UpstreamUnavailable
    val message: String = s"$upstream could not be reached"
  }

  /** The upstream did not answer inside the time it was given.
    *
    * `afterMs <= 0` means the caller does not know that budget. The message then omits a duration, because
    * "within 0ms" would tell an operator the call was given no time at all.
    */
  final case class Timeout(operation: String, afterMs: Long) extends InfrastructureError {
    val code: ErrorCode = ErrorCode.Timeout
    val message: String =
      if afterMs > 0L then s"$operation did not finish within ${afterMs}ms"
      else s"$operation did not finish"
  }

  /** KUI's own credentials for the upstream were rejected. Not the user's problem, and never retryable: the
    * configuration has to change first.
    */
  final case class AuthFailed(upstream: String) extends InfrastructureError {
    val code: ErrorCode = ErrorCode.UpstreamAuth
    val message: String = s"KUI is not authenticated against $upstream"
  }

  /** The upstream answered, with a status KUI cannot use. It carries the status and deliberately not the
    * body: an upstream body is the most reliable way to leak a secret into a user-visible response (ADR-034).
    */
  final case class Upstream(upstream: String, status: Int) extends InfrastructureError {
    val code: ErrorCode = ErrorCode.UpstreamUnavailable
    val message: String = s"$upstream answered with status $status"
  }

  /** The circuit breaker for this upstream is open, so KUI is not even trying (ADR-037). */
  final case class CircuitOpen(upstream: String, since: Instant) extends InfrastructureError {
    val code: ErrorCode = ErrorCode.UpstreamUnavailable
    val message: String = s"calls to $upstream are suspended while it recovers"
  }

  /** A configured serde exists and cannot work right now — almost always a Schema Registry that is not
    * answering (ADR-014, ADR-028).
    *
    * An infrastructure failure and not an application one, and that classification is load-bearing. ADR-039
    * §6 reports only infrastructure failures to the capability registry, so this is what dims the message
    * capability to `Degraded` for everyone; a user asking for a serde that was never configured is a bad
    * request and must not dim anything for anybody.
    *
    * It is distinct from `Unreachable` because the user-facing sentence is different: they picked a serde,
    * and what they need to be told is that *that serde* is unusable, not that some host is down.
    */
  final case class SerdeUnavailable(serde: String, reason: String) extends InfrastructureError {
    val code: ErrorCode = ErrorCode.SerdeUnavailable
    val message: String = s"the serde '$serde' cannot be used right now: $reason"
  }

  /** The infrastructure half of `ApplicationError.Remote`: a transport-level failure another KUI process
    * reported, carried across the boundary with its code and message intact.
    */
  final case class Remote(code: ErrorCode, message: String, fields: List[FieldError])
      extends InfrastructureError {
    override val details: List[FieldError] = fields
  }
}

object KuiError {

  /** The codes that describe a failure of a *system*, as opposed to a failure of a request.
    *
    * The list is short and closed on purpose. Everything not named here is a statement about what the caller
    * asked for — a bad field, a missing resource, a forbidden operation — and so is an `ApplicationError`.
    * Getting this classification wrong has a visible consequence: ADR-039 §6 reports only infrastructure
    * failures to the capability registry, so a code wrongly listed here would let any user dim a feature for
    * everyone else by typing a bad URL.
    */
  val InfrastructureCodes: Set[ErrorCode] =
    Set(
      ErrorCode.UpstreamUnavailable,
      ErrorCode.UpstreamAuth,
      ErrorCode.UpstreamKsql,
      ErrorCode.Timeout,
      ErrorCode.Internal,
      // KUI's own metadata store is an upstream like any other: when it cannot be reached, the features
      // that depend on it are genuinely unavailable and the capability that carries them should say so.
      //
      // Two neighbouring store codes are deliberately absent. `StoreNotConfigured` is a deployment
      // choice, not a failure — a deployment that runs from files has no store to lose. And
      // `ConfigVersionConflict` is a user's stale form. Either one listed here would let an ordinary
      // action dim a feature for everybody else, which is exactly what this classification exists to
      // prevent (ADR-039 §6).
      ErrorCode.StoreUnavailable,
      ErrorCode.StoreReplayTimeout,
      // A Schema Registry that is not answering is an upstream that is not answering: the serde picker
      // genuinely cannot offer Avro, and the capability should say so rather than let every user
      // rediscover it one topic at a time.
      ErrorCode.SerdeUnavailable
    )

  /** Rebuilds the error another KUI process reported, on the correct side of the application / infrastructure
    * split.
    *
    * Used by the gateway when it decodes an `ErrorEnvelope` from a service it called. Keeping the
    * classification here rather than at the call site means every consumer of a proxied error — the route
    * that answers the browser and the capability registry that decides whether to dim a feature — agrees
    * about what kind of failure it was.
    */
  def remote(code: ErrorCode, message: String, fields: List[FieldError] = Nil): KuiError =
    if InfrastructureCodes.contains(code) then InfrastructureError.Remote(code, message, fields)
    else ApplicationError.Remote(code, message, fields)
}
