package kui.identity.domain

import kui.kernel.error.ErrorCode
import kui.kernel.{Secret, UserName}

/** One account KUI itself knows about — the form-login mode's whole model of a person.
  *
  * @param name
  *   the login name. It is the audit trail's answer to "who", so it is a `UserName` rather than a string from
  *   the moment it is read.
  * @param hash
  *   the stored password. Never a password, and never printable: see [[PasswordHash]].
  * @param groups
  *   the groups this account is in, which is what a role's subjects match against. They are *not* roles: a
  *   role is a property of the deployment's policy and a group is a property of the person, and keeping them
  *   apart is what lets one role file describe a form deployment and an OIDC one.
  * @param mustChangePassword
  *   whether this person has to set a new password before they get a session at all. Enforced, not
  *   advertised: [[LoginOutcome.PasswordChangeRequired]] is what a login against such an account produces,
  *   and there is no path from it to an authenticated principal that does not go through a completed change.
  */
final case class UserRecord(
    name: UserName,
    hash: PasswordHash,
    groups: Set[String],
    mustChangePassword: Boolean
) {

  /** The same account with a new password and the forced change cleared. */
  def withPassword(next: PasswordHash): UserRecord =
    copy(hash = next, mustChangePassword = false)
}

object UserRecord {
  given CanEqual[UserRecord, UserRecord] = CanEqual.derived
}

/** What somebody typed into a login form.
  *
  * The password is a `Secret` from the moment it is decoded and is never anything else: it is not in an audit
  * record, not in a log line, not in an error message, and `Secret`'s own `toString` is what makes that
  * mechanical rather than a matter of everybody remembering.
  */
final case class Credentials(username: String, password: Secret[String])

object Credentials {
  given CanEqual[Credentials, Credentials] = CanEqual.derived
}

/** Where the accounts live.
  *
  * A port, with one adapter today (the configured list) and an obvious second one later (a store-backed list
  * an administrator can edit, RB-004). `update` exists because the forced password change has to be able to
  * write, and because a directory that can only be read makes that flow impossible to finish.
  */
trait UserDirectory[F[_]] {

  /** The account with this login name, compared case-insensitively.
    *
    * Case-insensitive because a person who signs in as `Admin` on Monday and `admin` on Tuesday is the same
    * person, and any other answer is a support ticket rather than a security control.
    */
  def find(username: String): F[Option[UserRecord]]

  /** Replaces an account's stored password, or says why it cannot.
    *
    * `Either` rather than a raised exception because "this deployment has nowhere to keep a changed password"
    * is a fact an operator has to be told, not a bug: the configured-accounts adapter has no writable place
    * at all, and the store-backed one can be talking to a broker that is down. Both refusals reach the caller
    * as a sentence rather than as a 500.
    */
  def update(record: UserRecord): F[Either[UpdateRefused, Unit]]
}

/** Why an account could not be written.
  *
  * `message` is the sentence an operator can act on. `code` is the store's own error when the directory has
  * one, and [[ErrorCode.Unsupported]] when the deployment simply has nowhere to write. A version conflict and
  * a broker that is down are different facts; collapsing both into "unsupported" is what made a password
  * change during a store outage look like a missing feature.
  */
final case class UpdateRefused(message: String, code: ErrorCode = ErrorCode.Unsupported)

object UpdateRefused {
  given CanEqual[UpdateRefused, UpdateRefused] = CanEqual.derived
}

/** Turning a password into a stored hash, and checking one against it.
  *
  * Both directions are here rather than on [[PasswordHash]] because both need a key derivation function, and
  * a `domain` module that could reach for `javax.crypto` is one that eventually does.
  *
  * `verify` must be constant-time in the comparison of the derived keys. It is stated here, on the port,
  * because it is a property of the *contract* and not of one adapter: an implementation that returned as soon
  * as two bytes differed would turn "is this password right" into a measurable side channel.
  */
trait PasswordHasher[F[_]] {

  def hash(password: Secret[String]): F[PasswordHash]

  def verify(password: Secret[String], against: PasswordHash): F[Boolean]
}
