package kui.identity.application

import cats.effect.kernel.{Clock, Sync}
import cats.syntax.all.*

import kui.identity.domain.*
import kui.kernel.error.{ApplicationError, DomainError, ErrorCode, KuiError}
import kui.kernel.{Secret, UserName}
import kui.security.audit.{AuthAuditSink, AuthenticationEvent, AuthenticationRecord, MutationOutcome}
import kui.security.{Principal, PrincipalKind}

/** Setting a new password, and finishing the forced change a first sign-in requires (AU-002).
  *
  * ==Why it takes a challenge and not a username==
  *
  * The challenge comes from [[LoginUseCase]], which issued it only after verifying the current password. So
  * reaching this use case at all is proof that the caller knew the password a moment ago, and the endpoint
  * cannot be used to set somebody else's password by naming them. It is single-use and expires in minutes
  * ([[SingleUseTokens]]), so a challenge left in a browser's history is worthless.
  *
  * ==Why it does not check the old password again==
  *
  * Because the challenge *is* that check, and asking twice would mean the endpoint accepts a username and a
  * password — which is a second login endpoint with different rate limiting and different logging. One way
  * in, not two.
  */
final class ChangePasswordUseCase[F[_]: Sync](
    config: IdentityConfig,
    users: UserDirectory[F],
    hasher: PasswordHasher[F],
    challenges: SingleUseTokens[F, UserName],
    audit: AuthAuditSink[F]
) {

  def apply(challenge: Secret[String], newPassword: Secret[String]): F[Either[KuiError, Unit]] =
    if config.mode != AuthMode.Form then
      Sync[F]
        .pure(
          ApplicationError
            .Unsupported(s"changing a KUI password when kui.auth.type is '${config.mode.wire}'")
            .asLeft[Unit]
        )
    else
      PasswordRules.check(newPassword) match {
        case Left(broken) => Sync[F].pure(DomainError.fromValidation(broken).asLeft[Unit])
        case Right(accepted) => redeem(challenge, accepted)
      }

  private def redeem(challenge: Secret[String], newPassword: Secret[String]): F[Either[KuiError, Unit]] =
    for {
      now <- Clock[F].realTimeInstant
      claimed <- challenges.redeem(challenge, now)
      result <- claimed match {
        case None => Sync[F].pure(ChangePasswordUseCase.Refusal.asLeft[Unit])
        case Some(name) =>
          users.find(name.value).flatMap {
            // The account was there a moment ago and is not now — a configuration reload between the
            // login and the change. Refused with the same sentence as an invalid challenge: the caller
            // can do nothing different either way, and the two are not worth distinguishing to them.
            case None => Sync[F].pure(ChangePasswordUseCase.Refusal.asLeft[Unit])
            case Some(account) =>
              for {
                hashed <- hasher.hash(newPassword)
                written <- users.update(account.withPassword(hashed))
                result <- written match {
                  case Left(refused) =>
                    // The challenge is already spent. The audit record says the save failed, and the caller
                    // is told the store's own reason — a conflict, a store that is down, or no store at all —
                    // rather than one "unsupported" code for every refusal.
                    audit
                      .record(
                        AuthenticationRecord(
                          now,
                          AuthenticationEvent.PasswordChange,
                          account.name.value,
                          Principal(account.name, Set.empty, PrincipalKind.Session),
                          MutationOutcome.Failed,
                          Map.empty
                        )
                      )
                      .as(ChangePasswordUseCase.refusal(refused).asLeft[Unit])

                  case Right(()) =>
                    audit
                      .record(
                        AuthenticationRecord(
                          now,
                          AuthenticationEvent.PasswordChange,
                          account.name.value,
                          Principal(account.name, Set.empty, PrincipalKind.Session),
                          MutationOutcome.Succeeded,
                          Map.empty
                        )
                      )
                      .as(().asRight[KuiError])
                }
              } yield result
          }
      }
    } yield result
}

object ChangePasswordUseCase {

  /** A challenge that was never issued, has already been used, or has expired. One sentence for all three,
    * because the caller's next step is the same in every case: sign in again.
    */
  val Refusal: KuiError =
    ApplicationError.Unauthenticated("that password change is no longer valid; sign in again")

  /** What the caller is told when the directory could not save the new password.
    *
    * The challenge has already been redeemed, so none of these sentences invite repeating this request. A
    * version conflict means the account changed under this attempt. A store that could not be reached, or a
    * write whose read-back timed out, may have landed; both use the retryable store-unavailable code, and the
    * message says to sign in again before another attempt. A deployment with no store keeps the unsupported
    * refusal whose message says what to configure. Any other store code is passed through.
    */
  private def refusal(refused: UpdateRefused): KuiError =
    refused.code match {
      case ErrorCode.ConfigVersionConflict =>
        ApplicationError.Refused(
          ErrorCode.ConfigVersionConflict,
          "that account's password changed elsewhere; sign in again"
        )
      case ErrorCode.StoreUnavailable | ErrorCode.Timeout =>
        ApplicationError.Refused(
          ErrorCode.StoreUnavailable,
          "the password change may have been applied; sign in again before trying another"
        )
      case ErrorCode.StoreNotConfigured | ErrorCode.Unsupported =>
        ApplicationError.Refused(ErrorCode.Unsupported, refused.message)
      case other =>
        ApplicationError.Refused(other, refused.message)
    }
}
