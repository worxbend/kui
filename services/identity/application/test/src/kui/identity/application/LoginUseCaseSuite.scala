package kui.identity.application

import cats.effect.{IO, Ref}

import kui.identity.application.IdentityFixtures.*
import kui.identity.domain.{AuthMode, Credentials, UpdateRefused, UserDirectory, UserRecord}
import kui.kernel.error.ErrorCode
import kui.kernel.{RoleName, Secret, UserName}
import kui.security.audit.{AuthenticationEvent, MutationOutcome}
import kui.security.rbac.RbacPolicy
import kui.security.{Principal as SecurityPrincipal, PrincipalKind}
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** What signing in with a password does, and — more importantly — what it refuses to do.
  *
  * Every case below is a rule somebody could remove without breaking a screen, which is exactly why they are
  * written down: the refusals are invisible until the day they matter.
  */
final class LoginUseCaseSuite extends KuiIOSuite {

  private def rig(
      accounts: List[kui.identity.domain.UserRecord],
      config: IdentityConfig = formConfig()
  ): IO[(LoginUseCase[IO], FakeDirectory, RecordingAudit, FakeHasher, SingleUseTokens[IO, UserName])] =
    for {
      users <- FakeDirectory.make(accounts)
      audit <- RecordingAudit.make
      hasher <- FakeHasher.make
      challenges <- SingleUseTokens.make[IO, UserName]()
      logger <- FakeStructuredLogger[IO]
    } yield (
      new LoginUseCase[IO](config, users, hasher, challenges, audit, logger),
      users,
      audit,
      hasher,
      challenges
    )

  test("the right password signs the person in, as a session principal") {
    for {
      (login, _, _, _, _) <- rig(List(account()))
      result <- login(Credentials("ada", Secret("correct horse battery staple")))
    } yield result match {
      case Right(LoginResult.SignedIn(principal)) =>
        assertEquals(principal.name, Ada)
        assertEquals(principal.kind, PrincipalKind.Session)
      case other => fail(s"expected a signed-in principal, got $other")
    }
  }

  test("the login name is matched case-insensitively") {
    for {
      (login, _, _, _, _) <- rig(List(account()))
      result <- login(Credentials("ADA", Secret("correct horse battery staple")))
    } yield assert(result.isRight, result.toString)
  }

  test("roles are resolved once, at login, from the deployment's policy") {
    for {
      (login, _, _, _, _) <- rig(
        List(account(groups = Set("platform"))),
        formConfig(Policy)
      )
      result <- login(Credentials("ada", Secret("correct horse battery staple")))
    } yield result match {
      case Right(LoginResult.SignedIn(principal)) =>
        assertEquals(principal.roles, Set(RoleName.unsafe("operators")))
      case other => fail(s"expected a signed-in principal, got $other")
    }
  }

  test("a group that no role names yields no roles, rather than a failure") {
    for {
      (login, _, _, _, _) <- rig(List(account(groups = Set("interns"))), formConfig(Policy))
      result <- login(Credentials("ada", Secret("correct horse battery staple")))
    } yield result match {
      case Right(LoginResult.SignedIn(principal)) => assertEquals(principal.roles, Set.empty[RoleName])
      case other => fail(s"expected a signed-in principal, got $other")
    }
  }

  test("a wrong password and an unknown account are refused with the same sentence") {
    for {
      (login, _, _, _, _) <- rig(List(account()))
      wrongPassword <- login(Credentials("ada", Secret("hunter2")))
      unknownUser <- login(Credentials("grace", Secret("correct horse battery staple")))
    } yield {
      assertEquals(wrongPassword.left.toOption.map(_.message), Some(LoginUseCase.Refusal.message))
      assertEquals(unknownUser.left.toOption.map(_.message), Some(LoginUseCase.Refusal.message))
      assertEquals(wrongPassword.left.toOption.map(_.code), Some(ErrorCode.Unauthenticated))
    }
  }

  test("an unknown account still costs a password verification, so timing does not enumerate accounts") {
    for {
      (login, _, _, hasher, _) <- rig(List(account()))
      _ <- login(Credentials("grace", Secret("anything")))
      calls <- hasher.calls.get
    } yield assertEquals(calls, 1)
  }

  test("an account that must change its password gets a challenge and no principal") {
    for {
      (login, _, _, _, challenges) <- rig(List(account(mustChangePassword = true)))
      result <- login(Credentials("ada", Secret("correct horse battery staple")))
      redeemed <- result match {
        case Right(LoginResult.MustChangePassword(challenge)) => challenges.redeem(challenge, Now)
        case other => fail(s"expected a required password change, got $other")
      }
    } yield assertEquals(redeemed, Some(Ada))
  }

  test("signing in when authentication is disabled is refused, not quietly allowed") {
    for {
      (login, _, _, _, _) <- rig(
        List(account()),
        IdentityConfig(AuthMode.Disabled, None, RbacPolicy.Disabled)
      )
      result <- login(Credentials("ada", Secret("correct horse battery staple")))
    } yield assertEquals(result.left.toOption.map(_.code), Some(ErrorCode.Unsupported))
  }

  test("signing in when the deployment uses a provider is refused for the same reason") {
    for {
      (login, _, _, _, _) <- rig(
        List(account()),
        IdentityConfig(AuthMode.Oidc, None, RbacPolicy.Disabled)
      )
      result <- login(Credentials("ada", Secret("correct horse battery staple")))
    } yield assertEquals(result.left.toOption.map(_.code), Some(ErrorCode.Unsupported))
  }

  // -----------------------------------------------------------------------------------------------
  // The audit trail
  // -----------------------------------------------------------------------------------------------

  test("every outcome leaves exactly one audit record") {
    for {
      (login, _, audit, _, _) <- rig(List(account()))
      _ <- login(Credentials("ada", Secret("correct horse battery staple")))
      _ <- login(Credentials("ada", Secret("hunter2")))
      entries <- audit.entries.get
    } yield {
      assertEquals(entries.size, 2)
      assertEquals(entries.map(_.event), List(AuthenticationEvent.Login, AuthenticationEvent.Login))
      assertEquals(entries.map(_.outcome), List(MutationOutcome.Succeeded, MutationOutcome.Refused))
    }
  }

  test("a refused sign-in records the name that was attempted and nobody as the principal") {
    for {
      (login, _, audit, _, _) <- rig(List(account()))
      _ <- login(Credentials("root", Secret("hunter2")))
      entries <- audit.entries.get
    } yield {
      assertEquals(entries.map(_.subject), List("root"))
      assertEquals(entries.map(_.principal), List(SecurityPrincipal.Anonymous))
    }
  }

  test("no audit record carries the password or a reason a refusal could be enumerated from") {
    for {
      (login, _, audit, _, _) <- rig(List(account()))
      _ <- login(Credentials("root", Secret("hunter2")))
      _ <- login(Credentials("ada", Secret("hunter2")))
      entries <- audit.entries.get
    } yield {
      val rendered = entries.map(entry => (entry.detail.values.toList :+ entry.subject).mkString(" "))
      assert(rendered.forall(!_.contains("hunter2")), rendered.toString)
      // "no such user" and "wrong password" must be indistinguishable in the trail.
      assert(rendered.forall(line => !line.contains("password") && !line.contains("user")), rendered.toString)
      assertEquals(entries.map(_.outcome).distinct, List(MutationOutcome.Refused))
    }
  }

  test("a successful sign-in records the principal, and how many roles it resolved") {
    for {
      (login, _, audit, _, _) <- rig(List(account(groups = Set("platform"))), formConfig(Policy))
      _ <- login(Credentials("ada", Secret("correct horse battery staple")))
      entries <- audit.entries.get
    } yield {
      assertEquals(entries.map(_.principal.name), List(Ada))
      assertEquals(entries.flatMap(_.detail.get("roles")), List("1"))
    }
  }
}

/** What the forced password change does, and what it will not do without proof. */
final class ChangePasswordUseCaseSuite extends KuiIOSuite {

  private def rig(
      writable: Boolean = true
  ): IO[(LoginUseCase[IO], ChangePasswordUseCase[IO], FakeDirectory, RecordingAudit)] =
    for {
      users <- FakeDirectory.make(List(account(mustChangePassword = true)), writable)
      audit <- RecordingAudit.make
      hasher <- FakeHasher.make
      challenges <- SingleUseTokens.make[IO, UserName]()
      logger <- FakeStructuredLogger[IO]
      config = formConfig()
    } yield (
      new LoginUseCase[IO](config, users, hasher, challenges, audit, logger),
      new ChangePasswordUseCase[IO](config, users, hasher, challenges, audit),
      users,
      audit
    )

  private def challengeFrom(result: Either[kui.kernel.error.KuiError, LoginResult]): Secret[String] =
    result match {
      case Right(LoginResult.MustChangePassword(challenge)) => challenge
      case other => fail(s"expected a required password change, got $other")
    }

  test("a challenge from a login sets the new password and clears the forced change") {
    for {
      (login, change, users, _) <- rig()
      first <- login(Credentials("ada", Secret("correct horse battery staple")))
      changed <- change(challengeFrom(first), Secret("a much longer new password"))
      stored <- users.snapshot
      second <- login(Credentials("ada", Secret("a much longer new password")))
    } yield {
      assertEquals(changed, Right(()))
      assertEquals(stored.get("ada").map(_.mustChangePassword), Some(false))
      assert(
        second.exists {
          case LoginResult.SignedIn(_) => true
          case _ => false
        },
        second.toString
      )
    }
  }

  test("a challenge is single use: the second attempt is refused") {
    for {
      (login, change, _, _) <- rig()
      first <- login(Credentials("ada", Secret("correct horse battery staple")))
      challenge = challengeFrom(first)
      _ <- change(challenge, Secret("a much longer new password"))
      again <- change(challenge, Secret("another much longer password"))
    } yield assertEquals(again.left.toOption.map(_.message), Some(ChangePasswordUseCase.Refusal.message))
  }

  test("a challenge nobody issued is refused") {
    for {
      (_, change, _, _) <- rig()
      result <- change(Secret("invented"), Secret("a much longer new password"))
    } yield assertEquals(result.left.toOption.map(_.code), Some(ErrorCode.Unauthenticated))
  }

  test("a new password that breaks the length rule is refused before anything is redeemed") {
    for {
      (login, change, users, _) <- rig()
      first <- login(Credentials("ada", Secret("correct horse battery staple")))
      challenge = challengeFrom(first)
      tooShort <- change(challenge, Secret("short"))
      // The challenge survives, because it was never redeemed: the caller can try again with a longer one.
      retried <- change(challenge, Secret("a much longer new password"))
      stored <- users.snapshot
    } yield {
      assertEquals(tooShort.left.toOption.map(_.code), Some(ErrorCode.Validation))
      assertEquals(retried, Right(()))
      assertEquals(stored.get("ada").map(_.mustChangePassword), Some(false))
    }
  }

  test("a store conflict and an unreachable store are not reported as unsupported") {
    // The challenge is spent before the write. A version conflict used to come back as 501
    // KUI-UNSUPPORTED, and so did a store that could not be reached or a write that timed out.
    val account = IdentityFixtures.account(mustChangePassword = true)
    val conflict = UpdateRefused("re-read it and apply the change again", ErrorCode.ConfigVersionConflict)
    val down = UpdateRefused("the metadata store could not be used", ErrorCode.StoreUnavailable)
    val timedOut = UpdateRefused(
      "it may still have been applied, so re-read the record before retrying",
      ErrorCode.Timeout
    )
    val unconfigured = UpdateRefused(
      "no metadata store is configured; set kui.store.kafka.* to enable it",
      ErrorCode.StoreNotConfigured
    )

    def attempt(
        login: LoginUseCase[IO],
        change: ChangePasswordUseCase[IO]
    ): IO[Either[kui.kernel.error.KuiError, Unit]] =
      login(Credentials("ada", Secret("correct horse battery staple"))).flatMap { first =>
        change(challengeFrom(first), Secret("a much longer new password"))
      }

    for {
      refusals <- Ref.of[IO, List[UpdateRefused]](List(conflict, down, timedOut, unconfigured))
      users = new UserDirectory[IO] {
        def find(username: String): IO[Option[UserRecord]] =
          IO.pure(Option.when(username.trim.equalsIgnoreCase(account.name.value))(account))

        def update(record: UserRecord): IO[Either[UpdateRefused, Unit]] =
          refusals.modify {
            case next :: rest => (rest, Left(next))
            case Nil => (Nil, Left(UpdateRefused("the scripted directory ran out of refusals")))
          }
      }
      audit <- RecordingAudit.make
      hasher <- FakeHasher.make
      challenges <- SingleUseTokens.make[IO, UserName]()
      logger <- FakeStructuredLogger[IO]
      login = new LoginUseCase[IO](formConfig(), users, hasher, challenges, audit, logger)
      change = new ChangePasswordUseCase[IO](formConfig(), users, hasher, challenges, audit)
      conflicted <- attempt(login, change)
      unavailable <- attempt(login, change)
      timed <- attempt(login, change)
      missing <- attempt(login, change)
      entries <- audit.entries.get
    } yield {
      assertEquals(conflicted.left.toOption.map(_.code), Some(ErrorCode.ConfigVersionConflict))
      assertEquals(
        conflicted.left.toOption.map(_.message),
        Some("that account's password changed elsewhere; sign in again")
      )
      assertEquals(unavailable.left.toOption.map(_.code), Some(ErrorCode.StoreUnavailable))
      assert(unavailable.left.exists(_.code.retryable), unavailable.toString)
      assert(
        unavailable.left.toOption.exists(_.message.contains("may have been applied")),
        unavailable.toString
      )
      assertEquals(timed.left.toOption.map(_.code), Some(ErrorCode.StoreUnavailable))
      assert(timed.left.toOption.exists(_.message.contains("may have been applied")), timed.toString)
      assertEquals(missing.left.toOption.map(_.code), Some(ErrorCode.Unsupported))
      assert(missing.left.toOption.exists(_.message.contains("kui.store")), missing.toString)
      assertEquals(
        entries.filter(_.event == AuthenticationEvent.PasswordChange).map(_.outcome),
        List.fill(4)(MutationOutcome.Failed)
      )
    }
  }

  test("a deployment with nowhere to save the change says so, and says what to configure") {
    for {
      (login, change, _, audit) <- rig(writable = false)
      first <- login(Credentials("ada", Secret("correct horse battery staple")))
      result <- change(challengeFrom(first), Secret("a much longer new password"))
      entries <- audit.entries.get
    } yield {
      assertEquals(result.left.toOption.map(_.code), Some(ErrorCode.Unsupported))
      assert(result.left.toOption.exists(_.message.contains("file")), result.toString)
      // The failed attempt is in the trail too: an audit that only records successes cannot answer
      // "did anybody try to change that password".
      assertEquals(
        entries.filter(_.event == AuthenticationEvent.PasswordChange).map(_.outcome),
        List(MutationOutcome.Failed)
      )
    }
  }

  test("changing a password when authentication is disabled is refused") {
    for {
      users <- FakeDirectory.make(List(account()))
      audit <- RecordingAudit.make
      hasher <- FakeHasher.make
      challenges <- SingleUseTokens.make[IO, UserName]()
      change = new ChangePasswordUseCase[IO](
        IdentityConfig(AuthMode.Disabled, None, RbacPolicy.Disabled),
        users,
        hasher,
        challenges,
        audit
      )
      result <- change(Secret("anything"), Secret("a much longer new password"))
    } yield assertEquals(result.left.toOption.map(_.code), Some(ErrorCode.Unsupported))
  }
}

/** The permission answer, which is the one thing four microfrontends all read. */
final class PermissionsUseCaseSuite extends KuiIOSuite {

  private val anonymous: SecurityPrincipal = SecurityPrincipal.Anonymous

  test("a deployment with no roles answers a wildcard grant per resource, not an empty list") {
    val permissions = new PermissionsUseCase[IO](IdentityConfig.Disabled)

    permissions(anonymous).map { answer =>
      val grants = answer.fold(error => fail(error.message), identity)
      assertEquals(grants.size, kui.security.rbac.Resource.values.length)
      assert(grants.forall(_.clusters == kui.security.rbac.ClusterScope.Every), grants.toString)
    }
  }

  test("a deployment with roles answers what the principal's roles grant") {
    val permissions = new PermissionsUseCase[IO](formConfig(Policy))
    val operator = SecurityPrincipal(Ada, Set(RoleName.unsafe("operators")), PrincipalKind.Session)

    permissions(operator).map { answer =>
      val grants = answer.fold(error => fail(error.message), identity)
      assertEquals(grants.map(_.permission.resource), List(kui.security.rbac.Resource.Topic))
    }
  }

  test("a principal whose roles grant nothing gets nothing, which is different from getting everything") {
    val permissions = new PermissionsUseCase[IO](formConfig(Policy))

    permissions(anonymous).map(answer =>
      assertEquals(answer.fold(error => fail(error.message), identity), Nil)
    )
  }

  test("the settings answer carries no credential, only a mode, a label and a flag") {
    val settings = new SettingsUseCase[IO](
      IdentityConfig(AuthMode.Oidc, Some(ProviderSummary("Example")), Policy)
    )

    settings().map { answer =>
      val value = answer.fold(error => fail(error.message), identity)
      assertEquals(value, AuthSettings(AuthMode.Oidc, Some(ProviderSummary("Example")), rbacEnabled = true))
    }
  }
}
