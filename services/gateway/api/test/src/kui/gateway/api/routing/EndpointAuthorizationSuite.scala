package kui.gateway.api.routing

import munit.FunSuite

import kui.contracts.rbac.{EndpointAuthorization, NameSource}

/** Every endpoint the gateway proxies declares what permission it needs.
  *
  * This is the enumeration the declaration exists for. The gateway is the one module that can see every
  * service's published contract at once, so it is the only place the question "is anything unprotected?" can
  * be asked in full — and asked at build time, which is the difference between a rule and a hope.
  *
  * An endpoint added without a declaration fails here. It would also be refused at runtime, deliberately, but
  * a build failure names the endpoint and arrives before anyone deploys it.
  */
final class EndpointAuthorizationSuite extends FunSuite {

  private val proxied =
    ServiceContracts.byService.toList.flatMap { case (service, endpoints) =>
      endpoints.map(service -> _)
    }

  test("everyProxiedEndpointDeclaresItsPermission") {
    val undeclared = proxied.collect {
      case (service, endpoint) if EndpointAuthorization.of(endpoint).isEmpty =>
        s"${service.value}: ${endpoint.info.name.getOrElse(endpoint.showShort)}"
    }

    assertEquals(
      undeclared,
      Nil,
      "these endpoints carry no authorization declaration, so the gateway cannot decide whether a " +
        "caller may reach them and will refuse every call to them"
    )
  }

  test("everyDeclarationNamesTheOperationTheEndpointIsNamedFor") {
    // Not equality: several endpoints share one operation on purpose — a plan and the apply it precedes
    // are one operation in two hops, and the audit trail should say so. What must not happen is a
    // declaration whose operation names nothing at all.
    val empty = proxied.collect {
      case (_, endpoint) if EndpointAuthorization.of(endpoint).exists(_.operation.isBlank) =>
        endpoint.info.name.getOrElse(endpoint.showShort)
    }

    assertEquals(empty, Nil)
  }

  test("everyPathNamedRequirementNamesAParameterItsEndpointActuallyHas") {
    // A requirement pointing at a path parameter that does not exist is refused at runtime, every time,
    // with a message about the code rather than about the caller. That is the right behaviour and a
    // terrible way to find out, so the typo is caught here instead.
    val broken = proxied.flatMap { case (_, endpoint) =>
      EndpointAuthorization
        .of(endpoint)
        .toList
        .flatMap(_.requirements)
        .collect { case requirement =>
          requirement.name match {
            case NameSource.PathParam(param) =>
              val sample = List.fill(EndpointAuthorization.pathLength(endpoint))("x")
              Option.when(EndpointAuthorization.pathValue(endpoint, param, sample).isEmpty)(
                s"${endpoint.info.name.getOrElse(endpoint.showShort)} names '$param'"
              )
            case _ => None
          }
        }
        .flatten
    }

    assertEquals(broken, Nil)
  }

  test("theEndpointsThatDependOnTheirServiceForTheNameCheckAreTheOnesWeExpect") {
    // A body-named requirement is checked only coarsely at the edge (EndpointDecision explains why), so
    // the list of endpoints in that position is worth pinning down: it should change when somebody adds
    // an endpoint that names its resource in the body, and a reviewer should see it change.
    val bodyNamed = proxied.flatMap { case (_, endpoint) =>
      EndpointAuthorization
        .of(endpoint)
        .toList
        .flatMap(_.requirements)
        .collect { case requirement =>
          requirement.name match {
            case NameSource.RequestBody(field) =>
              Some(s"${endpoint.info.name.getOrElse(endpoint.showShort)}.$field")
            case _ => None
          }
        }
        .flatten
    }

    assertEquals(
      bodyNamed.sorted,
      List("message.resend.toTopic", "message.track.topics", "topic.create.name")
    )
  }
}
