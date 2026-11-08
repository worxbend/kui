package kui.consumer.api

import java.nio.charset.StandardCharsets
import java.time.Instant

import cats.effect.IO
import io.circe.Printer
import io.circe.parser.parse
import io.circe.syntax.*
import sttp.client4.*
import sttp.model.StatusCode

import kui.consumer.application.{LagUpdate, SnapshotFreshness}
import kui.consumer.contract.dto.ResetPlanRequest
import kui.kernel.GroupId
import kui.kernel.error.InfrastructureError
import kui.kernel.group.{GroupState, ResetTarget}
import kui.testkit.KuiIOSuite

/** The order the router tries this service's routes in, asserted through the router rather than by reading
  * the list.
  *
  * ==Why this suite exists==
  *
  * `GET /clusters/{c}/consumer-groups/lag` and `GET /clusters/{c}/consumer-groups/{groupId}` match the same
  * URL. Whichever is registered first claims it. That would be an ordinary routing footnote except for one
  * property of Kafka: describing a group that does not exist does not fail. `Admin.describeConsumerGroups`
  * answers with a *fabricated* group in state `DEAD`, and the detail route is deliberately built to pass that
  * through, because a stale bookmark should land on an empty page rather than an error.
  *
  * Put those two together and the wrong order produces the worst kind of defect: a well-formed `200`, a body
  * that decodes, no error anywhere, and a lag column on the consumers screen that silently stops updating
  * because every poll is answered with the detail of a group called "lag". Nothing in the layers below can
  * see it — the use cases are both correct, and each one's own suite passes.
  *
  * So the assertion is not "the list is in this order". It is: the lag URL reaches the lag use case, and the
  * detail use case is never asked about a group named `lag`.
  */
final class ConsumerRoutesSuite extends KuiIOSuite {

  import ConsumerTestServer.*

  private def get(server: ConsumerTestServer, requestPath: String): IO[Response[Either[String, String]]] =
    token(requestPath).flatMap(principal =>
      basicRequest
        .get(uri"${uri(requestPath)}")
        .header(kui.contracts.KuiEndpoint.PrincipalHeader, principal.value)
        .send(server.backend)
    )

  test("theLagUrlReachesTheLagUseCaseAndNotTheDetailOne") {
    val update =
      LagUpdate(Group, totalLag = Some(9L), pace = None, state = GroupState.Stable, memberCount = 1)

    resource(lag = List(update)).use { (server, stubs) =>
      for {
        response <- get(server, path("/consumer-groups/lag"))
        asked <- stubs.detail.asked.get
        body = response.body.getOrElse(fail(s"the lag poll failed: ${response.body}"))
        json = parse(body).getOrElse(fail(s"the lag poll did not answer JSON: $body"))
      } yield {
        assertEquals(response.code, StatusCode.Ok)

        // The shape of a lag delta, which a group detail does not have. `token` is the marker: the
        // detail document has no field by that name, so a body carrying one came from the lag route.
        assert(
          json.hcursor.downField("token").succeeded,
          clue = s"expected a lag delta with a resync token, got $body"
        )
        assertEquals(
          json.hcursor.downField("changed").downArray.get[String]("groupId").toOption,
          Some(Group.value)
        )

        // The load-bearing half. Reaching the detail route at all means the route order regressed.
        assertEquals(
          asked,
          List.empty[GroupId],
          clue = "the detail use case was asked about a group; the lag route no longer claims /lag"
        )
      }
    }
  }

  test("aGroupWhoseIdIsLiterallyLagIsStillUnreachableAndThatIsTheAcceptedCost") {
    // Stated rather than left implicit: a real group called `lag` cannot be opened through this API,
    // because its URL is the poll endpoint's. That is the trade the ordering makes, and it is the right
    // way round — a lag column that silently stops is invisible, and a group that will not open is not.
    resource().use { (server, stubs) =>
      for {
        _ <- get(server, path("/consumer-groups/lag"))
        asked <- stubs.detail.asked.get
      } yield assertEquals(asked, List.empty[GroupId])
    }
  }

  test("anOrdinaryGroupIdStillReachesTheDetailRoute") {
    // The other half of the same fact: the ordering must not have swallowed every detail request.
    resource().use { (server, stubs) =>
      for {
        response <- get(server, path(s"/consumer-groups/${Group.value}"))
        asked <- stubs.detail.asked.get
      } yield {
        assertEquals(response.code, StatusCode.Ok)
        assertEquals(asked, List(Group))
      }
    }
  }

  test("anUnknownClusterIs404AndNotAnEmptyPage") {
    resource().use { (server, _) =>
      val requestPath = "/internal/v1/clusters/nowhere/consumer-groups"
      for {
        principal <- token(requestPath)
        response <- basicRequest
          .get(uri"${uri(requestPath)}")
          .header(kui.contracts.KuiEndpoint.PrincipalHeader, principal.value)
          .send(server.backend)
      } yield {
        assertEquals(response.code, StatusCode.NotFound)
        assert(
          response.body.swap.getOrElse("").contains("KUI-CLUSTER-NOT-FOUND"),
          clue = s"expected the cluster-not-found envelope, got ${response.body}"
        )
      }
    }
  }

  test("aListReadFromADeadClusterIsMarkedStaleOnTheWire") {
    // The defect this replaces: the list answered a bare 200 carrying the rows of the last successful
    // scrape, so a browser had no way to know that the lag figures in front of an operator had stopped
    // moving because the broker was gone rather than because the consumers had caught up. Lag is the
    // worst field in the product to freeze silently — a number that stops changing reads as health.
    val broker = InfrastructureError.Unreachable("kafka", "connection refused")

    resource(
      groups = List(summary(Group, lag = Some(9L))),
      freshness = SnapshotFreshness.Stale(At, broker)
    ).use { (server, _) =>
      for {
        response <- get(server, path("/consumer-groups"))
        body = response.body.getOrElse(fail(s"the list failed: ${response.body}"))
        json = parse(body).getOrElse(fail(s"the list did not answer JSON: $body"))
        groups = json.hcursor.downField("groups")
      } yield {
        assertEquals(response.code, StatusCode.Ok)
        assertEquals(
          groups.get[String]("status").toOption,
          Some("stale"),
          clue = s"the rows are from before the broker died and must say so, got $body"
        )
        assertEquals(groups.get[String]("reason").toOption, Some("UPSTREAM_UNAVAILABLE"))
        assertEquals(groups.get[String]("fetchedAt").toOption, Some("2026-09-04T09:00:00.000Z"))

        // The rows are still there. A stale section that dropped its data would be a blank table,
        // which is a different lie from the one being fixed.
        assertEquals(
          groups.downField("data").downField("items").downArray.get[String]("groupId").toOption,
          Some(Group.value)
        )
      }
    }
  }

  test("aListReadFromAHealthyClusterIsMarkedFreshOnTheWire") {
    // The control. Without it, "always stale" would pass the assertion above.
    resource(groups = List(summary(Group, lag = Some(9L)))).use { (server, _) =>
      for {
        response <- get(server, path("/consumer-groups"))
        body = response.body.getOrElse(fail(s"the list failed: ${response.body}"))
        json = parse(body).getOrElse(fail(s"the list did not answer JSON: $body"))
      } yield assertEquals(json.hcursor.downField("groups").get[String]("status").toOption, Some("ok"))
    }
  }

  test("anAbsurdResetDurationIsAValidationErrorAndNotAnInternalOne") {
    // `FiniteDuration(Long.MaxValue, MILLISECONDS)` throws. Thrown from the route, that is a 500
    // `KUI-INTERNAL` and an error log, for a value that was never an offset.
    val request = ResetPlanRequest(
      topic = ConsumerTestServer.Topic,
      partitions = Nil,
      target = ResetTarget.Duration,
      timestamp = None,
      offsets = None,
      shiftBy = None,
      durationMs = Some(Long.MaxValue)
    )
    val requestPath = path(s"/consumer-groups/${Group.value}/offsets/plan")
    val body = Printer.noSpaces.print(request.asJson).getBytes(StandardCharsets.UTF_8)

    resource().use { (server, stubs) =>
      for {
        response <- postPlan(server, requestPath, body)
        planned <- stubs.reset.planned.get
      } yield {
        assertEquals(response.code, StatusCode.BadRequest, clue = response.body)
        assert(
          response.body.swap.getOrElse("").contains("KUI-VALIDATION"),
          clue = response.body
        )
        assertEquals(planned, Nil)
      }
    }
  }

  test("anAbsurdResetTimestampIsAValidationErrorAndNotAnInternalOne") {
    val request = ResetPlanRequest(
      topic = ConsumerTestServer.Topic,
      partitions = Nil,
      target = ResetTarget.Timestamp,
      timestamp = Some(Instant.parse("+300000000-01-01T00:00:00Z")),
      offsets = None,
      shiftBy = None,
      durationMs = None
    )
    val requestPath = path(s"/consumer-groups/${Group.value}/offsets/plan")
    val body = Printer.noSpaces.print(request.asJson).getBytes(StandardCharsets.UTF_8)

    resource().use { (server, _) =>
      for {
        response <- postPlan(server, requestPath, body)
      } yield {
        assertEquals(response.code, StatusCode.BadRequest, clue = response.body)
        assert(
          response.body.swap.getOrElse("").contains("KUI-VALIDATION"),
          clue = response.body
        )
      }
    }
  }

  private def postPlan(
      server: ConsumerTestServer,
      requestPath: String,
      body: Array[Byte]
  ): IO[Response[Either[String, String]]] =
    token(requestPath, method = "POST", body = body).flatMap(principal =>
      basicRequest
        .post(uri"${uri(requestPath)}")
        .header(kui.contracts.KuiEndpoint.PrincipalHeader, principal.value)
        .header(kui.contracts.HttpHeaders.Csrf, "a-csrf-token")
        .header("Content-Type", "application/json")
        .body(new String(body, StandardCharsets.UTF_8))
        .send(server.backend)
    )

  test("aRequestWithNoPrincipalIsRefusedBeforeAnyUseCaseRuns") {
    resource().use { (server, stubs) =>
      for {
        response <- basicRequest
          .get(uri"${uri(path(s"/consumer-groups/${Group.value}"))}")
          .send(server.backend)
        asked <- stubs.detail.asked.get
      } yield {
        assertEquals(response.code, StatusCode.Unauthorized)
        assertEquals(asked, List.empty[GroupId], clue = "the use case ran for an unauthenticated request")
      }
    }
  }
}
