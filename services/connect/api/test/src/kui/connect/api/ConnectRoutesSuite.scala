package kui.connect.api

import cats.effect.IO
import io.circe.Json
import munit.CatsEffectSuite

import kui.connect.contract.ConnectEndpoints
import kui.connect.domain.{ConnectorFacts, ConnectorOperation, ConnectorState, ConnectorTask}
import kui.security.rbac.*

/** The four routes, answered end to end: the interceptor chain, the principal check, the permission guard,
  * the use case, the worker and the mapping onto the wire.
  */
final class ConnectRoutesSuite extends CatsEffectSuite {

  import ConnectTestServer.*

  /** A policy granting `operator` the named Connect actions on the Connect cluster called `payments`.
    *
    * The pattern matters: `Resource.Connect` is a *named* resource, so a grant always carries one, and a
    * requirement that named no Connect cluster could never be covered by it.
    */
  private def policy(pattern: String, actions: Action*): RbacPolicy =
    RbacPolicy(
      roles = List(
        Role(
          name = operator,
          clusters = Set(cluster, readOnly),
          subjects = Nil,
          permissions = List(
            RbacPolicy.permission(
              Resource.Connect,
              Some(ResourcePattern.compile(pattern).getOrElse(ResourcePattern.Everything)),
              actions.toSet
            )
          )
        )
      ),
      defaultRole = None
    )

  test("a connector list keeps only the connectors the caller's pattern allows") {
    // `connect.list` is cluster-scoped, so a 200 is not evidence of filtering. The hidden connector carries
    // a task trace: that string leaving the service is the leak, not merely an extra name.
    val hidden = connector(
      name = "b",
      tasks = List(
        ConnectorTask(
          kui.kernel.TaskId.unsafe(0),
          ConnectorState("FAILED"),
          None,
          Some("SECRET_TRACE do not show")
        )
      )
    )
    val policy = RbacPolicy(
      roles = List(
        Role(
          name = operator,
          clusters = Set(cluster),
          subjects = Nil,
          permissions = List(
            RbacPolicy.permission(
              Resource.Connector,
              Some(ResourcePattern.compile("payments/a.*").getOrElse(fail("payments/a.*"))),
              Set(Action.ConnectorView)
            )
          )
        )
      ),
      defaultRole = None
    )

    resource(
      rbac = policy,
      answer = Right(
        ConnectorFacts(
          connectors = List(connector(name = "a"), hidden),
          unreadable = List("a-unreadable", "b-unreadable")
        )
      )
    ).use { rig =>
      get(rig, connectorsPath(cluster), Set(operator)).map { response =>
        val items = body(response).hcursor
          .downField("connectors")
          .downField("data")
          .downField("workers")
          .downN(0)
          .downField("connectors")
          .downField("data")
        assertEquals(response.code.code, 200, response.body)
        assertEquals(
          items.downField("items").as[List[Json]].map(_.flatMap(_.hcursor.get[String]("name").toOption)),
          Right(List("a"))
        )
        assertEquals(items.get[List[String]]("unreadable"), Right(List("a-unreadable")))
        assert(!response.body.contains("SECRET_TRACE"), response.body)
        assert(!response.body.contains("b-unreadable"), response.body)
      }
    }
  }

  test("the connector list answers 200 with one section per configured Connect cluster") {
    resource().use(get(_, connectorsPath(cluster))).map { response =>
      val section = body(response).hcursor.downField("connectors")

      assertEquals(response.code.code, 200, response.body)
      assertEquals(section.get[String]("status"), Right("ok"))

      val workers = section.downField("data").downField("workers")

      assertEquals(workers.downN(0).get[String]("connect"), Right("payments"))
      assertEquals(
        workers
          .downN(0)
          .downField("connectors")
          .downField("data")
          .downField("items")
          .downN(0)
          .get[String]("name"),
        Right("elastic-sink")
      )
    }
  }

  test("every task's state reaches the wire, with the worker's own trace on the one that failed") {
    resource().use(get(_, connectorsPath(cluster))).map { response =>
      val connector = body(response).hcursor
        .downField("connectors")
        .downField("data")
        .downField("workers")
        .downN(0)
        .downField("connectors")
        .downField("data")
        .downField("items")
        .downN(0)

      assertEquals(connector.downField("tasks").downN(0).get[String]("state"), Right("RUNNING"))
      assertEquals(connector.downField("tasks").downN(1).get[String]("state"), Right("FAILED"))
      assertEquals(connector.get[Int]("runningTasks"), Right(1))
      assertEquals(connector.get[Int]("taskCount"), Right(2))
      assertEquals(connector.get[Boolean]("failed"), Right(true))
      assertEquals(
        connector.downField("tasks").downN(1).get[String]("reason"),
        Right("org.apache.kafka.connect.errors.ConnectException: connection refused to es-01:9200")
      )
    }
  }

  test("a deployment with no Connect address answers not_configured with a 200") {
    // The required case, end to end. Not a 404 and not an empty list: a deployment that never intended to
    // run Kafka Connect is not a broken one, and ADR-032's rule is that the browser hides the row.
    resource().use(get(_, connectorsPath(bare))).map { response =>
      assertEquals(response.code.code, 200, response.body)
      assertEquals(
        body(response).hcursor.downField("connectors").get[String]("status"),
        Right("not_configured")
      )
      assert(!response.body.contains("\"workers\""), response.body)
    }
  }

  test("a cluster KUI has never heard of is a 404 rather than an empty screen") {
    resource().use(get(_, connectorsPath(kui.kernel.ClusterId.unsafe("nowhere")))).map { response =>
      assertEquals(response.code.code, 404, response.body)
      assert(response.body.contains("KUI-CLUSTER-NOT-FOUND"), response.body)
    }
  }

  test("a rebalancing worker answers 200 with a STARTING section, not a failure") {
    resource(answer = Left(rebalancing)).use(get(_, connectorsPath(cluster))).map { response =>
      val worker = body(response).hcursor
        .downField("connectors")
        .downField("data")
        .downField("workers")
        .downN(0)
        .downField("connectors")

      assertEquals(response.code.code, 200, response.body)
      assertEquals(worker.get[String]("status"), Right("unavailable"))
      assertEquals(worker.get[String]("reason"), Right("STARTING"))
      assert(worker.get[String]("message").getOrElse("").contains("rebalancing"), response.body)
    }
  }

  test("a restart a principal without CONNECT:RESTART asks for is refused before the worker is called") {
    // The required case, and the order is the claim. `SecuredRoutes` runs the guard between "who is this"
    // and "do the work", so a caller holding only `VIEW` never reaches the worker at all — which is why
    // the fixture counts the calls the worker was asked to make rather than reading a state back.
    //
    // `CONNECT:RESTART` is the alias an operator's file spells for `Action.ConnectOperate`, which is what
    // the endpoint declares; `Action.fromWire` maps the two, and `ConnectContractSuite` is the only case
    // in the repository that asserts that mapping.
    resource(rbac = policy("payments", Action.ConnectView)).use { rig =>
      for {
        response <- post(rig, operationPath(cluster, "payments", "elastic-sink", "restart"), Set(operator))
        asked <- rig.worker.asked.get
      } yield {
        assertEquals(response.code.code, 403, response.body)
        assert(response.body.contains("KUI-FORBIDDEN"), response.body)
        assertEquals(asked, Nil, clue = "the worker was called despite the refusal")
      }
    }
  }

  test("a principal holding CONNECT:OPERATE on that Connect cluster restarts it") {
    resource(rbac = policy("payments", Action.ConnectOperate, Action.ConnectView)).use { rig =>
      for {
        response <- post(rig, operationPath(cluster, "payments", "elastic-sink", "restart"), Set(operator))
        asked <- rig.worker.asked.get
      } yield {
        assertEquals(response.code.code, 200, response.body)
        assertEquals(asked, List((elastic, ConnectorOperation.Restart)))
        assertEquals(
          body(response).hcursor.get[String]("operation"),
          Right(ConnectEndpoints.RestartOperation)
        )
        assertEquals(body(response).hcursor.get[String]("connect"), Right("payments"))
      }
    }
  }

  test("a grant on another Connect cluster's name does not carry to this one") {
    // The reason the requirement is named by the path parameter rather than unnamed: a grant is written
    // over a pattern, and `payments` must not let somebody restart `analytics`.
    resource(rbac = policy("analytics", Action.ConnectOperate, Action.ConnectView)).use { rig =>
      for {
        response <- post(rig, operationPath(cluster, "payments", "elastic-sink", "pause"), Set(operator))
        asked <- rig.worker.asked.get
      } yield {
        assertEquals(response.code.code, 403, response.body)
        assertEquals(asked, Nil)
      }
    }
  }

  test("a pause on a read-only cluster is refused at the guard and the worker is never called") {
    // Two layers refuse this and the outer one wins. `RbacGuard.fromPolicy` is given the deployment's
    // read-only flags and refuses an *altering* action on a read-only cluster before the route's logic runs
    // at all; `MutationGuard`'s `KUI-READ-ONLY` is the refusal behind it, asserted in `MutationGuardSuite`
    // where no permission guard stands in front. Both are the decision `Action.ConnectOperate.isAlter`
    // encodes, and what matters here is that neither reaches the worker.
    resource(rbac = policy("payments", Action.ConnectOperate, Action.ConnectView)).use { rig =>
      for {
        response <- post(rig, operationPath(readOnly, "payments", "elastic-sink", "pause"), Set(operator))
        asked <- rig.worker.asked.get
      } yield {
        assertEquals(response.code.code, 403, response.body)
        assertEquals(asked, Nil)
      }
    }
  }

  test("a Connect cluster name the service could never address is a 400 naming the parameter") {
    val tooLong = "c" * 300

    resource().use { rig =>
      for {
        response <- post(rig, operationPath(cluster, tooLong, "elastic-sink", "pause"))
        asked <- rig.worker.asked.get
      } yield {
        assertEquals(response.code.code, 400, response.body)
        assert(response.body.contains(ConnectEndpoints.ConnectNameParam), response.body)
        assertEquals(asked, Nil)
      }
    }
  }

  test("a Connect cluster this deployment does not configure is a 501 and reaches no worker") {
    resource().use { rig =>
      for {
        response <- post(rig, operationPath(cluster, "ghost", "elastic-sink", "resume"))
        asked <- rig.worker.asked.get
      } yield {
        assertEquals(response.code.code, 501, response.body)
        assert(response.body.contains("KUI-UNSUPPORTED"), response.body)
        assertEquals(asked, Nil)
      }
    }
  }

  test("each of the three verbs reaches the worker as its own operation") {
    resource().use { rig =>
      for {
        _ <- post(rig, operationPath(cluster, "payments", "elastic-sink", "pause"))
        _ <- post(rig, operationPath(cluster, "payments", "elastic-sink", "resume"))
        _ <- post(rig, operationPath(cluster, "payments", "elastic-sink", "restart"))
        asked <- rig.worker.asked.get
      } yield assertEquals(
        asked.map(_._2),
        List(ConnectorOperation.Pause, ConnectorOperation.Resume, ConnectorOperation.Restart)
      )
    }
  }

  test("an unsigned caller is refused before anything is read") {
    resource().use { rig =>
      for {
        response <- sttp.client4.basicRequest
          .get(address(connectorsPath(cluster)))
          .response(sttp.client4.asStringAlways)
          .send(rig.backend)
        reads <- rig.worker.reads.get
      } yield {
        assertEquals(response.code.code, 401, response.body)
        assertEquals(reads, 0)
      }
    }
  }

  test("a worker that is unreachable costs one row and answers 200") {
    val unreachable = kui.kernel.error.InfrastructureError.Unreachable("kafka-connect.payments", "refused")

    resource(answer = Left(unreachable)).use(get(_, connectorsPath(cluster))).map { response =>
      val worker = body(response).hcursor
        .downField("connectors")
        .downField("data")
        .downField("workers")
        .downN(0)
        .downField("connectors")

      assertEquals(response.code.code, 200, response.body)
      assertEquals(worker.get[String]("status"), Right("unavailable"))
      assertEquals(worker.get[String]("reason"), Right("UPSTREAM_UNAVAILABLE"))
    }
  }

  test("a worker that answered and named no connectors draws no zero of its own") {
    // "The worker answered and there is nothing running" is an `ok` section with an empty list, which is a
    // different document from a worker that could not be asked. The screen's sentence is the browser's;
    // what this asserts is that the two are distinguishable.
    resource(answer = Right(ConnectorFacts.complete(Nil))).use(get(_, connectorsPath(cluster))).map {
      response =>
        val worker = body(response).hcursor
          .downField("connectors")
          .downField("data")
          .downField("workers")
          .downN(0)
          .downField("connectors")

        assertEquals(worker.get[String]("status"), Right("ok"))
        assertEquals(worker.downField("data").get[List[String]]("unreadable"), Right(Nil))
        assertEquals(
          worker.downField("data").downField("items").as[List[io.circe.Json]].map(_.size),
          Right(0)
        )
    }
  }
}
