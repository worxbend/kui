package kui.connect.infrastructure

import cats.effect.{IO, Ref}
import sttp.client4.Backend
import sttp.client4.impl.cats.implicits.*
import sttp.client4.testing.{BackendStub, ResponseStub, StubBody}
import sttp.model.{Method, StatusCode}

import kui.config.SafeUrl
import kui.connect.domain.*
import kui.kernel.error.{ErrorCode, InfrastructureError}
import kui.kernel.{ConnectName, ConnectorName}
import kui.testkit.KuiIOSuite

/** What the client does with each answer a Kafka Connect cluster can give.
  *
  * A stub rather than a running worker: every promise here is a promise about a *response*, and a real
  * Connect cluster is the slowest possible way to produce one — and cannot be made to produce most of them at
  * all. The worker KUI has to survive is the one mid-rebalance, the one that lists a connector and refuses
  * its status, and the one from 2018 that has never heard of `?expand=`.
  */
final class ConnectHttpSuite extends KuiIOSuite {

  private val base: SafeUrl = SafeUrl.unsafe("http://kafka-connect:8083")
  private val payments: ConnectName = ConnectName.unsafe("payments")
  private val elastic: ConnectorName = ConnectorName.unsafe("elastic-sink")

  private def worker(
      respond: PartialFunction[String, (StatusCode, String)],
      address: SafeUrl = base
  ): ConnectHttp[IO] = {
    val backend: Backend[IO] = BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest
      .thenRespondF { request =>
        val path = "/" + request.uri.path.mkString("/")
        val (status, body) = respond.applyOrElse(path, (_: String) => (StatusCode.NotFound, ""))
        IO.pure(ResponseStub.adjust(body, status): sttp.client4.Response[StubBody])
      }
    new ConnectHttp[IO](backend, address, payments, ConnectCredentials.anonymous[IO])
  }

  /** The trace an Elasticsearch sink writes when its cluster is unreachable, as JSON escapes it.
    *
    * Held apart from the document below only so that neither line runs past this codebase's column limit:
    * `\\n` and `\\t` here are the two characters JSON wants, not a real newline, so the fixture is a document
    * a worker could actually have sent.
    */
  private val elasticTrace: String =
    "org.apache.kafka.connect.errors.ConnectException: connection refused to es-01:9200" +
      "\\n\\tat org.apache.kafka.connect.runtime.WorkerSinkTask.poll(WorkerSinkTask.java:329)"

  /** The expanded document Kafka 2.3 and later answer: one key per connector, each with its status.
    *
    * ==Every order in it is deliberately wrong==
    *
    * Circe hands back the worker's own key order and a Connect herder is under no obligation to keep one, so
    * the three orderings this client imposes have to be visible in what it answers. The keys here are
    * `elastic-sink, orders-source, archive-sink`, whose reverse (`archive, orders, elastic`) is *not* the
    * sorted order (`archive, elastic, orders`) — while the previous fixture's keys reversed to exactly the
    * sorted list, so `.sortBy(_.name.value)` could be replaced with `.reverse` and all 129 cases stayed
    * green. `orders-source`'s tasks are listed `1, 0, 2` for the same reason: sorted reads `0, 1, 2`,
    * reversed reads `2, 0, 1`, and the worker's own order reads `1, 0, 2`, so no two of the three agree.
    *
    * `archive-sink` carries **no `tasks` key at all**, which is what a paused or freshly created connector
    * genuinely sends, and is the other rule this fixture is the input to: it must be a connector with zero
    * tasks and not an `unreadable` one.
    */
  private val expanded: String =
    s"""{
      |  "elastic-sink": {
      |    "info": { "name": "elastic-sink", "type": "sink" },
      |    "status": {
      |      "name": "elastic-sink",
      |      "connector": { "state": "RUNNING", "worker_id": "10.0.0.1:8083" },
      |      "tasks": [
      |        { "id": 0, "state": "FAILED", "worker_id": "10.0.0.2:8083", "trace": "$elasticTrace" }
      |      ],
      |      "type": "sink"
      |    }
      |  },
      |  "orders-source": {
      |    "info": { "name": "orders-source", "type": "source" },
      |    "status": {
      |      "name": "orders-source",
      |      "connector": { "state": "RUNNING", "worker_id": "10.0.0.1:8083" },
      |      "tasks": [
      |        { "id": 1, "state": "RUNNING", "worker_id": "10.0.0.2:8083", "trace": "" },
      |        { "id": 0, "state": "RUNNING", "worker_id": "10.0.0.1:8083" },
      |        { "id": 2, "state": "RUNNING", "worker_id": "10.0.0.3:8083", "trace": "" }
      |      ],
      |      "type": "source"
      |    }
      |  },
      |  "archive-sink": {
      |    "status": {
      |      "name": "archive-sink",
      |      "connector": { "state": "PAUSED", "worker_id": "10.0.0.1:8083" }
      |    }
      |  }
      |}""".stripMargin

  private val rebalanceBody: String =
    """{"error_code":409,"message":"Cannot complete request momentarily due to stale configuration """ +
      """(typically caused by a concurrent config change)"}"""

  test("a worker that names three connectors reaches the domain with each task's own state") {
    // The required case, at the boundary the states actually cross. Tasks arrive in id order however the
    // worker listed them, because two screens reading one document must not order them differently.
    worker { case "/connectors" => (StatusCode.Ok, expanded) }.connectors.map {
      case Right(facts) =>
        assertEquals(
          facts.connectors.map(_.name.value),
          List("archive-sink", "elastic-sink", "orders-source")
        )
        assertEquals(
          facts.connectors.map(_.tasks.map(task => s"${task.id.value}:${task.state.wire}")),
          List(Nil, List("0:FAILED"), List("0:RUNNING", "1:RUNNING", "2:RUNNING"))
        )
        assertEquals(facts.connectors.map(_.runningTasks), List(0, 0, 3))
        assertEquals(facts.unreadable, Nil)
      case Left(error) => fail(s"expected connectors, got $error")
    }
  }

  test("a task that failed carries the worker's own trace, and the reason is its first line") {
    // Required case. Never a word KUI chose: the sentence is a slice of the worker's text, and the whole
    // trace travels beside it so that nobody has to open a worker's log to read what KUI already has.
    worker { case "/connectors" => (StatusCode.Ok, expanded) }.connectors.map {
      case Right(facts) =>
        val failed = facts.connectors
          .find(_.name.value == "elastic-sink")
          .flatMap(_.tasks.headOption)
          .getOrElse(fail("the fixture has no failed task"))

        assertEquals(
          failed.reason,
          Some("org.apache.kafka.connect.errors.ConnectException: connection refused to es-01:9200")
        )
        assert(clue(failed.trace.getOrElse("")).contains("WorkerSinkTask.poll"))
      case Left(error) => fail(s"expected connectors, got $error")
    }
  }

  test("an empty trace on a healthy task is no trace at all") {
    // Connect writes `"trace": ""` on a task that has not failed. An empty reason would put an empty
    // sentence under a healthy connector.
    worker { case "/connectors" => (StatusCode.Ok, expanded) }.connectors.map {
      case Right(facts) =>
        val healthy = facts.connectors.find(_.name.value == "orders-source").toList.flatMap(_.tasks)

        assertEquals(healthy.flatMap(_.trace), Nil)
        assertEquals(healthy.flatMap(_.reason), Nil)
      case Left(error) => fail(s"expected connectors, got $error")
    }
  }

  test("a worker that named no type leaves the direction unknown rather than guessing") {
    worker { case "/connectors" => (StatusCode.Ok, expanded) }.connectors.map {
      case Right(facts) =>
        assertEquals(
          facts.connectors.map(connector => connector.name.value -> connector.kind.wire).toMap,
          Map("orders-source" -> "source", "elastic-sink" -> "sink", "archive-sink" -> "")
        )
      case Left(error) => fail(s"expected connectors, got $error")
    }
  }

  test("a rebalancing worker is KUI-CONNECT-REBALANCING and an application error, not an outage") {
    // The rule this packet owns, at the point the 409 is classified. `ApplicationError` is what keeps it
    // out of the capability registry entirely (ADR-039 §6): a business refusal must never dim a feature.
    worker { case "/connectors" => (StatusCode.Conflict, rebalanceBody) }.connectors.map {
      case Left(error) =>
        assertEquals(error.code, ErrorCode.ConnectRebalancing)
        assertNotEquals(error.code, ErrorCode.UpstreamUnavailable)
        assert(error.isInstanceOf[kui.kernel.error.ApplicationError])
        assert(clue(error.message).contains("rebalancing"))
        assert(clue(error.message).contains("stale configuration"))
      case Right(facts) => fail(s"expected a rebalance refusal, got $facts")
    }
  }

  test("a pre-2.3 worker that answers a bare list is read one status at a time, in name order") {
    // **The rule this packet owns**, on the second of the two readers. `perConnector` sorts the names it
    // was given and the connectors come back in that order; the worker's own array order is `elastic,
    // orders, archive`, whose reverse is `archive, orders, elastic` and whose sort is `archive, elastic,
    // orders` — so no two of the three agree and `names.sorted` cannot be replaced by `names.reverse` or
    // dropped. The previous fixture named two connectors whose reverse *was* their sorted order, which is
    // why that replacement left all 129 cases green.
    worker {
      case "/connectors" => (StatusCode.Ok, """["elastic-sink","orders-source","archive-sink"]""")
      case "/connectors/orders-source/status" =>
        (
          StatusCode.Ok,
          """{"name":"orders-source","connector":{"state":"RUNNING"},"tasks":[{"id":0,"state":"RUNNING"}]}"""
        )
      case "/connectors/elastic-sink/status" =>
        (StatusCode.Ok, """{"name":"elastic-sink","connector":{"state":"PAUSED"},"tasks":[]}""")
      case "/connectors/archive-sink/status" =>
        (StatusCode.Ok, """{"name":"archive-sink","connector":{"state":"PAUSED"}}""")
    }.connectors.map {
      case Right(facts) =>
        assertEquals(
          facts.connectors.map(_.name.value),
          List("archive-sink", "elastic-sink", "orders-source")
        )
        assertEquals(facts.unreadable, Nil)
      case Left(error) => fail(s"expected connectors, got $error")
    }
  }

  test("the expanded document's connectors arrive in name order and not in the worker's key order") {
    // **The rule this packet owns**, on the first of the two readers, and it compares two connectors'
    // positions rather than asserting a whole list — which is what makes it a statement about *order*
    // rather than about content. A Connect herder rebuilds its status map on every rebalance, so the key
    // order changes between polls; a panel that drew rows in that order would reshuffle under an operator
    // reading it, and `Connector.reason`'s own promise about task-id order rests on the same argument one
    // level down.
    worker { case "/connectors" => (StatusCode.Ok, expanded) }.connectors.map {
      case Right(facts) =>
        val names = facts.connectors.map(_.name.value)

        assert(names.indexOf("archive-sink") < names.indexOf("elastic-sink"), clue = names)
        assert(names.indexOf("elastic-sink") < names.indexOf("orders-source"), clue = names)
        assertEquals(names, names.sorted, clue = "the worker's key order reached the domain")
      case Left(error) => fail(s"expected connectors, got $error")
    }
  }

  test("one connector's tasks arrive in id order whatever order the worker listed them in") {
    // **The rule this packet owns.** `tasksFrom` sorts by task id, and two positions are compared rather
    // than a list asserted. `Connector.reason` picks *the first failed task in id order* so that the card
    // and the drawer cannot name different tasks; if this sort went away, the input to that rule would be
    // whatever order Circe handed back and the two screens would disagree between polls.
    worker { case "/connectors" => (StatusCode.Ok, expanded) }.connectors.map {
      case Right(facts) =>
        val tasks = facts.connectors.find(_.name.value == "orders-source").toList.flatMap(_.tasks)
        val ids = tasks.map(_.id.value)

        assertEquals(ids, List(0, 1, 2))
        assert(ids.indexOf(0) < ids.indexOf(1), clue = ids)
      case Left(error) => fail(s"expected connectors, got $error")
    }
  }

  test("a connector with no tasks key is a connector with no tasks, not an unreadable one") {
    // **The rule this packet owns.** `tasksFrom`'s `case None => Some(Nil)`, which the fixture's paused
    // `archive-sink` is the input to. Answering `None` there makes a paused or freshly created connector
    // — the case the code's own comment names out loud — vanish from the list and reappear in the screen's
    // *"KUI could not describe it"* row, which is a KUI defect reported as a broken connector.
    //
    // A connector whose `tasks` key is present and unreadable is still unreadable — `a task list KUI
    // cannot read makes the connector unreadable` further down holds that half — and the two together are
    // what make the distinction a rule rather than a leniency.
    worker { case "/connectors" => (StatusCode.Ok, expanded) }.connectors.map {
      case Right(facts) =>
        val paused = facts.connectors
          .find(_.name.value == "archive-sink")
          .getOrElse(fail("the paused connector with no tasks key was dropped from the list"))

        assertEquals(paused.tasks, Nil)
        assertEquals(paused.taskCount, 0)
        assertEquals(paused.state.wire, "PAUSED")
        assertEquals(facts.unreadable, Nil)
      case Left(error) => fail(s"expected connectors, got $error")
    }
  }

  test("a connector the worker will not describe is named rather than dropped") {
    // A connector missing from a list looks like a connector that was deleted, and one worker of a Connect
    // cluster being wedged loses the status of its own share while the rest answer normally.
    worker {
      case "/connectors" => (StatusCode.Ok, """["orders-source","elastic-sink"]""")
      case "/connectors/orders-source/status" =>
        (StatusCode.Ok, """{"name":"orders-source","connector":{"state":"RUNNING"},"tasks":[]}""")
    }.connectors.map {
      case Right(facts) =>
        assertEquals(facts.connectors.map(_.name.value), List("orders-source"))
        assertEquals(facts.unreadable, List("elastic-sink"))
      case Left(error) => fail(s"expected connectors, got $error")
    }
  }

  test("a connector whose expanded entry has no status is unreadable rather than absent") {
    worker { case "/connectors" =>
      (StatusCode.Ok, """{"elastic-sink":{"info":{"name":"elastic-sink"}}}""")
    }.connectors.map {
      case Right(facts) =>
        assertEquals(facts.connectors, Nil)
        assertEquals(facts.unreadable, List("elastic-sink"))
      case Left(error) => fail(s"expected connectors, got $error")
    }
  }

  test("the connectors KUI could not describe are listed in name order too, by both readers") {
    // The fourth ordering in this file, and the one that had no case. W8-02 rebuilt the fixtures for the
    // three orderings above — the expanded document's connectors, one connector's tasks, and
    // `perConnector`'s roster — and `unreadable`'s own `.sorted` one line below the first of them stayed
    // measured by nothing: replacing it with `.reverse` left all 138 connect cases green.
    //
    // The list is not decoration. It is the *"KUI could not describe it"* row on the connectors screen,
    // and it reshuffles between polls for exactly the reason the connectors list does: circe hands back
    // the worker's own key order and a Connect herder rebuilds its status map on every rebalance.
    //
    // Three names, in an order that is neither the sorted one nor its reverse: the document reads
    // `elastic, orders, archive`, sorted reads `archive, elastic, orders`, the document reversed reads
    // `archive, orders, elastic` and the sort reversed reads `orders, elastic, archive`. No two of the
    // four agree, which is what it takes for `.sorted` to be the only expression that answers.
    //
    // Both readers are driven here because they reach `unreadable` by different routes — `expanded`
    // sorts the list it collected, `perConnector` inherits the order from `names.sorted` — and a rule
    // held in one reader and not the other is the shape §3.14 is about: two screens reading one worker.
    val undescribable =
      """{
        |  "elastic-sink": { "info": { "name": "elastic-sink" } },
        |  "orders-source": { "info": { "name": "orders-source" } },
        |  "archive-sink": { "info": { "name": "archive-sink" } }
        |}""".stripMargin

    // The bare-list worker refuses every status request: three names, no connector readable. The stub
    // answers 404 to anything it does not match, so naming only `/connectors` is that refusal.
    for {
      fromExpanded <- worker { case "/connectors" => (StatusCode.Ok, undescribable) }.connectors
      fromList <- worker { case "/connectors" =>
        (StatusCode.Ok, """["elastic-sink","orders-source","archive-sink"]""")
      }.connectors
    } yield (fromExpanded, fromList) match {
      case (Right(expandedFacts), Right(listedFacts)) =>
        assertEquals(expandedFacts.connectors, Nil)
        assertEquals(listedFacts.connectors, Nil)

        List(expandedFacts.unreadable, listedFacts.unreadable).foreach { unreadable =>
          assert(
            unreadable.indexOf("archive-sink") < unreadable.indexOf("elastic-sink"),
            s"the worker's own order survived into the unreadable list: $unreadable"
          )
          assertEquals(unreadable, unreadable.sorted)
          assertEquals(unreadable, List("archive-sink", "elastic-sink", "orders-source"))
        }
      case (other, another) => fail(s"expected two sets of facts, got $other and $another")
    }
  }

  test("a task list KUI cannot read makes the connector unreadable rather than a connector with 0 tasks") {
    // All or nothing per connector: a connector drawn with three of its four tasks reports `3/3 tasks`
    // over a cluster with four, and the task quietly dropped is the one that was failing.
    worker { case "/connectors" =>
      (
        StatusCode.Ok,
        """{"elastic-sink":{"status":{"connector":{"state":"RUNNING"},""" +
          """"tasks":[{"id":-1,"state":"RUNNING"}]}}}"""
      )
    }.connectors.map {
      case Right(facts) =>
        assertEquals(facts.connectors, Nil)
        assertEquals(facts.unreadable, List("elastic-sink"))
      case Left(error) => fail(s"expected connectors, got $error")
    }
  }

  test("a 404 on the connector list is an address that is not a Connect worker") {
    // `/connectors` exists on every Connect worker there has ever been, so this is a proxy or an ingress
    // pointed at the wrong service — which is a sentence an operator can act on.
    worker { case "/never" => (StatusCode.Ok, "") }.connectors.map {
      case Left(error) =>
        assertEquals(error.code, ErrorCode.UpstreamUnavailable)
        assert(clue(error.message).contains("does not look like a Kafka Connect worker"))
      case Right(facts) => fail(s"expected a failure, got $facts")
    }
  }

  test("credentials that are rejected are KUI's problem and say so") {
    worker { case "/connectors" => (StatusCode.Unauthorized, "") }.connectors.map(answer =>
      assertEquals(answer.left.map(_.code), Left(ErrorCode.UpstreamAuth))
    )
  }

  test("a transport failure never copies a credential-bearing URL into its diagnostic") {
    val canary = "https://user:connect-secret@connect.internal"
    val backend: Backend[IO] = BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest
      .thenRespondF(_ => IO.raiseError(new java.net.ConnectException(canary)))

    new ConnectHttp[IO](backend, base, payments, ConnectCredentials.anonymous[IO]).connectors.map {
      case Left(InfrastructureError.Unreachable(upstream, cause)) =>
        assertEquals(upstream, ConnectHttp.upstreamName(payments))
        assertEquals(cause, "ConnectException")
        assert(!cause.contains("connect-secret"), cause)
      case other => fail(s"expected an unreachable upstream, got $other")
    }
  }

  test("requests are built relative to the root, because failover puts the base path back on") {
    // `RegistryHttp`'s defect, which cost the schema service a milestone: `Failover.rebase` replaces the
    // scheme and authority and *prefixes the base URL's own path*, so a client that also built its
    // requests against the full configured URL had that path applied twice. A Connect cluster behind an
    // ingress at `/connect` would have produced `/connect/connect/connectors` and an honest 404.
    Ref.of[IO, String]("").flatMap { asked =>
      val backend: Backend[IO] = BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest
        .thenRespondF { request =>
          asked
            .set("/" + request.uri.path.mkString("/"))
            .as(ResponseStub.adjust("{}", StatusCode.Ok): sttp.client4.Response[StubBody])
        }

      new ConnectHttp[IO](
        backend,
        SafeUrl.unsafe("http://gateway.internal/connect"),
        payments,
        ConnectCredentials.anonymous[IO]
      ).connectors >> asked.get.assertEquals("/connectors")
    }
  }

  test("legacy restart explicitly restarts every task, including running tasks") {
    Ref.of[IO, List[String]](Nil).flatMap { asked =>
      val backend: Backend[IO] = BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest
        .thenRespondF { request =>
          val path = "/" + request.uri.path.mkString("/")
          val body = path match {
            case "/" => """{"version":"2.2.2"}"""
            case "/connectors/elastic-sink/status" =>
              """{"tasks":[{"id":0,"state":"FAILED"},{"id":2,"state":"RUNNING"}]}"""
            case _ => ""
          }
          asked
            .update(_ :+ s"${request.method.method} $path")
            .as(ResponseStub.adjust(body, StatusCode.Ok): sttp.client4.Response[StubBody])
        }
      for {
        result <- new ConnectHttp[IO](backend, base, payments, ConnectCredentials.anonymous[IO])
          .operate(elastic, ConnectorOperation.Restart)
        calls <- asked.get
      } yield {
        assertEquals(result, Right(()))
        assertEquals(
          calls.filter(_.startsWith("POST")),
          List(
            "POST /connectors/elastic-sink/restart",
            "POST /connectors/elastic-sink/tasks/0/restart",
            "POST /connectors/elastic-sink/tasks/2/restart"
          )
        )
      }
    }
  }

  test("a legacy task restart failure is not reported as a successful connector restart") {
    worker {
      case "/" => (StatusCode.Ok, """{"version":"2.2.2"}""")
      case "/connectors/elastic-sink/status" => (StatusCode.Ok, """{"tasks":[{"id":0}]}""")
      case "/connectors/elastic-sink/restart" => (StatusCode.NoContent, "")
      case "/connectors/elastic-sink/tasks/0/restart" => (StatusCode.Conflict, rebalanceBody)
    }.operate(elastic, ConnectorOperation.Restart)
      .map(answer => assertEquals(answer.left.map(_.code), Left(ErrorCode.ConnectRebalancing)))
  }

  test("a legacy worker without task identities is refused before any mutation") {
    Ref.of[IO, List[String]](Nil).flatMap { asked =>
      val backend: Backend[IO] = BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest
        .thenRespondF { request =>
          asked
            .update(_ :+ request.method.method)
            .as(ResponseStub.adjust("{}", StatusCode.Ok): sttp.client4.Response[StubBody])
        }
      for {
        answer <- new ConnectHttp[IO](backend, base, payments, ConnectCredentials.anonymous[IO])
          .operate(elastic, ConnectorOperation.Restart)
        methods <- asked.get
      } yield {
        assert(answer.isLeft, clue = answer)
        assert(!methods.contains("POST"))
      }
    }
  }

  test("a restart asks for the tasks too, and asks for all of them") {
    // Both parameters are the opposite of the API's defaults, and both are the point: `includeTasks=false`
    // restarts the connector and leaves its dead tasks dead, which is the state §7.7's operator is
    // pressing the button to get out of.
    Ref.of[IO, String]("").flatMap { asked =>
      val backend: Backend[IO] = BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest
        .thenRespondF { request =>
          asked
            .set(s"${request.method.method} ${request.uri.toString}")
            .as(
              ResponseStub
                .adjust("""{"version":"3.0.0"}""", StatusCode.Accepted): sttp.client4.Response[StubBody]
            )
        }

      new ConnectHttp[IO](backend, base, payments, ConnectCredentials.anonymous[IO])
        .operate(elastic, ConnectorOperation.Restart) >>
        asked.get.map { line =>
          assert(clue(line).startsWith(Method.POST.method))
          assert(clue(line).contains("/connectors/elastic-sink/restart"))
          assert(clue(line).contains("includeTasks=true"))
          assert(clue(line).contains("onlyFailed=false"))
        }
    }
  }

  test("pause and resume are the worker's own two verbs and nothing else") {
    Ref.of[IO, List[String]](Nil).flatMap { asked =>
      val backend: Backend[IO] = BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest
        .thenRespondF { request =>
          asked
            .update(_ :+ s"${request.method.method} /${request.uri.path.mkString("/")}")
            .as(ResponseStub.adjust("", StatusCode.Accepted): sttp.client4.Response[StubBody])
        }

      val client = new ConnectHttp[IO](backend, base, payments, ConnectCredentials.anonymous[IO])

      client.operate(elastic, ConnectorOperation.Pause) >>
        client.operate(elastic, ConnectorOperation.Resume) >>
        asked.get.assertEquals(
          List("PUT /connectors/elastic-sink/pause", "PUT /connectors/elastic-sink/resume")
        )
    }
  }

  test("an operation on a connector the worker does not have names the connector it could not find") {
    // There is no KUI-CONNECTOR-NOT-FOUND in the shipped vocabulary and house rule 3 forbids adding one.
    // A 501 would say this deployment cannot do it at all, which is false; a 409 naming the connector is
    // the answer the alerts service already gives for an event id that names nothing.
    worker { case "/never" => (StatusCode.Ok, "") }
      .operate(elastic, ConnectorOperation.Pause)
      .map {
        case Left(error) =>
          assertEquals(error.code, ErrorCode.InvalidState)
          assert(clue(error.message).contains("elastic-sink"))
          assert(clue(error.message).contains("payments"))
        case Right(_) => fail("expected a refusal")
      }
  }

  test("an operation refused mid-rebalance is the rebalance refusal, not a failed restart") {
    worker {
      case "/" => (StatusCode.Ok, """{"version":"3.0.0"}""")
      case "/connectors/elastic-sink/restart" => (StatusCode.Conflict, rebalanceBody)
    }
      .operate(elastic, ConnectorOperation.Restart)
      .map(answer => assertEquals(answer.left.map(_.code), Left(ErrorCode.ConnectRebalancing)))
  }

  test("an answer that is neither a list nor an expanded document is reported as not understood") {
    worker { case "/connectors" => (StatusCode.Ok, "\"yes\"") }.connectors.map {
      case Left(error) => assert(clue(error.message).contains("could not understand"))
      case Right(facts) => fail(s"expected a failure, got $facts")
    }
  }

  test("the upstream is named per Connect cluster, so a dashboard says which one is failing") {
    assertEquals(ConnectHttp.upstreamName(payments), "kafka-connect.payments")
  }
}
