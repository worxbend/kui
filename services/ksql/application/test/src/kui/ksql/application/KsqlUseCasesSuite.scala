package kui.ksql.application

import cats.effect.IO
import munit.CatsEffectSuite

import kui.kernel.ClusterId
import kui.kernel.error.{ErrorCode, InfrastructureError, KuiError}
import kui.ksql.domain.*
import kui.security.audit.MutationOutcome

/** The four things a caller can do to this service, and the refusals that shape them.
  *
  * Every case that is about a refusal asserts **what the server was asked**, not only what came back: "the
  * statement was refused" and "the statement was refused before ksqlDB was called" are different promises,
  * and only the second is worth anything to somebody who just found a topic missing.
  */
final class KsqlUseCasesSuite extends CatsEffectSuite {

  import KsqlRig.*

  private val pushQuery = "SELECT * FROM ORDERS EMIT CHANGES;"
  private val dropWithTopic = "DROP STREAM ORDERS DELETE TOPIC;"

  // -----------------------------------------------------------------------------------------------
  // The read
  // -----------------------------------------------------------------------------------------------

  test("a deployment with no ksqlDB address answers not_configured rather than an empty list") {
    // The required case, at this layer: `NotConfigured` is a `Right`, so the route answers 200 and the
    // browser hides the row (ADR-032). An empty list here would say "this ksqlDB has no streams", which is
    // a claim about a server that does not exist.
    rig().flatMap { rig =>
      for {
        answer <- rig.useCases.objects(alice, bare)
        reads <- rig.server.reads.get
      } yield {
        assertEquals(answer, Right(KsqlListing.NotConfigured))
        // And nothing was asked of any server, because there is nothing to ask.
        assertEquals(reads, 0)
      }
    }
  }

  test("a cluster KUI has never heard of is a 404 and not an empty answer") {
    rig().flatMap(rig =>
      rig.useCases.objects(alice, ClusterId.unsafe("nope")).map {
        case Left(error) => assertEquals(error.code, ErrorCode.ClusterNotFound)
        case Right(other) => fail(s"expected a not-found, got $other")
      }
    )
  }

  test("a server that answered reaches the caller with what it said") {
    rig().flatMap(rig =>
      rig.useCases.objects(alice, cluster).map {
        case Right(KsqlListing.Answered(Right(objects))) =>
          assertEquals(objects.items.map(_.name), List("ORDERS", "USERS"))
        case other => fail(s"expected an answer, got $other")
      }
    )
  }

  test("a server that did not answer is a failure inside a success, not a failed request") {
    // The metrics service's rule: the ksqlDB screen sits in a product where the rest of the screens work,
    // and a 4xx would make a server behaving exactly as designed indistinguishable from a broken KUI.
    val down: Either[KuiError, KsqlObjects] =
      Left(InfrastructureError.Unreachable("ksqldb", "connection refused"))

    rig(objectsAnswer = down).flatMap(rig =>
      rig.useCases.objects(alice, cluster).map {
        case Right(KsqlListing.Answered(Left(error))) =>
          assertEquals(error.code, ErrorCode.UpstreamUnavailable)
        case other => fail(s"expected an answered-with-failure, got $other")
      }
    )
  }

  // -----------------------------------------------------------------------------------------------
  // The plan
  // -----------------------------------------------------------------------------------------------

  test("a destructive statement plans to a token and names the topic it would delete") {
    rig().flatMap(rig =>
      rig.useCases.plan(alice, cluster, dropWithTopic).map {
        case Right(plan) =>
          assert(plan.statement.destructive)
          assert(plan.token.isDefined)
          assert(plan.expiresAt.isDefined)
          // The whole content of the confirmation: "this deletes something" is not a thing anybody can
          // weigh, and `orders` is.
          assert(clue(plan.warnings).exists(_.contains("'orders'")))
        case Left(error) => fail(s"the plan failed: ${error.message}")
      }
    )
  }

  test("a plan that cannot identify the target cannot authorize deletion") {
    // The product's central promise applied to a warning. The object is not in this cluster's listing, so
    // the sentence says KUI cannot say which topic — it does not guess from the object's name.
    rig().flatMap(rig =>
      rig.useCases.plan(alice, cluster, "DROP STREAM NOWHERE DELETE TOPIC;").map {
        case Right(plan) => fail(s"an unidentified target received a plan: $plan")
        case Left(error) => assertEquals(error.code, ErrorCode.Validation)
      }
    )
  }

  test("a plan whose server did not answer says the server did not answer") {
    val down: Either[KuiError, KsqlObjects] =
      Left(InfrastructureError.Unreachable("ksqldb", "connection refused"))

    rig(objectsAnswer = down).flatMap(rig =>
      rig.useCases.plan(alice, cluster, dropWithTopic).map {
        case Right(plan) => fail(s"an unreachable server received a plan: $plan")
        case Left(error) => assertEquals(error.code, ErrorCode.UpstreamUnavailable)
      }
    )
  }

  test("a harmless statement plans to no token at all") {
    // A screen that asked for a confirmation on every statement would teach an operator to click past
    // them, which is the failure a two-phase flow exists to avoid rather than to cause.
    rig().flatMap(rig =>
      rig.useCases.plan(alice, cluster, "CREATE STREAM A AS SELECT * FROM ORDERS;").map {
        case Right(plan) =>
          assert(!plan.statement.destructive)
          assertEquals(plan.token, None)
          assertEquals(plan.expiresAt, None)
          assertEquals(plan.warnings, Nil)
        case Left(error) => fail(s"the plan failed: ${error.message}")
      }
    )
  }

  test("planning a push query answers the address that will run it, rather than nothing") {
    // W9-A1: `describe`'s `if statement.push then List(PushQueryElsewhere) else Nil` was held by nothing —
    // collapsing it to `Nil` left all 22 cases here and all 4,299 in the repository green. The browser
    // routes a push query off `shape` and never reads this, but the plan endpoint is also what somebody
    // holding a `curl` gets, and "wrong endpoint" with no alternative is a dead end. It is the same
    // sentence `execute` refuses with, from the same constant, so the two cannot drift apart.
    rig().flatMap(rig =>
      rig.useCases.plan(alice, cluster, pushQuery).map {
        case Right(plan) =>
          assertEquals(plan.statement.shape, StatementShape.PushQuery)
          assertEquals(plan.warnings, List(KsqlUseCases.PushQueryElsewhere))
          // And it still needs no confirmation: a push query changes nothing.
          assertEquals(plan.token, None)
        case Left(error) => fail(s"the plan failed: ${error.message}")
      }
    )
  }

  test("a plan writes no audit record, because a preview is not a change") {
    rig().flatMap(rig =>
      for {
        _ <- rig.useCases.plan(alice, cluster, dropWithTopic)
        entries <- rig.sink.entries.get
      } yield assertEquals(entries, Nil)
    )
  }

  // -----------------------------------------------------------------------------------------------
  // The apply
  // -----------------------------------------------------------------------------------------------

  test("DROP ... DELETE TOPIC is refused without a plan token and accepted with one") {
    // The required case, both halves in one, and the refusal is asserted against the *server*: the topic
    // must not be dropped and then reported as refused.
    rig().flatMap { rig =>
      for {
        refused <- rig.useCases.execute(alice, cluster, dropWithTopic, None)
        afterRefusal <- rig.server.executed.get
        planned <- rig.useCases.plan(alice, cluster, dropWithTopic)
        token = planned.toOption.flatMap(_.token)
        accepted <- rig.useCases.execute(alice, cluster, dropWithTopic, token)
        afterApply <- rig.server.executed.get
      } yield {
        assert(refused.isLeft, clue = refused)
        assertEquals(refused.left.toOption.map(_.code), Some(ErrorCode.Validation))
        assertEquals(afterRefusal, Nil)

        assert(accepted.isRight, clue = accepted)
        assertEquals(afterApply, List(dropWithTopic))
      }
    }
  }

  test("an apostrophe inside a quoted identifier cannot bypass the service confirmation guard") {
    rig().flatMap { rig =>
      for {
        result <- rig.useCases.execute(alice, cluster, "DROP STREAM `orders'archive` DELETE TOPIC;", None)
        executed <- rig.server.executed.get
      } yield {
        assertEquals(result.left.toOption.map(_.code), Some(ErrorCode.Validation))
        assertEquals(executed, Nil)
      }
    }
  }

  test("a token cannot delete a stream remapped to a different Kafka topic") {
    for {
      before <- rig()
      after <- rig(objectsAnswer =
        Right(KsqlObjects.of(List(KsqlObject.Stream("ORDERS", "replacement", Some("JSON"))), Nil))
      )
      plan <- before.useCases.plan(alice, cluster, dropWithTopic)
      result <- after.useCases.execute(alice, cluster, dropWithTopic, plan.toOption.flatMap(_.token))
      sent <- after.server.executed.get
    } yield {
      assert(result.isLeft, clue = result)
      assertEquals(sent, Nil)
    }
  }

  test("quoted DROP targets match exact case and object kind") {
    rig().flatMap { rig =>
      for {
        wrongCase <- rig.useCases.plan(alice, cluster, "DROP STREAM `orders` DELETE TOPIC;")
        wrongKind <- rig.useCases.plan(alice, cluster, "DROP TABLE ORDERS DELETE TOPIC;")
      } yield {
        assert(wrongCase.isLeft, clue = wrongCase)
        assert(wrongKind.isLeft, clue = wrongKind)
      }
    }
  }

  test("a token minted for one statement cannot be spent on another") {
    // The substitution ADR-045 exists to make impossible, in the shape a statement editor makes it take:
    // the token signs the statement's own text, so a confirmed `DROP ORDERS` cannot become `DROP USERS`.
    rig().flatMap { rig =>
      for {
        planned <- rig.useCases.plan(alice, cluster, dropWithTopic)
        token = planned.toOption.flatMap(_.token)
        swapped <- rig.useCases.execute(alice, cluster, "DROP TABLE USERS DELETE TOPIC;", token)
        executed <- rig.server.executed.get
      } yield {
        assert(swapped.isLeft, clue = swapped)
        assertEquals(executed, Nil)
      }
    }
  }

  test("a token sent with a harmless statement is ignored rather than refused") {
    // A client that always sends back the token it last received is doing nothing wrong, and refusing it
    // would make the editor fail on the request after a confirmed one.
    rig().flatMap(rig =>
      rig.useCases
        .execute(alice, cluster, "CREATE STREAM A AS SELECT * FROM ORDERS;", Some("not-a-token"))
        .map(answer => assert(answer.isRight, clue = answer))
    )
  }

  test("a SELECT ... EMIT CHANGES is refused here and the refusal names the address that answers it") {
    // The required case's first half: a push query answers a stream rather than a document. Buffering an
    // unbounded result into one JSON body is a request that never returns and a heap that never stops
    // growing, so it is refused before the server is called at all.
    rig().flatMap { rig =>
      for {
        answer <- rig.useCases.execute(alice, cluster, pushQuery, None)
        executed <- rig.server.executed.get
        audited <- rig.sink.entries.get
      } yield {
        assertEquals(answer.left.toOption.map(_.code), Some(ErrorCode.Validation))
        assert(clue(answer.left.toOption.map(_.message).getOrElse("")).contains("/ksql/stream"))
        assertEquals(executed, Nil)
        // And it is not audited: nothing happened to the cluster, and an audit trail full of rows about
        // things that did not happen is one nobody can read.
        assertEquals(audited, Nil)
      }
    }
  }

  test("a statement on a read-only cluster is refused before the server is called, and audited") {
    rig().flatMap { rig =>
      for {
        answer <- rig.useCases.execute(alice, readOnly, "CREATE STREAM A AS SELECT * FROM ORDERS;", None)
        executed <- rig.server.executed.get
        outcomes <- rig.outcomes
      } yield {
        assertEquals(answer.left.toOption.map(_.code), Some(ErrorCode.ReadOnly))
        assertEquals(executed, Nil)
        assertEquals(outcomes, List(MutationOutcome.Refused))
      }
    }
  }

  test("a statement against a cluster with no ksqlDB is KUI-UNSUPPORTED, not a 404") {
    rig().flatMap(rig =>
      rig.useCases
        .execute(alice, bare, "CREATE STREAM A AS SELECT * FROM ORDERS;", None)
        .map(answer => assertEquals(answer.left.toOption.map(_.code), Some(ErrorCode.Unsupported)))
    )
  }

  test("a statement on a configured cluster with no client is a wiring failure, not a deployment choice") {
    // W9-A1: `execute`'s `case None => notWired(cluster)` was held by nothing — replacing it with
    // `notConfigured` left all 4,299 cases in the repository green. The two sentences send an operator to
    // different places: `KUI-UNSUPPORTED` says "you did not configure a ksqlDB here", which is a screen
    // that looks deliberately switched off, and `KUI-INVALID-STATE` says "you did, and this process could
    // not build a client for it", which is a KUI defect somebody has to go and look at.
    //
    // `KsqlRig.unwired` exists for this case: it is the only profile in the rig that is configured,
    // writable, and has no client, which is the one combination that reaches this arm.
    rig().flatMap { rig =>
      for {
        answer <- rig.useCases.execute(alice, unwired, "CREATE STREAM A AS SELECT * FROM ORDERS;", None)
        executed <- rig.server.executed.get
        outcomes <- rig.outcomes
      } yield {
        assertEquals(answer.left.toOption.map(_.code), Some(ErrorCode.InvalidState))
        assert(clue(answer.left.toOption.map(_.message).getOrElse("")).contains("could not build a client"))
        assertEquals(executed, Nil)
        // And it is audited, because the guard wraps it: a statement that did not run on a cluster
        // somebody may write to is a thing an incident review looks for. `Refused` and not `Failed`,
        // because `KUI-INVALID-STATE` carries HTTP 409 and `MutationGuard` splits the two at 500 — which
        // is arguably the wrong side for a KUI wiring fault, and is recorded here as measured rather than
        // quietly asserted the other way.
        assertEquals(outcomes, List(MutationOutcome.Refused))
      }
    }
  }

  test("a statement that is not one statement is refused with a sentence about what to do") {
    rig().flatMap { rig =>
      for {
        batch <- rig.useCases.execute(alice, cluster, "SHOW STREAMS; SHOW TABLES;", None)
        empty <- rig.useCases.execute(alice, cluster, "   ", None)
        executed <- rig.server.executed.get
      } yield {
        assertEquals(batch.left.toOption.map(_.code), Some(ErrorCode.Validation))
        assertEquals(empty.left.toOption.map(_.code), Some(ErrorCode.Validation))
        assertEquals(executed, Nil)
      }
    }
  }

  test("an accepted command still pending is audited as unknown, never succeeded") {
    rig(executeAnswer = Right(StatementOutcome.Pending("queued", Some("stream/X/create")))).flatMap { rig =>
      for {
        answer <- rig.useCases.execute(alice, cluster, "CREATE STREAM X (ID STRING);", None)
        outcomes <- rig.outcomes
      } yield {
        assertEquals(answer.map(_.outcome.wire), Right("pending"))
        assertEquals(outcomes, List(MutationOutcome.Unknown))
      }
    }
  }

  test("a statement that ran is audited as succeeded and carries its own text") {
    rig().flatMap { rig =>
      for {
        _ <- rig.useCases.execute(alice, cluster, "CREATE STREAM A AS SELECT * FROM ORDERS;", None)
        entries <- rig.sink.entries.get
      } yield {
        assertEquals(entries.map(_.outcome), List(MutationOutcome.Succeeded))
        // The statement is the only thing an incident review can act on: "a ksqlDB statement was run on
        // production" names nothing, and this names the stream.
        assertEquals(entries.map(_.statement), List("CREATE STREAM A AS SELECT * FROM ORDERS;"))
        assertEquals(entries.map(_.cluster), List(cluster))
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // The stream
  // -----------------------------------------------------------------------------------------------

  test("a push query answers a stream, and its first frame is the header") {
    // The required case's second half. The header arrives as soon as the server accepts the query, which
    // is what makes a push query over an idle topic a stream that has visibly started.
    rig().flatMap { rig =>
      rig.useCases.stream(alice, cluster, pushQuery).flatMap {
        case Left(error) => IO(fail(s"the push query was refused: ${error.message}"))
        case Right(rows) =>
          for {
            frames <- rows.compile.toList
            opened <- rig.server.opened.get
          } yield {
            assertEquals(frames.head, Right(QueryFrame.Header(List("ID"))))
            assertEquals(frames.size, 2)
            assertEquals(opened, List(pushQuery))
          }
      }
    }
  }

  test("a statement that finishes is refused by the stream, and the refusal names the address") {
    rig().flatMap { rig =>
      for {
        answer <- rig.useCases.stream(alice, cluster, "SELECT * FROM ORDERS WHERE ID = '1';")
        opened <- rig.server.opened.get
      } yield {
        assertEquals(answer.left.toOption.map(_.code), Some(ErrorCode.Validation))
        assert(clue(answer.left.toOption.map(_.message).getOrElse("")).contains("/ksql/statements"))
        assertEquals(opened, Nil)
      }
    }
  }

  test("a push query on a read-only cluster is refused before the query is opened") {
    // `Action.KsqlExecute.isAlter` is what decides it: a push query asks ksqlDB to start and hold a query,
    // and a read-only cluster whose ksqlDB anybody can set queries running on is not read-only in any
    // sense an operator means. ADR-055 §3 states the cost.
    rig().flatMap { rig =>
      for {
        answer <- rig.useCases.stream(alice, readOnly, pushQuery)
        opened <- rig.server.opened.get
      } yield {
        assertEquals(answer.left.toOption.map(_.code), Some(ErrorCode.ReadOnly))
        assertEquals(opened, Nil)
      }
    }
  }

  test("a push query against a cluster with no ksqlDB is KUI-UNSUPPORTED") {
    rig().flatMap(rig =>
      rig.useCases
        .stream(alice, bare, pushQuery)
        .map(answer => assertEquals(answer.left.toOption.map(_.code), Some(ErrorCode.Unsupported)))
    )
  }

  test("a configured cluster with no client built says so, and does not say 'not configured'") {
    // A wiring failure reported as a deployment choice would hide a KUI defect behind a screen that looks
    // deliberately switched off.
    rig(onEveryCluster = false).flatMap(rig =>
      rig.useCases.objects(alice, readOnly).map {
        case Right(KsqlListing.Answered(Left(error))) =>
          assertEquals(error.code, ErrorCode.InvalidState)
          assert(clue(error.message).contains("could not build a client"))
        case other => fail(s"expected an answered-with-failure, got $other")
      }
    )
  }
}
