package kui.ksql.application

import cats.effect.{IO, Ref}
import fs2.Stream

import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.kernel.{ClusterId, Secret, UserName}
import kui.ksql.domain.*
import kui.security.audit.MutationOutcome
import kui.security.{Principal, PrincipalKind}

/** The fakes every application-layer case in this service is built from.
  *
  * Hand-written rather than mocked (ADR-018), and every one of them **counts what it was asked**. That is not
  * decoration: half the rules in this layer are statements about *whether the server was called at all* — a
  * read-only cluster is refused before the statement goes out, a missing confirmation is refused before the
  * topic is dropped — and a fake that answered a constant could not tell a caller who was refused from one
  * who was never allowed to reach it.
  */
object KsqlRig {

  val cluster: ClusterId = ClusterId.unsafe("prod-eu")
  val readOnly: ClusterId = ClusterId.unsafe("prod-us")
  val bare: ClusterId = ClusterId.unsafe("no-ksql")

  /** Configured, writable, and no client is ever built for it.
    *
    * W9-A1 added it. The three profiles above cannot express "a KUI wiring failure on a cluster somebody is
    * allowed to write to": `bare` is not configured, so it never reaches the client lookup, and `readOnly` is
    * refused by the guard before it gets there. So `execute`'s `notWired` arm had no input that could reach
    * it and the arm could be replaced by `notConfigured` with the whole repository still green — exactly the
    * fixture-shaped hole W8-A1's clue names.
    */
  val unwired: ClusterId = ClusterId.unsafe("wiring-broken")

  val alice: Principal = Principal(UserName.unsafe("alice"), Set.empty, PrincipalKind.Session)

  /** Thirty-two bytes, which is what HMAC-SHA256 wants. */
  val key: Secret[Array[Byte]] = Secret(Array.fill[Byte](32)(7))

  val orders: KsqlObject = KsqlObject.Stream("ORDERS", "orders", Some("JSON"))
  val users: KsqlObject = KsqlObject.Table("USERS", "users", Some("AVRO"), windowed = false)

  val listing: KsqlObjects = KsqlObjects.of(List(orders, users), Nil)

  private def only(items: List[KsqlObject], name: String, kind: KsqlObjectKind): Option[KsqlObject] =
    items.filter(item => item.name == name && item.kind == kind) match {
      case item :: Nil => Some(item)
      case _ => None
    }

  val created: StatementOutcome = StatementOutcome.Status("Stream created and running", None)

  /** A ksqlDB that answers what it was told to and records every call it was asked to make. */
  final class CountingServer(
      objectsAnswer: Either[KuiError, KsqlObjects],
      executeAnswer: Either[KuiError, StatementOutcome],
      val executed: Ref[IO, List[String]],
      val reads: Ref[IO, Int],
      val opened: Ref[IO, List[String]],
      frames: List[Either[KuiError, QueryFrame]],
      catalog: Option[List[KsqlObject]]
  ) extends KsqlClient[IO] {

    def objects: IO[Either[KuiError, KsqlObjects]] =
      reads.update(_ + 1).as(catalog.fold(objectsAnswer)(items => Right(KsqlObjects.of(items, Nil))))

    def destructiveTarget(name: String, kind: KsqlObjectKind): IO[Either[KuiError, Option[KsqlObject]]] = {
      val found = catalog match {
        case Some(items) => Right(only(items, name, kind))
        case None => objectsAnswer.map(objects => only(objects.items, name, kind))
      }
      IO.pure(found)
    }

    def execute(statement: KsqlStatement): IO[Either[KuiError, StatementOutcome]] =
      executed.update(_ :+ statement.canonical).as(executeAnswer)

    def rows(statement: KsqlStatement): Stream[IO, Either[KuiError, QueryFrame]] =
      Stream.exec(opened.update(_ :+ statement.canonical)) ++ Stream.emits(frames)
  }

  final class Source(clients: Map[ClusterId, KsqlClient[IO]]) extends ClusterKsqlSource[IO] {

    private val views = List(
      KsqlProfileView(cluster, "Production EU", readOnly = false, configured = true),
      KsqlProfileView(KsqlRig.readOnly, "Production US", readOnly = true, configured = true),
      KsqlProfileView(bare, "Staging", readOnly = false, configured = false),
      KsqlProfileView(unwired, "Broken wiring", readOnly = false, configured = true)
    )

    def profileOf(id: ClusterId): IO[Either[KuiError, KsqlProfileView]] =
      IO.pure(
        views
          .find(_.cluster == id)
          .toRight(ApplicationError.NotFound("cluster", id.value, ErrorCode.ClusterNotFound): KuiError)
      )

    def all: IO[List[KsqlProfileView]] = IO.pure(views)

    def client(id: ClusterId): IO[Option[KsqlClient[IO]]] = IO.pure(clients.get(id))
  }

  final class RecordingSink(val entries: Ref[IO, List[KsqlStatementRecord]]) extends KsqlStatementSink[IO] {
    def record(entry: KsqlStatementRecord): IO[Unit] = entries.update(_ :+ entry)
  }

  /** A sink that throws, for the rule that says the audit trail may not fail the operation it describes. */
  val failingSink: KsqlStatementSink[IO] = new KsqlStatementSink[IO] {
    def record(entry: KsqlStatementRecord): IO[Unit] =
      IO.raiseError(new RuntimeException("the audit disk is full"))
  }

  final case class Rig(
      useCases: KsqlUseCases[IO],
      server: CountingServer,
      sink: RecordingSink,
      tokens: KsqlPlanToken[IO]
  ) {
    def outcomes: IO[List[MutationOutcome]] = sink.entries.get.map(_.map(_.outcome))
  }

  /** The whole application layer, wired the way the composition root wires it.
    *
    * `onEveryCluster` puts the same server behind the read-only cluster too, which is what lets a case assert
    * that a refusal happened *before* it was called rather than that it would have failed if it had been.
    */
  def rig(
      objectsAnswer: Either[KuiError, KsqlObjects] = Right(listing),
      executeAnswer: Either[KuiError, StatementOutcome] = Right(created),
      frames: List[Either[KuiError, QueryFrame]] = List(
        Right(QueryFrame.Header(List("ID"))),
        Right(QueryFrame.Row(QueryRow(List(Some("17")))))
      ),
      onEveryCluster: Boolean = true,
      catalog: Option[List[KsqlObject]] = None
  ): IO[Rig] =
    for {
      executed <- Ref.of[IO, List[String]](Nil)
      reads <- Ref.of[IO, Int](0)
      opened <- Ref.of[IO, List[String]](Nil)
      entries <- Ref.of[IO, List[KsqlStatementRecord]](Nil)
      logger <- kui.testkit.fakes.FakeStructuredLogger[IO]
      server = new CountingServer(objectsAnswer, executeAnswer, executed, reads, opened, frames, catalog)
      clients =
        if onEveryCluster then Map(cluster -> (server: KsqlClient[IO]), readOnly -> (server: KsqlClient[IO]))
        else Map(cluster -> (server: KsqlClient[IO]))
      sources = new Source(clients)
      sink = new RecordingSink(entries)
      tokens = KsqlPlanToken.make[IO](key)
      guard = MutationGuard.make[IO](sources, sink, logger)
    } yield Rig(KsqlUseCases.make[IO](sources, tokens, guard), server, sink, tokens)
}
