package kui.message.application

import java.time.Instant

import scala.concurrent.duration.DurationInt

import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import fs2.Stream

import kui.kernel.browse.PollBudget
import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.kernel.serde.{PayloadKind, SerdeName, SerdeUse, Target}
import kui.kernel.{ClusterId, Offset, PartitionId, TopicName}
import kui.message.domain.ports.{BrowseCluster, ClusterProfileSource, SerdeChoice, SerdeSource}
import kui.message.domain.{
  BrowseRequest,
  Decoded,
  MatchOperator,
  MatchSource,
  TimestampType,
  TrackMatch,
  TrackQuery
}
import kui.testkit.KuiIOSuite

/** What a track promises: it reads the topics it was told to, it stops where it was told to, and it says how
  * much it read.
  *
  * The last of those is the one that is easy to leave out and expensive to be without. A support engineer who
  * is told "no hits" needs to know whether the scan read a million records and rejected them or read nothing
  * at all, because the first means the value is not there and the second means they searched the wrong
  * window.
  */
final class TrackUseCaseSuite extends KuiIOSuite {

  private val cluster = ClusterId.unsafe("local")
  private val orders = TopicName.unsafe("orders.v1")
  private val shipments = TopicName.unsafe("shipments.v1")
  private val budget = PollBudget.unsafe(10_000, 1L << 20, 30.seconds)

  private val start = Instant.parse("2026-09-04T09:00:00Z")

  private val clusters: ClusterProfileSource[IO] = (id: ClusterId) =>
    IO.pure(
      if id == cluster then Right(BrowseCluster(cluster, "Local", readOnly = false, Instant.EPOCH, false))
      else Left(ApplicationError.NotFound("cluster", id.value, ErrorCode.ClusterNotFound))
    )

  private val serdes: SerdeSource[IO] = new SerdeSource[IO] {

    def decode(
        cluster: ClusterId,
        topic: TopicName,
        target: Target,
        requested: Option[SerdeName],
        bytes: Option[Array[Byte]]
    ): IO[(Decoded, Option[String])] =
      IO.pure(
        (
          Decoded(bytes.fold("")(new String(_, "UTF-8")), PayloadKind.Text, SerdeName.String, Map.empty),
          None
        )
      )

    def serialize(
        cluster: ClusterId,
        topic: TopicName,
        target: Target,
        requested: Option[SerdeName],
        properties: Map[String, String],
        text: Option[String]
    ): IO[Either[KuiError, Option[Array[Byte]]]] = IO.pure(Right(None))

    def choices(
        cluster: ClusterId,
        topic: TopicName,
        target: Target,
        use: SerdeUse
    ): IO[Either[KuiError, List[SerdeChoice]]] = IO.pure(Right(Nil))
  }

  private def record(offset: Long, value: String, at: Instant = start): RawRecord =
    RawRecord(
      partition = PartitionId.unsafe(0),
      offset = Offset.unsafe(offset),
      timestamp = at,
      timestampType = TimestampType.CreateTime,
      key = Some(s"key-$offset".getBytes("UTF-8")),
      value = Some(value.getBytes("UTF-8")),
      headers = Nil,
      keySize = 5,
      valueSize = value.length,
      headersSize = 0
    )

  /** A record source that answers with a different log per topic, so that a track over two topics can be told
    * apart from a track that read one of them twice.
    */
  private def sourceOf(logs: Map[TopicName, List[RawRecord]]): RecordSource[IO] =
    new RecordSource[IO] {
      override def scan(request: BrowseRequest, budget: PollBudget, upperOffsets: Map[PartitionId, Offset]) =
        FiniteRecordScan(logs.getOrElse(request.topic, Nil), budget)

      def browse(request: BrowseRequest, budget: PollBudget): Stream[IO, Either[KuiError, RawRecord]] =
        Stream.emits(logs.getOrElse(request.topic, Nil).map(_.asRight[KuiError]))

      def assignedStarts(request: BrowseRequest): IO[Either[KuiError, Map[PartitionId, Offset]]] =
        IO.pure(Right(Map.empty))
    }

  private def queryOver(
      topics: List[TopicName],
      value: String,
      operator: MatchOperator = MatchOperator.Contains,
      source: MatchSource = MatchSource.Value,
      limit: Option[Int] = None
  ): TrackQuery =
    TrackQuery
      .of(
        cluster = cluster,
        topics = topics,
        from = start.minusSeconds(60),
        until = start.plusSeconds(60),
        matcher = TrackMatch(source, operator, value),
        limit = limit,
        isolation = None,
        correlationKey = None
      )
      .getOrElse(fail("the query under test is not a legal track"))

  private def run(
      logs: Map[TopicName, List[RawRecord]],
      query: TrackQuery,
      masking: RecordMasking[IO] = RecordMasking.none[IO]
  ): IO[List[TrackEvent]] =
    TrackUseCase
      .make[IO](clusters, serdes, sourceOf(logs), masking)
      .track(query, budget)
      .compile
      .toList

  private def hits(events: List[TrackEvent]): List[(String, String)] =
    events.collect { case TrackEvent.Hit(hit) => (hit.topic.value, hit.record.value.text) }

  private def ending(events: List[TrackEvent]): TrackEvent.Finished =
    events
      .collectFirst { case finished: TrackEvent.Finished => finished }
      .getOrElse(fail("a track must always say how it ended"))

  test("tracking uses one deadline rather than restarting it for every topic") {
    val logs = Map(orders -> List(record(0L, "a")), shipments -> List(record(0L, "b")))
    val slow = new RecordSource[IO] {
      def browse(request: BrowseRequest, budget: PollBudget) = sourceOf(logs).browse(request, budget)
      def assignedStarts(request: BrowseRequest) = sourceOf(logs).assignedStarts(request)
      override def scan(request: BrowseRequest, budget: PollBudget, upperOffsets: Map[PartitionId, Offset]) =
        Stream.sleep_[IO](6.seconds) ++ sourceOf(logs).scan(request, budget, upperOffsets)
    }
    TestControl.executeEmbed(
      TrackUseCase
        .make[IO](clusters, serdes, slow, RecordMasking.none[IO])
        .track(queryOver(List(orders, shipments), "absent"), PollBudget.unsafe(100, 10000, 10.seconds))
        .compile
        .toList
        .map { events =>
          assertEquals(ending(events).read, 1L)
          assert(ending(events).truncated)
        }
    )
  }

  test("tracking shares its byte budget across topics") {
    val logs = Map(orders -> List(record(0L, "a")), shipments -> List(record(0L, "b")))
    TrackUseCase
      .make[IO](clusters, serdes, sourceOf(logs), RecordMasking.none[IO])
      .track(queryOver(List(orders, shipments), "absent"), PollBudget.unsafe(100, 6, 30.seconds))
      .compile
      .toList
      .map { events =>
        assertEquals(ending(events).read, 1L)
        assert(ending(events).truncated)
      }
  }

  test("tracking shares its record budget across topics and reports incomplete scans") {
    val logs = Map(orders -> List(record(0L, "a")), shipments -> List(record(0L, "b"), record(1L, "c")))
    TrackUseCase
      .make[IO](clusters, serdes, sourceOf(logs), RecordMasking.none[IO])
      .track(queryOver(List(orders, shipments), "absent"), PollBudget.unsafe(2, 10000, 30.seconds))
      .compile
      .toList
      .map { events =>
        assertEquals(ending(events).read, 2L)
        assert(ending(events).truncated)
      }
  }

  test("tracking excludes timestamps below from even at later offsets") {
    val logs = Map(orders -> List(record(0L, "hit"), record(1L, "hit", start.minusSeconds(120))))
    run(logs, queryOver(List(orders), "hit")).map(events => assertEquals(hits(events).size, 1))
  }

  test("a track finds the value in every topic it was given, and says which topic each hit came from") {
    // The whole point of the feature: the answer to "where did order 4711 go" spans topics, and a result
    // that did not name them would leave the reader with a list of records and no story.
    val logs = Map(
      orders -> List(record(0L, "order-4711 created"), record(1L, "order-9 created")),
      shipments -> List(record(0L, "order-4711 shipped"))
    )

    run(logs, queryOver(List(orders, shipments), "order-4711")).map { events =>
      assertEquals(
        hits(events),
        List((orders.value, "order-4711 created"), (shipments.value, "order-4711 shipped"))
      )
    }
  }

  test("a track reports how much it read, not only what it matched") {
    val logs = Map(orders -> List(record(0L, "a"), record(1L, "b"), record(2L, "target")))

    run(logs, queryOver(List(orders), "target")).map { events =>
      val finished = ending(events)
      assertEquals((finished.read, finished.matched), (3L, 1L))
      assertEquals(finished.truncated, false)
    }
  }

  test("a track that matched nothing is a finished scan and not a failure") {
    // "Not found" is an answer. A track that failed here would send a support engineer looking for a
    // broken cluster instead of a different window.
    val logs = Map(orders -> List(record(0L, "nothing here")))

    run(logs, queryOver(List(orders), "order-4711")).map { events =>
      assert(hits(events).isEmpty)
      assertEquals(ending(events).read, 1L)
      assert(!events.exists {
        case TrackEvent.Failed(_) => true
        case _ => false
      })
    }
  }

  test("a track stops at its hit cap and says that it did") {
    // A truncated result presented as a complete one is the failure this flag exists to prevent: the
    // reader concludes the sixth topic has nothing in it when the scan never reached it.
    val logs = Map(orders -> List(record(0L, "hit"), record(1L, "hit"), record(2L, "hit")))

    run(logs, queryOver(List(orders), "hit", limit = Some(2))).map { events =>
      assertEquals(hits(events).size, 2)
      assertEquals(ending(events).truncated, true)
    }
  }

  test("a record outside the window is not a hit even though the browse read it") {
    // A browse seeks to a timestamp and reads forwards; it has no notion of "until". The far end of the
    // window is this use case's to enforce, and without it a track would answer with records from after
    // the incident the user is investigating.
    val logs = Map(
      orders -> List(
        record(0L, "order-4711 inside"),
        record(1L, "order-4711 outside", at = start.plusSeconds(3600))
      )
    )

    run(logs, queryOver(List(orders), "order-4711")).map(events =>
      assertEquals(hits(events).map(_._2), List("order-4711 inside"))
    )
  }

  test("a header search looks at that header and not at the value") {
    // The reference product's defect, refused by construction here: a header search with the value in the
    // payload must not match, or a user who mis-selects the source gets plausible wrong answers.
    val withHeader = record(0L, "no mention in the value").copy(headers =
      List(RawHeader("order-id", Some("4711".getBytes("UTF-8"))))
    )
    val withValue = record(1L, "4711 in the value")

    val logs = Map(orders -> List(withHeader, withValue))

    run(logs, queryOver(List(orders), "4711", source = MatchSource.Header("order-id"))).map(events =>
      assertEquals(hits(events).map(_._2), List("no mention in the value"))
    )
  }

  test("a regular expression is compiled once and matches anywhere in the field") {
    val logs = Map(orders -> List(record(0L, "id=ORDER-4711;state=PAID"), record(1L, "id=CART-1")))

    run(logs, queryOver(List(orders), "ORDER-\\d+", operator = MatchOperator.Regex)).map(events =>
      assertEquals(hits(events).map(_._2), List("id=ORDER-4711;state=PAID"))
    )
  }

  test("a cluster nobody configured fails the track rather than answering with no hits") {
    // "No hits" and "there is no such cluster" must not be the same screen: the first is an answer about
    // the data and the second is a mistake in the request.
    val query = queryOver(List(orders), "anything")
    val elsewhere = TrackQuery
      .of(
        cluster = ClusterId.unsafe("nowhere"),
        topics = List(orders),
        from = query.from,
        until = query.until,
        matcher = query.matcher,
        limit = None,
        isolation = None,
        correlationKey = None
      )
      .getOrElse(fail("the query under test is not a legal track"))

    run(Map.empty, elsewhere).map(events =>
      assert(events.exists {
        case TrackEvent.Failed(_) => true
        case _ => false
      })
    )
  }

  // ----------------------------------------------------------------------------------- the masking

  test("a track's hits are masked, and a masked value is not trackable") {
    // A TRACK IS A SECOND WAY OUT OF THIS SERVICE, AND ADR-023 SAYS "BEFORE ANY DTO LEAVES THE SERVICE".
    // Masking only the browse would leave the same record readable in full through `POST /track`, which
    // is the shape of defect a per-endpoint rule always takes. It is masked at the decode, so the
    // matcher sees the mask too: a track that matched on the unmasked text would return the record while
    // drawing asterisks, which tells the searcher what they were looking for without printing it.
    val logs = Map(orders -> List(record(0L, "id=ORDER-4711;card=4111111111111111")))
    val hideEverything: RecordMasking[IO] =
      (_, _) => IO.pure(hit => hit.copy(value = hit.value.copy(text = "<masked>")))

    for {
      byMask <- run(logs, queryOver(List(orders), "<masked>"), hideEverything)
      byOriginal <- run(logs, queryOver(List(orders), "4111111111111111"), hideEverything)
    } yield {
      assertEquals(hits(byMask).map(_._2), List("<masked>"))
      assert(hits(byOriginal).isEmpty, clue = hits(byOriginal))
    }
  }

  test("a track over two topics resolves each topic's own mask, so a rule scoped to the second reaches it") {
    // ONCE PER TOPIC MEANS ONCE PER *THAT* TOPIC. The mask is resolved inside the per-topic loop, and the
    // topic it is resolved for is the one the loop is on -- resolving it for `query.topics.head` instead
    // would hand every topic of the track the FIRST topic's rules, which on a six-topic track is five
    // topics masked by somebody else's rules and, when the first topic has none, five topics not masked
    // at all. The masking fake beside this one takes `(_, _)` and cannot see that; this one reads its
    // topic argument, and the two topics are given deliberately different answers. Filed as W9-06/F2.
    val logs = Map(
      orders -> List(record(0L, "card=4111111111111111")),
      shipments -> List(record(0L, "card=4111111111111111"))
    )
    val onlyShipments: RecordMasking[IO] =
      (_, topic) =>
        IO.pure(
          if topic == shipments then
            hit => hit.copy(value = hit.value.copy(text = hit.value.text.replace("4111111111111111", "****")))
          else RecordMask.identity
        )

    run(logs, queryOver(List(orders, shipments), "card="), onlyShipments).map { events =>
      assertEquals(
        hits(events),
        List(orders.value -> "card=4111111111111111", shipments.value -> "card=****")
      )
    }
  }
}
