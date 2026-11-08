package kui.consumer.contract

import io.circe.parser.{decode, parse}
import io.circe.syntax.*
import io.circe.{Decoder, Encoder, Json}
import munit.ScalaCheckSuite
import org.scalacheck.Prop.forAll
import org.scalacheck.{Arbitrary, Gen}

import kui.consumer.contract.dto.*
import kui.contracts.consumer.*
import kui.contracts.consumer.GroupCodecs.given
import kui.contracts.paging.PageDto
import kui.kernel.GroupId
import kui.kernel.group.{GroupProtocol, GroupState, LagAnomaly, ResetTarget}

/** That the documents this service sends are the documents committed beside it, and that a browser reading
  * them gets the same values back.
  *
  * Cross-compiled on purpose. Running the identical assertions under Node and on the JVM is what makes M1's
  * second integration defect impossible: a browser decoding `{"items": …}` from a server sending
  * `{"clusters": …}`, whose decoder defaulted the missing field to `Nil`, so every response decoded
  * *successfully* into zero rows under a "last updated just now" timestamp. That defect passed every unit
  * test on both sides, because no test ever ran one side's bytes through the other side's decoder.
  */
final class ConsumerContractSuite extends ScalaCheckSuite {

  /** Compares two documents as JSON rather than as text, so that a difference in whitespace is not reported
    * as a difference in the contract — while a difference in a field name or a value still is.
    */
  private def assertMatchesGolden[A: Encoder](name: String, value: A, golden: String): Unit = {
    val expected = parse(golden).fold(failure => fail(s"$name is not JSON: ${failure.message}"), identity)
    assertNoDiff(value.asJson.spaces2, expected.spaces2)
  }

  private def assertRoundTrips[A: {Encoder, Decoder}](name: String, value: A)(using CanEqual[A, A]): Unit =
    decode[A](value.asJson.noSpaces) match {
      case Right(back) => assertEquals(back, value, s"$name did not survive a round trip")
      case Left(failure) => fail(s"$name failed to decode: ${failure.getMessage}")
    }

  test("theGroupPageMatchesItsGoldenFile") {
    assertMatchesGolden("group-page", ConsumerSamples.page, GoldenDocuments.groupPage)
  }

  test("theGroupListResponseMatchesItsGoldenFile") {
    assertMatchesGolden("groups-response", ConsumerSamples.groupsResponse, GoldenDocuments.groupsResponse)
  }

  test("theStaleGroupListResponseMatchesItsGoldenFile") {
    // The document the freshness envelope exists for. Its rows are byte-for-byte the fresh document's, so
    // if `status` and `reason` were ever dropped from the encoding this assertion is the only thing that
    // would notice — the rows themselves cannot tell anyone the cluster has gone away.
    assertMatchesGolden(
      "groups-response-stale",
      ConsumerSamples.groupsResponseStale,
      GoldenDocuments.groupsResponseStale
    )
  }

  test("aStaleGroupListStillCarriesEveryRow") {
    // Stated as its own assertion because the alternative failure is silent: a stale section that dropped
    // its data would render as an empty table, which claims the cluster has no consumer groups.
    val decoded = decode[GroupsResponse](GoldenDocuments.groupsResponseStale)
      .getOrElse(fail("the stale list document did not decode"))

    assertEquals(decoded.groups.status, "stale")
    assertEquals(decoded.groups.toOption.map(_.items.size), Some(2))
  }

  test("theGroupDetailMatchesItsGoldenFile") {
    assertMatchesGolden("group-detail", ConsumerSamples.detail, GoldenDocuments.groupDetail)
  }

  test("theLagDeltaMatchesItsGoldenFile") {
    assertMatchesGolden("lag-delta", ConsumerSamples.lagDelta, GoldenDocuments.lagDelta)
  }

  test("theTopicConsumersMatchTheirGoldenFile") {
    assertMatchesGolden("topic-consumers", ConsumerSamples.topicConsumers, GoldenDocuments.topicConsumers)
  }

  test("theResetPlanMatchesItsGoldenFile") {
    assertMatchesGolden("reset-plan", ConsumerSamples.resetPlan, GoldenDocuments.resetPlan)
  }

  test("theResetPlanRequestMatchesItsGoldenFile") {
    assertMatchesGolden(
      "reset-plan-request",
      ConsumerSamples.resetPlanRequest,
      GoldenDocuments.resetPlanRequest
    )
  }

  test("theResetApplyRequestMatchesItsGoldenFile") {
    assertMatchesGolden(
      "reset-apply-request",
      ConsumerSamples.resetApplyRequest,
      GoldenDocuments.resetApplyRequest
    )
  }

  test("theDeletedOffsetsMatchTheirGoldenFile") {
    assertMatchesGolden("deleted-offsets", ConsumerSamples.deletedOffsets, GoldenDocuments.deletedOffsets)
  }

  test("theIncompleteSectionMatchesItsGoldenFile") {
    val incomplete = ConsumerSamples.unknownLagSummary.incomplete.getOrElse(fail("the sample has none"))
    assertMatchesGolden("incomplete", incomplete, GoldenDocuments.incomplete)
  }

  test("everyGoldenFileDecodes") {
    // The reverse direction. Without it a golden file that is not actually valid — a trailing comma, a
    // renamed field — sits there passing every encode test, because an encode test never reads it back.
    assert(decode[PageDto[GroupSummaryDto]](GoldenDocuments.groupPage).isRight)
    assert(decode[GroupsResponse](GoldenDocuments.groupsResponse).isRight)
    assert(decode[GroupsResponse](GoldenDocuments.groupsResponseStale).isRight)
    assert(decode[GroupDetailDto](GoldenDocuments.groupDetail).isRight)
    assert(decode[LagDeltaDto](GoldenDocuments.lagDelta).isRight)
    assert(decode[TopicConsumersDto](GoldenDocuments.topicConsumers).isRight)
    assert(decode[ResetPlanDto](GoldenDocuments.resetPlan).isRight)
    assert(decode[ResetPlanRequest](GoldenDocuments.resetPlanRequest).isRight)
    assert(decode[ResetApplyRequest](GoldenDocuments.resetApplyRequest).isRight)
    assert(decode[DeletedOffsetsDto](GoldenDocuments.deletedOffsets).isRight)
    assert(decode[IncompleteDto](GoldenDocuments.incomplete).isRight)
  }

  /** A coordinator, whole or absent — never half of one, which is the invariant the mapping keeps. */
  private val coordinators: Gen[Option[(Int, String, Int)]] =
    Gen.option(
      for {
        id <- Gen.chooseNum(0, 12)
        port <- Gen.oneOf(9092, 9093, 29092)
      } yield (id, s"broker-$id.kafka.svc", port)
    )

  private val summaries: Gen[GroupSummaryDto] =
    for {
      id <- Gen.identifier.map(GroupId.unsafe)
      state <- Gen.oneOf(GroupState.All)
      protocol <- Gen.oneOf(GroupProtocol.All)
      members <- Gen.chooseNum(0, 50)
      topics <- Gen.chooseNum(0, 20)
      partitions <- Gen.chooseNum(0, 500)
      coordinator <- coordinators
      lag <- Gen.option(Gen.chooseNum(0L, 1000000L))
      pace <- Gen.option(Gen.chooseNum(-1000.0d, 1000.0d))
      excluded <- Gen.chooseNum(0, partitions)
    } yield GroupSummaryDto(
      id,
      state,
      protocol,
      false,
      members,
      topics,
      partitions,
      coordinator.map(_._1),
      coordinator.map(_._2),
      coordinator.map(_._3),
      lag,
      pace,
      excluded,
      None
    )

  property("theCoordinatorsThreeFieldsAreOnTheWireTogetherOrNotAtAll") {
    // An encoder that dropped the port while keeping the host would put `broker-2:undefined` on the screen.
    // The round-trip property below does catch *that* — it compares whole records, so a dropped port fails
    // it, which was checked by mutation. What it cannot catch is a rename: an encoder and a decoder that
    // agreed on `coordinator_port` round-trip perfectly and put nothing on a screen expecting
    // `coordinatorPort`. A rename is caught elsewhere too, and the earlier wording here denied it:
    // `assertMatchesGolden` compares encodings against committed strings that spell the three keys out, so
    // renaming the encoder and decoder keys together fails the golden cases as well as this one — checked by
    // mutation (`coordinatorPort` -> `coordinator_port` fails five cases in this class). What this property
    // adds is the part a golden cannot state: over every generated summary, and not over the handful of
    // samples the goldens fix, the three keys are present or absent *together*.
    forAll(summaries) { summary =>
      val cursor = summary.asJson.hcursor
      val present = List(
        cursor.get[Option[Int]]("coordinatorId").toOption.flatten.isDefined,
        cursor.get[Option[String]]("coordinatorHost").toOption.flatten.isDefined,
        cursor.get[Option[Int]]("coordinatorPort").toOption.flatten.isDefined
      )

      assertEquals(present.distinct.size, 1, clue = summary.asJson.noSpaces)
      true
    }
  }

  property("absentLagEncodesAsNullNotZero") {
    forAll(summaries) { summary =>
      val encoded = summary.asJson
      val lag = encoded.hcursor.get[Option[Long]]("totalLag").getOrElse(fail("totalLag is not on the wire"))
      // The whole of DEVPLAN §10 D6, on the wire. A `0` for an unknown lag is a lie the browser has no way
      // to detect, and the operator who reads it stops investigating.
      assertEquals(lag, summary.totalLag)
      if summary.totalLag.isEmpty then
        assertEquals(encoded.hcursor.downField("totalLag").focus, Some(Json.Null))
      true
    }
  }

  test("anAbsentOptionalFieldIsNullNotOmitted") {
    // Decided once, here: KUI's documents carry explicit nulls. A browser that has to distinguish "the field
    // was absent" from "the field was null" is a browser with two code paths where the server has one.
    val keys = ConsumerSamples.healthySummary.asJson.asObject.map(_.keys.toSet).getOrElse(Set.empty)
    assert(keys.contains("incomplete"), "an absent optional field was omitted rather than written as null")
    assertEquals(ConsumerSamples.healthySummary.asJson.hcursor.downField("incomplete").focus, Some(Json.Null))
  }

  test("roundTripsForEveryDto") {
    assertRoundTrips("group-page", ConsumerSamples.page)
    assertRoundTrips("group-detail", ConsumerSamples.detail)
    assertRoundTrips("lag-delta", ConsumerSamples.lagDelta)
    assertRoundTrips("topic-consumers", ConsumerSamples.topicConsumers)
    assertRoundTrips("reset-plan", ConsumerSamples.resetPlan)
    assertRoundTrips("reset-plan-request", ConsumerSamples.resetPlanRequest)
    assertRoundTrips("reset-apply-request", ConsumerSamples.resetApplyRequest)
    assertRoundTrips("deleted-offsets", ConsumerSamples.deletedOffsets)
  }

  property("roundTripsForAnyGroupSummary") {
    forAll(summaries) { summary =>
      decode[GroupSummaryDto](summary.asJson.noSpaces) == Right(summary)
    }
  }

  test("unknownFieldsAreIgnored") {
    // Additive compatibility (`ARCHITECTURE.md` §5): a newer service may add a field, and an older browser
    // must go on working rather than failing to decode the whole page.
    val withExtra = ConsumerSamples.lagDelta.asJson.deepMerge(Json.obj("somethingNew" -> Json.fromInt(1)))
    assertEquals(decode[LagDeltaDto](withExtra.noSpaces), Right(ConsumerSamples.lagDelta))
  }

  test("aMissingRequiredFieldIsADecodeFailureNotADefault") {
    // M1's defect 2, as a test. Decoding `{}` into a page must fail; it must not yield an empty list under a
    // fresh timestamp, which is what the browser did for a whole milestone with nothing anywhere reporting it.
    assert(decode[PageDto[GroupSummaryDto]]("{}").isLeft)
    assert(decode[TopicConsumersDto]("{}").isLeft)
    assert(decode[GroupDetailDto]("{}").isLeft)
    assert(decode[LagDeltaDto]("""{"changed":[],"gone":[]}""").isLeft)
  }

  test("vocabularyCodecsUseTheKernelWireStrings") {
    // One line each, over `wire` and `from`, so that renaming a case renames it everywhere at once. A codec
    // written as `case "STABLE" => Stable` would be a second declaration of the vocabulary, which is what
    // build rule A14 exists to forbid.
    GroupState.All.foreach(state => assertEquals(state.asJson, Json.fromString(state.wire)))
    GroupProtocol.All.foreach(protocol => assertEquals(protocol.asJson, Json.fromString(protocol.wire)))
    ResetTarget.All.foreach(target => assertEquals(target.asJson, Json.fromString(target.wire)))
    LagAnomaly.All.foreach(anomaly => assertEquals(anomaly.asJson, Json.fromString(anomaly.wire)))
  }

  test("anUnknownStateStringIsADecodeFailure") {
    // Not `Unknown`. `UNKNOWN` means "the broker did not say"; answering a typo with it would return the
    // wrong page instead of a 400 naming the parameter.
    assert(decode[GroupState](""""STABEL"""").isLeft)
    assert(decode[ResetTarget](""""EARLIST"""").isLeft)
    assert(decode[LagAnomaly](""""NO_COMMITS"""").isLeft)
  }

  test("noDtoFieldNameLooksLikeACredential") {
    // A reflective walk is not available on Scala.js, so this is a walk over the encoded documents instead:
    // the field names that actually reach the wire, which is the set that matters.
    //
    // `token` is on the allow-list, named here rather than pattern-matched away: a plan token is an HMAC over
    // offsets that the server itself computed, it expires in five minutes, and it carries no authority beyond
    // applying that one plan to that one group. It is not a credential; a password, a JAAS config or a
    // session cookie would be.
    val allowed = Set("token")
    val suspicious = "(?i).*(password|secret|jaas|credential|passphrase|apikey).*".r
    val documents = List(
      ConsumerSamples.page.asJson,
      ConsumerSamples.detail.asJson,
      ConsumerSamples.lagDelta.asJson,
      ConsumerSamples.topicConsumers.asJson,
      ConsumerSamples.resetPlan.asJson,
      ConsumerSamples.resetPlanRequest.asJson,
      ConsumerSamples.resetApplyRequest.asJson,
      ConsumerSamples.deletedOffsets.asJson
    )
    def fields(json: Json): List[String] =
      json.fold(
        Nil,
        _ => Nil,
        _ => Nil,
        _ => Nil,
        array => array.toList.flatMap(fields),
        obj => obj.toList.flatMap((key, value) => key :: fields(value))
      )
    val offenders = documents.flatMap(fields).filterNot(allowed.contains).filter(suspicious.matches)
    assertEquals(offenders, Nil, "a consumer document carries a field whose name reads like a credential")
  }

  test("instantsAreIso8601Utc") {
    // One rendering, so the browser's formatter has one input shape. M1's integration found two screens
    // disagreeing about a timezone offset; a single wire format is where that stops.
    val rendered = ConsumerSamples.detail.asJson.hcursor.get[String]("observedAt")
    assertEquals(rendered, Right("2026-09-04T09:15:00.000Z"))
    assertEquals(
      ConsumerSamples.resetPlan.asJson.hcursor.get[String]("expiresAt"),
      Right("2026-09-04T09:20:00.000Z")
    )
  }

  test("theResetPlanRequestRejectsAModeWithoutItsParameter") {
    // `TIMESTAMP` with no timestamp is a malformed request, not a request that means "now". Defaulting it
    // would reset a consumer group to a point in time nobody asked for.
    def body(target: String, extra: String): String =
      s"""{"topic":"orders","partitions":[],"target":"$target"$extra}"""
    assert(decode[ResetPlanRequest](body("TIMESTAMP", "")).isLeft)
    assert(decode[ResetPlanRequest](body("OFFSET", "")).isLeft)
    assert(decode[ResetPlanRequest](body("SHIFT_BY", "")).isLeft)
    assert(decode[ResetPlanRequest](body("DURATION", "")).isLeft)
    assert(decode[ResetPlanRequest](body("EARLIEST", "")).isRight)
    assert(decode[ResetPlanRequest](body("LATEST", "")).isRight)
    assert(decode[ResetPlanRequest](body("SHIFT_BY", ""","shiftBy":-500""")).isRight)
  }

  test("anOutOfRangeResetParameterIsADecodeFailure") {
    // `FiniteDuration` and `Instant.toEpochMilli` throw once these values get large enough, and a throw
    // on the plan route is a 500. The bound is the decoder's, so the route never sees the value.
    def body(target: String, extra: String): String =
      s"""{"topic":"orders","partitions":[],"target":"$target"$extra}"""

    assert(decode[ResetPlanRequest](body("DURATION", ""","durationMs":9223372036854775807""")).isLeft)
    assert(decode[ResetPlanRequest](body("DURATION", ""","durationMs":0""")).isLeft)
    assert(decode[ResetPlanRequest](body("DURATION", ""","durationMs":-1""")).isLeft)
    assert(decode[ResetPlanRequest](body("DURATION", ""","durationMs":60000""")).isRight)
    assert(
      decode[ResetPlanRequest](
        body("TIMESTAMP", ""","timestamp":"+300000000-01-01T00:00:00Z"""")
      ).isLeft
    )
    assert(decode[ResetPlanRequest](body("TIMESTAMP", ""","timestamp":"1969-12-31T23:59:59Z"""")).isLeft)
    assert(decode[ResetPlanRequest](body("TIMESTAMP", ""","timestamp":"2026-09-04T09:00:00Z"""")).isRight)
  }

  test("anEmptyOffsetMapDoesNotSatisfyTheOffsetMode") {
    // `{}` is not an offset map, it is a request with no offsets in it wearing one.
    assert(decode[ResetPlanRequest]("""{"topic":"orders","target":"OFFSET","offsets":{}}""").isLeft)
  }

  test("pageCountIsNotOnTheWireAsASecondNumber") {
    // `PageInfo` writes `pageCount` but never reads it back, and it is derived from `totalItems`. One number
    // computed from another cannot disagree with it, which is the whole fix for the reference product's
    // "Page 1 of 3" over a filtered list of one page.
    val page = ConsumerSamples.page.asJson.hcursor.downField("page")
    assertEquals(page.get[Int]("pageCount"), Right(1))
    assertEquals(page.get[Option[Long]]("totalItems"), Right(Some(2L)))
    val lying =
      """{"items":[],"page":{"page":1,"pageSize":25,"totalItems":100,"pageCount":999,"nextPageToken":null}}"""
    val decoded = decode[PageDto[GroupSummaryDto]](lying)
    assertEquals(decoded.map(_.page.pageCount), Right(Some(4)))
  }

  given Arbitrary[GroupSummaryDto] = Arbitrary(summaries)
}
