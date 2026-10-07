package kui.message.contract

/** The committed sample message documents, as text.
  *
  * Same device, and same reason, as `kui.contracts.GoldenDocuments`: a browser has no filesystem, so a
  * Scala.js suite cannot read the files under `test/resources/golden`. Both platforms assert against these
  * constants, and the JVM-only `GoldenFilesSuite` asserts each constant is byte for byte the file committed
  * beside it, so the two copies cannot drift.
  *
  * These particular documents matter more than most: they are what travels inside an SSE stream, and MSG-043
  * reads the same bytes from the frontend's own parser. If a field name here changes and the browser is not
  * rebuilt, that suite is what says so.
  */
object GoldenDocuments {

  /** A record that decoded cleanly. `deserializeErrors` is present and empty — the field is required. */
  val message: String =
    """{
      |  "partition" : 3,
      |  "offset" : 41284,
      |  "timestamp" : "2026-09-03T10:11:12.000Z",
      |  "timestampType" : "CreateTime",
      |  "key" : {
      |    "text" : "A-1",
      |    "kind" : "string",
      |    "serde" : "String",
      |    "properties" : {
      |    }
      |  },
      |  "value" : {
      |    "text" : "{\"orderId\":\"A-1\"}",
      |    "kind" : "json",
      |    "serde" : "Avro",
      |    "properties" : {
      |      "schemaId" : "42"
      |    }
      |  },
      |  "headers" : {
      |    "traceparent" : "00-0af7651916cd43dd-b7ad6b7169203331-01"
      |  },
      |  "keySize" : 3,
      |  "valueSize" : 128,
      |  "headersSize" : 66,
      |  "deserializeErrors" : [
      |  ]
      |}""".stripMargin

  /** The same record, undecodable. The record is still delivered, by the fallback serde, and the failure
    * travels beside it — which is why one bad record cannot hide the good ones after it.
    */
  val messageWithDecodeError: String =
    """{
      |  "partition" : 0,
      |  "offset" : 7,
      |  "timestamp" : "2026-09-03T10:11:12.000Z",
      |  "timestampType" : "LogAppendTime",
      |  "key" : {
      |    "text" : "",
      |    "kind" : "null",
      |    "serde" : "String",
      |    "properties" : {
      |    }
      |  },
      |  "value" : {
      |    "text" : "7b 22 6f 72 64",
      |    "kind" : "binary",
      |    "serde" : "Hex",
      |    "properties" : {
      |    }
      |  },
      |  "headers" : {
      |  },
      |  "keySize" : 0,
      |  "valueSize" : 5,
      |  "headersSize" : 0,
      |  "deserializeErrors" : [
      |    {
      |      "target" : "value",
      |      "serde" : "Avro",
      |      "cause" : "Unknown magic byte 0x7b at position 0"
      |    }
      |  ]
      |}""".stripMargin

  /** The `phase` event's payload: a sentence, for a person. */
  val phaseEvent: String =
    """{
      |  "name" : "Seeking to offset 41284 on 3 partitions"
      |}""".stripMargin

  /** The `consumed` event's payload. `records` counts what was read from Kafka, not what was delivered; the
    * gap between it and the rows on screen is the filter doing its job.
    */
  val consumedEvent: String =
    """{
      |  "bytes" : 1048576,
      |  "records" : 4096,
      |  "elapsedMs" : 2310,
      |  "filterErrors" : 0,
      |  "budget" : {
      |    "recordsLeft" : 96,
      |    "bytesLeft" : 51380224,
      |    "millisLeft" : 57690
      |  }
      |}""".stripMargin

  /** A produce acknowledgement. */
  val produceResult: String =
    """{
      |  "records" : [
      |    {
      |      "partition" : 3,
      |      "offset" : 41285,
      |      "timestamp" : "2026-09-03T10:11:12.000Z"
      |    }
      |  ]
      |}""".stripMargin

  /** A resend whose source had been partly removed by retention: `read` is below what the ranges asked for,
    * and `written` says what actually landed.
    */
  val resendResult: String =
    """{
      |  "toTopic" : "orders-replay",
      |  "read" : 512,
      |  "written" : 512,
      |  "failures" : [],
      |  "rangeFailures" : []
      |}""".stripMargin

  /** A purge that one partition refused. Seven partitions were still purged, and saying so is the point. */
  val purgeResult: String =
    """{
      |  "purged" : [
      |    {
      |      "partition" : 0,
      |      "deletedBefore" : 1024
      |    }
      |  ],
      |  "failed" : [
      |    {
      |      "partition" : 1,
      |      "reason" : "the partition has no leader"
      |    }
      |  ]
      |}""".stripMargin

  /** A track query. `match.source` is spelled out; there is no default that would change its meaning. */
  val trackQuery: String =
    """{
      |  "topics" : [
      |    "orders",
      |    "payments",
      |    "shipments"
      |  ],
      |  "match" : {
      |    "source" : "header",
      |    "header" : "correlationId",
      |    "operator" : "equals",
      |    "value" : "A-1"
      |  },
      |  "from" : "2026-09-03T10:11:12.000Z",
      |  "to" : "2026-09-03T11:11:12.000Z",
      |  "limit" : 100
      |}""".stripMargin

  /** A serde suggestion, with the reason a user needs in order to act on it. */
  val serdeSuggestion: String =
    """{
      |  "name" : "Avro",
      |  "target" : "value",
      |  "preferred" : true,
      |  "reason" : "the schema registry has a subject named orders-value"
      |}""".stripMargin

  /** A filter that neither matched nor did not match, because evaluating it threw. */
  val filterTestResult: String =
    """{
      |  "matched" : false,
      |  "error" : "no such field: value.orderID"
      |}""".stripMargin

  /** Every constant above, by the file name it is committed under. */
  val all: List[(String, String)] = List(
    "message.json" -> message,
    "message-decode-error.json" -> messageWithDecodeError,
    "phase-event.json" -> phaseEvent,
    "consumed-event.json" -> consumedEvent,
    "produce-result.json" -> produceResult,
    "resend-result.json" -> resendResult,
    "purge-result.json" -> purgeResult,
    "track-query.json" -> trackQuery,
    "serde-suggestion.json" -> serdeSuggestion,
    "filter-test-result.json" -> filterTestResult
  )
}
