package kui.http.upstream

import java.nio.charset.StandardCharsets

import munit.FunSuite
import sttp.client4.*
import sttp.client4.testing.{ResponseStub, SyncBackendStub}
import sttp.model.{Header, StatusCode}

final class BoundedResponseSuite extends FunSuite {

  test("a response exactly at the byte limit succeeds") {
    val body = "three bytes: €"
    val limit = body.getBytes(StandardCharsets.UTF_8).length.toLong

    assertEquals(send(body, limit, None), body)
  }

  test("one byte over the encoded length fails") {
    val body = "three bytes: €"
    val limit = body.getBytes(StandardCharsets.UTF_8).length.toLong - 1

    val failure = intercept[BoundedResponse.LimitExceeded] {
      send(body, limit, None)
    }

    assertEquals(failure.limitBytes, limit)
  }

  test("the response handler uses Content-Length as an early rejection hint") {
    val failure = intercept[BoundedResponse.LimitExceeded] {
      send("short", 5L, Some(6L))
    }

    assertEquals(failure.limitBytes, 5L)
  }

  test("Content-Length is only an early hint and cannot weaken the byte cap") {
    val failure = intercept[BoundedResponse.LimitExceeded] {
      send("123456", 5L, Some(4L))
    }

    assertEquals(failure.limitBytes, 5L)
  }

  private def send(body: String, maxBytes: Long, contentLength: Option[Long]): String = {
    val headers = contentLength.toList.map(Header.contentLength)
    val backend = SyncBackendStub.whenAnyRequest.thenRespond(
      ResponseStub.adjust(body, StatusCode.Ok).copy(headers = headers)
    )

    basicRequest
      .get(uri"http://upstream.invalid")
      .response(BoundedResponse.asString(maxBytes))
      .send(backend)
      .body
  }
}
