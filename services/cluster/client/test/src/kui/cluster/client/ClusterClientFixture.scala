package kui.cluster.client

import java.time.Instant

import cats.effect.IO
import cats.effect.kernel.{Ref, Resource}
import fs2.Stream
import io.circe.Json
import io.circe.syntax.*
import sttp.capabilities.fs2.Fs2Streams
import sttp.client4.StreamBackend
import sttp.client4.impl.cats.implicits.*
import sttp.client4.testing.{ResponseStub, StreamBackendStub, StubBody}
import sttp.model.{Header, StatusCode, Uri}

import kui.cluster.contract.dto.{ClusterChangeDto, ClusterProfileDto}
import kui.contracts.Section
import kui.contracts.cluster.{ClusterRowDto, ClusterSecurityDto}
import kui.http.sse.SseEvent
import kui.kernel.cluster.*
import kui.kernel.{ClusterId, Secret, UserName}
import kui.observability.Telemetry
import kui.security.PrincipalCodec
import kui.testkit.fakes.FakeStructuredLogger

/** A cluster service that answers from a script, and records what it was asked.
  *
  * It is an sttp stub rather than a running server on purpose. What is under test here is this client's
  * protocol — conditional fetch, subscription, fallback poll, backoff, cancellation — every part of which is
  * about *what happens after a delay*, and all of which is driven on a virtual clock. A real socket would
  * make each case take real seconds and would add a second thing that could fail.
  *
  * The bytes it answers with are the recorded ones, so the decoding path under test is the same one
  * `ProfileDecodingSuite` holds against the producing side's encoder.
  */
object ClusterClientFixture {

  val At: Instant = Instant.parse("2026-09-03T10:11:12Z")

  val BaseUri: Uri = Uri.unsafeParse("http://cluster-service:8080")

  val Prod: ClusterId = ClusterId.unsafe("prod-eu")
  val Staging: ClusterId = ClusterId.unsafe("staging")

  /** A profile whose every credential is a distinctive token, so a leak into a log line is visible. */
  def profile(id: ClusterId = Prod, version: Long = 1L): ClusterProfileDto =
    ClusterProfileDto(
      id = id,
      name = s"Cluster ${id.value}",
      version = version,
      readOnly = false,
      bootstrapServers = BootstrapServers.unsafe("broker-1.example.com:9093"),
      security = ClusterSecurity.Sasl(
        SaslProtocol.SaslSsl,
        SaslMechanism.ScramSha512("kui-service", Secret("kui-secret-canary")),
        None
      ),
      properties = ClientProperties.fromRaw(Map("ssl.truststore.password" -> "kui-secret-canary")),
      admin = AdminTuning.default,
      updatedAt = At
    )

  /** What the service is doing right now. Every field is something a real deployment does. */
  final case class Behaviour(
      profiles: Map[ClusterId, ClusterProfileDto],
      listFails: Boolean = false,
      profileFails: Boolean = false,
      /** `None` means the change stream cannot be opened at all. */
      events: Option[Stream[IO, Byte]] = Some(Stream.never[IO]),
      beforeProfileAnswer: IO[Unit] = IO.unit
  )

  object Behaviour {
    def of(profiles: ClusterProfileDto*): Behaviour =
      Behaviour(profiles.map(dto => dto.id -> dto).toMap)
  }

  /** One request the client made, as its method and path. */
  final case class Call(method: String, path: String, ifNoneMatch: Option[String])

  final class Fake(behaviour: Ref[IO, Behaviour], calls: Ref[IO, Vector[Call]]) {

    def set(next: Behaviour): IO[Unit] = behaviour.set(next)

    def update(f: Behaviour => Behaviour): IO[Unit] = behaviour.update(f)

    def recorded: IO[List[Call]] = calls.get.map(_.toList)

    def reset: IO[Unit] = calls.set(Vector.empty)

    def pathsFor(segment: String): IO[List[String]] =
      recorded.map(_.map(_.path).filter(_.contains(segment)))

    val backend: StreamBackend[IO, Fs2Streams[IO]] =
      StreamBackendStub[IO, Fs2Streams[IO]](summon[sttp.monad.MonadError[IO]]).whenAnyRequest
        .thenRespondF { request =>
          val path = request.uri.path.mkString("/", "/", "")
          val ifNoneMatch = request.headers.find(_.name.equalsIgnoreCase("If-None-Match")).map(_.value)

          calls.update(_ :+ Call(request.method.method, path, ifNoneMatch)) >>
            behaviour.get.flatMap(current =>
              (if path.endsWith("/profile") then current.beforeProfileAnswer else IO.unit) >>
                answer(path, ifNoneMatch, current)
            )
        }

    private def answer(
        path: String,
        ifNoneMatch: Option[String],
        current: Behaviour
    ): IO[sttp.client4.Response[StubBody]] =
      if path.endsWith("/stream") then
        current.events match {
          case None => IO.raiseError(new RuntimeException("connection refused"))
          case Some(body) => IO.pure(ResponseStub.adjust(body, StatusCode.Ok))
        }
      else if path.endsWith("/profile") then
        if current.profileFails then IO.raiseError(new RuntimeException("connection refused"))
        else {
          val id = ClusterId.unsafe(path.split('/').dropRight(1).last)
          current.profiles.get(id) match {
            case None => IO.pure(ResponseStub.adjust(notFound.noSpaces, StatusCode.NotFound))
            case Some(dto) =>
              val etag = ClusterProfileDto.etagOf(dto.version)
              if ifNoneMatch.contains(etag) then
                IO.pure(ResponseStub.adjust("", StatusCode.NotModified, List(Header("ETag", etag))))
              else
                IO.pure(
                  ResponseStub.adjust(dto.asJson.noSpaces, StatusCode.Ok, List(Header("ETag", etag)))
                )
          }
        }
      else if current.listFails then IO.raiseError(new RuntimeException("connection refused"))
      else IO.pure(ResponseStub.adjust(list(current).noSpaces, StatusCode.Ok))

    private def list(current: Behaviour): Json =
      Json.obj(
        "items" -> current.profiles.values.toList
          .sortBy(_.id.value)
          .map(dto =>
            ClusterRowDto(
              id = dto.id,
              name = dto.name,
              readOnly = dto.readOnly,
              bootstrapServers = dto.bootstrapServers.value,
              security = ClusterSecurityDto("SASL_SSL", Some("SCRAM-SHA-512"), false, false),
              summary = Section.NotConfigured
            )
          )
          .asJson,
        "generatedAt" -> At.asJson
      )

    private def notFound: Json =
      Json.obj(
        "code" -> "KUI-CLUSTER-NOT-FOUND".asJson,
        "message" -> "no such cluster".asJson,
        "details" -> Json.arr(),
        "correlationId" -> "0123456789abcdef".asJson,
        "timestamp" -> At.asJson,
        "retryable" -> false.asJson
      )
  }

  def fake(initial: Behaviour): IO[Fake] =
    for {
      behaviour <- Ref.of[IO, Behaviour](initial)
      calls <- Ref.of[IO, Vector[Call]](Vector.empty)
    } yield new Fake(behaviour, calls)

  /** The rendered bytes of a change event, as the cluster service writes them. */
  def changeBytes(id: ClusterId, version: Long, kind: String = ClusterChangeDto.Updated): String =
    SseEvent.render(
      SseEvent(
        ProfileSubscription.EventName,
        ClusterChangeDto(id, version, kind, At).asJson,
        Some(version.toString)
      )
    )

  /** A stream that emits the given frames and then stays open, which is what a healthy SSE connection looks
    * like: it does not end, it just goes quiet.
    */
  def openStreamOf(frames: String*): Stream[IO, Byte] =
    Stream.emits(frames.mkString.getBytes("UTF-8").toList).covary[IO] ++ Stream.never[IO]

  def client(
      fake: Fake,
      config: ClusterProfilesConfig = ClusterProfilesConfig.default
  ): Resource[IO, (ClusterProfiles[IO], FakeStructuredLogger[IO])] =
    for {
      logger <- Resource.eval(FakeStructuredLogger[IO])
      profiles <- HttpClusterProfiles.resource[IO](
        BaseUri,
        fake.backend,
        PrincipalCodec.inProcess[IO],
        UserName.unsafe("kui-topic-service"),
        config,
        Telemetry.noop[IO],
        logger
      )
    } yield (profiles, logger)
}
