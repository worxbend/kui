package kui.cluster.client

import scala.concurrent.duration.{DurationLong, FiniteDuration}

import cats.effect.kernel.{Async, Clock, Ref, Resource}
import cats.effect.std.Semaphore
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.Stream
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.otel4s.metrics.{Counter, UpDownCounter}
import org.typelevel.otel4s.{Attribute, Attributes}
import sttp.capabilities.fs2.Fs2Streams
import sttp.client4.*
import sttp.model.Uri
import sttp.tapir.DecodeResult
import sttp.tapir.client.sttp4.SttpClientInterpreter

import kui.cluster.contract.dto.{ClusterProfileDto, ProfileResult}
import kui.cluster.contract.{ClusterEndpoints, ProfileEndpoints}
import kui.contracts.ErrorEnvelope.given
import kui.contracts.{ErrorEnvelope, KuiEndpoint}
import kui.http.sse.SseWire
import kui.http.upstream.UpstreamClient
import kui.kernel.error.{FieldError, InfrastructureError, KuiError}
import kui.kernel.{ClusterId, UserName}
import kui.observability.{MetricNames, Telemetry}
import kui.security.{PrincipalClaims, PrincipalCodec, PrincipalKind, RequestDigest, SignedPrincipal}

/** The HTTP implementation of [[ClusterProfiles]]: the one consumer of the cluster service's profile contract
  * (ADR-046).
  *
  * ==The five behaviours, and why each is here==
  *
  *   1. **On start** it fetches the cluster list and each profile. A total failure is *not* fatal: the
  *      resource still becomes available, with no profiles and a recorded error, and the consuming service
  *      starts `Degraded`. A service that refused to start because the cluster service was briefly down would
  *      turn one outage into two, and would make container boot order a correctness requirement.
  *   2. **In steady state** it holds the change subscription. A version bump refetches exactly the named
  *      cluster, with `If-None-Match`, and a 304 fires no change at all — a consumer rebuilding its Kafka
  *      clients because a scrape re-serialised an unchanged profile is expensive and pointless.
  *   3. **As a fallback** it re-reads the list every `pollInterval`. This is what covers a stream that died
  *      without either end noticing, which is what a middlebox dropping an idle socket looks like: exactly
  *      like a quiet cluster.
  *   4. **On disconnect** it reconnects with capped exponential backoff and jitter, and logs one WARN per
  *      disconnect rather than one per attempt — a log line per attempt turns a two-hour outage into
  *      thousands of identical lines that bury the one that mattered.
  *   5. **On removal** — a cluster absent from a *successful* listing — it drops the profile and fires
  *      `Removed`. A **failed** listing never does, because "I cannot see the list" is not "the cluster was
  *      deleted", and treating the two alike tears down every Kafka client in the process during a blip.
  *
  * ==Why the fallback poll re-fetches conditionally rather than comparing versions from the list==
  *
  * The cluster list is the browser-facing row shape and carries no store version — deliberately, because a
  * store-internal counter has no business on a dashboard. So the poll learns the *set* of clusters from the
  * list and asks each profile whether it has changed, with the `ETag` it already holds. An unchanged profile
  * costs one 304 and no body, which is precisely what the conditional request exists for.
  */
object HttpClusterProfiles {

  /** How long a minted token is valid. Seconds, because it is spent immediately on the call it was minted
    * for; the only thing it has to survive is clock skew between two processes.
    */
  val TokenLifetimeSeconds: Long = 30L

  /** Builds the client and starts its two fibers.
    *
    * @param principals
    *   how this service signs its own internal calls. A codec rather than a pre-minted token, because ADR-020
    *   binds a token to one method and one path: a single `SignedPrincipal` could authorise exactly one
    *   request, so a client that held one would work for the first fetch and be refused for every fetch after
    *   it
    * @param identity
    *   the account this service calls as, recorded as the `sub` claim and hashed into the cluster service's
    *   log lines
    */
  def resource[F[_]: Async](
      baseUri: Uri,
      backend: StreamBackend[F, Fs2Streams[F]],
      principals: PrincipalCodec[F],
      identity: UserName,
      config: ClusterProfilesConfig,
      telemetry: Telemetry[F],
      logger: StructuredLogger[F]
  ): Resource[F, ClusterProfiles[F]] =
    for {
      metrics <- Resource.eval(ProfileMetrics.of[F](telemetry))
      state <- Resource.eval(Ref.of[F, State[F]](State.empty[F]))
      refreshGate <- Resource.eval(Semaphore[F](1L))
      client = new Impl[F](
        baseUri,
        backend,
        principals,
        identity,
        config,
        state,
        metrics,
        logger,
        refreshGate
      )
      // The first fetch runs inside acquisition rather than in a fiber, so that a service which comes up
      // against a healthy cluster service has its profiles before it serves its first request. Its
      // failure is recorded and swallowed: see behaviour 1 above.
      _ <- Resource.eval(client.refreshAll.attempt.void)
      // `background` is the cancellation path, and it is the reason it is spelled this way rather than
      // with `start`: releasing the resource cancels both fibers and waits for them, so after release
      // nothing is running and no request is in flight. `releasingTheClientLeavesNothingRunning`
      // asserts that rather than trusting it.
      _ <- client.pollLoop.compile.drain.background
      _ <- client.subscriptionLoop.background
    } yield client

  /** The two instruments ADR-046's operational question needs.
    *
    * "Is this service being told about changes, or is it polling because the stream is broken?" looks
    * identical in a latency graph and completely different here.
    */
  final private class ProfileMetrics[F[_]](
      val fetches: Counter[F, Long],
      val subscribed: UpDownCounter[F, Long]
  )

  private object ProfileMetrics {
    def of[F[_]: cats.Monad](telemetry: Telemetry[F]): F[ProfileMetrics[F]] =
      telemetry.meter("kui.cluster.client").flatMap { meter =>
        for {
          fetches <- meter
            .counter[Long](MetricNames.ClusterProfileFetch)
            .withDescription("Cluster profile fetches, by outcome")
            .create
          subscribed <- meter
            .upDownCounter[Long](MetricNames.ClusterProfileSubscribed)
            .withDescription("Whether the cluster change subscription is open")
            .create
        } yield new ProfileMetrics[F](fetches, subscribed)
      }
  }

  /** Everything the client knows, in one `Ref`.
    *
    * One cell rather than several, because the invariants are *between* the fields: a profile and the ETag
    * that describes it must move together, and a failure that clears `failingSince` must also set
    * `lastSuccessAt`. Two `Ref`s would let a reader see half an update.
    */
  final private case class State[F[_]](
      profiles: Map[ClusterId, ClusterProfile],
      etags: Map[ClusterId, String],
      handlers: Map[Long, ProfileChange => F[Unit]],
      nextHandler: Long,
      health: ProfileClientHealth,
      /** Whether the current disconnection has already been logged. See behaviour 4. */
      disconnectLogged: Boolean
  )

  private object State {
    def empty[F[_]]: State[F] =
      State[F](Map.empty, Map.empty, Map.empty, 0L, ProfileClientHealth.initial, disconnectLogged = false)
  }

  final private class Impl[F[_]: Async](
      baseUri: Uri,
      backend: StreamBackend[F, Fs2Streams[F]],
      principals: PrincipalCodec[F],
      identity: UserName,
      config: ClusterProfilesConfig,
      state: Ref[F, State[F]],
      metrics: ProfileMetrics[F],
      logger: StructuredLogger[F],
      refreshGate: Semaphore[F]
  ) extends ClusterProfiles[F] {

    private val interpreter: SttpClientInterpreter = SttpClientInterpreter()

    /** The path the change stream is served on.
      *
      * Composed from the contract's own segment constants rather than written as a literal: the endpoint
      * value itself lives in the service's `api` layer (it needs fs2 to describe a byte stream) and rule A11
      * keeps that layer out of this module. Composing from the shared constants is what stops the two sides
      * from spelling the path differently.
      */
    private val streamPath: List[String] =
      List("internal", "v1", ClusterEndpoints.ClustersSegment, ProfileEndpoints.StreamSegment)

    def all: F[Map[ClusterId, ClusterProfile]] = state.get.map(_.profiles)

    def get(id: ClusterId): F[Option[ClusterProfile]] = state.get.map(_.profiles.get(id))

    def health: F[ProfileClientHealth] = state.get.map(_.health)

    def onChange(handler: ProfileChange => F[Unit]): F[F[Unit]] =
      state
        .modify { current =>
          val id = current.nextHandler
          (
            current.copy(handlers = current.handlers.updated(id, handler), nextHandler = id + 1L),
            id
          )
        }
        .map(id => state.update(current => current.copy(handlers = current.handlers.removed(id))))

    // -------------------------------------------------------------------------------------------
    // Fetching
    // -------------------------------------------------------------------------------------------

    /** Re-read the whole set: which clusters exist, and whether each one's profile has moved. */
    def refreshAll: F[Unit] =
      refreshGate.permit.use(_ => refreshAllLocked)

    private def refreshAllLocked: F[Unit] =
      listClusters.flatMap {
        case Left(error) =>
          // No removals on a failed listing. This is behaviour 5, and it is the assertion that stops a
          // network blip from tearing down every Kafka client in the process.
          recordFailure(error) *> logger.debug(
            Map("error" -> error.message)
          )("the cluster list could not be read; keeping the last known set")
        case Right(ids) =>
          for {
            _ <- recordSuccess
            _ <- ids.toList.traverse_(refreshOne)
            _ <- forgetMissing(ids)
          } yield ()
      }

    /** Re-read one cluster's profile, conditionally. A 304 changes nothing and fires nothing. */
    def refreshOne(id: ClusterId): F[Unit] =
      state.get.map(_.etags.get(id)).flatMap(refreshProfile(id, _))

    private def refreshProfile(id: ClusterId, etag: Option[String]): F[Unit] =
      fetchProfile(id, etag).flatMap {
        case Left(error) =>
          countFetch("failed") *> recordFailure(error) *> logger.debug(
            Map("cluster" -> id.value, "error" -> error.message)
          )("a cluster profile could not be read; keeping the last known one")

        case Right(ProfileResult.NotModified(_)) =>
          countFetch("not_modified") *> recordSuccess

        case Right(ProfileResult.Current(etag, dto)) =>
          countFetch("current") *> recordSuccess *> applyProfile(etag, dto)
      }

    /** Installs a freshly fetched profile and fires `Updated` when its effective settings or version moved.
      *
      * A recreated profile can reuse the same store version. Compare the decoded settings as well, not the
      * wire document (whose timestamp and field order are irrelevant), so this also detects that reset.
      */
    private def applyProfile(etag: String, dto: ClusterProfileDto): F[Unit] = {
      val profile = ClusterProfile(
        id = dto.id,
        name = dto.name,
        readOnly = dto.readOnly,
        connection = ClusterProfileDto.connectionOf(dto),
        version = dto.version
      )

      state
        .modify { current =>
          val previous = current.profiles.get(profile.id)
          val updated = current.copy(
            profiles = current.profiles.updated(profile.id, profile),
            etags = current.etags.updated(profile.id, etag)
          )
          (updated, previous)
        }
        .flatMap {
          case Some(previous) if previous == profile => Async[F].unit
          case previous =>
            logger.info(
              Map(
                "cluster" -> profile.id.value,
                "from" -> previous.fold("none")(_.version.toString),
                "to" -> profile.version.toString
              )
            )("a cluster profile changed; dependent clients will be rebuilt") *>
              // Never the profile itself. Its credentials are `Secret` and would redact themselves, but
              // a wall of redacted text is not a log line anybody reads.
              fire(ProfileChange.Updated(profile.id, previous.map(_.version), profile.version))
        }
    }

    /** Drops every cluster the (successful) listing did not mention. */
    private def forgetMissing(present: Set[ClusterId]): F[Unit] =
      state
        .modify { current =>
          val gone = current.profiles.keySet.diff(present)
          val updated = current.copy(
            profiles = current.profiles.removedAll(gone),
            etags = current.etags.removedAll(gone)
          )
          (updated, gone.toList.sortBy(_.value))
        }
        .flatMap(_.traverse_(id => fire(ProfileChange.Removed(id))))

    /** A removal event is an invalidation, not proof of current absence. Events can arrive after a newer
      * fetch or a recreation, whose store version can restart at one.
      */
    private def revalidateRemoval(id: ClusterId): F[Unit] =
      listClusters.flatMap {
        case Left(error) =>
          recordFailure(error) *> logger.debug(
            Map("cluster" -> id.value, "error" -> error.message)
          )("a cluster removal could not be verified; keeping the last known profile")
        // A version-only ETag may belong to an earlier incarnation. An unconditional fetch is necessary
        // even when the version in the delayed event is older than our cache.
        case Right(ids) if ids.contains(id) => refreshProfile(id, None)
        case Right(_) => recordSuccess *> forget(id)
      }

    /** Drops one cluster after the current listing confirms its absence. */
    private def forget(id: ClusterId): F[Unit] =
      state
        .modify(current =>
          (
            current.copy(profiles = current.profiles.removed(id), etags = current.etags.removed(id)),
            current.profiles.contains(id)
          )
        )
        .flatMap(existed => Async[F].whenA(existed)(fire(ProfileChange.Removed(id))))

    // -------------------------------------------------------------------------------------------
    // The two fibers
    // -------------------------------------------------------------------------------------------

    /** The fallback poll. `awakeEvery` and not `fixedRate`: a poll that fell behind must not then run a burst
      * of catch-up polls against a service that is already struggling.
      */
    def pollLoop: Stream[F, Unit] =
      Stream.awakeEvery[F](config.pollInterval).evalMap(_ => refreshAll.attempt.void)

    /** The change subscription, with its reconnect policy. */
    def subscriptionLoop: F[Unit] = connect(attempt = 1)

    /** One connection attempt, then the next.
      *
      * `opened` records whether this attempt got as far as a live stream. It decides two things: whether the
      * backoff resets — a connection that ran for an hour and then dropped should be retried immediately, not
      * after the thirty seconds the *previous* outage had backed off to — and whether the up-down counter has
      * a `+1` to undo.
      */
    private def connect(attempt: Int): F[Unit] =
      Ref.of[F, Boolean](false).flatMap { opened =>
        events(opened).evalMap(handle).compile.drain.attempt.flatMap { outcome =>
          for {
            wasOpen <- opened.get
            _ <- Async[F].whenA(wasOpen)(subscribed(false))
            _ <- noteDisconnect(outcome.fold(failure => failure, _ => EndOfStream))
            next = if wasOpen then 1 else attempt
            delay <- jittered(config.backoffFor(next))
            _ <- Async[F].sleep(delay)
            _ <- connect(next + 1)
          } yield ()
        }
      }

    /** One WARN per disconnect, not one per attempt (behaviour 4). */
    private def noteDisconnect(cause: Throwable): F[Unit] =
      state
        .modify(current => (current.copy(disconnectLogged = true), current.disconnectLogged))
        .flatMap(alreadyLogged =>
          Async[F].unlessA(alreadyLogged)(
            logger.warn(Map("reason" -> Option(cause.getMessage).getOrElse(cause.toString)))(
              "the cluster change stream closed; falling back to polling until it reconnects"
            )
          )
        )

    private def subscribed(open: Boolean): F[Unit] =
      state.update(current =>
        current.copy(
          health = current.health.copy(subscribed = open),
          disconnectLogged = if open then false else current.disconnectLogged
        )
      ) *> metrics.subscribed.add(if open then 1L else -1L)

    private def handle(event: kui.http.sse.SseEvent): F[Unit] =
      refreshGate.permit.use(_ => handleLocked(event))

    private def handleLocked(event: kui.http.sse.SseEvent): F[Unit] =
      ProfileSubscription.instructionFor(event) match {
        case ProfileSubscription.Instruction.Refetch(change) => refreshOne(change.id)
        case ProfileSubscription.Instruction.Forget(change) => revalidateRemoval(change.id)
        case ProfileSubscription.Instruction.Ignored(reason) =>
          logger.debug(Map("reason" -> reason))("ignoring a cluster stream event")
      }

    // -------------------------------------------------------------------------------------------
    // Transport
    // -------------------------------------------------------------------------------------------

    private def listClusters: F[Either[KuiError, Set[ClusterId]]] = {
      val request = (token: SignedPrincipal) =>
        interpreter.toSecureRequest(ClusterEndpoints.listClusters, Some(baseUri)).apply(token).apply(())

      signedSend(request).map(_.map(_.items.map(_.id).toSet))
    }

    /** Fetches one profile, conditionally.
      *
      * ==Why this one request is built by hand==
      *
      * Every other call here goes through Tapir's client interpreter over the endpoint value, which is the
      * right default: the client then cannot disagree with the contract about a path or a body. This one
      * cannot. The endpoint models its two answers as a `oneOf` of 200-with-a-body and 304-without-one, and
      * Tapir's generated client cannot decode the 304 half: it attempts a body decode against an empty
      * payload and fails with "Cannot decode from: ". A client that treated every 304 as a transport failure
      * would report a permanently `Degraded` service that is in fact working perfectly — which is precisely
      * the seam this milestone exists to catch, found by driving the two sides against each other rather than
      * by reading either one.
      *
      * So the request is assembled here and the answer is interpreted here — but the *decoder* is still the
      * contract's own, and the ETag is still built by the contract's own `etagOf`, so the two sides cannot
      * disagree about the shape or about the tag. `ProfileDecodingSuite` holds that decoder against recorded
      * bytes from the producing side.
      */
    private def fetchProfile(
        id: ClusterId,
        etag: Option[String]
    ): F[Either[KuiError, ProfileResult]] = {
      val uri = baseUri.addPath(profilePath(id))

      sign("GET", pathOf(uri))
        .flatMap { token =>
          basicRequest
            .get(uri)
            .header(KuiEndpoint.PrincipalHeader, token.value)
            .headers(etag.map(sttp.model.Header(ProfileEndpoints.IfNoneMatchHeader, _)).toList*)
            .response(asStringAlways)
            .readTimeout(config.requestTimeout)
            .send(backend)
            .attempt
        }
        .map {
          case Left(failure) => Left(unreachable(failure))
          case Right(response) => profileAnswer(response)
        }
    }

    private def profileAnswer(response: Response[String]): Either[KuiError, ProfileResult] = {
      val tag = response.header(ProfileEndpoints.ETagHeader)

      response.code.code match {
        case 304 =>
          // A 304 with no ETag is a proxy that stripped it. The caller's copy is still current — that is
          // what the status means — so the tag it already holds stands.
          Right(ProfileResult.NotModified(tag.getOrElse("")))
        case 200 =>
          io.circe.parser
            .parse(response.body)
            .flatMap(_.as[ClusterProfileDto])
            .fold(
              failure =>
                Left(
                  InfrastructureError
                    .Unreachable(ServiceName, s"the profile could not be decoded: ${failure.getMessage}")
                ),
              // The tag the service sent, and only as a fallback the one derived from the body's own
              // version — the two always agree, and `theBodyVersionAndTheETagAgree` on the producing side
              // is what keeps them agreeing.
              dto => Right(ProfileResult.Current(tag.getOrElse(ClusterProfileDto.etagOf(dto.version)), dto))
            )
        case status =>
          Left(
            io.circe.parser
              .parse(response.body)
              .flatMap(_.as[ErrorEnvelope])
              .toOption
              .flatMap(envelope =>
                ErrorEnvelope
                  .codeOf(envelope)
                  .map(code =>
                    KuiError.remote(
                      code,
                      envelope.message,
                      envelope.details.map(detail => FieldError(detail.field, detail.restrictions))
                    )
                  )
              )
              .getOrElse(InfrastructureError.Upstream(ServiceName, status))
          )
      }
    }

    private def profilePath(id: ClusterId): List[String] =
      List("internal", "v1", ClusterEndpoints.ClustersSegment, id.value, ProfileEndpoints.ProfileSegment)

    /** Signs, sends and decodes. Interpreting twice is the only honest way to sign a request whose digest
      * covers its own method and path: the request has to exist before it can be signed, and the token is
      * part of the request. The cost is an in-memory object, not a round trip.
      */
    private def signedSend[O](
        build: SignedPrincipal => Request[DecodeResult[Either[ErrorEnvelope, O]]]
    ): F[Either[KuiError, O]] = {
      val shape = build(Placeholder)

      sign(shape.method.method, pathOf(shape.uri))
        .map(build)
        .flatMap(request =>
          request
            .readTimeout(config.requestTimeout)
            .send(backend)
            .attempt
            .map {
              case Left(failure) => Left(unreachable(failure))
              case Right(response) => decoded(response.code.code, response.body)
            }
        )
    }

    private def decoded[O](
        status: Int,
        body: DecodeResult[Either[ErrorEnvelope, O]]
    ): Either[KuiError, O] =
      body match {
        case DecodeResult.Value(Right(output)) => Right(output)
        // The service's own error code wins over a verdict derived from the status. A 404 here means
        // "there is no such cluster", which a consumer answers by releasing its snapshot — not
        // "the cluster service is broken", which would dim the whole feature (ADR-039 §6).
        case DecodeResult.Value(Left(envelope)) =>
          Left(
            ErrorEnvelope
              .codeOf(envelope)
              .fold[KuiError](InfrastructureError.Upstream(ServiceName, status))(code =>
                KuiError.remote(
                  code,
                  envelope.message,
                  envelope.details.map(detail => FieldError(detail.field, detail.restrictions))
                )
              )
          )
        case failure: DecodeResult.Failure =>
          // A profile this version cannot decode leaves that cluster at the profile it already has, and
          // says so once. One malformed cluster must not blind a service to the other thirty-nine.
          Left(InfrastructureError.Unreachable(ServiceName, s"the response could not be decoded: $failure"))
      }

    /** The change stream, as parsed events. Opened once per connection attempt.
      *
      * `opened` is set — and the client counted as subscribed — only once the far side has answered, not when
      * the request was built. A client that called itself subscribed while it was failing to connect would
      * report the healthy state during exactly the outage the flag exists to reveal.
      */
    private def events(opened: Ref[F, Boolean]): Stream[F, kui.http.sse.SseEvent] = {
      val uri = baseUri.addPath(streamPath)

      Stream
        .eval(sign("GET", pathOf(uri)))
        .flatMap { token =>
          Stream
            .eval(
              basicRequest
                .get(uri)
                .header(KuiEndpoint.PrincipalHeader, token.value)
                .response(asStreamAlwaysUnsafe(Fs2Streams[F]))
                .send(backend)
            )
            .flatMap { response =>
              Stream.eval(opened.set(true) *> subscribed(true)) >> response.body.through(SseWire.parse)
            }
        }
    }

    private def sign(method: String, path: String): F[SignedPrincipal] =
      Clock[F].realTimeInstant.flatMap(now =>
        principals.sign(
          PrincipalClaims(
            subject = identity,
            roles = Set.empty,
            // A machine, not a person: this is one KUI process calling another, and an audit line that
            // named a human for it would be a lie.
            kind = PrincipalKind.Bearer,
            sessionRef = None,
            issuedAt = now,
            expiresAt = now.plusSeconds(TokenLifetimeSeconds),
            audience = ProfileEndpoints.Audience,
            requestDigest = RequestDigest.ofRequestLine(method, path)
          )
        )
      )

    /** The path exactly as the far side sees it when it computes the digest. */
    private def pathOf(uri: Uri): String = uri.path.mkString("/", "/", "")

    // -------------------------------------------------------------------------------------------
    // Health, handlers and jitter
    // -------------------------------------------------------------------------------------------

    private def recordSuccess: F[Unit] =
      Clock[F].realTimeInstant.flatMap(now =>
        state.update(current =>
          current.copy(health =
            current.health.copy(lastSuccessAt = Some(now), lastError = None, failingSince = None)
          )
        )
      )

    /** `failingSince` is sticky: it is set on the first failure of a run and left alone by every failure
      * after it, because the question it answers is "how long has this been broken".
      */
    private def recordFailure(error: KuiError): F[Unit] =
      Clock[F].realTimeInstant.flatMap(now =>
        state.update(current =>
          current.copy(health =
            current.health.copy(
              lastError = Some(error),
              failingSince = current.health.failingSince.orElse(Some(now))
            )
          )
        )
      )

    /** A consumer's bad callback degrades that consumer, not this client and not the other handlers. */
    private def fire(change: ProfileChange): F[Unit] =
      state.get
        .map(_.handlers.values.toList)
        .flatMap(_.traverse_ { handler =>
          handler(change).handleErrorWith(failure =>
            logger.warn(failure)("a cluster profile change handler failed; the subscription is unaffected")
          )
        })

    /** Up to a fifth of the delay, added, derived from the monotonic clock.
      *
      * Jitter matters because every consuming service reconnects to the same cluster service, and a restart
      * of it would otherwise be met by all of them at the same instant, for ever. Derived from the clock
      * rather than from a random source so that a suite on a virtual clock still gets a deterministic answer
      * while two real processes still get different ones.
      */
    private def jittered(base: FiniteDuration): F[FiniteDuration] =
      Clock[F].monotonic.map { now =>
        val spread = base.toMillis / 5L
        if spread <= 0L then base else (base.toMillis + math.floorMod(now.toNanos, spread)).millis
      }

    private def countFetch(outcome: String): F[Unit] =
      metrics.fetches.inc(Attributes(Attribute(MetricNames.Attr.Outcome, outcome)))

    private def unreachable(failure: Throwable): KuiError =
      InfrastructureError.Unreachable(ServiceName, UpstreamClient.safeFailureCause(failure))
  }

  /** What this client calls the far side in an error message and on a metric. */
  private val ServiceName: String = "cluster"

  /** Ends a subscription that closed cleanly — the far side finished the stream rather than failing it. It is
    * still a disconnect from this side's point of view, and it still means a reconnect.
    */
  private val EndOfStream: Throwable = new RuntimeException("the stream ended")

  /** The token used only to shape the request that is about to be signed. It never leaves the process. */
  private val Placeholder: SignedPrincipal = SignedPrincipal.unsafe("unsigned")
}
