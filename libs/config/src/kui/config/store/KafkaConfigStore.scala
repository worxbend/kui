package kui.config.store

import scala.concurrent.duration.FiniteDuration

import cats.Parallel
import cats.data.NonEmptySet
import cats.effect.std.Supervisor
import cats.effect.syntax.all.*
import cats.effect.{Async, Clock, Ref, Resource}
import cats.syntax.all.*
import fs2.Stream
import fs2.concurrent.Topic
import fs2.io.file.Files
import fs2.kafka.{KafkaConsumer, KafkaProducer}
import io.circe.Json
import io.circe.parser.parse
import io.circe.syntax.*
import org.apache.kafka.common.TopicPartition
import org.typelevel.log4cats.{Logger, LoggerFactory}

import kui.config.{StoreConfig, StoreKafkaConfig}
import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}

/** The Kafka-backed `ConfigStore`: replay the whole log, then follow its tail forever.
  *
  * Acquiring this `Resource` **is** the bootstrap step of ADR-042 §1. A caller holding the value knows which
  * clusters exist, because nothing is handed out until replay has reached the end of the log. That ordering
  * is what lets a service gate its readiness on this resource and never report itself Ready with an empty
  * registry — "you have no clusters" and "your clusters were deleted" look identical to a user.
  *
  * The other thing this file is shaped by is the worst startup failure a service can have, which is a hang.
  * Replay takes the end offset once, before it consumes anything, so it is never chasing a moving target; and
  * the whole of it runs under a timeout that fails with `KUI-STORE-REPLAY-TIMEOUT` naming how far it got,
  * where it was going, and how long it took.
  */
object KafkaConfigStore {

  /** One record as it comes off the log: the key, the value (absent for a physical tombstone) and the offset
    * it sat at.
    */
  final case class LogRecord(key: String, value: Option[String], offset: Long)

  /** The log, as the replay and the follower see it.
    *
    * It exists so that the two things this file has to get right — that replay terminates, and that
    * cancelling it releases everything — can be tested without a broker. Driving them through a real consumer
    * would mean the only tests of the milestone's highest-risk loop needed a container, ran for seconds, and
    * could not express "the log never produces the record replay is waiting for".
    */
  private[store] trait StoreLog[F[_]] {

    /** Where the log ends, read once before anything is consumed. */
    def endOffset: F[Long]

    /** Every record from the current position onward. Does not complete while the log is open. */
    def records: Stream[F, LogRecord]
  }

  /** How many changes a subscriber may fall behind before it starts losing the oldest ones.
    *
    * A slow subscriber must never back-pressure the fold, because the fold is what every read depends on. 256
    * is more than any burst of metadata edits a person can produce and small enough to be free. A subscriber
    * that overflows it has lost changes and has to re-read from the store, so every consumer of `changes` is
    * written to be re-readable.
    */
  val ChangeBufferSize: Int = 256

  def resource[F[_]: {Async, Parallel, Files, LoggerFactory}](
      config: StoreConfig,
      kafka: StoreKafkaConfig,
      crypto: FieldCrypto[F],
      clientId: String
  ): Resource[F, ConfigStore[F]] = {
    val logger = LoggerFactory[F].getLogger
    val topic = config.configTopic
    val partition = new TopicPartition(topic, 0)

    for {
      state <- Resource.eval(Ref.of[F, StoreState](StoreState.empty))
      changes <- Resource.eval(Topic[F, StoreChange])
      waiter <- Resource.eval(WriteWaiter.create[F])
      health <- Resource.eval(
        StoreHealthRef.of[F](StoreHealth.Healthy(-1L, java.time.Instant.EPOCH, Nil))
      )
      producer <- StoreClients.producer[F](kafka, clientId)
      consumer <- StoreClients.consumer[F](kafka, clientId)
      log = kafkaLog(consumer, topic, partition)
      endOffset <- Resource.eval(prepare(log, topic, config.replayTimeout, logger))
      _ <- Resource.eval(replay(log, state, changes, crypto, topic, endOffset, config.replayTimeout, logger))
      _ <- Resource.eval(waiter.advance(endOffset - 1L))
      // The follower is started under a supervisor owned by this resource, so releasing the resource
      // cancels the fiber *before* the consumer resource above it is finalized. A consumer closed
      // underneath a running poll is how an orderly shutdown turns into a stack trace.
      _ <- Resource.eval(health.markHealthy(endOffset - 1L))
      // The first attempt keeps the consumer replay positioned at the tip. A later attempt opens a new
      // one: fs2-kafka does not survive a failed poll, and a retry on the dead actor would fail at once
      // for the rest of the process.
      restarted <- Resource.eval(Ref.of[F, Boolean](false))
      supervisor <- Supervisor[F]
      _ <- Resource.eval(
        supervisor.supervise(
          followForever(
            openTail(consumer, kafka, clientId, topic, partition, state, restarted),
            state,
            changes,
            crypto,
            waiter,
            health,
            kafka.bootstrapServers.value,
            logger
          )
        )
      )
    } yield writable(
      state,
      changes,
      waiter,
      health,
      kafkaSink(producer),
      crypto,
      topic,
      config.writeTimeout,
      logger
    )
  }

  /** A follower attempt's log. The first one is the replay consumer; each retry builds a new consumer sought
    * to the offset this replica has already folded.
    */
  private def openTail[F[_]: {Async, Files}](
      consumer: KafkaConsumer[F, String, Option[String]],
      kafka: StoreKafkaConfig,
      clientId: String,
      topic: String,
      partition: TopicPartition,
      state: Ref[F, StoreState],
      restarted: Ref[F, Boolean]
  ): Resource[F, StoreLog[F]] =
    Resource.eval(restarted.getAndSet(true)).flatMap {
      case false => Resource.pure(fromPosition(consumer, partition))
      case true =>
        StoreClients
          .consumer[F](kafka, s"$clientId-tail")
          .evalTap { fresh =>
            fresh.assign(topic, NonEmptySet.one(partition.partition)) *>
              state.get.flatMap(current => fresh.seek(partition, current.lastAppliedOffset + 1L))
          }
          .map(fresh => fromPosition(fresh, partition))
    }

  /** Assign the single partition, rewind, and find out where the log ends.
    *
    * **Assign, never subscribe**, and this is not a style choice. A consumer group would make replay wait for
    * a rebalance, would let a second replica take the partition away from the first, and would mean each
    * replica saw only part of the log — the exact opposite of "every replica replays the whole thing".
    */
  private[store] def kafkaLog[F[_]: Async](
      consumer: KafkaConsumer[F, String, Option[String]],
      topic: String,
      partition: TopicPartition
  ): StoreLog[F] =
    new StoreLog[F] {
      def endOffset: F[Long] =
        for {
          _ <- consumer.assign(topic, NonEmptySet.one(partition.partition))
          _ <- consumer.seekToBeginning
          offsets <- consumer.endOffsets(Set(partition))
        } yield offsets.getOrElse(partition, 0L)

      def records: Stream[F, LogRecord] = recordStream(consumer)
    }

  /** A consumer that is already assigned, read from its current position.
    *
    * `endOffset` must not seek. Seeking here would rewind a consumer replay has already walked to the tip,
    * and the follower would fold the log a second time.
    */
  private def fromPosition[F[_]: cats.Functor](
      consumer: KafkaConsumer[F, String, Option[String]],
      partition: TopicPartition
  ): StoreLog[F] =
    new StoreLog[F] {
      def endOffset: F[Long] =
        consumer.endOffsets(Set(partition)).map(_.getOrElse(partition, 0L))

      def records: Stream[F, LogRecord] = recordStream(consumer)
    }

  private def recordStream[F[_]](
      consumer: KafkaConsumer[F, String, Option[String]]
  ): Stream[F, LogRecord] =
    consumer.records.map(committable =>
      LogRecord(committable.record.key, committable.record.value, committable.record.offset)
    )

  private def prepare[F[_]: Async](
      log: StoreLog[F],
      topic: String,
      timeout: FiniteDuration,
      logger: Logger[F]
  ): F[Long] =
    for {
      // Taken once, before a single record is consumed. Replay that chased a moving end offset would
      // never terminate on a busy topic, which is the other way a startup hangs.
      end <- log.endOffset
      _ <- logger.info(s"store replay started: topic=$topic endOffset=$end timeoutMs=${timeout.toMillis}")
    } yield end

  /** Fold the log into the state until the end offset is reached, or fail by name.
    *
    * An empty log completes immediately. Waiting for a record that will never come is precisely the hang this
    * whole design is arranged to avoid.
    */
  private[store] def replay[F[_]: Async](
      log: StoreLog[F],
      state: Ref[F, StoreState],
      changes: Topic[F, StoreChange],
      crypto: FieldCrypto[F],
      topic: String,
      endOffset: Long,
      timeout: FiniteDuration,
      logger: Logger[F]
  ): F[Unit] =
    if endOffset <= 0L then
      logger.info(s"store replay complete: topic=$topic records=0 endOffset=0 (empty log)")
    else
      for {
        started <- Clock[F].monotonic
        outcome <- log.records
          .through(applyEach(state, changes, crypto, None, logger, _ => Async[F].unit))
          .takeThrough(offset => offset < endOffset - 1L)
          .compile
          .drain
          .timeout(timeout)
          .attempt
        elapsed <- Clock[F].monotonic.map(_ - started)
        current <- state.get
        _ <- outcome match {
          case Right(_) =>
            logger.info(
              s"store replay complete: topic=$topic reached=${current.lastAppliedOffset} " +
                s"endOffset=$endOffset unreadable=${current.unreadable.size} " +
                s"ignoredGaps=${current.ignoredGaps} elapsedMs=${elapsed.toMillis}"
            )
          case Left(_: java.util.concurrent.TimeoutException) =>
            val error =
              StoreError.ReplayTimeout(topic, current.lastAppliedOffset, endOffset, elapsed.toMillis)
            logger.error(s"${error.code.wire}: ${error.message}") *>
              Async[F].raiseError[Unit](new StoreFailure(error))
          case Left(other) => Async[F].raiseError[Unit](other)
        }
      } yield ()

  /** The tail follower: the same fold, forever, from where replay stopped.
    *
    * It is the only writer of the state `Ref`. A write (STORE-007) produces to the topic and then waits for
    * its own record to come back around through this loop, which is what makes read-your-writes a real read
    * of the log rather than a hopeful local mutation.
    */
  private[store] def follow[F[_]: Async](
      log: StoreLog[F],
      state: Ref[F, StoreState],
      changes: Topic[F, StoreChange],
      crypto: FieldCrypto[F],
      waiter: Option[WriteWaiter[F]],
      logger: Logger[F],
      onApplied: Long => F[Unit]
  ): F[Unit] =
    log.records.through(applyEach(state, changes, crypto, waiter, logger, onApplied)).compile.drain

  /** The follower, restarted for ever with bounded backoff.
    *
    * It never gives up. An operator restarting a broker for twenty minutes must not have to restart KUI as
    * well, and a store that gave up would leave reads working and writes failing with no way back except a
    * deployment. Between attempts the store is `Degraded`: reads keep serving the last state, writes are
    * rejected rather than queued — a queued write applied minutes later lands on top of somebody else's edit
    * — and every parked writer is freed at once instead of each waiting out its own timeout.
    *
    * A failed attempt is not the end of the outage. The next one opens its own log (the Kafka consumer does
    * not survive a failed poll) and becomes `Healthy` again once it is caught up or has folded a record.
    * Until that happens writes stay rejected, and the waiter's failure from the outage stays sticky so a
    * writer is not parked against a log nobody is reading.
    *
    * Every step is cancellable, including the sleep: a shutdown must not have to wait out a thirty-second
    * backoff.
    */
  private[store] def followForever[F[_]: Async](
      open: Resource[F, StoreLog[F]],
      state: Ref[F, StoreState],
      changes: Topic[F, StoreChange],
      crypto: FieldCrypto[F],
      waiter: WriteWaiter[F],
      health: StoreHealthRef[F],
      bootstrapServers: String,
      logger: Logger[F]
  ): F[Unit] = {
    def attempt(number: Int): F[Unit] =
      Ref.of[F, Boolean](false).flatMap { resumed =>
        // One transition per attempt. Marking healthy on every record would move `since` forward for as
        // long as the follower stayed degraded, and `since` answers "how long has this been fine".
        def resume(offset: Long): F[Unit] =
          resumed.get.flatMap {
            case true => Async[F].unit
            case false =>
              resumed.set(true) *> health.markHealthy(offset) *> waiter.recover
          }

        open
          .use { log =>
            (log.endOffset, state.get).flatMapN { (end, current) =>
              // A quiet topic produces nothing to fold. Caught up means the outage is over anyway.
              if current.lastAppliedOffset >= end - 1L then resume(current.lastAppliedOffset)
              else Async[F].unit
            } *> follow(log, state, changes, crypto, Some(waiter), logger, resume)
          }
          .attempt
          .flatMap {
            case Right(_) =>
              // The log ended, which for a live topic means the client was closed under us.
              logger.info("store tail follower completed; the store is closing")
            case Left(error) =>
              val reason = StoreHealthRef.classify(error)
              val pause = StoreRetryPolicy.Default.delay(number, scala.util.Random.nextDouble())
              health.markDegraded(reason) *>
                waiter.fail(StoreError.Unreachable(bootstrapServers, reason)) *>
                logger.warn(
                  s"store tail follower failed: reason=$reason attempt=${number + 1} delayMs=${pause.toMillis}; " +
                    "reads continue from the last replayed state and writes are rejected until it recovers"
                ) *>
                Async[F].sleep(pause) *>
                attempt(number + 1)
          }
      }

    attempt(0)
  }

  /** Decodes, decrypts and folds one record, publishing what changed. Yields each record's offset.
    *
    * Every failure here costs one key and no more. One record that will not decode, or that was encrypted
    * under a key that has since been rotated away, must not stop KUI from serving the other ninety-nine
    * clusters — that is exactly the "keep serving from last known state" behaviour ADR-042 §8 is about.
    */
  private def applyEach[F[_]: Async](
      state: Ref[F, StoreState],
      changes: Topic[F, StoreChange],
      crypto: FieldCrypto[F],
      waiter: Option[WriteWaiter[F]],
      logger: Logger[F],
      onApplied: Long => F[Unit]
  ): fs2.Pipe[F, LogRecord, Long] =
    _.evalMap { record =>
      val offset = record.offset
      decode(record.key, record.value, crypto).flatMap { decoded =>
        // Uncancelable: the state update and the change publication are one observable step. A
        // cancellation between them would leave a subscriber never told about a record that reads can
        // already see.
        val folded = Async[F].uncancelable(_ =>
          decoded match {
            case Decoded.Readable(store) =>
              state
                .modify(_.apply(store, offset))
                .flatMap(outcome => report(outcome, offset, changes, logger))
            case Decoded.Erased(key) =>
              state.modify(_.erase(key, offset)).flatMap(outcome => report(outcome, offset, changes, logger))
            case Decoded.Unreadable(key, reason, knownVersion) =>
              state
                .modify(_.markUnreadable(key, offset, reason, knownVersion))
                .flatMap(outcome => report(outcome, offset, changes, logger))
          }
        )
        // The waiter is advanced *after* the state has been updated, so a writer that is woken and then
        // reads the state cannot see a moment in which its own record has not landed yet. Health recovers
        // after that, so a writer let back in cannot observe a record the fold has not stored.
        (folded *> waiter.traverse_(_.advance(offset)) *> onApplied(offset)).as(offset)
      }
    }

  /** Logs what folding one record did, and tells subscribers when it changed what a read returns. */
  private def report[F[_]: Async](
      outcome: StoreApplied,
      offset: Long,
      changes: Topic[F, StoreChange],
      logger: Logger[F]
  ): F[Unit] =
    outcome match {
      case StoreApplied.Accepted(change) =>
        val described = change match {
          case StoreChange.Upserted(record) => s"key=${record.key.render} version=${record.version}"
          case StoreChange.Deleted(key, version, _) => s"key=${key.render} version=$version deleted"
          case StoreChange.Desynchronized(missed) => s"desynchronized missed=$missed"
        }
        logger.info(s"store change applied: $described offset=$offset") *> changes.publish1(change).void
      case StoreApplied.Ignored(key, recordVersion, expectedVersion) =>
        logger.warn(
          s"store record ignored: key=${key.render} recordVersion=$recordVersion " +
            s"expectedVersion=$expectedVersion offset=$offset"
        )
      case StoreApplied.Unreadable(key, reason) =>
        logger.warn(s"store record unreadable: key=${key.render} offset=$offset reason=$reason")
    }

  /** One Kafka record, as far as this KUI could read it. */
  private enum Decoded {
    case Readable(record: StoreRecord)

    /** A `null` value: a physical tombstone. */
    case Erased(key: StoreKey)

    /** `knownVersion` is set only once the envelope has parsed. A decrypt failure knows its version; a record
      * that is not JSON does not.
      */
    case Unreadable(key: StoreKey, reason: String, knownVersion: Option[Long])
  }

  /** Turns one Kafka record into a decrypted `StoreRecord`, or names the key and why it could not.
    *
    * A `null` value is a physical tombstone. KUI writes logical ones — a `null` carries no timestamp and no
    * author, so it cannot answer "who deleted this cluster" — but a hand-edited topic, or another tool, may
    * contain them, so they are honoured: the key is forgotten, as compaction will eventually forget it.
    */
  private def decode[F[_]: Async](
      rawKey: String,
      rawValue: Option[String],
      crypto: FieldCrypto[F]
  ): F[Decoded] =
    StoreKey.parse(rawKey) match {
      case Left(error) =>
        // A key that will not parse cannot be named, so it is reported under a placeholder rather than
        // dropped in silence.
        Async[F].pure(
          Decoded.Unreadable(StoreKey(StoreSection.Other("unparseable"), "key"), error.message, None)
        )
      case Right(key) =>
        rawValue match {
          case None => Async[F].pure(Decoded.Erased(key))
          case Some(text) =>
            parse(text) match {
              case Left(failure) =>
                Async[F].pure(Decoded.Unreadable(key, s"it is not valid JSON (${failure.message})", None))
              case Right(json) =>
                StoreRecord.fromJsonWithKey(rawKey, json) match {
                  case Left(error) => Async[F].pure(Decoded.Unreadable(key, error.message, None))
                  case Right(record) =>
                    crypto.decryptPayload(key, record.payload).map {
                      case Right(payload) => Decoded.Readable(record.copy(payload = payload))
                      case Left(error) => Decoded.Unreadable(key, error.message, Some(record.version))
                    }
                }
            }
        }
    }

  /** The `ConfigStore` over the replayed state, with its write path.
    *
    * Reads are a lookup in the map the follower maintains. Writes produce to the log and then wait for their
    * own record to come back around through that same follower, which is what makes "the write succeeded"
    * mean "I have read my record from the log" rather than "I hopefully changed a local map".
    */
  /** What a write hands to the log. A test supplies one; production wraps the Kafka producer. */
  private[store] trait RecordSink[F[_]] {
    def produce(topic: String, key: String, value: Option[String]): F[Either[StoreError, Long]]
  }

  private def kafkaSink[F[_]: Async](
      producer: KafkaProducer[F, String, Option[String]]
  ): RecordSink[F] =
    new RecordSink[F] {
      def produce(topic: String, key: String, value: Option[String]): F[Either[StoreError, Long]] =
        // The send and its acknowledgement are one step: a cancellation between them would leave a record
        // on the log that nobody is waiting for and nobody knows about.
        Async[F]
          .uncancelable(_ => producer.produceOne_(topic, key, value).flatten.map(_.offset))
          .attempt
          .map {
            case Right(offset) => Right(offset)
            case Left(error) =>
              Left(StoreError.Unreachable(topic, s"the producer failed with ${error.getClass.getSimpleName}"))
          }
    }

  private[store] def writable[F[_]: Async](
      state: Ref[F, StoreState],
      changeTopic: Topic[F, StoreChange],
      waiter: WriteWaiter[F],
      healthRef: StoreHealthRef[F],
      sink: RecordSink[F],
      crypto: FieldCrypto[F],
      topic: String,
      writeTimeout: FiniteDuration,
      logger: Logger[F]
  ): ConfigStore[F] =
    new ConfigStore[F] {

      def get(key: StoreKey): F[Option[StoreRecord]] = state.get.map(_.get(key))

      def list(section: StoreSection): F[List[StoreRecord]] = state.get.map(_.list(section))

      def changes: Stream[F, StoreChange] = changeTopic.subscribe(ChangeBufferSize)

      /** The health value, assembled from the two things that know a piece of it: the follower's own
        * lifecycle (is the store reachable, and since when) and the replayed state (how far the log has been
        * applied, and which keys were unusable).
        */
      def health: F[StoreHealth] =
        (healthRef.get, state.get).mapN((reported, current) =>
          reported match {
            case StoreHealth.Healthy(_, since, _) =>
              StoreHealth.Healthy(current.lastAppliedOffset, since, current.unreadableKeys)
            case StoreHealth.Degraded(reason, since, _, _) =>
              StoreHealth.Degraded(reason, since, current.lastAppliedOffset, current.unreadableKeys)
            case StoreHealth.ReadOnly(reason, _) => StoreHealth.ReadOnly(reason, current.unreadableKeys)
          }
        )

      def put(
          key: StoreKey,
          payload: Json,
          baseVersion: Option[Long],
          updatedBy: String
      ): F[Either[KuiError, StoreRecord]] =
        rejectIfNotWritable.flatMap {
          case Some(rejection) => Async[F].pure(Left(rejection))
          case None => putWhenWritable(key, payload, baseVersion, updatedBy)
        }

      private def putWhenWritable(
          key: StoreKey,
          payload: Json,
          baseVersion: Option[Long],
          updatedBy: String
      ): F[Either[KuiError, StoreRecord]] =
        state.get.flatMap { current =>
          // A concealed envelope is not readable, but it is the version the log is at. Comparing only
          // against `get` would see "absent" and let a write based on the superseded record through.
          val actual = current.get(key).map(_.version).orElse(current.concealedVersion(key))
          if actual != baseVersion then
            logger.warn(
              s"store write conflict: key=${key.render} baseVersion=${baseVersion.fold("none")(_.toString)} " +
                s"currentVersion=${actual.fold("none")(_.toString)} stage=precheck"
            ) *> Async[F].pure(Left(conflict(key, baseVersion, actual)))
          else
            crypto
              .encryptPayload(key, payload)
              .attempt
              .flatMap {
                case Left(failure: StoreFailure) => Async[F].pure(Left(toKuiError(failure.error)))
                case Left(other) => Async[F].raiseError[Either[KuiError, StoreRecord]](other)
                case Right(encrypted) =>
                  now[F].flatMap { at =>
                    val record = StoreRecord(
                      StoreRecord.CurrentEnvelopeVersion,
                      key,
                      current.nextVersion(key),
                      at,
                      updatedBy,
                      deleted = false,
                      encrypted
                    )
                    send(record).map(_.map(_ => record.copy(payload = payload)))
                  }
              }
        }

      def delete(key: StoreKey, baseVersion: Long, updatedBy: String): F[Either[KuiError, Unit]] =
        rejectIfNotWritable.flatMap {
          case Some(rejection) => Async[F].pure(Left(rejection))
          case None => deleteWhenWritable(key, baseVersion, updatedBy)
        }

      /** A store that has lost its cluster rejects writes rather than queueing them. Queueing would mean a
        * write applied minutes later on top of somebody else's edit, with nobody able to see it coming.
        */
      private def rejectIfNotWritable: F[Option[KuiError]] =
        healthRef.get.flatMap {
          case StoreHealth.Healthy(_, _, _) => Async[F].pure(None)
          case StoreHealth.Degraded(reason, _, _, _) =>
            logger.warn(s"store write rejected: reason=degraded ($reason)") *>
              Async[F].pure(Some(toKuiError(StoreError.Unreachable(topic, reason))))
          case StoreHealth.ReadOnly(_, _) =>
            logger.warn("store write rejected: reason=not-configured") *>
              Async[F].pure(Some(ConfigStore.notConfigured))
        }

      private def deleteWhenWritable(
          key: StoreKey,
          baseVersion: Long,
          updatedBy: String
      ): F[Either[KuiError, Unit]] =
        state.get.flatMap { current =>
          current.get(key) match {
            // Deleting a key that is already gone is a success: the caller's intent — "this key must not
            // be there" — already holds, and no caller can act on the difference. A concealed envelope is
            // not gone: the log still has it, at the version the envelope carried.
            case None =>
              current.concealedVersion(key) match {
                case Some(held) if held != baseVersion =>
                  logger.warn(
                    s"store write conflict: key=${key.render} baseVersion=$baseVersion " +
                      s"currentVersion=$held stage=precheck"
                  ) *> Async[F].pure(Left(conflict(key, Some(baseVersion), Some(held))))
                case Some(_) =>
                  now[F].flatMap(at =>
                    send(StoreRecord.tombstone(key, current.nextVersion(key), updatedBy, at)).map(_.void)
                  )
                case None => Async[F].pure(Right(()))
              }
            case Some(existing) if existing.version != baseVersion =>
              logger.warn(
                s"store write conflict: key=${key.render} baseVersion=$baseVersion " +
                  s"currentVersion=${existing.version} stage=precheck"
              ) *> Async[F].pure(Left(conflict(key, Some(baseVersion), Some(existing.version))))
            case Some(_) =>
              now[F].flatMap(at =>
                send(StoreRecord.tombstone(key, current.nextVersion(key), updatedBy, at)).map(_.void)
              )
          }
        }

      /** Produce, then wait to read the record back, then ask the log what happened to *this* offset.
        *
        * Asking about the offset rather than comparing the map to what was written is the whole point. Two
        * replicas both produce version 3 of one key; the partition orders them; the follower on every replica
        * accepts the first and ignores the second. By the time the loser looks at the map, a third writer may
        * have moved the key on again — so the only reliable question is "what did the log do with the record
        * I produced", and the answer is a fact rather than an inference.
        */
      private def send(record: StoreRecord): F[Either[KuiError, Unit]] =
        sink.produce(topic, record.key.render, Some(record.asJson.noSpaces)).flatMap {
          case Left(error) => Async[F].pure(Left(toKuiError(error)))
          case Right(offset) =>
            waiter.await(offset, writeTimeout).flatMap {
              case Left(error) =>
                logger.error(s"store write timed out: key=${record.key.render} offset=$offset") *>
                  Async[F].pure(Left(toKuiError(error)))
              case Right(_) =>
                state.get.map(_.outcomeAt(offset)).flatMap {
                  case Some(StoreApplied.Accepted(_)) =>
                    logger.info(
                      s"store write accepted: key=${record.key.render} version=${record.version} offset=$offset " +
                        s"updatedBy=${record.updatedBy}"
                    ) *> Async[F].pure(Right(()))
                  case Some(StoreApplied.Ignored(_, recordVersion, expectedVersion)) =>
                    logger.warn(
                      s"store write conflict: key=${record.key.render} baseVersion=${recordVersion - 1L} " +
                        s"currentVersion=${expectedVersion - 1L} stage=readback"
                    ) *> Async[F].pure(
                      Left(conflict(record.key, Some(recordVersion - 1L), Some(expectedVersion - 1L)))
                    )
                  case Some(StoreApplied.Unreadable(_, reason)) =>
                    Async[F].pure(Left(toKuiError(StoreError.MalformedRecord(record.key.render, reason))))
                  // The outcome window rolled past this offset while the writer was waiting. Only
                  // possible under a write rate this store will never see, and reported honestly as
                  // "may have been applied, go and re-read" rather than guessed at.
                  case None =>
                    Async[F].pure(Left(toKuiError(StoreError.WriteTimeout(offset, writeTimeout.toMillis))))
                }
            }
        }

    }

  /** The envelope's timestamp: whole seconds, because a millisecond difference between two replicas writing
    * "the same" record is noise to the person reading a diff.
    */
  private def now[F[_]: Async]: F[java.time.Instant] =
    Clock[F].realTime.map(since =>
      java.time.Instant.ofEpochMilli(since.toMillis).truncatedTo(java.time.temporal.ChronoUnit.SECONDS)
    )

  private def conflict(key: StoreKey, baseVersion: Option[Long], current: Option[Long]): KuiError =
    ApplicationError.Remote(
      ErrorCode.ConfigVersionConflict,
      s"${key.render} changed since it was read: this write was based on version " +
        s"${baseVersion.fold("none")(_.toString)} and the store is at ${current.fold("none")(_.toString)}; " +
        "re-read it and apply the change again",
      Nil
    )

  private def toKuiError(error: StoreError): KuiError = StoreError.toKuiError(error)
}
