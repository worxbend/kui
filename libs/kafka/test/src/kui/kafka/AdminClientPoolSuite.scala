package kui.kafka

import java.io.ByteArrayOutputStream
import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.nio.file.{Files as JFiles, Path as JPath}
import java.security.KeyStore
import java.util.Base64

import scala.concurrent.duration.*

import cats.effect.std.CountDownLatch
import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all.*
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig}
import org.apache.kafka.common.errors.{TimeoutException, TopicAuthorizationException}

import kui.kernel.cluster.*
import kui.kernel.{ClusterId, Secret}
import kui.testkit.KuiIOSuite

/** The pool's own behaviour, without a broker.
  *
  * What is under test here is sharing, generations, invalidation and measurement — not whether `Admin.create`
  * works, which is a different question answered against a real broker in KAFKA-007. So the client factory is
  * a parameter and the client is a do-nothing proxy: a fake that can be counted, closed and handed out, which
  * is everything the pool does with one.
  */
final class AdminClientPoolSuite extends KuiIOSuite {

  private val id: ClusterId = ClusterId.unsafe("prod")

  private val plaintext: ClusterConnection = ClusterConnection(
    id = id,
    bootstrapServers = BootstrapServers.unsafe("broker:9092"),
    security = ClusterSecurity.Plaintext,
    overrides = ClientProperties.empty,
    admin = AdminTuning.default
  )

  /** An empty PKCS12 store, inline, so that a connection has something to materialize.
    *
    * Hoisted out of `invalidationRunsTheClientsFinalizer` because a second case needs the same connection:
    * one asserts that the file is gone afterwards, the other that it was still there while the client that
    * named it was being closed.
    */
  private lazy val inlineKeystore: String = {
    val keystore = KeyStore.getInstance("PKCS12")
    keystore.load(null, "changeit".toCharArray)

    val bytes = new ByteArrayOutputStream()
    keystore.store(bytes, "changeit".toCharArray)
    Base64.getEncoder.encodeToString(bytes.toByteArray)
  }

  private lazy val withKeystore: ClusterConnection = plaintext.copy(
    security = ClusterSecurity.Ssl(
      TlsConfig.default.copy(
        truststore = Some(
          TrustStoreRef(
            StoreSource.Inline(Secret(inlineKeystore)),
            Some(Secret("changeit")),
            StoreType.Pkcs12
          )
        )
      )
    )
  )

  /** An `Admin` that does nothing. `Admin` has some fifty methods and the pool calls none of them; a
    * reflective proxy is a fake with no maintenance cost rather than fifty stubs.
    */
  private def fakeAdmin(): Admin = {
    val handler: InvocationHandler = (_: Any, method: Method, _: Array[Object]) =>
      if method.getName == "toString" then "FakeAdmin"
      else if method.getName == "hashCode" then Integer.valueOf(0)
      else if method.getName == "equals" then java.lang.Boolean.FALSE
      else null

    Proxy
      .newProxyInstance(classOf[Admin].getClassLoader, Array(classOf[Admin]), handler)
      .asInstanceOf[Admin]
  }

  /** What the factory did, so a test can assert on it. */
  final private case class Tracker(
      created: Ref[IO, Int],
      closed: Ref[IO, Int],
      properties: Ref[IO, List[Map[String, String]]]
  )

  private def tracker: IO[Tracker] =
    (Ref.of[IO, Int](0), Ref.of[IO, Int](0), Ref.of[IO, List[Map[String, String]]](Nil))
      .mapN(Tracker.apply)

  private def factoryOf(t: Tracker): AdminClientPool.Factory[IO] =
    (_, _, rendered) =>
      Resource.make(
        t.properties.update(_ :+ rendered.unsafeValues) >> t.created.update(_ + 1) >> IO(fakeAdmin())
      )(_ => t.closed.update(_ + 1))

  private def poolOf(t: Tracker, metrics: AdminMetrics[IO]): Resource[IO, AdminClientPool[IO]] =
    AdminClientPool.resourceWith[IO](metrics, factoryOf(t))

  private def succeed(client: Admin): IO[String] = IO.pure(s"ok-${client.hashCode}")

  test("oneClientIsCreatedForTenConcurrentCalls") {
    for {
      t <- tracker
      _ <- poolOf(t, AdminMetrics.noop[IO]).use { pool =>
        List.fill(10)(pool.run(plaintext, "describeCluster")(succeed)).parSequence.void
      }
      created <- t.created.get
    } yield assertEquals(created, 1)
  }

  test("theAdminTuningTimeoutsAndTheClientIdReachTheClient") {
    for {
      t <- tracker
      _ <- poolOf(t, AdminMetrics.noop[IO]).use(_.run(plaintext, "describeCluster")(succeed))
      seen <- t.properties.get
    } yield {
      val properties = seen.headOption.getOrElse(fail("no client was created"))

      assertEquals(properties.get(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG), Some("30000"))
      assertEquals(properties.get(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG), Some("60000"))
      assert(
        properties.get(AdminClientConfig.CLIENT_ID_CONFIG).exists(_.startsWith("kui-admin-prod-")),
        properties.toString
      )
      assertEquals(properties.get("bootstrap.servers"), Some("broker:9092"))
    }
  }

  test("anOperatorOverrideBeatsTheAdminTuningDefault") {
    val overridden = plaintext.copy(
      overrides = ClientProperties.fromRaw(Map(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG -> "1234"))
    )

    for {
      t <- tracker
      _ <- poolOf(t, AdminMetrics.noop[IO]).use(_.run(overridden, "describeCluster")(succeed))
      seen <- t.properties.get
    } yield assertEquals(
      seen.headOption.flatMap(_.get(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG)),
      Some("1234")
    )
  }

  test("aReconnectClassFailureReplacesTheClientExactlyOnce") {
    for {
      t <- tracker
      // The two failures have to be holding the *same* client before either of them fails, because
      // that is the situation the generation check exists for. `parSequence` on its own does not
      // establish it, it only makes it likely: a scheduler that runs the first call to completion
      // before starting the second hands the second a client of its own, and three creations is the
      // right answer to the question that was then asked. This case failed once under sixteen-way
      // parallel load and passed alone, and inserting a 50ms delay before the second call reproduces
      // that failure every time — so what was reported as a flake is this precondition being left to
      // the scheduler. The latch makes it a fact: both calls arrive, then both fail.
      arrived <- CountDownLatch[IO](2)
      failing = (_: Admin) =>
        arrived.release >> arrived.await >> IO.raiseError[String](new TimeoutException("timed out"))
      _ <- poolOf(t, AdminMetrics.noop[IO]).use { pool =>
        for {
          _ <- pool.run(plaintext, "describeCluster")(succeed)
          _ <- List
            .fill(2)(pool.run(plaintext, "describeCluster")(failing).attempt)
            .parSequence
          _ <- pool.run(plaintext, "describeCluster")(succeed)
        } yield ()
      }
      created <- t.created.get
      closed <- t.closed.get
    } yield {
      assertEquals(created, 2, "the client was rebuilt more than once for one dead connection")
      assertEquals(closed, 2, "not every client was closed")
    }
  }

  test("aRequestLevelFailureDoesNotReplaceTheClient") {
    // The regression guard against Kafbat's "invalidate on any error": asking about a topic you are
    // not authorized for must not cost a reconnect and a fresh SASL handshake.
    val refused: Admin => IO[String] = _ => IO.raiseError(new TopicAuthorizationException("no"))

    for {
      t <- tracker
      _ <- poolOf(t, AdminMetrics.noop[IO]).use { pool =>
        pool.run(plaintext, "describeConfigs")(succeed) >>
          pool.run(plaintext, "describeConfigs")(refused).attempt >>
          pool.run(plaintext, "describeConfigs")(succeed)
      }
      created <- t.created.get
    } yield assertEquals(created, 1)
  }

  test("theFailingCallStillFails") {
    // Invalidation does not retry: the caller sees one failure with its original error, and the
    // next call gets a fresh client. Retrying inside the pool would double every timeout and hide
    // the failure from the metric.
    val boom = new TimeoutException("timed out")

    for {
      t <- tracker
      result <- poolOf(t, AdminMetrics.noop[IO])
        .use(_.run(plaintext, "describeCluster")(_ => IO.raiseError[String](boom)))
        .attempt
    } yield assertEquals(result.left.toOption.map(_.getMessage), Some("timed out"))
  }

  test("invalidationRunsTheClientsFinalizer") {
    // The materialized keystore has to go with the client that used it: it is a private key on
    // disk, and the client that named it no longer exists.
    for {
      t <- tracker
      location <- poolOf(t, AdminMetrics.noop[IO]).use { pool =>
        for {
          _ <- pool.run(withKeystore, "describeCluster")(succeed)
          seen <- t.properties.get
          path = seen.headOption
            .flatMap(_.get("ssl.truststore.location"))
            .getOrElse(fail("no truststore was materialized"))
          existedBefore <- IO(JFiles.exists(JPath.of(path)))
          _ <- IO(assert(existedBefore, s"$path was never written"))
          _ <- pool.invalidate(id)
        } yield path
      }
      closed <- t.closed.get
    } yield {
      assertEquals(closed, 1)
      assert(!JFiles.exists(JPath.of(location)), s"$location outlived the client that used it")
    }
  }

  test("the client is closed before the keystore it was using is deleted") {
    // `Entry.release` is `releaseClient >> releaseProperties`, and the line above it says "Order
    // matters: the client has to stop using the keystore before the keystore is deleted". Swapping the
    // two left all 279 cases of this module, `libs/kafka-auth` and `libs/serde-confluent` green — a
    // client with a live network thread would go on reading a truststore that had already been removed,
    // and the failure it eventually produced would name a missing file rather than a shutdown.
    for {
      t <- tracker
      atClose <- Ref.of[IO, Option[Boolean]](None)
      factory = (
          (_, _, rendered) =>
            Resource.make(
              t.properties.update(_ :+ rendered.unsafeValues) >> t.created.update(_ + 1) >> IO(fakeAdmin())
            ) { _ =>
              val named = rendered.unsafeValues.get("ssl.truststore.location")
              IO(named.exists(path => JFiles.exists(JPath.of(path))))
                .flatMap(present => atClose.set(Some(present))) >> t.closed.update(_ + 1)
            }
      ): AdminClientPool.Factory[IO]
      location <- AdminClientPool.resourceWith[IO](AdminMetrics.noop[IO], factory).use { pool =>
        for {
          _ <- pool.run(withKeystore, "describeCluster")(succeed)
          seen <- t.properties.get
        } yield seen.headOption
          .flatMap(_.get("ssl.truststore.location"))
          .getOrElse(fail("no truststore was materialized"))
      }
      seenAtClose <- atClose.get
    } yield {
      assertEquals(
        seenAtClose,
        Some(true),
        clue = "the keystore was deleted while the client that named it was still being closed"
      )
      assert(!JFiles.exists(JPath.of(location)), s"$location outlived the client that used it")
    }
  }

  test("evictClosesAndForgets") {
    for {
      t <- tracker
      _ <- poolOf(t, AdminMetrics.noop[IO]).use { pool =>
        pool.run(plaintext, "describeCluster")(succeed) >>
          pool.evict(id) >>
          pool.run(plaintext, "describeCluster")(succeed)
      }
      created <- t.created.get
      closed <- t.closed.get
    } yield {
      assertEquals(created, 2)
      assertEquals(closed, 2)
    }
  }

  test("evictionWaitsForAnInFlightCreationBeforeForgettingTheClient") {
    for {
      t <- tracker
      creationStarted <- Deferred[IO, Unit]
      allowCreation <- Deferred[IO, Unit]
      evictionStarted <- Deferred[IO, Unit]
      factory = (
          (_, _, rendered) =>
            Resource.make(
              creationStarted.complete(()).void >> allowCreation.get >>
                t.properties.update(_ :+ rendered.unsafeValues) >>
                t.created.update(_ + 1) >> IO(fakeAdmin())
            )(_ => t.closed.update(_ + 1))
      ): AdminClientPool.Factory[IO]
      _ <- AdminClientPool.resourceWith[IO](AdminMetrics.noop[IO], factory).use { pool =>
        for {
          first <- pool.run(plaintext, "describeCluster")(succeed).start
          _ <- creationStarted.get
          eviction <- (evictionStarted.complete(()).void >> pool.evict(id)).start
          // Let eviction reach the cluster gate before the paused factory is allowed to finish.
          _ <- evictionStarted.get >> IO.cede
          _ <- allowCreation.complete(())
          _ <- first.joinWithNever
          _ <- eviction.joinWithNever
          _ <- pool.run(plaintext, "describeCluster")(succeed)
        } yield ()
      }
      created <- t.created.get
      closed <- t.closed.get
    } yield {
      assertEquals(created, 2, "the client created before eviction completed was cached afterwards")
      assertEquals(closed, 2, "not every created client was closed")
    }
  }

  test("aPropertyRenderingFailureCachesNothing") {
    // AWS MSK IAM's login module is deliberately not on KUI's classpath. A cluster configured for
    // it must fail every call with the actionable error and must not occupy a pool slot.
    val misconfigured = plaintext.copy(
      security = ClusterSecurity.Sasl(
        SaslProtocol.SaslSsl,
        SaslMechanism.AwsMskIam(None, None, None),
        None
      )
    )

    for {
      t <- tracker
      result <- poolOf(t, AdminMetrics.noop[IO])
        .use(_.run(misconfigured, "describeCluster")(succeed))
        .attempt
      created <- t.created.get
    } yield {
      assertEquals(created, 0)
      assert(
        result.left.toOption.exists {
          case KafkaClientConfigurationFailure(_) => true
          case _ => false
        },
        result.toString
      )
      assert(result.left.exists(_.getMessage.contains("aws-msk-iam-auth")), result.toString)
    }
  }

  test("everyRunIsMeasured") {
    for {
      t <- tracker
      metrics <- FakeAdminMetrics.create[IO]
      _ <- poolOf(t, metrics).use { pool =>
        pool.run(plaintext, "describeCluster")(succeed) >>
          pool
            .run(plaintext, "describeLogDirs")(_ => IO.raiseError[String](new TimeoutException("x")))
            .attempt
      }
      entries <- metrics.entries
    } yield assertEquals(
      entries,
      List(
        FakeAdminMetrics.Entry(id, "describeCluster", succeeded = true),
        FakeAdminMetrics.Entry(id, "describeLogDirs", succeeded = false)
      )
    )
  }

  test("closeIsCalledOnResourceReleaseForEveryCluster") {
    val staging = plaintext.copy(id = ClusterId.unsafe("staging"))

    for {
      t <- tracker
      _ <- poolOf(t, AdminMetrics.noop[IO]).use { pool =>
        pool.run(plaintext, "describeCluster")(succeed) >>
          pool.run(staging, "describeCluster")(succeed)
      }
      created <- t.created.get
      closed <- t.closed.get
    } yield {
      assertEquals(created, 2)
      assertEquals(closed, 2)
    }
  }

  test("aCancelledPoolStillClosesEveryClientItOpened") {
    // The `Resource` release runs on the cancellation path too. Without it, a cancelled startup
    // leaves a Kafka network thread alive for the life of the process.
    for {
      t <- tracker
      started <- Ref.of[IO, Boolean](false)
      fiber <- poolOf(t, AdminMetrics.noop[IO])
        .use(pool => pool.run(plaintext, "describeCluster")(succeed) >> started.set(true) >> IO.never[Unit])
        .start
      _ <- started.get.iterateUntil(identity)
      _ <- fiber.cancel
      closed <- t.closed.get
    } yield assertEquals(closed, 1)
  }

  test("releasingThePoolDoesNotWaitOutTheCloseGraceForACallNobodyIsWaitingFor") {
    // A real client, because the grace period is Kafka's: `Admin.close(timeout)` keeps its I/O thread
    // until every pending call finishes or the timeout passes. A caller cancelled against a broker that
    // never answers leaves its call pending, so each pool spent the whole five-second grace on a result
    // nobody could receive, and a process with one pool per service stopped that many times slower.
    val unreachable = plaintext.copy(bootstrapServers = BootstrapServers.unsafe("127.0.0.1:1"))

    for {
      allocated <- AdminClientPool.resource[IO](AdminMetrics.noop[IO]).allocated
      (pool, release) = allocated
      submitted <- Deferred[IO, Unit]
      caller <- pool
        .run(unreachable, "describeCluster")(admin =>
          IO.blocking(admin.describeCluster()) >> submitted.complete(()) >> IO.never[Unit]
        )
        .start
      _ <- submitted.get
      _ <- caller.cancel
      elapsed <- release.timed.map(_._1)
    } yield assert(
      elapsed < 5.seconds,
      s"release waited ${elapsed.toMillis}ms, the full close grace, for a call whose caller was gone"
    )
  }
}
