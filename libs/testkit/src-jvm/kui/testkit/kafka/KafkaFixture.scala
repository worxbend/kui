package kui.testkit.kafka

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.Locale

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters.*

import cats.effect.{Async, Resource}
import cats.syntax.all.*
import com.github.dockerjava.api.command.InspectContainerResponse
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.Transferable
import org.testcontainers.utility.{DockerImageName, MountableFile}

/** Starts one Kafka broker in one of the three security configurations of [[KafkaTopology]], and stops it
  * when the resource closes.
  *
  * ==Why this is not `org.testcontainers.kafka.KafkaContainer`==
  *
  * That class covers the PLAINTEXT case and nothing else: its listener map is fixed, so neither the SASL nor
  * the TLS mode can be expressed through it. Rather than use it for one mode and hand-roll two, this fixture
  * hand-rolls all three, so the three brokers differ only in the environment they are given — which is
  * exactly what the parity suite needs to be able to claim.
  *
  * ==The advertised-listener problem==
  *
  * A broker has to advertise an address a client can reach, and under Testcontainers that address is
  * `localhost:<mapped port>` — which is not known until the container has started. The standard answer, used
  * here and by Testcontainers itself, is to hold the entrypoint at the door:
  *
  *   1. the container's command waits for a starter script to appear;
  *   2. Testcontainers starts it and maps the client port;
  *   3. this fixture writes the starter script, with the now-known port in `KAFKA_ADVERTISED_LISTENERS`, and
  *      the broker boots.
  *
  * ==What happens when it does not come up==
  *
  * Five facts are attached to the failure: the topology, the pinned image, the mapped port, the last lines of
  * the container log and — for TLS — the broker certificate's subject and SAN list. Those separate "the image
  * changed", "the port was not mapped", "the broker refused its own configuration" and "the certificate did
  * not match the host", which is every way this fixture actually fails.
  */
object KafkaFixture {

  /** The broker image, pinned. Never a floating tag: a suite whose behaviour depends on when it ran is not a
    * suite.
    *
    * `KUI_TEST_KAFKA_IMAGE` overrides it, which is how ADR-030's nightly job points the same fixture at a
    * 2.8-era broker to check the minimum-version promise rather than merely asserting it.
    */
  val Image: String = sys.env.getOrElse("KUI_TEST_KAFKA_IMAGE", "apache/kafka:4.3.1")

  /** Whether a Docker daemon is reachable.
    *
    * A suite calls this and skips *loudly* when it is false. A security mode that could not be checked must
    * never be reported as a pass — that is the difference between "M1's exit criterion holds" and "nothing
    * contradicted it".
    */
  def dockerAvailable: Boolean =
    scala.util.Try(DockerClientFactory.instance().isDockerAvailable).getOrElse(false)

  /** The port the client listener binds inside the container. */
  private val ClientPort: Int = 9093

  private val StarterScript: String = "/kui-start.sh"

  /** Where the generated PKCS12 stores are mounted inside the container. */
  private val SecretsDirectory: String = "/etc/kafka/secrets"

  /** `rw-r--r--`: the mode each store is copied in with, so the image's non-root broker can read it. */
  private val ReadableByAll: Integer = Integer.valueOf(0x1a4)

  /** Fixed, so a fixture that is restarted keeps the same cluster identity and a failure message can be
    * grepped for it.
    */
  private val ClusterId: String = "4L6g3nShT-eMCtK--X86sw"

  /** The bootstrap SASL user. It exists only so that [[ScramProvisioner]] has a way in before any SCRAM user
    * exists; see the note there on why a PLAIN mechanism alongside SCRAM does not weaken what this mode
    * demonstrates.
    */
  private val bootstrapAdmin: ScramCredentials = ScramCredentials("kui-bootstrap", "kui-bootstrap-secret")

  /** The SCRAM user a client actually authenticates as. */
  val scramUser: ScramCredentials = ScramCredentials("kui", "kui-scram-secret")

  def apply[F[_]: Async](
      topology: KafkaTopology,
      startTimeout: FiniteDuration = 90.seconds
  ): Resource[F, RunningBroker] =
    for {
      directory <- temporaryDirectory[F](
        s"kui-kafka-${topology.securityProtocol.toLowerCase(Locale.ROOT)}"
      )
      materials <- Resource.eval(tlsMaterials[F](topology, directory))
      container <- container[F](topology, materials.map(_._2), startTimeout)
      broker <- Resource.eval(describe[F](topology, container, materials.map(_._1)))
      _ <- Resource.eval(provision[F](broker))
    } yield broker

  // -----------------------------------------------------------------------------------------------
  // The container
  // -----------------------------------------------------------------------------------------------

  private def container[F[_]: Async](
      topology: KafkaTopology,
      broker: Option[BrokerTlsMaterials],
      startTimeout: FiniteDuration
  ): Resource[F, GenericContainer[?]] =
    Resource.make(Async[F].blocking(started(topology, broker, startTimeout)))(container =>
      // Stopping is `blocking` and uncancellable by construction: `Resource.make`'s release runs even when
      // the acquiring fiber was cancelled, and a container that outlives its test run is a leak the next
      // developer discovers as a port already in use.
      Async[F].blocking(container.stop())
    )

  private def started(
      topology: KafkaTopology,
      broker: Option[BrokerTlsMaterials],
      startTimeout: FiniteDuration
  ): GenericContainer[?] = {
    val container = HeldKafkaContainer(DockerImageName.parse(Image))

    val _ = container.withExposedPorts(Integer.valueOf(ClientPort))
    val _ = container.withEnv(environment(topology).asJava)
    val _ = container.withCommand(
      "sh",
      "-c",
      s"while [ ! -f $StarterScript ]; do sleep 0.1; done; chmod +x $StarterScript; $StarterScript"
    )
    val _ = container.waitingFor(
      Wait
        .forLogMessage(".*Kafka Server started.*\\n", 1)
        .withStartupTimeout(java.time.Duration.ofMillis(startTimeout.toMillis))
    )

    // Each store is copied on its own with an explicit, world-readable mode. Copying the directory instead
    // carries over the host's ownership and the 0700 of `createTempDirectory`, and the image's broker runs
    // as a non-root `appuser` that then cannot open its own keystore (`AccessDeniedException`). The stores
    // hold throwaway test keys, so readable-by-all inside a single-use container costs nothing.
    broker.foreach { tls =>
      List(tls.keystore -> "broker.keystore.p12", tls.truststore -> "truststore.p12").foreach {
        case (store, name) =>
          val _ = container.withCopyFileToContainer(
            MountableFile.forHostPath(store.toAbsolutePath, ReadableByAll),
            s"$SecretsDirectory/$name"
          )
      }
    }

    container.start()
    container
  }

  /** A container that writes its own starter script the moment Docker reports it running.
    *
    * `containerIsStarting` is the only hook that runs after the port is mapped and before the wait strategy
    * begins, which is precisely the window in which the advertised address becomes knowable.
    */
  final private class HeldKafkaContainer(image: DockerImageName)
      extends GenericContainer[HeldKafkaContainer](image) {

    override def containerIsStarting(info: InspectContainerResponse): Unit = {
      val advertised = s"CLIENT://localhost:${getMappedPort(ClientPort)},BROKER://localhost:9092"
      val script =
        s"""#!/bin/sh
           |export KAFKA_ADVERTISED_LISTENERS='$advertised'
           |exec /etc/kafka/docker/run
           |""".stripMargin

      copyFileToContainer(Transferable.of(script.getBytes(StandardCharsets.UTF_8), 0x1ff), StarterScript)
    }
  }

  /** Everything the broker reads out of its environment.
    *
    * The controller and inter-broker listeners stay PLAINTEXT in every mode, and that is deliberate rather
    * than an oversight: what is under test is how *KUI's client* reaches a broker. Securing a single-node
    * broker's conversation with itself would add three more ways for the fixture to fail and would test
    * nothing KUI does.
    */
  private def environment(topology: KafkaTopology): Map[String, String] = {
    val common = Map(
      "CLUSTER_ID" -> ClusterId,
      "KAFKA_NODE_ID" -> "1",
      "KAFKA_PROCESS_ROLES" -> "broker,controller",
      "KAFKA_CONTROLLER_QUORUM_VOTERS" -> "1@localhost:9094",
      "KAFKA_CONTROLLER_LISTENER_NAMES" -> "CONTROLLER",
      "KAFKA_INTER_BROKER_LISTENER_NAME" -> "BROKER",
      "KAFKA_LISTENERS" -> s"CLIENT://0.0.0.0:$ClientPort,BROKER://0.0.0.0:9092,CONTROLLER://0.0.0.0:9094",
      "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP" ->
        s"CONTROLLER:PLAINTEXT,CLIENT:${topology.securityProtocol},BROKER:PLAINTEXT",
      "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR" -> "1",
      "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR" -> "1",
      "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR" -> "1",
      "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS" -> "0"
    )

    common ++ (topology match {
      case KafkaTopology.Plaintext => Map.empty

      case KafkaTopology.SaslScram =>
        Map(
          "KAFKA_SASL_ENABLED_MECHANISMS" -> "PLAIN,SCRAM-SHA-512",
          "KAFKA_LISTENER_NAME_CLIENT_PLAIN_SASL_JAAS_CONFIG" ->
            ("org.apache.kafka.common.security.plain.PlainLoginModule required " +
              s"""username="${bootstrapAdmin.username}" password="${bootstrapAdmin.password}" """ +
              s"""user_${bootstrapAdmin.username}="${bootstrapAdmin.password}";"""),
          "KAFKA_LISTENER_NAME_CLIENT_SCRAM___SHA___512_SASL_JAAS_CONFIG" ->
            "org.apache.kafka.common.security.scram.ScramLoginModule required;"
        )

      case KafkaTopology.MutualTls =>
        // The Apache image maps `KAFKA_<PROPERTY>` straight onto `<property>` in server.properties, so
        // these are the Kafka settings themselves. The `_FILENAME` / `_CREDENTIALS` indirection belongs to
        // the Confluent image and is silently ignored here — which shows up as a broker that starts happily
        // with no keystore and then fails every handshake.
        Map(
          "KAFKA_SSL_KEYSTORE_LOCATION" -> s"$SecretsDirectory/broker.keystore.p12",
          "KAFKA_SSL_KEYSTORE_PASSWORD" -> CertificateAuthority.StorePassword,
          "KAFKA_SSL_KEY_PASSWORD" -> CertificateAuthority.StorePassword,
          "KAFKA_SSL_KEYSTORE_TYPE" -> "PKCS12",
          "KAFKA_SSL_TRUSTSTORE_LOCATION" -> s"$SecretsDirectory/truststore.p12",
          "KAFKA_SSL_TRUSTSTORE_PASSWORD" -> CertificateAuthority.StorePassword,
          "KAFKA_SSL_TRUSTSTORE_TYPE" -> "PKCS12",
          // The whole point of the mode: a client that presents no certificate is refused.
          "KAFKA_SSL_CLIENT_AUTH" -> "required",
          // The broker's own client-side checks against itself; KUI's client keeps verification on.
          "KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM" -> ""
        )
    })
  }

  // -----------------------------------------------------------------------------------------------
  // What a client needs
  // -----------------------------------------------------------------------------------------------

  private def describe[F[_]: Async](
      topology: KafkaTopology,
      container: GenericContainer[?],
      materials: Option[TlsMaterials]
  ): F[RunningBroker] =
    Async[F].blocking {
      val bootstrap = s"localhost:${container.getMappedPort(ClientPort)}"
      RunningBroker(
        topology = topology,
        bootstrapServers = bootstrap,
        clientProperties = clientProperties(topology, bootstrap, materials),
        materials = materials,
        credentials = Option.when(topology == KafkaTopology.SaslScram)(scramUser),
        logs = () => lastLines(container.getLogs)
      )
    }

  /** Client properties that reach this broker, assembled by hand.
    *
    * Deliberately by hand rather than through KUI's own renderer: this map is the *independent* answer the
    * parity suite compares KUI's rendering against. If both sides came from `libs/kafka-auth`, a bug in the
    * renderer would agree with itself.
    */
  private def clientProperties(
      topology: KafkaTopology,
      bootstrap: String,
      materials: Option[TlsMaterials]
  ): Map[String, String] = {
    val base = Map("bootstrap.servers" -> bootstrap, "security.protocol" -> topology.securityProtocol)

    topology match {
      case KafkaTopology.Plaintext => base

      case KafkaTopology.SaslScram =>
        base ++ Map(
          "sasl.mechanism" -> "SCRAM-SHA-512",
          "sasl.jaas.config" ->
            ("org.apache.kafka.common.security.scram.ScramLoginModule required " +
              s"""username="${scramUser.username}" password="${scramUser.password}";""")
        )

      case KafkaTopology.MutualTls =>
        base ++ materials.toList.flatMap { tls =>
          List(
            "ssl.truststore.location" -> tls.truststore.toAbsolutePath.toString,
            "ssl.truststore.password" -> tls.truststorePassword,
            "ssl.truststore.type" -> "PKCS12",
            "ssl.keystore.location" -> tls.keystore.toAbsolutePath.toString,
            "ssl.keystore.password" -> tls.keystorePassword,
            "ssl.keystore.type" -> "PKCS12",
            "ssl.key.password" -> tls.keyPassword
          )
        }.toMap
    }
  }

  private def tlsMaterials[F[_]: Async](
      topology: KafkaTopology,
      directory: Path
  ): F[Option[(TlsMaterials, BrokerTlsMaterials)]] =
    topology match {
      case KafkaTopology.MutualTls => CertificateAuthority.materialize[F](directory).map(Some(_))
      case _ => Async[F].pure(None)
    }

  private def provision[F[_]: Async](broker: RunningBroker): F[Unit] =
    broker.topology match {
      case KafkaTopology.SaslScram =>
        ScramProvisioner.create[F](broker.bootstrapServers, bootstrapAdmin, scramUser)
      case _ => Async[F].unit
    }

  // -----------------------------------------------------------------------------------------------
  // Housekeeping
  // -----------------------------------------------------------------------------------------------

  private def temporaryDirectory[F[_]: Async](prefix: String): Resource[F, Path] =
    Resource.make(Async[F].blocking(Files.createTempDirectory(prefix)))(directory =>
      Async[F].blocking(deleteTree(directory))
    )

  private def deleteTree(directory: Path): Unit = {
    val walked = Files.walk(directory)
    val paths = walked.iterator().asScala.toList.reverse
    walked.close()
    paths.foreach(path => Files.deleteIfExists(path).discard())
  }

  /** The tail of the container log, which is what a failure message wants. The whole log of a broker that
    * would not start is several hundred lines of successful configuration parsing followed by the one line
    * that matters.
    */
  private def lastLines(log: String): String = log.linesIterator.toList.takeRight(50).mkString("\n")

  extension (value: Boolean) private def discard(): Unit = ()
}
