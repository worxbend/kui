package kui.http.upstream

import java.io.{ByteArrayOutputStream, InputStream}
import java.net.{InetAddress, InetSocketAddress}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.security.{KeyStore, SecureRandom}
import java.util.Base64
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

import scala.jdk.CollectionConverters.*
import scala.util.Using

import cats.effect.{IO, Resource}
import com.sun.net.httpserver.{HttpsConfigurator, HttpsParameters, HttpsServer}

import kui.config.{HttpKeyStore, HttpStoreFormat, HttpStoreMaterial, HttpTrustStore}
import kui.kernel.Secret
import kui.testkit.kafka.CertificateAuthority

/** Generated stores and real loopback HTTPS listeners for HTTP TLS tests.
  *
  * Both listeners present the same certificate. One binds to the certificate's `127.0.0.1` SAN; the other
  * binds to unnamed `127.0.0.2`, making hostname verification observable without DNS or an external network.
  */
object HttpsUpstreamFixture {

  final case class Materials(
      pkcs12Truststore: Path,
      pkcs12Keystore: Path,
      jksTruststore: Path,
      jksKeystore: Path,
      password: String,
      serverKeystore: Path,
      serverTruststore: Path,
      serverStorePassword: String,
      serverKeyPassword: String
  ) {

    def truststore(format: HttpStoreFormat, inline: Boolean): HttpTrustStore = {
      val path = pathFor(format, key = false)
      HttpTrustStore(material(path, inline), Secret(password), format)
    }

    def keystore(format: HttpStoreFormat, inline: Boolean): HttpKeyStore = {
      val path = pathFor(format, key = true)
      HttpKeyStore(material(path, inline), Secret(password), Secret(password), format)
    }

    private def pathFor(format: HttpStoreFormat, key: Boolean): Path =
      (format, key) match {
        case (HttpStoreFormat.Pkcs12, false) => pkcs12Truststore
        case (HttpStoreFormat.Pkcs12, true) => pkcs12Keystore
        case (HttpStoreFormat.Jks, false) => jksTruststore
        case (HttpStoreFormat.Jks, true) => jksKeystore
      }

    private def material(path: Path, inline: Boolean): HttpStoreMaterial =
      if inline then
        HttpStoreMaterial.Inline(Secret(Base64.getEncoder.encodeToString(Files.readAllBytes(path))))
      else HttpStoreMaterial.Location(path.toString)
  }

  final case class Running(
      materials: Materials,
      url: String,
      wrongHostnameUrl: String,
      serverName: AtomicReference[String]
  )

  def resource: Resource[IO, Materials] =
    Resource
      .make(IO.blocking(Files.createTempDirectory("kui-http-tls")))(deleteRecursively)
      .evalMap { directory =>
        CertificateAuthority.materialize[IO](directory).flatMap { case (client, broker) =>
          IO.blocking {
            val jksTruststore = directory.resolve("truststore.jks")
            val jksKeystore = directory.resolve("client.keystore.jks")
            convert(client.truststore, jksTruststore, client.truststorePassword)
            convert(client.keystore, jksKeystore, client.keystorePassword)
            Materials(
              client.truststore,
              client.keystore,
              jksTruststore,
              jksKeystore,
              client.truststorePassword,
              broker.keystore,
              broker.truststore,
              broker.storePassword,
              broker.keyPassword
            )
          }
        }
      }

  /** Two real HTTPS listeners using the same certificate: one on a named SAN and one on an unnamed address.
    */
  def server(requireClientCertificate: Boolean): Resource[IO, Running] =
    resource.flatMap { materials =>
      val serverName = new AtomicReference[String]("")
      for {
        correct <- listener(materials, "127.0.0.1", requireClientCertificate, serverName)
        wrong <- listener(materials, "127.0.0.2", requireClientCertificate, serverName)
      } yield Running(
        materials,
        url(correct, "127.0.0.1"),
        url(wrong, "127.0.0.2"),
        serverName
      )
    }

  def emptyStoreBase64(format: HttpStoreFormat, password: String): String = {
    val store = KeyStore.getInstance(format.wireName)
    store.load(noInput, password.toCharArray)
    val bytes = ByteArrayOutputStream()
    store.store(bytes, password.toCharArray)
    Base64.getEncoder.encodeToString(bytes.toByteArray)
  }

  private def convert(source: Path, destination: Path, password: String): Unit = {
    val chars = password.toCharArray
    val from = KeyStore.getInstance("PKCS12")
    Using.resource(Files.newInputStream(source))(from.load(_, chars))

    val to = KeyStore.getInstance("JKS")
    to.load(noInput, chars)
    from.aliases().asScala.foreach { alias =>
      if from.isKeyEntry(alias) then
        to.setKeyEntry(alias, from.getKey(alias, chars), chars, from.getCertificateChain(alias))
      else to.setCertificateEntry(alias, from.getCertificate(alias))
    }
    Using.resource(Files.newOutputStream(destination))(to.store(_, chars))
  }

  private def listener(
      materials: Materials,
      address: String,
      requireClientCertificate: Boolean,
      serverName: AtomicReference[String]
  ): Resource[IO, HttpsServer] =
    for {
      executor <- Resource.make(IO.blocking(testExecutor))(shutdown)
      server <- Resource.make(
        IO.blocking(HttpsServer.create(InetSocketAddress(InetAddress.getByName(address), 0), 0))
      )(server => IO.blocking(server.stop(0)))
      _ <- Resource.eval(
        IO.blocking(start(server, executor, materials, requireClientCertificate, serverName))
      )
    } yield server

  private def start(
      server: HttpsServer,
      executor: ExecutorService,
      materials: Materials,
      requireClientCertificate: Boolean,
      serverName: AtomicReference[String]
  ): Unit = {
    val context = serverContext(materials, requireClientCertificate)
    server.setExecutor(executor)
    server.setHttpsConfigurator(new HttpsConfigurator(context) {
      override def configure(parameters: HttpsParameters): Unit = {
        val ssl = context.getDefaultSSLParameters
        ssl.setNeedClientAuth(requireClientCertificate)
        ssl.setSNIMatchers(java.util.List.of(new javax.net.ssl.SNIMatcher(0) {
          override def matches(name: javax.net.ssl.SNIServerName): Boolean = {
            serverName.set(new javax.net.ssl.SNIHostName(name.getEncoded).getAsciiName)
            true
          }
        }))
        parameters.setSSLParameters(ssl)
      }
    })
    val _ = server.createContext(
      "/probe",
      exchange => {
        val body = "ok".getBytes(StandardCharsets.UTF_8)
        exchange.sendResponseHeaders(200, body.length.toLong)
        Using.resource(exchange.getResponseBody)(_.write(body))
        exchange.close()
      }
    )
    server.start()
  }

  private def serverContext(materials: Materials, requireClientCertificate: Boolean): SSLContext = {
    val keys = KeyStore.getInstance("PKCS12")
    val keyStorePassword = materials.serverStorePassword.toCharArray
    val keyPassword = materials.serverKeyPassword.toCharArray
    try {
      Using.resource(Files.newInputStream(materials.serverKeystore))(keys.load(_, keyStorePassword))
      val keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
      keyManagers.init(keys, keyPassword)

      val trustManagers =
        if requireClientCertificate then {
          val trust = KeyStore.getInstance("PKCS12")
          Using.resource(Files.newInputStream(materials.serverTruststore))(
            trust.load(_, keyStorePassword)
          )
          val factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
          factory.init(trust)
          factory.getTrustManagers
        } else Option.empty[Array[javax.net.ssl.TrustManager]].orNull

      val context = SSLContext.getInstance("TLS")
      context.init(keyManagers.getKeyManagers, trustManagers, SecureRandom())
      context
    } finally {
      java.util.Arrays.fill(keyStorePassword, '\u0000')
      java.util.Arrays.fill(keyPassword, '\u0000')
    }
  }

  private def testExecutor: ExecutorService =
    Executors.newCachedThreadPool { (runnable: Runnable) =>
      val thread = Thread(runnable, "kui-http-tls-test")
      thread.setDaemon(true)
      thread
    }

  private def shutdown(executor: ExecutorService): IO[Unit] =
    IO.blocking {
      executor.shutdownNow()
      val _ = executor.awaitTermination(5L, TimeUnit.SECONDS)
    }

  private def url(server: HttpsServer, host: String): String =
    s"https://$host:${server.getAddress.getPort}/probe"

  private def deleteRecursively(directory: Path): IO[Unit] =
    IO.blocking {
      if Files.exists(directory) then
        Using.resource(Files.walk(directory))(
          _.sorted(java.util.Comparator.reverseOrder()).iterator().asScala.foreach(Files.deleteIfExists)
        )
    }

  private def noInput: InputStream = Option.empty[InputStream].orNull
}
