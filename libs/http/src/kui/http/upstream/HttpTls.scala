package kui.http.upstream

import java.io.ByteArrayInputStream
import java.net.{InetAddress, InetSocketAddress}
import java.nio.file.{Files, Path}
import java.security.{KeyStore, PrivateKey, SecureRandom}
import java.util.{Arrays, Base64}
import javax.net.ssl.{
  KeyManager,
  KeyManagerFactory,
  SSLContext,
  TrustManager,
  TrustManagerFactory,
  X509TrustManager
}

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.jdk.CollectionConverters.*
import scala.util.control.{NoStackTrace, NonFatal}
import scala.util.{Try, Using}

import cats.effect.kernel.{Async, Deferred, Fiber, Ref, Resource}
import cats.effect.std.{Semaphore, Supervisor}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.client.{Client, RequestKey}
import sttp.capabilities.Effect
import sttp.capabilities.fs2.Fs2Streams
import sttp.client4.http4s.Http4sBackend
import sttp.client4.wrappers.DelegateBackend
import sttp.client4.{
  GenericRequest,
  GenericResponseAs,
  MappedResponseAs,
  Response,
  ResponseAsBoth,
  ResponseAsFromMetadata,
  ResponseAsStream,
  ResponseAsStreamUnsafe,
  StreamBackend
}

import kui.config.{
  HttpKeyStore,
  HttpStoreFormat,
  HttpStoreMaterial,
  HttpTlsConfig,
  HttpTrustStore,
  SafeUrl,
  UrlPolicy
}
import kui.kernel.Secret

/** Builds one source-owned HTTP transport from validated TLS configuration.
  *
  * A custom truststore is handed to `TrustManagerFactory` by itself: JVM roots are not merged into it. When
  * no truststore is configured the SSL context delegates trust to the JVM defaults. Hostname verification is
  * enforced against the original hostname, independently of the numeric socket destination.
  */
object HttpTls {

  /** Separate, bounded HTTP/1.1 budgets. Queued calls count separately in each pool. */
  final case class Capacity(finite: Int = 10, streaming: Int = 10, queuedPerPool: Int = 256) {
    require(finite > 0 && streaming > 0, "HTTP capacities must be positive")
    require(queuedPerPool >= 0, "HTTP queue capacity must be nonnegative")
  }

  final private[upstream] case class Diagnostics[F[_]](
      finiteSlots: F[Long],
      streamingSlots: F[Long],
      ownedRequests: F[Int]
  )

  /** The resource owns both pools and every adapter response fiber, including handed-off streams. */
  def resource[F[_]: Async](
      config: HttpTlsConfig,
      policy: UrlPolicy = UrlPolicy.Strict,
      capacity: Capacity = Capacity()
  ): Resource[F, StreamBackend[F, Fs2Streams[F]]] =
    resourceWithResolver(config, policy, InetAddress.getAllByName, capacity)

  private[upstream] def resourceWithResolver[F[_]: Async](
      config: HttpTlsConfig,
      policy: UrlPolicy,
      resolve: String => Array[InetAddress],
      capacity: Capacity = Capacity()
  ): Resource[F, StreamBackend[F, Fs2Streams[F]]] =
    resourceWithDiagnostics(config, policy, resolve, capacity).map(_._1)

  private[upstream] def resourceWithDiagnostics[F[_]: Async](
      config: HttpTlsConfig,
      policy: UrlPolicy,
      resolve: String => Array[InetAddress],
      capacity: Capacity
  ): Resource[F, (StreamBackend[F, Fs2Streams[F]], Diagnostics[F])] =
    for {
      finite <- clientResource(config, policy, resolve, capacity.finite, capacity.queuedPerPool)
      streaming <- clientResource(config, policy, resolve, capacity.streaming, capacity.queuedPerPool)
      owners <- Resource.eval(Ref.of[F, Int](0))
      supervisor <- Supervisor[F](await = false)
      closed <- Resource.make(Deferred[F, Unit])(_.complete(()).void)
    } yield {
      val backend = new DelegateBackend[F, Fs2Streams[F]](Http4sBackend.usingClient(finite._1))
        with StreamBackend[F, Fs2Streams[F]] {
        override def send[T](request: GenericRequest[T, Fs2Streams[F] & Effect[F]]): F[Response[T]] =
          SafeUrl.from(request.uri.toString, policy) match {
            case Left(_) => Async[F].raiseError(AddressRejected())
            case Right(_) =>
              Ref.of[F, Option[F[Unit]]](None).flatMap { running =>
                val client = if streamsResponse(request.response.delegate) then streaming._1 else finite._1
                val adapterAsync = supervisedAsync(supervisor, running, owners)
                val sent = closed.tryGet
                  .flatMap {
                    case Some(_) => Async[F].raiseError[Response[T]](TransportClosed())
                    case None =>
                      Http4sBackend
                        .usingClient(client)(using adapterAsync)
                        .send(request.followRedirects(false))
                  }
                  .race(closed.get *> Async[F].raiseError[Response[T]](TransportClosed()))
                  .map(_.merge)
                val bounded = request.options.readTimeout match {
                  case finite: FiniteDuration => sent.timeout(finite)
                  case _ => sent
                }
                bounded.guaranteeCase {
                  case cats.effect.kernel.Outcome.Succeeded(_) => Async[F].unit
                  case _ => running.get.flatMap(_.sequence_)
                }
              }
          }
      }
      (backend, Diagnostics(finite._2, streaming._2, owners.get))
    }

  final case class TransportClosed() extends RuntimeException("upstream transport closed") with NoStackTrace
  final case class CapacityExceeded()
      extends RuntimeException("upstream transport queue full")
      with NoStackTrace

  /** Route by the response algebra, including mapped and conditional streams, never URL or result casts.
    * Metadata is unknown at admission: any possibly streaming branch reserves streaming capacity. Source:
    * https://github.com/softwaremill/sttp/blob/v4.0.26/core/src/main/scala/sttp/client4/ResponseAs.scala
    */
  private[upstream] def streamsResponse(response: GenericResponseAs[?, ?]): Boolean = response match {
    case _: ResponseAsStream[?, ?, ?, ?] => true
    case _: ResponseAsStreamUnsafe[?, ?] => true
    case MappedResponseAs(raw, _, _) => streamsResponse(raw)
    case ResponseAsFromMetadata(conditions, default) =>
      conditions.exists(condition => streamsResponse(condition.responseAs)) || streamsResponse(default)
    case ResponseAsBoth(left, right) => streamsResponse(left) || streamsResponse(right)
    case _ => false
  }

  /** sttp 4.0.26 starts a detached response fiber. Intercept only that Async.start: supervise it for backend
    * shutdown and register its cancellation action atomically for send failure/cancellation. All other Async
    * primitives delegate unchanged. No response-body cast or extra exchange-owner fiber. Sources: sttp
    * v4.0.26 Http4sBackendBase.send; cats-effect v3.7.1 Async and Supervisor.
    */
  private def supervisedAsync[F[_]](
      supervisor: Supervisor[F],
      running: Ref[F, Option[F[Unit]]],
      owners: Ref[F, Int]
  )(using root: Async[F]): Async[F] = new Async[F] {
    export root.{
      pure,
      flatMap,
      tailRecM,
      raiseError,
      handleErrorWith,
      monotonic,
      realTime,
      forceR,
      uncancelable,
      onCancel,
      canceled,
      cede,
      ref,
      deferred,
      cont,
      executionContext,
      evalOn,
      suspend
    }
    override protected def sleep(time: FiniteDuration): F[Unit] = root.sleep(time: Duration)
    override def racePair[A, B](fa: F[A], fb: F[B]): F[Either[
      (cats.effect.kernel.Outcome[F, Throwable, A], Fiber[F, Throwable, B]),
      (Fiber[F, Throwable, A], cats.effect.kernel.Outcome[F, Throwable, B])
    ]] = root.racePair(fa, fb)
    override def start[A](fa: F[A]): F[Fiber[F, Throwable, A]] =
      root.uncancelable { _ =>
        for {
          registered <- Deferred[F, Unit]
          fiber <- supervisor.supervise(
            registered.get *> Resource.make(owners.update(_ + 1))(_ => owners.update(_ - 1)).use(_ => fa)
          )
          _ <- running.update(_.orElse(Some(fiber.cancel)))
          _ <- registered.complete(())
        } yield fiber
      }
  }

  /** No host, URL or resolver/provider message is exposed by a policy refusal. */
  final case class AddressRejected()
      extends RuntimeException("upstream address refused by URL policy")
      with NoStackTrace

  /** Blaze connects this exact resolved InetSocketAddress, but creates its SSLEngine from RequestKey. No
    * hostname is passed to the socket layer, so there is no second resolution / rebinding window. Source:
    * https://github.com/http4s/blaze/blob/v0.23.17/blaze-client/src/main/scala/org/http4s/blaze/client/Http1Support.scala
    */
  private[upstream] def destination(
      key: RequestKey,
      policy: UrlPolicy,
      resolve: String => Array[InetAddress]
  ): Either[Throwable, InetSocketAddress] =
    Try(resolve(key.authority.host.value).toList).toEither.flatMap { addresses =>
      if addresses.isEmpty || addresses.exists(address => !UrlPolicy.allowsAddress(address, policy)) then
        Left(AddressRejected())
      else {
        // Strip the resolver's hostname metadata, not the HTTP authority or TLS peer name.
        val address = InetAddress.getByAddress(addresses.head.getAddress)
        val port = key.authority.port.getOrElse(if key.scheme == org.http4s.Uri.Scheme.https then 443 else 80)
        Right(new InetSocketAddress(address, port))
      }
    }

  /** A prepared context plus the managers explicitly installed into it.
    *
    * The manager lists are package-visible so unit tests can prove replacement rather than merely prove that
    * `SSLContext.init` accepted a configuration. Empty trust managers mean the JVM default, not no trust.
    */
  final private[upstream] case class Prepared(
      context: SSLContext,
      usesSystemTrust: Boolean,
      trustManagers: List[TrustManager],
      keyManagers: List[KeyManager]
  )

  /** A startup failure whose text contains only a fixed component name.
    *
    * The original exception is deliberately not retained as a cause: file exceptions include configured
    * paths, and provider exceptions are free to include passwords, aliases or store implementation detail.
    */
  final case class InitializationFailure private[HttpTls] (component: String)
      extends RuntimeException(s"HTTP TLS $component could not be initialized")
      with NoStackTrace

  private[upstream] def prepared[F[_]: Async](config: HttpTlsConfig): F[Prepared] =
    if config == HttpTlsConfig.Default then
      guarded[F, Prepared]("default context")(
        Prepared(SSLContext.getDefault, usesSystemTrust = true, Nil, Nil)
      )
    else
      for {
        trustManagers <- config.truststore.traverse(loadTrustManagers[F]).map(_.toList.flatten)
        keyManagers <- config.keystore.traverse(loadKeyManagers[F]).map(_.toList.flatten)
        context <- guarded[F, SSLContext]("context") {
          val value = SSLContext.getInstance("TLS")
          value.init(arrayOrNull(keyManagers), arrayOrNull(trustManagers), SecureRandom())
          value
        }
      } yield Prepared(context, config.truststore.isEmpty, trustManagers, keyManagers)

  /** Admission precedes Blaze's masked borrow. No admitted load can fill its internal queue: total and
    * per-key limits both equal the external active budget, and queueing is disabled defensively. Idle
    * connections for other keys are evicted by PoolManager.borrow before opening a new connection. Source:
    * https://github.com/http4s/blaze/blob/v0.23.17/blaze-client/src/main/scala/org/http4s/blaze/client/PoolManager.scala
    */
  private def clientResource[F[_]: Async](
      config: HttpTlsConfig,
      policy: UrlPolicy,
      resolve: String => Array[InetAddress],
      active: Int,
      queued: Int
  ): Resource[F, (Client[F], F[Long])] =
    for {
      tls <- Resource.eval(prepared[F](config))
      slots <- Resource.eval(Semaphore[F](active.toLong))
      outstanding <- Resource.eval(Semaphore[F](active.toLong + queued.toLong))
      client <- BlazeClientBuilder[F]
        .withSslContext(tls.context)
        .withCheckEndpointAuthentication(true)
        .withCustomDnsResolver(key => destination(key, policy, resolve))
        .withRetries(0)
        .withMaxTotalConnections(active)
        .withMaxConnectionsPerRequestKey(_ => active)
        .withMaxWaitQueueLimit(0)
        .withRequestTimeout(Duration.Inf)
        .resource
    } yield {
      val admitted = Client[F] { request =>
        Resource.make(outstanding.tryAcquire.flatMap {
          case true => Async[F].unit
          case false => Async[F].raiseError[Unit](CapacityExceeded())
        })(_ => outstanding.release) *> slots.permit *> client.run(request)
      }
      (admitted, slots.count)
    }

  private def loadTrustManagers[F[_]: Async](config: HttpTrustStore): F[List[TrustManager]] =
    loadStore[F](config.material, config.password, config.format, "truststore").flatMap { store =>
      guarded[F, List[TrustManager]]("truststore") {
        val factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
        factory.init(store)
        factory.getTrustManagers.toList
      }.flatMap { managers =>
        val hasTrustAnchor = managers
          .collect { case manager: X509TrustManager => manager }
          .exists(_.getAcceptedIssuers.nonEmpty)
        if hasTrustAnchor then Async[F].pure(managers)
        else Async[F].raiseError(InitializationFailure("truststore"))
      }
    }

  private def loadKeyManagers[F[_]: Async](config: HttpKeyStore): F[List[KeyManager]] =
    loadStore[F](config.material, config.password, config.format, "keystore").flatMap { store =>
      guarded[F, Option[List[KeyManager]]]("keystore") {
        val keyPassword = config.keyPassword.value.toCharArray
        try
          Option.when(hasUsablePrivateKey(store, keyPassword)) {
            val factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
            factory.init(store, keyPassword)
            factory.getKeyManagers.toList
          }
        finally Arrays.fill(keyPassword, '\u0000')
      }.flatMap {
        case Some(managers) => Async[F].pure(managers)
        case None => Async[F].raiseError(InitializationFailure("keystore"))
      }
    }

  private def loadStore[F[_]: Async](
      material: HttpStoreMaterial,
      password: Secret[String],
      format: HttpStoreFormat,
      component: String
  ): F[KeyStore] =
    guarded[F, KeyStore](component) {
      val chars = password.value.toCharArray
      try {
        val store = KeyStore.getInstance(format.wireName)
        material match {
          case HttpStoreMaterial.Location(path) =>
            Using.resource(Files.newInputStream(Path.of(path)))(store.load(_, chars))
          case HttpStoreMaterial.Inline(base64) =>
            val bytes = Base64.getDecoder.decode(base64.value.filterNot(_.isWhitespace))
            try Using.resource(ByteArrayInputStream(bytes))(store.load(_, chars))
            finally Arrays.fill(bytes, 0.toByte)
        }
        store
      } finally Arrays.fill(chars, '\u0000')
    }

  private def hasUsablePrivateKey(store: KeyStore, password: Array[Char]): Boolean =
    store.aliases().asScala.exists { alias =>
      store.isKeyEntry(alias) &&
      store.getKey(alias, password).isInstanceOf[PrivateKey] &&
      Option(store.getCertificateChain(alias)).exists(_.nonEmpty)
    }

  private def arrayOrNull[A: reflect.ClassTag](values: List[A]): Array[A] =
    Option.when(values.nonEmpty)(values.toArray).orNull

  private def guarded[F[_]: Async, A](component: String)(value: => A): F[A] =
    Async[F].blocking(value).handleErrorWith {
      case failure: InitializationFailure => Async[F].raiseError(failure)
      case NonFatal(_) => Async[F].raiseError(InitializationFailure(component))
      case fatal => Async[F].raiseError(fatal)
    }
}
