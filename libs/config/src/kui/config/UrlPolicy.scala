package kui.config

import java.net.{InetAddress, URI}
import java.util.Locale

import scala.util.Try

import kui.kernel.ValidationError

/** What a URL is allowed to look like before KUI will call it.
  *
  * KUI calls addresses that an operator typed into a configuration file: a schema registry, a Kafka Connect
  * cluster, an OTLP collector. That makes every one of them a server-side request forgery risk — the classic
  * attack is to point KUI at `http://169.254.169.254/`, the address a cloud instance uses to hand out its own
  * credentials, and read the answer back through KUI. `ARCHITECTURE.md` §14 therefore restricts outbound
  * URLs, and this type is that restriction as data so the same rule can be applied strictly in production and
  * loosely in development.
  *
  * @param allowLoopback
  *   whether `localhost`, `127.0.0.1` and `[::1]` are acceptable hosts
  * @param allowPrivate
  *   whether addresses that are not routable on the public internet — the private ranges (`10.0.0.0/8`,
  *   `172.16.0.0/12`, `192.168.0.0/16`), link-local addresses (`169.254.0.0/16`, `fe80::/10`) and the cloud
  *   metadata addresses that live inside them — are acceptable
  * @param allowedSchemes
  *   the URL schemes that may be used, lowercase
  */
final case class UrlPolicy(allowLoopback: Boolean, allowPrivate: Boolean, allowedSchemes: Set[String])

object UrlPolicy {

  /** The scheme allow-list, which is the same under every policy.
    *
    * `ARCHITECTURE.md` §14 says `http` and `https` only, with no exception for development: a `file://` or
    * `ftp://` upstream is never a legitimate KUI configuration, and relaxing it for developers would mean the
    * strict path is the one nobody exercises.
    */
  val HttpSchemes: Set[String] = Set("http", "https")

  /** The production default: public HTTP(S) addresses only. */
  val Strict: UrlPolicy = UrlPolicy(allowLoopback = false, allowPrivate = false, HttpSchemes)

  /** Development and tests, where every upstream is a container on `localhost` or on a private Docker
    * network.
    */
  val Dev: UrlPolicy = UrlPolicy(allowLoopback = true, allowPrivate = true, HttpSchemes)

  /** The environment variable that relaxes the address rule, and nothing else.
    *
    * It is an environment variable rather than a configuration key on purpose, in the same style as
    * `KUI_ALLOW_UNSIGNED`: a security relaxation should be deliberate, visible in the process's environment
    * where an operator or an auditor can see it in one place, and impossible to arrive by accident inside a
    * large YAML file that somebody copied.
    */
  val AllowPrivateUpstreams: String = "KUI_ALLOW_PRIVATE_UPSTREAMS"

  /** [[Dev]] when `KUI_ALLOW_PRIVATE_UPSTREAMS` is exactly `true`, [[Strict]] otherwise.
    *
    * Without this there was no switch at all: every composition root called the loader with the default, so a
    * loopback or private-network upstream could not be configured anywhere -- not for two local processes
    * talking to each other, not for an OTLP collector running as a sidecar on `http://localhost:4317`, and
    * not for a Kubernetes ClusterIP such as `http://10.96.4.7:8080`. The documentation described a
    * development relaxation that did not exist.
    *
    * Anything other than `true` -- unset, empty, `1`, `yes`, a typo -- leaves the strict policy in place,
    * because a security control must not be switched off by a value nobody meant as an affirmative.
    */
  def fromEnv(env: Map[String, String]): UrlPolicy =
    if env.get(AllowPrivateUpstreams).exists(_.trim.equalsIgnoreCase("true")) then Dev else Strict

  /** Check the numeric address the transport will actually dial, without performing DNS. Configuration-time
    * name validation is not an SSRF boundary: DNS can change after it.
    */
  def allowsAddress(address: InetAddress, policy: UrlPolicy): Boolean =
    if address.isLoopbackAddress || address.isAnyLocalAddress then policy.allowLoopback
    else !SafeUrl.addressIsNotPubliclyRoutable(address) || policy.allowPrivate

  given CanEqual[UrlPolicy, UrlPolicy] = CanEqual.derived
}

/** A URL string that has been checked against a [[UrlPolicy]].
  *
  * It is an opaque type over `String` so that the check cannot be skipped: the only way to obtain one is
  * [[SafeUrl.from]], and every function that takes a `SafeUrl` therefore knows the policy was applied.
  * Nothing is normalised or rewritten — the value is the operator's own text, so that what appears in a log
  * or an error message is what they wrote.
  */
opaque type SafeUrl = String

object SafeUrl {

  private val Field: String = "url"

  /** Hosts that always mean "this machine", whatever the DNS says. */
  private val LoopbackNames: Set[String] = Set("localhost", "127.0.0.1", "::1", "[::1]", "0.0.0.0")

  /** Checks `raw` against `policy`.
    *
    * The failure is a kernel `ValidationError` rather than a bespoke type because the caller is always either
    * the configuration loader (which turns it into one line of a startup error) or the upstream client
    * factory (which turns it into a `KUI-VALIDATION` response). Both already know how to render one.
    */
  def from(raw: String, policy: UrlPolicy): Either[ValidationError, SafeUrl] =
    for {
      uri <- parse(raw)
      _ <- schemeIsAllowed(uri, raw, policy)
      _ <- noUserInfo(uri)
      host <- hostOf(uri, raw)
      _ <- hostIsAllowed(host, raw, policy)
    } yield raw

  /** Builds a `SafeUrl` without checking. Reserved for tests and for literals in this repository that are
    * known-good; production code paths go through [[from]].
    */
  def unsafe(raw: String): SafeUrl = raw

  extension (u: SafeUrl) def value: String = u

  given Ordering[SafeUrl] = Ordering.String
  given CanEqual[SafeUrl, SafeUrl] = CanEqual.derived

  private def parse(raw: String): Either[ValidationError, URI] =
    Try(new URI(raw)).toEither.left.map(_ =>
      ValidationError.Format(Field, "an absolute http or https URL", raw)
    )

  private def schemeIsAllowed(
      uri: URI,
      raw: String,
      policy: UrlPolicy
  ): Either[ValidationError, Unit] =
    Option(uri.getScheme).map(_.toLowerCase(Locale.ROOT)) match {
      case Some(scheme) if policy.allowedSchemes.contains(scheme) => Right(())
      case _ =>
        val expected = policy.allowedSchemes.toList.sorted.mkString(" or ")
        Left(ValidationError.Format(Field, expected, raw))
    }

  private def noUserInfo(uri: URI): Either[ValidationError, Unit] =
    if Option(uri.getUserInfo).isEmpty && Option(uri.getRawUserInfo).isEmpty then Right(())
    else
      Left(
        ValidationError.Invariant(
          Field,
          "a URL must not carry a username or password; put credentials in their own configuration " +
            "keys so they can be redacted"
        )
      )

  private def hostOf(uri: URI, raw: String): Either[ValidationError, String] =
    Option(uri.getHost).filter(_.nonEmpty) match {
      case Some(host) => Right(host.toLowerCase(Locale.ROOT))
      case None => Left(ValidationError.Format(Field, "a URL with a host name", raw))
    }

  private def hostIsAllowed(
      host: String,
      raw: String,
      policy: UrlPolicy
  ): Either[ValidationError, Unit] =
    if isLoopback(host) && !policy.allowLoopback then
      Left(
        ValidationError.Invariant(
          Field,
          s"'$raw' points at this machine; a loopback address is only allowed in development"
        )
      )
    else if !isLoopback(host) && isNotPubliclyRoutable(host) && !policy.allowPrivate then
      Left(
        ValidationError.Invariant(
          Field,
          s"'$raw' is a private, link-local or cloud-metadata address; those are refused so that " +
            "a mistyped or hostile URL cannot make KUI read a network's internal endpoints"
        )
      )
    else Right(())

  private def isLoopback(host: String): Boolean =
    LoopbackNames.contains(host) || host.startsWith("127.") ||
      literalAddress(host).exists(address => address.isLoopbackAddress || address.isAnyLocalAddress)

  /** Private ranges, link-local ranges (which contain the cloud metadata addresses) and IPv6 unique-local
    * addresses.
    *
    * Two layers, because neither alone is enough. The textual prefixes catch a host *name* that happens to be
    * spelled like a private address, and they never depend on DNS, so a configuration file validates the same
    * way on every machine. The numeric layer catches the spellings of the same address that no prefix match
    * can see: `2130706433`, `0x7f000001`, `017700000001` and `[::ffff:169.254.169.254]` all reach exactly the
    * same host as `127.0.0.1` and `169.254.169.254`, and the operating system's resolver accepts every one of
    * them. Only address *literals* are parsed here — a name is never resolved.
    */
  private def isNotPubliclyRoutable(host: String): Boolean =
    isLoopback(host) ||
      host.startsWith("10.") ||
      host.startsWith("192.168.") ||
      host.startsWith("169.254.") ||
      isPrivateIpv6(host) ||
      isCarrierGradeOrPrivate172(host) ||
      literalAddress(host).exists(addressIsNotPubliclyRoutable)

  private[config] def addressIsNotPubliclyRoutable(address: InetAddress): Boolean = {
    val bytes = address.getAddress
    val carrierGrade =
      bytes.length == 4 && (bytes(0) & 0xff) == 100 && (bytes(1) & 0xff) >= 64 && (bytes(1) & 0xff) <= 127
    val uniqueLocalIpv6 = bytes.length == 16 && (bytes(0) & 0xfe) == 0xfc
    address.isLoopbackAddress ||
    address.isAnyLocalAddress ||
    address.isMulticastAddress ||
    address.isLinkLocalAddress ||
    address.isSiteLocalAddress ||
    carrierGrade ||
    uniqueLocalIpv6
  }

  /** IPv6 link-local (`fe80::/10`) and unique-local (`fc00::/7`) prefixes.
    *
    * The `contains(':')` guard matters: without it a perfectly ordinary host name such as `fdn.example.com`
    * would be refused for starting with the letters of a private prefix.
    */
  private def isPrivateIpv6(host: String): Boolean =
    host.contains(':') &&
      (host.startsWith("fe80:") || host.startsWith("fd") || host.startsWith("fc"))

  private def isCarrierGradeOrPrivate172(host: String): Boolean =
    host.split('.').toList match {
      case first :: second :: _ :: _ :: Nil =>
        (first, second.toIntOption) match {
          case ("172", Some(octet)) => octet >= 16 && octet <= 31
          case ("100", Some(octet)) => octet >= 64 && octet <= 127
          case _ => false
        }
      case _ => false
    }

  /** The address `host` denotes, if and only if `host` is an address literal.
    *
    * A name is never passed to the resolver: `getByName` would perform a DNS lookup, which would make a
    * configuration file valid on one machine and invalid on another, and would let whoever controls the name
    * decide the answer. An IPv6 literal is recognised by the colon, which no host name may contain, so
    * `getByName` is safe for it. An IPv4 literal is parsed here rather than handed to the JDK, because a
    * string such as `cafe.dead` is made only of hexadecimal letters and dots and is nevertheless a name.
    */
  private def literalAddress(host: String): Option[InetAddress] =
    if host.contains(':') then Try(InetAddress.getByName(host.stripPrefix("[").stripSuffix("]"))).toOption
    else ipv4Literal(host).flatMap(bytes => Try(InetAddress.getByAddress(bytes)).toOption)

  /** `inet_aton` semantics: one to four parts, each decimal, hexadecimal (`0x…`) or octal (`0…`), with the
    * last part supplying every byte the earlier parts did not. This is what the C library, and therefore
    * every operating system resolver, accepts — `2130706433`, `0x7f.1` and `127.1` are all `127.0.0.1`.
    */
  private def ipv4Literal(host: String): Option[Array[Byte]] = {
    val parts = host.split("\\.", -1).toList
    if parts.isEmpty || parts.sizeIs > 4 then None
    else
      allParts(parts).flatMap { values =>
        val leading = values.init
        val trailing = values.last
        val trailingLimit = 1L << (8 * (4 - leading.size))
        if leading.exists(_ > 255L) || trailing >= trailingLimit then None
        else {
          val packed = leading.zipWithIndex.foldLeft(trailing) { case (acc, (value, index)) =>
            acc | (value << (8 * (3 - index)))
          }
          Some(
            Array(
              ((packed >> 24) & 0xff).toByte,
              ((packed >> 16) & 0xff).toByte,
              ((packed >> 8) & 0xff).toByte,
              (packed & 0xff).toByte
            )
          )
        }
      }
  }

  private def unsignedPart(part: String): Option[Long] =
    if part.isEmpty then None
    else if part.startsWith("0x") || part.startsWith("0X") then digits(part.drop(2), 16)
    else if part.startsWith("0") && part.length > 1 then digits(part.drop(1), 8)
    else digits(part, 10)

  private def digits(text: String, radix: Int): Option[Long] =
    if text.isEmpty || text.length > 11 then None
    else Try(java.lang.Long.parseLong(text, radix)).toOption.filter(_ >= 0L)

  /** Every part parsed, or nothing at all if any one of them is not a number. */
  private def allParts(parts: List[String]): Option[List[Long]] =
    parts.foldRight(Option(List.empty[Long]))((part, accumulated) =>
      for {
        rest <- accumulated
        value <- unsignedPart(part)
      } yield value :: rest
    )
}
