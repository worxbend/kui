package kui.config

import java.net.{Inet6Address, InetAddress}

import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

import kui.testkit.KuiSuite

/** That an operator cannot point KUI at an address it must not call.
  *
  * `ARCHITECTURE.md` §14 is the rule; the reason is server-side request forgery. KUI fetches addresses
  * supplied in configuration, so a URL such as `http://169.254.169.254/` — the address a cloud instance uses
  * to hand out its own credentials — would turn a configuration field into a way of reading a private network
  * through KUI's own network position.
  */
final class UrlPolicySuite extends KuiSuite {

  private def accepted(raw: String, policy: UrlPolicy): Boolean =
    SafeUrl.from(raw, policy).isRight

  private val privateAddresses = List(
    "http://169.254.169.254/latest/meta-data/",
    "http://[::1]/",
    "http://10.0.0.1/",
    "http://192.168.1.1/",
    "http://172.16.0.1/",
    "http://127.0.0.1:8080/",
    "http://localhost:8080/"
  )

  privateAddresses.foreach { url =>
    test(s"Strict refuses $url and Dev accepts it") {
      assert(!accepted(url, UrlPolicy.Strict), s"$url was accepted under the strict policy")
      assert(accepted(url, UrlPolicy.Dev), s"$url was refused under the dev policy")
    }
  }

  test("a public host is accepted under both policies") {
    assert(accepted("https://registry.example.com:8081", UrlPolicy.Strict))
    assert(accepted("https://registry.example.com:8081", UrlPolicy.Dev))
  }

  /** The same two addresses as above, written the ways an attacker would write them.
    *
    * `2130706433` is `127.0.0.1` in plain decimal, `0x7f000001` the same number in hexadecimal and
    * `017700000001` in octal; `127.1` is the short form the C library has always accepted. The `::ffff:`
    * prefix is an IPv4-mapped IPv6 address, which every resolver unwraps back to the IPv4 address inside it.
    * Each of these reaches exactly the same host as the spelling above, so a check that refuses one and
    * accepts another is not a check at all.
    */
  private val disguisedAddresses = List(
    "http://2130706433:8080/",
    "http://0x7f000001:8080/",
    "http://017700000001:8080/",
    "http://127.1:8080/",
    "http://[::ffff:127.0.0.1]:8080/",
    "http://[::ffff:169.254.169.254]/latest/meta-data/",
    "http://2852039166/latest/meta-data/", // 169.254.169.254 in decimal
    "http://0xa000001/", // 10.0.0.1 in hexadecimal
    "http://[::ffff:0a00:0001]/" // 10.0.0.1 mapped into IPv6
  )

  disguisedAddresses.foreach { url =>
    test(s"Strict refuses $url, which is a loopback or metadata address in disguise") {
      assert(!accepted(url, UrlPolicy.Strict), s"$url was accepted under the strict policy")
    }
  }

  test("a public address written in an unusual form is still accepted") {
    // The disguise check must not swallow ordinary public addresses, in any spelling.
    assert(accepted("https://93.184.216.34/", UrlPolicy.Strict))
    assert(accepted("https://[2606:2800:220:1:248:1893:25c8:1946]/", UrlPolicy.Strict))
  }

  /** An IPv6 address object whose last four bytes are an IPv4 address.
    *
    * Built with `Inet6Address.getByAddress` because that is the only constructor that keeps the IPv6 form:
    * `InetAddress.getByName("::ffff:127.0.0.1")` hands back an `Inet4Address`, which is why a test written
    * with it never saw what the system resolver returns for a DNS name whose AAAA record is IPv4-mapped.
    */
  private def ipv6Embedding(prefix: Array[Int], ipv4: (Int, Int, Int, Int)): Inet6Address = {
    val (a, b, c, d) = ipv4
    Inet6Address.getByAddress(null, (prefix ++ Array(a, b, c, d)).map(_.toByte), 0)
  }

  private val ipv4Mapped: Array[Int] = Array.fill(10)(0) ++ Array(0xff, 0xff)
  private val ipv4Compatible: Array[Int] = Array.fill(12)(0)
  private val nat64WellKnown: Array[Int] = Array(0x00, 0x64, 0xff, 0x9b) ++ Array.fill(8)(0)

  test("an IPv6 answer that embeds a loopback, private or metadata IPv4 address is refused under Strict") {
    val internal =
      List((169, 254, 169, 254), (127, 0, 0, 1), (10, 0, 0, 1), (192, 168, 1, 1), (100, 64, 0, 1))
    for {
      prefix <- List(ipv4Mapped, ipv4Compatible, nat64WellKnown)
      ipv4 <- internal
    } {
      val address = ipv6Embedding(prefix, ipv4)
      assert(!UrlPolicy.allowsAddress(address, UrlPolicy.Strict), s"$address was allowed under Strict")
      assert(UrlPolicy.allowsAddress(address, UrlPolicy.Dev), s"$address was refused under Dev")
    }
  }

  test("an IPv4-mapped loopback answer needs the loopback permission, not the private one") {
    // The socket layer dials `::ffff:127.0.0.1` as `127.0.0.1`, so it is the loopback rule that applies.
    val loopback = ipv6Embedding(ipv4Mapped, (127, 0, 0, 1))
    assert(UrlPolicy.allowsAddress(loopback, UrlPolicy.Strict.copy(allowLoopback = true)))
    assert(!UrlPolicy.allowsAddress(loopback, UrlPolicy.Strict.copy(allowPrivate = true)))
  }

  test("an IPv6 answer that embeds a public IPv4 address is still allowed under Strict") {
    List(ipv4Mapped, nat64WellKnown).foreach { prefix =>
      val address = ipv6Embedding(prefix, (93, 184, 216, 34))
      assert(UrlPolicy.allowsAddress(address, UrlPolicy.Strict), s"$address was refused under Strict")
    }
    assert(
      UrlPolicy.allowsAddress(InetAddress.getByName("2606:2800:220:1:248:1893:25c8:1946"), UrlPolicy.Strict)
    )
  }

  test("an IPv4-compatible or NAT64 literal of an internal address is refused in configuration too") {
    List(
      "http://[::169.254.169.254]/latest/meta-data/",
      "http://[::a9fe:a9fe]/",
      "http://[64:ff9b::169.254.169.254]/",
      "http://[64:ff9b::a00:1]/"
    ).foreach(url => assert(!accepted(url, UrlPolicy.Strict), s"$url was accepted under the strict policy"))
    assert(accepted("https://[64:ff9b::5db8:d822]/", UrlPolicy.Strict))
  }

  test("a host name that merely starts with a private prefix is not mistaken for one") {
    // `fdn.example.com` starts with the letters of the IPv6 unique-local prefix, and `100.example`
    // with the carrier-grade one. Both are ordinary public names.
    assert(accepted("https://fdn.example.com", UrlPolicy.Strict))
    assert(accepted("https://100.example.com", UrlPolicy.Strict))
  }

  test("a scheme other than http or https is refused under every policy") {
    // Dev relaxes which *addresses* may be called, never which schemes. `ARCHITECTURE.md` §14
    // allows http and https and nothing else, and a policy that relaxed it for developers would
    // leave the production path as the one nobody ever exercises.
    List("ftp://x/", "file:///etc/passwd", "jar:file:///x", "gopher://x/").foreach { url =>
      assert(!accepted(url, UrlPolicy.Strict), s"$url was accepted under the strict policy")
      assert(!accepted(url, UrlPolicy.Dev), s"$url was accepted under the dev policy")
    }
  }

  test("credentials in the URL are refused under every policy") {
    assert(!accepted("http://user:pass@example.com/", UrlPolicy.Strict))
    assert(!accepted("http://user:pass@example.com/", UrlPolicy.Dev))
    assert(!accepted("http://user@example.com/", UrlPolicy.Dev))
  }

  test("a URL with no host is refused") {
    assert(!accepted("http:///path", UrlPolicy.Dev))
    assert(!accepted("not-a-url", UrlPolicy.Dev))
  }

  test("the failure message names the field and does not echo an empty expectation") {
    SafeUrl.from("ftp://x/", UrlPolicy.Strict) match {
      case Right(_) => fail("ftp was accepted")
      case Left(error) =>
        assertEquals(error.fieldName, "url")
        assert(error.message.contains("http or https"), error.message)
    }
  }

  /** The values that must not relax the policy, and the one that must.
    *
    * There was no switch at all before this: `UrlPolicy.Strict` was the default in the loader and no
    * composition root passed anything else, so a developer running the gateway and a service as two local
    * processes, or a deployment with an OTLP collector on `http://localhost:4317`, simply could not start --
    * while both the operations guide and the design note claimed a development relaxation existed.
    */
  test("only the exact value true relaxes the policy") {
    assertEquals(UrlPolicy.fromEnv(Map(UrlPolicy.AllowPrivateUpstreams -> "true")), UrlPolicy.Dev)
    assertEquals(UrlPolicy.fromEnv(Map(UrlPolicy.AllowPrivateUpstreams -> " TRUE ")), UrlPolicy.Dev)

    List("", "1", "yes", "false", "ture", "on").foreach { value =>
      assertEquals(
        UrlPolicy.fromEnv(Map(UrlPolicy.AllowPrivateUpstreams -> value)),
        UrlPolicy.Strict,
        s"'$value' relaxed the policy"
      )
    }
  }

  test("an environment that does not mention the variable gets the strict policy") {
    assertEquals(UrlPolicy.fromEnv(Map.empty), UrlPolicy.Strict)
    assertEquals(UrlPolicy.fromEnv(Map("PATH" -> "/usr/bin")), UrlPolicy.Strict)
  }

  private val hosts: Gen[String] =
    Gen.oneOf(
      "example.com",
      "registry.internal",
      "10.0.0.1",
      "localhost",
      "169.254.169.254",
      "2130706433",
      "0x7f000001",
      "[::ffff:169.254.169.254]"
    )

  private val userInfos: Gen[String] = Gen.oneOf("", "u@", "u:p@")

  private val schemes: Gen[String] = Gen.oneOf("http", "https", "ftp", "file")

  property("no accepted URL carries credentials, under either policy") {
    forAll(schemes, userInfos, hosts, Gen.oneOf(UrlPolicy.Strict, UrlPolicy.Dev)) {
      (scheme, userInfo, host, policy) =>
        val raw = s"$scheme://$userInfo$host/path"
        SafeUrl.from(raw, policy).isLeft || userInfo.isEmpty
    }
  }

  property("no accepted URL under the strict policy is a loopback or private address") {
    forAll(schemes, hosts) { (scheme, host) =>
      val raw = s"$scheme://$host/path"
      val isPrivate = host != "example.com" && host != "registry.internal"
      SafeUrl.from(raw, UrlPolicy.Strict).isLeft || !isPrivate
    }
  }
}
