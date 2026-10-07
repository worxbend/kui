package kui.config

import java.net.InetAddress

/** Literal addresses only: never perform DNS on untrusted forwarding headers or proxy configuration. */
object IpLiteral {
  def canonical(raw: String): Option[String] = {
    val ipv4 = raw.matches("[0-9]{1,3}(\\.[0-9]{1,3}){3}") &&
      raw.split("\\.").forall(part => part.toInt <= 255)
    val ipv6 = raw.contains(":") && raw.matches("[0-9a-fA-F:.]+")
    if ipv4 || ipv6 then scala.util.Try(InetAddress.getByName(raw).getHostAddress).toOption
    else None
  }
}
