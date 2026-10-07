package kui.gateway.api.auth

import sttp.tapir.model.ServerRequest

import kui.config.IpLiteral

/** Trust X-Forwarded-For only across an explicitly configured chain, walking from the socket inward. */
private[auth] object ClientAddress {
  def of(request: ServerRequest, trustedProxies: Set[String]): String = {
    val peer = request.connectionInfo.remote
      .flatMap(remote => Option(remote.getAddress))
      .map(_.getHostAddress)
      .getOrElse("unknown")
    val headers = request.headers.filter(_.is("X-Forwarded-For")).map(_.value)
    resolve(peer, headers.toList, trustedProxies)
  }

  private[auth] def resolve(peer: String, forwarded: List[String], configured: Set[String]): String = {
    val trusted = configured.flatMap(IpLiteral.canonical)
    val remote = IpLiteral.canonical(peer).getOrElse("unknown")
    if !trusted.contains(remote) || forwarded.size != 1 then remote
    else {
      val raw = forwarded.head.split(",", -1).toList.map(_.trim)
      val parsed = raw.map(IpLiteral.canonical)
      if raw.size > 32 || parsed.exists(_.isEmpty) then remote
      else
        (parsed.flatten :+ remote).reverse
          .dropWhile(trusted.contains)
          .headOption
          .getOrElse(parsed.flatten.headOption.getOrElse(remote))
    }
  }
}
