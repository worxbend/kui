package kui.gateway.api.openapi

import kui.http.OpenApiDocumentFile
import kui.kernel.ServiceId

/** Writes `docs/api/openapi.json` and `docs/api/openapi.browser.json`, and fails the build when either
  * committed copy has gone stale.
  *
  * A generated document that nobody regenerates is worse than none: an integrator reads it, believes it, and
  * builds against an API that changed three releases ago. `--check` is what makes the committed file
  * trustworthy, and it runs in CI on every change (PLAN §46).
  *
  * The document is generated for the **full** product -- every service KUI has a contract for -- and not for
  * whatever a particular deployment happens to have configured. The committed file describes the API, which
  * is a property of the release; a deployment that has not enabled a service still ships the same
  * documentation.
  *
  * Two files, one merge. The second is the *edge view* of the first -- the same endpoints with every header
  * the gateway strips from browsers removed -- and it is what the browser's TypeScript client is generated
  * from. [[BrowserProjection]] explains why the browser cannot use the first.
  */
object OpenApiDocument {

  /** The deployment-independent server entry. A committed document cannot know a deployment's hostname, so it
    * says what every KUI installation has in common and leaves the host to the reader.
    */
  val DefaultServer: String = "/"

  def documentedServices: List[ServiceId] =
    kui.gateway.api.routing.ServiceContracts.byService.keys.toList.sortBy(_.value)

  private def merged: sttp.apispec.openapi.OpenAPI =
    DocsRoutes
      .document[cats.effect.IO](documentedServices, List(DefaultServer))
      .fold(problem => sys.error(problem), identity)

  def render: String = DocsRoutes.render(merged)

  /** The same document with every header the edge strips removed: the source the browser's TypeScript client
    * is generated from. See [[BrowserProjection]] for why it cannot simply be [[render]].
    */
  def renderBrowser: String = DocsRoutes.render(BrowserProjection.project(merged))

  def main(args: Array[String]): Unit = {
    val targets = args.filterNot(_.startsWith("--")).toList
    val (target, browserTarget) = targets match {
      case service :: browser :: Nil => (service, browser)
      case _ =>
        Console.err.println(
          "usage: OpenApiDocument <path to openapi.json> <path to openapi.browser.json> [--check]"
        )
        sys.exit(2)
    }
    val check = args.contains("--check")

    // Both documents are written or checked in one run, from one merge. Two Mill tasks calling this twice
    // would generate the endpoint set twice for no benefit, and -- worse -- could in principle disagree.
    OpenApiDocumentFile.emit(target, render, check, "services.gateway.api")
    OpenApiDocumentFile.emit(browserTarget, renderBrowser, check, "services.gateway.api")
  }
}
