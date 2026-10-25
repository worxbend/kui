package kui.ksql.api

import io.circe.syntax.*
import sttp.apispec.openapi.circe.*

/** Writing this service's OpenAPI document to a file, and checking that the committed one is current.
  *
  * The document is generated from the same endpoint values the server is built from, so it cannot describe a
  * path the service does not serve. It is *committed* rather than only generated because the gateway merges
  * every service's document into one (ADR-003), and a merge that had to build eleven services first would be
  * a merge nobody could run.
  *
  * A committed generated file is only trustworthy if something fails when it goes stale, which is what
  * `--check` is for: `./mill services.ksql.api.openApiCheck` fails when the file on disk no longer matches
  * the endpoints, and CI runs it.
  */
object OpenApiDocument {

  /** The document, as the exact text that belongs on disk: two-space indented JSON with a trailing newline,
    * which is what every other JSON file in this repository looks like.
    *
    * `cats.Id` is the effect the document is rendered for, and it is arbitrary: a Tapir endpoint's *shape*
    * does not depend on its effect, and the push query's body is described as an event stream whichever one
    * is supplied. `services/alerts` renders its own stream endpoint the same way.
    */
  def render: String = KsqlApi.openApi[cats.Id].asJson.deepDropNullValues.spaces2 + "\n"

  /** `OpenApiDocument <path> [--check]`: see [[kui.http.OpenApiDocumentFile]]. */
  def main(args: Array[String]): Unit =
    kui.http.OpenApiDocumentFile.main(args, "services.ksql.api", render)
}
