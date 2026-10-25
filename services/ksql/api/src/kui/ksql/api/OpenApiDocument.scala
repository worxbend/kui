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

  /** `OpenApiDocument <path> [--check]`.
    *
    * Writing prints the path so a developer can see what changed; checking prints nothing on success, because
    * a check that is quiet when it passes is a check people leave switched on.
    */
  def main(args: Array[String]): Unit = {
    val path = args.headOption.getOrElse {
      Console.err.println("usage: OpenApiDocument <path to openapi.json> [--check]")
      sys.exit(2)
    }
    val check = args.contains("--check")
    val file = java.nio.file.Path.of(path)
    val expected = render

    if !check then {
      Option(file.getParent).foreach(java.nio.file.Files.createDirectories(_))
      java.nio.file.Files.writeString(file, expected)
      println(s"wrote $path")
    } else if !java.nio.file.Files.exists(file) then {
      Console.err.println(s"$path does not exist; run ./mill services.ksql.api.openApi")
      sys.exit(1)
    } else if java.nio.file.Files.readString(file) != expected then {
      Console.err.println(
        s"$path is out of date with the endpoints it is generated from; " +
          "run ./mill services.ksql.api.openApi and commit the result"
      )
      sys.exit(1)
    }
  }
}
