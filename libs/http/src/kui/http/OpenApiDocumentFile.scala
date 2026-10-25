package kui.http

import java.nio.file.{Files, Path}

/** The command-line half of every service's `OpenApiDocument`: write the rendered document to a file, or with
  * `--check` fail when the committed file no longer matches it.
  *
  * Each service supplies its own rendering and the Mill module that regenerates it, which is the only part
  * that differs; the exit codes and messages below are what `./mill __.openApiCheck` and its readers depend
  * on, so they are written once rather than ten times.
  *
  * Writing prints the path so a developer can see what changed; checking prints nothing on success, because a
  * check that is quiet when it passes is a check people leave switched on.
  */
object OpenApiDocumentFile {

  /** `OpenApiDocument <path> [--check]`, for a service that commits one document. `module` is the Mill module
    * whose `openApi` command regenerates it, named in the failure so the fix is one copy-paste away.
    */
  def main(args: Array[String], module: String, render: => String): Unit = {
    val path = args.headOption.getOrElse {
      Console.err.println("usage: OpenApiDocument <path to openapi.json> [--check]")
      sys.exit(2)
    }
    emit(path, render, args.contains("--check"), module)
  }

  /** Writes `expected` to `target`, or checks that `target` already holds exactly `expected`, exiting 1 when
    * it is missing or stale.
    */
  def emit(target: String, expected: String, check: Boolean, module: String): Unit = {
    val file = Path.of(target)
    if !check then {
      Option(file.getParent).foreach(Files.createDirectories(_))
      Files.writeString(file, expected)
      println(s"wrote $target")
    } else if !Files.exists(file) then {
      Console.err.println(s"$target does not exist; run ./mill $module.openApi")
      sys.exit(1)
    } else if Files.readString(file) != expected then {
      Console.err.println(
        s"$target is out of date with the endpoints it is generated from; " +
          s"run ./mill $module.openApi and commit the result"
      )
      sys.exit(1)
    }
  }
}
