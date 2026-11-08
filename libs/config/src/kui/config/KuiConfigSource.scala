package kui.config

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.Locale

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.util.Try
import scala.util.matching.Regex

import cats.data.{NonEmptyList, ValidatedNel}
import cats.effect.kernel.Async
import cats.syntax.all.*
import ciris.{ConfigError, ConfigKey, ConfigValue}
import io.circe.Json

import kui.kernel.cluster.{AdminTuning, BootstrapServers, ClientProperties}
import kui.kernel.search.SearchMode
import kui.kernel.serde.SerdeName
import kui.kernel.{ClusterId, ConnectName, Host, PageSize, Port, PositiveInt, Secret, ServiceId}
import kui.security.masking.MaskingRule
import kui.security.rbac.RbacPolicy

/** Loads [[KuiConfig]] from the command line, the environment and YAML files.
  *
  * ==Precedence==
  *
  * Command-line flags beat environment variables, which beat YAML files, which beat the defaults baked into
  * this file. The chain is expressed once, in [[Layers.candidates]], so no field can accidentally get a
  * different order from its neighbour.
  *
  * ==Reporting every problem==
  *
  * A load either produces a `KuiConfig` or produces *every* [[ConfigProblem]] it found. It never stops at the
  * first one and it never returns a half-valid configuration: a gateway that boots with three of its four
  * upstreams configured is harder to diagnose than one that refused to start and said which key was wrong.
  *
  * ==Key naming==
  *
  * Every key has one dotted name (`kui.server.port`) and one derived environment name (`KUI_SERVER_PORT`):
  * uppercase, with `.` and `-` both becoming `_`. Command-line flags use the dotted name, with the `kui.`
  * prefix optional: `--kui.server.port=9999` and `--server.port=9999` are the same flag. `--config <path>`
  * adds a YAML file to the ones passed in by the caller.
  *
  * Repeated sections use a numeric or named segment in the same dotted namespace, so there is only ever one
  * way to spell a key:
  *
  * {{{
  * kui.gateway.services.cluster.url        KUI_GATEWAY_SERVICES_CLUSTER_URL
  * kui.gateway.principalKeys.0.kid         KUI_GATEWAY_PRINCIPALKEYS_0_KID
  * }}}
  */
object KuiConfigSource {

  /** Loads the configuration with the documented precedence.
    *
    * @param args
    *   the process's command-line arguments, unfiltered
    * @param files
    *   YAML files in increasing order of precedence, so a later file overrides an earlier one. Files named by
    *   `--config` flags are appended to this list and therefore win over it.
    * @param policy
    *   the URL policy applied to every URL-shaped key. Production leaves it at [[UrlPolicy.Strict]]; Docker
    *   Compose and the test suites pass [[UrlPolicy.Dev]] because every upstream there is on loopback or a
    *   private network.
    */
  def load[F[_]: Async](
      args: List[String],
      files: List[Path],
      policy: UrlPolicy = UrlPolicy.Strict
  ): F[Either[ConfigErrors, KuiConfig]] =
    Async[F].delay(sys.env).flatMap(environment => loadFrom[F](args, files, environment, policy))

  /** The same load, with the environment supplied rather than read from the process.
    *
    * This is the seam the suites use. Reading `sys.env` inside the loader would make the
    * environment-precedence tests depend on variables being set outside the build, which is both unreliable
    * and impossible to arrange for more than one case per run.
    */
  def loadFrom[F[_]: Async](
      args: List[String],
      files: List[Path],
      env: Map[String, String],
      policy: UrlPolicy = UrlPolicy.Strict
  ): F[Either[ConfigErrors, KuiConfig]] = {
    val arguments = CommandLine.parse(args)
    for {
      documents <- readAll(files ++ arguments.configFiles)
      result <- documents match {
        case Left(problems) => Async[F].pure(problems.invalid[KuiConfig])
        case Right(loaded) =>
          val layers = Layers(arguments.flags, env, loaded)
          decode(layers, policy).flatMap(resolveSecrets(_, env))
      }
    } yield result.toEither.leftMap(ConfigErrors.apply)
  }

  // ---------------------------------------------------------------------------------------------
  // Reading the layers
  // ---------------------------------------------------------------------------------------------

  private type Problems[A] = ValidatedNel[ConfigProblem, A]

  private def readAll[F[_]: Async](
      files: List[Path]
  ): F[Either[NonEmptyList[ConfigProblem], List[Document]]] =
    files.traverse(readOne[F]).map { attempts =>
      attempts.separate match {
        case (Nil, documents) => Right(documents)
        case (first :: rest, _) => Left(NonEmptyList(first, rest))
      }
    }

  private def readOne[F[_]: Async](path: Path): F[Either[ConfigProblem, Document]] =
    Async[F]
      .blocking(new String(Files.readAllBytes(path), StandardCharsets.UTF_8))
      .attempt
      .map {
        case Left(_) =>
          Left(ConfigProblem("kui", "could not be read", ConfigSourceName.File(path.toString)))
        case Right(text) =>
          io.circe.yaml.parser.parse(text) match {
            case Left(failure) =>
              Left(
                ConfigProblem(
                  "kui",
                  s"is not valid YAML: ${firstLineOf(failure.message)}",
                  ConfigSourceName.File(path.toString)
                )
              )
            case Right(json) => Right(Document(path.toString, json))
          }
      }

  /** The first line of a parser message, and nothing after it.
    *
    * The YAML parser renders a failure as a sentence naming the line and column, followed by the source line
    * it choked on. That echoed source line is the problem: KUI's promise is that a secret never reaches a log
    * line or an error message, and it keeps that promise with the `Secret` type -- but a parse failure
    * happens before anything is decoded, so nothing is a `Secret` yet and the redaction in `problemFor`
    * cannot apply. An unclosed quote on the line holding a signing key would print the key into `docker logs`
    * for anyone with log access. The first line still says exactly where to look.
    */
  private def firstLineOf(message: String): String =
    message.linesIterator.map(_.trim).find(_.nonEmpty).getOrElse("the document could not be parsed")

  /** One parsed YAML file. */
  final private case class Document(path: String, json: Json)

  /** The command line, split into the flags it sets and the files it names. */
  final private case class CommandLine(flags: Map[String, String], configFiles: List[Path])

  private object CommandLine {

    /** Accepts `--key=value`, `--key value`, `--config <path>` and `--config=<path>`; ignores anything else,
      * because a process may have arguments that are none of this loader's business.
      */
    def parse(args: List[String]): CommandLine = {
      val (flags, paths) = fold(args, Map.empty, Nil)
      CommandLine(flags, paths.reverse)
    }

    @annotation.tailrec
    private def fold(
        remaining: List[String],
        flags: Map[String, String],
        paths: List[Path]
    ): (Map[String, String], List[Path]) =
      remaining match {
        case Nil => (flags, paths)

        case "--config" :: path :: rest =>
          fold(rest, flags, Path.of(path) :: paths)

        // Before the generic `--key=value` case below, which would otherwise turn `--config=/etc/kui.yaml`
        // into a flag named `kui.config` that nothing reads. Flags are not checked against the recognised
        // key names, so that flag was dropped without a word and the process started on its defaults with
        // the operator's file unread. Every other key accepts both spellings, so `--config=` is the
        // natural thing to type.
        case flag :: rest if flag.startsWith(ConfigFlag) =>
          fold(rest, flags, Path.of(flag.drop(ConfigFlag.length)) :: paths)

        case flag :: rest if flag.startsWith("--") && flag.contains('=') =>
          val (name, value) = flag.drop(2).span(_ != '=')
          fold(rest, flags.updated(canonical(name), value.drop(1)), paths)

        case flag :: value :: rest if flag.startsWith("--") && !value.startsWith("--") =>
          fold(rest, flags.updated(canonical(flag.drop(2)), value), paths)

        case _ :: rest => fold(rest, flags, paths)
      }

    private val ConfigFlag: String = "--config="

    /** `--server.port` and `--kui.server.port` name the same key. */
    private def canonical(name: String): String =
      if name.startsWith("kui.") then name else s"kui.$name"
  }

  /** The three configured layers plus the derived environment names, with the precedence order in one place.
    */
  final private case class Layers(
      cli: Map[String, String],
      env: Map[String, String],
      files: List[Document]
  ) {

    /** Every value supplied for `key`, highest precedence first. */
    def candidates(key: String): List[(ConfigSourceName, String)] = {
      val fromCli = cli.get(key).map(ConfigSourceName.Cli -> _)
      val fromEnv = env.get(Layers.envName(key)).map(ConfigSourceName.Env -> _)
      val fromFiles = files.reverse.flatMap(document =>
        Layers.at(document.json, key).map(ConfigSourceName.File(document.path) -> _)
      )
      fromCli.toList ++ fromEnv.toList ++ fromFiles
    }

    def first(key: String): Option[(ConfigSourceName, String)] = candidates(key).headOption

    /** Keys that exist under `prefix` in any layer, used to discover map and list members whose names the
      * model cannot know in advance (the configured services, the signing keys).
      */
    def childrenOf(prefix: String): Set[String] = {
      val dotted = s"$prefix."
      val fromCli = cli.keySet.filter(_.startsWith(dotted)).map(_.drop(dotted.length).takeWhile(_ != '.'))
      val envPrefix = s"${Layers.envName(prefix)}_"
      val fromEnv = env.keySet
        .filter(_.startsWith(envPrefix))
        .map(_.drop(envPrefix.length))
        // The suffix after the member name is one of the fixed leaf names, so the member name is
        // everything up to the last underscore. That is what lets `SCHEMA_REGISTRY_URL` mean the
        // service `schema-registry` rather than the service `schema` with a key `registry_url`.
        .flatMap(rest => Option.when(rest.contains('_'))(rest.substring(0, rest.lastIndexOf('_'))))
        .map(_.toLowerCase(Locale.ROOT).replace('_', '-'))
      val fromFiles = files.flatMap(document => Layers.membersOf(document.json, prefix)).toSet
      fromCli ++ fromEnv ++ fromFiles
    }

    /** Direct map members under `prefix` when their values may have nested, fixed-shape leaves.
      *
      * Environment names flatten both member IDs and leaf paths with underscores, so splitting at the last
      * underscore is only valid for a one-segment leaf. Match the known leaf suffix instead: for example,
      * `..._PRODUCTION_EU_AUTH_TYPE` is source `production-eu` plus `auth.type`, not source
      * `production-eu-auth` plus `type`.
      */
    def mapMembersOf(prefix: String, envLeafPaths: Set[String]): Set[String] = {
      val dotted = s"$prefix."
      val fromCli = cli.keySet.filter(_.startsWith(dotted)).map(_.drop(dotted.length).takeWhile(_ != '.'))
      val envPrefix = s"${Layers.envName(prefix)}_"
      val suffixes = envLeafPaths.toList
        .map(path => s"_${Layers.envName(path)}")
        .sortBy(suffix => -suffix.length)
      val fromEnv = env.keySet
        .filter(_.startsWith(envPrefix))
        .flatMap { key =>
          val rest = key.drop(envPrefix.length)
          suffixes.collectFirst {
            case suffix if rest.endsWith(suffix) && rest.length > suffix.length =>
              rest.dropRight(suffix.length).toLowerCase(Locale.ROOT).replace('_', '-')
          }
        }
      val fromFiles = files.flatMap(document => Layers.membersOf(document.json, prefix)).toSet
      fromCli ++ fromEnv ++ fromFiles
    }

    /** Numeric member names directly under `prefix`, in ascending order, across all three layers.
      *
      * [[childrenOf]] cannot be used for this. Its environment branch takes everything up to the *last*
      * underscore, which turns `KUI_CLUSTERS_0_SECURITY_PROTOCOL` into the member `0-security` — right for
      * the flat `services.<id>.<leaf>` map it was written for, and wrong for a nested list. So a list gets
      * its own discovery: the first segment after the prefix, required to be a non-negative integer.
      */
    def indicesOf(prefix: String): List[Int] = {
      val dotted = s"$prefix."
      val fromCli = cli.keySet.filter(_.startsWith(dotted)).map(_.drop(dotted.length).takeWhile(_ != '.'))
      val envPrefix = s"${Layers.envName(prefix)}_"
      val fromEnv =
        env.keySet.filter(_.startsWith(envPrefix)).map(_.drop(envPrefix.length).takeWhile(_ != '_'))
      val fromFiles = files.flatMap(document => Layers.membersOf(document.json, prefix)).toSet
      (fromCli ++ fromEnv ++ fromFiles).toList.flatMap(_.toIntOption).filter(_ >= 0).distinct.sorted
    }

    /** Environment variable names under `prefix`, as the dotted keys they would have been.
      *
      * Only [[decodeClusters]] needs this, and only to refuse them: a raw Kafka property name contains dots
      * that the `KUI_*` mapping cannot round-trip, so the free `properties` map is file-only (D-4). Refusing
      * is what turns "my cipher suites setting did nothing" into a startup error.
      */
    def envNamesUnder(prefix: String): List[String] = {
      val envPrefix = s"${Layers.envName(prefix)}_"
      env.keySet.filter(_.startsWith(envPrefix)).toList.sorted
    }

    def documents: List[Document] = files
  }

  private object Layers {

    /** `kui.server.basePath` becomes `KUI_SERVER_BASEPATH`. Dashes become underscores too, so a service id
      * like `schema-registry` has a spellable environment name.
      */
    def envName(key: String): String = key.toUpperCase(Locale.ROOT).replace('.', '_').replace('-', '_')

    /** Reads one dotted path out of a parsed document, rendering the leaf as text.
      *
      * A numeric segment indexes into an array, so `kui.gateway.principalKeys.0.kid` reads the first
      * element's `kid`. Everything is rendered to a string here and decoded later, so that `port: 8080` in
      * YAML and `KUI_SERVER_PORT=8080` in the environment travel the same path.
      */
    def at(json: Json, key: String): Option[String] =
      descend(json, key.split('.').toList).flatMap(scalar)

    /** Every scalar leaf under `prefix`, as `full.dotted.name -> text`.
      *
      * Used for the raw client-property map, whose key names KUI cannot know in advance and which therefore
      * cannot be read one `Field` at a time.
      */
    def leavesUnder(json: Json, prefix: String): List[(String, String)] =
      descend(json, prefix.split('.').toList).toList
        .flatMap(node => flatten(node, Nil))
        .map((path, value) => path.mkString(".") -> value)

    private def flatten(json: Json, path: List[String]): List[(List[String], String)] =
      json.asObject match {
        case Some(obj) if obj.nonEmpty => obj.toList.flatMap((name, child) => flatten(child, path :+ name))
        case _ => scalar(json).map(path -> _).toList
      }

    def membersOf(json: Json, prefix: String): List[String] =
      descend(json, prefix.split('.').toList).toList.flatMap { node =>
        node.asObject
          .map(_.keys.toList)
          .orElse(node.asArray.map(_.indices.map(_.toString).toList))
          .toList
          .flatten
      }

    private def descend(json: Json, path: List[String]): Option[Json] =
      path.foldLeft(Option(json)) { (current, segment) =>
        current.flatMap { node =>
          segment.toIntOption match {
            case Some(index) => node.asArray.flatMap(_.lift(index))
            case None => node.asObject.flatMap(_(segment))
          }
        }
      }

    /** A YAML leaf as the text a decoder sees. A list of scalars becomes a comma-separated string, which is
      * also how a list is spelled in an environment variable.
      */
    private def scalar(json: Json): Option[String] =
      json.asString
        .orElse(json.asNumber.map(_.toString))
        .orElse(json.asBoolean.map(_.toString))
        .orElse(json.asArray.flatMap(items => items.toList.traverse(scalar).map(_.mkString(","))))
  }

  // ---------------------------------------------------------------------------------------------
  // Fields
  // ---------------------------------------------------------------------------------------------

  /** One configurable value: its key, what it expects, how to read it and what it is when absent.
    *
    * `secret` is not decoration. It is what makes the redaction guarantee mechanical: a problem reported for
    * a secret field prints `***` in place of the offending value, so no code path can echo a signing key into
    * a startup error.
    */
  final private case class Field[A](
      key: String,
      expectation: String,
      read: String => Either[String, A],
      fallback: Option[A],
      secret: Boolean = false
  )

  private def field[A](
      key: String,
      expectation: String,
      read: String => Either[String, A]
  ): Field[A] = Field(key, expectation, read, None)

  private def field[A](
      key: String,
      expectation: String,
      read: String => Either[String, A],
      default: A
  ): Field[A] = Field(key, expectation, read, Some(default))

  private def readHost(raw: String): Either[String, Host] = Host.from(raw).leftMap(_.message)
  private def readPort(raw: String): Either[String, Port] =
    raw.toIntOption.toRight(s"'$raw' is not a whole number").flatMap(Port.from(_).leftMap(_.message))

  private def readBoolean(raw: String): Either[String, Boolean] =
    raw.toLowerCase(Locale.ROOT) match {
      case "true" | "yes" | "on" => Right(true)
      case "false" | "no" | "off" => Right(false)
      case other => Left(s"'$other' is not a boolean")
    }

  private def readPositiveInt(raw: String): Either[String, PositiveInt] =
    raw.toIntOption
      .toRight(s"'$raw' is not a whole number")
      .flatMap(PositiveInt.from(_).leftMap(_.message))

  private def readMillis(raw: String): Either[String, FiniteDuration] =
    raw.toLongOption
      .toRight(s"'$raw' is not a whole number of milliseconds")
      .flatMap(millis =>
        if millis > 0 then Right(FiniteDuration(millis, java.util.concurrent.TimeUnit.MILLISECONDS))
        else Left(s"'$raw' is not a positive number of milliseconds")
      )

  private def readDuration(raw: String): Either[String, FiniteDuration] =
    Try(Duration(raw)).toEither.left
      .map(_ => s"'$raw' is not a duration such as 10s or 500ms")
      .flatMap {
        case finite: FiniteDuration if finite.length > 0 => Right(finite)
        case _ => Left(s"'$raw' is not a positive, finite duration")
      }

  private def readInstant(raw: String): Either[String, Instant] =
    Try(Instant.parse(raw)).toEither.left.map(_ => s"'$raw' is not an RFC 3339 instant")

  private def readNonEmpty(raw: String): Either[String, String] =
    if raw.trim.nonEmpty then Right(raw) else Left("must not be empty")

  private def readLogFormat(raw: String): Either[String, LogFormat] =
    LogFormat.fromWire(raw).toRight(s"'$raw' is neither json nor text")

  private def readUrl(policy: UrlPolicy)(raw: String): Either[String, SafeUrl] =
    SafeUrl.from(raw, policy).leftMap(_.message)

  /** Metrics source addresses are never rendered in startup diagnostics. [[SafeUrl.redacted]] already drops
    * user-info and the query, and this boundary goes further: the path of a metrics URL is itself an operator
    * choice that the query-core rule keeps out of startup errors, so every failure collapses to one
    * value-free sentence.
    */
  private def readMetricsSourceUrl(policy: UrlPolicy)(raw: String): Either[String, SafeUrl] =
    SafeUrl.from(raw, policy).leftMap(_ => "is not an allowed http or https upstream URL")

  /** Origins are a list, and `*` is refused: combined with credentials it would let any website read a
    * signed-in user's Kafka data. ADR-019 makes the allow-list explicit for that reason.
    */
  private def readOrigins(raw: String): Either[String, List[String]] = {
    val origins = raw.split(',').toList.map(_.trim).filter(_.nonEmpty)
    if origins.contains("*") then
      Left(
        "must be an explicit list of origins; '*' is refused because it cannot be combined safely with credentials"
      )
    else Right(origins)
  }

  // ---------------------------------------------------------------------------------------------
  // Decoding
  // ---------------------------------------------------------------------------------------------

  /** Reads one field through its precedence chain and turns a failure into a [[ConfigProblem]].
    *
    * Ciris owns the chain (`or` falls back only when a layer supplies nothing, so a *bad* value in a
    * high-precedence layer is an error rather than something the next layer silently papers over). The
    * problem itself is built here, from the field and the layers, because ciris's `ConfigError` carries only
    * free text and this loader promises a structured key and source.
    */
  private def read[F[_]: Async, A](field: Field[A], layers: Layers): F[Problems[A]] =
    chain(field, layers).attempt[F].map {
      case Right(value) => value.validNel
      case Left(_) => problemFor(field, layers).invalidNel
    }

  private def chain[A](field: Field[A], layers: Layers): ConfigValue[ciris.Effect, A] = {
    val key = ConfigKey(field.key)
    val start: ConfigValue[ciris.Effect, A] =
      field.fallback.fold(ConfigValue.failed[A](ConfigError(s"${field.key} is required")))(
        ConfigValue.default(_)
      )

    layers.candidates(field.key).foldRight(start) { case ((_, raw), next) =>
      val leaf = field.read(raw) match {
        case Right(value) => ConfigValue.loaded(key, value)
        case Left(problem) => ConfigValue.failed[A](ConfigError(problem))
      }
      leaf.or(next)
    }
  }

  private def problemFor[A](field: Field[A], layers: Layers): ConfigProblem =
    layers.first(field.key) match {
      case Some((source, raw)) =>
        // `redacted` leaves an ordinary value untouched and strips user-info, the query and the fragment
        // from a URL, so a password pasted into an address does not depend on the field being `secret`.
        val shown = if field.secret then Secret.Redacted else s"'${SafeUrl.redacted(raw)}'"
        val detail = field.read(raw).fold(identity, _ => "could not be read")
        ConfigProblem(field.key, s"expected ${field.expectation}; $detail (found $shown)", source)
      case None =>
        ConfigProblem(
          field.key,
          s"is required and was not set anywhere; expected ${field.expectation}",
          ConfigSourceName.Default
        )
    }

  private def readOptional[F[_]: Async, A](field: Field[A], layers: Layers): F[Problems[Option[A]]] =
    if layers.first(field.key).isEmpty then Async[F].pure(none[A].validNel)
    else read[F, A](field, layers).map(_.map(_.some))

  private def decode[F[_]: Async](layers: Layers, policy: UrlPolicy): F[Problems[Draft]] =
    for {
      unknown <- Async[F].pure(UnknownKeys.check(layers.documents))
      server <- decodeServer[F](layers)
      gateway <- decodeGateway[F](layers, policy)
      telemetry <- decodeTelemetry[F](layers, policy)
      store <- decodeStore[F](layers)
      topics <- decodeTopics[F](layers, policy)
      consumers <- decodeConsumers[F](layers)
      streaming <- decodeStreaming[F](layers)
      metrics <- decodeMetrics[F](layers, policy)
      alerts <- decodeAlerts[F](layers)
      clusters <- decodeClusters[F](layers, policy)
      // `kui.auth` and `kui.rbac` are read through their own sections rather than through the `Field`
      // chain above, because both are list-shaped: how many accounts and how many roles a file has is
      // not known until the file has been read, and a `Field` names one key. They ask the layers the
      // two questions `ConfigReader` declares — what is at this key, and which indices exist under this
      // prefix — and see nothing else of the precedence chain.
      auth <- Async[F].pure(AuthConfigSection.decode(layers.first, layers.indicesOf))
      rbac <- Async[F].pure(RbacConfigSection.decode(layers.first, layers.indicesOf))
    } yield (
      unknown,
      server,
      gateway,
      telemetry,
      store,
      topics,
      consumers,
      streaming,
      clusters,
      auth,
      rbac,
      metrics,
      alerts
    ).mapN((_, s, g, t, st, tp, cn, sr, cs, a, r, m, al) => Draft(s, g, t, st, tp, cn, sr, cs, a, r, m, al))
      // The one rule that needs two decoded sections at once, so it cannot live in either of them.
      // `.andThen` and not a fourteenth `mapN` argument: if any section above is invalid this stays quiet,
      // which is `checkStoreRules`' discipline applied one level up -- a role naming an unknown cluster is
      // not worth reporting when the cluster list itself did not decode.
      .andThen(draft => rolesNameConfiguredClusters(draft).map(_ => draft))

  /** A role may only name a cluster this deployment has, when this file is the whole list of them.
    *
    * `kui.rbac.roles[].clusters` is matched against `kui.clusters[].id` by `RbacPolicy.held`, and a role
    * naming an id that does not exist grants nothing to anybody, anywhere, for ever. Nothing said so:
    * `deployment/quickstart/kui-quickstart-auth.yaml` pointed both of its roles at `no-such-cluster` and the
    * file loaded, both demonstration accounts signed in, and every screen was empty -- which is the
    * demonstration failing in front of whoever is being shown it. The file's own comment claimed this was
    * caught and it was not; that claim is what this rule makes true.
    *
    * ==The two deployments it deliberately says nothing about==
    *
    * **A file with `kui.clusters: []`.** There is no set to check against. That is not an empty set of
    * clusters, it is a deployment that registered none here -- the M0 default, the Compose gateway's own
    * `kui.yaml`, and every fixture that exercises `kui.rbac` without also writing a cluster.
    *
    * **A deployment with a metadata store.** ADR-036 as amended by ADR-042 lets the store's records overlay
    * `kui.clusters[]` at run time, so an operator who registers `prod-3` through the UI and writes a role for
    * it is correct and this file cannot see it. Refusing there would refuse a working deployment at start-up,
    * which is a worse answer than the one this rule exists to improve on.
    *
    * Both exemptions are stated rather than derived, because each is a hole somebody will otherwise find and
    * report as a bug in this rule.
    */
  private def rolesNameConfiguredClusters(draft: Draft): Problems[Unit] = {
    val configured = draft.clusters.map(_.id).toSet
    val storeMayAddMore = draft.store.kafka.isDefined || draft.store.dir.isDefined

    if configured.isEmpty || storeMayAddMore then ().validNel
    else
      draft.rbac.roles.zipWithIndex.flatMap { (role, index) =>
        role.clusters.toList
          .filterNot(configured.contains)
          .sortBy(_.value)
          .map { unknown =>
            ConfigProblem(
              s"kui.rbac.roles.$index.clusters",
              s"role '${role.name.value}' names '${unknown.value}', which is not a configured cluster; " +
                s"this file configures ${configured.toList.map(_.value).sorted.mkString(", ")}. " +
                "A role on a cluster that is not here grants nothing to anybody: everyone in it signs in " +
                "and sees empty screens. Fix the id, add the cluster, or -- if the cluster is registered " +
                "at run time through kui.store -- configure the store so this file is no longer the whole " +
                "list",
              ConfigSourceName.Default
            )
          }
      } match {
        case Nil => ().validNel
        case first :: rest => cats.data.Validated.Invalid(NonEmptyList(first, rest))
      }
  }

  private def decodeServer[F[_]: Async](layers: Layers): F[Problems[ServerConfig]] =
    for {
      host <- read[F, Host](
        field("kui.server.host", "a host name or IP address", readHost, ServerConfig.Default.host),
        layers
      )
      port <- read[F, Port](
        field("kui.server.port", "a port between 1 and 65535", readPort, ServerConfig.Default.port),
        layers
      )
      basePath <- read[F, String](
        field("kui.server.basePath", "a path such as / or /kui", readNonEmpty, ServerConfig.Default.basePath),
        layers
      )
    } yield (host, port, basePath).mapN(ServerConfig.apply)

  private def decodeGateway[F[_]: Async](
      layers: Layers,
      policy: UrlPolicy
  ): F[Problems[GatewayDraft]] =
    for {
      services <- decodeServices[F](layers, policy)
      readiness <- read[F, FiniteDuration](
        field(
          "kui.gateway.readinessIntervalMs",
          "a positive number of milliseconds",
          readMillis,
          GatewayConfig.DefaultReadinessInterval
        ),
        layers
      )
      keys <- decodePrincipalKeys[F](layers)
      cors <- decodeCors[F](layers)
      devInsecureCookies <- read[F, Boolean](
        field(
          "kui.server.devInsecureCookies",
          "true or false",
          readBoolean,
          GatewayConfig.Default.devInsecureCookies
        ),
        layers
      )
    } yield (services, readiness, keys, cors, devInsecureCookies).mapN(GatewayDraft.apply)

  private def decodeServices[F[_]: Async](
      layers: Layers,
      policy: UrlPolicy
  ): F[Problems[Map[ServiceId, UpstreamServiceConfig]]] =
    layers
      .childrenOf("kui.gateway.services")
      .toList
      .sorted
      .traverse(name => decodeService[F](layers, policy, name))
      .map(_.sequence.map(_.toMap))

  private def decodeService[F[_]: Async](
      layers: Layers,
      policy: UrlPolicy,
      name: String
  ): F[Problems[(ServiceId, UpstreamServiceConfig)]] = {
    val prefix = s"kui.gateway.services.$name"
    for {
      url <- read[F, SafeUrl](
        field(s"$prefix.url", "an http or https URL this deployment is allowed to call", readUrl(policy)),
        layers
      )
      timeout <- read[F, FiniteDuration](
        field(
          s"$prefix.timeout",
          "a duration such as 10s",
          readDuration,
          UpstreamServiceConfig.DefaultTimeout
        ),
        layers
      )
      concurrency <- read[F, PositiveInt](
        field(
          s"$prefix.maxConcurrent",
          "a positive whole number",
          readPositiveInt,
          UpstreamServiceConfig.DefaultMaxConcurrent
        ),
        layers
      )
      id = ServiceId
        .from(name)
        .leftMap(error =>
          NonEmptyList.one(
            ConfigProblem(
              prefix,
              error.message,
              layers.first(s"$prefix.url").map(_._1).getOrElse(ConfigSourceName.Default)
            )
          )
        )
        .toValidated
    } yield (id, url, timeout, concurrency).mapN((serviceId, u, t, c) =>
      serviceId -> UpstreamServiceConfig(u, t, c)
    )
  }

  /** The signing keys, which are a *list*: their children are the list indices.
    *
    * A child that is not a number means the section was written as a map — an indentation slip that turns
    * `- kid: k1` into `k1:` — and every such child used to be dropped by a `flatMap(_.toIntOption)` that said
    * nothing. The result was an empty key list from a file with keys plainly written in it: the cluster
    * service then refused to start with "no principal signing keys are configured" while the operator was
    * looking straight at one, and the gateway started from the same file and signed nothing. Naming the
    * offending child is the whole difference between a five-minute fix and an evening.
    */
  private def decodePrincipalKeys[F[_]: Async](layers: Layers): F[Problems[List[KeyDraft]]] = {
    val prefix = "kui.gateway.principalKeys"
    val (notIndices, indices) =
      layers.childrenOf(prefix).toList.partitionMap(name => name.toIntOption.toRight(name))
    val misshapen = notIndices.sorted.map(name =>
      ConfigProblem(
        s"$prefix.$name",
        s"is not a list entry; '$prefix' is a list of keys, so each entry is written as a '- ' item " +
          "rather than as a named child",
        sourceOfPrincipalKey(layers, name)
      )
    )
    indices.sorted
      .traverse(index => decodePrincipalKey[F](layers, index))
      .map(_.sequence)
      .map(decoded =>
        misshapen match {
          case Nil => decoded
          case first :: rest => decoded *> NonEmptyList(first, rest).invalid
        }
      )
  }

  /** Where the operator wrote the misshapen entry, so the message can point at the right file. */
  private def sourceOfPrincipalKey(layers: Layers, name: String): ConfigSourceName = {
    val prefix = s"kui.gateway.principalKeys.$name"
    List("kid", "key", "notBefore")
      .flatMap(leaf => layers.first(s"$prefix.$leaf").map(_._1))
      .headOption
      .getOrElse(ConfigSourceName.Default)
  }

  private def decodePrincipalKey[F[_]: Async](layers: Layers, index: Int): F[Problems[KeyDraft]] = {
    val prefix = s"kui.gateway.principalKeys.$index"
    for {
      kid <- read[F, String](field(s"$prefix.kid", "a non-empty key id", readNonEmpty), layers)
      key <- read[F, SecretRef](
        Field(
          s"$prefix.key",
          "a literal secret, env:NAME or file:/path",
          raw => readNonEmpty(raw).map(SecretRef.parse),
          None,
          secret = true
        ),
        layers
      )
      notBefore <- read[F, Instant](
        field(s"$prefix.notBefore", "an RFC 3339 instant", readInstant, Instant.EPOCH),
        layers
      )
    } yield (kid, key, notBefore).mapN(KeyDraft.apply)
  }

  private def decodeCors[F[_]: Async](layers: Layers): F[Problems[CorsConfig]] =
    for {
      enabled <- read[F, Boolean](
        field("kui.gateway.cors.enabled", "true or false", readBoolean, CorsConfig.Default.enabled),
        layers
      )
      origins <- read[F, List[String]](
        field(
          "kui.gateway.cors.origins",
          "a comma-separated list of origins",
          readOrigins,
          CorsConfig.Default.origins
        ),
        layers
      )
    } yield (enabled, origins).mapN(CorsConfig.apply)

  private def decodeTelemetry[F[_]: Async](
      layers: Layers,
      policy: UrlPolicy
  ): F[Problems[TelemetryConfig]] =
    for {
      otlp <- readOptional[F, SafeUrl](
        field("kui.telemetry.otlpEndpoint", "an http or https URL", readUrl(policy)),
        layers
      )
      prometheus <- readOptional[F, Port](
        field("kui.telemetry.prometheusPort", "a port between 1 and 65535", readPort),
        layers
      )
      format <- read[F, LogFormat](
        field("kui.telemetry.logFormat", "json or text", readLogFormat, TelemetryConfig.Default.logFormat),
        layers
      )
      hash <- read[F, Boolean](
        field(
          "kui.telemetry.hashUserIds",
          "true or false",
          readBoolean,
          TelemetryConfig.Default.hashUserIds
        ),
        layers
      )
    } yield (otlp, prometheus, format, hash).mapN(TelemetryConfig.apply)

  // ---------------------------------------------------------------------------------------------
  // The metadata store
  // ---------------------------------------------------------------------------------------------

  /** The `kui.store.*` slice (ADR-042).
    *
    * `kui.store.kafka.bootstrapServers` being present is the on/off switch for the Kafka store; absent means
    * the file adapter, and there is no third state. Everything else is a scalar with a default, plus four
    * cross-field rules that no single field can check on its own and which are therefore checked here, each
    * producing its own problem so that an operator sees all of them at once.
    */
  private def decodeStore[F[_]: Async](layers: Layers): F[Problems[StoreDraft]] =
    for {
      prefix <- read[F, String](
        field(
          "kui.store.topicPrefix",
          s"a prefix matching ${StoreConfig.TopicPrefixPattern}",
          readTopicPrefix,
          StoreConfig.DefaultTopicPrefix
        ),
        layers
      )
      replication <- read[F, Short](
        field(
          "kui.store.replicationFactor",
          "a whole number between 1 and 32767",
          readReplicationFactor,
          StoreConfig.DefaultReplicationFactor
        ),
        layers
      )
      minIsr <- read[F, PositiveInt](
        field(
          "kui.store.minInSyncReplicas",
          "a positive whole number",
          readPositiveInt,
          StoreConfig.DefaultMinInSyncReplicas
        ),
        layers
      )
      maxFile <- read[F, Long](
        field(
          "kui.store.maxFileBytes",
          s"a size between ${StoreConfig.MinFileBytes} and ${StoreConfig.MaxFileBytes} bytes",
          readBounded(StoreConfig.MinFileBytes, StoreConfig.MaxFileBytes),
          StoreConfig.DefaultMaxFileBytes
        ),
        layers
      )
      replay <- read[F, FiniteDuration](
        field(
          "kui.store.replayTimeout",
          s"a duration between ${StoreConfig.MinReplayTimeout} and ${StoreConfig.MaxReplayTimeout}",
          readBoundedDuration(StoreConfig.MinReplayTimeout, StoreConfig.MaxReplayTimeout),
          StoreConfig.DefaultReplayTimeout
        ),
        layers
      )
      write <- read[F, FiniteDuration](
        field(
          "kui.store.writeTimeout",
          s"a duration between ${StoreConfig.MinWriteTimeout} and ${StoreConfig.MaxWriteTimeout}",
          readBoundedDuration(StoreConfig.MinWriteTimeout, StoreConfig.MaxWriteTimeout),
          StoreConfig.DefaultWriteTimeout
        ),
        layers
      )
      dir <- readOptional[F, Path](
        field("kui.store.dir", "a directory path", raw => readNonEmpty(raw).map(Path.of(_))),
        layers
      )
      kafka <- decodeStoreKafka[F](layers)
      encryption = decodeStoreEncryption(layers)
      fields = (prefix, replication, minIsr, maxFile, replay, write, dir, kafka, encryption)
        .mapN(StoreDraft.apply)
    } yield (fields, checkStoreRules(layers)).mapN((draft, _) => draft)

  private def readTopicPrefix(raw: String): Either[String, String] =
    if raw.matches(StoreConfig.TopicPrefixPattern) then Right(raw)
    else Left(s"'$raw' does not match ${StoreConfig.TopicPrefixPattern}")

  private def readReplicationFactor(raw: String): Either[String, Short] =
    raw.toIntOption.toRight(s"'$raw' is not a whole number").flatMap { value =>
      if value >= 1 && value <= Short.MaxValue then Right(value.toShort)
      else Left(s"$value is outside 1..${Short.MaxValue}")
    }

  private def readBounded(min: Long, max: Long)(raw: String): Either[String, Long] =
    raw.toLongOption.toRight(s"'$raw' is not a whole number").flatMap { value =>
      if value >= min && value <= max then Right(value) else Left(s"$value is outside $min..$max")
    }

  private def readBoundedDuration(min: FiniteDuration, max: FiniteDuration)(
      raw: String
  ): Either[String, FiniteDuration] =
    Try(Duration(raw)).toEither.left
      .map(_ => s"'$raw' is not a duration such as 30s or 2m")
      .flatMap {
        case finite: FiniteDuration if finite >= min && finite <= max => Right(finite)
        case other => Left(s"$other is outside $min..$max")
      }

  /** The store cluster's connection, or `None` when no bootstrap servers are configured. */
  private def decodeStoreKafka[F[_]: Async](layers: Layers): F[Problems[Option[StoreKafkaDraft]]] = {
    val key = "kui.store.kafka.bootstrapServers"
    if layers.first(key).isEmpty then Async[F].pure(none[StoreKafkaDraft].validNel)
    else
      read[F, BootstrapServers](
        field(key, "a comma-separated list of host:port entries", readBootstrapServers),
        layers
      ).map { servers =>
        val security = ClusterSecurityConfig.decode("kui.store.kafka.security", layers.first)
        val properties = propertiesUnder(layers, "kui.store.kafka.properties")
        (servers, security).mapN((s, sec) => Some(StoreKafkaDraft(s, sec, properties)))
      }
  }

  private def readBootstrapServers(raw: String): Either[String, BootstrapServers] =
    BootstrapServers.from(raw).leftMap(_.message)

  /** Raw client properties, read out of the file layers by name. They are not settable from the environment:
    * a Kafka property name contains dots that the `KUI_*` mapping cannot round-trip.
    */
  private def propertiesUnder(layers: Layers, prefix: String): Map[String, String] =
    layers.documents.flatMap(document => Layers.leavesUnder(document.json, prefix)).toMap

  /** `encryptionKey` is one key with the id `k1`; `encryptionKeys` is `id:base64,id:base64`. Setting both is
    * an error rather than a merge: the shorthand exists for the common case, not as an alias.
    */
  private def decodeStoreEncryption(layers: Layers): Problems[Option[StoreEncryptionDraft]] = {
    val single = layers.first("kui.store.encryptionKey")
    val many = layers.first("kui.store.encryptionKeys")
    val activeId = layers.first("kui.store.encryptionKeyId").map(_._2.trim).filter(_.nonEmpty)
    (single, many) match {
      case (Some(_), Some((source, _))) =>
        ConfigProblem(
          "kui.store.encryptionKeys",
          "cannot be set together with kui.store.encryptionKey; use encryptionKey for one key and " +
            "encryptionKeys for a rotation",
          source
        ).invalidNel

      case (Some((_, raw)), None) =>
        val id = activeId.getOrElse(StoreConfig.DefaultKeyId)
        Some(StoreEncryptionDraft(Map(id -> SecretRef.parse(raw)), id)).validNel

      case (None, Some((source, raw))) =>
        parseKeyList(raw, source).andThen { keys =>
          activeId match {
            case None =>
              ConfigProblem(
                "kui.store.encryptionKeyId",
                s"is required with kui.store.encryptionKeys; it names which of ${keys.keys.toList.sorted
                    .mkString(", ")} new writes use",
                ConfigSourceName.Default
              ).invalidNel
            case Some(id) if keys.contains(id) => Some(StoreEncryptionDraft(keys, id)).validNel
            case Some(id) =>
              ConfigProblem(
                "kui.store.encryptionKeyId",
                s"'$id' is not among the configured key ids (${keys.keys.toList.sorted.mkString(", ")})",
                source
              ).invalidNel
          }
        }

      case (None, None) => none[StoreEncryptionDraft].validNel
    }
  }

  /** `id:base64,id:base64`. The material is never echoed, whatever went wrong. */
  private def parseKeyList(raw: String, source: ConfigSourceName): Problems[Map[String, SecretRef]] = {
    val entries = raw.split(',').toList.map(_.trim).filter(_.nonEmpty)
    val parsed = entries.map(entry => entry.span(_ != ':'))
    val malformed = parsed.exists((id, rest) => id.isEmpty || rest.length <= 1)
    if entries.isEmpty || malformed then
      ConfigProblem(
        "kui.store.encryptionKeys",
        "expected 'id:base64,id:base64'; one entry is missing its id or its material",
        source
      ).invalidNel
    else parsed.map((id, rest) => id -> SecretRef.parse(rest.drop(1))).toMap.validNel
  }

  /** The rules no single field can check.
    *
    * They read the layers directly rather than the decoded draft, and that is the point: a draft only exists
    * when *every* field decoded, so a cross-field rule expressed over the draft would go unreported whenever
    * some unrelated field was also wrong. An operator fixing one key per restart is precisely what the
    * accumulate-everything discipline exists to prevent. A field that is itself invalid already has its own
    * problem, so the rule that would have used it stays quiet rather than reporting the same mistake twice.
    */
  private def checkStoreRules(layers: Layers): Problems[Unit] = {
    val replication = layers.first("kui.store.replicationFactor").flatMap(v => v._2.toIntOption)
    val minIsr = layers.first("kui.store.minInSyncReplicas").flatMap(v => v._2.toIntOption)

    val isrRule = (replication.orElse(Some(StoreConfig.DefaultReplicationFactor.toInt)), minIsr) match {
      case (Some(factor), Some(isr)) if isr > factor =>
        ConfigProblem(
          "kui.store.minInSyncReplicas",
          s"expected <= kui.store.replicationFactor ($factor), got $isr",
          layers.first("kui.store.minInSyncReplicas").map(_._1).getOrElse(ConfigSourceName.Default)
        ).invalidNel
      case _ => ().validNel
    }

    val keyRule =
      if layers.first("kui.store.kafka.bootstrapServers").isDefined &&
        layers.first("kui.store.encryptionKey").isEmpty &&
        layers.first("kui.store.encryptionKeys").isEmpty
      then
        ConfigProblem(
          "kui.store.encryptionKey",
          "a Kafka metadata store requires an encryption key; generate one with `openssl rand -base64 32` " +
            "and set KUI_STORE_ENCRYPTION_KEY (see docs/operations/metadata-store.md §4.2)",
          ConfigSourceName.Default
        ).invalidNel
      else ().validNel

    (isrRule, keyRule).mapN((_, _) => ())
  }

  final private case class StoreDraft(
      topicPrefix: String,
      replicationFactor: Short,
      minInSyncReplicas: PositiveInt,
      maxFileBytes: Long,
      replayTimeout: FiniteDuration,
      writeTimeout: FiniteDuration,
      dir: Option[Path],
      kafka: Option[StoreKafkaDraft],
      encryption: Option[StoreEncryptionDraft]
  )

  final private case class StoreKafkaDraft(
      bootstrapServers: BootstrapServers,
      security: ClusterSecurityConfig.ClusterSecurityDraft,
      properties: Map[String, String]
  )

  final private case class StoreEncryptionDraft(keys: Map[String, SecretRef], activeKeyId: String)

  private def resolveStore[F[_]: Async](
      draft: StoreDraft,
      env: Map[String, String]
  ): F[Problems[StoreConfig]] =
    for {
      kafka <- draft.kafka.traverse(k =>
        ClusterSecurityConfig
          .resolve[F](k.security, env)
          .map(_.map(security => StoreKafkaConfig(k.bootstrapServers, security, k.properties)))
      )
      encryption <- draft.encryption.traverse(resolveEncryption[F](_, env))
    } yield (kafka.sequence, encryption.sequence).mapN((k, e) =>
      StoreConfig(
        draft.topicPrefix,
        draft.replicationFactor,
        draft.minInSyncReplicas,
        draft.maxFileBytes,
        draft.replayTimeout,
        draft.writeTimeout,
        draft.dir,
        k,
        e
      )
    )

  private def resolveEncryption[F[_]: Async](
      draft: StoreEncryptionDraft,
      env: Map[String, String]
  ): F[Problems[StoreEncryptionConfig]] =
    draft.keys.toList
      .sortBy(_._1)
      .traverse { (id, ref) =>
        SecretRef.resolve[F](ref, env).map {
          case Right(secret) => (id -> secret).validNel
          case Left(problem) =>
            ConfigProblem("kui.store.encryptionKey", problem, ConfigSourceName.Default).invalidNel
        }
      }
      .map(_.sequence.map(pairs => StoreEncryptionConfig(pairs.toMap, draft.activeKeyId)))

  // ---------------------------------------------------------------------------------------------
  // The managed clusters
  // ---------------------------------------------------------------------------------------------

  /** The `kui.clusters[]` slice: the static base of the cluster registry (ADR-022, ADR-031).
    *
    * Nothing here opens a socket. Validation is syntactic, because reachability is not knowable at load time
    * and because one dead broker must never stop KUI from starting (M1 DEVPLAN §10, D4).
    *
    * Everything that is wrong is reported: every bad field of every cluster, plus the three cross-cluster
    * rules — a gap in the index, two clusters that resolve to the same id, and a raw property set from the
    * environment — so that an operator fixes their file once rather than once per restart.
    */
  // ---------------------------------------------------------------------------------------------
  // The topic service
  // ---------------------------------------------------------------------------------------------

  /** The `kui.topics.*` and `kui.clusterProfiles.*` slices (TOP-032).
    *
    * `kui.clusterProfiles.url` being present is the on/off switch for the profile client: absent means the
    * process has none, which is the right answer for the gateway, the cluster service and the store. The
    * topic service's composition root is what refuses to start without it, because it is the only process
    * that can say so truthfully.
    */
  private def decodeTopics[F[_]: Async](layers: Layers, policy: UrlPolicy): F[Problems[TopicsConfig]] =
    for {
      refreshInterval <- read[F, FiniteDuration](
        field(
          "kui.topics.refreshInterval",
          s"a duration between ${TopicsConfig.MinRefreshInterval} and ${TopicsConfig.MaxRefreshInterval}",
          readBoundedDuration(TopicsConfig.MinRefreshInterval, TopicsConfig.MaxRefreshInterval),
          TopicsConfig.DefaultRefreshInterval
        ),
        layers
      )
      scrapeTimeout <- read[F, FiniteDuration](
        field(
          "kui.topics.scrapeTimeout",
          s"a duration between ${TopicsConfig.MinScrapeTimeout} and ${TopicsConfig.MaxScrapeTimeout}",
          readBoundedDuration(TopicsConfig.MinScrapeTimeout, TopicsConfig.MaxScrapeTimeout),
          TopicsConfig.DefaultScrapeTimeout
        ),
        layers
      )
      internalPrefix <- read[F, String](
        field(
          "kui.topics.internalPrefix",
          s"${TopicsConfig.MinInternalPrefixLength} to ${TopicsConfig.MaxInternalPrefixLength} characters",
          readInternalPrefix,
          TopicsConfig.DefaultInternalPrefix
        ),
        layers
      )
      searchMode <- read[F, SearchMode](
        field("kui.topics.defaultSearchMode", "plain or fts", readSearchMode, SearchMode.Default),
        layers
      )
      defaultPageSize <- read[F, PageSize](
        field(
          "kui.topics.defaultPageSize",
          s"a page size between 1 and ${PageSize.Max.value}",
          readPageSize,
          PageSize.Default
        ),
        layers
      )
      maxPageSize <- read[F, PageSize](
        field(
          "kui.topics.maxPageSize",
          s"a page size between 1 and ${PageSize.Max.value}",
          readPageSize,
          PageSize.Max
        ),
        layers
      )
      profiles <- decodeProfileClient[F](layers, policy)
      rules = checkTopicRules(layers)
    } yield (
      refreshInterval,
      scrapeTimeout,
      internalPrefix,
      searchMode,
      defaultPageSize,
      maxPageSize,
      profiles,
      rules
    ).mapN((interval, timeout, prefix, mode, pageSize, maxSize, profileClient, _) =>
      TopicsConfig(interval, timeout, prefix, mode, pageSize, maxSize, profileClient)
    )

  // ---------------------------------------------------------------------------------------------
  // The consumer service, and the signing key both browser-facing tokens share
  // ---------------------------------------------------------------------------------------------

  /** The `kui.consumers.*` slice. One key, bounded the way every other interval is. */
  private def decodeConsumers[F[_]: Async](layers: Layers): F[Problems[ConsumersConfig]] =
    read[F, FiniteDuration](
      field(
        "kui.consumers.refreshInterval",
        s"a duration between ${ConsumersConfig.MinRefreshInterval} and " +
          s"${ConsumersConfig.MaxRefreshInterval}",
        readBoundedDuration(ConsumersConfig.MinRefreshInterval, ConsumersConfig.MaxRefreshInterval),
        ConsumersConfig.DefaultRefreshInterval
      ),
      layers
    ).map(_.map(ConsumersConfig.apply))

  /** The `kui.streaming.*` slice.
    *
    * Absent is legal and means "generate one per process", which is what every composition root did before
    * this key existed; see [[StreamingConfig]] for why that is still the right default and wrong the moment
    * there are two replicas.
    */
  private def decodeStreaming[F[_]: Async](layers: Layers): F[Problems[StreamingDraft]] = {
    val key = "kui.streaming.cursorKey"
    if layers.first(key).isEmpty then Async[F].pure(StreamingDraft(None).validNel)
    else
      read[F, SecretRef](
        Field(
          key,
          "a literal value, env:NAME or file:/path",
          raw => Right(SecretRef.parse(raw)),
          None,
          secret = true
        ),
        layers
      ).map(_.map(ref => StreamingDraft(Some(ref))))
  }

  // ---------------------------------------------------------------------------------------------
  // The metrics and alerts services, which do not exist yet
  // ---------------------------------------------------------------------------------------------

  /** The `kui.metrics.*` slice.
    *
    * Every key has a default and the section as a whole is optional, so a file written before either of these
    * sections existed loads unchanged. What is *not* defaulted into existence is a source: a cluster with no
    * entry under `kui.metrics.sources` has none, the metrics service answers `not_configured` for it, and the
    * cards keep their sentence. That is the design (ADR-032) and not a degraded mode, which is why the
    * absence is a value rather than a problem to report.
    */
  private def decodeMetrics[F[_]: Async](layers: Layers, policy: UrlPolicy): F[Problems[MetricsConfig]] =
    for {
      scrapeInterval <- read[F, FiniteDuration](
        field(
          "kui.metrics.scrapeInterval",
          s"a duration between ${MetricsConfig.MinScrapeInterval} and ${MetricsConfig.MaxScrapeInterval}",
          readBoundedDuration(MetricsConfig.MinScrapeInterval, MetricsConfig.MaxScrapeInterval),
          MetricsConfig.DefaultScrapeInterval
        ),
        layers
      )
      retention <- read[F, FiniteDuration](
        field(
          "kui.metrics.retention",
          s"a duration between ${MetricsConfig.MinRetention} and ${MetricsConfig.MaxRetention}",
          readBoundedDuration(MetricsConfig.MinRetention, MetricsConfig.MaxRetention),
          MetricsConfig.DefaultRetention
        ),
        layers
      )
      maxSamples <- read[F, Int](
        field(
          "kui.metrics.maxSamplesPerSeries",
          s"a whole number between ${MetricsConfig.MinSamplesPerSeries} and " +
            s"${MetricsConfig.MaxSamplesPerSeries}",
          readBoundedInt(MetricsConfig.MinSamplesPerSeries, MetricsConfig.MaxSamplesPerSeries),
          MetricsConfig.DefaultMaxSamplesPerSeries
        ),
        layers
      )
      sources <- decodeMetricsSources[F](layers, policy)
      rules = checkMetricsRules(layers)
    } yield (scrapeInterval, retention, maxSamples, sources, rules).mapN(
      (interval, window, cap, addresses, _) => MetricsConfig(interval, window, cap, addresses)
    )

  /** The per-cluster sources, keyed by cluster id.
    *
    * Sources have fixed nested auth/TLS leaves, so environment discovery strips a recognized leaf suffix
    * rather than guessing at the last underscore. That keeps `..._LOCAL_AUTH_USERNAME` attached to `local`
    * just as the equivalent YAML and command-line paths are.
    */
  private def decodeMetricsSources[F[_]: Async](
      layers: Layers,
      policy: UrlPolicy
  ): F[Problems[Map[ClusterId, MetricsSourceSettings]]] =
    layers
      .mapMembersOf(MetricsSourcesPrefix, MetricsSourceEnvLeafPaths)
      .toList
      .sorted
      .traverse(name => decodeMetricsSource[F](layers, policy, name))
      .map(_.sequence.map(_.toMap))

  private def decodeMetricsSource[F[_]: Async](
      layers: Layers,
      policy: UrlPolicy,
      name: String
  ): F[Problems[(ClusterId, MetricsSourceSettings)]] = {
    val prefix = s"$MetricsSourcesPrefix.$name"
    for {
      url <- read[F, SafeUrl](
        Field(
          s"$prefix.url",
          "an http or https URL this deployment is allowed to call, such as http://broker-1:9404/metrics",
          readMetricsSourceUrl(policy),
          None,
          secret = true
        ),
        layers
      )
      kind <- read[F, MetricsSourceKind](
        field(
          s"$prefix.kind",
          MetricsSourceKind.All.map(_.wireName).mkString(" or "),
          readMetricsSourceKind,
          MetricsSourceKind.Prometheus
        ),
        layers
      )
      callTimeout <- read[F, FiniteDuration](
        field(
          s"$prefix.callTimeout",
          s"a duration between ${MetricsSourceSettings.MinCallTimeout} and " +
            s"${MetricsSourceSettings.MaxCallTimeout}",
          readBoundedDuration(
            MetricsSourceSettings.MinCallTimeout,
            MetricsSourceSettings.MaxCallTimeout
          ),
          MetricsSourceSettings.DefaultCallTimeout
        ),
        layers
      )
      queryTimeout <- read[F, FiniteDuration](
        field(
          s"$prefix.queryTimeout",
          s"a duration between ${MetricsSourceSettings.MinQueryTimeout} and " +
            s"${MetricsSourceSettings.MaxQueryTimeout}",
          readBoundedDuration(
            MetricsSourceSettings.MinQueryTimeout,
            MetricsSourceSettings.MaxQueryTimeout
          ),
          MetricsSourceSettings.DefaultQueryTimeout
        ),
        layers
      )
      maxConcurrentQueries <- read[F, Int](
        field(
          s"$prefix.maxConcurrentQueries",
          s"a whole number between ${MetricsSourceSettings.MinConcurrentQueries} and " +
            s"${MetricsSourceSettings.MaxConcurrentQueries}",
          readBoundedInt(
            MetricsSourceSettings.MinConcurrentQueries,
            MetricsSourceSettings.MaxConcurrentQueries
          ),
          MetricsSourceSettings.DefaultMaxConcurrentQueries
        ),
        layers
      )
      maxSeriesPerQuery <- read[F, Int](
        field(
          s"$prefix.maxSeriesPerQuery",
          s"a whole number between ${MetricsSourceSettings.MinSeriesPerQuery} and " +
            s"${MetricsSourceSettings.MaxSeriesPerQuery}",
          readBoundedInt(
            MetricsSourceSettings.MinSeriesPerQuery,
            MetricsSourceSettings.MaxSeriesPerQuery
          ),
          MetricsSourceSettings.DefaultMaxSeriesPerQuery
        ),
        layers
      )
      maxPointsPerSeries <- read[F, Int](
        field(
          s"$prefix.maxPointsPerSeries",
          s"a whole number between ${MetricsSourceSettings.MinPointsPerSeries} and " +
            s"${MetricsSourceSettings.MaxPointsPerSeries}",
          readBoundedInt(
            MetricsSourceSettings.MinPointsPerSeries,
            MetricsSourceSettings.MaxPointsPerSeries
          ),
          MetricsSourceSettings.DefaultMaxPointsPerSeries
        ),
        layers
      )
      maxResponseBytes <- read[F, Int](
        field(
          s"$prefix.maxResponseBytes",
          s"a whole number between ${MetricsSourceSettings.MinResponseBytes} and " +
            s"${MetricsSourceSettings.MaxResponseBytes}",
          readBoundedInt(
            MetricsSourceSettings.MinResponseBytes,
            MetricsSourceSettings.MaxResponseBytes
          ),
          MetricsSourceSettings.DefaultMaxResponseBytes
        ),
        layers
      )
      maxCacheBytes <- read[F, Int](
        field(
          s"$prefix.maxCacheBytes",
          s"a whole number between ${MetricsSourceSettings.MinCacheBytes} and " +
            s"${MetricsSourceSettings.MaxCacheBytes}",
          readBoundedInt(MetricsSourceSettings.MinCacheBytes, MetricsSourceSettings.MaxCacheBytes),
          MetricsSourceSettings.DefaultMaxCacheBytes
        ),
        layers
      )
      cacheTtl <- read[F, FiniteDuration](
        field(
          s"$prefix.cacheTtl",
          s"a duration between ${MetricsSourceSettings.MinCacheTtl} and " +
            s"${MetricsSourceSettings.MaxCacheTtl}",
          readBoundedDuration(MetricsSourceSettings.MinCacheTtl, MetricsSourceSettings.MaxCacheTtl),
          MetricsSourceSettings.DefaultCacheTtl
        ),
        layers
      )
      staleTtl <- read[F, FiniteDuration](
        field(
          s"$prefix.staleTtl",
          s"a duration between ${MetricsSourceSettings.MinStaleTtl} and " +
            s"${MetricsSourceSettings.MaxStaleTtl}",
          readBoundedDuration(MetricsSourceSettings.MinStaleTtl, MetricsSourceSettings.MaxStaleTtl),
          MetricsSourceSettings.DefaultStaleTtl
        ),
        layers
      )
      auth <- decodeMetricsSourceAuth[F](layers, policy, prefix)
      tls <- decodeMetricsSourceTls[F](layers, prefix)
      // The member name is a cluster id and not a free label: it is how a source is matched to the cluster
      // in `kui.clusters[]`, so a name that could never be a `ClusterId` is a source that could never be
      // read, and saying so at load time is the difference between a typo and a card that never fills.
      id = ClusterId
        .from(name)
        .leftMap(error =>
          NonEmptyList.one(
            ConfigProblem(
              prefix,
              s"${error.message}; the member name is the id of the cluster this source measures",
              layers.first(s"$prefix.url").map(_._1).getOrElse(ConfigSourceName.Default)
            )
          )
        )
        .toValidated
    } yield (
      id,
      url,
      kind,
      callTimeout,
      queryTimeout,
      maxConcurrentQueries,
      maxSeriesPerQuery,
      maxPointsPerSeries,
      maxResponseBytes,
      maxCacheBytes,
      cacheTtl,
      staleTtl,
      auth,
      tls
    ).mapN {
      (
          clusterId,
          address,
          protocol,
          callBudget,
          queryBudget,
          concurrency,
          seriesCap,
          pointsCap,
          responseCap,
          cacheCap,
          freshFor,
          staleFor,
          credentials,
          tlsConfig
      ) =>
        clusterId -> MetricsSourceSettings(
          address,
          protocol,
          callBudget,
          queryBudget,
          concurrency,
          seriesCap,
          pointsCap,
          responseCap,
          cacheCap,
          freshFor,
          staleFor,
          credentials,
          tlsConfig
        )
    }.andThen { case pair @ (_, source) => validateMetricsSourceSecurity(layers, prefix, source).as(pair) }
  }

  private def decodeMetricsSourceAuth[F[_]: Async](
      layers: Layers,
      policy: UrlPolicy,
      prefix: String
  ): F[Problems[UpstreamAuthConfig]] = {
    val kind = layers.first(s"$prefix.kind") match {
      case None => Some(MetricsSourceKind.Prometheus)
      case Some((_, raw)) => readMetricsSourceKind(raw).toOption
    }
    kind match {
      case Some(MetricsSourceKind.PrometheusApi) =>
        decodeUpstreamAuth[F](
          layers,
          s"$prefix.auth",
          "Prometheus API",
          policy,
          UpstreamAuthConfig.Mechanism.All
        ).map(
          _.andThen {
            case UpstreamAuthConfig.OAuth(endpoint, _, _, _)
                if !endpoint.value.toLowerCase(Locale.ROOT).startsWith("https://") =>
              ConfigProblem(
                s"$prefix.auth.tokenEndpoint",
                "must use HTTPS for OAuth client credentials",
                layers
                  .first(s"$prefix.auth.tokenEndpoint")
                  .map(_._1)
                  .getOrElse(ConfigSourceName.Default)
              ).invalidNel
            case auth => auth.validNel
          }
        )
      case _ => Async[F].pure(UpstreamAuthConfig.Anonymous.validNel)
    }
  }

  private def decodeMetricsSourceTls[F[_]: Async](
      layers: Layers,
      prefix: String
  ): F[Problems[HttpTlsConfig]] = {
    val kind = layers.first(s"$prefix.kind") match {
      case None => Some(MetricsSourceKind.Prometheus)
      case Some((_, raw)) => readMetricsSourceKind(raw).toOption
    }
    kind match {
      case Some(MetricsSourceKind.PrometheusApi) => decodeHttpTls[F](layers, s"$prefix.tls")
      case _ => Async[F].pure(HttpTlsConfig.Default.validNel)
    }
  }

  private def decodeHttpTls[F[_]: Async](layers: Layers, prefix: String): F[Problems[HttpTlsConfig]] = {
    val trustPrefix = s"$prefix.truststore"
    val keyPrefix = s"$prefix.keystore"
    val trustConfigured =
      HttpTlsConfig.keysUnder(prefix).filter(_.startsWith(trustPrefix)).exists(layers.first(_).isDefined)
    val keyConfigured =
      HttpTlsConfig.keysUnder(prefix).filter(_.startsWith(keyPrefix)).exists(layers.first(_).isDefined)

    for {
      truststore <-
        if trustConfigured then decodeHttpTrustStore[F](layers, trustPrefix).map(_.map(_.some))
        else Async[F].pure(none[HttpTrustStore].validNel)
      keystore <-
        if keyConfigured then decodeHttpKeyStore[F](layers, keyPrefix).map(_.map(_.some))
        else Async[F].pure(none[HttpKeyStore].validNel)
    } yield (truststore, keystore).mapN(HttpTlsConfig.apply)
  }

  private def decodeHttpTrustStore[F[_]: Async](
      layers: Layers,
      prefix: String
  ): F[Problems[HttpTrustStore]] =
    for {
      format <- readHttpStoreFormat[F](layers, s"$prefix.type")
      location <- readOptional[F, String](
        field(s"$prefix.location", "a non-empty readable path", readNonEmpty),
        layers
      )
      inline <- readOptionalUpstreamSecret[F](layers, s"$prefix.inline")
      password <- readUpstreamSecret[F](layers, s"$prefix.password")
      material = httpStoreMaterial(layers, prefix, location, inline)
    } yield (material, password, format).mapN(HttpTrustStore.apply)

  private def decodeHttpKeyStore[F[_]: Async](
      layers: Layers,
      prefix: String
  ): F[Problems[HttpKeyStore]] =
    for {
      format <- readHttpStoreFormat[F](layers, s"$prefix.type")
      location <- readOptional[F, String](
        field(s"$prefix.location", "a non-empty readable path", readNonEmpty),
        layers
      )
      inline <- readOptionalUpstreamSecret[F](layers, s"$prefix.inline")
      password <- readUpstreamSecret[F](layers, s"$prefix.password")
      keyPassword <- readUpstreamSecret[F](layers, s"$prefix.keyPassword")
      material = httpStoreMaterial(layers, prefix, location, inline)
    } yield (material, password, keyPassword, format).mapN(HttpKeyStore.apply)

  private def readHttpStoreFormat[F[_]: Async](layers: Layers, key: String): F[Problems[HttpStoreFormat]] =
    read[F, HttpStoreFormat](
      field(
        key,
        HttpStoreFormat.All.map(_.wireName).mkString(" or "),
        raw =>
          HttpStoreFormat
            .fromWire(raw)
            .toRight(s"'$raw' is not ${HttpStoreFormat.All.map(_.wireName).mkString(" or ")}")
      ),
      layers
    )

  private def readOptionalUpstreamSecret[F[_]: Async](
      layers: Layers,
      key: String
  ): F[Problems[Option[Secret[String]]]] =
    if layers.first(key).isEmpty then Async[F].pure(none[Secret[String]].validNel)
    else readUpstreamSecret[F](layers, key).map(_.map(_.some))

  private def httpStoreMaterial(
      layers: Layers,
      prefix: String,
      location: Problems[Option[String]],
      inline: Problems[Option[Secret[String]]]
  ): Problems[HttpStoreMaterial] =
    (location, inline).mapN((_, _)).andThen {
      case (Some(path), None) => HttpStoreMaterial.Location(path).validNel
      case (None, Some(material)) => HttpStoreMaterial.Inline(material).validNel
      case _ =>
        ConfigProblem(
          prefix,
          "must set exactly one of location or inline",
          List(s"$prefix.location", s"$prefix.inline")
            .flatMap(layers.first)
            .headOption
            .map(_._1)
            .getOrElse(ConfigSourceName.Default)
        ).invalidNel
    }

  private def validateMetricsSourceSecurity(
      layers: Layers,
      prefix: String,
      source: MetricsSourceSettings
  ): Problems[Unit] =
    source.kind match {
      case MetricsSourceKind.PrometheusApi =>
        val uri = URI.create(source.url.value)
        val baseRule = prometheusBaseUrlProblem(uri).fold(().validNel[ConfigProblem]) { problem =>
          ConfigProblem(
            s"$prefix.url",
            problem,
            layers.first(s"$prefix.url").map(_._1).getOrElse(ConfigSourceName.Default)
          ).invalidNel
        }
        val secure = Option(uri.getScheme).exists(_.equalsIgnoreCase("https"))
        val requiresHttps = source.auth match {
          case UpstreamAuthConfig.Anonymous => source.tls != HttpTlsConfig.Default
          case _ => true
        }
        val transportRule =
          if !requiresHttps || secure then ().validNel
          else
            ConfigProblem(
              s"$prefix.url",
              "must use HTTPS when authentication or custom TLS material is configured",
              layers.first(s"$prefix.url").map(_._1).getOrElse(ConfigSourceName.Default)
            ).invalidNel
        (baseRule, transportRule).mapN((_, _) => ())

      case _ => ().validNel
    }

  private def prometheusBaseUrlProblem(uri: URI): Option[String] = {
    val path = Option(uri.getPath).getOrElse("").reverse.dropWhile(_ == '/').reverse
    if Option(uri.getRawQuery).isDefined then
      Some("must not contain a query string; configure the Prometheus server base URL")
    else if Option(uri.getRawFragment).isDefined then
      Some("must not contain a fragment; configure the Prometheus server base URL")
    else if path.endsWith("/api/v1/query") || path.endsWith("/api/v1/query_range") then
      Some("must be a Prometheus server base URL, not an /api/v1/query or /api/v1/query_range endpoint")
    else None
  }

  private val MetricsSourcesPrefix: String = "kui.metrics.sources"

  private val MetricsSourceQueryLeafPaths: Set[String] = Set(
    "queryTimeout",
    "maxConcurrentQueries",
    "maxSeriesPerQuery",
    "maxPointsPerSeries",
    "maxResponseBytes",
    "maxCacheBytes",
    "cacheTtl",
    "staleTtl"
  )

  /** The fixed leaves used only to recover a direct source ID from flattened environment names.
    * Query/auth/TLS leaves are included before their decoders land so adding a nested setting cannot regress
    * source discovery while those slices are implemented incrementally.
    */
  private val MetricsSourceEnvLeafPaths: Set[String] = Set(
    "url",
    "kind",
    "callTimeout",
    "queryTimeout",
    "maxConcurrentQueries",
    "maxSeriesPerQuery",
    "maxPointsPerSeries",
    "maxResponseBytes",
    "maxCacheBytes",
    "cacheTtl",
    "staleTtl",
    "auth.type",
    "auth.username",
    "auth.password",
    "auth.token",
    "auth.tokenEndpoint",
    "auth.clientId",
    "auth.clientSecret",
    "auth.scope"
  ) ++ HttpTlsConfig.keysUnder("tls")

  private def readMetricsSourceKind(raw: String): Either[String, MetricsSourceKind] =
    MetricsSourceKind
      .fromWire(raw)
      .toRight(s"'$raw' is not ${MetricsSourceKind.All.map(_.wireName).mkString(" or ")}")

  /** The cross-field rules of `kui.metrics.*`.
    *
    * They read the layers rather than the decoded values, for the reason [[checkTopicRules]] gives: a rule
    * expressed over a decoded value goes unreported whenever some unrelated field is also wrong.
    */
  private def checkMetricsRules(layers: Layers): Problems[Unit] = {
    val scrapeInterval = effectiveDuration(
      layers,
      "kui.metrics.scrapeInterval",
      MetricsConfig.MinScrapeInterval,
      MetricsConfig.MaxScrapeInterval,
      MetricsConfig.DefaultScrapeInterval
    )
    val retention = effectiveDuration(
      layers,
      "kui.metrics.retention",
      MetricsConfig.MinRetention,
      MetricsConfig.MaxRetention,
      MetricsConfig.DefaultRetention
    )

    val windowRule = (scrapeInterval, retention) match {
      case (Some(interval), Some(window)) if window < interval =>
        ConfigProblem(
          "kui.metrics.retention",
          s"($window) must not be shorter than kui.metrics.scrapeInterval ($interval); a window that ends " +
            "before the next sample arrives retains nothing and every chart would be empty",
          layers.first("kui.metrics.retention").map(_._1).getOrElse(ConfigSourceName.Default)
        ).invalidNel
      case _ => ().validNel
    }

    // Exposition sources scrape on a cadence; API sources instead have a nested transport/query budget.
    val budgetRules = layers
      .mapMembersOf(MetricsSourcesPrefix, MetricsSourceEnvLeafPaths)
      .toList
      .sorted
      .map { name =>
        val prefix = s"$MetricsSourcesPrefix.$name"
        val key = s"$prefix.callTimeout"
        val kind = layers.first(s"$prefix.kind") match {
          case None => Some(MetricsSourceKind.Prometheus)
          case Some((_, raw)) => readMetricsSourceKind(raw).toOption
        }
        val callTimeout = effectiveDuration(
          layers,
          key,
          MetricsSourceSettings.MinCallTimeout,
          MetricsSourceSettings.MaxCallTimeout,
          MetricsSourceSettings.DefaultCallTimeout
        )
        kind match {
          case Some(MetricsSourceKind.PrometheusApi) =>
            val queryKey = s"$prefix.queryTimeout"
            val queryTimeout = effectiveDuration(
              layers,
              queryKey,
              MetricsSourceSettings.MinQueryTimeout,
              MetricsSourceSettings.MaxQueryTimeout,
              MetricsSourceSettings.DefaultQueryTimeout
            )
            val cacheKey = s"$prefix.cacheTtl"
            val cacheTtl = effectiveDuration(
              layers,
              cacheKey,
              MetricsSourceSettings.MinCacheTtl,
              MetricsSourceSettings.MaxCacheTtl,
              MetricsSourceSettings.DefaultCacheTtl
            )
            val staleKey = s"$prefix.staleTtl"
            val staleTtl = effectiveDuration(
              layers,
              staleKey,
              MetricsSourceSettings.MinStaleTtl,
              MetricsSourceSettings.MaxStaleTtl,
              MetricsSourceSettings.DefaultStaleTtl
            )
            val queryRule = (queryTimeout, callTimeout) match {
              case (Some(query), Some(call)) if query >= call =>
                ConfigProblem(
                  queryKey,
                  s"($query) must be shorter than $key ($call)",
                  layers.first(queryKey).map(_._1).getOrElse(ConfigSourceName.Default)
                ).invalidNel
              case _ => ().validNel
            }
            val staleRule = (cacheTtl, staleTtl) match {
              case (Some(fresh), Some(stale)) if stale < fresh =>
                ConfigProblem(
                  staleKey,
                  s"($stale) must not be shorter than $cacheKey ($fresh)",
                  layers.first(staleKey).map(_._1).getOrElse(ConfigSourceName.Default)
                ).invalidNel
              case _ => ().validNel
            }
            (queryRule, staleRule).mapN((_, _) => ())

          case Some(_) =>
            val scrapeRule = (scrapeInterval, callTimeout) match {
              case (Some(interval), Some(budget)) if budget >= interval =>
                val origin =
                  if layers.first("kui.metrics.scrapeInterval").isDefined then ""
                  else ", which is the default"
                ConfigProblem(
                  key,
                  s"($budget) must be shorter than kui.metrics.scrapeInterval " +
                    s"($interval$origin); a scrape that outlives its interval overlaps the next one",
                  layers.first(key).map(_._1).getOrElse(ConfigSourceName.Default)
                ).invalidNel
              case _ => ().validNel
            }
            val queryOnlyRules = MetricsSourceQueryLeafPaths.toList.sorted
              .flatMap { leaf =>
                val leafKey = s"$prefix.$leaf"
                layers.first(leafKey).map { (source, _) =>
                  ConfigProblem(
                    leafKey,
                    "is only valid for a prometheus-api metrics source",
                    source
                  )
                }
              }
            val queryOnlyRule = queryOnlyRules match {
              case Nil => ().validNel
              case first :: rest => NonEmptyList(first, rest).invalid[Unit]
            }
            val configuredAuthKeys = UpstreamAuthConfig
              .keysUnder(s"$prefix.auth")
              .filter(layers.first(_).isDefined)
            val authRule = configuredAuthKeys match {
              case Nil => ().validNel
              case _ =>
                ConfigProblem(
                  s"$prefix.auth.type",
                  "authentication is only valid for a prometheus-api metrics source",
                  configuredAuthKeys
                    .flatMap(layers.first)
                    .headOption
                    .map(_._1)
                    .getOrElse(ConfigSourceName.Default)
                ).invalidNel
            }
            val configuredTlsKeys = HttpTlsConfig.keysUnder(s"$prefix.tls").filter(layers.first(_).isDefined)
            val tlsRule = configuredTlsKeys match {
              case Nil => ().validNel
              case _ =>
                ConfigProblem(
                  s"$prefix.tls",
                  "TLS material is only valid for a prometheus-api metrics source",
                  configuredTlsKeys
                    .flatMap(layers.first)
                    .headOption
                    .map(_._1)
                    .getOrElse(ConfigSourceName.Default)
                ).invalidNel
            }
            (scrapeRule, queryOnlyRule, authRule, tlsRule).mapN((_, _, _, _) => ())

          case None => ().validNel
        }
      }
      .sequence_

    (windowRule, budgetRules).mapN((_, _) => ())
  }

  /** What one duration key is worth once the layers and the default are taken into account, and `None` when
    * the operator wrote something the field itself will already refuse.
    *
    * The `None` is the interesting half. A cross-field rule that compares an out-of-bounds value reports a
    * second problem about the same key — "your retention is below your interval" on top of "your interval is
    * above the maximum" — and the second sentence sends the operator looking for a mistake they did not make.
    * Staying quiet while the field speaks for itself is the discipline [[checkTopicRules]] describes.
    */
  private def effectiveDuration(
      layers: Layers,
      key: String,
      min: FiniteDuration,
      max: FiniteDuration,
      fallback: FiniteDuration
  ): Option[FiniteDuration] =
    layers.first(key) match {
      case None => Some(fallback)
      case Some((_, raw)) => readBoundedDuration(min, max)(raw).toOption
    }

  /** The `kui.alerts.*` slice: a retention window, a cadence, and the five numbers M8's rules compare
    * against. No rule is expressed here — see [[AlertThresholds]] for why that line is drawn where it is.
    */
  private def decodeAlerts[F[_]: Async](layers: Layers): F[Problems[AlertsConfig]] =
    for {
      retention <- read[F, FiniteDuration](
        field(
          "kui.alerts.retention",
          s"a duration between ${AlertsConfig.MinRetention} and ${AlertsConfig.MaxRetention}",
          readBoundedDuration(AlertsConfig.MinRetention, AlertsConfig.MaxRetention),
          AlertsConfig.DefaultRetention
        ),
        layers
      )
      evaluationInterval <- read[F, FiniteDuration](
        field(
          "kui.alerts.evaluationInterval",
          s"a duration between ${AlertsConfig.MinEvaluationInterval} and " +
            s"${AlertsConfig.MaxEvaluationInterval}",
          readBoundedDuration(AlertsConfig.MinEvaluationInterval, AlertsConfig.MaxEvaluationInterval),
          AlertsConfig.DefaultEvaluationInterval
        ),
        layers
      )
      thresholds <- decodeAlertThresholds[F](layers)
    } yield (retention, evaluationInterval, thresholds).mapN(AlertsConfig.apply)

  private def decodeAlertThresholds[F[_]: Async](layers: Layers): F[Problems[AlertThresholds]] = {
    val prefix = "kui.alerts.thresholds"
    val counts =
      s"a whole number between ${AlertThresholds.MinPartitionCount} and ${AlertThresholds.MaxPartitionCount}"
    val percent =
      s"a percentage between ${AlertThresholds.MinPercent} and ${AlertThresholds.MaxPercent}"
    for {
      offline <- read[F, Int](
        field(
          s"$prefix.offlinePartitions",
          counts,
          readBoundedInt(AlertThresholds.MinPartitionCount, AlertThresholds.MaxPartitionCount),
          AlertThresholds.DefaultOfflinePartitions
        ),
        layers
      )
      underReplicated <- read[F, Int](
        field(
          s"$prefix.underReplicatedPartitions",
          counts,
          readBoundedInt(AlertThresholds.MinPartitionCount, AlertThresholds.MaxPartitionCount),
          AlertThresholds.DefaultUnderReplicatedPartitions
        ),
        layers
      )
      rebalance <- read[F, FiniteDuration](
        field(
          s"$prefix.rebalanceDuration",
          s"a duration between ${AlertThresholds.MinRebalanceDuration} and " +
            s"${AlertThresholds.MaxRebalanceDuration}",
          readBoundedDuration(
            AlertThresholds.MinRebalanceDuration,
            AlertThresholds.MaxRebalanceDuration
          ),
          AlertThresholds.DefaultRebalanceDuration
        ),
        layers
      )
      diskWarning <- read[F, Int](
        field(
          s"$prefix.diskUsedWarningPercent",
          percent,
          readBoundedInt(AlertThresholds.MinPercent, AlertThresholds.MaxPercent),
          AlertThresholds.DefaultDiskUsedWarningPercent
        ),
        layers
      )
      diskCritical <- read[F, Int](
        field(
          s"$prefix.diskUsedCriticalPercent",
          percent,
          readBoundedInt(AlertThresholds.MinPercent, AlertThresholds.MaxPercent),
          AlertThresholds.DefaultDiskUsedCriticalPercent
        ),
        layers
      )
      rules = checkAlertRules(layers, prefix)
    } yield (offline, underReplicated, rebalance, diskWarning, diskCritical, rules).mapN(
      (offlineCount, underReplicatedCount, stuck, warning, critical, _) =>
        AlertThresholds(offlineCount, underReplicatedCount, stuck, warning, critical)
    )
  }

  /** A critical disk threshold at or below the warning one, which would mean the warning never fires. */
  private def checkAlertRules(layers: Layers, prefix: String): Problems[Unit] = {
    def percent(leaf: String, fallback: Int): Option[Int] =
      layers.first(s"$prefix.$leaf") match {
        case None => Some(fallback)
        case Some((_, raw)) =>
          readBoundedInt(AlertThresholds.MinPercent, AlertThresholds.MaxPercent)(raw).toOption
      }

    val warning = percent("diskUsedWarningPercent", AlertThresholds.DefaultDiskUsedWarningPercent)
    val critical = percent("diskUsedCriticalPercent", AlertThresholds.DefaultDiskUsedCriticalPercent)

    (warning, critical) match {
      case (Some(warn), Some(danger)) if danger <= warn =>
        ConfigProblem(
          s"$prefix.diskUsedCriticalPercent",
          s"($danger%) must be above $prefix.diskUsedWarningPercent ($warn%); a critical threshold at or " +
            "below the warning one means a directory becomes critical without ever having been a warning",
          layers.first(s"$prefix.diskUsedCriticalPercent").map(_._1).getOrElse(ConfigSourceName.Default)
        ).invalidNel
      case _ => ().validNel
    }
  }

  private def decodeProfileClient[F[_]: Async](
      layers: Layers,
      policy: UrlPolicy
  ): F[Problems[Option[ProfileClientConfig]]] = {
    val key = "kui.clusterProfiles.url"
    if layers.first(key).isEmpty then Async[F].pure(none[ProfileClientConfig].validNel)
    else
      for {
        url <- read[F, SafeUrl](
          field(key, "the cluster service's base URL, such as http://kui-cluster:8081", readUrl(policy)),
          layers
        )
        pollInterval <- read[F, FiniteDuration](
          field(
            "kui.clusterProfiles.pollInterval",
            s"a duration between ${ProfileClientConfig.MinPollInterval} and " +
              s"${ProfileClientConfig.MaxPollInterval}",
            readBoundedDuration(ProfileClientConfig.MinPollInterval, ProfileClientConfig.MaxPollInterval),
            ProfileClientConfig.DefaultPollInterval
          ),
          layers
        )
        requestTimeout <- read[F, FiniteDuration](
          field(
            "kui.clusterProfiles.requestTimeout",
            s"a duration between ${ProfileClientConfig.MinRequestTimeout} and " +
              s"${ProfileClientConfig.MaxRequestTimeout}",
            readBoundedDuration(ProfileClientConfig.MinRequestTimeout, ProfileClientConfig.MaxRequestTimeout),
            ProfileClientConfig.DefaultRequestTimeout
          ),
          layers
        )
        reconnectBackoff <- read[F, FiniteDuration](
          field(
            "kui.clusterProfiles.reconnectBackoff",
            s"a duration between ${ProfileClientConfig.MinBackoff} and ${ProfileClientConfig.MaxBackoff}",
            readBoundedDuration(ProfileClientConfig.MinBackoff, ProfileClientConfig.MaxBackoff),
            ProfileClientConfig.DefaultReconnectBackoff
          ),
          layers
        )
        maxReconnectBackoff <- read[F, FiniteDuration](
          field(
            "kui.clusterProfiles.maxReconnectBackoff",
            s"a duration between ${ProfileClientConfig.MinBackoff} and ${ProfileClientConfig.MaxBackoff}",
            readBoundedDuration(ProfileClientConfig.MinBackoff, ProfileClientConfig.MaxBackoff),
            ProfileClientConfig.DefaultMaxReconnectBackoff
          ),
          layers
        )
        startupTimeout <- read[F, FiniteDuration](
          field(
            "kui.clusterProfiles.startupTimeout",
            s"a duration between ${ProfileClientConfig.MinStartupTimeout} and " +
              s"${ProfileClientConfig.MaxStartupTimeout}",
            readBoundedDuration(
              ProfileClientConfig.MinStartupTimeout,
              ProfileClientConfig.MaxStartupTimeout
            ),
            ProfileClientConfig.DefaultStartupTimeout
          ),
          layers
        )
      } yield (url, pollInterval, requestTimeout, reconnectBackoff, maxReconnectBackoff, startupTimeout)
        .mapN(ProfileClientConfig.apply)
        .map(_.some)
  }

  /** The cross-field rules of `kui.topics.*`.
    *
    * They read the layers rather than the decoded values, for the reason [[checkStoreRules]] gives: a rule
    * expressed over a decoded value goes unreported whenever some unrelated field is also wrong, and an
    * operator fixing one key per restart is exactly what accumulating every problem exists to prevent. A
    * field that is itself out of bounds already carries its own problem, so the rule that would have used it
    * stays quiet rather than saying the same thing twice.
    */
  private def checkTopicRules(layers: Layers): Problems[Unit] = {
    def duration(key: String): Option[FiniteDuration] =
      layers.first(key).flatMap(value => readDuration(value._2).toOption)

    def pageSize(key: String): Option[Int] =
      layers.first(key).flatMap(value => value._2.toIntOption)

    val refreshInterval = duration("kui.topics.refreshInterval")
    val scrapeTimeout = duration("kui.topics.scrapeTimeout")

    val budgetRule =
      (
        refreshInterval.orElse(Some(TopicsConfig.DefaultRefreshInterval)),
        scrapeTimeout.orElse(Some(TopicsConfig.DefaultScrapeTimeout))
      ) match {
        case (Some(interval), Some(timeout)) if timeout >= interval =>
          ConfigProblem(
            "kui.topics.scrapeTimeout",
            s"($timeout) must be shorter than kui.topics.refreshInterval ($interval); a scrape that " +
              "outlives its interval overlaps the next one and doubles the load on the cluster",
            layers
              .first("kui.topics.scrapeTimeout")
              .map(_._1)
              .getOrElse(ConfigSourceName.Default)
          ).invalidNel
        case _ => ().validNel
      }

    val pageRule =
      (
        pageSize("kui.topics.defaultPageSize").orElse(Some(PageSize.Default.value)),
        pageSize("kui.topics.maxPageSize").orElse(Some(PageSize.Max.value))
      ) match {
        case (Some(default), Some(max)) if default > max =>
          ConfigProblem(
            "kui.topics.defaultPageSize",
            s"($default) must not exceed kui.topics.maxPageSize ($max)",
            layers
              .first("kui.topics.defaultPageSize")
              .map(_._1)
              .getOrElse(ConfigSourceName.Default)
          ).invalidNel
        case _ => ().validNel
      }

    val backoffRule =
      (
        duration("kui.clusterProfiles.reconnectBackoff")
          .orElse(Some(ProfileClientConfig.DefaultReconnectBackoff)),
        duration("kui.clusterProfiles.maxReconnectBackoff")
          .orElse(Some(ProfileClientConfig.DefaultMaxReconnectBackoff))
      ) match {
        case (Some(first), Some(cap)) if cap < first =>
          ConfigProblem(
            "kui.clusterProfiles.maxReconnectBackoff",
            s"($cap) must not be shorter than kui.clusterProfiles.reconnectBackoff ($first); a cap below " +
              "the first delay would make the backoff shrink instead of grow",
            layers
              .first("kui.clusterProfiles.maxReconnectBackoff")
              .map(_._1)
              .getOrElse(ConfigSourceName.Default)
          ).invalidNel
        case _ => ().validNel
      }

    (budgetRule, pageRule, backoffRule).mapN((_, _, _) => ())
  }

  private def readInternalPrefix(raw: String): Either[String, String] = {
    val trimmed = raw.trim
    if trimmed.isEmpty then
      Left("must not be empty; an empty prefix would make every topic on the cluster internal")
    else if trimmed.length > TopicsConfig.MaxInternalPrefixLength then
      Left(s"is ${trimmed.length} characters long, and the limit is ${TopicsConfig.MaxInternalPrefixLength}")
    else Right(trimmed)
  }

  private def readSearchMode(raw: String): Either[String, SearchMode] =
    SearchMode.fromWire(raw.trim.toLowerCase(Locale.ROOT)).toRight(s"'$raw' is neither plain nor fts")

  private def readPageSize(raw: String): Either[String, PageSize] =
    raw.toIntOption
      .toRight(s"'$raw' is not a whole number")
      .flatMap(PageSize.from(_).leftMap(_.message))

  private def decodeClusters[F[_]: Async](
      layers: Layers,
      policy: UrlPolicy
  ): F[Problems[List[ClusterConfig]]] = {
    val indices = layers.indicesOf(ClustersPrefix)
    indices
      .traverse(index => decodeCluster[F](layers, index, policy).map(_.map(index -> _)))
      .map(_.sequence)
      .map { drafts =>
        (
          drafts.andThen(rejectDuplicateIds),
          denseIndex(indices),
          rejectEnvironmentProperties(layers, indices)
        ).mapN((list, _, _) => list)
      }
  }

  private val ClustersPrefix: String = "kui.clusters"

  /** D-3: the index must be dense and start at zero.
    *
    * `kui.clusters.0` and `kui.clusters.2` with no `1` almost always means a deleted entry or a typo in an
    * environment variable name. Silently renumbering would hide both.
    */
  private def denseIndex(indices: List[Int]): Problems[Unit] =
    indices.zipWithIndex.collectFirst {
      case (configured, expected) if configured != expected =>
        val previous =
          if expected == 0 then "the list starts at 0" else s"$configured follows ${expected - 1}"
        ConfigProblem(
          s"$ClustersPrefix.$configured",
          s"expected clusters to be numbered from 0 with no gaps; $previous",
          ConfigSourceName.Default
        )
    } match {
      case None => ().validNel
      case Some(problem) => problem.invalidNel
    }

  /** D-4: `properties` is file-only, and an environment variable under it says so rather than being ignored.
    */
  private def rejectEnvironmentProperties(layers: Layers, indices: List[Int]): Problems[Unit] = {
    val offenders = indices.flatMap(index => layers.envNamesUnder(s"$ClustersPrefix.$index.properties"))
    offenders match {
      case Nil => ().validNel
      case names =>
        ConfigProblem(
          s"$ClustersPrefix.<n>.properties",
          s"cannot be set from the environment (${names.mkString(", ")}); a Kafka property name contains " +
            "dots that the KUI_* mapping cannot round-trip, so raw properties are read from a YAML file " +
            "only. Values under properties are literal text and are not resolved: env:NAME and file:/path " +
            "written there are sent to the Kafka client unchanged. A secret belongs on the typed security " +
            "keys, which do resolve those references",
          ConfigSourceName.Env
        ).invalidNel
    }
  }

  /** Two clusters that resolve to the same id, named by both of their configured names.
    *
    * The id is what every URL, cache key and RBAC rule is written against, so a collision would silently make
    * one of the two clusters unreachable. Naming both is what tells the operator which one to give an
    * explicit `id`.
    */
  private def rejectDuplicateIds(clusters: List[(Int, ClusterConfig)]): Problems[List[ClusterConfig]] =
    clusters
      .groupBy((_, cluster) => cluster.id)
      .toList
      .sortBy((id, _) => id.value)
      .collect {
        case (id, clashing) if clashing.sizeIs > 1 =>
          ConfigProblem(
            s"$ClustersPrefix.${clashing.map((index, _) => index).min}.id",
            s"'${id.value}' is used by more than one cluster " +
              s"(${clashing.map((_, cluster) => s"'${cluster.name}'").mkString(", ")}); " +
              "set kui.clusters.<n>.id explicitly on all but one of them",
            ConfigSourceName.Default
          )
      } match {
      case Nil => clusters.map((_, cluster) => cluster).validNel
      case first :: rest => cats.data.Validated.Invalid(NonEmptyList(first, rest))
    }

  /** One cluster, with its `env:` and `file:` secret references already followed.
    *
    * Resolving here rather than in [[resolveSecrets]] is deliberate. That second phase runs only when the
    * *whole* decode succeeded, so a missing `KUI_PROD_PASSWORD` on cluster 1 would be hidden by an unrelated
    * typo on cluster 0 and only appear on the next restart — which is exactly the one-problem-per-restart
    * loop the accumulate-everything discipline exists to prevent. Reading a secret is already an effect here,
    * so nothing is lost by doing it in place.
    */
  private def decodeCluster[F[_]: Async](
      layers: Layers,
      index: Int,
      policy: UrlPolicy
  ): F[Problems[ClusterConfig]] = {
    val prefix = s"$ClustersPrefix.$index"
    for {
      name <- read[F, String](
        field(
          s"$prefix.name",
          s"a display name of 1 to ${ClusterConfig.MaxNameLength} characters",
          readClusterName
        ),
        layers
      )
      servers <- read[F, BootstrapServers](
        field(
          s"$prefix.bootstrapServers",
          "a comma-separated list of host:port entries",
          readBootstrapServers
        ),
        layers
      )
      readOnly <- read[F, Boolean](field(s"$prefix.readOnly", "true or false", readBoolean, false), layers)
      security <- ClusterSecurityConfig
        .decode(s"$prefix.security", layers.first)
        .traverse(draft => ClusterSecurityConfig.resolve[F](draft, layers.env))
        .map(_.andThen(identity))
      admin <- decodeAdminTuning[F](layers, index)
      registry <- decodeSchemaRegistry[F](layers, s"$prefix.schemaRegistry", policy)
      connect <- decodeConnectClusters[F](layers, s"$prefix.connect", policy)
      ksql <- decodeKsql[F](layers, s"$prefix.ksql", policy)
      serde <- decodeClusterSerde[F](layers, s"$prefix.serde")
      masking <- decodeMasking[F](layers, s"$prefix.masking")
      properties = ClientProperties.fromRaw(propertiesUnder(layers, s"$prefix.properties"))
    } yield (name, servers, readOnly, security, admin, registry, connect, ksql, serde, masking).tupled
      .andThen {
        (
            clusterName,
            bootstrap,
            readonly,
            sec,
            tuning,
            schemaRegistry,
            connectClusters,
            ksqldb,
            serdes,
            maskingRules
        ) =>
          // The id is derived last, and only from a name that decoded: deriving it from a name that did
          // not would report the same bad name twice, once under `.name` and once under `.id`.
          clusterId(layers, prefix, clusterName).map { id =>
            ClusterConfig(
              id = id,
              name = clusterName,
              bootstrapServers = bootstrap,
              security = sec,
              properties = properties,
              readOnly = readonly,
              admin = tuning,
              serde = serdes,
              schemaRegistry = schemaRegistry,
              connect = connectClusters,
              ksql = ksqldb,
              masking = maskingRules
            )
          }
      }
  }

  // -------------------------------------------------------------------------------------------
  // `kui.clusters.<n>.schemaRegistry`: the optional Schema Registry attached to one cluster
  // -------------------------------------------------------------------------------------------

  /** The registry block, which is absent far more often than it is present.
    *
    * `url` being set is the on/off switch, and its absence is not a problem to report: a cluster with no
    * Schema Registry is an ordinary cluster, and the schema service answers `not_configured` for it. That is
    * the whole reason this returns an `Option` rather than a value with empty defaults — an empty address
    * list would make "no registry" indistinguishable from "a registry KUI cannot reach", and those two must
    * look different on a screen (ADR-032).
    */
  private def decodeSchemaRegistry[F[_]: Async](
      layers: Layers,
      prefix: String,
      policy: UrlPolicy
  ): F[Problems[Option[SchemaRegistrySettings]]] = {
    val urlKey = s"$prefix.url"
    if layers.first(urlKey).isEmpty then Async[F].pure(none[SchemaRegistrySettings].validNel)
    else
      for {
        urls <- read[F, NonEmptyList[SafeUrl]](
          field(
            urlKey,
            "one or more http or https URLs this deployment is allowed to call, separated by commas",
            readUpstreamUrls(policy)
          ),
          layers
        )
        callTimeout <- read[F, FiniteDuration](
          field(
            s"$prefix.callTimeout",
            s"a duration between ${SchemaRegistrySettings.MinCallTimeout} and " +
              s"${SchemaRegistrySettings.MaxCallTimeout}",
            readBoundedDuration(
              SchemaRegistrySettings.MinCallTimeout,
              SchemaRegistrySettings.MaxCallTimeout
            ),
            SchemaRegistrySettings.DefaultCallTimeout
          ),
          layers
        )
        auth <- decodeRegistryAuth[F](layers, s"$prefix.auth", policy)
      } yield (urls, callTimeout, auth).mapN((addresses, timeout, credentials) =>
        Some(SchemaRegistrySettings(addresses, credentials, timeout))
      )
  }

  // -------------------------------------------------------------------------------------------
  // `kui.clusters.<n>.connect`: the Kafka Connect clusters attached to one cluster
  // -------------------------------------------------------------------------------------------

  /** The Connect list, which is empty far more often than it is not.
    *
    * A *list*, unlike the registry, because one set of brokers routinely has a source Connect cluster and a
    * sink one; and dense, like `kui.clusters[]` itself, because the entry's default name is derived from its
    * index and renumbering it silently would rename a Connect cluster that RBAC rules and bookmarks already
    * point at.
    *
    * An empty list is not a problem to report. A cluster with no Connect cluster is an ordinary cluster: the
    * connect service answers `not_configured` and ADR-032's rule hides the drawer row, rather than showing a
    * row that can never fill.
    */
  private def decodeConnectClusters[F[_]: Async](
      layers: Layers,
      prefix: String,
      policy: UrlPolicy
  ): F[Problems[List[ConnectClusterSettings]]] = {
    val indices = layers.indicesOf(prefix)
    indices
      .traverse(index => decodeConnectCluster[F](layers, prefix, index, policy))
      .map(_.sequence)
      .map { entries =>
        (
          entries.andThen(rejectDuplicateConnectNames(prefix, _)),
          denseListIndex(prefix, "Connect clusters", indices)
        ).mapN((connect, _) => connect)
      }
  }

  private def decodeConnectCluster[F[_]: Async](
      layers: Layers,
      prefix: String,
      index: Int,
      policy: UrlPolicy
  ): F[Problems[ConnectClusterSettings]] = {
    val entry = s"$prefix.$index"
    for {
      name <- read[F, ConnectName](
        field(
          s"$entry.name",
          "a name for this Connect cluster, which is what URLs and RBAC rules address it by",
          readConnectName,
          ConnectClusterSettings.defaultName(index)
        ),
        layers
      )
      urls <- read[F, NonEmptyList[SafeUrl]](
        field(
          s"$entry.url",
          "one or more http or https URLs this deployment is allowed to call, separated by commas",
          readUpstreamUrls(policy)
        ),
        layers
      )
      callTimeout <- read[F, FiniteDuration](
        field(
          s"$entry.callTimeout",
          s"a duration between ${ConnectClusterSettings.MinCallTimeout} and " +
            s"${ConnectClusterSettings.MaxCallTimeout}",
          readBoundedDuration(
            ConnectClusterSettings.MinCallTimeout,
            ConnectClusterSettings.MaxCallTimeout
          ),
          ConnectClusterSettings.DefaultCallTimeout
        ),
        layers
      )
      auth <- decodeUpstreamAuth[F](
        layers,
        s"$entry.auth",
        "Kafka Connect cluster",
        policy,
        UpstreamAuthConfig.Mechanism.LegacyHttp
      )
    } yield (name, urls, callTimeout, auth).mapN((connectName, addresses, timeout, credentials) =>
      ConnectClusterSettings(connectName, addresses, credentials, timeout)
    )
  }

  /** Two Connect clusters that answer to the same name, named by the entries that clash.
    *
    * The name is what a URL, a cache key and an RBAC rule are written against, exactly as a `ClusterId` is,
    * so a collision would silently make one of the two unreachable. The default name is the index and cannot
    * collide, so this only ever fires on names an operator wrote.
    */
  private def rejectDuplicateConnectNames(
      prefix: String,
      entries: List[ConnectClusterSettings]
  ): Problems[List[ConnectClusterSettings]] =
    entries.zipWithIndex
      .groupBy((settings, _) => settings.name)
      .toList
      .sortBy((name, _) => name.value)
      .collect {
        case (name, clashing) if clashing.sizeIs > 1 =>
          ConfigProblem(
            s"$prefix.${clashing.map((_, index) => index).min}.name",
            s"'${name.value}' names more than one Connect cluster " +
              s"(entries ${clashing.map((_, index) => index).sorted.mkString(", ")}); a connector is " +
              "addressed as (Connect cluster, connector), so two clusters with one name make one of them " +
              "unreachable",
            ConfigSourceName.Default
          )
      } match {
      case Nil => entries.validNel
      case first :: rest => cats.data.Validated.Invalid(NonEmptyList(first, rest))
    }

  private def readConnectName(raw: String): Either[String, ConnectName] =
    ConnectName.from(raw.trim).leftMap(_.message)

  // -------------------------------------------------------------------------------------------
  // `kui.clusters.<n>.ksql`: the optional ksqlDB cluster attached to one cluster
  // -------------------------------------------------------------------------------------------

  /** The ksqlDB block, which is optional and singular.
    *
    * Singular where Connect is plural because a second ksqlDB cluster over the same brokers shares the same
    * command topic and is the same logical service; `url` being set is the on/off switch, and its absence is
    * an ordinary cluster rather than a problem to report.
    */
  private def decodeKsql[F[_]: Async](
      layers: Layers,
      prefix: String,
      policy: UrlPolicy
  ): F[Problems[Option[KsqlSettings]]] = {
    val urlKey = s"$prefix.url"
    if layers.first(urlKey).isEmpty then Async[F].pure(none[KsqlSettings].validNel)
    else
      for {
        urls <- read[F, NonEmptyList[SafeUrl]](
          field(
            urlKey,
            "one or more http or https URLs this deployment is allowed to call, separated by commas",
            readUpstreamUrls(policy)
          ),
          layers
        )
        callTimeout <- read[F, FiniteDuration](
          field(
            s"$prefix.callTimeout",
            s"a duration between ${KsqlSettings.MinCallTimeout} and ${KsqlSettings.MaxCallTimeout}",
            readBoundedDuration(KsqlSettings.MinCallTimeout, KsqlSettings.MaxCallTimeout),
            KsqlSettings.DefaultCallTimeout
          ),
          layers
        )
        streamTimeout <- read[F, FiniteDuration](
          field(
            s"$prefix.streamTimeout",
            s"a duration between ${KsqlSettings.MinStreamTimeout} and ${KsqlSettings.MaxStreamTimeout}",
            readBoundedDuration(KsqlSettings.MinStreamTimeout, KsqlSettings.MaxStreamTimeout),
            KsqlSettings.DefaultStreamTimeout
          ),
          layers
        )
        auth <- decodeUpstreamAuth[F](
          layers,
          s"$prefix.auth",
          "ksqlDB cluster",
          policy,
          UpstreamAuthConfig.Mechanism.LegacyHttp
        )
      } yield (urls, callTimeout, streamTimeout, auth, checkKsqlTimeouts(layers, prefix)).mapN(
        (addresses, call, stream, credentials, _) => Some(KsqlSettings(addresses, credentials, call, stream))
      )
  }

  /** A push query's budget shorter than a listing's is refused, naming both keys.
    *
    * The two keys exist because the two promises differ, and a `streamTimeout` below `callTimeout` inverts
    * them: the query that by definition does not finish would be cut off before the listing that must. The
    * message names the other key and its effective value, including when that value is the default, which is
    * the case the operator cannot see for themselves — the rule `kui.clusters.<n>.admin` already follows.
    */
  private def checkKsqlTimeouts(layers: Layers, prefix: String): Problems[Unit] = {
    val call = effectiveDuration(
      layers,
      s"$prefix.callTimeout",
      KsqlSettings.MinCallTimeout,
      KsqlSettings.MaxCallTimeout,
      KsqlSettings.DefaultCallTimeout
    )
    val stream = effectiveDuration(
      layers,
      s"$prefix.streamTimeout",
      KsqlSettings.MinStreamTimeout,
      KsqlSettings.MaxStreamTimeout,
      KsqlSettings.DefaultStreamTimeout
    )

    (call, stream) match {
      case (Some(ordinary), Some(push)) if push < ordinary =>
        val origin =
          if layers.first(s"$prefix.callTimeout").isDefined then "" else ", which is the default"
        ConfigProblem(
          s"$prefix.streamTimeout",
          s"expected a duration at least as long as $prefix.callTimeout ($ordinary$origin); got $push. A " +
            "push query is the request that is meant to outlive an ordinary one",
          layers.first(s"$prefix.streamTimeout").map(_._1).getOrElse(ConfigSourceName.Default)
        ).invalidNel
      case _ => ().validNel
    }
  }

  /** D-3's rule, for any list that is not `kui.clusters[]` itself: dense, and starting at zero.
    *
    * A `connect.0` and a `connect.2` with no `1` almost always means a deleted entry or a typo in an
    * environment variable name, and silently renumbering would hide both — and here it would also rename the
    * entry that took its default name from the index.
    */
  private def denseListIndex(prefix: String, what: String, indices: List[Int]): Problems[Unit] =
    indices.zipWithIndex.collectFirst {
      case (configured, expected) if configured != expected =>
        ConfigProblem(
          s"$prefix.$configured",
          s"expected $what to be numbered from 0 with no gaps; " +
            (if expected == 0 then "the list starts at 0" else s"$configured follows ${expected - 1}"),
          ConfigSourceName.Default
        )
    } match {
      case None => ().validNel
      case Some(problem) => problem.invalidNel
    }

  // -------------------------------------------------------------------------------------------
  // `kui.clusters.<n>.serde`: which serde reads which topic (SD-003)
  // -------------------------------------------------------------------------------------------

  /** The serde block, which is what lets an operator say "this cluster is Avro" once.
    *
    * Every key in it is optional and the whole section is optional, so a cluster without one behaves exactly
    * as every cluster behaved before this slice existed: auto-detection from the bytes, then `String`, then
    * the fallback that cannot fail. What it adds is the two rungs of `SerdeResolution`'s order that had no
    * way to be filled in — the per-topic patterns and the cluster default — which is why the resolution table
    * has been correct and unreachable since it was written.
    *
    * Nothing here checks that the named serde will actually be *available*: `SchemaRegistry` is only built
    * when `schemaRegistry.url` is set and the registry answers, and a default naming a serde that could not
    * be built falls through to the next rung rather than failing the browse (`SerdeResolution.resolve`).
    * Refusing the combination at load time would stop KUI from starting because a registry was down, which is
    * the opposite of what this product promises.
    */
  private def decodeClusterSerde[F[_]: Async](
      layers: Layers,
      prefix: String
  ): F[Problems[ClusterSerdeConfig]] =
    for {
      defaultKey <- readOptional[F, SerdeName](
        field(s"$prefix.defaultKey", "a serde name such as String or SchemaRegistry", readSerdeName),
        layers
      )
      defaultValue <- readOptional[F, SerdeName](
        field(s"$prefix.defaultValue", "a serde name such as String or SchemaRegistry", readSerdeName),
        layers
      )
      schemaCacheSize <- read[F, Long](
        field(
          s"$prefix.schemaCacheSize",
          s"a whole number between ${ClusterSerdeConfig.MinSchemaCacheSize} and " +
            s"${ClusterSerdeConfig.MaxSchemaCacheSize}",
          readBoundedLong(ClusterSerdeConfig.MinSchemaCacheSize, ClusterSerdeConfig.MaxSchemaCacheSize),
          ClusterSerdeConfig.DefaultSchemaCacheSize
        ),
        layers
      )
      subjectCacheTtl <- read[F, FiniteDuration](
        field(
          s"$prefix.subjectCacheTtl",
          s"a duration between ${ClusterSerdeConfig.MinSubjectCacheTtl} and " +
            s"${ClusterSerdeConfig.MaxSubjectCacheTtl}",
          readBoundedDuration(
            ClusterSerdeConfig.MinSubjectCacheTtl,
            ClusterSerdeConfig.MaxSubjectCacheTtl
          ),
          ClusterSerdeConfig.DefaultSubjectCacheTtl
        ),
        layers
      )
      patterns <- decodeSerdePatterns[F](layers, s"$prefix.patterns")
    } yield (defaultKey, defaultValue, schemaCacheSize, subjectCacheTtl, patterns).mapN(
      (key, value, cacheSize, ttl, rules) => ClusterSerdeConfig(key, value, rules, cacheSize, ttl)
    )

  /** The pattern list, in the order the operator wrote it.
    *
    * Order is load-bearing: `SerdeResolution` takes the *first* matching pattern, so two overlapping entries
    * mean the earlier one wins. That is why this reads a dense numeric index and refuses a gap, exactly as
    * the cluster list itself does — a `patterns.0` and a `patterns.2` with no `1` is a deleted entry or a
    * mistyped environment variable, and renumbering it silently would change which serde reads a topic.
    */
  private def decodeSerdePatterns[F[_]: Async](
      layers: Layers,
      prefix: String
  ): F[Problems[List[SerdePatternConfig]]] = {
    val indices = layers.indicesOf(prefix)
    indices
      .traverse(index => decodeSerdePattern[F](layers, prefix, index))
      .map(_.sequence)
      .map(entries => (entries, denseListIndex(prefix, "serde patterns", indices)).mapN((rules, _) => rules))
  }

  private def decodeSerdePattern[F[_]: Async](
      layers: Layers,
      prefix: String,
      index: Int
  ): F[Problems[SerdePatternConfig]] = {
    val entry = s"$prefix.$index"
    for {
      serde <- read[F, SerdeName](
        field(s"$entry.serde", "a serde name such as String or SchemaRegistry", readSerdeName),
        layers
      )
      keys <- readOptional[F, Regex](
        field(s"$entry.topicKeysPattern", "a regular expression matching whole topic names", readPattern),
        layers
      )
      values <- readOptional[F, Regex](
        field(s"$entry.topicValuesPattern", "a regular expression matching whole topic names", readPattern),
        layers
      )
    } yield (serde, keys, values)
      .mapN(SerdePatternConfig.apply)
      .andThen(rule =>
        SerdePatternConfig.validate(index, rule) match {
          case Right(valid) => valid.validNel
          case Left(problem) =>
            ConfigProblem(entry, problem, ConfigSourceName.Default).invalidNel
        }
      )
  }

  // -------------------------------------------------------------------------------------------
  // `kui.clusters.<n>.masking`: which fields never leave the service in full (DM-001, ADR-023)
  // -------------------------------------------------------------------------------------------

  /** The masking block, which is the first thing in this repository that can define a masking rule.
    *
    * Absent for almost every cluster, and absent is free: with no rules `MaskingEngine.applies` is false, the
    * browse takes its fast path and no record is re-walked. That is why this returns [[MaskingConfig.empty]]
    * for a missing section rather than reporting a problem.
    *
    * ==Why the whole rule is built here, at load time==
    *
    * Because `MaskingEngine`'s own scaladoc promises it: *"The one dangerous case — a rule that matches
    * nothing because of a typo — cannot be caught here at all; it is caught at startup, where an unusable
    * regex is a configuration error."* Until this section existed there was no startup to catch it at. A rule
    * whose pattern will not compile, whose `kind` is misspelled, or which names both `fields` and
    * `fieldsNamePattern` now stops the process with the key in the message, instead of loading and protecting
    * nothing — which is the failure an operator discovers by reading their own protected data on a screen.
    */
  private def decodeMasking[F[_]: Async](
      layers: Layers,
      prefix: String
  ): F[Problems[MaskingConfig]] = {
    val indices = layers.indicesOf(prefix)
    indices
      .traverse(index => decodeMaskingRule[F](layers, prefix, index))
      .map(_.sequence)
      .map(entries =>
        (entries, denseListIndex(prefix, "masking rules", indices))
          .mapN((rules, _) => MaskingConfig(rules))
      )
  }

  /** One `masking[]` entry.
    *
    * Order is load-bearing exactly as it is for serde patterns, and for a sharper reason: a JSON value gets
    * **every** matching rule in configuration order, so "replace this field" followed by "mask everything
    * else" composes, and the two written the other way round do not compose the same way. A gap in the index
    * is therefore refused rather than renumbered.
    */
  private def decodeMaskingRule[F[_]: Async](
      layers: Layers,
      prefix: String,
      index: Int
  ): F[Problems[MaskingRule]] = {
    val entry = s"$prefix.$index"
    for {
      kind <- read[F, MaskingConfig.Kind](
        field(s"$entry.kind", "one of remove, mask or replace", MaskingConfig.readKind),
        layers
      )
      fields <- readOptional[F, NonEmptyList[String]](
        field(s"$entry.fields", "a list of field names", MaskingConfig.readFields),
        layers
      )
      fieldsPattern <- readOptional[F, Regex](
        field(
          s"$entry.fieldsNamePattern",
          "a regular expression matching whole field names",
          MaskingConfig.readPattern
        ),
        layers
      )
      keys <- readOptional[F, Regex](
        field(
          s"$entry.topicKeysPattern",
          "a regular expression matching whole topic names",
          MaskingConfig.readPattern
        ),
        layers
      )
      values <- readOptional[F, Regex](
        field(
          s"$entry.topicValuesPattern",
          "a regular expression matching whole topic names",
          MaskingConfig.readPattern
        ),
        layers
      )
      // `readNonEmpty` and not a bare identity: `replacement: ""` is a `remove` written as a `replace`,
      // and an operator who typed an empty string into a masking rule did not mean "reveal the shape of
      // the field and nothing else".
      replacement <- readOptional[F, String](
        field(s"$entry.replacement", "the literal a matched field becomes", readNonEmpty),
        layers
      )
      replacementChars <- readOptional[F, String](
        field(
          s"$entry.maskingCharsReplacement",
          "the characters a mask cycles through, such as *",
          readNonEmpty
        ),
        layers
      )
      keepPrefix <- readOptional[F, Int](
        field(
          s"$entry.keep.prefix",
          s"a whole number between 0 and ${MaskingConfig.MaxKeep}",
          MaskingConfig.readKeep
        ),
        layers
      )
      keepSuffix <- readOptional[F, Int](
        field(
          s"$entry.keep.suffix",
          s"a whole number between 0 and ${MaskingConfig.MaxKeep}",
          MaskingConfig.readKeep
        ),
        layers
      )
    } yield (
      kind,
      fields,
      fieldsPattern,
      keys,
      values,
      replacement,
      replacementChars,
      keepPrefix,
      keepSuffix
    ).mapN(MaskingConfig.Draft.apply)
      .andThen(draft =>
        MaskingConfig.validate(draft) match {
          case Right(rule) => rule.validNel
          case Left(problem) => ConfigProblem(entry, problem, ConfigSourceName.Default).invalidNel
        }
      )
  }

  private def readSerdeName(raw: String): Either[String, SerdeName] =
    ClusterSerdeConfig.readSerdeName(raw)

  private def readPattern(raw: String): Either[String, Regex] = ClusterSerdeConfig.readPattern(raw)

  private def readBoundedLong(min: Long, max: Long)(raw: String): Either[String, Long] =
    raw.toLongOption.toRight(s"'$raw' is not a whole number").flatMap { value =>
      if value < min then Left(s"$value is below the minimum of $min")
      else if value > max then Left(s"$value is above the maximum of $max")
      else Right(value)
    }

  /** A comma-separated address list, in preference order.
    *
    * Comma-separated rather than one key per address because that is how every other list-valued key in KUI
    * is spelled, and because a YAML list of scalars already arrives here as a comma-joined string
    * (`Layers.scalar`), so both spellings work with one reader.
    */
  private def readUpstreamUrls(policy: UrlPolicy)(raw: String): Either[String, NonEmptyList[SafeUrl]] =
    raw.split(',').toList.map(_.trim).filter(_.nonEmpty) match {
      case Nil => Left("must name at least one address")
      case addresses =>
        addresses
          .traverse(readUrl(policy))
          .flatMap(urls => NonEmptyList.fromList(urls).toRight("must name at least one address"))
    }

  /** Basic, OAuth client credentials, or nothing — chosen by `auth.type` and never inferred.
    *
    * ADR-014's "never both" is enforced here rather than left to the operator: a configuration that names a
    * username *and* a client secret is refused with a message saying which keys are surplus, instead of one
    * of the two silently losing. The one that loses is always the one somebody changes when the other
    * expires, and the resulting outage looks like an upstream problem rather than a KUI configuration
    * problem.
    *
    * One decoder for every HTTP dependency a cluster has. A Schema Registry, a Kafka Connect cluster and a
    * ksqlDB cluster accept exactly the same three spellings, and three decoders for one shape is precisely
    * what ADR-013's "one hand-written loader per field" discipline exists to make visible. `dependency`
    * reaches nothing but the sentence an operator reads, so each upstream is named by the thing they
    * configured rather than by the type this returns.
    */
  private def decodeUpstreamAuth[F[_]: Async](
      layers: Layers,
      prefix: String,
      dependency: String,
      policy: UrlPolicy,
      allowedMechanisms: Set[UpstreamAuthConfig.Mechanism]
  ): F[Problems[UpstreamAuthConfig]] = {
    val typeKey = s"$prefix.type"
    val basicKeys = List("username", "password").map(leaf => s"$prefix.$leaf")
    val bearerKeys = List(s"$prefix.token")
    val oauthKeys = List("tokenEndpoint", "clientId", "clientSecret", "scope").map(leaf => s"$prefix.$leaf")

    /** Keys that belong to a mechanism other than the one chosen. */
    def surplus(allowed: List[String], chosen: String): Problems[Unit] =
      (basicKeys ++ bearerKeys ++ oauthKeys)
        .filterNot(allowed.contains)
        .filter(layers.first(_).isDefined) match {
        case Nil => ().validNel
        case offenders =>
          ConfigProblem(
            typeKey,
            s"is '$chosen', so ${offenders.mkString(", ")} " +
              s"${if offenders.sizeIs == 1 then "is" else "are"} not read. KUI authenticates to a " +
              s"$dependency with exactly one authentication mechanism, never both; remove " +
              "the keys belonging to the mechanism you are not using",
            layers.first(typeKey).map(_._1).getOrElse(ConfigSourceName.Default)
          ).invalidNel
      }

    read[F, UpstreamAuthConfig.Mechanism](
      field(
        typeKey,
        "none, basic, bearer or oauth",
        raw =>
          UpstreamAuthConfig
            .fromWire(raw)
            .toRight(s"'$raw' is not none, basic, bearer or oauth"),
        UpstreamAuthConfig.Mechanism.None
      ),
      layers
    ).flatMap {
      case cats.data.Validated.Invalid(problems) =>
        Async[F].pure(cats.data.Validated.Invalid(problems))

      case cats.data.Validated.Valid(mechanism) if !allowedMechanisms.contains(mechanism) =>
        Async[F].pure(
          ConfigProblem(
            typeKey,
            s"is '${mechanism.wireName}', which is not supported for this $dependency",
            layers.first(typeKey).map(_._1).getOrElse(ConfigSourceName.Default)
          ).invalidNel
        )

      case cats.data.Validated.Valid(UpstreamAuthConfig.Mechanism.None) =>
        Async[F].pure(surplus(Nil, "none").map(_ => UpstreamAuthConfig.Anonymous))

      case cats.data.Validated.Valid(UpstreamAuthConfig.Mechanism.Basic) =>
        for {
          username <- read[F, String](
            field(s"$prefix.username", s"the user name the $dependency knows KUI by", readNonEmpty),
            layers
          )
          password <- readUpstreamSecret[F](layers, s"$prefix.password")
        } yield (username, password, surplus(basicKeys, "basic"))
          .mapN((user, secret, _) => UpstreamAuthConfig.Basic(user, secret))

      case cats.data.Validated.Valid(UpstreamAuthConfig.Mechanism.Bearer) =>
        readUpstreamSecret[F](layers, s"$prefix.token")
          .map((_, surplus(bearerKeys, "bearer")).mapN((token, _) => UpstreamAuthConfig.Bearer(token)))

      case cats.data.Validated.Valid(UpstreamAuthConfig.Mechanism.OAuth) =>
        for {
          endpoint <- read[F, SafeUrl](
            field(
              s"$prefix.tokenEndpoint",
              "the OAuth token endpoint, such as https://login.example.com/oauth2/token",
              readUrl(policy)
            ),
            layers
          )
          clientId <- read[F, String](field(s"$prefix.clientId", "the OAuth client id", readNonEmpty), layers)
          clientSecret <- readUpstreamSecret[F](layers, s"$prefix.clientSecret")
          scope <- readOptional[F, String](
            field(s"$prefix.scope", "the scope to request, if the issuer needs one", readNonEmpty),
            layers
          )
        } yield (endpoint, clientId, clientSecret, scope, surplus(oauthKeys, "oauth"))
          .mapN((url, id, secret, requested, _) => UpstreamAuthConfig.OAuth(url, id, secret, requested))
    }
  }

  /** The registry's own spelling of those same three cases.
    *
    * `RegistryAuthConfig` predates [[UpstreamAuthConfig]] and is the type `services/schema` names, so it
    * stays and this is a projection rather than a second decoder. The two enums should become one the next
    * time that service is opened; what matters here is that the *decoding* — which keys, which mechanism,
    * which mistake is refused — has only one implementation.
    */
  private def decodeRegistryAuth[F[_]: Async](
      layers: Layers,
      prefix: String,
      policy: UrlPolicy
  ): F[Problems[RegistryAuthConfig]] =
    decodeUpstreamAuth[F](
      layers,
      prefix,
      "Schema Registry",
      policy,
      UpstreamAuthConfig.Mechanism.LegacyHttp
    ).map(
      _.andThen {
        case UpstreamAuthConfig.Anonymous => RegistryAuthConfig.Anonymous.validNel
        case UpstreamAuthConfig.Basic(username, password) =>
          RegistryAuthConfig.Basic(username, password).validNel
        case UpstreamAuthConfig.OAuth(endpoint, clientId, clientSecret, scope) =>
          RegistryAuthConfig.OAuth(endpoint, clientId, clientSecret, scope).validNel
        case UpstreamAuthConfig.Bearer(_) =>
          ConfigProblem(
            s"$prefix.type",
            "is 'bearer', which is not supported for this Schema Registry",
            layers.first(s"$prefix.type").map(_._1).getOrElse(ConfigSourceName.Default)
          ).invalidNel
      }
    )

  /** One upstream credential, with its `env:` or `file:` reference already followed.
    *
    * Resolved here rather than in [[resolveSecrets]] for the reason [[decodeCluster]] gives about every other
    * per-cluster secret: that second phase runs only when the whole decode succeeded, so a missing
    * environment variable on one cluster would be hidden by an unrelated typo on another and would only
    * appear on the next restart.
    */
  private def readUpstreamSecret[F[_]: Async](
      layers: Layers,
      key: String
  ): F[Problems[Secret[String]]] =
    read[F, SecretRef](
      Field(
        key,
        "a literal value, env:NAME or file:/path",
        raw => Right(SecretRef.parse(raw)),
        None,
        secret = true
      ),
      layers
    ).flatMap {
      case cats.data.Validated.Valid(ref) =>
        SecretRef.resolve[F](ref, layers.env).map {
          case Right(secret) => secret.validNel
          case Left(problem) => ConfigProblem(key, problem, ConfigSourceName.Default).invalidNel
        }
      case cats.data.Validated.Invalid(problems) =>
        Async[F].pure(cats.data.Validated.Invalid(problems))
    }

  // -------------------------------------------------------------------------------------------
  // `kui.clusters.<n>.admin`: how KUI talks to one cluster's brokers
  // -------------------------------------------------------------------------------------------

  /** The five per-cluster admin knobs (CFGOP-002).
    *
    * The section is optional in full and every key inside it is optional on its own, so a cluster with
    * `admin: { parallelism: 8 }` keeps the defaults for the other four. Per-key defaults, not per-section:
    * configuring one knob must never silently reset another.
    *
    * Every value is bounded. An unbounded knob is a way for an operator to make KUI worse without being told
    * — a 200-topic request that a broker times out on, or 200 concurrent requests queued behind the admin
    * client's single network thread.
    */
  private def decodeAdminTuning[F[_]: Async](layers: Layers, index: Int): F[Problems[AdminTuning]] = {
    val prefix = s"$ClustersPrefix.$index.admin"
    val defaults = AdminTuning.default
    for {
      requestTimeout <- read[F, FiniteDuration](
        field(
          s"$prefix.requestTimeout",
          s"a duration between $MinAdminTimeout and $MaxRequestTimeout",
          readBoundedDuration(MinAdminTimeout, MaxRequestTimeout),
          defaults.requestTimeout
        ),
        layers
      )
      apiTimeout <- read[F, FiniteDuration](
        field(
          s"$prefix.apiTimeout",
          s"a duration between $MinAdminTimeout and $MaxApiTimeout",
          readBoundedDuration(MinAdminTimeout, MaxApiTimeout),
          defaults.apiTimeout
        ),
        layers
      )
      chunkSize <- read[F, Int](
        field(
          s"$prefix.chunkSize",
          s"a whole number between 1 and $MaxChunkSize",
          readBoundedInt(1, MaxChunkSize),
          defaults.topicChunkSize
        ),
        layers
      )
      groupChunkSize <- read[F, Int](
        field(
          s"$prefix.groupChunkSize",
          s"a whole number between 1 and $MaxChunkSize",
          readBoundedInt(1, MaxChunkSize),
          defaults.groupChunkSize
        ),
        layers
      )
      parallelism <- read[F, Int](
        field(
          s"$prefix.parallelism",
          s"a whole number between 1 and $MaxParallelism",
          readBoundedInt(1, MaxParallelism),
          defaults.parallelism
        ),
        layers
      )
    } yield (requestTimeout, apiTimeout, chunkSize, groupChunkSize, parallelism).tupled.andThen {
      (request, api, chunk, groupChunk, concurrency) =>
        checkApiTimeout(layers, prefix, request, api).map { _ =>
          AdminTuning(
            requestTimeout = request,
            apiTimeout = api,
            // One operator-facing key sets both, because "how many things go in one admin request" is one
            // question. Splitting it would be two knobs nobody could tell apart from the outside.
            topicChunkSize = chunk,
            partitionChunkSize = chunk,
            groupChunkSize = groupChunk,
            parallelism = concurrency,
            // Not operator-facing in M1: the snapshot cadence is ADR-016's, and a per-cluster override with
            // no screen to observe its effect would be a knob nobody could evaluate.
            metadataRefresh = defaults.metadataRefresh,
            capabilityRefresh = defaults.capabilityRefresh
          )
        }
    }
  }

  /** D-2: a whole-call budget shorter than one request's budget is refused, naming both keys.
    *
    * A client whose `apiTimeout` is 10s and whose `requestTimeout` is the default 30s gives up before its own
    * single request can finish, and that looks exactly like a broken cluster. The message names the other key
    * and its effective value — including when that value came from the default, which is the case the
    * operator cannot see for themselves.
    */
  private def checkApiTimeout(
      layers: Layers,
      prefix: String,
      requestTimeout: FiniteDuration,
      apiTimeout: FiniteDuration
  ): Problems[Unit] =
    if apiTimeout >= requestTimeout then ().validNel
    else {
      val source = layers.first(s"$prefix.apiTimeout").map(_._1).getOrElse(ConfigSourceName.Default)
      val origin = if layers.first(s"$prefix.requestTimeout").isDefined then "" else ", which is the default"
      ConfigProblem(
        s"$prefix.apiTimeout",
        s"expected a duration at least as long as $prefix.requestTimeout " +
          s"($requestTimeout$origin); got $apiTimeout",
        source
      ).invalidNel
    }

  private val MinAdminTimeout: FiniteDuration = 1.second
  private val MaxRequestTimeout: FiniteDuration = 5.minutes
  private val MaxApiTimeout: FiniteDuration = 15.minutes
  private val MaxChunkSize: Int = 1000
  private val MaxParallelism: Int = 32

  private def readBoundedInt(min: Int, max: Int)(raw: String): Either[String, Int] =
    raw.toIntOption.toRight(s"'$raw' is not a whole number").flatMap { value =>
      if value >= min && value <= max then Right(value) else Left(s"$value is outside $min..$max")
    }

  /** D-1: an explicit `id` wins, and the default is the slug of the name.
    *
    * ADR-031 derives the id from the name, and that is right as a default and wrong as an absolute: an
    * operator who fixes a typo in a display name would otherwise silently break every bookmark and every RBAC
    * entry that named the old slug.
    */
  private def clusterId(layers: Layers, prefix: String, name: String): Problems[ClusterId] =
    layers.first(s"$prefix.id") match {
      case Some((source, raw)) =>
        ClusterId.from(raw.trim) match {
          case Right(id) => id.validNel
          case Left(error) =>
            ConfigProblem(s"$prefix.id", s"expected ${error.message} (found '$raw')", source).invalidNel
        }
      case None =>
        ClusterConfig.slug(name) match {
          case Right(id) => id.validNel
          case Left(problem) => ConfigProblem(s"$prefix.id", problem, ConfigSourceName.Default).invalidNel
        }
    }

  private def readClusterName(raw: String): Either[String, String] = {
    val trimmed = raw.trim
    if trimmed.isEmpty then Left("must not be empty")
    else if trimmed.length > ClusterConfig.MaxNameLength then
      Left(s"is ${trimmed.length} characters long, and the limit is ${ClusterConfig.MaxNameLength}")
    else Right(trimmed)
  }

  // ---------------------------------------------------------------------------------------------
  // Secrets
  // ---------------------------------------------------------------------------------------------

  /** The configuration as it is after decoding but before `env:` and `file:` secret references have been
    * followed. Keeping the two phases apart is what lets the decode stay pure.
    */
  final private case class Draft(
      server: ServerConfig,
      gateway: GatewayDraft,
      telemetry: TelemetryConfig,
      store: StoreDraft,
      topics: TopicsConfig,
      consumers: ConsumersConfig,
      streaming: StreamingDraft,
      clusters: List[ClusterConfig],
      auth: AuthConfigSection.Draft,
      rbac: RbacPolicy,
      metrics: MetricsConfig,
      alerts: AlertsConfig
  )

  /** `kui.streaming` before its `env:` / `file:` reference has been followed. */
  final private case class StreamingDraft(cursorKey: Option[SecretRef])

  final private case class GatewayDraft(
      services: Map[ServiceId, UpstreamServiceConfig],
      readinessInterval: FiniteDuration,
      principalKeys: List[KeyDraft],
      cors: CorsConfig,
      devInsecureCookies: Boolean
  )

  final private case class KeyDraft(kid: String, key: SecretRef, notBefore: Instant)

  private def resolveSecrets[F[_]: Async](
      draft: Problems[Draft],
      env: Map[String, String]
  ): F[Problems[KuiConfig]] =
    draft match {
      case cats.data.Validated.Invalid(problems) => Async[F].pure(cats.data.Validated.Invalid(problems))
      case cats.data.Validated.Valid(value) =>
        for {
          keys <- value.gateway.principalKeys.zipWithIndex
            .traverse { case (key, index) => resolveKey[F](key, index, env) }
            .map(_.sequence)
          store <- resolveStore[F](value.store, env)
          streaming <- resolveStreaming[F](value.streaming, env)
          auth <- AuthConfigSection.resolve[F](value.auth, env)
        } yield (keys, store, streaming, auth).mapN(
          (principalKeys, storeConfig, streamingConfig, authConfig) =>
            KuiConfig(
              value.server,
              GatewayConfig(
                value.gateway.services,
                value.gateway.readinessInterval,
                principalKeys,
                value.gateway.cors,
                value.gateway.devInsecureCookies
              ),
              value.telemetry,
              storeConfig,
              value.topics,
              value.consumers,
              streamingConfig,
              value.clusters,
              authConfig,
              value.rbac,
              value.metrics,
              value.alerts
            )
        )
    }

  /** Follows `kui.streaming.cursorKey` and checks it is long enough to be an HMAC-SHA256 key.
    *
    * The length check is here rather than in the composition root because a key that is too short weakens
    * every cursor and every plan token without anything looking wrong, and the moment to say so is before the
    * process listens — the same rule, and the same 32 bytes, that `kui.gateway.principalKeys` applies.
    */
  private def resolveStreaming[F[_]: Async](
      draft: StreamingDraft,
      env: Map[String, String]
  ): F[Problems[StreamingConfig]] = {
    val key = "kui.streaming.cursorKey"

    draft.cursorKey match {
      case None => Async[F].pure(StreamingConfig(None).validNel)
      case Some(ref) =>
        SecretRef.resolve[F](ref, env).map {
          case Left(problem) => ConfigProblem(key, problem, ConfigSourceName.Default).invalidNel
          case Right(secret) =>
            val bytes = secret.value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
            if bytes >= StreamingConfig.MinCursorKeyBytes then StreamingConfig(Some(secret)).validNel
            else
              ConfigProblem(
                key,
                s"resolves to $bytes bytes, and HMAC-SHA256 needs at least " +
                  s"${StreamingConfig.MinCursorKeyBytes}; generate one with: openssl rand -base64 48",
                ConfigSourceName.Default
              ).invalidNel
        }
    }
  }

  private def resolveKey[F[_]: Async](
      draft: KeyDraft,
      index: Int,
      env: Map[String, String]
  ): F[Problems[PrincipalKeyConfig]] =
    SecretRef.resolve[F](draft.key, env).map {
      case Right(secret) => PrincipalKeyConfig(draft.kid, secret, draft.notBefore).validNel
      case Left(problem) =>
        ConfigProblem(
          s"kui.gateway.principalKeys.$index.key",
          problem,
          ConfigSourceName.Default
        ).invalidNel
    }

  // ---------------------------------------------------------------------------------------------
  // Unknown keys
  // ---------------------------------------------------------------------------------------------

  /** Refuses a YAML key that maps to nothing.
    *
    * Silently ignoring `kui.server.prot: 8080` is the failure mode this exists to prevent: the process
    * starts, listens on the default port, and the operator spends an evening wondering why their setting did
    * nothing. Suggesting a correction for the typo is deliberately out of scope; naming the key is enough to
    * find it.
    */
  private object UnknownKeys {

    /** Path patterns that a key may match. `*` matches any single segment. A pattern ending in `*` as its own
      * trailing segment additionally allows anything below it, which is how the not-yet-modelled sections are
      * tolerated without being read.
      */
    private val Known: List[List[String]] = List(
      List("kui", "server", "host"),
      List("kui", "server", "port"),
      List("kui", "server", "basePath"),
      List("kui", "server", "devInsecureCookies"),
      List("kui", "gateway", "services", "*", "url"),
      List("kui", "gateway", "services", "*", "timeout"),
      List("kui", "gateway", "services", "*", "maxConcurrent"),
      List("kui", "gateway", "readinessIntervalMs"),
      List("kui", "gateway", "principalKeys", "*", "kid"),
      List("kui", "gateway", "principalKeys", "*", "key"),
      List("kui", "gateway", "principalKeys", "*", "notBefore"),
      List("kui", "gateway", "cors", "enabled"),
      List("kui", "gateway", "cors", "origins"),
      List("kui", "gateway", "cors", "origins", "*"),
      List("kui", "telemetry", "otlpEndpoint"),
      List("kui", "telemetry", "prometheusPort"),
      List("kui", "telemetry", "logFormat"),
      List("kui", "telemetry", "hashUserIds"),
      // Declared so the shape exists and a file that already carries them still loads. Nothing is
      // read out of either: `kui.clusters[]` arrives with the cluster registry in M1 and
      // `kui.rbac` with the authorization model in M6.
      List("kui", "store", "topicPrefix"),
      List("kui", "store", "replicationFactor"),
      List("kui", "store", "minInSyncReplicas"),
      List("kui", "store", "maxFileBytes"),
      List("kui", "store", "replayTimeout"),
      List("kui", "store", "writeTimeout"),
      List("kui", "store", "dir"),
      List("kui", "store", "encryptionKey"),
      List("kui", "store", "encryptionKeys"),
      List("kui", "store", "encryptionKeyId"),
      List("kui", "store", "kafka", "bootstrapServers"),
      List("kui", "store", "kafka", "bootstrapServers", "*"),
      // `properties` is deliberately open: its whole purpose is to carry Kafka client properties KUI
      // does not model, so it cannot have a list of legal names.
      List("kui", "store", "kafka", "properties", "**"),
      List("kui", "topics", "refreshInterval"),
      List("kui", "topics", "scrapeTimeout"),
      List("kui", "topics", "internalPrefix"),
      List("kui", "topics", "defaultSearchMode"),
      List("kui", "topics", "defaultPageSize"),
      List("kui", "topics", "maxPageSize"),
      List("kui", "consumers", "refreshInterval"),
      List("kui", "streaming", "cursorKey"),
      // `kui.metrics` and `kui.alerts` are read in full even though the services that consume them are not
      // written yet. A section that parses but is not registered here would refuse a perfectly valid file,
      // which is the failure this whole list exists to prevent.
      List("kui", "metrics", "scrapeInterval"),
      List("kui", "metrics", "retention"),
      List("kui", "metrics", "maxSamplesPerSeries"),
      List("kui", "metrics", "sources", "*", "url"),
      List("kui", "metrics", "sources", "*", "kind"),
      List("kui", "metrics", "sources", "*", "callTimeout"),
      List("kui", "metrics", "sources", "*", "queryTimeout"),
      List("kui", "metrics", "sources", "*", "maxConcurrentQueries"),
      List("kui", "metrics", "sources", "*", "maxSeriesPerQuery"),
      List("kui", "metrics", "sources", "*", "maxPointsPerSeries"),
      List("kui", "metrics", "sources", "*", "maxResponseBytes"),
      List("kui", "metrics", "sources", "*", "maxCacheBytes"),
      List("kui", "metrics", "sources", "*", "cacheTtl"),
      List("kui", "metrics", "sources", "*", "staleTtl"),
      List("kui", "alerts", "retention"),
      List("kui", "alerts", "evaluationInterval"),
      List("kui", "alerts", "thresholds", "offlinePartitions"),
      List("kui", "alerts", "thresholds", "underReplicatedPartitions"),
      List("kui", "alerts", "thresholds", "rebalanceDuration"),
      List("kui", "alerts", "thresholds", "diskUsedWarningPercent"),
      List("kui", "alerts", "thresholds", "diskUsedCriticalPercent"),
      List("kui", "clusterProfiles", "url"),
      List("kui", "clusterProfiles", "pollInterval"),
      List("kui", "clusterProfiles", "requestTimeout"),
      List("kui", "clusterProfiles", "reconnectBackoff"),
      List("kui", "clusterProfiles", "maxReconnectBackoff"),
      List("kui", "clusterProfiles", "startupTimeout"),
      List("kui", "clusters", "*", "name"),
      List("kui", "clusters", "*", "id"),
      List("kui", "clusters", "*", "bootstrapServers"),
      List("kui", "clusters", "*", "bootstrapServers", "*"),
      List("kui", "clusters", "*", "readOnly"),
      List("kui", "clusters", "*", "admin", "requestTimeout"),
      List("kui", "clusters", "*", "admin", "apiTimeout"),
      List("kui", "clusters", "*", "admin", "chunkSize"),
      List("kui", "clusters", "*", "admin", "groupChunkSize"),
      List("kui", "clusters", "*", "admin", "parallelism"),
      // Open for the same reason as the store's: this map's whole purpose is to carry Kafka client
      // properties KUI does not model, so it cannot have a list of legal names.
      List("kui", "clusters", "*", "properties", "**"),
      List("kui", "clusters", "*", "schemaRegistry", "url"),
      List("kui", "clusters", "*", "schemaRegistry", "url", "*"),
      List("kui", "clusters", "*", "schemaRegistry", "callTimeout"),
      List("kui", "clusters", "*", "schemaRegistry", "auth", "type"),
      List("kui", "clusters", "*", "schemaRegistry", "auth", "username"),
      List("kui", "clusters", "*", "schemaRegistry", "auth", "password"),
      List("kui", "clusters", "*", "schemaRegistry", "auth", "tokenEndpoint"),
      List("kui", "clusters", "*", "schemaRegistry", "auth", "clientId"),
      List("kui", "clusters", "*", "schemaRegistry", "auth", "clientSecret"),
      List("kui", "clusters", "*", "schemaRegistry", "auth", "scope"),
      List("kui", "clusters", "*", "connect", "*", "name"),
      List("kui", "clusters", "*", "connect", "*", "url"),
      List("kui", "clusters", "*", "connect", "*", "url", "*"),
      List("kui", "clusters", "*", "connect", "*", "callTimeout"),
      List("kui", "clusters", "*", "ksql", "url"),
      List("kui", "clusters", "*", "ksql", "url", "*"),
      List("kui", "clusters", "*", "ksql", "callTimeout"),
      List("kui", "clusters", "*", "ksql", "streamTimeout"),
      List("kui", "clusters", "*", "serde", "defaultKey"),
      List("kui", "clusters", "*", "serde", "defaultValue"),
      List("kui", "clusters", "*", "serde", "schemaCacheSize"),
      List("kui", "clusters", "*", "serde", "subjectCacheTtl"),
      List("kui", "clusters", "*", "serde", "patterns", "*", "serde"),
      List("kui", "clusters", "*", "serde", "patterns", "*", "topicKeysPattern"),
      List("kui", "clusters", "*", "serde", "patterns", "*", "topicValuesPattern"),
      // `kui.clusters.<n>.masking[]` (DM-001, ADR-023). `fields` is known both as a scalar and as a
      // sequence for the same reason `bootstrapServers` is: a YAML list of scalars is indexed, and the
      // loader reads the whole list as one comma-separated value.
      List("kui", "clusters", "*", "masking", "*", "kind"),
      List("kui", "clusters", "*", "masking", "*", "fields"),
      List("kui", "clusters", "*", "masking", "*", "fields", "*"),
      List("kui", "clusters", "*", "masking", "*", "fieldsNamePattern"),
      List("kui", "clusters", "*", "masking", "*", "topicKeysPattern"),
      List("kui", "clusters", "*", "masking", "*", "topicValuesPattern"),
      List("kui", "clusters", "*", "masking", "*", "replacement"),
      List("kui", "clusters", "*", "masking", "*", "maskingCharsReplacement"),
      List("kui", "clusters", "*", "masking", "*", "keep", "prefix"),
      List("kui", "clusters", "*", "masking", "*", "keep", "suffix"),
      List("kui", "clusters", "*", "properties", "**")
    ) ++ HttpTlsConfig
      .keysUnder("kui.metrics.sources.*.tls")
      .map(_.split('.').toList)
      ++ UpstreamAuthConfig
        .keysUnder("kui.metrics.sources.*.auth")
        .map(_.split('.').toList)
      ++ UpstreamAuthConfig
        .keysUnder("kui.clusters.*.connect.*.auth")
        .map(_.split('.').toList)
      ++ UpstreamAuthConfig.keysUnder("kui.clusters.*.ksql.auth").map(_.split('.').toList)
      ++ AuthConfigSection.keys ++ RbacConfigSection.keys ++ ClusterSecurityConfig
        .keysUnder("kui.store.kafka.security")
        .map(_.split('.').toList)
      ++ ClusterSecurityConfig
        .keysUnder("kui.clusters.*.security")
        .flatMap { key =>
          val path = key.split('.').toList
          // The list-valued security keys (`enabledProtocols`, `cipherSuites`) are legal as a YAML list,
          // whose leaves are indexed, so each key is known both as a scalar and as a sequence.
          List(path, path :+ "*")
        }

    def check(documents: List[Document]): ValidatedNel[ConfigProblem, Unit] =
      documents.flatMap(document => unknownIn(document)) match {
        case Nil => ().validNel
        case first :: rest => cats.data.Validated.Invalid(NonEmptyList(first, rest))
      }

    private def unknownIn(document: Document): List[ConfigProblem] =
      leaves(document.json, Nil)
        .filterNot(leaf => isKnown(leaf))
        .map(leaf =>
          ConfigProblem(
            leaf.path.mkString("."),
            "is not a KUI configuration key",
            ConfigSourceName.File(document.path)
          )
        )

    /** A leaf of the document: the path it sits at, and whether what sits there is an empty container.
      *
      * The distinction is the whole point. A scalar has to name a key that exists. An empty container
      * (`services: {}`, `principalKeys: []`, `telemetry:` with everything under it commented out) names no
      * key at all — it supplies nothing — so it is legal wherever a known key could appear below it, and
      * refusing it would refuse a configuration that says exactly what it means.
      */
    final private case class Leaf(path: List[String], isEmptyContainer: Boolean)

    private def isKnown(leaf: Leaf): Boolean =
      if leaf.isEmptyContainer then
        Known.exists(pattern => matches(pattern, leaf.path) || isPrefixOf(pattern, leaf.path))
      else Known.exists(matches(_, leaf.path))

    /** Every path from the document root down to a scalar or to an empty container. */
    private def leaves(json: Json, path: List[String]): List[Leaf] =
      json.asObject match {
        case Some(obj) if obj.nonEmpty =>
          obj.toList.flatMap { case (name, child) => leaves(child, path :+ name) }
        case _ =>
          json.asArray match {
            case Some(items) if items.nonEmpty =>
              items.toList.zipWithIndex.flatMap { case (child, index) =>
                leaves(child, path :+ index.toString)
              }
            case _ =>
              val isContainer = json.isObject || json.isArray
              List(Leaf(path, isContainer))
          }
      }

    /** Whether `path` names a section that `pattern` has keys underneath, e.g. `kui.gateway.services` against
      * `kui.gateway.services.*.url`.
      */
    private def isPrefixOf(pattern: List[String], path: List[String]): Boolean =
      (pattern, path) match {
        case ("**" :: _, _) => true
        case (Nil, _) => false
        case (_, Nil) => true
        case (patternHead :: patternTail, pathHead :: pathTail) =>
          (patternHead == "*" || patternHead == pathHead) && isPrefixOf(patternTail, pathTail)
      }

    private def matches(pattern: List[String], path: List[String]): Boolean =
      (pattern, path) match {
        case ("**" :: _, _) => true
        case (Nil, Nil) => true
        case (Nil, _) | (_, Nil) => false
        case (patternHead :: patternTail, pathHead :: pathTail) =>
          val segmentMatches = patternHead == "*" || patternHead == pathHead
          segmentMatches && matches(patternTail, pathTail)
      }
  }
}
