package kui.config

import java.util.Locale

import kui.kernel.cluster.{
  AdminTuning,
  BootstrapServers,
  ClientProperties,
  ClusterConnection,
  ClusterSecurity
}
import kui.kernel.{ClusterId, ValidationError}

/** One statically configured cluster: everything KUI needs to open a client, and nothing about what it found
  * when it did.
  *
  * `properties` is the escape hatch of ADR-022: raw Kafka client properties, applied after everything the
  * typed security model rendered, so a mechanism KUI has not modelled yet is still usable without waiting for
  * a release.
  *
  * Nothing here has been checked against a broker. A cluster that is spelled correctly and unreachable is a
  * perfectly valid configuration — it becomes a `Section.Unavailable(reason)` on the dashboard at runtime (M1
  * DEVPLAN §10, D4) — because otherwise one dead broker would stop KUI from starting at all.
  */
final case class ClusterConfig(
    id: ClusterId,
    name: String,
    bootstrapServers: BootstrapServers,
    security: ClusterSecurity,
    properties: ClientProperties,
    readOnly: Boolean,
    admin: AdminTuning,
    serde: ClusterSerdeConfig = ClusterSerdeConfig.empty,
    schemaRegistry: Option[SchemaRegistrySettings] = None,
    connect: List[ConnectClusterSettings] = Nil,
    ksql: Option[KsqlSettings] = None,
    masking: MaskingConfig = MaskingConfig.empty
) {

  /** The four things a client needs, as the one value every port takes.
    *
    * Assembling it here rather than at each call site is what keeps `ClusterConnection` and this type from
    * drifting: a field added to the connection has exactly one place that fails to compile.
    */
  def connection: ClusterConnection =
    ClusterConnection(id, bootstrapServers, security, properties, admin)

  /** Identity and shape, never credentials.
    *
    * The generated `toString` of a case class prints every field, and `security` reaches transitively into a
    * SASL password and a keystore's bytes. Those are all `Secret` and would redact themselves, but a future
    * field that is not would leak silently, and the wall of text nobody reads is not worth the risk.
    */
  override def toString: String = {
    val mechanism = security.saslMechanism.fold("")(m => s", mechanism=${m.wireName}")
    val extra = if properties.isEmpty then "" else s", properties=[${properties.render}]"
    val registry = schemaRegistry.fold("")(settings => s", schemaRegistry=$settings")
    val connectClusters = if connect.isEmpty then "" else s", connect=[${connect.mkString(", ")}]"
    val ksqldb = ksql.fold("")(settings => s", ksql=$settings")
    // The count and never the rules. `MaskingConfig.toString` says why: the roster of masked fields is a
    // map of where this cluster's secrets are, and a startup diagnostic is not the place for one.
    val masked = if masking.isEmpty then "" else s", masking=$masking"
    s"ClusterConfig(${id.value}, '$name', ${bootstrapServers.value}, " +
      s"${security.securityProtocol}$mechanism, readOnly=$readOnly$extra$registry$connectClusters" +
      s"$ksqldb$masked)"
  }
}

object ClusterConfig {

  /** How long a display name may be. Long enough for "Production — EU West (regulated)", short enough that a
    * cluster switcher does not have to truncate every entry.
    */
  val MaxNameLength: Int = 64

  /** ADR-031's derivation, as a total function with a named failure.
    *
    * Lower-cases the name, replaces every run of characters outside `[a-z0-9]` with a single dash, and trims
    * leading and trailing dashes. `"Production EU"` becomes `production-eu`; `"prod / eu"` becomes `prod-eu`.
    *
    * A name that leaves nothing behind — `"***"`, or a name written entirely in a non-Latin script — is a
    * `Left` telling the operator to set `id` explicitly. Inventing `cluster-1` for them would put an
    * identifier they never chose into every URL and every RBAC rule.
    */
  def slug(name: String): Either[String, ClusterId] = {
    val lowered = name.toLowerCase(Locale.ROOT)
    val dashed = lowered.map(character => if isSlugCharacter(character) then character else '-')
    val collapsed = dashed.split('-').filter(_.nonEmpty).mkString("-")

    // A name longer than the id's own limit is truncated rather than refused, and the trailing dash a
    // truncation can leave behind is trimmed, because `production-eu-` is not a legal id.
    val bounded = collapsed.take(MaxIdLength).reverse.dropWhile(_ == '-').reverse

    if bounded.isEmpty then
      Left(
        s"'$name' contains no letters or digits that can be made into a URL slug; " +
          "set kui.clusters.<n>.id explicitly"
      )
    else
      ClusterId.from(bounded) match {
        case Right(id) => Right(id)
        case Left(error: ValidationError) =>
          Left(s"'$name' does not produce a usable id ('$bounded': ${error.message}); set it explicitly")
      }
  }

  /** `ClusterId`'s own upper bound, repeated here because the opaque type does not publish it. */
  private val MaxIdLength: Int = 64

  /** Which raw property keys hold a secret.
    *
    * Delegates to [[ClientProperties.isSensitiveKey]], the classifier `ClientProperties.fromRaw` actually
    * uses, so that `kui.clusters.<n>.properties` and `kui.store.kafka.properties` cannot drift. A key wrongly
    * treated as secret prints `***` in a diagnostic, and a key wrongly treated as public prints a password
    * into `docker logs` and stores it in the clear.
    */
  def isSecretProperty(key: String): Boolean = ClientProperties.isSensitiveKey(key)

  private def isSlugCharacter(character: Char): Boolean =
    (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9')

  given CanEqual[ClusterConfig, ClusterConfig] = CanEqual.derived
}
