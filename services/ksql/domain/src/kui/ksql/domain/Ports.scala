package kui.ksql.domain

import kui.kernel.error.KuiError

/** One ksqlDB cluster, as this service needs to speak to it for anything that finishes.
  *
  * The port is per *ksqlDB cluster*, of which a Kafka cluster has at most one — `KsqlSettings` is singular
  * where `ConnectClusterSettings` is a list, because a second ksqlDB cluster over the same brokers shares the
  * same command topic and is the same logical service. So there is no aggregation layer above this one and no
  * per-worker section on the wire; what varies is whether the one server answered.
  *
  * ==The push query is not here==
  *
  * A push query never finishes, so its port is `kui.ksql.application.KsqlQueryStream` and is declared a layer
  * up. That is not a taste: an unbounded answer is an `fs2.Stream`, rule A1 keeps fs2 out of a domain module,
  * and `services/message` splits its browse the same way for the same reason. What is here is everything
  * whose answer is a value.
  *
  * ==Nothing here throws==
  *
  * Every method answers a `KuiError` on the left. A server that is down, slow, starting up or rejecting KUI's
  * credentials arrives as a value, because the caller has to keep the rest of the product working while this
  * one upstream is broken.
  */
trait KsqlServerPort[F[_]] {

  /** Every stream, table, running query and topic the server can name.
    *
    * One call, because §3.16's pane is one pane and `ConnectEndpoints`' argument applies unchanged: several
    * requests for one screen is how several panels come to disagree about how many objects there are.
    */
  def objects: F[Either[KuiError, KsqlObjects]]

  /** The stream or table a `DROP … DELETE TOPIC` names, without the display cap on [[objects]].
    *
    * [[KsqlObjects.MaxObjects]] keeps the pane readable. A deletion that looked the target up in that capped
    * list could never plan an object past it, and refreshing would not help. `None` means there is no single
    * object of this kind and name. Query and topic are not deletion targets and answer `None`.
    */
  def destructiveTarget(name: String, kind: KsqlObjectKind): F[Either[KuiError, Option[KsqlObject]]]

  /** Run one statement that finishes, and answer what it produced.
    *
    * The statement has already been classified — that is what having a [[KsqlStatement]] rather than a
    * `String` means — so an adapter is not the place a push query is noticed. A push query reaching this
    * method is a defect in the caller, and the adapter says so rather than opening an unbounded response and
    * buffering it.
    */
  def execute(statement: KsqlStatement): F[Either[KuiError, StatementOutcome]]
}
