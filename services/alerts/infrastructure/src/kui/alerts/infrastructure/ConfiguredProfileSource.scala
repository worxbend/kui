package kui.alerts.infrastructure

import cats.syntax.all.*
import cats.{Applicative, Monad}

import kui.alerts.application.{ClusterProfileSource, ClusterProfileView}
import kui.config.ClusterConfig
import kui.kernel.ClusterId
import kui.kernel.cluster.ClusterConnection
import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}

/** The alerts service's `ClusterProfileSource`, answered from this process's own configuration.
  *
  * The consumer and topic services both hold one of these and the argument is theirs: the all-in-one
  * deployment has already loaded `kui.clusters[]` in order to wire the cluster service, and making it call
  * itself over a socket to read a list it is holding in memory would add a listener, a timeout and a failure
  * mode to a lookup that cannot fail. The distributed shape uses ADR-046's HTTP client instead; both satisfy
  * the same port, which is the point of the port.
  *
  * ==What it carries that the application layer cannot see==
  *
  * A `ClusterProfileView` has the id, the display name and `readOnly`, and no connection material. The
  * adapter that opens admin clients does need it, so this class holds both: the views it hands the
  * application layer, and the [[connectionFor]] lookup that only `infrastructure` can see. That is what keeps
  * a password out of a use case, an audit record and a log line by construction rather than by review.
  */
final class ConfiguredProfileSource[F[_]: Applicative](clusters: List[ClusterConfig])
    extends ClusterProfileSource[F] {

  private val views: List[ClusterProfileView] =
    clusters
      .map(cluster => ClusterProfileView(cluster.id, cluster.name, cluster.readOnly))
      .sortBy(_.cluster.value)

  private val connections: Map[ClusterId, ClusterConnection] =
    clusters.map(cluster => cluster.id -> cluster.connection).toMap

  def profileOf(cluster: ClusterId): F[Either[KuiError, ClusterProfileView]] =
    views
      .find(_.cluster == cluster)
      .toRight(ApplicationError.NotFound("cluster", cluster.value, ErrorCode.ClusterNotFound): KuiError)
      .pure[F]

  def all: F[List[ClusterProfileView]] = views.pure[F]

  /** The connection material for one cluster, for the module that builds admin clients.
    *
    * `None` means the cluster is not configured, which every caller must turn into `KUI-CLUSTER-NOT-FOUND`
    * rather than into an empty feed: "KUI has never heard of this cluster" and "this cluster has no open
    * alerts" are different screens, and only one of them is good news.
    */
  def connectionFor(cluster: ClusterId): Option[ClusterConnection] = connections.get(cluster)
}

object ConfiguredProfileSource {

  /** Resolve every lookup from the same owned profile snapshot as the evaluator resources. */
  def live[F[_]: Monad](current: F[List[ClusterConfig]]): ClusterProfileSource[F] =
    new ClusterProfileSource[F] {
      def all: F[List[ClusterProfileView]] =
        current.flatMap(clusters => new ConfiguredProfileSource[F](clusters).all)

      def profileOf(cluster: ClusterId): F[Either[KuiError, ClusterProfileView]] =
        current.flatMap(clusters => new ConfiguredProfileSource[F](clusters).profileOf(cluster))
    }
}
