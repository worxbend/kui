package kui.schema.application

import cats.Monad
import cats.effect.kernel.{Async, Sync}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger

import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.kernel.{ClusterId, Page, Subject}
import kui.schema.domain.*

/** The four things a screen reads out of a registry, each as its own use case.
  *
  * Every one of them goes through [[RegistryQuery.on]], which is where "unknown cluster", "cluster with no
  * registry" and "registry that did not answer" are turned into their three different answers. No use case
  * here contains an `if` about any of that, which is the point: the decision is made once and cannot drift
  * between the subject list and the schema panel.
  *
  * ==None of these caches anything==
  *
  * A registry's answers change when somebody registers a schema, and the person most likely to be looking at
  * this screen is the person who just did. A stale subject list would show them their own change missing,
  * which reads as a failure of the registration rather than of the cache. The read path is therefore a
  * request per call, bounded by the upstream's own timeout and circuit breaker — the registry is protected
  * from KUI by the bulkhead, not by hiding its state behind a snapshot.
  */
object RegistryQuery {

  /** Runs `use` against a cluster's registry, or explains why there is none to run it against.
    *
    * The three outcomes, in the order they are decided:
    *
    *   1. no such cluster — `404 KUI-CLUSTER-NOT-FOUND`, because the caller followed a link to something that
    *      does not exist;
    *   1. the cluster exists and has no registry — `KUI-UNSUPPORTED` naming the configuration key, which the
    *      capability report has already told the browser about so the screen should not have been reachable;
    *   1. otherwise the port, whose own failures travel back as they are.
    */
  def on[F[_]: Monad, A](registries: ClusterRegistries[F], cluster: ClusterId)(
      use: SchemaRegistryPort[F] => F[Either[KuiError, A]]
  ): F[Either[KuiError, A]] =
    registries.profile(cluster).flatMap {
      case None => RegistryAccess.unknownCluster(cluster).asLeft[A].pure[F]
      case Some(_) =>
        registries.registry(cluster).flatMap {
          case None => RegistryAccess.notConfigured(cluster).asLeft[A].pure[F]
          case Some(port) => use(port)
        }
    }

  /** A registry answer of "there is no such thing" as the 404 a route needs.
    *
    * `KUI-SCHEMA-NOT-FOUND` for both a missing subject and a missing version, because from the caller's side
    * they are one mistake — a link to a schema that is not there — and inventing a second code for the
    * narrower case would make a client handle two errors that need the same screen.
    */
  def orNotFound[A](what: String, id: String)(result: Option[A]): Either[KuiError, A] =
    result.toRight(ApplicationError.NotFound(what, id, ErrorCode.SchemaNotFound))
}

/** One page of a cluster's subjects, searched, sorted, and enriched with the facts its rows show.
  *
  * The registry hands back every subject name in a single response and offers no search, sort or paging of
  * its own, so the whole list crosses the wire on every request and [[SubjectCatalog]] cuts the page. That is
  * fine at the sizes registries actually reach — a few thousand names is a few hundred kilobytes — and it is
  * the only option the API offers, but it is why this call has the shortest timeout budget of the four and
  * why this endpoint's page size is bounded below the kernel's: `SchemaEndpoints.MaxPageSize` is a hundred
  * and `PageSize.Max` is five hundred, because a row here is three registry requests rather than a slice of a
  * list KUI already holds.
  *
  * ==The page is cut before anything is enriched==
  *
  * A subject's format, version count and compatibility level cost a call each, and the page is what bounds
  * how many are made: twenty-five rows on screen is twenty-five [[SchemaRegistryPort.summary]] calls, no
  * matter whether the registry holds six subjects or six thousand. Enriching the whole list and then cutting
  * a page out of it would turn opening a list into two thousand calls to a component that is a single-writer
  * JVM, which is the failure this ordering exists to prevent — and it is why the fold happens here rather
  * than in [[SubjectCatalog]], which is pure and has no calls to make.
  *
  * ==Enrichment never fails the page==
  *
  * The subject list is one call, and it either arrived or it did not; that failure travels, because an empty
  * subject list is a claim the registry holds nothing. Everything after it is a secondary call about one row,
  * and a row that loses its facts keeps its name. A subject that disappeared from a list because the call
  * that decorated it did not answer would send an operator looking for a schema nobody deleted.
  */
trait SubjectListUseCase[F[_]] {

  /** `visible` runs before paging and before enrichment, so a hidden subject is neither counted nor fetched.
    */
  def list(
      cluster: ClusterId,
      query: SubjectQuery,
      visible: Subject => Boolean = _ => true
  ): F[Either[KuiError, Page[SubjectSummary]]]
}

object SubjectListUseCase {

  /** How many of a page's rows are enriched at once.
    *
    * Half of `SchemaWiring.MaxConcurrentPerRegistry`, and deliberately not equal to it: the bulkhead is the
    * limit for *everything* KUI sends one registry, and a single list page that filled it would queue every
    * other screen's request behind itself. Eight is enough that a page of twenty-five costs four rounds of
    * latency instead of twenty-five, and few enough that a list is never the reason another panel waits.
    */
  val MaxConcurrentRows: Int = 8

  def make[F[_]: Async](
      registries: ClusterRegistries[F],
      logger: StructuredLogger[F]
  ): SubjectListUseCase[F] =
    new SubjectListUseCase[F] {

      def list(
          cluster: ClusterId,
          query: SubjectQuery,
          visible: Subject => Boolean
      ): F[Either[KuiError, Page[SubjectSummary]]] =
        RegistryQuery.on(registries, cluster) { port =>
          port.subjects.flatMap {
            case Left(error) => error.asLeft[Page[SubjectSummary]].pure[F]
            case Right(names) =>
              enrich(port, cluster, SubjectCatalog.page(names.filter(visible), query))
                .map(_.asRight[KuiError])
          }
        }

      /** The page's rows, filled in. The pagination metadata is carried across untouched: it was computed
        * over the filtered list and nothing here adds or removes a row.
        *
        * The empty-page branch is the whole of this service's cheap path, and it is load bearing twice over.
        * A search that matched nothing must not still pay for the registry-wide compatibility call, and
        * `pageSize=0` — the drawer badge asking for the total and no rows — arrives here as an empty page for
        * exactly that reason. Both are one request to the registry and no more; `SubjectSummarySuite` counts
        * the calls, the global one included, because the two branches produce identical rows and only the
        * call count tells them apart.
        */
      private def enrich(
          port: SchemaRegistryPort[F],
          cluster: ClusterId,
          page: Page[Subject]
      ): F[Page[SubjectSummary]] =
        if page.isEmpty then page.map(SubjectSummary.bare).pure[F]
        else
          for {
            global <- globalLevel(port, cluster)
            rows <- page.items.parTraverseN(MaxConcurrentRows)(row(port, cluster, global, _))
          } yield page.copy(items = rows)

      /** The registry-wide level, once for the page, or `None` and a line in the log.
        *
        * One call for the whole page rather than one per row, because it is the same answer for every row. A
        * failure here costs the inheriting rows their level and costs the page nothing else.
        */
      private def globalLevel(
          port: SchemaRegistryPort[F],
          cluster: ClusterId
      ): F[Option[CompatibilityLevel]] =
        port.globalCompatibility.flatMap {
          case Right(level) => level.some.pure[F]
          case Left(error) =>
            logger
              .warn(Map("cluster.id" -> cluster.value))(
                "the registry-wide compatibility level could not be read, so the subject rows that " +
                  s"inherit it show none: ${error.message}"
              )
              .as(none[CompatibilityLevel])
        }

      /** One row. The reason a row is bare is logged and never invented into a field on the wire: "we could
        * not read this" and "this subject has no format" are different facts, and a client that was handed a
        * reason string would print it as though the registry had said it.
        */
      private def row(
          port: SchemaRegistryPort[F],
          cluster: ClusterId,
          global: Option[CompatibilityLevel],
          subject: Subject
      ): F[SubjectSummary] =
        port.summary(subject).flatMap {
          case Right(Some(summary)) => summary.inheriting(global).pure[F]
          // The subject was listed and is gone. It keeps its row for this page — the list said it was
          // there — and gains no inherited level, because a subject that no longer exists inherits nothing.
          case Right(None) => SubjectSummary.bare(subject).pure[F]
          case Left(error) =>
            logger
              .warn(Map("cluster.id" -> cluster.value, "subject" -> subject.value))(
                "the subject's list facts could not be read, so the row carries the name alone: " +
                  error.message
              )
              .as(SubjectSummary.bare(subject))
        }
    }
}

/** Every version number of one subject, ascending.
  *
  * Numbers and not schemas. A subject with two hundred versions holds two hundred schema documents, and a
  * version list that fetched them all would move megabytes so a screen could draw a dropdown. The schema
  * behind a version is one more request, made when somebody picks one.
  */
trait SubjectVersionsUseCase[F[_]] {
  def versions(cluster: ClusterId, subject: Subject): F[Either[KuiError, List[SchemaVersion]]]
}

object SubjectVersionsUseCase {

  def make[F[_]: Sync](registries: ClusterRegistries[F]): SubjectVersionsUseCase[F] =
    new SubjectVersionsUseCase[F] {
      def versions(cluster: ClusterId, subject: Subject): F[Either[KuiError, List[SchemaVersion]]] =
        RegistryQuery.on(registries, cluster)(
          _.versions(subject).map(_.flatMap(RegistryQuery.orNotFound("subject", subject.value)))
        )
    }
}

/** The schema behind one version of one subject. */
trait SchemaVersionUseCase[F[_]] {

  def schema(
      cluster: ClusterId,
      subject: Subject,
      version: VersionSelector
  ): F[Either[KuiError, RegisteredSchema]]
}

object SchemaVersionUseCase {

  def make[F[_]: Sync](registries: ClusterRegistries[F]): SchemaVersionUseCase[F] =
    new SchemaVersionUseCase[F] {

      def schema(
          cluster: ClusterId,
          subject: Subject,
          version: VersionSelector
      ): F[Either[KuiError, RegisteredSchema]] =
        RegistryQuery.on(registries, cluster)(
          _.schema(subject, version)
            .map(_.flatMap(RegistryQuery.orNotFound("schema", s"${subject.value}/${version.path}")))
        )
    }
}

/** The compatibility level in force, globally or for one subject.
  *
  * The subject answer says whether the level is the subject's own or the global one it is falling back to,
  * because those are different states that look identical if flattened — and because an operator who
  * "confirms" an inherited level in a form has just written an override that will stop following the global
  * setting they thought they were reading.
  */
trait CompatibilityReadUseCase[F[_]] {

  def global(cluster: ClusterId): F[Either[KuiError, CompatibilityLevel]]

  def forSubject(cluster: ClusterId, subject: Subject): F[Either[KuiError, SubjectCompatibility]]
}

object CompatibilityReadUseCase {

  def make[F[_]: Sync](
      registries: ClusterRegistries[F],
      logger: StructuredLogger[F]
  ): CompatibilityReadUseCase[F] =
    new CompatibilityReadUseCase[F] {

      def global(cluster: ClusterId): F[Either[KuiError, CompatibilityLevel]] =
        RegistryQuery.on(registries, cluster)(_.globalCompatibility)

      /** Two calls, and the second one only when it is needed.
        *
        * The subject's own level is asked for first. When the registry says it has none, the global level is
        * fetched and reported as inherited — which is one extra round trip on the common path and is worth
        * it, because the alternative is a screen that cannot tell an operator which setting is actually
        * governing their subject.
        */
      def forSubject(cluster: ClusterId, subject: Subject): F[Either[KuiError, SubjectCompatibility]] =
        RegistryQuery.on(registries, cluster) { port =>
          port.subjectCompatibility(subject).flatMap {
            case Left(error) => error.asLeft[SubjectCompatibility].pure[F]
            case Right(Some(level)) => SubjectCompatibility.own(level).asRight[KuiError].pure[F]
            case Right(None) =>
              port.globalCompatibility.flatMap {
                case Right(level) => SubjectCompatibility.inherited(level).asRight[KuiError].pure[F]
                // The subject has no level of its own and the global level could not be read. Reporting
                // the registry's default as if it had been observed would be a guess presented as a fact
                // on a screen an operator uses to decide whether a breaking change is allowed, so the
                // failure travels instead.
                case Left(error) =>
                  logger
                    .warn(
                      Map("cluster.id" -> cluster.value, "subject" -> subject.value)
                    )(
                      "the subject inherits its compatibility level and the global level could not be read: " +
                        error.message
                    )
                    .as(error.asLeft[SubjectCompatibility])
              }
          }
        }
    }
}
