package kui.metrics.app

import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.Semaphore
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.Stream
import org.typelevel.log4cats.StructuredLogger

import kui.config.ClusterConfig
import kui.kernel.ClusterId

/** Owns one resource per resolved profile. Replacement closes the old collector before opening the new one;
  * unchanged profiles retain their buffers. Allocation-to-registration is masked, but acquisition itself
  * remains cancellable. The watcher must be released before this owner's drain.
  */
final private[app] class ClusterCollectors[F[_]: Async, A] private (
    entries: Ref[F, Map[ClusterId, (ClusterConfig, A, F[Unit])]],
    gate: Semaphore[F],
    build: ClusterConfig => Resource[F, A]
) {
  def current: F[Map[ClusterId, (ClusterConfig, A)]] =
    entries.get.map(_.view.mapValues { case (profile, value, _) => (profile, value) }.toMap)

  def reconcile(profiles: List[ClusterConfig]): F[Unit] = gate.permit.use { _ =>
    val wanted = profiles.map(profile => profile.id -> profile).toMap
    for {
      held <- entries.get
      _ <- held.toList.traverse_ { (id, entry) =>
        if wanted.get(id).contains(entry._1) then Async[F].unit
        else Async[F].uncancelable(_ => entries.update(_ - id) *> entry._3)
      }
      remaining <- entries.get
      _ <- profiles.filterNot(profile => remaining.contains(profile.id)).traverse_ { profile =>
        Async[F].uncancelable { poll =>
          poll(build(profile).allocated).flatMap { (value, release) =>
            entries.update(_.updated(profile.id, (profile, value, release)))
          }
        }
      }
    } yield ()
  }

  def watch(changes: Stream[F, List[ClusterConfig]], logger: StructuredLogger[F]): Resource[F, Unit] =
    changes
      .evalMap(profiles =>
        reconcile(profiles).handleErrorWith(error =>
          logger.error(error)("could not reconcile cluster collectors")
        )
      )
      .compile
      .drain
      .background
      .void

  private def close: F[Unit] = gate.permit.use { _ =>
    entries
      .getAndSet(Map.empty)
      .flatMap(_.values.toList.foldLeft(Async[F].unit) { (release, entry) =>
        release.guarantee(entry._3)
      })
  }
}

private[app] object ClusterCollectors {
  def resource[F[_]: Async, A](initial: List[ClusterConfig])(
      build: ClusterConfig => Resource[F, A]
  ): Resource[F, ClusterCollectors[F, A]] =
    for {
      entries <- Resource.eval(Ref.of[F, Map[ClusterId, (ClusterConfig, A, F[Unit])]](Map.empty))
      gate <- Resource.eval(Semaphore[F](1))
      owner <- Resource.make(Async[F].pure(new ClusterCollectors(entries, gate, build)))(_.close)
      _ <- Resource.eval(owner.reconcile(initial))
    } yield owner
}
