package kui.cluster.client

import java.time.Instant

import kui.kernel.ClusterId
import kui.kernel.cluster.ClusterConnection
import kui.kernel.error.KuiError

/** One cluster's connection settings, plus the identity a screen shows.
  *
  * @param version
  *   the metadata-store version this profile was resolved at. A consumer rebuilds its Kafka clients when this
  *   moves or revalidation finds different effective settings after recreation. A version can reset on
  *   recreation; a re-serialised wire document alone is not a settings change.
  */
final case class ClusterProfile(
    id: ClusterId,
    name: String,
    readOnly: Boolean,
    connection: ClusterConnection,
    version: Long
)

object ClusterProfile {
  given CanEqual[ClusterProfile, ClusterProfile] = CanEqual.derived
}

/** What happened to one cluster's profile. */
enum ProfileChange {

  /** Its version or effective settings moved. Recreation can reuse a version, so `from` can equal `to`.
    * `from` is `None` for a cluster this client had never seen.
    */
  case Updated(id: ClusterId, from: Option[Long], to: Long)

  /** It is gone from a **successful** listing. Never fired because a fetch failed: "I cannot see the list" is
    * not "the cluster was deleted", and treating the two the same tears down every Kafka client in the
    * process during a network blip.
    */
  case Removed(id: ClusterId)
}

object ProfileChange {
  given CanEqual[ProfileChange, ProfileChange] = CanEqual.derived
}

/** Whether this client is in touch with the cluster service, and since when.
  *
  * The consuming service folds this into its own capability report (ADR-039): a topic service serving a
  * three-minute-old profile is `Degraded` with a reason, not `Available` and not `Unavailable`.
  *
  * @param failingSince
  *   **sticky**: the instant the current run of failures began, not the instant of the latest attempt. An
  *   operator's question is "how long has this been broken", and a field that moved with every retry would
  *   answer "a second ago" during an outage that started at breakfast
  * @param subscribed
  *   whether the change stream is currently open. With it closed the client still works, from the fallback
  *   poll, but changes arrive up to one poll interval late — which is the difference between "my cluster edit
  *   did not take effect" and "my cluster edit took a minute"
  */
final case class ProfileClientHealth(
    lastSuccessAt: Option[Instant],
    lastError: Option[KuiError],
    failingSince: Option[Instant],
    subscribed: Boolean
)

object ProfileClientHealth {

  /** Before the first attempt. Not "healthy": nothing has succeeded yet. */
  val initial: ProfileClientHealth = ProfileClientHealth(None, None, None, subscribed = false)
}

/** How a Kafka-facing KUI service learns what it may connect to.
  *
  * ==One implementation, on purpose==
  *
  * Four services need this — topics (M2), messages (M3), consumer groups (M4) and security (M7) — and each
  * needs the same protocol: a conditional fetch, a change subscription, a fallback poll, a last-known cache
  * and a cancellation path. A protocol implemented four times is a protocol implemented four different ways,
  * and the differences would be invisible from inside any one of them. ADR-046 makes this module the only
  * implementation; rule A11 is what admits the dependency and admits nothing else.
  *
  * ==Everything returns a value, and nothing throws==
  *
  * Each of these can be answered from the cache while the cluster service is down, which is the whole point:
  * a running scrape must not stop because the process that knows the passwords is restarting.
  */
trait ClusterProfiles[F[_]] {

  /** Every cluster this KUI knows about. Served from the last known set when the cluster service is
    * unreachable; empty only before the first successful fetch.
    */
  def all: F[Map[ClusterId, ClusterProfile]]

  def get(id: ClusterId): F[Option[ClusterProfile]]

  /** Registers a handler for profile changes and returns its deregistration.
    *
    * A callback rather than an `fs2.Stream`, for the reason ADR-041 Amendment 3 gives about the domain: a
    * stream is a concrete runtime type in a signature, and this interface is implemented twice — over HTTP
    * and in process — by callers who should not have to agree about a streaming library in order to do it.
    */
  def onChange(handler: ProfileChange => F[Unit]): F[F[Unit]]

  /** Whether the last attempt to reach the cluster service succeeded, and when. */
  def health: F[ProfileClientHealth]
}
