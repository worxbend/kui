package kui.serde.confluent

import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.AtomicReference

/** Runs a codec call where a stack overflow can be observed and asserted on instead of depending on the JIT.
  */
object Isolated {

  /** `body` on a fresh thread with a 256 KiB stack, returning whatever it threw, fatal errors included.
    *
    * The small stack makes an overflow deterministic. Whether a few thousand levels of recursion fit in the
    * default 1 MiB depends on how far the JIT has compiled the recursive methods, and a regression test whose
    * outcome depends on JIT state proves nothing.
    */
  def onSmallStack[A](body: => A): Either[Throwable, A] = {
    val outcome = new AtomicReference[Either[Throwable, A]]()
    val thread = new Thread(
      null,
      () =>
        outcome.set(try Right(body)
        catch { case thrown: Throwable => Left(thrown) }),
      "serde-confluent-small-stack",
      256L * 1024L
    )
    thread.start()
    thread.join()
    outcome.get()
  }

  /** `(result, bytes allocated on this thread while body ran)`.
    *
    * Used to show that a schema-declared buffer was reserved before any of the record was read. The counter
    * is HotSpot's; a JVM without it cannot run the assertion, and saying so is better than a number of zero.
    */
  def allocating[A](body: => A): (A, Long) =
    ManagementFactory.getThreadMXBean match {
      case bean: com.sun.management.ThreadMXBean =>
        val thread = Thread.currentThread().threadId()
        val before = bean.getThreadAllocatedBytes(thread)
        val result = body
        (result, bean.getThreadAllocatedBytes(thread) - before)
      case other => throw new IllegalStateException(s"no per-thread allocation counter on $other")
    }
}
