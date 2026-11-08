package kui.serde.confluent

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
}
