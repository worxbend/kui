package kui.security

import java.util.Locale

/** The account key used by credential lookup, stored overrides and edge throttling. */
object AccountName {
  def canonical(raw: String): String = raw.trim.toLowerCase(Locale.ROOT)
}
