# Gateway authentication boundary

When `kui.auth.type` is `form` or `oidc`, gateway application APIs require a
signed-in session even when RBAC is disabled. Public API paths are explicitly
limited to build information, liveness/readiness, and the authentication flow
(`me`, `settings`, `login`, `password`, `logout`, OIDC start/callback). Static UI
assets remain public. Authentication-disabled deployments retain anonymous API
access and CSRF checks on mutations.

## Reverse proxies and client-address budgets

By default **no proxy is trusted**. Login, password-change and anonymous-session
creation budgets use the socket peer address. A deployment behind a reverse proxy
must explicitly name its proxy's literal addresses:

```yaml
kui:
  auth:
    trustedProxies:
      - 127.0.0.1
      - "::1"
```

Use the actual gateway-facing proxy addresses, not the browser addresses. Only
literal IPv4/IPv6 addresses are accepted; hostnames, CIDRs and wildcards are not.
Do not add an address controlled by a client. Restrict direct gateway access at
the network boundary where possible.

A trusted proxy must overwrite `X-Forwarded-For` or append the actual connecting
peer to it. KUI walks that chain from right to left, starting with the socket
peer, and stops at the first untrusted address. Values farther left cannot change
the selected client. An untrusted socket peer cannot affect the result using any
forwarding header. Missing, duplicate, malformed or over-32-hop headers fall back
to the socket peer; the RFC `Forwarded` header is not used. IPv6 spellings are
canonicalized before comparison.

Budgets are per gateway process, in fixed one-minute windows:

- Login: 20 attempts per client IP and 5 per canonical username (trimmed,
  case-insensitive).
- Password changes: 10 attempts per client IP.
- New anonymous sessions: 30 per client IP and 600 process-wide. Existing valid
  sessions do not consume that creation budget. Exhaustion returns HTTP 429.

Anonymous and authenticated sessions have separate LRU capacity, each bounded by
`SessionConfig.maxSessions` (at most twice that many sessions overall). Anonymous
traffic cannot evict authenticated sessions. Responses do not restamp existing
session cookies; only creation or explicit sign-in rotation issues a cookie.

## OIDC and credential persistence

An OIDC state is bound to the initiating gateway session, expires after ten
minutes, and is consumed atomically before code redemption. A callback without
that browser's session cannot redeem its state. One pending flow per session is
kept, bounded to 10,000 pending browser sessions. Starting again replaces that
browser's previous flow. Session loss or gateway restart requires restarting
sign-in.

Identity password overrides use `ConfigStoreResource`: Kafka configuration takes
precedence, with topic bootstrap, encryption and replay owned by the resource.
Directory/empty stores remain read-only. An unreadable or undecryptable stored
credential, unavailable store, or degraded store view refuses authentication;
it never revives an older configured password. A genuinely absent override in a
readable store still permits the configured initial credential.

## Permission grant consumers

`PermissionDto` and `GrantDto` include `defaultRole`. Consumers first scope all
grants to the requested cluster. If any non-default grant exists there (regardless
of resource/action), only those grants apply. Otherwise, default-role grants
apply. For a global operation, perform the same selection across all grants.
Only then evaluate resource, action, pattern and connector-parent fallback. This
matches server `Rbac.decide`; default-role rights must not be unioned with held
role rights. The server remains authoritative.
