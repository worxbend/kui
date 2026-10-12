## Set the deployment boundary

Authentication defaults to disabled. The Docker playground is local. For shared access, configure form login or OIDC, explicit RBAC, HTTPS, secrets, and the permissions of the Kafka principal KUI uses.

## Form authentication

Generate a password hash through standard input:

```bash
./mill services.identity.app.runMain kui.identity.app.HashPassword
```

Store the encoded hash in a secret file, and reference it using a value such as `file:/run/secrets/admin-password-hash`. The [auth quickstart configuration](https://github.com/worxbend/kui/blob/main/deployment/quickstart/kui-quickstart-auth.yaml) demonstrates the form-login and RBAC structure. Replace its demo accounts and hashes.

## OIDC authentication

Set `issuer`, `clientId`, `clientSecret`, and `redirectUri` under `kui.auth.oidc`. Register the callback with your identity provider:

```text
<public-origin><base-path>/api/v1/auth/oidc/callback
```

Use a secret reference for the client secret. Review [gateway authentication](../gateway-authentication/) for credential persistence and proxy trust.

## Roles and read-only clusters

An empty `kui.rbac` permits all actions. Define explicit role grants for the deployment, then test each role with the actual login flow. Set `readOnly: true` for cluster profiles that must never allow mutations.

Read-only mode and RBAC are separate controls. Kafka authorization remains another boundary: KUI cannot perform a broker operation its Kafka principal is not allowed to perform.

## Cookies and HTTPS

Leave `kui.server.devInsecureCookies` unset or false in production. Secure cookies require HTTPS at the public edge. Keep `/ui/` and `/api/` on the same origin through the frontend proxy so the session and CSRF behavior remain coherent.

## Secrets and isolation

Use `env:NAME` or `file:/absolute/path` references for secret fields. Keep domain services private and distribute principal signing keys consistently. Review [production deployment](../deployment/) for key rotation, cursor signing, store encryption, and TLS to Kafka and optional upstreams.

## Payload protection

Configure [message masking](../masking/) where operators should see redacted payload fields. Read its boundaries carefully; it is not a replacement for Kafka authorization or a blanket rewrite of original record bytes.
