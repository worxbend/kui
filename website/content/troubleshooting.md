## The quickstart will not start

Check `docker info` and `docker compose version`. Read the script's port-conflict message and select unused ports using the [quickstart overrides](../quickstart/). The first start compiles the backend and downloads images; inspect build output before assuming it has stalled.

## The browser shows a gateway error

Confirm that both the frontend and backend are running. The UI container serves `/ui/` on port 8082 internally; it proxies the API to `KUI_GATEWAY_URL`. Check the selected Compose project's status and bounded logs:

```bash
docker compose --project-directory deployment/quickstart \
  -f deployment/quickstart/docker-compose.quickstart.yml \
  -f deployment/frontend/docker-compose.frontend.yml \
  logs --tail=200 kui frontend
```

If you changed host ports, use those ports when checking `/healthz`, `/api/v1/health/ready`, and `/api/v1/info`. A frontend health response alone does not prove backend readiness.

## Kafka is unreachable

Verify that bootstrap and advertised broker addresses resolve and connect from the KUI backend container. Host `localhost` is not container `localhost`. Check SASL credentials, security protocol, truststore, and hostname verification. Read-only mode does not bypass the need for Kafka read permissions.

## An integration says not configured

Schema Registry, Connect, and ksqlDB are optional upstreams. Configure the integration for the selected cluster. Check endpoint reachability, credentials, TLS, and the private-upstream policy. A working topic list does not prove that these independent integrations are reachable.

## A filter returns no records

Select the right serde and load a visible sample. Use **Try it** before **Use this filter**. Distinguish a valid non-match from a missing-field or non-boolean evaluation error. Check the browse start point and retention window.

## A copy reports zero or partial results

Read both read and written counts. Selected offsets may have aged out of the source. Verify the destination before retrying because copies already written remain and a retry can duplicate them. Keep each request within the 10,000-record bound.

## A plan cannot be applied

A plan may expire or the action may be blocked by permissions, cluster read-only mode, group state, or broker authorization. Return to the editor, create a new preview, and review it; do not assume it has the same offsets as the earlier plan.

## Report a useful issue

Include the source revision, frontend and API build information, deployment shape, sanitized configuration, exact workflow, and relevant request IDs or bounded logs. Remove credentials and sensitive payloads. See [observability](../observability/) and [GitHub issues](https://github.com/worxbend/kui/issues).
