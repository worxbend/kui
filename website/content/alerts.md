## Read the live feed

Open **Alerts** for the selected cluster. Read event details and context, then use the feed's filters to narrow the set. Cluster notifications also keep signals available alongside the current workflow.

![Cluster alert feed](/screenshots/19-alerts.webp)

## Acknowledge deliberately

After reviewing an event, use its acknowledgement action when your role permits it. Acknowledgement records that an event has been seen; it does not resolve the underlying Kafka or upstream issue.

## Correlate with health

Use cluster metrics, broker state, and consumer lag to investigate an alert. For failures in KUI itself, correlate logs, traces, and request IDs using the [observability guide](../observability/).

## Understand guardrails

Read-only mode and RBAC constrain operations. Authenticated writes use CSRF protection, and audit records support operational review. Configure the [security posture](../security/) before deploying beyond the local playground.
