## Browse the contracts

The repository publishes merged API contracts. Download [OpenAPI JSON](/api/openapi.json) or the [browser-facing OpenAPI JSON](/api/openapi.browser.json) from this site. They describe endpoints and data types; the generated frontend client is built from these contracts.

For a running deployment, inspect `/api/v1/info` to identify its build, and use `/api/v1/health/live` and `/api/v1/health/ready` for runtime probes. See [observability](../observability/) for what each health result means.

## Stable errors

Read the [generated error-code reference](https://github.com/worxbend/kui/blob/main/docs/api/error-codes.md) for HTTP statuses, retryability, areas, and meanings. A retryable transport failure is different from a permission refusal or invalid operation.

## Runtime architecture

KUI supports an all-in-one backend or a gateway with independent domain services. The frontend remains a separate nginx image in both shapes. Domains share contracts while keeping application boundaries explicit; the gateway composes their capabilities.

Read the [architecture document](https://github.com/worxbend/kui/blob/main/ARCHITECTURE.md) for the module graph, fault isolation, streaming behavior, and architectural decisions.

## Contribute to the microsite

The microsite lives in `website/`. Its build renders static HTML from Markdown, imports the existing operations guides, and emits the screenshots, search index, and sitemap. [Source and local build instructions](https://github.com/worxbend/kui/tree/main/website) explain how to update it.
