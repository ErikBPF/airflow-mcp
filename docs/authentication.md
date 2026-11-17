# Authentication and access

There are two separate boundaries: a client launching this MCP process, and the process authenticating to Airflow. Airflow credentials are not MCP bearer tokens.

## Client to MCP

The initial release supports local stdio. OS process ownership and the client's permission to launch the command establish access. `--allow-writes` adds mutation and connection-test tools; `--allow-connection-export` separately adds the privileged export tool. Tool annotations describe read/destructive behavior, but Airflow RBAC remains the authorization authority. Startup flags are enforced by registration: disabled tools are absent and cannot be called through MCP.

There is no Streamable HTTP, SSE listener, OAuth callback server or automatic token passthrough. A future remote transport would need MCP protected-resource metadata, an authorization-server integration, token audience validation and per-user upstream authorization. Simply adding a shared API token to an unauthenticated HTTP listener is not supported.

## MCP to Airflow

| Setting | Behavior |
|---|---|
| `AIRFLOW_URL` | API base URL, default `http://127.0.0.1:8080`; HTTPS required except loopback HTTP |
| `AIRFLOW_TOKEN` | Existing Airflow bearer token supplied by the client environment |
| `AIRFLOW_AUTH_FILE` | Mode `0600`, gitignored, untracked `*.secrets.json`; either a `token` or `username`/`password` object |
| `AIRFLOW_CONNECTION_EXPORT_COMMAND` | Optional JSON argv array for an administrator-owned exporter; not an authentication mechanism for REST |

Set exactly one of `AIRFLOW_TOKEN` or `AIRFLOW_AUTH_FILE`. Username/password files are exchanged at `/auth/token`; support depends on the deployed Airflow auth manager. The exchange is repeated per API operation, with no persistent token cache. Bearer tokens supplied by a client are not refreshed automatically; replace expired tokens through that client's secret mechanism.

Use a dedicated least-privilege identity. SimpleAuthManager is suitable for a disposable test instance; production deployments need their supported auth manager and RBAC configuration. OAuth/OIDC-authenticated Airflow can be used with a valid Airflow API token, but this client does not perform interactive OIDC login, client-credentials grants or token exchange with an arbitrary identity provider.

TLS verification remains enabled. Redirects and ambient proxy configuration are disabled. URLs may not contain credentials, query strings or fragments. Error responses are withheld because upstream messages may contain credentials. Authentication files and exported connection files must live in a Git workspace whose ignore rules cover `*.secrets.json`; files outside that boundary are rejected.

The [MCP authorization specification](https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization) distinguishes local stdio credentials from HTTP authorization. This implementation follows that separation; it does not claim HTTP OAuth compliance.
