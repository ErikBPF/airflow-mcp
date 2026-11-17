# Design references

Reviewed upstream documentation on 2026-11-17. These projects inform behavior and documentation structure; no source code was copied from them.

| Reference | Observed pattern | Applied here |
|---|---|---|
| [GitHub MCP Server](https://github.com/github/github-mcp-server) | Explicit tool selection, read-only controls, installation examples and a documented remote OAuth story | Small documented tool inventory, read-only startup default, explicit mutation flags, client configuration examples; remote OAuth is clearly out of scope |
| [Grafana MCP](https://github.com/grafana/mcp-grafana) | Write controls, upstream service-account authentication, TLS settings and operational tool documentation | Separate upstream credentials, TLS verification, explicit privileged connection handling, capability and safety tables |
| [Official MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk) | Typed tools, stdio transport, protocol clients and tool annotations | FastMCP, typed filter schemas, read/destructive annotations and real protocol-level tests |
| [MCP authorization](https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization) | HTTP authorization is distinct from environment-based stdio access | No claim that an Airflow bearer token authenticates remote MCP clients; no unauthenticated network listener |
| [Airflow 3.3.1 REST API](https://airflow.apache.org/docs/apache-airflow/3.3.1/stable-rest-api-ref.html) | Public operational endpoints and version-specific filtering | Native query names and defaults, existing-run clearing, API-side connection testing |

The initial project supports the operational behavior already validated in a disposable Airflow instance. It deliberately avoids universal OpenAPI exposure, arbitrary SQL/shell tools, new-run creation, hosted authentication infrastructure and a second monitoring daemon. Add capabilities when their behavior and authorization can be tested, rather than advertising unimplemented features.
