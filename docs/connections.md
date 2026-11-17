# Connection management

`edit_connection` reads a local mode `0600`, gitignored, untracked `*.secrets.json` directly. It validates the connection ID and allowed fields, then patches the Airflow database entry. Values never appear in the tool response. Use the authoritative secret backend's own workflow for externally managed connections: a metastore edit can be shadowed by environment or backend values.

`test_connection` submits the stored connection ID and type without resending masked passwords. Airflow must have connection testing enabled, and the provider hook must implement it. The result identifies the API server as the execution location and omits potentially sensitive diagnostic text. A successful API-server test does not prove worker connectivity. Connection testing is gated with write tools because it makes outbound requests using credentials.

## Privileged export

REST masks connection passwords. Enable actual-value export only when an operator provides a trusted command:

```json
{"AIRFLOW_CONNECTION_EXPORT_COMMAND": "[\"python\", \"/trusted/path/export_connection.py\"]"}
```

Then start `airflow-mcp --allow-connection-export`. The executable and arguments come from operator configuration, never from a tool call. The server appends a validated connection ID, runs without a local shell, captures output, and enforces a 45-second timeout. IDs must start with an ASCII letter, digit or underscore and contain only letters, digits, underscores, dots or hyphens, at most 200 characters.

The helper must emit one JSON object as its last stdout line, using `connection_id`, `conn_type`, `host`, `port`, `login`, `password`, `schema`, `extra` and optional `description`. Run it inside an environment authorized to resolve Airflow connections. [The example helper](../examples/export_connection.py) uses Airflow's connection resolution; it is not a generally available API privilege. Container/SSH wrappers must be administrator-owned and must not log values or interpolate unchecked arguments.

`export_connection` accepts a destination ending in `*.secrets.json`. It refuses symlinks, tracked/nonignored paths and overwrites, creates the file at mode `0600`, and returns only its path. The helper inherits process credentials and may resolve a higher-priority secrets backend: enabling it grants access beyond normal masked REST reads. The local filesystem is still sensitive; Git ignore does not encrypt data or prevent access by the same OS user. Do not put values into MCP resource responses, prompts, examples, logs or issues.
