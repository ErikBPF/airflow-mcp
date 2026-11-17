# airflow-mcp

A local Model Context Protocol server for inspecting and managing Apache Airflow 3. List DAGs and tasks, investigate runs, pause scheduling, retry work in an existing run, and manage connections without returning credentials to the model.

**Read-only by default.** Mutation and privileged connection-export tools require explicit startup flags. Tested against Airflow **3.3.1**. This is an independent project, not an Apache Software Foundation project.

## Quick start

Requires Python 3.11+, Git, and an Airflow API identity. Configure a local MCP client to launch:

```json
{
  "mcpServers": {
    "airflow": {
      "command": "uvx",
      "args": ["--from", "git+https://github.com/ErikBPF/airflow-mcp", "airflow-mcp"],
      "env": {
        "AIRFLOW_URL": "https://airflow.example.com",
        "AIRFLOW_TOKEN": "<supply through your client's secret configuration>"
      }
    }
  }
}
```

Pin a reviewed commit in the Git URL for repeatable installations. No PyPI release is claimed. Never commit a real token in client configuration. Alternatively, set `AIRFLOW_AUTH_FILE` to an absolute path to a mode `0600`, gitignored, untracked `*.secrets.json` file containing either `token` or `username` and `password`. See [authentication](docs/authentication.md).

For a checkout:

```sh
uv sync --locked
uv run airflow-mcp
```

Transport is **stdio only**. The client starts the process locally; there is no network MCP listener or hosted OAuth service. Airflow requests verify TLS and do not follow redirects. HTTP is allowed only for loopback addresses, for example through a secure tunnel.

## Tools

| Tool | Purpose | Availability |
|---|---|---|
| `list_dags` | Native API filters, sorting, pagination and DAG-ID regex | Default |
| `list_tasks` | DAG task definitions, sorting and task-ID regex | Default |
| `list_task_instances` | Execution state, timestamps, duration, pools, queues, attempts, mapped indexes and regex | Default |
| `list_runs` | Existing run inventory | Default |
| `inspect_run` | Task attempts and elapsed run time; explicitly reports truncation | Default |
| `set_dag_paused` | Pause/unpause scheduling | `--allow-writes` |
| `restart_run` | Preview or clear tasks plus downstream, or a full existing run | `--allow-writes` |
| `fail_run` | Mark an existing run failed | `--allow-writes` |
| `edit_connection` | Edit a database-managed connection from a local private JSON file | `--allow-writes` |
| `test_connection` | Test a stored connection from the API server | `--allow-writes` |
| `export_connection` | Export actual values through an operator-configured privileged helper | `--allow-connection-export` |

No tool creates a new DAG run. Restart preserves its run ID and original DAG version; it defaults to preview (`execute=false`). Selected task IDs include downstream tasks. Omitting task IDs clears the full existing run, including successful tasks. Only terminal runs are accepted. A mapped task ID selects all its indexes.

Pause does not cancel running compute. Marking a run failed does not prove external jobs stopped. Retrying tasks can repeat data writes; assess task idempotency before execution.

## Filters

All query parameters from the tested API's DAG and task-instance list endpoints are exposed: 29 DAG options and 52 task-instance options. Task definitions support `order_by`; state/time filters belong to task instances. Native filters retain their original names and defaults. Repeated values use arrays. Date filters use timezone-aware ISO 8601 strings with `_gte`, `_gt`, `_lte`, `_lt` bounds.

```json
{
  "dag_id": "~",
  "dag_run_id": "~",
  "dag_id_regex": "^etl_",
  "task_id_regex": "^(extract|load)_.*$",
  "state": ["failed", "upstream_failed"],
  "start_date_gte": "2026-11-17T00:00:00Z",
  "limit": 20
}
```

Native `*_pattern` means case-insensitive substring matching; `*_prefix_pattern` means case-sensitive prefix matching. MCP's ID regex extensions use Python search, case-sensitive unless an inline flag such as `(?i)` is supplied. Native and regex filters combine using AND.

Regex scans at most 1,000 native-filtered candidates, then applies offset/limit to matches. Totals reflect matches, not the upstream page. Regex rejects `cursor`; use offset pagination. Patterns are capped at 512 characters and execute in a subprocess with a one-second timeout. With regex, `limit=0` returns all matches within the scan bound. Without regex, native cursor pagination remains available. Listings are not transactional snapshots during concurrent updates.

## Connection values

Airflow's REST responses mask passwords. This project does not disable that protection or pretend masked values are credentials. An optional administrator-configured helper can resolve actual values in the Airflow runtime and write them directly to a new local private file. The model receives only the file path and status. See [connections](docs/connections.md) for the privilege boundary and helper contract.

## Development and verification

```sh
uv sync --locked
uv run python -m unittest discover -s tests -p 'test_*.py' -v
uv build
```

[Sandbox validation](docs/testing.md) uses synthetic fixtures. Unit tests cover filter forwarding, regex bounds, credential-file handling, authentication and tool exposure. Live MCP validation exercises run recovery, connection round-trips and timestamp boundaries. These results do not establish production RBAC or external-secret backend behavior.

See [design references](docs/design-references.md), [contributing](CONTRIBUTING.md), and [security](SECURITY.md). Licensed under [Apache License 2.0](LICENSE).
