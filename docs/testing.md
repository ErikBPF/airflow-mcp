# Testing

## Local checks

```sh
uv sync --locked
uv run python -m unittest discover -s tests -p 'test_*.py' -v
uv build
```

Tests do not need Airflow, Kubernetes or credentials. The regex timeout test intentionally evaluates a pathological expression in a killable child process. Authentication checks use synthetic responses. The API query-name fixture was captured from Airflow 3.3.1; live validation checks against the current server schema too.

## Disposable integration instance

`examples/sandbox.yaml` runs Airflow 3.3.1 standalone with ephemeral metadata and a tiny localhost-only POST endpoint for positive connection tests. It is a test fixture, not production deployment guidance. Pod replacement loses its runs and connections. Choose an isolated Minikube profile and use its explicit context for every command.

```sh
minikube start -p airflow-mcp --cpus=2 --memory=4096 --keep-context
kubectl --context airflow-mcp create namespace airflow-mcp
```

Create synthetic passwords without displaying them, from this Git checkout:

```sh
python - <<'PY'
import json, os, secrets
password = secrets.token_urlsafe(32)
for path, data in [
    ('sandbox-auth.secrets.json', {'username': 'admin', 'password': password}),
    ('sandbox-passwords.secrets.json', {'admin': password}),
]:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as stream:
        json.dump(data, stream)
PY
kubectl --context airflow-mcp -n airflow-mcp create secret generic airflow-auth --from-file=passwords.json=sandbox-passwords.secrets.json
kubectl --context airflow-mcp -n airflow-mcp create configmap validation-dag --from-file=sandbox_dag.py=examples/sandbox-dag.py
kubectl --context airflow-mcp apply -f examples/sandbox.yaml
kubectl --context airflow-mcp -n airflow-mcp rollout status deployment/airflow --timeout=180s
kubectl --context airflow-mcp -n airflow-mcp port-forward service/airflow 18080:8080 --address 127.0.0.1
```

Keep that forwarding terminal open. Set `AIRFLOW_URL=http://127.0.0.1:18080` and `AIRFLOW_AUTH_FILE` to the absolute path of `sandbox-auth.secrets.json`; unset `AIRFLOW_TOKEN` if it was previously configured. For the connection-export checks, install the helper into the disposable Airflow container:

```sh
kubectl --context airflow-mcp -n airflow-mcp exec -i deployment/airflow -c airflow -- python -c 'import pathlib,sys; pathlib.Path("/tmp/export_connection.py").write_text(sys.stdin.read())' < examples/export_connection.py
export AIRFLOW_CONNECTION_EXPORT_COMMAND='["kubectl","--context","airflow-mcp","-n","airflow-mcp","exec","deployment/airflow","-c","airflow","--","python","/tmp/export_connection.py"]'
```

After explicitly authorizing mutations on this disposable target:

```sh
AIRFLOW_MCP_LIVE_TEST=1 uv run python tests/validate.py
uv run python tests/validate_listing.py
```

The first command creates synthetic fixtures directly, then exercises real MCP calls. Its success marker is `AIRFLOW_MCP_E2E_PASS`. It checks same-run task/full restarts, unaffected tasks, failed-state mutation, timing, credential export permissions, login/password edits, positive and negative connection tests. The final synthetic run is left failed and its DAG paused. Exported synthetic files remain ignored locally.

The second is read-only and requires those fixtures. Its marker is `AIRFLOW_MCP_LISTING_E2E_PASS`. It checks native parameter parity, regex, filtered pagination, native cursor pagination, timestamp boundaries and timezone equivalence. CI runs unit/build checks only; it does not provision a cluster or claim these live checks ran there.

Stop the forwarding process and `minikube stop -p airflow-mcp` when finished. Delete only this dedicated profile if its synthetic data is no longer needed. Never use the mutating validator against production.
