"""Local stdio MCP for Apache Airflow 3 operations."""
import json
import os
import re
import sys
import time
from pathlib import Path
import argparse
import stat
import subprocess
from datetime import datetime, timezone
from urllib.parse import quote, urlsplit
from typing import Annotated, Literal

from pydantic import Field

import httpx
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

ROOT = Path.cwd()
BASE = os.environ.get("AIRFLOW_URL", "http://127.0.0.1:8080").rstrip("/")
mcp = FastMCP("airflow-mcp")


def private_file(path):
    path = Path(path).absolute()
    if not path.name.endswith(".secrets.json") or path.parent.resolve() != path.parent:
        raise ValueError("Use a local *.secrets.json path without symlink parents")
    if path.is_symlink():
        raise ValueError("Symlink destinations are forbidden")
    ignored = subprocess.run(["git", "check-ignore", "-q", "--", str(path)], cwd=path.parent, capture_output=True)
    tracked = subprocess.run(["git", "ls-files", "--error-unmatch", "--", str(path)], cwd=path.parent, capture_output=True)
    if ignored.returncode != 0 or tracked.returncode == 0:
        raise ValueError("Secret file must be gitignored and untracked")
    return path


def read_secret(path):
    path = private_file(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd) as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600:
            raise ValueError("Secret input must be a regular mode 0600 file")
        return json.load(stream)


def write_secret(path, data):
    path = private_file(path)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(data, stream, indent=2)
        stream.write("\n")
    return {"path": str(path), "saved": True}


def auth_headers(client):
    token = os.environ.get("AIRFLOW_TOKEN")
    auth_file = os.environ.get("AIRFLOW_AUTH_FILE")
    if token and auth_file:
        raise ValueError("Set AIRFLOW_TOKEN or AIRFLOW_AUTH_FILE, not both")
    if auth_file:
        credentials = read_secret(auth_file)
        if not isinstance(credentials, dict):
            raise ValueError("Authentication file must contain a JSON object")
        token = credentials.get("token")
        if not token:
            if not all(isinstance(credentials.get(key), str) and credentials[key] for key in ("username", "password")):
                raise ValueError("Authentication file requires token or username/password")
            response = client.post("/auth/token", json={key: credentials[key] for key in ("username", "password")})
            if response.status_code not in (200, 201):
                raise RuntimeError("Airflow authentication failed; response withheld")
            token = response.json().get("access_token")
    if not isinstance(token, str) or not token:
        raise ValueError("Configure AIRFLOW_TOKEN or a private AIRFLOW_AUTH_FILE")
    return {"Authorization": "Bearer " + token}


def api(method, path, body=None, params=None):
    try:
        with httpx.Client(base_url=BASE, timeout=30, trust_env=False, follow_redirects=False) as client:
            headers = auth_headers(client)
            response = client.request(method, "/api/v2" + path, json=body, params=params, headers=headers)
            if not response.is_success:
                raise RuntimeError(f"Airflow request failed (HTTP {response.status_code}); response withheld")
            return response.json() if response.content else {}
    except (httpx.HTTPError, KeyError, json.JSONDecodeError):
        raise RuntimeError("Airflow request failed; credential-bearing details withheld") from None


def segment(value):
    if not value or value in (".", "..", "~"):
        raise ValueError("An explicit non-wildcard identifier is required")
    return quote(value, safe="")


def run_path(dag_id, run_id):
    return f"/dags/{segment(dag_id)}/dagRuns/{segment(run_id)}"


def fields(data, names):
    return {key: data.get(key) for key in names.split()}


def regex_matches(rows, patterns):
    # Run untrusted regex outside the MCP process so backtracking can be killed.
    code = """import json,re,sys
rows, patterns = json.load(sys.stdin)
compiled = {k: re.compile(v) for k, v in patterns.items()}
print(json.dumps([i for i, row in enumerate(rows) if all(p.search(row.get(k) or '') for k, p in compiled.items())]))
"""
    identifiers = [{key: row.get(key) for key in patterns} for row in rows]
    try:
        result = subprocess.run([sys.executable, "-c", code], input=json.dumps([identifiers, patterns]),
                                capture_output=True, text=True, timeout=1, check=True)
    except subprocess.TimeoutExpired:
        raise ValueError("ID regex timed out; simplify the expression") from None
    except subprocess.CalledProcessError:
        raise ValueError("ID regex evaluation failed") from None
    return [rows[i] for i in json.loads(result.stdout)]


def fetch_listing(path, key, params, patterns, paginated=True):
    patterns = {key: value for key, value in patterns.items() if value is not None}
    if not patterns:
        return api("GET", path, params=params)
    for pattern in patterns.values():
        if len(pattern) > 512:
            raise ValueError("ID regex must be at most 512 characters")
        try:
            re.compile(pattern)
        except (re.error, RecursionError):
            raise ValueError("Invalid ID regex") from None
    if "cursor" in params:
        raise ValueError("Regex uses offset pagination; omit cursor")
    rows = []
    deadline = time.monotonic() + 60
    total = None
    while True:
        if time.monotonic() > deadline:
            raise ValueError("Regex scan timed out; narrow native filters")
        query = {**params, "limit": 100, "offset": len(rows)} if paginated else params
        data = api("GET", path, params=query)
        if data["total_entries"] > 1000:
            raise ValueError("Regex scan exceeds 1000 candidates; narrow native filters")
        if total is not None and total != data["total_entries"]:
            raise ValueError("Listing changed during regex scan; retry")
        total = data["total_entries"]
        rows.extend(data[key])
        if len(rows) == total:
            break
        if not data[key] or len(rows) > total or not paginated:
            raise ValueError("Incomplete regex scan; retry with narrower native filters")
    matches = regex_matches(rows, patterns)
    offset = params.get("offset", 0)
    limit = params.get("limit", 0)
    return {key: matches[offset:offset + limit if limit else None],
            "total_entries": len(matches), "next_cursor": None}


@mcp.tool()
def list_dags(
    limit: Annotated[int, Field(ge=0)] = 50,
    offset: Annotated[int, Field(ge=0)] = 0,
    tags: list[str] | None = None,
    tags_match_mode: Literal['any', 'all'] | None = None,
    owners: list[str] | None = None,
    dag_id_pattern: str | None = None,
    dag_id_prefix_pattern: str | None = None,
    dag_display_name_pattern: str | None = None,
    dag_display_name_prefix_pattern: str | None = None,
    exclude_stale: bool = True,
    paused: bool | None = None,
    has_import_errors: bool | None = None,
    last_dag_run_state: Literal['queued', 'running', 'success', 'failed'] | None = None,
    bundle_name: str | None = None,
    bundle_version: str | None = None,
    has_asset_schedule: bool | None = None,
    asset_dependency: str | None = None,
    dag_run_start_date_gte: str | None = None,
    dag_run_start_date_gt: str | None = None,
    dag_run_start_date_lte: str | None = None,
    dag_run_start_date_lt: str | None = None,
    dag_run_end_date_gte: str | None = None,
    dag_run_end_date_gt: str | None = None,
    dag_run_end_date_lte: str | None = None,
    dag_run_end_date_lt: str | None = None,
    dag_run_state: list[str] | None = None,
    order_by: list[str] | None = None,
    is_favorite: bool | None = None,
    timetable_type: list[str] | None = None,
    dag_id_regex: str | None = None,
) -> dict:
    """List DAGs using Airflow 3.3.1 API filters, pagination and sorting.

    *_pattern is case-insensitive substring matching; *_prefix_pattern is case-sensitive.
    Dates use ISO 8601 with timezone. Lists become repeated query parameters.
    Prefix order_by fields with '-' for descending order. Unset filters use API defaults.
    dag_id_regex adds Python regex search (inline flags supported), applied before offset/limit.
    Regex scans at most 1000 native-filtered candidates; limit=0 returns all matches.
    """
    params = {key: value for key, value in locals().items() if value is not None and key != "dag_id_regex"}
    data = fetch_listing("/dags", "dags", params, {"dag_id": dag_id_regex})
    return {"dags": [fields(d, "dag_id dag_display_name is_paused has_import_errors owners tags bundle_name bundle_version") for d in data["dags"]], "total_entries": data["total_entries"]}


@mcp.tool()
def list_tasks(
    dag_id: str,
    order_by: str = 'task_id',
    task_id_regex: str | None = None,
) -> dict:
    """List task definitions in a DAG. This API supports order_by only (default task_id).

    For state, duration, queue, pool or attempt filters, use list_task_instances.
    task_id_regex adds Python regex search over actual IDs, with a 1000-candidate cap.
    """
    data = fetch_listing(f"/dags/{segment(dag_id)}/tasks", "tasks", {"order_by": order_by},
                         {"task_id": task_id_regex}, paginated=False)
    return {"tasks": [fields(t, "task_id task_display_name operator_name downstream_task_ids pool queue") for t in data["tasks"]], "total_entries": data["total_entries"]}


@mcp.tool()
def list_task_instances(
    dag_id: str,
    dag_run_id: str,
    cursor: str | None = None,
    task_id: str | None = None,
    run_after_gte: str | None = None,
    run_after_gt: str | None = None,
    run_after_lte: str | None = None,
    run_after_lt: str | None = None,
    logical_date_gte: str | None = None,
    logical_date_gt: str | None = None,
    logical_date_lte: str | None = None,
    logical_date_lt: str | None = None,
    start_date_gte: str | None = None,
    start_date_gt: str | None = None,
    start_date_lte: str | None = None,
    start_date_lt: str | None = None,
    end_date_gte: str | None = None,
    end_date_gt: str | None = None,
    end_date_lte: str | None = None,
    end_date_lt: str | None = None,
    updated_at_gte: str | None = None,
    updated_at_gt: str | None = None,
    updated_at_lte: str | None = None,
    updated_at_lt: str | None = None,
    duration_gte: float | None = None,
    duration_gt: float | None = None,
    duration_lte: float | None = None,
    duration_lt: float | None = None,
    task_display_name_pattern: str | None = None,
    task_display_name_prefix_pattern: str | None = None,
    task_group_id: str | None = None,
    dag_id_pattern: str | None = None,
    dag_id_prefix_pattern: str | None = None,
    run_id_pattern: str | None = None,
    run_id_prefix_pattern: str | None = None,
    state: list[str] | None = None,
    pool: list[str] | None = None,
    pool_name_pattern: str | None = None,
    pool_name_prefix_pattern: str | None = None,
    queue: list[str] | None = None,
    queue_name_pattern: str | None = None,
    queue_name_prefix_pattern: str | None = None,
    executor: list[str] | None = None,
    version_number: list[int] | None = None,
    try_number: list[int] | None = None,
    operator: list[str] | None = None,
    operator_name_pattern: str | None = None,
    operator_name_prefix_pattern: str | None = None,
    map_index: list[int] | None = None,
    rendered_map_index_pattern: str | None = None,
    rendered_map_index_prefix_pattern: str | None = None,
    limit: Annotated[int, Field(ge=0)] = 50,
    offset: Annotated[int, Field(ge=0)] = 0,
    order_by: list[str] | None = None,
    dag_id_regex: str | None = None,
    task_id_regex: str | None = None,
) -> dict:
    """List task executions with all Airflow 3.3.1 API query filters.

    Use '~' for all DAGs or runs. Date filters are timezone-aware ISO 8601 strings.
    Lists use repeated query parameters; order_by accepts '-' for descending order.
    cursor='' starts cursor pagination; pass returned next_cursor for the next page.
    When cursor is supplied, Airflow ignores offset. Patterns match substrings;
    prefix patterns are case-sensitive. No rendered fields or XCom values are returned.
    ID regexes use Python search with inline flags and combine with native filters using AND.
    Regex scans at most 1000 candidates before offset/limit pagination, forbids cursor,
    and returns a filtered total. With regex, limit=0 returns all matches.
    """
    params = {key: value for key, value in locals().items()
              if key not in ("dag_id", "dag_run_id", "dag_id_regex", "task_id_regex") and value is not None}
    dag = "~" if dag_id == "~" else segment(dag_id)
    run = "~" if dag_run_id == "~" else segment(dag_run_id)
    data = fetch_listing(f"/dags/{dag}/dagRuns/{run}/taskInstances", "task_instances", params,
                         {"dag_id": dag_id_regex, "task_id": task_id_regex})
    return {"task_instances": [fields(t, "dag_id dag_run_id task_id task_display_name map_index state try_number start_date end_date duration pool queue operator") for t in data["task_instances"]],
            "total_entries": data["total_entries"], "next_cursor": data.get("next_cursor")}


@mcp.tool()
def set_dag_paused(dag_id: str, paused: bool) -> dict:
    """Turn a DAG off/on by pausing/unpausing it; does not cancel running compute."""
    path = f"/dags/{segment(dag_id)}"
    api("PATCH", path, {"is_paused": paused}, {"update_mask": "is_paused"})
    return fields(api("GET", path), "dag_id is_paused")


@mcp.tool()
def list_runs(dag_id: str, limit: int = 50, offset: int = 0) -> dict:
    """List existing runs; never creates runs."""
    data = api("GET", f"/dags/{segment(dag_id)}/dagRuns", params={"limit": min(max(limit, 1), 100), "offset": max(offset, 0)})
    return {"dag_runs": [fields(r, "dag_run_id state start_date end_date logical_date") for r in data["dag_runs"]], "total_entries": data["total_entries"]}


@mcp.tool()
def inspect_run(dag_id: str, run_id: str) -> dict:
    """Snapshot an existing run, its task attempts and elapsed wall-clock time (not summed task time)."""
    path = run_path(dag_id, run_id)
    run = api("GET", path)
    tasks = api("GET", path + "/taskInstances", params={"limit": 100})
    now = datetime.now(timezone.utc)
    elapsed = None
    if run.get("start_date"):
        end = datetime.fromisoformat(run["end_date"]) if run.get("end_date") else now
        elapsed = max(0, (end - datetime.fromisoformat(run["start_date"])).total_seconds())
    return {"run": fields(run, "dag_run_id state start_date end_date"), "observed_at": now.isoformat(),
            "elapsed_seconds": elapsed, "tasks": [fields(t, "task_id map_index state try_number start_date end_date duration") for t in tasks["task_instances"]],
            "total_tasks": tasks["total_entries"], "truncated": tasks["total_entries"] > len(tasks["task_instances"])}


@mcp.tool()
def fail_run(dag_id: str, run_id: str) -> dict:
    """Mark this existing run failed. Does not prove termination of external compute."""
    path = run_path(dag_id, run_id)
    api("PATCH", path, {"state": "failed"})
    return fields(api("GET", path), "dag_run_id state")


def restart_body(run_id, task_ids):
    segment(run_id)
    if task_ids == []:
        raise ValueError("Select tasks, or omit task_ids to restart the full existing run")
    body = {"dag_run_id": run_id, "dry_run": True, "only_failed": False, "reset_dag_runs": True,
            "include_downstream": task_ids is not None, "include_upstream": False,
            "include_past": False, "include_future": False, "run_on_latest_version": False,
            "prevent_running_task": True}
    if task_ids is not None:
        body["task_ids"] = task_ids
    return body


@mcp.tool()
def restart_run(dag_id: str, run_id: str, task_ids: list[str] | None = None, execute: bool = False) -> dict:
    """Preview or restart selected tasks plus downstream; omit task_ids for the full existing run.

    Keeps the run ID and original DAG version. Refuses active runs. Never triggers a new run.
    A mapped task ID selects all map indexes; per-index selection is not supported in this pilot.
    """
    path = run_path(dag_id, run_id)
    if api("GET", path)["state"] not in ("failed", "success"):
        raise ValueError("Pilot restarts require a terminal run")
    body = restart_body(run_id, task_ids)
    clear_path = f"/dags/{segment(dag_id)}/clearTaskInstances"
    preview = api("POST", clear_path, body)
    selected = [fields(t, "task_id map_index") for t in preview["task_instances"]]
    if execute:
        if api("GET", path)["state"] not in ("failed", "success"):
            raise ValueError("Run state changed; inspect it again")
        api("POST", clear_path, {**body, "dry_run": False})
    return {"dag_run_id": run_id, "executed": execute, "tasks": selected}


@mcp.tool()
def export_connection(connection_id: str, destination: str) -> dict:
    """Export actual connection values directly to a new local ignored *.secrets.json, mode 0600.

    Uses an operator-configured argv command because REST masks passwords. Never returns values.
    """
    command_json = os.environ.get("AIRFLOW_CONNECTION_EXPORT_COMMAND")
    if not command_json:
        raise ValueError("No connection export command configured")
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,199}", connection_id):
        raise ValueError("Connection export requires a simple connection ID")
    try:
        command = json.loads(command_json)
    except json.JSONDecodeError:
        raise ValueError("Connection export command must be a JSON argv array") from None
    if not isinstance(command, list) or not command or not all(isinstance(arg, str) and arg for arg in command):
        raise ValueError("Connection export command must be a nonempty JSON argv array")
    private_file(destination)
    try:
        result = subprocess.run([*command, connection_id], capture_output=True, text=True, timeout=45)
    except (OSError, subprocess.TimeoutExpired):
        raise RuntimeError("Connection export command failed; details withheld") from None
    if result.returncode:
        raise RuntimeError("Connection export failed; details withheld")
    try:
        data = json.loads(result.stdout.splitlines()[-1])
    except (ValueError, IndexError):
        raise RuntimeError("Connection export did not return connection JSON") from None
    allowed = {"connection_id", "conn_type", "description", "host", "port", "login", "password", "schema", "extra"}
    if (not isinstance(data, dict) or data.get("connection_id") != connection_id
            or not isinstance(data.get("conn_type"), str) or not data["conn_type"] or set(data) - allowed):
        raise ValueError("Exporter must return the requested connection object with a connection type")
    return write_secret(destination, data)


@mcp.tool()
def edit_connection(connection_id: str, source: str) -> dict:
    """Edit a database-managed connection from local mode 0600 *.secrets.json. Never returns values."""
    segment(connection_id)
    data = read_secret(source)
    allowed = {"connection_id", "conn_type", "description", "host", "login", "schema", "port", "password", "extra"}
    if not isinstance(data, dict) or set(data) - allowed or data.get("connection_id", connection_id) != connection_id:
        raise ValueError("Connection JSON has unexpected fields or a different connection_id")
    data["connection_id"] = connection_id
    if isinstance(data.get("extra"), dict):
        data["extra"] = json.dumps(data["extra"])
    api("PATCH", f"/connections/{segment(connection_id)}", data,
        {"update_mask": [k for k in data if k != "connection_id"]})
    return {"connection_id": connection_id, "updated": True}


@mcp.tool()
def test_connection(connection_id: str) -> dict:
    """Test stored connection from the API server; returns status without credential-bearing messages."""
    stored = api("GET", f"/connections/{segment(connection_id)}")
    data = api("POST", "/connections/test", {"connection_id": connection_id, "conn_type": stored["conn_type"]})
    return {"connection_id": connection_id, "status": data["status"], "execution_location": "api-server"}


READ_TOOLS = (list_dags, list_tasks, list_task_instances, list_runs, inspect_run)
WRITE_TOOLS = (set_dag_paused, fail_run, restart_run, edit_connection, test_connection)
_registered = {fn.__name__ for fn in (*READ_TOOLS, *WRITE_TOOLS, export_connection)}


def configure_tools(allow_writes=False, allow_export=False):
    for name in tuple(_registered):
        mcp.remove_tool(name)
    _registered.clear()
    enabled = [*READ_TOOLS]
    if allow_writes:
        enabled.extend(WRITE_TOOLS)
    if allow_export:
        if not os.environ.get("AIRFLOW_CONNECTION_EXPORT_COMMAND"):
            raise ValueError("Configure a connection export command before enabling export")
        enabled.append(export_connection)
    for fn in enabled:
        readonly = fn in READ_TOOLS
        mcp.add_tool(fn, annotations=ToolAnnotations(readOnlyHint=readonly,
                     destructiveHint=not readonly, openWorldHint=True))
        _registered.add(fn.__name__)


def main():
    parser = argparse.ArgumentParser(description="Local stdio MCP for Apache Airflow 3")
    parser.add_argument("--allow-writes", action="store_true", help="Expose run/DAG mutations and connection edit/test tools")
    parser.add_argument("--allow-connection-export", action="store_true", help="Enable the configured privileged connection exporter")
    args = parser.parse_args()
    url = urlsplit(BASE)
    if url.username or url.password or url.query or url.fragment or not url.hostname:
        parser.error("AIRFLOW_URL must be a base URL without credentials, query or fragment")
    if url.scheme != "https" and not (url.scheme == "http" and url.hostname in ("localhost", "127.0.0.1", "::1")):
        parser.error("Use HTTPS for Airflow, or HTTP through a loopback tunnel")
    try:
        configure_tools(args.allow_writes, args.allow_connection_export)
    except ValueError as error:
        parser.error(str(error))
    mcp.run()


configure_tools()

if __name__ == "__main__":
    main()
