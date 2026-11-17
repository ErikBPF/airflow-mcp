"""Real MCP protocol validation against the dedicated disposable sandbox; synthetic fixtures only."""
import asyncio
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from airflow_mcp.server import ROOT, api, write_secret

DAG = "mcp_validation"


async def main():
    if os.environ.get("AIRFLOW_MCP_LIVE_TEST") != "1":
        raise SystemExit("Set AIRFLOW_MCP_LIVE_TEST=1 only for an authorized disposable instance")
    run_id = "mcp-validation-" + secrets.token_hex(5)
    conn_id = "mcp_fixture_" + secrets.token_hex(5)
    password = secrets.token_urlsafe(24)
    edited_password = secrets.token_urlsafe(24)
    export = ROOT / f"{conn_id}.secrets.json"
    edit = ROOT / f"{conn_id}-edit.secrets.json"
    exported_again = ROOT / f"{conn_id}-after.secrets.json"
    # Setup deliberately bypasses MCP: no create-run/create-connection tools exist.
    api("POST", "/connections", {"connection_id": conn_id, "conn_type": "http", "host": "http://127.0.0.1:8081", "login": "fixture", "password": password})
    api("POST", f"/dags/{DAG}/dagRuns", {"dag_run_id": run_id, "logical_date": None})
    before = api("GET", f"/dags/{DAG}/dagRuns")["total_entries"]
    params = StdioServerParameters(command=sys.executable, args=["-m", "airflow_mcp.server", "--allow-writes", "--allow-connection-export"], env={key: os.environ[key] for key in ("AIRFLOW_URL", "AIRFLOW_TOKEN", "AIRFLOW_AUTH_FILE", "AIRFLOW_CONNECTION_EXPORT_COMMAND") if key in os.environ})
    with open(os.devnull, "w") as errors:
        async with stdio_client(params, errlog=errors) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools = await session.list_tools()
                names = {t.name for t in tools.tools}
                assert names == {"list_dags", "list_tasks", "list_task_instances", "set_dag_paused", "list_runs", "inspect_run", "fail_run", "restart_run", "export_connection", "edit_connection", "test_connection"}, names

                async def call(name, **arguments):
                    result = await session.call_tool(name, arguments)
                    encoded = result.model_dump_json()
                    assert all(value not in encoded for value in (password, edited_password)), "Secret leaked in MCP response"
                    assert not result.isError, f"MCP tool failed: {name} (details withheld)"
                    if result.structuredContent:
                        return result.structuredContent
                    return json.loads(result.content[0].text)

                async def wait_terminal(expected):
                    deadline = time.monotonic() + 150
                    while time.monotonic() < deadline:
                        state = await call("inspect_run", dag_id=DAG, run_id=run_id)
                        if state["run"]["state"] in ("success", "failed"):
                            assert state["run"]["state"] == expected, f"Run ended {state['run']['state']}, expected {expected}"
                            return state
                        await asyncio.sleep(2)
                    raise AssertionError("Run did not finish before deadline")

                assert any(d["dag_id"] == DAG for d in (await call("list_dags"))["dags"])
                assert (await call("set_dag_paused", dag_id=DAG, paused=True))["is_paused"]
                assert not (await call("set_dag_paused", dag_id=DAG, paused=False))["is_paused"]
                first = await wait_terminal("failed")
                first_tasks = {t["task_id"]: t for t in first["tasks"]}
                assert first_tasks["retry_me"]["state"] == "failed"
                assert first_tasks["unrelated"]["state"] == "success"
                assert first["elapsed_seconds"] >= 0 and not first["truncated"]
                print("PASS pause/unpause, failed fixture, timing snapshot", flush=True)
                preview = await call("restart_run", dag_id=DAG, run_id=run_id, task_ids=["retry_me"])
                assert {t["task_id"] for t in preview["tasks"]} == {"retry_me", "downstream"}
                assert not preview["executed"]
                await call("restart_run", dag_id=DAG, run_id=run_id, task_ids=["retry_me"], execute=True)
                await asyncio.sleep(2)
                retried = await wait_terminal("success")
                retried_tasks = {t["task_id"]: t for t in retried["tasks"]}
                for task in ("upstream", "unrelated"):
                    assert retried_tasks[task]["start_date"] == first_tasks[task]["start_date"], "Unselected task reran"
                assert retried_tasks["retry_me"]["try_number"] > first_tasks["retry_me"]["try_number"]
                print("PASS selected-task plus downstream restart", flush=True)
                full = await call("restart_run", dag_id=DAG, run_id=run_id)
                assert {t["task_id"] for t in full["tasks"]} == set(first_tasks)
                await call("restart_run", dag_id=DAG, run_id=run_id, execute=True)
                await asyncio.sleep(2)
                complete = await wait_terminal("success")
                for task in complete["tasks"]:
                    assert task["start_date"] != retried_tasks[task["task_id"]]["start_date"], "Full restart omitted task"
                assert (await call("fail_run", dag_id=DAG, run_id=run_id))["state"] == "failed"
                runs = await call("list_runs", dag_id=DAG)
                assert runs["total_entries"] == before, "MCP created a new run"
                assert any(r["dag_run_id"] == run_id for r in runs["dag_runs"])
                print("PASS full restart, mark failed, unchanged run identity/count", flush=True)
                saved = await call("export_connection", connection_id=conn_id, destination=str(export))
                assert saved["saved"] and export.stat().st_mode & 0o777 == 0o600
                subprocess.run(["git", "check-ignore", "-q", str(export)], cwd=ROOT, check=True)
                data = json.loads(export.read_text())
                assert data["password"] == password, "Export returned masked/incorrect password"
                data["description"] = "MCP synthetic edit verified"
                data["login"] = "edited-fixture"
                data["password"] = edited_password
                write_secret(edit, data)
                await call("edit_connection", connection_id=conn_id, source=str(edit))
                await call("export_connection", connection_id=conn_id, destination=str(exported_again))
                after = json.loads(exported_again.read_text())
                assert after["login"] == "edited-fixture" and after["password"] == edited_password
                tested = await call("test_connection", connection_id=conn_id)
                assert tested["status"] is True, "Connection test did not succeed"
                assert tested["execution_location"] == "api-server"
                # A known rejecting endpoint proves failed tests are not reported as successful.
                negative = ROOT / f"{conn_id}-negative.secrets.json"
                write_secret(negative, {"connection_id": conn_id, "conn_type": "http", "host": "http://127.0.0.1:8080"})
                await call("edit_connection", connection_id=conn_id, source=str(negative))
                assert (await call("test_connection", connection_id=conn_id))["status"] is False
                await call("edit_connection", connection_id=conn_id, source=str(edit))
                print("PASS private unmasked export, edit round-trip, connection test", flush=True)
                assert (await call("set_dag_paused", dag_id=DAG, paused=True))["is_paused"]
                print("AIRFLOW_MCP_E2E_PASS", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
