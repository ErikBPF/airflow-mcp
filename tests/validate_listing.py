"""Read-only protocol checks against an existing synthetic DAG and runs."""
import asyncio
import json
import os
from pathlib import Path
import sys
from datetime import datetime, timedelta, timezone

import httpx
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from airflow_mcp.server import BASE


async def main():
    schema = httpx.get(BASE + "/openapi.json", timeout=10, trust_env=False).json()
    contract = json.loads((Path(__file__).parents[1] / "tests/list_api_contract.json").read_text())
    params = StdioServerParameters(command=sys.executable, args=["-m", "airflow_mcp.server"], env={key: os.environ[key] for key in ("AIRFLOW_URL", "AIRFLOW_TOKEN", "AIRFLOW_AUTH_FILE", "AIRFLOW_CONNECTION_EXPORT_COMMAND") if key in os.environ})
    with open(os.devnull, "w") as errors:
        async with stdio_client(params, errlog=errors) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools = {t.name: t for t in (await session.list_tools()).tools}
                for name, operation in contract.items():
                    actual = set(tools[name].inputSchema["properties"]) - {"dag_id_regex", "task_id_regex"}
                    expected = {p["name"] for p in schema["paths"][operation["path"]]["get"]["parameters"]}
                    assert actual == expected, f"API parameter mismatch: {name}"

                async def call(name, **args):
                    result = await session.call_tool(name, args)
                    assert not result.isError, f"Listing tool failed: {name}"
                    return result.structuredContent or json.loads(result.content[0].text)

                dags = await call("list_dags", dag_id_pattern="MCP_VALIDATION", paused=True,
                                  has_import_errors=False, order_by=["-dag_id"], limit=1)
                assert [d["dag_id"] for d in dags["dags"]] == ["mcp_validation"]
                absent = await call("list_dags", dag_id_pattern="MCP_VALIDATION", paused=False)
                assert absent["total_entries"] == 0
                assert not (await call("list_dags", tags=["nonexistent-fixture-tag"], tags_match_mode="all"))["dags"]
                tasks = await call("list_tasks", dag_id="mcp_validation", order_by="-task_id")
                ids = [t["task_id"] for t in tasks["tasks"]]
                assert set(ids) == {"upstream", "retry_me", "downstream", "unrelated"}
                assert ids == sorted(ids, reverse=True)

                runs = await call("list_runs", dag_id="mcp_validation")
                run_id = runs["dag_runs"][0]["dag_run_id"]
                args = {"dag_id": "mcp_validation", "dag_run_id": run_id}
                all_tasks = await call("list_task_instances", **args)
                assert all_tasks["total_entries"] == 4
                target = all_tasks["task_instances"][0]
                selected = await call("list_task_instances", **args, task_id=target["task_id"],
                                      state=[target["state"]], map_index=[-1], duration_gte=0)
                assert [t["task_id"] for t in selected["task_instances"]] == [target["task_id"]]
                empty = await call("list_task_instances", **args, task_id="not_a_task")
                assert empty["total_entries"] == 0
                page = await call("list_task_instances", **args, cursor="", limit=2)
                assert len(page["task_instances"]) == 2 and page["next_cursor"]
                next_page = await call("list_task_instances", **args, cursor=page["next_cursor"], limit=2)
                keys = lambda rows: {(t["task_id"], t["map_index"]) for t in rows}
                assert not keys(page["task_instances"]) & keys(next_page["task_instances"])
                assert keys(page["task_instances"]) | keys(next_page["task_instances"]) == keys(all_tasks["task_instances"])
                wildcard = await call("list_task_instances", dag_id="~", dag_run_id="~",
                                      dag_id_pattern="mcp_validation", run_id_pattern=run_id)
                assert wildcard["total_entries"] == 4
                invalid = await session.call_tool("list_dags", {"limit": -1})
                assert invalid.isError
                regex_dags = await call("list_dags", dag_id_regex="(?i)^MCP_[a-z]+$")
                assert [d["dag_id"] for d in regex_dags["dags"]] == ["mcp_validation"]
                assert (await call("list_dags", dag_id_regex="^no_match$"))["total_entries"] == 0
                regex_tasks = await call("list_tasks", dag_id="mcp_validation", task_id_regex="^(retry_me|downstream)$")
                assert {t["task_id"] for t in regex_tasks["tasks"]} == {"retry_me", "downstream"}
                regex_args = {**args, "dag_id_regex": "^mcp_", "task_id_regex": "^(retry_me|downstream)$"}
                regex_all = await call("list_task_instances", **regex_args, limit=0)
                assert regex_all["total_entries"] == 2
                regex_first = await call("list_task_instances", **regex_args, limit=1)
                regex_second = await call("list_task_instances", **regex_args, limit=1, offset=1)
                assert keys(regex_first["task_instances"]) | keys(regex_second["task_instances"]) == keys(regex_all["task_instances"])
                assert keys(regex_first["task_instances"]) != keys(regex_second["task_instances"])
                for name, bad_args in [("list_dags", {"dag_id_regex": "["}),
                                       ("list_task_instances", {**regex_args, "cursor": ""})]:
                    assert (await session.call_tool(name, bad_args)).isError

                # Match the same instant in UTC and UTC-03:00, on inclusive/exclusive boundaries.
                for field in ("start_date", "end_date"):
                    instant = target[field]
                    assert instant is not None
                    local = datetime.fromisoformat(instant).astimezone(timezone(timedelta(hours=-3))).isoformat()
                    for bound, count in (("gte", 1), ("lte", 1), ("gt", 0), ("lt", 0)):
                        found = await call("list_task_instances", **args, task_id=target["task_id"],
                                           task_id_regex="^" + target["task_id"] + "$", **{f"{field}_{bound}": local})
                        assert found["total_entries"] == count, f"Task boundary mismatch: {field}_{bound}"
                    run_instant = max(r[field] for r in runs["dag_runs"] if r[field])
                    inclusive = await call("list_dags", dag_id_regex="^mcp_validation$", **{f"dag_run_{field}_gte": run_instant})
                    exclusive = await call("list_dags", dag_id_regex="^mcp_validation$", **{f"dag_run_{field}_gt": run_instant})
                    assert inclusive["total_entries"] == 1 and exclusive["total_entries"] == 0
                print("PASS ID regex, filtered pagination, invalid regex, timestamp boundaries and timezone equivalence")
                print("AIRFLOW_MCP_LISTING_E2E_PASS")


if __name__ == "__main__":
    asyncio.run(main())
