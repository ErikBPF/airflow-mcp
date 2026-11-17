import inspect
import json
from pathlib import Path
import unittest
from unittest.mock import patch

import httpx
from airflow_mcp import server

CONTRACT = json.loads(Path(__file__).with_name("list_api_contract.json").read_text())


class Listing(unittest.TestCase):
    def test_all_api_query_options_are_exposed(self):
        for name, operation in CONTRACT.items():
            with self.subTest(tool=name):
                fn = getattr(server, name, None)
                self.assertIsNotNone(fn, f"Missing {name}")
                expected = {p["name"] for p in operation["parameters"] if p["in"] == "query"}
                actual = set(inspect.signature(fn).parameters) - {"dag_id", "dag_run_id", "dag_id_regex", "task_id_regex"}
                self.assertEqual(actual, expected)

    def test_filters_preserve_false_zero_and_repeated_values(self):
        self.assertIn("paused", inspect.signature(server.list_dags).parameters)
        with patch.object(server, "api", return_value={"dags": [], "total_entries": 0}) as api:
            server.list_dags(limit=0, paused=False, exclude_stale=False,
                             tags=["one", "two"], order_by=["-dag_id", "dag_display_name"])
        params = api.call_args.kwargs["params"]
        self.assertEqual(params["limit"], 0)
        self.assertIs(params["paused"], False)
        self.assertNotIn("owners", params)
        encoded = httpx.QueryParams(params)
        self.assertEqual(encoded.get_list("tags"), ["one", "two"])
        self.assertEqual(encoded.get_list("order_by"), ["-dag_id", "dag_display_name"])

    def test_instances_keep_cursor_and_identifiers_without_secrets(self):
        fn = getattr(server, "list_task_instances", None)
        self.assertIsNotNone(fn, "Missing instance listing")
        raw = {"task_instances": [{"dag_id": "dag", "dag_run_id": "run", "task_id": "t",
                                  "rendered_fields": {"password": "must-not-return"}}],
               "total_entries": 2, "next_cursor": "next-page"}
        with patch.object(server, "api", return_value=raw) as api:
            result = fn("~", "~", cursor="", state=["failed", "success"], map_index=[-1, 0])
        self.assertEqual(api.call_args.args[1], "/dags/~/dagRuns/~/taskInstances")
        self.assertEqual(api.call_args.kwargs["params"]["cursor"], "")
        self.assertEqual(result["next_cursor"], "next-page")
        self.assertEqual(result["task_instances"][0]["dag_run_id"], "run")
        self.assertNotIn("must-not-return", json.dumps(result))
        with self.assertRaises(ValueError):
            server.run_path("~", "run")

    def test_regex_filters_before_pagination_and_combines_ids(self):
        self.assertIn("task_id_regex", inspect.signature(server.list_task_instances).parameters)
        rows = [{"dag_id": "etl_one", "task_id": "skip"},
                {"dag_id": "etl_one", "task_id": "load_1"},
                {"dag_id": "etl_two", "task_id": "load_2"},
                {"dag_id": "other", "task_id": "load_3"}]
        def pages(method, path, params=None):
            self.assertNotIn("task_id_regex", params)
            start = params["offset"]
            return {"task_instances": rows[start:start + 2], "total_entries": 4}
        with patch.object(server, "api", pages):
            result = server.list_task_instances("~", "~", dag_id_regex="^etl_", task_id_regex=r"^load_\d$", limit=1, offset=1)
        self.assertEqual(result["total_entries"], 2)
        self.assertEqual(result["task_instances"][0]["task_id"], "load_2")
        self.assertIsNone(result["next_cursor"])

    def test_regex_validation_limits_and_task_definitions(self):
        self.assertIn("dag_id_regex", inspect.signature(server.list_dags).parameters)
        with patch.object(server, "api") as api:
            with self.assertRaises(ValueError):
                server.list_dags(dag_id_regex="[")
            api.assert_not_called()
            with self.assertRaises(ValueError):
                server.list_task_instances("~", "~", task_id_regex="x", cursor="")
        with patch.object(server, "api", return_value={"dags": [], "total_entries": 1001}):
            with self.assertRaises(ValueError):
                server.list_dags(dag_id_regex="x")
        with patch.object(server, "api", return_value={"tasks": [{"task_id": "LOAD_a"}, {"task_id": "skip"}], "total_entries": 2}):
            result = server.list_tasks("dag", task_id_regex="(?i)^load_[a-z]$")
        self.assertEqual([t["task_id"] for t in result["tasks"]], ["LOAD_a"])

    def test_regex_timeout_is_reported(self):
        helper = getattr(server, "regex_matches", None)
        self.assertIsNotNone(helper)
        with patch.object(server.subprocess, "run", side_effect=server.subprocess.TimeoutExpired("regex", 1)):
            with self.assertRaisesRegex(ValueError, "timed out"):
                helper([{"task_id": "a"}], {"task_id": "a"})
        with self.assertRaisesRegex(ValueError, "timed out"):
            helper([{"task_id": "a" * 200 + "!"}], {"task_id": "(a+)+$"})


if __name__ == "__main__":
    unittest.main()
