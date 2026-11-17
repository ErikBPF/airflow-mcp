import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


class Contract(unittest.TestCase):
    def test_connection_test_uses_type_without_masked_password(self):
        from airflow_mcp import server
        calls = []
        def fake_api(method, path, body=None, params=None):
            calls.append((method, path, body))
            return {"conn_type": "http", "password": "***", "status": True}
        with patch.object(server, "api", fake_api):
            self.assertTrue(server.test_connection("fixture")["status"])
        self.assertEqual(calls[-1][2], {"connection_id": "fixture", "conn_type": "http"})

    def test_mcp_implementation_exists(self):
        self.assertIsNotNone(importlib.util.find_spec("airflow_mcp.server"), "MCP implementation missing")

    def test_restart_stays_in_existing_run(self):
        if importlib.util.find_spec("airflow_mcp.server") is None:
            self.skipTest("implementation missing")
        from airflow_mcp.server import restart_body
        body = restart_body("existing", ["retry_me"])
        self.assertEqual(body.get("dag_run_id"), "existing")
        self.assertTrue(body["include_downstream"])
        self.assertFalse(body["only_failed"])
        self.assertFalse(body["run_on_latest_version"])
        self.assertTrue(body["prevent_running_task"])
        self.assertNotIn("task_ids", restart_body("existing", None))
        with self.assertRaises(ValueError):
            restart_body("existing", [])

    def test_secret_export_requires_ignored_private_new_file(self):
        if importlib.util.find_spec("airflow_mcp.server") is None:
            self.skipTest("implementation missing")
        from airflow_mcp.server import write_secret
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            subprocess.run(["git", "init", "-q", tmp], check=True)
            (root / ".gitignore").write_text("*.secrets.json\n")
            dest = root / "connection.secrets.json"
            result = write_secret(dest, {"password": "synthetic"})
            self.assertNotIn("synthetic", json.dumps(result))
            self.assertTrue(dest.exists(), "Secret export did not create its file")
            self.assertEqual(json.loads(dest.read_text())["password"], "synthetic")
            self.assertEqual(dest.stat().st_mode & 0o777, 0o600)
            with self.assertRaises((ValueError, FileExistsError)):
                write_secret(dest, {})
            with self.assertRaises(ValueError):
                write_secret(root / "connection.json", {})
            link = root / "link.secrets.json"
            link.symlink_to(dest)
            with self.assertRaises((ValueError, FileExistsError)):
                write_secret(link, {})


if __name__ == "__main__":
    unittest.main()
