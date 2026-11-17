import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch
from types import SimpleNamespace

import httpx
from airflow_mcp import server


class PublicConfig(unittest.TestCase):
    def test_import_is_read_only(self):
        result = server.subprocess.run([server.sys.executable, "-c", "import asyncio,json; from airflow_mcp.server import mcp; print(json.dumps([t.name for t in asyncio.run(mcp.list_tools())]))"], capture_output=True, text=True, check=True)
        names = json.loads(result.stdout)
        self.assertIn("list_dags", names)
        self.assertNotIn("restart_run", names)
        self.assertNotIn("export_connection", names)

    def test_export_rejects_invalid_or_wrong_connection_payload(self):
        for payload in (None, [], {"connection_id": "other", "conn_type": "http"}, {"connection_id": "fixture"}):
            with self.subTest(payload=payload), patch.dict(os.environ, {"AIRFLOW_CONNECTION_EXPORT_COMMAND": '["helper"]'}), patch.object(server, "private_file"), patch.object(server, "write_secret") as write:
                with patch.object(server.subprocess, "run", return_value=SimpleNamespace(returncode=0, stdout=json.dumps(payload))):
                    with self.assertRaises(ValueError):
                        server.export_connection("fixture", "fixture.secrets.json")
                write.assert_not_called()

    def test_read_only_default_and_write_opt_in(self):
        configure = getattr(server, "configure_tools", None)
        self.assertIsNotNone(configure)
        import asyncio
        configure(allow_writes=False)
        names = {t.name for t in asyncio.run(server.mcp.list_tools())}
        self.assertIn("list_dags", names)
        self.assertNotIn("restart_run", names)
        self.assertNotIn("export_connection", names)
        configure(allow_writes=True)
        names = {t.name for t in asyncio.run(server.mcp.list_tools())}
        self.assertIn("restart_run", names)
        self.assertIn("edit_connection", names)
        self.assertNotIn("export_connection", names)

    def test_bearer_token_never_calls_password_endpoint(self):
        self.assertTrue(hasattr(server, "auth_headers"))
        with patch.dict(os.environ, {"AIRFLOW_TOKEN": "synthetic"}, clear=True):
            with httpx.Client(transport=httpx.MockTransport(lambda r: self.fail("Unexpected login"))) as client:
                self.assertEqual(server.auth_headers(client), {"Authorization": "Bearer synthetic"})

    def test_auth_file_password_exchange_and_sanitized_failure(self):
        self.assertTrue(hasattr(server, "auth_headers"))
        def respond(request):
            self.assertEqual(request.url.path, "/auth/token")
            self.assertEqual(json.loads(request.content), {"username": "user", "password": "secret"})
            return httpx.Response(201, json={"access_token": "synthetic"})
        with patch.dict(os.environ, {"AIRFLOW_AUTH_FILE": "fixture.secrets.json"}, clear=True), patch.object(server, "read_secret", return_value={"username": "user", "password": "secret"}):
            with httpx.Client(base_url="https://airflow.example", transport=httpx.MockTransport(respond)) as client:
                self.assertEqual(server.auth_headers(client), {"Authorization": "Bearer synthetic"})
            with httpx.Client(base_url="https://airflow.example", transport=httpx.MockTransport(lambda r: httpx.Response(401, text="secret"))) as client:
                with self.assertRaisesRegex(RuntimeError, "authentication failed") as error:
                    server.auth_headers(client)
                self.assertNotIn("secret", str(error.exception))

    def test_export_requires_operator_config_and_safe_id(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(server, "private_file"), patch.object(server.subprocess, "run", side_effect=AssertionError("No export command configured")):
            with self.assertRaisesRegex(ValueError, "export command"):
                server.export_connection("fixture", "fixture.secrets.json")
        with patch.dict(os.environ, {"AIRFLOW_CONNECTION_EXPORT_COMMAND": '["python", "helper.py"]'}, clear=True):
            with self.assertRaises(ValueError):
                server.export_connection("x; echo unsafe", "fixture.secrets.json")


if __name__ == "__main__":
    unittest.main()
