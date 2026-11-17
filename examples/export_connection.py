"""Run only inside an administrator-authorized Airflow environment."""
import json
import re
import sys

from airflow.models.connection import Connection

if len(sys.argv) != 2 or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,199}", sys.argv[1]):
    raise SystemExit("Expected one simple connection ID")
connection = Connection.get_connection_from_secrets(sys.argv[1])
data = json.loads(connection.as_json())
data["connection_id"] = sys.argv[1]
print(json.dumps(data))
