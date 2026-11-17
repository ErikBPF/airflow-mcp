# Contributing

Use Python 3.11 or newer and `uv sync --locked`. Run `uv run python -m unittest discover -s tests -p 'test_*.py' -v` and `uv build` before a pull request. CI runs the same checks.

Keep tool descriptions, authentication docs and examples aligned with observed behavior. Preserve existing-run identity: a restart must never silently become a new run. Write a failing regression check before a behavior fix. Test both positive and negative results for filters and connection tests. Native filter changes should be compared with the deployed Airflow OpenAPI schema.

Use synthetic data for live validation. Mutating E2E checks require a named disposable instance and explicit authorization; never run them against production by default. See [testing](docs/testing.md). Do not commit credentials, state files, secret-bearing traces or personal host configuration.

Contributions are licensed under Apache License 2.0. This project is independent of Apache Airflow and the Apache Software Foundation.
