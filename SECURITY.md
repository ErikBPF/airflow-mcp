# Security policy

This is an early release. Use a dedicated Airflow identity and review exposed tools before granting access. Read-only defaults do not replace upstream RBAC. Write tools can repeat data processing; privileged export can reveal credentials to the local filesystem.

Report vulnerabilities using GitHub's private vulnerability reporting action on this repository. If it is unavailable, open an issue requesting a private contact channel without exploit details, credentials or sensitive configuration. Never post tokens, exported connections, logs containing secrets or real production data in public issues.

Keep `*.secrets.json` and `.env*` out of Git. Connection files are plaintext and require mode `0600`. The server does not expose an HTTP MCP listener, disable TLS validation or forward client MCP tokens to upstream services. Regex matching is bounded and isolated. Lists and previews are not transactional locks against concurrent operators.
