"""Fail-fast dependency, connector-task, and processor check for the demo."""

from __future__ import annotations

import os

import requests
from _common import API_URL, CONNECT_URL, SCHEMA_REGISTRY_URL, ch_connect, pg_connect


def main() -> None:
    checks: list[tuple[str, bool, str]] = []
    try:
        with pg_connect() as conn:
            checks.append(("postgres", conn.execute("SELECT 1").fetchone()[0] == 1, ""))
    except Exception as exc:  # noqa: BLE001
        checks.append(("postgres", False, str(exc)))
    try:
        client = ch_connect()
        checks.append(("clickhouse", client.command("SELECT 1") == 1, ""))
        client.close()
    except Exception as exc:  # noqa: BLE001
        checks.append(("clickhouse", False, str(exc)))
    for name, url in (
        ("schema-registry", f"{SCHEMA_REGISTRY_URL}/subjects"),
        ("kafka-connect", f"{CONNECT_URL}/connectors"),
        ("debezium-task", f"{CONNECT_URL}/connectors/orders-cdc/status"),
        ("processor-metrics", os.getenv("PROCESSOR_METRICS_URL", "http://localhost:19108/metrics")),
        ("api", f"{API_URL}/health"),
    ):
        try:
            response = requests.get(url, timeout=5)
            ok = response.ok
            detail = f"HTTP {response.status_code}"
            if name == "debezium-task" and ok:
                body = response.json()
                states = [body.get("connector", {}).get("state")]
                states.extend(task.get("state") for task in body.get("tasks", []))
                ok = bool(body.get("tasks")) and all(state == "RUNNING" for state in states)
                detail = "/".join(str(state) for state in states)
            checks.append((name, ok, detail))
        except Exception as exc:  # noqa: BLE001
            checks.append((name, False, str(exc)))
    for name, ok, detail in checks:
        print(f"{'OK  ' if ok else 'FAIL'} {name:<16} {detail}")
    failed = [name for name, ok, _ in checks if not ok]
    if failed:
        raise SystemExit(f"preflight failed: {', '.join(failed)}")


if __name__ == "__main__":
    main()
