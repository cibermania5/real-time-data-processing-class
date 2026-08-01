"""Create/update the Debezium connector and wait until its task is RUNNING.

This is the host-side twin of `connect/register.sh`, which Compose already runs
as `connector-init`. Use it to re-register by hand after deleting the connector
or editing its config. It reads the *same* JSON file the container does, so
there is exactly one source of truth for connector settings.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import requests
from _common import CONNECT_URL, eventually

NAME = os.getenv("CONNECTOR_NAME", "orders-cdc")
CONFIG_PATH = Path(__file__).resolve().parents[1] / "connect" / "orders-connector.json"


def connector_config() -> dict[str, str]:
    return json.loads(CONFIG_PATH.read_text())


def main() -> None:
    eventually(
        "Kafka Connect",
        lambda: True if requests.get(f"{CONNECT_URL}/", timeout=3).ok else None,
        timeout=120,
    )
    response = requests.put(
        f"{CONNECT_URL}/connectors/{NAME}/config", json=connector_config(), timeout=20
    )
    response.raise_for_status()
    print(f"registered connector {NAME!r}")

    def running():
        status = requests.get(f"{CONNECT_URL}/connectors/{NAME}/status", timeout=5)
        if not status.ok:
            return None
        body = status.json()
        failed = [task for task in body.get("tasks", []) if task["state"] == "FAILED"]
        if failed:
            raise RuntimeError(failed[0].get("trace", failed[0]))
        if body.get("connector", {}).get("state") == "RUNNING" and body.get("tasks") and all(
            task["state"] == "RUNNING" for task in body["tasks"]
        ):
            return body
        return None

    status = eventually("Debezium task RUNNING", running, timeout=120)
    print(f"connector={status['connector']['state']} task={status['tasks'][0]['state']}")


if __name__ == "__main__":
    main()
