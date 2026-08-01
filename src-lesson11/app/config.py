"""Environment-driven connections that work on the host and in Compose."""

from __future__ import annotations

import os

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "18123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "lesson11")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "lesson11")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "lesson11")


def clickhouse_client():
    """Create a ClickHouse HTTP client; callers decide its thread lifetime."""
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )
