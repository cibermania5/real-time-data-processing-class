"""Shared host-friendly connections for Lesson 11 command-line exercises."""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from typing import TypeVar

import clickhouse_connect
import psycopg

PG_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://{user}:{password}@{host}:{port}/{database}".format(
        user=os.getenv("POSTGRES_USER", "lesson11"),
        password=os.getenv("POSTGRES_PASSWORD", "lesson11"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "15432"),
        database=os.getenv("POSTGRES_DB", "orders"),
    ),
)
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "18123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "lesson11")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "lesson11")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "lesson11")
CONNECT_URL = os.getenv("CONNECT_URL", "http://localhost:18083").rstrip("/")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:18081").rstrip("/")
API_URL = os.getenv("API_URL", "http://localhost:18000").rstrip("/")


def pg_connect(*, autocommit: bool = False):
    return psycopg.connect(PG_DSN, autocommit=autocommit)


def ch_connect():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


T = TypeVar("T")


def eventually(description: str, check: Callable[[], T | None], timeout: float = 90) -> T:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            value = check()
            if value is not None:
                return value
        except Exception as exc:  # noqa: BLE001
            last_error = exc
        time.sleep(1)
    detail = f"; last error: {last_error}" if last_error else ""
    raise TimeoutError(f"timed out waiting for {description}{detail}")
