"""Shared wiring for Lesson 10: ClickHouse real-time OLAP over Kafka."""

import io
import os
import sys
from pathlib import Path

# Line-buffer stdout so progress is visible when scripts redirect to files.
if isinstance(sys.stdout, io.TextIOWrapper):
    sys.stdout.reconfigure(line_buffering=True)

# ── Paths ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / os.environ.get("L10_DATA_DIR", "data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ── Kafka ────────────────────────────────────────────────────────────────────
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
TOPIC_PREFIX = os.environ.get("L10_TOPIC_PREFIX", "")
EVENTS_TOPIC = f"{TOPIC_PREFIX}events"
DEMO_FLOOR_TOPIC = f"{TOPIC_PREFIX}events-floor"

# ── ClickHouse ───────────────────────────────────────────────────────────────
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "lesson10")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "default")

# ── Teaching narration helpers ───────────────────────────────────────────────
def banner(title: str, *lines: str) -> None:
    print("\n" + "═" * 78)
    print(f"  {title}")
    if lines:
        print("─" * 78)
        for ln in lines:
            print(f"  {ln}")
    print("═" * 78)


def lesson(*lines: str) -> None:
    print("\n" + "═" * 78)
    print("  ⟐  THE LESSON  ·  what this demo just showed")
    print("─" * 78)
    for ln in lines:
        print(f"  {ln}")
    print("═" * 78 + "\n")


# ── Lazy ClickHouse client ───────────────────────────────────────────────────
def get_ch_client():
    """Return a clickhouse_connect client configured from the environment."""
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


def await_topic(admin, topic: str, present: bool, timeout: float = 30.0) -> None:
    """Block until `topic` exists (present=True) or is gone (present=False).

    The AdminClient futures returned by create_topics/delete_topics are not
    reliable here: the broker performs the operation but the future can stay
    unresolved indefinitely, so `future.result()` hangs and
    `future.result(timeout=...)` raises even though the operation succeeded.
    Broker metadata is authoritative and `list_topics` is dependable, so fire
    the request and confirm the end state instead of trusting the future.
    """
    import time

    deadline = time.time() + timeout
    while time.time() < deadline:
        if (topic in admin.list_topics(timeout=5).topics) == present:
            return
        time.sleep(0.5)

    state = "appear" if present else "disappear"
    raise TimeoutError(f"topic {topic!r} did not {state} within {timeout}s")


def explain_granules(client, query: str) -> tuple[int, int]:
    """Return (granules_selected, granules_total) for `query`'s primary-key index.

    `EXPLAIN indexes = 1` prints one block per index step, each with its own
    `Granules: selected/total` line:

        MinMax        Granules: 181/181
        Partition     Granules: 181/181
        PrimaryKey    Granules: 2/181     <-- the one that reflects ORDER BY

    Searching for the first `Granules:` therefore reports the partition totals
    and hides primary-key pruning completely -- which inverts the conclusion of
    any demo built to show that pruning. Always read the PrimaryKey step.

    If the plan has no PrimaryKey step, the key could not be used at all, so
    every granule is read and selected == total.
    """
    import re

    result = client.query(f"EXPLAIN indexes = 1\n{query}")
    text = "\n".join(str(row[0]) for row in result.result_rows)

    match = re.search(r"PrimaryKey.*?Granules:\s*(\d+)/(\d+)", text, re.DOTALL)
    if match is None:
        # Fall back to the widest granule count the plan mentions: a full scan.
        totals = [int(t) for _, t in re.findall(r"Granules:\s*(\d+)/(\d+)", text)]
        return (max(totals), max(totals)) if totals else (0, 0)

    return int(match.group(1)), int(match.group(2))


def format_granules(selected: int, total: int) -> str:
    """Render a granule count with the share of the table it avoided reading."""
    if total <= 0:
        return "?"
    skipped = 100.0 * (1 - selected / total)
    return f"{selected:,}/{total:,} granules  ({skipped:.1f}% skipped)"


def wait_for_clickhouse(timeout: float = 60.0, interval: float = 1.0):
    """Poll ClickHouse until it accepts a ping."""
    import time

    import clickhouse_connect

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                username=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                database=CLICKHOUSE_DATABASE,
            )
            client.command("SELECT 1")
            return client
        except Exception as exc:  # noqa: BLE001
            print(f"  waiting for ClickHouse... ({exc})", file=sys.stderr)
            time.sleep(interval)
    raise RuntimeError(f"ClickHouse not reachable after {timeout}s")
