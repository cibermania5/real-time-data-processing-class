"""Micro-demo: the escape hatch that does exist, and what it costs.

Part two ends on "the ORDER BY is the one decision you cannot take back."
That is nearly true and worth overstating once, because the fix is not free:
a projection is a second physical copy of the data, sorted differently, that
ClickHouse maintains and picks automatically.

This runs against `events`, so run `src/seed_bulk.py` first.

Usage:
    uv run python src/demo_projection.py
"""

import time
import uuid

from config import banner, get_ch_client, lesson

USER_QUERY = "SELECT count(), sum(amount) FROM events WHERE user_id = 42"

SIZE_QUERY = """
    SELECT sum(bytes_on_disk)
    FROM system.parts
    WHERE active AND database = currentDatabase() AND table = 'events'
"""


def human(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if n < 1024:
            return f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} TiB"


def measure(client, label: str) -> tuple[int, float]:
    """Run the probe query and read marks + duration back from query_log."""
    tag = f"proj-{label}-{uuid.uuid4().hex[:8]}"
    client.command(f"{USER_QUERY} FORMAT Null SETTINGS log_comment = '{tag}'")
    client.command("SYSTEM FLUSH LOGS")

    row = client.query(
        """
        SELECT ProfileEvents['SelectedMarks'], query_duration_ms
        FROM system.query_log
        WHERE type = 'QueryFinish' AND log_comment = {tag:String}
        ORDER BY event_time DESC LIMIT 1
        """,
        parameters={"tag": tag},
    ).result_rows
    return (row[0][0], float(row[0][1])) if row else (0, 0.0)


def main() -> None:
    banner(
        "Projections: a second sort order for the same table",
        "events is sorted by (event_type, region, event_time).",
        "A user_id lookup has to read everything. Can we fix it after the fact?",
    )

    client = get_ch_client()

    if client.query("SELECT count() FROM events").result_rows[0][0] < 1_000_000:
        print("\n  `events` is too small to show anything.")
        print("  Run: uv run python src/seed_bulk.py\n")
        return

    size_before = float(client.query(SIZE_QUERY).result_rows[0][0] or 0)
    marks_before, ms_before = measure(client, "before")
    print(f"\n  before:  {marks_before:,} marks, {ms_before:,.0f} ms, table {human(size_before)}")

    try:
        print("\n  adding and materializing a projection ordered by user_id...")
        client.command("ALTER TABLE events DROP PROJECTION IF EXISTS p_by_user")
        client.command("""
            ALTER TABLE events ADD PROJECTION p_by_user (
                SELECT * ORDER BY user_id
            )
        """)
        start = time.perf_counter()
        client.command("ALTER TABLE events MATERIALIZE PROJECTION p_by_user",
                       settings={"mutations_sync": 2})
        print(f"  materialized in {time.perf_counter() - start:,.1f}s")

        size_after = float(client.query(SIZE_QUERY).result_rows[0][0] or 0)
        marks_after, ms_after = measure(client, "after")
        print(f"\n  after:   {marks_after:,} marks, {ms_after:,.0f} ms, table {human(size_after)}")

        print("\n  " + "─" * 56)
        if marks_after:
            print(f"  marks read:   {marks_before:,}  ->  {marks_after:,}"
                  f"   ({marks_before / max(marks_after, 1):,.0f}x less)")
        print(f"  query time:   {ms_before:,.0f} ms  ->  {ms_after:,.0f} ms")
        print(f"  storage:      {human(size_before)}  ->  {human(size_after)}"
              f"   (+{100 * (size_after / max(size_before, 1) - 1):,.0f}%)")
        print("  " + "─" * 56)
    finally:
        print("\n  dropping the projection...")
        client.command("ALTER TABLE events DROP PROJECTION IF EXISTS p_by_user")

    lesson(
        "So the ORDER BY is not quite a one-way door. A projection gives the",
        "same table a second sort order, and the query planner picks it",
        "without the query changing at all.",
        "",
        "What it costs: a full second copy of the data. You pay in disk, in",
        "write throughput on every insert, and in merge work forever after.",
        "",
        "What it cannot do: rescue a base ORDER BY that is wrong for most of",
        "your queries. One projection to cover a secondary access pattern is",
        "an optimisation. Four projections means you chose the wrong key.",
    )


if __name__ == "__main__":
    main()
