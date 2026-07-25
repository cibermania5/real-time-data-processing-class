"""Demonstrate that the ClickHouse ORDER BY key is the primary index.

Create a second table with a different sort order, insert the same recent data,
and compare granules scanned for a user-filter query vs. a time-filter query.

Usage:
    uv run python src/experiment_order_by.py
"""

import time

from config import banner, explain_granules, format_granules, get_ch_client


def timed(client, query: str) -> tuple[float, tuple[int, int]]:
    start = time.perf_counter()
    client.query(query)
    elapsed_ms = (time.perf_counter() - start) * 1000
    return elapsed_ms, explain_granules(client, query)


def main():
    banner(
        "ORDER BY key experiment",
        "Two tables with the same data but different physical sort orders.",
        "Watch how the matching ORDER BY skips dramatically more granules.",
    )

    client = get_ch_client()

    print("creating events_by_user table...")
    client.command("DROP TABLE IF EXISTS events_by_user")
    client.command("""
        CREATE TABLE events_by_user (
            event_time DateTime64(3),
            user_id UInt32,
            event_type LowCardinality(String),
            amount Decimal(10, 2),
            region LowCardinality(String)
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD(event_time)
        ORDER BY (user_id, event_time)
        SETTINGS index_granularity = 8192
    """)

    print("loading last 5 minutes of events into events_by_user...")
    client.command("""
        INSERT INTO events_by_user
        SELECT * FROM events
        WHERE event_time >= now() - INTERVAL 5 MINUTE
    """)

    time_query = """
        SELECT count(), sum(amount)
        FROM events
        WHERE event_time >= now() - INTERVAL 5 MINUTE
    """
    user_query_events = """
        SELECT count(), sum(amount)
        FROM events
        WHERE user_id = 42
    """
    user_query_by_user = """
        SELECT count(), sum(amount)
        FROM events_by_user
        WHERE user_id = 42
    """

    print("\n" + "─" * 58)
    print("QUERY 1: time range (favors events table ORDER BY event_type/region/event_time)")
    ms, granules = timed(client, time_query)
    print(f"  latency: {ms:.2f} ms  |  {format_granules(*granules)}")

    print("\nQUERY 2: user filter on events table (bad match)")
    ms, bad = timed(client, user_query_events)
    print(f"  latency: {ms:.2f} ms  |  {format_granules(*bad)}")

    print("\nQUERY 3: user filter on events_by_user table (good match)")
    ms, good = timed(client, user_query_by_user)
    print(f"  latency: {ms:.2f} ms  |  {format_granules(*good)}")
    print("─" * 58)

    if good[0] > 0:
        print(f"\n  Same filter, same data: {bad[0]:,} granules read vs {good[0]:,}.")
        print(f"  The matching ORDER BY read {bad[0] / good[0]:,.0f}x less of the table.")

    print("\ncleaning up events_by_user...")
    client.command("DROP TABLE IF EXISTS events_by_user")


if __name__ == "__main__":
    main()
