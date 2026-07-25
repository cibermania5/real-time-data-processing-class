"""Micro-demo: show how the ORDER BY key affects granule skipping.

Creates an alternative table sorted by (user_id, event_time) and prints the
EXPLAIN output for a user-filter query on both tables.

Usage:
    uv run python src/demo_order_by_granules.py
"""

from config import banner, explain_granules, format_granules, get_ch_client


def main():
    banner(
        "ORDER BY = primary index",
        "Comparing granules scanned for the same user filter on two tables",
        "with different sort orders.",
    )

    client = get_ch_client()

    client.command("DROP TABLE IF EXISTS events_by_user_demo")
    client.command("""
        CREATE TABLE events_by_user_demo (
            event_time DateTime64(3),
            user_id UInt32,
            event_type LowCardinality(String),
            amount Decimal(10, 2),
            region LowCardinality(String)
        ) ENGINE = MergeTree()
        ORDER BY (user_id, event_time)
    """)

    print("loading last 2 minutes of events into events_by_user_demo...")
    client.command("""
        INSERT INTO events_by_user_demo
        SELECT * FROM events
        WHERE event_time >= now() - INTERVAL 2 MINUTE
    """)

    query_events = "SELECT count(), sum(amount) FROM events WHERE user_id = 42"
    query_user = "SELECT count(), sum(amount) FROM events_by_user_demo WHERE user_id = 42"

    bad = explain_granules(client, query_events)
    good = explain_granules(client, query_user)

    print("\n" + "─" * 58)
    print("events (ORDER BY event_type, region, event_time)")
    print(f"  {format_granules(*bad)}")

    print("\nevents_by_user_demo (ORDER BY user_id, event_time)")
    print(f"  {format_granules(*good)}")
    print("─" * 58)

    client.command("DROP TABLE IF EXISTS events_by_user_demo")


if __name__ == "__main__":
    main()
