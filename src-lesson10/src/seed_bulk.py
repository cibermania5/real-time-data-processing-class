"""Bulk-seed the `events` table with history, generated inside ClickHouse.

Why this exists
---------------
The Kafka producer is the *live* path: it demonstrates ingestion latency and
query-under-load, and it runs at a rate a laptop can sustain (a few hundred
events/s). That is the right tool for measuring freshness.

It is the wrong tool for building a table. The lesson's storage demos --
granule skipping, per-column compression, projections -- only say anything at
scale. With the default `index_granularity = 8192`, a table of ~9 000 rows is
two granules, so "the ORDER BY skipped 99.9% of the data" has nothing to skip.
Streaming 20M events through Kafka at 400/s would take 14 hours.

So we generate history server-side with `numbers_mt`. ClickHouse writes 20M
rows in a few seconds because nothing crosses the network. The distributions
mirror `producer.py` exactly, so the seeded history and the live stream look
like the same process -- which matters, because both land in the same table.

Usage:
    uv run python src/seed_bulk.py                      # 20M rows over 60 min
    uv run python src/seed_bulk.py --rows 5000000       # smaller/faster
    uv run python src/seed_bulk.py --reset              # truncate first
"""

import argparse
import time

from config import banner, get_ch_client, lesson

# Must match producer.py so seeded history and live stream are the same process.
REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "ap-south"]
EVENT_TYPES = ["purchase", "refund", "page_view", "add_to_cart"]


def sql_array(values: list[str]) -> str:
    inner = ",".join(f"'{v}'" for v in values)
    return f"[{inner}]"


def build_insert(rows: int, users: int, minutes: int) -> str:
    """Generate `rows` events spread over the last `minutes` minutes.

    Each expression uses a distinct rand() seed so the columns are independent;
    reusing rand() with the same argument would correlate them and quietly
    flatter the compression numbers.
    """
    seconds = minutes * 60
    return f"""
        INSERT INTO events
        SELECT
            now() - INTERVAL toUInt32(rand(1) % {seconds}) SECOND
                  - INTERVAL toUInt32(rand(2) % 1000) MILLISECOND   AS event_time,
            toUInt32(rand(3) % {users})                             AS user_id,
            {sql_array(EVENT_TYPES)}[toUInt8(rand(4) % 4) + 1]      AS event_type,
            -- 30% of events carry no money, matching producer.py
            if(rand(5) % 10 < 3, toDecimal64(0, 2),
               toDecimal64(rand(6) % 49900 / 100 + 1, 2))           AS amount,
            {sql_array(REGIONS)}[toUInt8(rand(7) % 5) + 1]          AS region
        FROM numbers_mt({rows})
    """


def report(client) -> None:
    stats = client.query("""
        SELECT sum(rows), sum(marks), formatReadableSize(sum(bytes_on_disk)),
               formatReadableSize(sum(data_uncompressed_bytes)),
               round(sum(data_uncompressed_bytes) / greatest(sum(data_compressed_bytes), 1), 2)
        FROM system.parts
        WHERE active AND database = currentDatabase() AND table = 'events'
    """).result_rows[0]

    rows, marks, on_disk, uncompressed, ratio = stats
    print(f"\n  rows in events:   {rows:,}")
    print(f"  granules (marks): {marks:,}")
    print(f"  on disk:          {on_disk}")
    print(f"  uncompressed:     {uncompressed}")
    print(f"  compression:      {ratio}x")


def main() -> None:
    parser = argparse.ArgumentParser(description="bulk-seed the events table for L10 storage demos")
    parser.add_argument("--rows", type=int, default=20_000_000, help="rows to generate")
    parser.add_argument("--users", type=int, default=100_000, help="distinct user_id values")
    # Wide enough that the seeded history still covers a `WHERE event_time >=
    # now() - INTERVAL 10 MINUTE` query three hours into the lesson. At 60
    # minutes the data ages out mid-class and every dashboard query quietly
    # starts scanning an almost-empty range.
    parser.add_argument("--minutes", type=int, default=240, help="spread events over the last N minutes")
    parser.add_argument("--reset", action="store_true", help="truncate events (and the revenue MV) first")
    args = parser.parse_args()

    banner(
        "Lesson 10 bulk seed",
        f"rows:    {args.rows:,}",
        f"users:   {args.users:,}  (~{args.rows // max(args.users, 1)} events per user)",
        f"window:  last {args.minutes} minutes",
        "generated inside ClickHouse -- no Kafka, no network round-trips",
    )

    client = get_ch_client()

    if args.reset:
        print("truncating events and revenue_by_region_minute...")
        client.command("TRUNCATE TABLE IF EXISTS events")
        client.command("TRUNCATE TABLE IF EXISTS revenue_by_region_minute")

    print(f"generating {args.rows:,} rows...")
    start = time.perf_counter()
    client.command(build_insert(args.rows, args.users, args.minutes))
    elapsed = time.perf_counter() - start
    print(f"  done in {elapsed:.1f}s ({args.rows / elapsed:,.0f} rows/s)")

    # Merge down to a realistic part count. Without this the table is a pile of
    # small parts and the granule numbers in the ORDER BY demo move around.
    print("merging parts (OPTIMIZE FINAL)...")
    merge_start = time.perf_counter()
    client.command("OPTIMIZE TABLE events FINAL")
    print(f"  done in {time.perf_counter() - merge_start:.1f}s")

    report(client)

    lesson(
        "The live Kafka producer and this seeder write to the same table.",
        "Use the producer to measure freshness -- it is the real ingestion path.",
        "Use this seeder to give the storage demos something to actually skip:",
        "granule pruning, per-column compression and projections are all",
        "invisible until the table is large enough to have parts worth pruning.",
    )


if __name__ == "__main__":
    main()
