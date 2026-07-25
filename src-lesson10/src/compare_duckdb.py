"""Export recent ClickHouse events to Parquet and compare query latency vs DuckDB.

Run while the producer is active (or shortly after):
    uv run python src/compare_duckdb.py

"""

import time

import duckdb
import pandas as pd

from config import DATA_DIR, banner, get_ch_client


def main():
    banner(
        "ClickHouse vs DuckDB comparison",
        "Exporting the last 10 minutes of events to Parquet, then running the",
        "same aggregation in DuckDB and in ClickHouse raw table.",
    )

    ch = get_ch_client()
    minutes = 10

    print("exporting recent events from ClickHouse...")
    start = time.perf_counter()
    result = ch.query(
        """
        SELECT event_time, user_id, event_type, amount, region
        FROM events
        WHERE event_time >= now() - INTERVAL {minutes:UInt16} MINUTE
        """,
        parameters={"minutes": minutes},
    )
    rows = result.result_rows
    export_ms = (time.perf_counter() - start) * 1000
    print(f"  exported {len(rows):,} rows in {export_ms:.1f} ms")

    df = pd.DataFrame(
        rows,
        columns=["event_time", "user_id", "event_type", "amount", "region"],
    )
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
    parquet_path = DATA_DIR / "events_last_10min.parquet"
    df.to_parquet(parquet_path, index=False)
    print(f"  wrote {parquet_path}")

    print("\nrunning the same revenue query in DuckDB...")
    duck = duckdb.connect()
    duck.execute("CREATE OR REPLACE TABLE events AS SELECT * FROM read_parquet(?)", [str(parquet_path)])

    start = time.perf_counter()
    duck.sql("""
        SELECT
            date_trunc('minute', event_time) AS minute,
            region,
            sum(amount) AS revenue,
            count(*) AS purchases
        FROM events
        WHERE event_type = 'purchase'
        GROUP BY minute, region
        ORDER BY minute DESC, region
    """)
    duckdb_ms = (time.perf_counter() - start) * 1000
    print(f"  DuckDB query: {duckdb_ms:.1f} ms")

    print("\nrunning the equivalent query in ClickHouse raw table...")
    start = time.perf_counter()
    ch.query(
        """
        SELECT
            toStartOfMinute(event_time) AS minute,
            region,
            sum(amount) AS revenue,
            count() AS purchases
        FROM events
        WHERE event_type = 'purchase'
          AND event_time >= now() - INTERVAL {minutes:UInt16} MINUTE
        GROUP BY minute, region
        ORDER BY minute DESC, region
        """,
        parameters={"minutes": minutes},
    )
    ch_ms = (time.perf_counter() - start) * 1000
    print(f"  ClickHouse query: {ch_ms:.1f} ms")

    print("\n" + "─" * 58)
    print("  The export step itself is the batch workflow's ingestion latency.")
    print("  ClickHouse answers continuously; DuckDB answers the snapshot.")
    print("─" * 58)


if __name__ == "__main__":
    main()
