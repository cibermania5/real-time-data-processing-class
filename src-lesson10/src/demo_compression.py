"""Micro-demo: what actually compresses in a column store, and why.

Students reliably predict that the timestamp compresses best (it is "sorted",
after all) and that the integer user_id compresses well because integers are
small. Both guesses are usually wrong, and the reasons are the whole lesson:
compression follows the *shape* of the data in the order it is stored, not the
declared type.

Run `src/seed_bulk.py` first -- this reads whatever is already in `events`.

Usage:
    uv run python src/demo_compression.py
"""

from config import banner, get_ch_client, lesson

PER_COLUMN = """
    SELECT
        column,
        sum(column_data_uncompressed_bytes) AS raw,
        sum(column_data_compressed_bytes)   AS packed,
        round(sum(column_data_uncompressed_bytes)
              / greatest(sum(column_data_compressed_bytes), 1), 1) AS ratio
    FROM system.parts_columns
    WHERE active AND database = currentDatabase() AND table = 'events'
    GROUP BY column
    ORDER BY ratio DESC
"""

CARDINALITY = """
    SELECT
        uniqExact(event_type) AS event_types,
        uniqExact(region)     AS regions,
        uniqExact(user_id)    AS users,
        count()               AS rows
    FROM events
"""


def human(n: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if n < 1024:
            return f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} TiB"


def main() -> None:
    banner(
        "What compresses, and what does not",
        "Same table, same 20M rows, five columns with very different shapes.",
        "Predict the ratio for each column before looking.",
    )

    client = get_ch_client()

    rows = client.query(PER_COLUMN).result_rows
    if not rows:
        print("\n  `events` is empty. Run: uv run python src/seed_bulk.py\n")
        return

    print(f"\n  {'column':<14}{'raw':>12}{'compressed':>14}{'ratio':>9}")
    print("  " + "─" * 49)
    for column, raw, packed, ratio in rows:
        print(f"  {column:<14}{human(raw):>12}{human(packed):>14}{ratio:>8}x")

    types, regions, users, total = client.query(CARDINALITY).result_rows[0]
    print("\n  distinct values:")
    print(f"    event_type {types:>10,}   region {regions:>10,}")
    print(f"    user_id    {users:>10,}   rows   {total:>10,}")

    lesson(
        "LowCardinality columns win by a mile: a handful of distinct values",
        "become a dictionary plus tiny indexes, so 20M rows cost ~90 KiB.",
        "",
        "user_id barely compresses at all. It is a dense random 32-bit range,",
        "so there is no repetition to exploit -- being an integer does not help.",
        "",
        "event_time does worse than students expect. The table is sorted by",
        "(event_type, region, event_time), so timestamps are only sorted",
        "*within* each group, not globally: delta encoding gets far less",
        "than it would if event_time led the ORDER BY.",
        "",
        "Compression follows the shape of the data in storage order.",
        "The ORDER BY key you pick decides it, the same key that decides",
        "which granules a query can skip.",
    )


if __name__ == "__main__":
    main()
