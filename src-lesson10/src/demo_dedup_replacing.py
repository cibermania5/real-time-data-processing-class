"""Micro-demo: what a CDC stream does to an append-only table.

Three acts:

  1. A retried insert silently doubles your revenue. MergeTree has no primary
     key constraint -- nothing stops the same block landing twice.
  2. `insert_deduplication_token` stops the exact-replay case, and only that.
  3. ReplacingMergeTree handles the general case: the updates and deletes a
     Lesson 5 CDC stream actually carries, expressed as inserts.

Usage:
    uv run python src/demo_dedup_replacing.py
"""

from config import banner, get_ch_client, lesson

ORDERS = [
    (1, "alice", 120.00),
    (2, "bob", 80.00),
    (3, "carol", 200.00),
]


def values_clause(version: int = 1, is_deleted: int = 0, versioned: bool = False) -> str:
    rows = []
    for oid, customer, amount in ORDERS:
        if versioned:
            rows.append(f"({oid}, '{customer}', {amount}, 'placed', {version}, {is_deleted})")
        else:
            rows.append(f"({oid}, '{customer}', {amount})")
    return ", ".join(rows)


def totals(client, table: str, final: bool = False) -> tuple[int, float]:
    suffix = " FINAL" if final else ""
    where = " WHERE is_deleted = 0" if final and "replacing" in table else ""
    row = client.query(
        f"SELECT count(), sum(amount) FROM {table}{suffix}{where}"
    ).result_rows[0]
    return row[0], float(row[1] or 0)


def act_one(client) -> None:
    print("\n─── ACT 1 · the retry ─────────────────────────────────────")
    client.command("DROP TABLE IF EXISTS orders_plain")
    client.command("""
        CREATE TABLE orders_plain (
            order_id UInt32,
            customer String,
            amount Decimal(10, 2)
        ) ENGINE = MergeTree() ORDER BY order_id
    """)
    client.command("DROP TABLE IF EXISTS orders_dedup")
    client.command("""
        CREATE TABLE orders_dedup (
            order_id UInt32,
            customer String,
            amount Decimal(10, 2)
        ) ENGINE = ReplacingMergeTree() ORDER BY order_id
    """)

    for table in ("orders_plain", "orders_dedup"):
        client.command(f"INSERT INTO {table} VALUES {values_clause()}")
    rows, total = totals(client, "orders_plain")
    print(f"  after the first insert:   {rows} rows, revenue {total:,.2f}")

    # The consumer crashed after writing but before committing its offset, so
    # on restart it replays the same block. Nothing rejects it.
    for table in ("orders_plain", "orders_dedup"):
        client.command(f"INSERT INTO {table} VALUES {values_clause()}")

    rows, total = totals(client, "orders_plain")
    print(f"  MergeTree, block twice:   {rows} rows, revenue {total:,.2f}   <-- wrong, and silent")

    raw, raw_total = totals(client, "orders_dedup")
    print(f"  ReplacingMergeTree, raw:  {raw} rows, revenue {raw_total:,.2f}   <-- not fixed yet either")

    fin, fin_total = totals(client, "orders_dedup", final=True)
    print(f"  ReplacingMergeTree FINAL: {fin} rows, revenue {fin_total:,.2f}   <-- correct")


def act_two(client) -> None:
    print("\n─── ACT 2 · insert_deduplication_token ────────────────────")
    client.command("DROP TABLE IF EXISTS orders_token")
    client.command("""
        CREATE TABLE orders_token (
            order_id UInt32,
            customer String,
            amount Decimal(10, 2)
        ) ENGINE = MergeTree() ORDER BY order_id
        SETTINGS non_replicated_deduplication_window = 100
    """)

    for attempt in (1, 2):
        client.command(
            f"INSERT INTO orders_token VALUES {values_clause()}",
            settings={"insert_deduplication_token": "kafka-events-partition0-offset1000"},
        )
        rows, total = totals(client, "orders_token")
        print(f"  insert #{attempt} with the same token: {rows} rows, revenue {total:,.2f}")

    print("  the second insert was dropped: same token, same block.")


def act_three(client) -> None:
    print("\n─── ACT 3 · the CDC stream: updates and deletes ───────────")
    client.command("DROP TABLE IF EXISTS orders_replacing")
    client.command("""
        CREATE TABLE orders_replacing (
            order_id UInt32,
            customer String,
            amount Decimal(10, 2),
            status String,
            version UInt64,
            is_deleted UInt8
        ) ENGINE = ReplacingMergeTree(version, is_deleted)
        ORDER BY order_id
    """)

    client.command(f"INSERT INTO orders_replacing VALUES {values_clause(1, 0, True)}")
    print("  3 orders inserted (version 1)")

    # Lesson 5's CDC stream carries an UPDATE. There is no UPDATE here: the
    # new state arrives as another INSERT carrying a higher version.
    client.command(
        "INSERT INTO orders_replacing VALUES (2, 'bob', 95.00, 'amended', 2, 0)"
    )
    # ...and a DELETE, which is an insert with the tombstone flag set.
    client.command(
        "INSERT INTO orders_replacing VALUES (3, 'carol', 200.00, 'cancelled', 2, 1)"
    )

    raw, raw_total = totals(client, "orders_replacing")
    print(f"  raw rows on disk:            {raw} rows, revenue {raw_total:,.2f}")

    fin, fin_total = totals(client, "orders_replacing", final=True)
    print(f"  SELECT ... FINAL:            {fin} rows, revenue {fin_total:,.2f}   <-- correct")

    client.command("OPTIMIZE TABLE orders_replacing FINAL")
    merged, _ = totals(client, "orders_replacing")
    print(f"  after OPTIMIZE ... FINAL:    {merged} rows (tombstone still present until CLEANUP)")


def main() -> None:
    banner(
        "Duplicates, updates and deletes in an append-only store",
        "Kafka delivery is at-least-once and MergeTree never rejects a row.",
        "Predict each number before it prints.",
    )

    client = get_ch_client()
    act_one(client)
    act_two(client)
    act_three(client)

    lesson(
        "MergeTree has no primary key constraint. A replayed block is simply",
        "more rows, and the only symptom is that your revenue is too high.",
        "",
        "insert_deduplication_token fixes exactly one case: the identical",
        "block arriving twice. It does nothing for a genuine update.",
        "",
        "ReplacingMergeTree is the general answer, and it changes the deal:",
        "dedup happens at merge time, and merges run when they run. Until",
        "then the duplicates are visible, so reads need FINAL -- which is",
        "why the next slide measures what FINAL costs.",
        "",
        "This is the Lesson 5 CDC stream landing in an OLAP store: every",
        "INSERT, UPDATE and DELETE becomes an insert, and correctness moves",
        "from the write path to the read path.",
    )

    for table in ("orders_plain", "orders_dedup", "orders_token", "orders_replacing"):
        client.command(f"DROP TABLE IF EXISTS {table}")


if __name__ == "__main__":
    main()
