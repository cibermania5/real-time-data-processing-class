"""Prove one stable canary across insert, update, nullable update, API and delete."""

from __future__ import annotations

import argparse
from decimal import Decimal
from typing import Any

import requests
from _common import API_URL, ch_connect, eventually, pg_connect


def api_order(order_id: int) -> dict[str, Any] | None:
    response = requests.get(f"{API_URL}/api/orders/{order_id}", timeout=5)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


def matches_api(order_id: int, **expected: Any):
    row = api_order(order_id)
    if row is None:
        return None
    return row if all(row.get(key) == value for key, value in expected.items()) else None


def delete_won(order_id: int):
    client = ch_connect()
    try:
        rows = client.query(
            """
            SELECT _is_deleted, version, source_topic, kafka_partition, kafka_offset
            FROM orders_current FINAL WHERE order_id={id:UInt64}
            """,
            parameters={"id": order_id},
        ).result_rows
    finally:
        client.close()
    return rows[0] if rows and rows[0][0] == 1 else None


def has_discount_column(conn) -> bool:
    return bool(
        conn.execute(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='orders'
              AND column_name='discount_code'
            """
        ).fetchone()
    )


def narrate(verbose: bool, message: str) -> None:
    """Print what we're about to do / wait for, before we block on it."""
    if verbose:
        print(f"  … {message}")


def hop_detail(verbose: bool, label: str, **fields: Any) -> None:
    """Print one indented 'this is where it actually lives' line, verbose-only."""
    if not verbose:
        return
    rendered = "  ".join(f"{key}={value}" for key, value in fields.items())
    print(f"      ↳ {label}: {rendered}")


# ─── individual, independently runnable steps ──────────────────────────────
# Each step does its own Postgres write (if any) plus its own wait/assert, and
# prints the same PASS line whether it runs alone (--insert, --update, ...) or
# as part of the full run_canary() lifecycle below.


def step_insert(conn, timeout: float, verbose: bool) -> int:
    narrate(
        verbose,
        "STEP 1/5 — INSERT: writing today's canary row into Postgres `orders`. "
        "This commit is hop 1: the source-of-truth transaction.",
    )
    order_id, wal_lsn = conn.execute(
        """
        INSERT INTO orders (customer_id, amount, status)
        VALUES (1111, %s, 'pending') RETURNING id, pg_current_wal_lsn()
        """,
        (Decimal("11.11"),),
    ).fetchone()
    conn.commit()
    hop_detail(verbose, "Postgres", table="orders", id=order_id, wal_lsn=wal_lsn)

    narrate(
        verbose,
        f"Committed. Now polling the API for id={order_id} — this wait *is* the "
        "Debezium WAL read, the Kafka produce, the Spark micro-batch, and the "
        "ClickHouse write, all happening while we watch.",
    )
    inserted = eventually(
        "canary INSERT to reach the API",
        lambda: matches_api(order_id, customer_id=1111, amount=11.11, status="pending"),
        timeout,
    )
    hop_detail(
        verbose,
        "Debezium/Kafka",
        topic=inserted["source_topic"],
        partition=inserted["kafka_partition"],
        offset=inserted["kafka_offset"],
    )
    hop_detail(
        verbose,
        "ClickHouse/API",
        version=inserted.get("version"),
        amount=inserted.get("amount"),
        status=inserted.get("status"),
    )
    print(
        "PASS insert: "
        f"id={order_id} event={inserted['source_topic']}:"
        f"{inserted['kafka_partition']}:{inserted['kafka_offset']}"
    )
    return order_id


def step_update(conn, order_id: int, timeout: float, verbose: bool) -> None:
    narrate(
        verbose,
        "STEP 2/5 — UPDATE: changing amount/status in Postgres. Watch the "
        "*version* number in ClickHouse below — it must rise, because version "
        "is the Debezium LSN, not a row we overwrite in place.",
    )
    conn.execute(
        "UPDATE orders SET amount=%s, status='paid' WHERE id=%s",
        (Decimal("42.42"), order_id),
    )
    conn.commit()
    updated = eventually(
        "canary UPDATE to replace current state",
        lambda: matches_api(order_id, amount=42.42, status="paid"),
        timeout,
    )
    hop_detail(
        verbose,
        "ClickHouse/API",
        version=updated.get("version"),
        amount=updated.get("amount"),
        status=updated.get("status"),
        note="version rose; old row still in ClickHouse until FINAL merges it away",
    )
    print("PASS update: API exposes the higher-LSN current state")


def step_nullable_set(
    conn, order_id: int, timeout: float, verbose: bool, standalone: bool = False
) -> None:
    if not has_discount_column(conn):
        raise SystemExit(
            "discount_code does not exist yet — run scripts/migrate_discount_code.py first"
        )
    narrate(
        verbose,
        "STEP 3a/5 — NULLABLE UPDATE (set): setting discount_code to 'VERIFY11', "
        "so there is a real before-image to try to resurrect.",
    )
    conn.execute("UPDATE orders SET discount_code='VERIFY11' WHERE id=%s", (order_id,))
    conn.commit()
    populated = eventually(
        "nullable field to become populated",
        lambda: matches_api(order_id, discount_code="VERIFY11"),
        timeout,
    )
    hop_detail(verbose, "ClickHouse/API", discount_code=populated.get("discount_code"))
    if standalone:
        print("PASS nullable-set: discount_code=VERIFY11 reached the API")


def step_nullable_clear(conn, order_id: int, timeout: float, verbose: bool) -> None:
    if not has_discount_column(conn):
        raise SystemExit(
            "discount_code does not exist yet — run scripts/migrate_discount_code.py first"
        )
    narrate(
        verbose,
        "STEP 3b/5 — NULLABLE UPDATE (clear): setting discount_code back to NULL. "
        "The bug this guards against: naively coalescing after/before images would "
        "silently bring back 'VERIFY11' instead of honoring NULL.",
    )
    conn.execute("UPDATE orders SET discount_code=NULL WHERE id=%s", (order_id,))
    conn.commit()
    nulled = eventually(
        "nullable field to remain NULL rather than resurrect before-image data",
        lambda: matches_api(order_id, discount_code=None),
        timeout,
    )
    hop_detail(
        verbose,
        "ClickHouse/API",
        discount_code=nulled.get("discount_code"),
        note="an explicit NULL, not the pre-VERIFY11 before-image",
    )
    print("PASS nullable update: NULL did not resurrect the before image")


def step_delete(conn, order_id: int, verbose: bool, standalone: bool = False) -> None:
    narrate(
        verbose,
        "STEP 4/5 — DELETE: removing the canary row from Postgres. Debezium "
        "emits a tombstone-carrying delete event with a higher LSN than every "
        "prior version of this row.",
    )
    conn.execute("DELETE FROM orders WHERE id=%s", (order_id,))
    conn.commit()
    hop_detail(verbose, "Postgres", note=f"tombstone written for id={order_id}")
    if standalone:
        print(f"PASS delete-write: tombstone committed for id={order_id}")


def step_verify_delete(order_id: int, timeout: float, verbose: bool) -> None:
    narrate(
        verbose,
        "Polling ClickHouse directly (bypassing the API) to see the raw "
        "_is_deleted=1 marker win under FINAL — this is the mechanism, not just "
        "the API's opinion about it.",
    )
    delete_marker = eventually(
        "delete marker to win in ClickHouse", lambda: delete_won(order_id), timeout
    )
    hop_detail(
        verbose,
        "ClickHouse",
        _is_deleted=delete_marker[0],
        version=delete_marker[1],
        topic=delete_marker[2],
        partition=delete_marker[3],
        offset=delete_marker[4],
    )
    narrate(
        verbose,
        f"STEP 5/5 — Now confirming the API itself agrees and returns 404 for id={order_id}.",
    )
    eventually(
        "deleted canary to disappear from the API",
        lambda: True if api_order(order_id) is None else None,
        timeout,
    )
    hop_detail(verbose, "API", response="404 Not Found")
    print("PASS delete: higher-LSN marker wins and API returns 404")


# ─── full lifecycle: the same five steps run back to back ─────────────────


def run_canary(timeout: float, verbose: bool = False) -> None:
    with pg_connect() as conn:
        has_discount = has_discount_column(conn)
        order_id = step_insert(conn, timeout, verbose)
        step_update(conn, order_id, timeout, verbose)
        if has_discount:
            step_nullable_set(conn, order_id, timeout, verbose)
            step_nullable_clear(conn, order_id, timeout, verbose)
        step_delete(conn, order_id, verbose)
    step_verify_delete(order_id, timeout, verbose)
    print("PASS: Postgres → Debezium → Avro/Kafka → Spark → ClickHouse → API")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Run the full five-step canary lifecycle, or isolate a single step "
            "with --order-id pointing at a canary an earlier step already created."
        )
    )
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="print the row/offset/LSN detail behind each PASS line",
    )
    parser.add_argument(
        "--order-id",
        type=int,
        help="existing canary id to act on (required by every step except --insert)",
    )
    step_group = parser.add_mutually_exclusive_group()
    step_group.add_argument(
        "--insert", action="store_true", help="step 1 only: insert a fresh canary"
    )
    step_group.add_argument(
        "--update", action="store_true", help="step 2 only: update amount/status"
    )
    step_group.add_argument(
        "--nullable-set",
        action="store_true",
        help="step 3a only: set discount_code='VERIFY11'",
    )
    step_group.add_argument(
        "--nullable-clear",
        action="store_true",
        help="step 3b only: set discount_code back to NULL",
    )
    step_group.add_argument(
        "--delete", action="store_true", help="step 4 only: delete the row in Postgres"
    )
    step_group.add_argument(
        "--verify-delete",
        action="store_true",
        help="step 5 only: confirm ClickHouse's tombstone and the API's 404",
    )
    args = parser.parse_args()

    single_step = any(
        [
            args.insert,
            args.update,
            args.nullable_set,
            args.nullable_clear,
            args.delete,
            args.verify_delete,
        ]
    )

    if not single_step:
        run_canary(args.timeout, verbose=args.verbose)
    elif args.insert:
        with pg_connect() as conn:
            step_insert(conn, args.timeout, args.verbose)
    elif args.verify_delete:
        if args.order_id is None:
            parser.error("--verify-delete requires --order-id")
        step_verify_delete(args.order_id, args.timeout, args.verbose)
    else:
        if args.order_id is None:
            parser.error("this step requires --order-id (the canary an earlier step created)")
        with pg_connect() as conn:
            if args.update:
                step_update(conn, args.order_id, args.timeout, args.verbose)
            elif args.nullable_set:
                step_nullable_set(
                    conn, args.order_id, args.timeout, args.verbose, standalone=True
                )
            elif args.nullable_clear:
                step_nullable_clear(conn, args.order_id, args.timeout, args.verbose)
            elif args.delete:
                step_delete(conn, args.order_id, args.verbose, standalone=True)
