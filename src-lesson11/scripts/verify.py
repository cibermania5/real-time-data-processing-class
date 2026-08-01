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


def run_canary(timeout: float) -> None:
    with pg_connect() as conn:
        has_discount = bool(
            conn.execute(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='orders'
                  AND column_name='discount_code'
                """
            ).fetchone()
        )
        order_id = conn.execute(
            """
            INSERT INTO orders (customer_id, amount, status)
            VALUES (1111, %s, 'pending') RETURNING id
            """,
            (Decimal("11.11"),),
        ).fetchone()[0]
        conn.commit()

        inserted = eventually(
            "canary INSERT to reach the API",
            lambda: matches_api(
                order_id, customer_id=1111, amount=11.11, status="pending"
            ),
            timeout,
        )
        print(
            "PASS insert: "
            f"id={order_id} event={inserted['source_topic']}:"
            f"{inserted['kafka_partition']}:{inserted['kafka_offset']}"
        )

        conn.execute(
            "UPDATE orders SET amount=%s, status='paid' WHERE id=%s",
            (Decimal("42.42"), order_id),
        )
        conn.commit()
        eventually(
            "canary UPDATE to replace current state",
            lambda: matches_api(order_id, amount=42.42, status="paid"),
            timeout,
        )
        print("PASS update: API exposes the higher-LSN current state")

        if has_discount:
            conn.execute(
                "UPDATE orders SET discount_code='VERIFY11' WHERE id=%s", (order_id,)
            )
            conn.commit()
            eventually(
                "nullable field to become populated",
                lambda: matches_api(order_id, discount_code="VERIFY11"),
                timeout,
            )
            conn.execute("UPDATE orders SET discount_code=NULL WHERE id=%s", (order_id,))
            conn.commit()
            eventually(
                "nullable field to remain NULL rather than resurrect before-image data",
                lambda: matches_api(order_id, discount_code=None),
                timeout,
            )
            print("PASS nullable update: NULL did not resurrect the before image")

        conn.execute("DELETE FROM orders WHERE id=%s", (order_id,))
        conn.commit()

    eventually(
        "delete marker to win in ClickHouse",
        lambda: delete_won(order_id),
        timeout,
    )
    eventually(
        "deleted canary to disappear from the API",
        lambda: True if api_order(order_id) is None else None,
        timeout,
    )
    print("PASS delete: higher-LSN marker wins and API returns 404")
    print("PASS: Postgres → Debezium → Avro/Kafka → Spark → ClickHouse → API")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=float, default=120)
    args = parser.parse_args()
    run_canary(args.timeout)
