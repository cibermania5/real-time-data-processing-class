"""Run the expand-first, mixed-Avro-version discount_code migration."""

from __future__ import annotations

import argparse
from decimal import Decimal

import requests
from _common import API_URL, SCHEMA_REGISTRY_URL, ch_connect, eventually, pg_connect

SUBJECT = "cdc.public.orders-value"


def visible(order_id: int, expected: str | None):
    client = ch_connect()
    try:
        rows = client.query(
            """
            SELECT discount_code FROM orders_current FINAL
            WHERE order_id={id:UInt64} AND _is_deleted=0
            """,
            parameters={"id": order_id},
        ).result_rows
    finally:
        client.close()
    return True if rows and rows[0][0] == expected else None


def api_visible(order_id: int, expected: str | None):
    response = requests.get(f"{API_URL}/api/orders/{order_id}", timeout=5)
    if not response.ok:
        return None
    return True if response.json().get("discount_code") == expected else None


def schema_versions() -> list[int]:
    response = requests.get(
        f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT}/versions", timeout=5
    )
    if response.status_code == 404:
        return []
    response.raise_for_status()
    return response.json()


def main(timeout: float) -> None:
    # EXPAND DOWNSTREAM. The processor discovers this column on its next batch;
    # no application restart or coordinated deployment is needed.
    client = ch_connect()
    try:
        client.command(
            "ALTER TABLE orders_current ADD COLUMN IF NOT EXISTS "
            "discount_code Nullable(String) AFTER status"
        )
    finally:
        client.close()
    print("1/4 downstream expanded: ClickHouse accepts discount_code=NULL")

    with pg_connect() as conn:
        already_expanded = bool(
            conn.execute(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='orders'
                  AND column_name='discount_code'
                """
            ).fetchone()
        )
        if not already_expanded:
            old_id = conn.execute(
                """
                INSERT INTO orders (customer_id, amount, status)
                VALUES (1101, %s, 'paid') RETURNING id
                """,
                (Decimal("11.01"),),
            ).fetchone()[0]
            conn.commit()
            eventually(
                "old Avro schema after downstream expansion",
                lambda: visible(old_id, None),
                timeout,
            )
            eventually("old-schema event in the API", lambda: api_visible(old_id, None), timeout)
            old_versions = eventually(
                "first Avro writer schema registration",
                lambda: schema_versions() or None,
                timeout,
            )
            compatibility = requests.put(
                f"{SCHEMA_REGISTRY_URL}/config/{SUBJECT}",
                json={"compatibility": "FULL"},
                timeout=10,
            )
            compatibility.raise_for_status()
            print(
                "2/4 old-schema event still flows through the API "
                f"(versions={old_versions}, compatibility=FULL)"
            )

            conn.execute("ALTER TABLE orders ADD COLUMN discount_code TEXT")
            conn.commit()
        else:
            print("2/4 source already expanded; old-schema proof was completed on the first run")
            old_versions = schema_versions()[:1]
        print("3/4 source expanded: Debezium will register a new value schema ID")

        new_id = conn.execute(
            """
            INSERT INTO orders (customer_id, amount, status, discount_code)
            VALUES (1102, %s, 'paid', 'MAGNUM11') RETURNING id
            """,
            (Decimal("111.11"),),
        ).fetchone()[0]
        conn.commit()

    eventually(
        "new Avro schema to decode without restarting Spark",
        lambda: visible(new_id, "MAGNUM11"),
        timeout,
    )
    eventually(
        "new-schema field to reach the API",
        lambda: api_visible(new_id, "MAGNUM11"),
        timeout,
    )
    new_versions = eventually(
        "Schema Registry to contain mixed writer versions",
        lambda: (items if len(items := schema_versions()) >= 2 else None),
        timeout,
    )
    print(
        "4/4 PASS: mixed writer schemas "
        f"{new_versions} coexist; API returned discount_code=MAGNUM11"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=float, default=120)
    args = parser.parse_args()
    main(args.timeout)
