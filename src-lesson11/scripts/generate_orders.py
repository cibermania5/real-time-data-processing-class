"""Generate INSERT/UPDATE/DELETE OLTP traffic; Debezium observes the WAL."""

from __future__ import annotations

import argparse
import random
import time
from decimal import Decimal

from _common import pg_connect

STATUSES = ("pending", "paid", "shipped", "delivered", "cancelled")
# MAGNUM11 and VERIFY11 are deliberately absent: the migration and the verifier
# use them as unique canaries, so ordinary load must never be able to forge one.
DISCOUNTS = (None, "WELCOME10", "VIP20", "FREESHIP")


def run(rate: float, duration: float, seed: int) -> None:
    rng = random.Random(seed)
    counts = {"insert": 0, "update": 0, "delete": 0}
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
        ids = [row[0] for row in conn.execute("SELECT id FROM orders ORDER BY id DESC LIMIT 5000")]
        started = time.monotonic()
        next_tick = started
        try:
            while duration <= 0 or time.monotonic() - started < duration:
                choice = rng.random()
                if choice < 0.62 or not ids:
                    if has_discount:
                        row = conn.execute(
                            """
                            INSERT INTO orders
                                (customer_id, amount, status, discount_code)
                            VALUES (%s, %s, %s, %s) RETURNING id
                            """,
                            (
                                rng.randint(1, 1000), Decimal(rng.randrange(500, 50000)) / 100,
                                rng.choice(STATUSES[:3]), rng.choice(DISCOUNTS),
                            ),
                        ).fetchone()
                    else:
                        row = conn.execute(
                            """
                            INSERT INTO orders (customer_id, amount, status)
                            VALUES (%s, %s, %s) RETURNING id
                            """,
                            (
                                rng.randint(1, 1000), Decimal(rng.randrange(500, 50000)) / 100,
                                rng.choice(STATUSES[:3]),
                            ),
                        ).fetchone()
                    ids.append(row[0])
                    counts["insert"] += 1
                elif choice < 0.92:
                    conn.execute(
                        "UPDATE orders SET status=%s WHERE id=%s",
                        (rng.choice(STATUSES), rng.choice(ids)),
                    )
                    counts["update"] += 1
                else:
                    index = rng.randrange(len(ids))
                    order_id = ids.pop(index)
                    conn.execute("DELETE FROM orders WHERE id=%s", (order_id,))
                    counts["delete"] += 1
                conn.commit()
                if rate > 0:
                    next_tick += 1 / rate
                    time.sleep(max(0, next_tick - time.monotonic()))
        except KeyboardInterrupt:
            pass
    print("generated " + ", ".join(f"{key}={value}" for key, value in counts.items()))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=10, help="committed changes per second")
    parser.add_argument("--duration", type=float, default=60, help="seconds; 0 runs until Ctrl-C")
    parser.add_argument("--seed", type=int, default=11)
    args = parser.parse_args()
    run(args.rate, args.duration, args.seed)

