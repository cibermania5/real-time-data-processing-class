"""Measure ClickHouse ingestion latency with canary events.

Sends a uniquely-marked event into Kafka and polls ClickHouse until it appears.
Reports the distribution of end-to-end ingestion latency.

Usage:
    uv run python src/measure_freshness.py --runs 10
"""

import argparse
import json
import statistics
import time
from datetime import UTC, datetime

from confluent_kafka import Producer

from config import BOOTSTRAP, EVENTS_TOPIC, banner, get_ch_client


def measure_one(client, producer, run_id: int) -> float:
    canary_id = 900_000_000 + run_id
    canary_time = datetime.now(UTC)
    event = {
        "event_time": canary_time.isoformat(),
        "user_id": canary_id,
        "event_type": "purchase",
        "amount": 0.01,
        "region": "canary",
    }

    producer.produce(EVENTS_TOPIC, json.dumps(event).encode("utf-8"))
    producer.flush()

    while True:
        result = client.query(
            "SELECT count() FROM events WHERE user_id = {uid:UInt32}",
            parameters={"uid": canary_id},
        )
        if result.result_rows[0][0] > 0:
            break
        time.sleep(0.1)

    return (datetime.now(UTC) - canary_time).total_seconds()


def main():
    parser = argparse.ArgumentParser(description="measure ClickHouse ingestion latency")
    parser.add_argument("--runs", type=int, default=10, help="number of canary events")
    parser.add_argument("--topic", default=EVENTS_TOPIC)
    args = parser.parse_args()

    banner(
        "Ingestion latency benchmark",
        f"bootstrap: {BOOTSTRAP}",
        f"topic:     {args.topic}",
        f"runs:      {args.runs}",
    )

    client = get_ch_client()
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    latencies: list[float] = []

    print("sending canary events...")
    for i in range(1, args.runs + 1):
        latency = measure_one(client, producer, i)
        latencies.append(latency)
        print(f"  run {i:02d}: {latency:.2f}s")

    latencies.sort()
    print("\n" + "─" * 58)
    print(f"  min:  {min(latencies):.2f}s")
    print(f"  p50:  {statistics.median(latencies):.2f}s")
    print(f"  p95:  {latencies[int(len(latencies) * 0.95)]:.2f}s")
    print(f"  max:  {max(latencies):.2f}s")
    print("─" * 58)

    # Rough guidance; the dominant factor is kafka_max_block_size / poll timeout.
    print("\nTIP: run this with and without the producer going. On an idle topic the")
    print("     flush timer sets the floor; under load a block fills first and the")
    print("     numbers collapse. src/demo_ingest_floor.py isolates both knobs.")


if __name__ == "__main__":
    main()
