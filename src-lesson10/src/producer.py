"""Generate a controlled-rate e-commerce event stream for the Lesson 10 OLAP demo.

Each event has an event-time timestamp (wall-clock when it is produced),
a user_id, event_type, amount, and region. ClickHouse ingests the stream directly.

Usage:
    uv run python src/producer.py --rate 1000 --duration-seconds 600
"""

import argparse
import json
import random
import sys
import time
from datetime import UTC, datetime

from confluent_kafka import Producer

from config import BOOTSTRAP, DATA_DIR, EVENTS_TOPIC, banner

# Must match seed_bulk.py so streamed events and seeded history are the same process.
REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "ap-south"]
EVENT_TYPES = ["purchase", "refund", "page_view", "add_to_cart"]


def delivery_report(err, _msg):
    if err is not None:
        print(f"delivery error: {err}", file=sys.stderr)


def make_event():
    return {
        "event_time": datetime.now(UTC).isoformat(),
        "user_id": random.randint(1, 100_000),
        "event_type": random.choice(EVENT_TYPES),
        "amount": round(random.uniform(1.0, 500.0), 2) if random.random() > 0.3 else 0,
        "region": random.choice(REGIONS),
    }


def main():
    parser = argparse.ArgumentParser(description="produce e-commerce events for L10")
    parser.add_argument("--rate", type=int, default=1000, help="events per second")
    parser.add_argument("--duration-seconds", type=int, default=600, help="how long to produce")
    parser.add_argument("--topic", default=EVENTS_TOPIC, help="target Kafka topic")
    args = parser.parse_args()

    banner(
        "Lesson 10 producer",
        f"bootstrap: {BOOTSTRAP}",
        f"topic:     {args.topic}",
        f"rate:      {args.rate} events/s",
        f"duration:  {args.duration_seconds}s (~{args.rate * args.duration_seconds:,} events)",
    )

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
    })

    # Pace in batches against a wall-clock deadline rather than sleeping once
    # per event. A per-event `time.sleep(1/rate)` cannot hold rates much above
    # a few hundred/s: the OS timer granularity and the produce() call each
    # cost more than the interval they are supposed to fill, so the error
    # compounds and the effective rate lands well under the target. Sleeping
    # per batch, to an absolute deadline derived from the total sent so far,
    # is self-correcting -- fall behind and the sleep is skipped until we
    # catch up.
    batch = max(1, args.rate // 100)  # ~100 wake-ups per second
    start = time.time()
    order_id = 0
    next_report = args.rate
    summary = {"start_time": datetime.now(UTC).isoformat(), "rate": args.rate}

    try:
        while time.time() - start < args.duration_seconds:
            for _ in range(batch):
                producer.produce(
                    topic=args.topic,
                    value=json.dumps(make_event()).encode("utf-8"),
                    callback=delivery_report,
                )
                order_id += 1
            producer.poll(0)

            if order_id >= next_report:
                elapsed = time.time() - start
                print(f"  produced {order_id:,} events in {elapsed:.1f}s ({order_id / elapsed:.1f} events/s)")
                next_report += args.rate

            slack = (start + order_id / args.rate) - time.time()
            if slack > 0:
                time.sleep(slack)
    except KeyboardInterrupt:
        print("\ninterrupted by user", file=sys.stderr)
    finally:
        producer.flush()

    duration = max(time.time() - start, 0.001)
    summary.update({
        "events": order_id,
        "duration_seconds": round(duration, 2),
        "effective_rate": round(order_id / duration, 2),
    })

    summary_file = DATA_DIR / "producer_summary.json"
    summary_file.write_text(json.dumps(summary, indent=2) + "\n")
    print(f"\nproducer summary written to {summary_file}")


if __name__ == "__main__":
    main()
