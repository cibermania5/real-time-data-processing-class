"""Micro-demo: which Kafka-engine setting actually sets the ingestion floor?

Two sweeps against a fresh Kafka table + MV, measuring how long a canary event
takes to become queryable.

  Act 1 sweeps kafka_max_block_size and finds it flat: at one event per run a
        block never fills, so the block size cannot be what ends the batch.
  Act 2 sweeps kafka_flush_interval_ms and finds the floor moving with it.

The tuning-guide answer ("smaller blocks mean fresher data") is only true once
the arrival rate is high enough to fill a block before the flush timer fires.

Usage:
    uv run python src/demo_ingest_floor.py
"""

import json
import sys
import time
import uuid
from datetime import UTC, datetime

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.cimpl import Producer

from config import BOOTSTRAP, DEMO_FLOOR_TOPIC, await_topic, get_ch_client, lesson


def drop_floor_tables(client):
    """Release this demo's Kafka consumer before touching the topic.

    A Kafka-engine table is a live consumer. If a previous run's
    events_floor_kafka is still attached, deleting the topic blocks and the
    demo hangs with no output at all -- so always drop first, then delete.
    """
    client.command("DROP TABLE IF EXISTS events_floor_mv")
    client.command("DROP TABLE IF EXISTS events_floor_kafka")
    client.command("DROP TABLE IF EXISTS events_floor")


def ensure_topic(topic: str):
    """Create a fresh topic for this run.

    This used to delete and recreate one fixed topic, which hung two different
    ways: deleting a topic a Kafka-engine table is still consuming blocks, and
    deleting a topic that does not exist can leave the AdminClient future
    unresolved past its operation_timeout. Both symptoms are identical from the
    front of a classroom -- no output, no error. A per-run topic sidesteps the
    whole problem, and the offsets are clean by construction.
    """
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    admin.create_topics([NewTopic(topic, 1, 1)])
    await_topic(admin, topic, present=True)
    print(f"  created topic: {topic}")


def delete_topic(topic: str):
    """Best-effort cleanup so runs do not accumulate topics on the broker."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    try:
        admin.delete_topics([topic], operation_timeout=10)
        await_topic(admin, topic, present=False, timeout=20)
        print(f"  deleted topic: {topic}")
    except (KafkaException, TimeoutError) as exc:
        print(f"  could not delete {topic} ({exc}); harmless, it is empty", file=sys.stderr)


def setup_floor_tables(client, block_size: int, run_id: str, topic: str, flush_ms: int = 1000):
    client.command("DROP TABLE IF EXISTS events_floor_mv")
    client.command("DROP TABLE IF EXISTS events_floor_kafka")
    client.command("DROP TABLE IF EXISTS events_floor")

    client.command("""
        CREATE TABLE events_floor (
            event_time DateTime64(3),
            user_id UInt32,
            event_type LowCardinality(String),
            amount Decimal(10, 2),
            region LowCardinality(String)
        ) ENGINE = MergeTree()
        ORDER BY (event_type, region, event_time)
    """)
    client.command(f"""
        CREATE TABLE events_floor_kafka (
            event_time String,
            user_id UInt32,
            event_type String,
            amount Decimal(10, 2),
            region String
        ) ENGINE = Kafka()
        SETTINGS
            kafka_broker_list = 'kafka:9092',
            kafka_topic_list = '{topic}',
            kafka_group_name = 'ch-floor-demo-{block_size}-{run_id}',
            kafka_format = 'JSONEachRow',
            kafka_max_block_size = {block_size},
            kafka_poll_timeout_ms = 100,
            kafka_flush_interval_ms = {flush_ms}
    """)
    # auto.offset.reset is a librdkafka property, not a table setting -- it is
    # set server-side in config/clickhouse-kafka.xml. Putting it here raises
    # UNKNOWN_SETTING.
    client.command("""
        CREATE MATERIALIZED VIEW events_floor_mv TO events_floor AS
        SELECT parseDateTime64BestEffort(event_time) AS event_time, user_id, event_type, amount, region
        FROM events_floor_kafka
    """)


def send_and_wait(client, producer, topic: str, uid: int, timeout: float) -> float:
    """Produce one canary with `uid` and return seconds until it is queryable."""
    sent = datetime.now(UTC)
    producer.produce(topic, json.dumps({
        "event_time": sent.isoformat(),
        "user_id": uid,
        "event_type": "purchase",
        "amount": 0.01,
        "region": "canary",
    }).encode("utf-8"))
    producer.flush()

    # Bounded wait. An unbounded poll here turns any upstream misconfiguration
    # -- a rejected Kafka setting, a consumer group stuck on the wrong offset --
    # into a demo that hangs silently in front of the class instead of failing.
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = client.query(
            "SELECT count() FROM events_floor WHERE user_id = {uid:UInt32}",
            parameters={"uid": uid},
        )
        if result.result_rows[0][0] > 0:
            return (datetime.now(UTC) - sent).total_seconds()
        time.sleep(0.02)
    return -1.0


def measure(block_size: int, topic: str, flush_ms: int = 1000, timeout: float = 60.0) -> float:
    client = get_ch_client()
    run_id = uuid.uuid4().hex[:8]
    setup_floor_tables(client, block_size, run_id, topic, flush_ms)

    base = 800_000_000 + block_size * 10 + flush_ms
    producer = Producer({"bootstrap.servers": BOOTSTRAP})

    # The first message through a freshly created table also pays for the
    # consumer joining its group and receiving a partition assignment. That is
    # startup cost, not steady-state ingestion latency, and it is roughly a
    # second -- big enough to swamp the knob we are trying to measure. Burn one
    # event to get the consumer running, then measure the next one.
    if send_and_wait(client, producer, topic, base, timeout) < 0:
        raise TimeoutError(
            f"warm-up canary for block_size={block_size}, flush_ms={flush_ms} "
            f"never arrived within {timeout}s. Check: SELECT * FROM system.kafka_consumers"
        )

    elapsed = send_and_wait(client, producer, topic, base + 1, timeout)
    if elapsed >= 0:
        return elapsed

    raise TimeoutError(
        f"canary for kafka_max_block_size={block_size} never arrived within {timeout}s. "
        "Check that the Kafka engine table was created and is consuming: "
        "SELECT * FROM system.kafka_consumers"
    )


def main():
    print("\n" + "═" * 58)
    print("  INGESTION LATENCY: which knob is the floor?")
    print("═" * 58)

    topic = f"{DEMO_FLOOR_TOPIC}-{uuid.uuid4().hex[:6]}"
    drop_floor_tables(get_ch_client())
    ensure_topic(topic)

    try:
        print("\n  ACT 1 · sweep kafka_max_block_size, flush interval fixed at 1000ms")
        print("  " + "─" * 56)
        for bs in (65536, 4096, 512, 64):
            print(f"  kafka_max_block_size = {bs:>6}:  {measure(bs, topic, 1000):.2f}s")

        print("\n  ACT 2 · sweep kafka_flush_interval_ms, block size fixed at 1000")
        print("  " + "─" * 56)
        for ms in (2000, 1000, 500, 100):
            print(f"  kafka_flush_interval_ms = {ms:>5}:  {measure(1000, topic, ms):.2f}s")
    finally:
        # Drop the consumer before the topic, or the delete blocks.
        drop_floor_tables(get_ch_client())
        delete_topic(topic)

    lesson(
        "Act 1 is flat. A 1000x range of block sizes moved nothing, because",
        "one canary event never fills a block of any size -- so the block",
        "size is not what ends the batch.",
        "",
        "Act 2 moves. Latency falls with kafka_flush_interval_ms and collapses",
        "once the timer is short, because the flush timer is what actually",
        "ends the batch at this arrival rate. The individual numbers are noisy:",
        "a single canary lands at a random point inside the current flush",
        "window, so one sample per setting measures the trend, not a constant.",
        "",
        "kafka_max_block_size only becomes the binding constraint when the",
        "stream is fast enough to fill a block before the timer fires. Which",
        "knob matters depends on your arrival rate -- so measure yours",
        "rather than copying a tuning guide.",
    )


if __name__ == "__main__":
    main()
