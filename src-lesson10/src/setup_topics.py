"""Create (and optionally reset) the Kafka topic used by Lesson 10."""

import argparse
import sys
import time

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from config import BOOTSTRAP, EVENTS_TOPIC

TOPICS = [EVENTS_TOPIC]


def reset_topics(admin: AdminClient, poll_timeout: float = 20.0, poll_interval: float = 0.5) -> None:
    futures = admin.delete_topics(TOPICS, operation_timeout=30)
    for name, future in futures.items():
        try:
            future.result()
            print(f"  deleted: {name}")
        except KafkaException as e:
            if e.args[0].code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                print(f"  did not exist: {name}")
            else:
                print(f"  FAILED to delete {name}: {e}", file=sys.stderr)

    # Poll until the broker actually removes the topic metadata. A fixed
    # sleep is not reliable and leads to TOPIC_ALREADY_EXISTS on recreate.
    deadline = time.time() + poll_timeout
    still_present = TOPICS
    while time.time() < deadline:
        metadata = admin.list_topics(timeout=5)
        still_present = [t for t in TOPICS if t in metadata.topics]
        if not still_present:
            return
        time.sleep(poll_interval)
    print(f"  WARNING: still present after {poll_timeout}s: {still_present}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="create (and optionally reset) L10 Kafka topics")
    parser.add_argument("--reset", action="store_true", help="delete existing topics before creating")
    args = parser.parse_args()

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})

    if args.reset:
        reset_topics(admin)

    topics = [NewTopic(name, num_partitions=1, replication_factor=1) for name in TOPICS]
    futures = admin.create_topics(topics)

    failed = False
    for name, future in futures.items():
        try:
            future.result()
            print(f"  created: {name}")
        except KafkaException as e:
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"  already exists: {name}")
            else:
                print(f"  FAILED: {name}: {e}", file=sys.stderr)
                failed = True

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
