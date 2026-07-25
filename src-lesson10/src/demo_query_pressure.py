"""Micro-demo: query latency under concurrent ingestion.

Runs the event producer in the background while concurrently hitting a
ClickHouse-backed endpoint. Shows that reads and writes can coexist at this scale.

Usage:
    uv run python src/demo_query_pressure.py --endpoint http://localhost:8000/api/throughput?minutes=2 --rate 1000
"""

import argparse
import concurrent.futures
import json
import threading
import time

import httpx

from config import BOOTSTRAP, EVENTS_TOPIC, banner
from producer import make_event


def producer_loop(rate: int, stop: threading.Event):
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    interval = 1.0 / rate
    order_id = 1_000_000
    while not stop.is_set():
        event = make_event(order_id)
        producer.produce(EVENTS_TOPIC, json.dumps(event).encode("utf-8"))
        producer.poll(0)
        order_id += 1
        time.sleep(interval)
    producer.flush()


def query_loop(url: str, requests: int) -> list[float]:
    latencies: list[float] = []
    with httpx.Client() as client:
        for _ in range(requests):
            start = time.perf_counter()
            client.get(url)
            latencies.append((time.perf_counter() - start) * 1000)
    return latencies


def main():
    parser = argparse.ArgumentParser(description="query latency under concurrent ingestion")
    parser.add_argument(
        "--endpoint",
        default="http://localhost:8000/api/throughput?minutes=2",
        help="URL to query",
    )
    parser.add_argument("--rate", type=int, default=1000, help="producer events per second")
    parser.add_argument("--clients", type=int, default=8, help="concurrent query clients")
    parser.add_argument("--requests", type=int, default=50, help="requests per client")
    args = parser.parse_args()

    banner(
        "Query pressure demo",
        f"endpoint: {args.endpoint}",
        f"producer: {args.rate} events/s",
        f"clients:  {args.clients}",
    )

    stop = threading.Event()
    thread = threading.Thread(target=producer_loop, args=(args.rate, stop), daemon=True)
    thread.start()
    time.sleep(2)  # let ingestion start

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.clients) as executor:
        futures = [executor.submit(query_loop, args.endpoint, args.requests) for _ in range(args.clients)]
        raw_results = [f.result() for f in concurrent.futures.as_completed(futures)]

    stop.set()
    thread.join(timeout=5)

    results = sorted(r for sub in raw_results for r in sub)
    print(f"\n  p50: {results[len(results) // 2]:.1f} ms")
    print(f"  p95: {results[int(len(results) * 0.95)]:.1f} ms")
    print(f"  p99: {results[int(len(results) * 0.99)]:.1f} ms")
    print(f"  samples: {len(results)}")


if __name__ == "__main__":
    main()
