"""Concurrent HTTP load test against the Lesson 10 FastAPI dashboard.

Run the producer and FastAPI first, then:
    uv run python src/query_loadtest.py --endpoint http://localhost:8000/api/revenue?minutes=5

"""

import argparse
import concurrent.futures
import time

import httpx

from config import banner


def query_loop(url: str, requests: int) -> list[tuple[float, float | None]]:
    results: list[tuple[float, float | None]] = []
    with httpx.Client() as client:
        for _ in range(requests):
            start = time.perf_counter()
            resp = client.get(url)
            elapsed_ms = (time.perf_counter() - start) * 1000
            try:
                data = resp.json()
                server_ms = data.get("query_ms")
            except Exception:  # noqa: BLE001
                server_ms = None
            results.append((elapsed_ms, server_ms))
    return results


def main():
    parser = argparse.ArgumentParser(description="HTTP load test for dashboard endpoints")
    parser.add_argument(
        "--endpoint",
        default="http://localhost:8000/api/revenue?minutes=5",
        help="URL to hammer",
    )
    parser.add_argument("--clients", type=int, default=10, help="concurrent query clients")
    parser.add_argument("--requests", type=int, default=100, help="requests per client")
    args = parser.parse_args()

    banner(
        "Query load test",
        f"endpoint: {args.endpoint}",
        f"clients:  {args.clients}",
        f"requests: {args.requests * args.clients}",
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.clients) as executor:
        futures = [executor.submit(query_loop, args.endpoint, args.requests) for _ in range(args.clients)]
        raw_results = [f.result() for f in concurrent.futures.as_completed(futures)]

    results = [r for sub in raw_results for r in sub]
    total = sorted(r[0] for r in results)
    server = sorted(r[1] for r in results if r[1] is not None)

    def pct(sorted_values, p):
        return sorted_values[int(len(sorted_values) * p)]

    print("\ntotal round-trip latency (ms):")
    print(f"  p50: {pct(total, 0.50):.1f}")
    print(f"  p95: {pct(total, 0.95):.1f}")
    print(f"  p99: {pct(total, 0.99):.1f}")

    if server:
        print("\nClickHouse-side query latency (ms):")
        print(f"  p50: {pct(server, 0.50):.1f}")
        print(f"  p95: {pct(server, 0.95):.1f}")
        print(f"  p99: {pct(server, 0.99):.1f}")


if __name__ == "__main__":
    main()
