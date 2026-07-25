"""FastAPI dashboard backend over ClickHouse for Lesson 10.

Run with:
    uv run uvicorn src.app:app --reload --host 0.0.0.0 --port 8000

Then hit:
    curl "http://localhost:8000/api/revenue?minutes=5"
    curl "http://localhost:8000/api/throughput?minutes=10"
    curl "http://localhost:8000/api/top-users?minutes=5&limit=10"
"""

import asyncio
import threading
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import clickhouse_connect
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from src.config import (
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


_local = threading.local()


def _client() -> Any:
    """Return this worker thread's ClickHouse client, creating it on first use.

    Building a client per request costs a TCP connect plus a handshake -- about
    5-6 ms on a laptop. That is larger than the pre-aggregated query it wraps,
    so it would dominate every latency number measured in this lesson.

    Sharing one client across every thread is the opposite mistake. Since
    queries run under `asyncio.to_thread`, a thread-local gives each worker in
    the executor pool its own connection, reused across requests.
    """
    client = getattr(_local, "client", None)
    if client is None:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE,
        )
        _local.client = client
    return client


def _run_query(query: str, parameters: dict):
    return _client().query(query, parameters=parameters)


def _serialize(value):
    """Make ClickHouse / Decimal values JSON-serializable."""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


ADDITIVE_ONLY = """
    SELECT
        minute,
        region,
        sumMerge(total_revenue) AS revenue,
        countMerge(purchase_count) AS purchases
    FROM revenue_by_region_minute
    WHERE minute >= now() - INTERVAL {minutes:UInt16} MINUTE
    GROUP BY minute, region
    ORDER BY minute DESC, region
"""

# Same grain, one extra column -- and it reads from a different table, because
# count-distinct is stored separately. Compare the two `query_ms` values.
WITH_UNIQUE_USERS = """
    SELECT r.minute, r.region, r.revenue, r.purchases, u.unique_users
    FROM (
        SELECT
            minute,
            region,
            sumMerge(total_revenue) AS revenue,
            countMerge(purchase_count) AS purchases
        FROM revenue_by_region_minute
        WHERE minute >= now() - INTERVAL {minutes:UInt16} MINUTE
        GROUP BY minute, region
    ) AS r
    LEFT JOIN (
        SELECT
            minute,
            region,
            uniqMerge(unique_users) AS unique_users
        FROM unique_users_by_region_minute
        WHERE minute >= now() - INTERVAL {minutes:UInt16} MINUTE
        GROUP BY minute, region
    ) AS u USING (minute, region)
    ORDER BY r.minute DESC, r.region
"""


@app.get("/api/revenue")
async def revenue_by_region(
    minutes: int = Query(default=10, ge=1, le=1440),
    unique_users: bool = Query(
        default=False,
        description="also return distinct users per region — measure what count-distinct costs",
    ),
):
    """Revenue per region for the last N minutes, from the pre-aggregated MVs.

    `sum` and `count` collapse to one number per group, so merging their states
    is a few additions. `uniq` cannot collapse — it carries a sketch — which is
    why it lives in its own table and behind its own flag. Hit this endpoint
    both ways and compare `query_ms`.
    """
    start = time.perf_counter()

    query = WITH_UNIQUE_USERS if unique_users else ADDITIVE_ONLY
    result = await asyncio.to_thread(_run_query, query, {"minutes": minutes})

    rows = [
        {
            "minute": _serialize(row[0]),
            "region": row[1],
            "revenue": _serialize(row[2]),
            "purchases": row[3],
            **({"unique_users": row[4]} if unique_users else {}),
        }
        for row in result.result_rows
    ]

    return {
        "query_ms": round((time.perf_counter() - start) * 1000, 2),
        "unique_users": unique_users,
        "rows": rows,
    }


@app.get("/api/throughput")
async def throughput(minutes: int = Query(default=10, ge=1, le=1440)):
    """Raw event throughput and unique users per minute from the events table."""
    start = time.perf_counter()

    query = """
        SELECT
            toStartOfMinute(event_time) AS minute,
            count() AS events,
            uniqExact(user_id) AS unique_users
        FROM events
        WHERE event_time >= now() - INTERVAL {minutes:UInt16} MINUTE
        GROUP BY minute
        ORDER BY minute DESC
    """
    result = await asyncio.to_thread(_run_query, query, {"minutes": minutes})

    rows = [
        {
            "minute": _serialize(row[0]),
            "events": row[1],
            "unique_users": row[2],
        }
        for row in result.result_rows
    ]

    return {"query_ms": round((time.perf_counter() - start) * 1000, 2), "rows": rows}


@app.get("/api/top-users")
async def top_users(
    minutes: int = Query(default=5, ge=1, le=1440),
    limit: int = Query(default=10, ge=1, le=100),
):
    """Top purchasers over the last N minutes — raw scan with high-cardinality GROUP BY."""
    start = time.perf_counter()

    query = """
        SELECT
            user_id,
            sum(amount) AS total_spend,
            count() AS purchase_count
        FROM events
        WHERE event_type = 'purchase'
          AND event_time >= now() - INTERVAL {minutes:UInt16} MINUTE
        GROUP BY user_id
        ORDER BY total_spend DESC
        LIMIT {limit:UInt16}
    """
    result = await asyncio.to_thread(_run_query, query, {"minutes": minutes, "limit": limit})

    rows = [
        {
            "user_id": row[0],
            "total_spend": _serialize(row[1]),
            "purchase_count": row[2],
        }
        for row in result.result_rows
    ]

    return {"query_ms": round((time.perf_counter() - start) * 1000, 2), "rows": rows}


@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now(UTC).isoformat()}
