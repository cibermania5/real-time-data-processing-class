"""FastAPI read model and Prometheus endpoint for the Lesson 11 pipeline."""

from __future__ import annotations

import asyncio
import threading
import time
from contextlib import suppress
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

from app.config import clickhouse_client

app = FastAPI(title="Lesson 11 Orders API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

REQUESTS = Counter(
    "api_http_requests_total", "API requests", ["method", "path", "status"]
)
REQUEST_SECONDS = Histogram(
    "api_http_request_duration_seconds", "API request duration", ["method", "path"]
)
VISIBLE_ORDERS = Gauge("api_visible_orders", "Non-deleted current orders in ClickHouse")
# Two different questions, deliberately not collapsed into one number:
#   staleness = now - newest source change. What a user feels. Grows when the
#              source is idle, even though nothing is wrong.
#   transit   = ingested_at - source_ts. How long the pipeline actually takes.
#              Flat while idle; rises under real backpressure.
# -1 means "no data to measure", which is not the same as "zero seconds old".
DATA_FRESHNESS = Gauge(
    "api_data_freshness_seconds",
    "Age of the newest source change visible in ClickHouse (-1 when the sink is empty)",
)
PIPELINE_TRANSIT = Gauge(
    "api_pipeline_transit_seconds",
    "Slowest source-to-queryable time over the last 5 minutes of writes (-1 when idle)",
)
_local = threading.local()
_metrics_lock = threading.Lock()
_metrics_refreshed_at = 0.0
_schema_lock = threading.Lock()
_schema_cache: tuple[float, bool] | None = None
_SCHEMA_TTL_SECONDS = 5.0


def _client() -> Any:
    client = getattr(_local, "client", None)
    if client is None:
        client = clickhouse_client()
        _local.client = client
    return client


def _query(sql: str, parameters: dict[str, Any] | None = None):
    return _client().query(sql, parameters=parameters or {})


def _json(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


@app.middleware("http")
async def observe_request(request, call_next):
    started = time.perf_counter()
    status = 500
    try:
        response = await call_next(request)
        status = response.status_code
        return response
    finally:
        route = request.scope.get("route")
        path = getattr(route, "path", request.url.path)
        REQUESTS.labels(request.method, path, str(status)).inc()
        REQUEST_SECONDS.labels(request.method, path).observe(time.perf_counter() - started)


@app.get("/health")
async def health() -> dict[str, Any]:
    """Readiness, not merely process liveness: it includes a ClickHouse round trip."""
    try:
        await asyncio.to_thread(_query, "SELECT 1")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"ClickHouse unavailable: {exc}") from exc
    return {"status": "ok", "time": datetime.now(UTC).isoformat()}


def _has_discount_column() -> bool:
    """Notice the mid-class ADD COLUMN without paying a probe per request.

    The migration happens while the API is serving, so this cannot be resolved
    once at startup. It also cannot cost a system.columns round trip on every
    request: that would double the query latency Lesson 10 taught students to
    measure. A few seconds of staleness on a DDL check is the right trade.
    """
    global _schema_cache
    cached = _schema_cache
    now = time.monotonic()
    if cached is not None and now - cached[0] < _SCHEMA_TTL_SECONDS:
        return cached[1]
    present = bool(
        _query(
            """
            SELECT count() FROM system.columns
            WHERE database = currentDatabase() AND table = 'orders_current'
              AND name = 'discount_code'
            """
        ).result_rows[0][0]
    )
    with _schema_lock:
        _schema_cache = (now, present)
    return present


def _order_select() -> str:
    """Keep the API online through the downstream-first ADD COLUMN."""
    has_discount = _has_discount_column()
    discount = (
        "discount_code"
        if has_discount
        else "CAST(NULL AS Nullable(String)) AS discount_code"
    )
    # ingested_at closes the provenance chain: with source_ts and ingested_at in
    # the same row, a reader can compute this order's own transit time without
    # trusting any dashboard.
    return f"""
    SELECT order_id, customer_id, amount, status, {discount},
           created_at, updated_at, version, source_topic,
           kafka_partition, kafka_offset, source_ts, ingested_at
    FROM orders_current FINAL
    """


def _order(row: tuple[Any, ...]) -> dict[str, Any]:
    keys = (
        "order_id", "customer_id", "amount", "status", "discount_code",
        "created_at", "updated_at", "version", "source_topic",
        "kafka_partition", "kafka_offset", "source_ts", "ingested_at",
    )
    return {key: _json(value) for key, value in zip(keys, row, strict=True)}


@app.get("/api/orders")
async def list_orders(
    limit: int = Query(100, ge=1, le=1000),
    status: str | None = None,
    customer_id: int | None = Query(None, ge=1),
) -> dict[str, Any]:
    clauses = ["_is_deleted = 0"]
    params: dict[str, Any] = {"limit": limit}
    if status is not None:
        clauses.append("status = {status:String}")
        params["status"] = status
    if customer_id is not None:
        clauses.append("customer_id = {customer_id:UInt32}")
        params["customer_id"] = customer_id
    def fetch():
        sql = _order_select() + " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY updated_at DESC, order_id DESC LIMIT {limit:UInt16}"
        return _query(sql, params)

    result = await asyncio.to_thread(fetch)
    return {"rows": [_order(row) for row in result.result_rows]}


@app.get("/api/orders/{order_id}")
async def get_order(order_id: int) -> dict[str, Any]:
    def fetch():
        # _order_select() may itself hit ClickHouse, so it belongs inside the
        # worker thread; evaluating it as an argument would block the event loop.
        return _query(
            _order_select() + " WHERE order_id = {id:UInt64} AND _is_deleted = 0 LIMIT 1",
            {"id": order_id},
        )

    result = await asyncio.to_thread(fetch)
    if not result.result_rows:
        raise HTTPException(status_code=404, detail="order not found")
    return _order(result.result_rows[0])


@app.get("/api/summary")
async def summary() -> dict[str, Any]:
    result = await asyncio.to_thread(
        _query,
        """
        SELECT status, count() AS orders, sum(amount) AS amount
        FROM orders_current FINAL
        WHERE _is_deleted = 0
        GROUP BY status ORDER BY status
        """,
    )
    rows = [
        {"status": row[0], "orders": row[1], "amount": _json(row[2])}
        for row in result.result_rows
    ]
    return {"orders": sum(row["orders"] for row in rows), "by_status": rows}


def _refresh_metrics() -> None:
    global _metrics_refreshed_at
    now = time.monotonic()
    if now - _metrics_refreshed_at < 5:
        return
    with _metrics_lock:
        if time.monotonic() - _metrics_refreshed_at < 5:
            return
        # max() over an empty table returns the type default (1970), not NULL,
        # so ifNull() would never fire and an empty sink would report ~56 years
        # of staleness. Count the rows and decide explicitly.
        visible, rows, staleness = _query(
            """
            SELECT countIf(_is_deleted = 0),
                   count(),
                   dateDiff('millisecond', max(source_ts), now64(3)) / 1000
            FROM orders_current FINAL
            """
        ).result_rows[0]
        VISIBLE_ORDERS.set(visible)
        DATA_FRESHNESS.set(float(staleness) if rows else -1.0)

        # Transit is measured over raw writes, not current state: every version
        # of a row crossed the pipeline, and a recent window keeps this a gauge
        # rather than a lifetime maximum that can only ever climb.
        written, transit = _query(
            """
            SELECT count(), max(dateDiff('millisecond', source_ts, ingested_at)) / 1000
            FROM orders_current
            WHERE ingested_at > now64(3) - toIntervalMinute(5)
            """
        ).result_rows[0]
        PIPELINE_TRANSIT.set(float(transit) if written else -1.0)
        _metrics_refreshed_at = time.monotonic()


@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    # Keep process/request metrics scrapeable while ClickHouse is down.
    with suppress(Exception):
        await asyncio.to_thread(_refresh_metrics)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
