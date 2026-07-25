# Lesson 10 — Real-time OLAP: serving the results

This lesson closes the loop from Lessons 6-9. We have a Kafka stream; now we ask **where the analytical results live** and **how fast we can serve them** while data is still flowing in.

We deploy ClickHouse as a real-time OLAP engine, ingest the Kafka stream directly through ClickHouse's Kafka engine + materialized views, and expose sub-second aggregations through FastAPI. Then we measure the tradeoffs: ingestion latency vs. query latency, pre-aggregation vs. raw scan, and ClickHouse vs. batch-loaded DuckDB.

## Architecture

```
┌─────────────┐      ┌─────────────┐      ┌──────────────────────────────┐
│  producer   │──────▶│    Kafka    │──────▶│  ClickHouse                  │
│  (Python)   │      │  (KRaft)    │      │  · events (MergeTree raw)    │
└─────────────┘      └─────────────┘      │  · revenue_by_region_minute  │
                                          │    (AggregatingMergeTree MV) │
                                          └──────────────┬───────────────┘
                                                         │
                              ┌──────────────────────────┘
                              ▼
                       ┌──────────────┐
                       │   FastAPI    │
                       │  /api/...    │
                       └──────────────┘
```

## Prerequisites

- Python 3.13+ managed by `uv`
- Docker and Docker Compose
- ~4 GB free RAM for Kafka + ClickHouse

## Quick start

1. Start the stack:
   ```bash
   docker compose up -d
   ```

2. Create the Kafka topic:
   ```bash
   uv run python src/setup_topics.py --reset
   ```

3. Initialize ClickHouse tables and materialized views:
   ```bash
   uv run python src/setup_clickhouse.py
   ```

4. Seed history so the storage demos have something to work with:
   ```bash
   uv run python src/seed_bulk.py
   ```

   This generates 20M rows **inside ClickHouse** (~4 seconds) spread over the
   last 4 hours. The Kafka producer is the real ingestion path and is what you
   measure freshness with, but at a few hundred events/s it cannot build a
   table big enough for granule skipping or compression to be visible — 9 000
   rows is two granules, and there is nothing to skip.

5. Start producing events in one terminal:
   ```bash
   uv run python src/producer.py --rate 1000 --duration-seconds 600
   ```

6. In another terminal, start the FastAPI dashboard:
   ```bash
   uv run uvicorn src.app:app --reload --host 0.0.0.0 --port 8000
   ```

7. Query it:
   ```bash
   curl "http://localhost:8000/api/revenue?minutes=10"
   curl "http://localhost:8000/api/revenue?minutes=10&unique_users=true"
   curl "http://localhost:8000/api/throughput?minutes=10"
   curl "http://localhost:8000/api/top-users?minutes=5&limit=10"
   ```

## Hour 3 experiments

### Ingestion latency

Send canary events and measure how long until they are queryable:

```bash
uv run python src/measure_freshness.py --runs 10
```

Measured on a laptop, this depends entirely on whether the producer is running:

| Topic state | p50 | p95 |
|---|---|---|
| idle (canaries only) | ~1.5 s | ~1.6 s |
| under a 1000/s producer | 0.05 s | 1.06 s |

That 30x gap is the lesson, and `demo_ingest_floor.py` isolates why:

```bash
uv run python src/demo_ingest_floor.py
```

It sweeps both knobs. `kafka_max_block_size` is **flat across a 1000x range**
— one canary never fills a block, so the block size cannot be what ends the
batch. `kafka_flush_interval_ms` is the floor: 2000 ms → 1.65 s, 100 ms →
0.11 s. Block size only binds once the arrival rate fills a block before the
timer fires, which is exactly what happens under load.

### What compresses

```bash
uv run python src/demo_compression.py
```

| Column | Uncompressed | Compressed | Ratio |
|---|---|---|---|
| `event_type` (LowCardinality) | 19.1 MiB | 91.7 KiB | 213.5x |
| `region` (LowCardinality) | 19.1 MiB | 91.9 KiB | 212.9x |
| `amount` (Decimal) | 152.6 MiB | 63.5 MiB | 2.4x |
| `event_time` (DateTime64) | 152.6 MiB | 68.1 MiB | 2.2x |
| `user_id` (UInt32) | 76.3 MiB | 75.8 MiB | 1.0x |

Compression follows the shape of the data in storage order, not the declared
type. Random `user_id`s do not compress at all.

### Duplicates, updates and deletes

```bash
uv run python src/demo_dedup_replacing.py
```

A replayed Kafka block doubles your revenue silently (3 rows / 400.00 → 6 rows
/ 800.00). `insert_deduplication_token` fixes the exact-replay case only;
`ReplacingMergeTree` handles the general case, which is what a Lesson 5 CDC
stream actually carries.

### Projections: the escape hatch that exists

```bash
uv run python src/demo_projection.py
```

A projection gives `events` a second sort order: 2 446 marks → 2 for a
`user_id` filter, for +80% storage.

### Query latency under concurrent ingestion

Hammer the API while events are streaming in:

```bash
uv run python src/query_loadtest.py --endpoint "http://localhost:8000/api/revenue?minutes=5" --clients 10
uv run python src/demo_query_pressure.py --endpoint "http://localhost:8000/api/throughput?minutes=2" --rate 1000 --clients 8
```

### ORDER BY matters

See how the primary index skips granules for different query patterns:

```bash
uv run python src/experiment_order_by.py
uv run python src/demo_order_by_granules.py
```

| Query | Table | Granules | Latency |
|---|---|---|---|
| time range, last 5 min | `events` | 71 / 2 446 | 6.2 ms |
| `user_id = 42` (bad match) | `events` | 2 446 / 2 446 | 14.1 ms |
| `user_id = 42` (good match) | `events_by_user` | **1 / 49** | **1.9 ms** |

Same filter, same data, 2 446 granules read versus 1.

### ClickHouse vs. DuckDB

Export the last 10 minutes to Parquet and compare query latency:

```bash
uv run python src/compare_duckdb.py
```

## Expected results on a laptop

Measured over 15 requests each, against 20M seeded rows (~830 000 in a
10-minute window), with the producer inserting continuously:

| Endpoint | Query type | p50 query_ms |
|---|---|---|
| `/api/revenue` | pre-aggregated MV, additive only | **4.2 ms** |
| `/api/revenue?unique_users=true` | + count-distinct from a second MV | **37.3 ms** |
| `/api/throughput` | raw scan, narrow time range | **19.7 ms** |
| `/api/top-users` | raw scan, high-cardinality GROUP BY | **8.9 ms** |

Two things worth noticing. Adding one count-distinct column costs ~9x: `sum`
and `count` collapse to a number per group, `uniq` has to carry a sketch, so
they live in separate materialized views and you can measure the difference by
flipping one query parameter.

And `/api/top-users` beats `/api/throughput` despite the high-cardinality
GROUP BY, because it filters on `event_type` — the first column of the
`ORDER BY` — and prunes granules (51 marks vs 407), while `throughput` filters
only on time and cannot.

All of them return while the producer is inserting — that is the point.

## Writing your analysis

The take-home deliverable should include:

1. **Measured ingestion latency** — distribution from `measure_freshness.py` and how it changed when you tuned `kafka_max_block_size`.
2. **Query latency under load** — p50/p95/p99 from the HTTP load test.
3. **ORDER BY experiment** — granules scanned in each table and the timing difference.
4. **ClickHouse vs. DuckDB** — when each is the right choice for this workload, backed by the numbers you measured.

## Common pitfalls

- **Port 19092 conflict**: if the Lesson 9 Kafka container (`kafka-l9`) is still running, stop it first (`docker stop kafka-l9`) because this project also exposes Kafka on `localhost:19092`.
- **ClickHouse not reachable yet**: `setup_clickhouse.py` waits for the server, but if you run other scripts first they may fail until ClickHouse is healthy.
- **Empty dashboard**: the Kafka engine is a consumer; it reads from the topic offset it was created at. If you produced events *before* creating the tables, run `src/setup_topics.py --reset` and re-create the ClickHouse tables (`setup_clickhouse.py --reset`) to start from a clean stream.
- **Stale data across runs**: Kafka retains topic data by default. Always run `src/setup_topics.py --reset` and `src/setup_clickhouse.py --reset` between independent measurements.
- **Seeded history ages out**: `seed_bulk.py` spreads rows over the last 4 hours by default. If a session runs longer than that, `WHERE event_time >= now() - INTERVAL 10 MINUTE` starts hitting an almost-empty range and every query looks suspiciously fast. Re-seed, or pass a wider `--minutes`.
- **`kafka_auto_offset_reset` is not a table setting.** `auto.offset.reset` is a librdkafka property and belongs in `config/clickhouse-kafka.xml`. Putting it in a `CREATE TABLE ... ENGINE = Kafka()` raises `UNKNOWN_SETTING`, which aborts the rest of `init_clickhouse.sql` and leaves the schema half-created.
- **Drop Kafka-engine tables before deleting their topic.** A Kafka-engine table is a live consumer; deleting a topic underneath one blocks. `demo_ingest_floor.py` uses a per-run topic to avoid the problem entirely.
- **`AdminClient` futures are unreliable here.** `create_topics(...)`/`delete_topics(...)` can complete on the broker while the returned future never resolves, so `future.result()` hangs forever. Confirm against `list_topics()` metadata instead — `config.await_topic()` does this.
