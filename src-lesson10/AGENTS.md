# AI assistance context for src-lesson10

## What this code does

Lesson 10 teaches real-time OLAP serving. ClickHouse ingests a Kafka topic directly
via the Kafka engine + materialized views, stores raw events in a `MergeTree`, and
maintains a pre-aggregated `AggregatingMergeTree` revenue table. A FastAPI
backend queries both tables and returns sub-second aggregations. Students measure
ingestion latency, query latency under concurrent load, the effect of the
`ORDER BY` key, and compare ClickHouse against batch-loaded DuckDB.

## Project layout

- `docker-compose.yml` — single-node Kafka (KRaft) + single ClickHouse server.
- `config/clickhouse-kafka.xml` — librdkafka properties (`auto.offset.reset`),
  mounted into `/etc/clickhouse-server/config.d/`. These cannot go in table DDL.
- `pyproject.toml` — uv project with `clickhouse-connect`, `fastapi`, `uvicorn`,
  `confluent-kafka`, `httpx`, `duckdb`, `pandas`, `numpy`, `matplotlib`.
- `sql/init_clickhouse.sql` — table and materialized view definitions.
- `src/config.py` — shared constants and helpers (`BOOTSTRAP`, `EVENTS_TOPIC`,
  ClickHouse connection, `get_ch_client()`, `wait_for_clickhouse()`).
- `src/setup_topics.py` — create / reset the `events` Kafka topic.
- `src/setup_clickhouse.py` — run `sql/init_clickhouse.sql`, optionally `--reset`.
- `src/producer.py` — controlled-rate e-commerce event generator. Paces in
  batches against a wall-clock deadline; a per-event sleep cannot hold rates
  above a few hundred/s. Verified accurate to 100% of target at 500/1000/5000.
- `src/seed_bulk.py` — generate history inside ClickHouse via `numbers_mt`
  (20M rows in ~4s). Required before any storage demo; see below.
- `src/app.py` — FastAPI with three endpoints (`/api/revenue`, `/api/throughput`,
  `/api/top-users`). Fully implemented; students should read and tweak queries.
- `src/measure_freshness.py` — canary-event ingestion latency distribution.
- `src/query_loadtest.py` — concurrent HTTP load test.
- `src/compare_duckdb.py` — export recent events to Parquet, query DuckDB vs.
  ClickHouse raw table.
- `src/experiment_order_by.py` — compare granule skipping and latency for two
  sort orders.
- `src/demo_ingest_floor.py` — micro-demo isolating `kafka_max_block_size`.
- `src/demo_query_pressure.py` — micro-demo isolating query latency under load.
- `src/demo_order_by_granules.py` — micro-demo isolating granule skipping.
- `src/demo_compression.py` — per-column compression ratios from
  `system.parts_columns`.
- `src/demo_dedup_replacing.py` — replayed blocks, `insert_deduplication_token`,
  and `ReplacingMergeTree` for CDC updates/deletes.
- `src/demo_projection.py` — `ADD PROJECTION` as a second sort order, and its
  storage cost.

## Conventions

- Kafka bootstrap from the host: `localhost:19092`. Inside Docker the broker is
  `kafka:9092` (service name).
- ClickHouse HTTP interface: `localhost:8123`.
- The Kafka engine table `events_kafka` is a consumer, not a query target.
- The materialized view `events_kafka_mv` writes parsed JSON events into
  `events` automatically.
- Query pre-aggregated results with `-Merge` combinators, e.g.
  `sumMerge(total_revenue)` and `countMerge(purchase_count)`.
- `revenue_by_region_minute` uses `AggregatingMergeTree` and stores partial
  aggregate states. It is correct even when multiple insert batches contribute
  rows for the same minute/region.
- **Count-distinct is deliberately in its own table.** `revenue_by_region_minute`
  holds only `sum`/`count`; `unique_users_by_region_minute` holds the `uniq`
  state. Keeping `uniqState` in the first table made the "fast" dashboard query
  ~9x slower (4.2 ms → 37.3 ms), because `sum`/`count` merge to a number while
  `uniq` merges sketches. `/api/revenue?unique_users=true` joins the second
  table so students can measure the difference. Do not merge them back.

## Hard constraints

- This stack must have the Kafka broker available on `localhost:19092`. If the
  Lesson 9 container `kafka-l9` is running, it will conflict on that port.
- ClickHouse must be healthy before `setup_clickhouse.py` returns. Other scripts
  that depend on it should wait; use `wait_for_clickhouse()` if writing a new
  entry point.
- Kafka topic data is retained across runs. Always reset topics and ClickHouse
  tables between independent measurements:
  ```bash
  uv run python src/setup_topics.py --reset
  uv run python src/setup_clickhouse.py --reset
  ```
- The `events_kafka` consumer starts from the latest offsets by default. If you
  produced data before creating it, recreate the tables after resetting topics.
- `parseDateTime64BestEffort` in the MV expects an ISO-8601 string. The producer
  writes `datetime.now(timezone.utc).isoformat()`, which matches.
- **`kafka_auto_offset_reset` is not a valid table setting.** It raises
  `UNKNOWN_SETTING`, which aborts every remaining statement in
  `init_clickhouse.sql` and leaves the schema half-created — with the older
  tables still present from the Docker volume, so it looks like it worked.
  `auto.offset.reset` belongs in `config/clickhouse-kafka.xml`.
- **Confluent `AdminClient` futures do not resolve reliably in this
  environment** (Python 3.14). `create_topics`/`delete_topics` complete on the
  broker while `future.result()` blocks forever. Use `config.await_topic()`,
  which polls `list_topics()` metadata instead.
- **Drop a Kafka-engine table before deleting the topic it consumes**, or the
  delete blocks with no output.
- **Storage demos need `seed_bulk.py` first.** At Kafka-producer rates the table
  is a couple of granules and granule skipping, compression and projections all
  measure nothing. Seeded rows span `--minutes` (default 240) — if a session
  outlives that window, recent-time queries hit an empty range.

## How to verify

1. `docker compose up -d`
2. `uv run python src/setup_topics.py --reset`
3. `uv run python src/setup_clickhouse.py --reset` — must print all 8
   `running:` lines and `ClickHouse schema is ready.` If it stops early, a
   statement was rejected and the schema is half-created.
4. `uv run python src/seed_bulk.py` — ~20M rows, ~2 440 granules, ~4 seconds.
5. `uv run python src/producer.py --rate 1000 --duration-seconds 120 &`
6. `uv run uvicorn src.app:app --host 0.0.0.0 --port 8000 &`
7. Wait ~10 seconds, then:
   ```bash
   curl "http://localhost:8000/api/revenue?minutes=10"
   curl "http://localhost:8000/api/revenue?minutes=10&unique_users=true"
   curl "http://localhost:8000/api/throughput?minutes=10"
   curl "http://localhost:8000/api/top-users?minutes=5&limit=5"
   ```
   All return JSON with `query_ms` and non-empty `rows`. Expect roughly
   4 / 37 / 20 / 9 ms — the `unique_users=true` variant being ~9x the plain one
   is the intended result, not a regression.
8. `uv run python src/measure_freshness.py --runs 10` — p50 ~0.05s while the
   producer runs, ~1.5s on an idle topic. Both are correct; the gap is the point.
9. `uv run python src/query_loadtest.py --endpoint "http://localhost:8000/api/revenue?minutes=10" --clients 10` should report p50 < 20 ms.
10. Each `demo_*.py` runs standalone and cleans up after itself.

## Extending the query layer

If adding a new endpoint to `app.py`, follow the existing pattern:

- Use ClickHouse typed parameters, e.g. `{minutes:UInt16}` and pass a dict to
  `client.query(query, parameters={...})`.
- Convert `Decimal` and `datetime` values with `_serialize()` before returning
  JSON.
- Return both `query_ms` and `rows` so the dashboard and load tests can measure
  both round-trip and ClickHouse-side latency.
