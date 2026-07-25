-- Lesson 10 — ClickHouse schema for real-time OLAP over Kafka.
--
-- Run this once with:
--   uv run python src/setup_clickhouse.py

-- ── Raw event store ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS events (
    event_time DateTime64(3),
    user_id UInt32,
    event_type LowCardinality(String),
    amount Decimal(10, 2),
    region LowCardinality(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (event_type, region, event_time)
SETTINGS index_granularity = 8192;

-- ── Kafka bridge ────────────────────────────────────────────────────────────
-- This table is a consumer, not a real table you query directly.
CREATE TABLE IF NOT EXISTS events_kafka (
    event_time String,
    user_id UInt32,
    event_type String,
    amount Decimal(10, 2),
    region String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'events',
    kafka_group_name = 'clickhouse-consumer-l10',
    kafka_format = 'JSONEachRow',
    -- These three set the freshness/query tradeoff measured in Part two.
    kafka_max_block_size = 1000,
    kafka_poll_timeout_ms = 100,
    kafka_flush_interval_ms = 1000;
-- auto.offset.reset is a librdkafka property, not a ClickHouse table setting:
-- it lives in config/clickhouse-kafka.xml. Writing `kafka_auto_offset_reset`
-- here raises UNKNOWN_SETTING and silently skips every statement below.

-- ── Materialized view: Kafka -> events ──────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS events_kafka_mv TO events AS
SELECT
    parseDateTime64BestEffort(event_time) AS event_time,
    user_id,
    event_type,
    amount,
    region
FROM events_kafka;

-- ── Pre-aggregated dashboard table: additive aggregates only ────────────────
-- Stores partial aggregates. Query with -Merge combinators.
--
-- Note what is NOT here: count-distinct. sum and count collapse to a single
-- number per group, so merging them at read time is a handful of additions.
-- Count-distinct cannot collapse -- it has to carry a sketch big enough to
-- recognise values it has already seen. Keeping it in this table made the
-- "fast" dashboard query ~17x slower than the two additive aggregates alone.
-- It lives in its own table below so the cost is measurable instead of hidden.
CREATE TABLE IF NOT EXISTS revenue_by_region_minute (
    minute DateTime,
    region LowCardinality(String),
    total_revenue AggregateFunction(sum, Decimal(10, 2)),
    purchase_count AggregateFunction(count)
) ENGINE = AggregatingMergeTree()
ORDER BY (region, minute);

CREATE MATERIALIZED VIEW IF NOT EXISTS revenue_by_region_minute_mv
TO revenue_by_region_minute AS
SELECT
    toStartOfMinute(event_time) AS minute,
    region,
    sumState(amount) AS total_revenue,
    countState() AS purchase_count
FROM events
WHERE event_type = 'purchase'
GROUP BY minute, region;

-- ── Count-distinct, kept separate on purpose ────────────────────────────────
-- Same grain, same source, one aggregate. Query /api/revenue with and without
-- ?unique_users=true to measure what this one column costs.
CREATE TABLE IF NOT EXISTS unique_users_by_region_minute (
    minute DateTime,
    region LowCardinality(String),
    unique_users AggregateFunction(uniq, UInt32)
) ENGINE = AggregatingMergeTree()
ORDER BY (region, minute);

CREATE MATERIALIZED VIEW IF NOT EXISTS unique_users_by_region_minute_mv
TO unique_users_by_region_minute AS
SELECT
    toStartOfMinute(event_time) AS minute,
    region,
    uniqState(user_id) AS unique_users
FROM events
WHERE event_type = 'purchase'
GROUP BY minute, region;
