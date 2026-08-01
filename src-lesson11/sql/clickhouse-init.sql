-- Both sides initially omit discount_code. scripts/migrate_discount_code.py
-- performs the downstream ADD COLUMN first, proves old-schema events still flow,
-- and only then expands Postgres to make Debezium publish a new Avro schema.

CREATE DATABASE IF NOT EXISTS lesson11;

CREATE TABLE IF NOT EXISTS lesson11.orders_current (
    order_id        UInt64,
    customer_id     UInt32,
    amount          Decimal(10, 2),
    status          LowCardinality(String),
    created_at      DateTime64(3, 'UTC'),
    updated_at      DateTime64(3, 'UTC'),
    version         UInt64,
    _is_deleted     UInt8,
    source_topic    LowCardinality(String),
    kafka_partition UInt16,
    kafka_offset    UInt64,
    source_ts       DateTime64(3, 'UTC'),
    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY modulo(order_id, 16)
ORDER BY order_id;

-- ReplacingMergeTree converges during merges; correctness-sensitive reads use
-- FINAL and then discard the winning delete marker. The API does exactly this.
