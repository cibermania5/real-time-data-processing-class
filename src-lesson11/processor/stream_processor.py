"""Decode Debezium Avro by wire schema ID and materialize current orders.

This is a real Structured Streaming query. Spark checkpoints its Kafka source
offsets and query progress at /checkpoints/orders; those files are *not* Kafka
consumer-group commits. The ClickHouse sink is at-least-once across a crash, so
ReplacingMergeTree(version) makes retries converge using the source WAL LSN.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Iterable
from contextlib import suppress
from functools import lru_cache
from pathlib import Path
from typing import Any

import requests
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import (
    DataType,
    IntegerType,
    NumericType,
    StringType,
    StructType,
    TimestampType,
)

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "cdc.public.orders")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:18081").rstrip("/")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "18123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "lesson11")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "lesson11")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "lesson11")
CLICKHOUSE_TABLE = os.getenv("CLICKHOUSE_TABLE", "orders_current")
METRICS_PORT = int(os.getenv("METRICS_PORT", os.getenv("PROCESSOR_METRICS_PORT", "9108")))
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "5 seconds")
SINK_DELAY_SECONDS = float(os.getenv("SINK_DELAY_SECONDS", "0"))
# Bounded batches. Without this the Kafka source claims every available offset
# in a single micro-batch, so a backlog hides *inside* one slow batch instead of
# appearing as offset-head distance -- lag would read 0 during the backpressure
# exercise. Bounding the batch is also what you want in production: it caps
# memory and makes recovery rate measurable. Set empty to disable.
MAX_OFFSETS_PER_TRIGGER = os.getenv("MAX_OFFSETS_PER_TRIGGER", "1000").strip()

_container_checkpoint = Path("/checkpoints")
CHECKPOINT_DIR = Path(
    os.getenv(
        "CHECKPOINT_PATH",
        os.getenv(
            "CHECKPOINT_DIR",
        str(_container_checkpoint / "orders")
        if _container_checkpoint.exists()
        else str(Path(__file__).resolve().parents[1] / "checkpoints" / "orders"),
        ),
    )
)

KAFKA_LAG = Gauge(
    "pipeline_kafka_lag_records",
    "Spark Kafka source offsets behind the broker heads (not consumer-group lag)",
)
BATCH_SECONDS = Histogram(
    "pipeline_batch_duration_seconds",
    "Structured Streaming foreachBatch duration",
    # The default prometheus_client buckets stop at 10s. The backpressure
    # exercise deliberately pushes batches past that, and histogram_quantile
    # can never report a value above the highest finite bucket -- the panel and
    # the alert would both flatline at exactly the number we want to exceed.
    buckets=(0.1, 0.25, 0.5, 1, 2.5, 5, 7.5, 10, 15, 20, 30, 45, 60, 120, float("inf")),
)
INPUT_ROWS = Counter(
    "pipeline_input_rows_total", "Kafka records presented to foreachBatch, including retries"
)
END_TO_END_SECONDS = Gauge(
    "pipeline_end_to_end_latency_seconds",
    "Age of the oldest source change in the last batch, measured after the sink write",
)
LAST_BATCH = Gauge("pipeline_last_successful_batch_id", "Last completed Spark batch ID")
SCHEMA_IDS = Gauge(
    "pipeline_schema_ids_in_batch", "Distinct Confluent value schema IDs in the last batch"
)


def _partition_offsets(raw: Any) -> dict[tuple[str, str], int] | None:
    """Parse Spark's {"topic":{"0":123,...}} offset JSON into a flat mapping."""
    if not raw:
        return None
    parsed = raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except ValueError:
            return None
    if not isinstance(parsed, dict):
        return None
    offsets: dict[tuple[str, str], int] = {}
    for topic, partitions in parsed.items():
        if isinstance(partitions, dict):
            for partition, offset in partitions.items():
                with suppress(TypeError, ValueError):
                    offsets[(topic, str(partition))] = int(offset)
    return offsets or None


def offset_head_distance(latest: Any, end: Any) -> float | None:
    """Records the broker has that this query has not claimed yet."""
    head = _partition_offsets(latest)
    consumed = _partition_offsets(end)
    if not head or not consumed:
        return None
    return float(sum(max(0, offset - consumed.get(key, offset)) for key, offset in head.items()))


class ProgressMetrics(StreamingQueryListener):
    """Export the broker-head distance Spark reports in query progress."""

    def onQueryStarted(self, event) -> None:  # noqa: N802
        pass

    def onQueryProgress(self, event) -> None:  # noqa: N802
        try:
            progress = json.loads(event.progress.json)
        except (AttributeError, TypeError, ValueError):
            return
        lags: list[float] = []
        for source in progress.get("sources", []):
            metrics = source.get("metrics") or {}
            reported = next(
                (metrics[name] for name in ("maxOffsetsBehindLatest", "numOffsetsBehindLatest")
                 if name in metrics),
                None,
            )
            if reported is not None:
                with suppress(TypeError, ValueError):
                    lags.append(float(reported))
                    continue
            # Not every Spark build reports those metrics. head - claimed is
            # the same quantity and is always present in the progress record.
            distance = offset_head_distance(source.get("latestOffset"), source.get("endOffset"))
            if distance is not None:
                lags.append(distance)
        if lags:
            KAFKA_LAG.set(max(lags))

    def onQueryIdle(self, event) -> None:  # noqa: N802
        pass

    def onQueryTerminated(self, event) -> None:  # noqa: N802
        pass


def confluent_schema_id(value: bytes | None) -> int | None:
    """Read magic-byte + big-endian schema ID without assuming one schema."""
    if value is None or len(value) < 5 or value[0] != 0:
        return None
    return int.from_bytes(value[1:5], byteorder="big", signed=False)


@lru_cache(maxsize=128)
def fetch_avro_schema(schema_id: int) -> str:
    response = requests.get(f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}", timeout=10)
    response.raise_for_status()
    body = response.json()
    if body.get("schemaType", "AVRO") != "AVRO":
        raise ValueError(f"schema {schema_id} is not Avro: {body.get('schemaType')}")
    # Parse once here so failures mention the registry schema rather than Spark.
    json.loads(body["schema"])
    return body["schema"]


def _field(struct_col, fields: StructType, name: str):
    return struct_col[name] if name in fields.fieldNames() else F.lit(None)


def _field_type(fields: StructType, name: str) -> DataType | None:
    return fields[name].dataType if name in fields.fieldNames() else None


def _timestamp(column, data_type: DataType | None):
    # Debezium PostgreSQL TIMESTAMPTZ normally arrives as a ZonedTimestamp
    # string. Also accept logical Avro timestamps and epoch milliseconds so a
    # converter configuration change is not silently corrupting.
    if isinstance(data_type, TimestampType):
        return column.cast("timestamp")
    if isinstance(data_type, NumericType):
        return (column.cast("double") / F.lit(1000.0)).cast("timestamp")
    if isinstance(data_type, StringType):
        return F.to_timestamp(column)
    return column.cast("timestamp")


def normalize_schema_version(rows: DataFrame, schema_id: int) -> DataFrame:
    """Decode one schema-ID slice and project every version to the sink contract."""
    schema = fetch_avro_schema(schema_id)
    decoded = rows.select(
        from_avro("_payload", schema, {"mode": "FAILFAST"}).alias("event"),
        "topic",
        "partition",
        "offset",
    )

    envelope = decoded.schema["event"].dataType
    if not isinstance(envelope, StructType):
        raise TypeError(f"schema {schema_id} did not decode to a Debezium envelope")
    before_type = envelope["before"].dataType
    after_type = envelope["after"].dataType
    source_type = envelope["source"].dataType
    if not all(isinstance(item, StructType) for item in (before_type, after_type, source_type)):
        raise TypeError(f"schema {schema_id} is missing Debezium before/after/source structs")
    if "lsn" not in source_type.fieldNames():
        raise ValueError(f"schema {schema_id} has no Debezium source.lsn for ordering")

    before = F.col("event.before")
    after = F.col("event.after")

    def current(name: str):
        # A SQL NULL in `after` is a real value, not permission to resurrect the
        # previous value from `before`. Only delete envelopes take their row from
        # `before`; creates, updates and snapshots take it from `after`.
        return F.when(
            F.col("event.op") == "d", _field(before, before_type, name)
        ).otherwise(_field(after, after_type, name))

    # Existing schema versions have no discount_code; they become NULL. A new
    # registry ID is discovered at runtime and starts populating it immediately.
    created_type = _field_type(after_type, "created_at") or _field_type(before_type, "created_at")
    updated_type = _field_type(after_type, "updated_at") or _field_type(before_type, "updated_at")
    source_ts_type = _field_type(source_type, "ts_ms")

    return decoded.select(
        current("id").cast("long").alias("order_id"),
        current("customer_id").cast("int").alias("customer_id"),
        current("amount").cast("decimal(10,2)").alias("amount"),
        current("status").cast("string").alias("status"),
        current("discount_code").cast("string").alias("discount_code"),
        _timestamp(current("created_at"), created_type).alias("created_at"),
        _timestamp(current("updated_at"), updated_type).alias("updated_at"),
        F.col("event.source.lsn").cast("long").alias("version"),
        F.when(F.col("event.op") == "d", 1).otherwise(0).cast("byte").alias("_is_deleted"),
        F.col("topic").alias("source_topic"),
        F.col("partition").cast("short").alias("kafka_partition"),
        F.col("offset").cast("long").alias("kafka_offset"),
        _timestamp(F.col("event.source.ts_ms"), source_ts_type).alias("source_ts"),
    ).filter(F.col("order_id").isNotNull())


SINK_COLUMNS = [
    "order_id", "customer_id", "amount", "status", "discount_code", "created_at",
    "updated_at", "version", "_is_deleted", "source_topic", "kafka_partition",
    "kafka_offset", "source_ts",
]


def clickhouse_client():
    """Executors import this lazily; the driver reuses the same connection shape."""
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


def write_partition(rows: Iterable[Any], sink_columns: list[str]) -> None:
    """Insert one Spark partition over ClickHouse HTTP in bounded chunks."""
    client = clickhouse_client()
    chunk: list[list[Any]] = []
    try:
        for row in rows:
            chunk.append([row[name] for name in sink_columns])
            if len(chunk) >= 5_000:
                client.insert(CLICKHOUSE_TABLE, chunk, column_names=sink_columns)
                chunk.clear()
        if chunk:
            client.insert(CLICKHOUSE_TABLE, chunk, column_names=sink_columns)
    finally:
        client.close()


def process_batch(batch: DataFrame, batch_id: int) -> None:
    started = time.perf_counter()
    try:
        # The batch is scanned once for its schema IDs and again per ID branch.
        # Without persist(), each scan re-reads the same Kafka offset range.
        batch.persist()
        try:
            ids = [row[0] for row in batch.select("_schema_id").distinct().collect()]
            SCHEMA_IDS.set(len(ids))
            if not ids:
                LAST_BATCH.set(batch_id)
                return

            normalized: DataFrame | None = None
            for schema_id in sorted(ids):
                version_rows = batch.filter(F.col("_schema_id") == schema_id)
                current = normalize_schema_version(version_rows, schema_id)
                normalized = current if normalized is None else normalized.unionByName(current)
            if normalized is None:  # unreachable while ids is non-empty
                raise RuntimeError("no schema versions decoded from a non-empty batch")

            normalized.persist()
            try:
                # One pass for every driver-side decision about this batch.
                stats = normalized.agg(
                    F.count("*").alias("rows"),
                    F.min(F.col("source_ts").cast("double")).alias("oldest_source_epoch"),
                    F.count(F.when(F.col("version").isNull(), 1)).alias("missing_version"),
                ).first()
                row_count = int(stats["rows"])
                if stats["missing_version"]:
                    raise RuntimeError(
                        "Debezium event has no source.lsn; incremental snapshots are "
                        "outside this lesson's current-state version contract"
                    )
                INPUT_ROWS.inc(row_count)
                if row_count:
                    if SINK_DELAY_SECONDS > 0:
                        time.sleep(SINK_DELAY_SECONDS)
                    # The downstream-first exercise adds discount_code live. Asking
                    # ClickHouse each batch lets the same processor span both sides
                    # of the migration without a restart.
                    client = clickhouse_client()
                    try:
                        target_names = {
                            row[0]
                            for row in client.query(
                                """
                                SELECT name FROM system.columns
                                WHERE database = currentDatabase() AND table = {table:String}
                                """,
                                parameters={"table": CLICKHOUSE_TABLE},
                            ).result_rows
                        }
                    finally:
                        client.close()
                    sink_columns = [name for name in SINK_COLUMNS if name in target_names]
                    required = set(SINK_COLUMNS) - {"discount_code"}
                    if not required.issubset(target_names):
                        missing = sorted(required - target_names)
                        raise RuntimeError(
                            f"ClickHouse sink is missing required columns: {missing}"
                        )
                    normalized.select(*sink_columns).foreachPartition(
                        lambda rows: write_partition(rows, sink_columns)
                    )
                # Measured *after* the sink write, so a slow sink shows up as
                # latency instead of hiding behind lag. SINK_DELAY_SECONDS is
                # part of the number the backpressure exercise reads.
                if stats["oldest_source_epoch"] is not None:
                    END_TO_END_SECONDS.set(
                        max(0.0, time.time() - float(stats["oldest_source_epoch"]))
                    )
            finally:
                normalized.unpersist()
            LAST_BATCH.set(batch_id)
        finally:
            batch.unpersist()
    finally:
        BATCH_SECONDS.observe(time.perf_counter() - started)


def build_spark() -> SparkSession:
    # Jar resolution happens before the JVM exists. In the container this file
    # is launched by spark-submit, which has already read spark.jars.packages
    # from spark/spark-defaults.conf -- setting it here changes nothing there.
    # It only takes effect when the script is run directly with `python`, which
    # starts its own JVM. Change spark-defaults.conf, not SPARK_PACKAGES, if you
    # need different jars in Compose.
    packages = os.getenv(
        "SPARK_PACKAGES",
        ",".join(
            [
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1",
                "org.apache.spark:spark-avro_2.13:4.0.1",
            ]
        ),
    )
    builder = (
        SparkSession.builder.appName("lesson11-orders-cdc")
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "1g"))
    )
    if packages:
        builder = builder.config("spark.jars.packages", packages)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


def main() -> None:
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    start_http_server(METRICS_PORT)
    spark = build_spark()
    spark.streams.addListener(ProgressMetrics())

    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", os.getenv("STARTING_OFFSETS", "earliest"))
        .option("failOnDataLoss", "true")
    )
    if MAX_OFFSETS_PER_TRIGGER:
        reader = reader.option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)

    raw = (
        reader.load()
        .filter(F.col("value").isNotNull())  # Debezium delete tombstones have no envelope.
        .withColumn("_schema_id", F.udf(confluent_schema_id, IntegerType())("value"))
        .filter(F.col("_schema_id").isNotNull())
        .withColumn("_payload", F.expr("substring(value, 6, length(value) - 5)"))
        .select("topic", "partition", "offset", "_schema_id", "_payload")
    )

    print(
        f"streaming {KAFKA_TOPIC} ({KAFKA_BOOTSTRAP}) -> "
        f"{CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}; checkpoint={CHECKPOINT_DIR}",
        flush=True,
    )
    query = (
        raw.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", str(CHECKPOINT_DIR))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName("lesson11-orders-cdc")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
