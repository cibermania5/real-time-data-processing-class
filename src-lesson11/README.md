# Lesson 11 — the end-to-end pipeline

This is the reference system the first ten lessons were building toward:

```text
Postgres → Debezium → Kafka + Schema Registry → Spark → ClickHouse → FastAPI
                                                               │
                                           Prometheus ←─────────┘
                                               │
                                            Grafana
```

The lesson is about the boundaries, not the logos. It proves how an order moves
through seven independently committed systems, how replay becomes harmless current
state, how two Avro schema versions coexist, and how lag becomes user-visible
staleness.

The detailed three-hour teaching sequence is in [LESSON_PLAN.md](LESSON_PLAN.md).

## Requirements

- Docker with at least 8 GB available (close the stacks from earlier lessons)
- `uv` for the host-side exercise scripts
- arm64 or amd64; every image used here is multi-architecture

The first cold start is large: Confluent Connect/Schema Registry and Spark need
several gigabytes of image layers, and Spark downloads its Kafka/Avro JARs once.
Subsequent starts reuse Docker layers and the `l11-spark-ivy` volume.

## Start

From this directory:

```bash
docker compose up -d --build
docker compose ps
uv sync
uv run python scripts/preflight.py
```

`connector-init` is a one-shot container. `Exited (0)` is its healthy final state;
it registered the Debezium connector with an idempotent `PUT`.

On the first run, wait roughly two minutes for Spark to resolve its Maven packages
and start the streaming query. Follow it with:

```bash
docker compose logs -f stream-processor
```

## Prove the golden path

Generate OLTP changes. Every write goes to Postgres; there is no shortcut producer
to Kafka.

```bash
uv run python scripts/load_generator.py --rate 10 --duration 30
uv run python scripts/verify.py
```

`verify.py` creates one stable canary and proves insert, update, nullable-update
(after migration), and delete semantics through the versioned ClickHouse state and
the HTTP API. This remains deterministic while the ordinary load generator runs.

Useful inspection points:

```bash
# Source
docker compose exec postgres psql -U lesson11 -d orders -c 'TABLE orders'

# Connector and registered Avro subjects
curl -s http://localhost:18083/connectors/orders-cdc/status
curl -s http://localhost:18081/subjects

# Sink and API
docker compose exec clickhouse clickhouse-client \
  --user lesson11 --password lesson11 \
  --query 'SELECT * FROM lesson11.orders_current FINAL ORDER BY order_id LIMIT 10'
curl -s http://localhost:18000/api/orders?limit=10
```

Every row the API returns carries its own provenance, so a student can verify the
whole chain from one response body instead of trusting a dashboard:

```json
{
  "order_id": 10494, "amount": 111.11, "discount_code": "MAGNUM11",
  "version": 34317200,                      // Debezium source LSN  (L1/L5)
  "source_topic": "cdc.public.orders",      // event identity       (L6)
  "kafka_partition": 1, "kafka_offset": 5314,
  "source_ts":   "2026-08-01T04:03:46.551", // when Postgres changed
  "ingested_at": "2026-08-01T04:03:50.855"  // when ClickHouse could answer
}
```

`ingested_at − source_ts` is this order's own transit time — 4.304s in that
sample. That is the number the freshness panel aggregates.

## Run the schema-evolution centerpiece

Keep ordinary load flowing in another terminal, then run:

```bash
uv run python scripts/load_generator.py --rate 10 --duration 0
uv run python scripts/migrate_discount_code.py
uv run python scripts/verify.py
```

The migration is deliberately ordered:

1. add `discount_code Nullable(String)` to ClickHouse;
2. prove an old-schema CDC record still reaches the sink;
3. add `discount_code TEXT` to Postgres;
4. let Debezium register the new Avro writer schema;
5. prove Spark discovers the new schema ID without restarting;
6. prove `MAGNUM11` reaches ClickHouse and the API.

This is the additive, expand-first half of expand/contract. The Spark processor reads
the Confluent magic byte and schema ID on every record, fetches each writer schema,
and normalizes mixed versions. It never assumes that “latest schema” can decode the
whole retained topic.

Check the versions:

```bash
curl -s http://localhost:18081/subjects/cdc.public.orders-value/versions
```

## Observe it

| Surface | URL | Credentials |
|---|---|---|
| Orders API | http://localhost:18000/docs | none |
| Spark UI | http://localhost:14040 | none |
| Processor metrics | http://localhost:19108/metrics | none |
| Prometheus | http://localhost:19090 | none |
| Grafana | http://localhost:13000 | `admin` / `lesson11` |

The dashboard is laid out as the lesson's three metrics, in order:

1. **Lag** — how far Spark is from the Kafka head. How much work is waiting.
2. **Throughput** — arrival vs processed. Arrival is derived as
   `processed + d(lag)/dt`, so you can see the two lines cross.
3. **Freshness** — staleness vs transit, plotted together so an idle source
   cannot be mistaken for a stalled pipeline.

Below them: micro-batch duration against the 5s trigger, the number of distinct
Avro writer schemas decoded per batch (this steps 1 → 2 during the migration),
API latency, and dependency health kept separate from data age.

`pipeline_kafka_lag_records` is **Spark offset-head distance**, computed from query
progress. It is not Kafka consumer-group lag: Structured Streaming stores its source
offsets in its checkpoint and does not commit a stable consumer position for this job.

For the controlled backpressure exercise, recreate only the processor with a
lesson-only sink delay, generate a burst, then remove the delay:

```bash
SINK_DELAY_SECONDS=10 docker compose up -d --force-recreate stream-processor
uv run python scripts/load_generator.py --rate 200 --duration 60
# Watch offset-head distance and batch duration rise in Grafana.
docker compose up -d --force-recreate stream-processor
# The lag must return to zero; recovery capacity must exceed the arrival rate.
```

Measured on an arm64 laptop with those exact parameters:

| | value |
|---|---|
| offset-head distance | `0 → 455 → 843 → 1210 → 1603 → 1957` peak, then drains |
| end-to-end latency | `14s → 62s`, climbing monotonically |
| p95 batch duration | `~19s` against a 5s trigger |
| after removing the delay | back to `0` within ~40s |
| source vs sink after ~12,000 changes | identical: 9093 rows, 2,280,502.17 |

Peak lag crosses the `Lesson11KafkaLagHigh` threshold of 1,000, so the alert
fires during the exercise rather than staying theoretical.

The processor reads with `maxOffsetsPerTrigger=1000`. This matters more than it
looks: with an unbounded batch the Kafka source claims every available offset at
once, so a backlog hides *inside* one slow micro-batch and offset-head distance
reads zero during an overload. Bounded batches make the queue visible, cap
memory, and let you measure a drain rate — which is the number that decides
whether recovery ever completes.

## Delivery semantics

- Spark checkpoints Kafka offsets and query progress on the persistent
  `l11-spark-checkpoints` volume.
- The ClickHouse write is outside Spark's checkpoint transaction and can be retried.
- `orders_current` uses `ReplacingMergeTree(version)` with the Debezium source LSN.
- `(source_topic, kafka_partition, kafka_offset)` remains the immutable event identity.
- Delete events write a higher-version `_is_deleted=1` row.
- Correctness-sensitive reads use `FINAL`, then exclude the winning delete marker.

The honest guarantee is **effectively-once current state**, not a distributed
exactly-once transaction across all components.

## Host ports

Every port here is chosen to avoid the earlier lessons, with one exception:
**Kafka's `29092` is also used by Lesson 6's second broker.** Lessons 6-10 all
share a Kafka external port by design, and the same rule applies here — stop the
previous lesson's stack before starting this one:

```bash
docker compose -f ../src-lesson6/docker-compose.yml down   # or whichever is up
```

| Service | Host port |
|---|---:|
| Postgres | 15432 |
| Kafka | 29092 |
| Schema Registry | 18081 |
| Kafka Connect | 18083 |
| ClickHouse HTTP / native / metrics | 18123 / 19000 / 19363 |
| FastAPI | 18000 |
| Spark UI / processor metrics | 14040 / 19108 |
| Prometheus | 19090 |
| Grafana | 13000 |

## Stop and reset

Stop while preserving Kafka, checkpoints, and database data:

```bash
docker compose down
```

For a clean classroom rehearsal, remove this lesson's named volumes:

```bash
docker compose down -v
```

The second command irreversibly removes Lesson 11 data and checkpoints. It does not
touch volumes from earlier lessons.

## Important distinctions

- The Kafka topic has three partitions, so a Kafka offset alone is not a global row
  version. The source WAL LSN orders changes to current state.
- A Schema Registry validates and locates schemas; it does not deploy consumer or
  sink migrations.
- BACKWARD compatibility means a new reader can read old writer data. FORWARD means
  an old reader can read new writer data. Test the actual reader/writer pair.
- Two different numbers are both called "freshness", and the dashboard plots both.
  `api_pipeline_transit_seconds` (`source_ts → ingested_at`) is how long the
  pipeline takes; it stays flat while the source is idle. `api_data_freshness_seconds`
  (`now → newest source_ts`) is what a user feels; it climbs whenever nobody is
  writing, which is not a failure. Either one alone will mislead you. Both report
  `-1` when there is nothing to measure.
- `created_at → source_ts` measures business time before the CDC change and is not
  sink/queryability latency.
