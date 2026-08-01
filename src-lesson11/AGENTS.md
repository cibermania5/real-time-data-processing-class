# Agent takeover guide — Lesson 11

Read this file completely before changing `src-lesson11`. Then read `README.md`
for the student runbook and `LESSON_PLAN.md` for the pedagogical intent.

## Mission

Lesson 11 is the course's integrated reference pipeline:

```text
Postgres → Debezium → Kafka + Schema Registry → Spark → ClickHouse → FastAPI
                                                               │
                                           Prometheus ←─────────┘
                                               │
                                            Grafana
```

The lesson is not a tour of product logos. It teaches how guarantees, schemas,
durable positions, replay, and backpressure compose across system boundaries.
Lesson 12 is expected to reuse this as its healthy reference pipeline.

## Current verified state

As of 2026-07-31, the complete stack has been built and run on an arm64 laptop
with roughly 8 GB RAM.

- All long-lived containers were healthy: Postgres, Kafka, Schema Registry,
  Connect, ClickHouse, Spark processor, API, Prometheus, and Grafana.
- `topic-init` and `connector-init` exited with status 0. That is their expected
  healthy state.
- Debezium connector and task both reported `RUNNING`.
- Prometheus reported `up == 1` for the API, processor, ClickHouse, and itself.
- A 100-change load run completed: 66 inserts, 29 updates, and 5 deletes.
- The canary verifier passed insert, update, delete, and API visibility.
- The expand-first migration passed without restarting Spark.
- Schema Registry contained writer schema versions `[1, 2]` for
  `cdc.public.orders-value`.
- `discount_code=MAGNUM11` reached ClickHouse and the API.
- After migration, the verifier proved that updating a populated nullable field
  back to `NULL` does not resurrect the Debezium before image.
- The processor was recreated with its persistent checkpoint and the verifier
  passed again.

The current local Docker volumes are therefore already migrated to schema v2.
`scripts/migrate_discount_code.py` is idempotent and detects that state.

**Cold-start rehearsal (2026-07-31).** The entire lesson has since been reproduced
from destroyed volumes — see item 1 under "Remaining work". Every init path,
including the first-run-only branch of the migration, is now verified rather than
inferred. Rebuilding the Spark image and re-resolving its Maven jars added roughly
a minute; budget for it before class rather than during.

## Worktree safety

`src-lesson11/` is currently an untracked directory in the parent repository.
There are unrelated untracked slide previews and Lesson 10 files elsewhere in the
worktree. They belong to the user. Do not delete, reset, stage, reformat, or modify
them as part of Lesson 11 work.

Do not run destructive Git commands. Do not remove Docker volumes unless the user
has explicitly approved a clean rehearsal; `docker compose down -v` deletes the
Postgres source, Kafka log, ClickHouse state, Spark checkpoint, Grafana state, and
the Spark Ivy cache for this lesson.

## Start here

Run commands from `src-lesson11/`:

```bash
docker compose up -d --build
uv sync
uv run python scripts/preflight.py
uv run python scripts/verify.py --timeout 180
```

Expected preflight checks:

```text
OK   postgres
OK   clickhouse
OK   schema-registry
OK   kafka-connect
OK   debezium-task    RUNNING/RUNNING
OK   processor-metrics
OK   api
```

Expected verifier conclusion:

```text
PASS insert
PASS update
PASS nullable update   # only after the schema migration
PASS delete
PASS: Postgres → Debezium → Avro/Kafka → Spark → ClickHouse → API
```

`verify.py` creates a stable canary and proves its lifecycle through ClickHouse and
the HTTP API. It must pass before claiming the pipeline works. Container health or
a green dashboard alone is not correctness evidence.

## Architecture invariants — do not casually change

1. **Spark is the only Kafka → ClickHouse path.** Do not add a ClickHouse Kafka
   engine table; that bypasses the integration boundary the lesson studies.

2. **Decode the Confluent schema ID per record.** A retained topic contains mixed
   Avro writer schemas. Fetching only `latest` once and applying it to all records
   is incorrect. `processor/stream_processor.py` groups each batch by schema ID,
   fetches the matching writer schema, and normalizes the resulting DataFrames.

3. **Choose the whole Debezium row image by operation.** Use `after` for create,
   update, and snapshot records; use `before` only for deletes. Never reconstruct
   individual fields with `coalesce(after.field, before.field)`: a real update to
   SQL `NULL` would incorrectly restore the old value.

4. **Use the Debezium source LSN as the current-state version.** Preserve
   `(source_topic, kafka_partition, kafka_offset)` as immutable event identity.
   Kafka offset alone is not global across the topic's three partitions.

5. **Describe the sink honestly.** Spark's ClickHouse write is outside Spark's
   checkpoint transaction and can be retried. The defensible guarantee is
   effectively-once *current state*, constructed with
   `ReplacingMergeTree(version)`, `FINAL`, and a winning `_is_deleted=1` marker.

6. **Keep checkpoints persistent.** Spark offsets live in
   `l11-spark-checkpoints`, not in Kafka consumer-group commits and never in
   `/tmp`. The dashboard's `pipeline_kafka_lag_records` is Spark's distance from
   Kafka head, not consumer-group lag.

6b. **Keep `maxOffsetsPerTrigger` set.** With an unbounded batch the Kafka source
   claims every available offset at once, the backlog hides inside one slow
   micro-batch, and offset-head distance reads `0` through an entire overload —
   which silently destroys the Hour 3 backpressure exercise. Verified: unbounded
   gave a flat `lag=0` under a 10s sink delay; at `maxOffsetsPerTrigger=1000` the
   same overload produced `0 → 455 → 843 → 1210 → 1603 → 1957` and then drained.

7. **Keep the schema exercise expand-first.** A clean v1 source and sink omit
   `discount_code`. The migration adds the nullable ClickHouse column, proves an
   old-schema event still flows, sets subject compatibility to `FULL`, alters
   Postgres, and proves v1/v2 coexist without a Spark restart.

8. **Separate dependency health from freshness.** An idle source naturally ages.
   API/ClickHouse reachability, Spark progress, offset-head distance, and data age
   are different signals. Do not collapse them into one misleading health bit.

9. **Use real Prometheus series, and size the buckets for the experiment.**
   Histograms expose `_bucket`, `_sum`, and `_count`; a bare histogram name is not
   a time series. `pipeline_end_to_end_latency_seconds` is a gauge holding the age
   of the *oldest* change in the last batch, sampled *after* the sink write so
   `SINK_DELAY_SECONDS` is included. `pipeline_batch_duration_seconds` carries
   explicit buckets out to 120s: `histogram_quantile` can never return more than
   the highest finite bucket, and the default bucket set stops at 10s — exactly the
   value the backpressure exercise is designed to exceed.

10. **Stay within the 8 GB resource model.** Spark intentionally runs in local mode
    in one container. Do not add Spark master/worker containers, Flink, DuckDB, or
    another query engine to the default stack.

## Course continuity — decisions already made

Lesson 11's job is recognition, not novelty. These choices are deliberate:

- **The source table is Lesson 1's `orders` table**, unchanged through Lessons 4
  and 5: `id`, `customer_id`, `amount NUMERIC(10,2)`, `status`, `created_at`, plus
  Lesson 5's `updated_at`. Keep the precision at `(10,2)`. Students have typed this
  table four times; it must not look new.
- **No `product_id`.** `lesson-details/en/lesson-11.md` sketches one, but no
  `orders` table students actually built ever had it. The code lineage wins over
  the design doc.
- **Two planted callbacks must be paid off explicitly**, because earlier lessons
  promised them by name:
  - `src-lesson5/lesson-05-teaching-notes.md:258` — "CDC is a schema contract …
    the reason schema registries exist. We'll meet those in Lesson 11."
  - `src-lesson8/README.md:8-10` — "In the full course pipeline (Lesson 11), the
    transaction stream comes out of an OLTP Postgres via Debezium CDC. Here
    `seed_transactions.py` stands in for that CDC feed."
- **The Debezium `topic.prefix: cdc` / `table.include.list: public.orders` pattern
  is inherited verbatim from the Lesson 5 and Lesson 6 overlays.** The topic name
  `cdc.public.orders` is a callback, not an arbitrary choice.
- **Lessons 6-10 fragmented the domain on purpose** (L8 → `transactions`, L9 → a
  synthetic `orders` topic, L10 → generic `events`). Those were single-topic
  stand-ins for studying one component. Lesson 11 returns to the real lineage and
  should say so rather than pretending the thread was unbroken.
- **Compatibility is set to `FULL`, not `BACKWARD`.** The syllabus says BACKWARD;
  FULL is BACKWARD ∧ FORWARD and is the stronger claim the mixed-version proof
  actually needs. Explain the difference; do not quietly weaken it.

## File map

- `docker-compose.yml` — complete local stack, health dependencies, named volumes,
  memory limits, and isolated Lesson 11 ports.
- `connect/Dockerfile` — Confluent Connect plus Debezium PostgreSQL 3.0.8.
- `connect/orders-connector.json` — canonical connector configuration.
- `connect/register.sh` — idempotent connector registration; waits for connector
  and task `RUNNING`, and fails loudly on task failure.
- `spark/Dockerfile`, `spark/spark-defaults.conf` — Spark 4.0.1, Scala 2.13,
  Java 17, local-mode resource settings, Kafka/Avro Maven packages.
- `processor/stream_processor.py` — Confluent framing, dynamic schema decoding,
  Debezium normalization, ClickHouse write, persistent checkpoint, and metrics.
- `sql/postgres-init.sql` — v1 OLTP table with logical-replication requirements.
- `sql/clickhouse-init.sql` — v1 `ReplacingMergeTree` current-state table.
- `app/main.py` — current-state API and API Prometheus metrics.
- `scripts/preflight.py` — dependency, connector-task, processor, and API readiness.
- `scripts/load_generator.py` — real OLTP insert/update/delete traffic.
- `scripts/verify.py` — deterministic canary lifecycle through the API.
- `scripts/migrate_discount_code.py` — downstream-first mixed-schema proof.
- `prometheus/`, `grafana/` — scrape config, alerts, provisioned datasource and
  dashboard.
- `dashboard.html` — single-file, no-build order tracer for class. One order id,
  six hop cards; each runs its own query *and* prints the equivalent shell
  command, so the page never becomes a number students must trust. It queries
  ClickHouse (`:18123`) and the API (`:18000`) directly from the browser rather
  than through a new backend endpoint — the point is that the layers are
  separately reachable. Connect and Schema Registry therefore carry
  `ACCESS_CONTROL_ALLOW_ORIGIN: "*"` in `docker-compose.yml` (lesson-only; both
  are admin surfaces in production). Postgres has no HTTP interface at all, so
  hop ① is honestly marked "shell only" instead of being proxied.
- `README.md` — operator/student runbook.
- `LESSON_PLAN.md` — three-hour run of show and slide-agent handoff.

## Ports and state

| Component | Host port / state |
|---|---|
| Postgres | `15432`, volume `l11-postgres-data` |
| Kafka | `29092`, volume `l11-kafka-data` |
| Schema Registry | `18081` |
| Kafka Connect | `18083` |
| ClickHouse | HTTP `18123`, native `19000`, metrics `19363` |
| FastAPI | `18000` |
| Spark | UI `14040`, metrics `19108` |
| Prometheus | `19090` |
| Grafana | `13000`, `admin` / `lesson11` |

First pulls are large. Confluent images include very large layers, and Spark resolves
about 63 MB of Kafka/Avro artifacts on first processor boot. Maven artifacts persist
in `l11-spark-ivy`.

`docker compose ps -a` should show the long-lived services as healthy and both init
services as `Exited (0)`.

## Validation matrix

After any meaningful change, run the smallest relevant checks and finish with the
full contract:

```bash
# Static
docker compose config --quiet
uv run python -m ruff check app processor scripts
uv run python -m compileall -q app processor scripts
python -m json.tool grafana/dashboards/lesson11-pipeline.json >/dev/null
git diff --check

# Runtime
uv run python scripts/preflight.py
uv run python scripts/load_generator.py --rate 20 --duration 5
uv run python scripts/verify.py --timeout 180

# Schema centerpiece, from a clean v1 state
uv run python scripts/migrate_discount_code.py --timeout 180
uv run python scripts/verify.py --timeout 180
```

If `app/` changes, rebuild/recreate the API because its source is copied into its
image:

```bash
docker compose build api
docker compose up -d api
```

`processor/` is bind-mounted read-only; restart `stream-processor` after changing
processor code. Rebuild the Spark image only when its Dockerfile or dependencies
change.

## Known cold-start and recovery facts

- An empty Debezium snapshot does not guarantee the CDC topic already exists.
  `topic-init` creates `cdc.public.orders` before Spark starts.
- `connector-init` must wait for the Debezium *task*, not just a successful config
  `PUT`.
- ClickHouse 24.12 rejects a too-small `background_pool_size` when its mutation
  free-entry threshold is larger. The resource config deliberately leaves that pool
  at its safe default while lowering other resource usage.
- `connector-init` and `topic-init` are rerunnable and idempotent.
- The source/sink schemas mounted in `docker-entrypoint-initdb.d` run only on fresh
  database volumes.

## Remaining work / next useful agent tasks

1. ~~Run one explicitly approved clean-volume rehearsal.~~ **Done, 2026-07-31.**
   `down -v` then a cold `up -d --build` reproduced the whole lesson from nothing:
   fresh DDL came up correct (`numeric(10,2)` / `Decimal(10, 2)`, no
   `discount_code`), `topic-init` and `connector-init` both `Exited (0)`, preflight
   all OK, and the v1 verifier passed with the nullable-update step correctly
   skipped. The migration then ran **under live load** and exercised step `2/4`
   — the old-schema-still-flows proof — which had never run before because earlier
   volumes were already expanded. Registry ended at `[1, 2]` with
   `compatibilityLevel: FULL`, the post-migration verifier passed all four steps,
   and source and sink agreed exactly (264 rows / 62,964.77).
2. ~~Run and record the controlled backpressure exercise.~~ **Done.** With
   `SINK_DELAY_SECONDS=10` and `--rate 200 --duration 60`: offset-head distance
   `0 → 1957` peak (crossing the 1,000 alert threshold), end-to-end latency
   `14s → 62s`, p95 batch duration ~19s against the 5s trigger, drained to `0`
   within ~40s of removing the delay. Source and sink agreed exactly afterwards
   (9093 rows / 2,280,502.17), proving replay under load did not corrupt current
   state. Numbers are recorded in `README.md`.
3. Capture screenshots for the slide deck: the Grafana dashboard mid-overload, and
   the v1/v2 registry view. The numeric timings are already recorded above.
4. If Lesson 12 begins, fork from this healthy contract; do not inject permanent
   chaos or poison pills into the Lesson 11 default path.

The slide agent should use `LESSON_PLAN.md` as the narrative source of truth and
this file as the implementation/verification source of truth.

