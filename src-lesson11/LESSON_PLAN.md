# Lesson 11 teaching plan — one order, seven boundaries

Lesson 11 is the course's integration payoff. Students do not build seven systems
from scratch. They receive a running reference pipeline, trace one order through
every durable boundary, prove its guarantees, evolve its contract under load, and
use observability to explain backpressure.

The repeated question is:

> Where is this event durable now, and what happens if the next process dies?

## What students must leave able to do

1. Name the durable position and delivery guarantee at every system boundary.
2. Construct effectively-once current state from replayable, at-least-once writes
   and an idempotent sink.
3. Evolve an Avro CDC contract without downtime or silent field loss.
4. Locate a bottleneck using lag, throughput, and freshness together.
5. Prove the pipeline is correct with executable checks, not a green dashboard.

## The reference pipeline

```text
Postgres --WAL/LSN--> Debezium --Avro/schema ID--> Kafka --partition/offset-->
Spark --checkpoint/batch--> ClickHouse --versioned current state--> FastAPI
                                  |                                |
                                  +---------- Prometheus ----------+
                                                   |
                                                Grafana
```

Components deliberately *not* included: CockroachDB, DuckDB, Airflow/Dagster,
and Flink. Earlier lessons established when those are useful; adding them here
would add containers without strengthening the end-to-end argument.

## The guarantee ledger

| Boundary | Durable position / identity | Honest guarantee | Recovery design |
|---|---|---|---|
| Postgres → Debezium | WAL LSN + replication slot | replayable CDC; normally at least once | do not acknowledge past an event that is not durable downstream |
| Debezium → Kafka | topic, partition, offset; Confluent schema ID | durable ordered log per partition | idempotent producer helps within a session; downstream still tolerates replay |
| Kafka → Spark | offsets in the Spark checkpoint | exactly-once offset/state progress inside Spark | checkpoint lives on a persistent volume, never `/tmp` |
| Spark → ClickHouse | Debezium LSN as row version; topic/partition/offset as event identity | external sink can be written more than once | versioned current-state rows make replay harmless |
| ClickHouse → API | query result at a known sink-ingest time | current state with measurable staleness | report dependency health and data freshness separately; an idle source is not a failure |

Do not call the whole pipeline transactionally exactly once. The defensible claim
is **effectively-once current state**: replay can happen, but replay does not change
the answer returned by the API.

## Three-hour run of show

### Hour 1 — the system, not the boxes (50 minutes)

**0–8 min — Cold open: follow one order.** Insert a canary order and keep its ID
on screen. Show the final API response first, then walk backward through
ClickHouse, Spark progress, Kafka, Debezium, and Postgres. Ask at each hop where
the event is durable.

**8–22 min — Guarantee ledger.** Rebuild the pipeline from callbacks:

- L1/L5: WAL and LSN
- L6: topic, partition, offset
- L7/L8: checkpoint plus idempotent external sink
- L9: latency is an SLA, not a brand
- L10: versioned OLAP state and a serving API

Predict first: Spark writes a batch to ClickHouse and dies before checkpointing.
What happens? The batch is replayed. Then show why the API answer remains stable.

**22–36 min — Schema contracts in the correct direction.** Draw writer schema and
reader schema separately. BACKWARD means the new schema reads old data; FORWARD
means the old schema reads new data. An optional field with a default can satisfy
both, but that is a property to test, not a slogan. Explain that each Confluent
Avro payload carries a schema ID and a mixed-version topic must be decoded per
record; fetching only “latest” once is not sufficient.

**36–46 min — Backpressure is stored time.** Slow ClickHouse → Spark batches take
longer → the distance from processed offsets to Kafka head grows → API freshness
ages. Kafka protects Postgres only until retention or disk becomes the next limit.
The three metrics are:

1. lag: how much work is waiting;
2. throughput: whether each stage can drain it;
3. freshness: the user-visible consequence.

**46–50 min — Lab contract.** A green dashboard is supporting evidence. The pass
condition is the verifier tracing one canary through the API and proving update,
delete, and replay semantics.

### Hour 2 — prove the golden path (50 minutes)

**0–12 min — Start and preflight.** Run the supplied Compose stack. Use the
preflight command to check every service and connector. Do not spend class time
authoring Compose YAML.

**12–25 min — Trace the canary at every hop.** Insert one order with a stable
correlation ID. Inspect:

1. the Postgres row and current WAL position;
2. the Debezium connector state;
3. Kafka key/value, partition, offset, and schema ID;
4. Spark batch and checkpoint progress;
5. ClickHouse source LSN, Kafka identity, and sink-ingest timestamp;
6. the FastAPI response.

**25–38 min — Prove replay-safe current state.** Update the same order, delete it,
and replay/restart the processor. Raw storage may contain several versions; the
current-state API must show one correct answer and must hide the deleted row.

**38–48 min — Read the dashboard as a causal graph.** Connect produces → Kafka
head advances → Spark consumes → ClickHouse becomes queryable → API freshness
returns to zero. Students explain the direction before looking at labels.

**48–50 min — Checkpoint.** Run the verifier. Save its output as the first piece of
deliverable evidence.

### Hour 3 — evolve it, then make it fall behind (50 minutes)

**0–8 min — Predict the unsafe migration.** If Postgres adds `discount_code` first,
an old reader may silently drop it or a new reader may fail against an old sink.
“The pipeline is green” does not prove the field arrived.

**8–28 min — Expand-first migration under load.** Keep the load generator running:

1. expand ClickHouse with nullable `discount_code`;
2. deploy/enable the tolerant Spark projection and API response;
3. verify the old schema still flows;
4. add the nullable column in Postgres;
5. insert `MAGNUM11`;
6. inspect Schema Registry v1 and v2;
7. prove old and new Avro records coexist and both decode;
8. prove `MAGNUM11` reaches the API.

This is expand/contract. Lesson 11 performs only the additive expand half; removal
and rollback belong in a later production migration.

**28–42 min — Backpressure experiment.** Apply a controlled sink delay or input
spike. Predict the graph shapes, then observe:

- input throughput rises above processed throughput;
- offset-head distance grows;
- batch duration approaches or exceeds the trigger;
- end-to-end freshness ages;
- after removing the delay, drain rate must exceed arrival rate or recovery never
  completes.

Measured with `SINK_DELAY_SECONDS=10` and `--rate 200 --duration 60`, so you know
what the room should be seeing:

| Metric | Shape |
|---|---|
| offset-head distance | `0 → 455 → 843 → 1210 → 1603 → 1957` peak, then down |
| end-to-end latency | `14s → 62s`, climbing the whole time |
| p95 batch duration | `~19s` against a 5s trigger |
| recovery | back to `0` within ~40s of removing the delay |

The turn happens the moment arrival stops: lag peaks and then falls, because drain
finally exceeds arrival. That inflection is the point of the exercise — make them
name it before you point at it. Peak lag crosses the 1,000-record alert threshold,
so the alert fires for real.

Afterwards, prove the payoff: Postgres and ClickHouse still agree exactly
(9093 rows / 2,280,502.17 in the recorded run). Replay under overload changed the
physical row count and did not change the answer.

**42–47 min — Three-metric incident drill.** Give pairs a screenshot with one panel
hidden. They must identify the failing boundary and request the one missing metric
that would disambiguate the cause.

**47–50 min — Close into Lesson 12.** Lesson 11 proves a healthy pipeline can be
understood and evolved. Lesson 12 starts from this exact reference system and asks
students to diagnose failures they did not choose.

## Executable proofs required from the source project

- `preflight`: every dependency and the Debezium task are ready.
- `verify`: a canary is visible at the source, sink, and API with one stable ID.
- mixed-schema proof: v1 and v2 records coexist and decode in one Spark query.
- update/delete/replay proof: raw versions may grow while current-state results stay
  correct.
- migration proof: `MAGNUM11` reaches the API while load continues.
- backpressure proof: lag rises, then drains after capacity is restored.

## Slide-agent handoff

Build the deck around one order ID and one dashboard, not around seven product
logos. The visual grammar should repeat the same pipeline and highlight one boundary
at a time. Every theory claim should point to a command, metric, or query students
will run in the next hour. Reserve product comparison and production-hardening lists
for an annex; the main story is guarantees, contracts, and diagnosis.

Three lines worth repeating:

- **Atomicity ends at the system boundary.**
- **A schema registry stores contracts; it does not migrate consumers for you.**
- **Lag is stored time, and freshness is what the user feels.**
