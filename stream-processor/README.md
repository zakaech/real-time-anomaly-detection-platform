# stream-processor

Spark Structured Streaming job. Reads `telemetry.raw`, validates it, aggregates
it into sliding windows per machine, scores each window with the Phase 2
artefact, and publishes `telemetry.scored` and `alerts`.

It reimplements no machine learning: the artefact carries the whole inference
path (per-machine normalisation, imputation, scaling, detector, ECDF
calibration) and is loaded as-is.

The measured results of a real execution are in
[`docs/10-phase-3-streaming.md`](../docs/10-phase-3-streaming.md). Every figure
there comes from `inspect-stream` reading the topics back.

> **Windows are scored only once complete (D-37, ADR-004).** In update mode a
> window is published while still filling, and the model was trained on complete
> ones. Measured before the fix: windows with 30-39 samples were flagged
> anomalous **100 %** of the time against **1.0 %** for complete ones, producing
> 16 CRITICAL alerts for 15 machines in five minutes. A window is now scored
> only when its event-time coverage spans the whole window at the machine's own
> cadence, at **both** ends. After the fix, on the same scenario: **1 alert, on a
> complete 60-sample window**, and the score arrives ~9 s after the window
> closes. Immature windows are still published, with
> `skip_reason=WINDOW_NOT_MATURE`.

## Pipeline

```
telemetry.raw
     │
     ├─ parse + validate ─────────┬─► telemetry.dlq   (data errors only, ADR-002)
     │                            └─► telemetry.late  (producer lag > watermark)
     ▼
event time + watermark 90 s
     ▼
sliding windows 60 s / 10 s, grouped by machine_id
     ▼
52 features (spec v2.0.0, identical to training)
     ▼
admission: >= 30 samples, running ratio >= 0.9, last state == RUNNING
     ▼
score (One-Class SVM 2.0.0, loaded once per worker, ADR-001)
     ▼
telemetry.scored  ◄── contractual boundary
     ▼
alert state machine (2 consecutive windows open, 120 s gap closes)
     ▼
alerts
```

Three queries, one checkpoint each. Sharing a checkpoint corrupts both states.

## Feature parity is the invariant

The same 52 features are computed by three engines: the `telemetry_core`
reference (pure Python), the pandas path in `ml-training`, and Spark here. A
silent divergence would make the model score a distribution it never saw, and
nothing would fail loudly.

The Spark translation is **driven by `FEATURE_SPECS`**, not written by hand
alongside it, and `tests/test_feature_conformance.py` compares Spark's output to
the reference on a full history, on heavily missing data and on a frozen sensor.

Two subtleties the parity forced:

- **`min_by`/`max_by` over event time**, never `first()`/`last()` — the latter
  are explicitly non-deterministic after a shuffle (decision D-26).
- **`stddev_samp`** (ddof=1) pinned across all three engines, and correlations
  computed by pairwise deletion, returning null on zero variance.

## Delivery semantics

**At-least-once**, end to end. *Effectively-once* at persistence, through a
deterministic UUIDv5 `alert_id` and an idempotent upsert.

This is **not** exactly-once and is never presented as such: the Kafka sink is
not transactional here, so a restart replays the uncommitted batch. The measured
number of duplicate deliveries after a real crash is in `docs/10`.

## Running it

```sh
# infrastructure
docker compose -f infra/docker-compose.yml --env-file .env up -d kafka kafka-init postgres

# produce data (replay = accelerated history, realtime = live)
docker compose -f infra/docker-compose.yml --env-file .env --profile sim run --rm \
  -e SIMULATOR_MODE=replay -e SIMULATOR_SEED=20260907 -e SIMULATOR_DURATION_SECONDS=14400 \
  event-simulator

# the job
docker compose -f infra/docker-compose.yml --env-file .env --profile stream up -d stream-processor

# read the output topics back
docker compose -f infra/docker-compose.yml --env-file .env --profile stream run --rm \
  stream-processor python -m stream_processor.tools.inspect_stream
```

**`STREAM_LATE_DETECTION_ENABLED=false` for a backfill.** During an accelerated
replay every sample carries a large ingest lag by construction, so late routing
would divert the entire history to `telemetry.late`.

## Configuration

Everything is environment-driven, prefix `STREAM_` (`config/settings.py`).
Brokers come from `telemetry_core.config.KafkaSettings`, which has no default on
purpose. The window shape defaults to what Phase 2 actually trained on, and the
artefact is checked against it at startup rather than trusted.

## Tests

```sh
cd stream-processor && pytest -q     # needs Java 17; see decision D-33
```

The trained binary **is** committed since D-48, so the three tests that load the
real artefact run rather than skip — including the one that flips a single byte
and asserts the job refuses the pickle. They still skip in one legitimate case:
the component images copy `ml-training/src` but not `artifacts/`, so running the
suite inside a container without the read-only bind mount finds no binary. The
artefact **rejection** tests never needed it.

## Design notes worth defending

- **Monitoring is pull-based**, not a `StreamingQueryListener`. The listener
  version was built, measured and removed: a Python listener needs the Py4J
  callback server, and with three concurrent queries the driver spent its time
  in that gateway instead of running batches. See `monitoring.py`.
- **The state timeout is clamped to the current watermark.** Spark kills the
  query if an event-time timeout lands behind the watermark, which happens on
  restart when the watermark is restored from the checkpoint while the replayed
  batch still carries older windows. Found by the crash test, never by the happy
  path.
- **Only data errors reach the DLQ.** An infrastructure failure is handled by
  retry and the checkpoint; routing it to the DLQ would discard valid messages
  during an incident (ADR-002).
