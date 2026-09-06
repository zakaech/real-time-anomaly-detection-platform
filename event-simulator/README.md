# event-simulator

Generates a coherent multivariate telemetry stream for a simulated machine
fleet, publishes it to `telemetry.raw`, and publishes its ground truth to
`telemetry.labels` — on a separate topic, so a leak into the feature pipeline is
impossible rather than merely discouraged.

## What makes the data worth training on

Five independently noised signals would contain no multivariate structure at
all, and a model trained on them could not beat a per-sensor threshold — the
comparison [docs/07](../docs/07-ml-methodology.md) makes mandatory. So every
sensor is derived from two hidden variables, a **load** and a **wear** level:

```
rotation_rpm = rated x motion x (0.85 + 0.15 x load)
power_kw     = base + k1 x rpm x load + k2 x wear x rpm
vibration    = base + k3 x load + k4 x wear
pressure_bar = (base + k5 x load) x motion
temperature  = ambient(t) + k6 x power, through a first-order lag
```

The second power term is the one that matters. As wear accumulates, friction
makes power rise **at constant speed**, while power and speed each stay inside
their own nominal band. That is the anomaly a per-sensor threshold structurally
cannot see, and exactly what the `power_per_rpm` feature was designed to catch.

Load follows an Ornstein-Uhlenbeck process rather than independent draws, so it
wanders smoothly over minutes; temperature reaches its target through thermal
inertia, which gives the signal a memory and turns a slow drift into a trend
rather than noise. Daily seasonality enters through ambient temperature and
through the load target.

## Anomalies

Three families, as interchangeable strategies, differing in **where** they act:

| Family | Types | Acts on | Effect |
|---|---|---|---|
| `SPIKE` | `OVERHEAT`, `POWER_SURGE`, `PRESSURE_DROP` | one reading | short raised-sine excursion |
| `DRIFT` | `BEARING_WEAR` | hidden **state** | wear rises, so power, vibration and temperature move **together** |
| `DRIFT` | `MOTOR_STALL` | one reading | speed falls while power holds, so specific consumption climbs |
| `SENSOR_FAILURE` | `SENSOR_STUCK`, `SENSOR_DROPOUT` | one reading | frozen value, or `null` — never zero |

The state/reading distinction is physical, not technical: bearing wear genuinely
changes the machine, a stuck sensor only changes the measurement.

Anomalies stay inside the contract's physical bounds. A temperature of 812 °C is
a broken sensor, not a machine anomaly, and a detector that "finds" it has
learned nothing. A clamp firing is therefore treated as a configuration bug and
counted in the run summary.

Episodes are **transient**: each is an independent occurrence for evaluation.
Lasting degradation is modelled separately by `wear.rate_per_hour`, and
maintenance restores it.

## Configuration

`config/fleet.yaml` splits each profile in two, and the split is load-bearing:

- **`nominal`** — the declared operating ranges. What a plant knows about its
  machines, and the only block that will be extracted into the Flyway seed at
  step 5.
- **`simulation`** — the true physics: noise sigmas, coupling coefficients, wear
  rate. Nobody knows these in production; publishing them to the platform would
  leak the answer into the design of the detector.

Run behaviour comes from the environment (`SIMULATOR_*`), Kafka connection from
`telemetry_core.config`, which refuses to start without an explicit broker.

## Running

```sh
# realtime, against the running stack
docker compose --env-file .env -f infra/docker-compose.yml --profile sim up -d event-simulator

# a bounded, reproducible replay: 2 hours of history as fast as possible
docker run --rm --network anomaly-platform_anomaly-net \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  event-simulator python -m event_simulator.main \
  --mode replay --duration-seconds 7200 --seed 42

# generate and validate without a broker
docker run --rm event-simulator python -m event_simulator.main \
  --dry-run --max-events 500
```

The seed is always logged, drawn or given, so any run can be replayed.

## Verifying

```sh
docker run --rm --network anomaly-platform_anomaly-net \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  event-simulator python -m event_simulator.tools.inspect --topic telemetry.raw

docker run --rm --network anomaly-platform_anomaly-net \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  event-simulator python -m event_simulator.tools.inspect --topic telemetry.labels
```

The tool decodes with the real contracts rather than printing bytes, and reports
the **lateness distribution** (`ingest_time - event_time`). That is not
decoration: [docs/04 §2.2](../docs/04-streaming-semantics.md) requires the
watermark to be set from the observed distribution rather than guessed, and this
is what observes it.

## Late data

Publication is delayed; `event_time` is never back-dated. Back-dating would
write a past timestamp onto a value computed for the present, and the sample
would stop being internally consistent.

Two shapes, and the second is the one that matters:

- **jitter** — individual samples held a few seconds. Easy for any watermark to
  absorb.
- **outage** — a machine goes silent, then flushes its buffer in one burst. This
  produces a run of samples far behind the stream's high-water mark, arriving
  out of order inside a single partition. That is what will actually stress the
  Phase 3 watermark.

Samples still held when a run ends are discarded and counted, not flushed:
emitting them at shutdown would compress an outage into an instantaneous burst
that is an artefact of stopping rather than of the network.

## Checks

```sh
docker build -f event-simulator/Dockerfile -t event-simulator .
docker run --rm -v "$PWD":/workspace -w /workspace/event-simulator event-simulator \
  sh -c "ruff check . && ruff format --check . && mypy && pytest -q"
```

Three tests are worth knowing about, because each guards something whose failure
mode is silent rather than loud:

- **`test_rng.py::test_anomaly_draws_cannot_shift_the_noise_sequence`** — if
  noise and injection shared a generator, enabling anomalies would shift the
  noise, the noise would correlate with the injection, and a model could learn
  that correlation instead of the anomaly. Offline metrics would look excellent
  and mean nothing.
- **`test_projections.py`** — compares the produced payload's keys against the
  JSON Schema itself, so a field added to the internal sample cannot slip into
  the telemetry message.
- **`test_generation.py::test_clamping_never_fires_on_the_shipped_configuration`**
  — proves no anomaly intensity pushes a sensor outside its physical range,
  which would make detection trivial and the evaluation worthless.
