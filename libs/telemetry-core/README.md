# telemetry-core

Shared library for every Python component of the platform: `event-simulator`,
`ml-training` and `stream-processor` all depend on it, and it is shipped to Spark
executors. It therefore stays small and carries no engine dependency — no
PySpark, no pandas.

## What lives here, and why it is not duplicated elsewhere

| Module | Responsibility | What breaks if it is duplicated |
|---|---|---|
| `schemas` | Message contracts as frozen dataclasses | Two components disagree about a field, and the mismatch only shows in production |
| `codec` | Bytes ↔ dataclass, and every decoding failure as one exception type | The stream processor can no longer tell a data error from an infrastructure error, and sends valid messages to the dead letter topic during a broker outage |
| `timeutil` | The one ISO-8601 UTC millisecond format | Two encodings of the same instant differ byte for byte, and the deterministic alert identifier stops matching |
| `ids` | Deterministic `alert_id` (UUIDv5) | Replayed batches stop deduplicating; alerts silently double |
| `features` | Feature specification and reference implementation | Training and serving compute different features, and the model scores a distribution it never saw |
| `config` | Typed settings, validated at startup | A missing variable is discovered on the first message instead of at deployment |
| `logging` | Structured JSON envelope | Logs cannot be correlated across the four languages of the platform |
| `enums`, `errors` | Shared vocabulary | — |

## Feature specification: what is actually shared

Spark aggregates with Spark SQL over distributed columns; training aggregates
with pandas or NumPy locally. Those cannot be the same code. What is shared is:

1. `FEATURE_SPECS` — a declarative specification with no engine dependency;
2. `compute_features` — a reference implementation fixing exact semantics;
3. a conformance test, added with the Spark job in step 4, asserting the Spark
   translation reproduces the reference output on identical input.

Two semantics are pinned because engines disagree on them silently:

- **standard deviation uses ddof=1** (`numpy.std` defaults to 0, Spark's
  `stddev` is `stddev_samp`, i.e. 1);
- **ratios are computed per sample, then aggregated** — the mean of a ratio is
  not the ratio of the means.

## Running the checks

The toolchain runs in a container so results do not depend on the interpreter
installed locally, and match CI exactly.

```sh
# from the repository root
docker build -f libs/telemetry-core/Dockerfile -t telemetry-core-dev .
docker run --rm -v "$PWD":/workspace -w /workspace/libs/telemetry-core telemetry-core-dev
```

That runs, in order: `ruff check`, `ruff format --check`, `mypy` (strict), and
`pytest`.

To run a single gate:

```sh
docker run --rm -v "$PWD":/workspace -w /workspace/libs/telemetry-core \
  telemetry-core-dev sh -c "pytest -q tests/test_ids.py"
```

With a local Python 3.11 available, the same works without Docker:

```sh
pip install -e "libs/telemetry-core[dev]"
cd libs/telemetry-core && ruff check . && mypy && pytest
```

## Two tests worth knowing about

- **`test_ids.py`** locks the alert identifier derivation to a golden value.
  If the derivation changes, nothing fails loudly — replayed batches simply stop
  colliding with rows already in PostgreSQL and duplicate alerts accumulate.
  This test is the only thing that turns that silent failure into a red build.
- **`test_schema_conformance.py`** decodes every committed example, re-encodes
  it, and validates the *output* against the JSON Schema. Validating only the
  input would miss a renamed field or a changed timestamp format.
