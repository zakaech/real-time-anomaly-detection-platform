# ml-training

Offline training and evaluation of the anomaly detector. Produces one artefact
carrying the whole inference path, so the streaming job loads it and
reimplements no machine learning.

The measured results live in [`reports/model-report.md`](reports/model-report.md),
generated from `reports/results.json` by an actual run. No figure in that report
is hand-entered — there is no code path by which one could be.

## Pipeline

```
telemetry.raw ──┐
                ├─► export ─► data/<run>/{samples,labels}.parquet + manifest.json
telemetry.labels┘                    │
                                     ▼
                   split by time ─► window ─► features (52, spec v2.0.0)
                                     │              │
                                     │              └─ labels joined AFTER, by
                                     │                 (machine_id, window_start)
                                     ▼
      per-machine normalisation ─► imputation (+ indicators) ─► scaling ─► detector
                                     ▼
                       ECDF calibration ─► threshold ─► artefact
```

## Anti-leakage, in the order it is enforced

1. **Split the samples by time, then window each split.** Windowing first would
   let a window straddle the boundary and carry training data into validation.
2. Features come from `telemetry.raw` alone; labels are attached afterwards by
   `(machine_id, window_start)`, into a **separate frame** that is never
   concatenated with the features.
3. Normaliser, imputer, scaler, detector and the calibration grid are fitted on
   **train only**.
4. The threshold is chosen on **validation**.
5. Test is touched **once**, with the threshold already frozen.

`tests/test_leakage.py` checks the two that could regress silently: the model's
input is exactly `machine_id` plus the 52 features, and permuting the labels
changes no score.

**Training data is not filtered by the labels** (decision D-27). Production
cannot know which windows are clean, so filtering them would hand the model
information it will never have. The filtered variant is measured as a secondary
experiment to put a number on the gap.

## Models

| | Hypothesis | Cost | Role |
|---|---|---|---|
| Isolation Forest | anomalies isolate in few random axis-aligned cuts | cheap to fit, `O(t log n)` to score | mandatory |
| One-Class SVM (RBF) | normal points are compact in kernel space | `O(n²)`–`O(n³)` to fit; scoring scales with support vectors | mandatory |
| 3-sigma | each sensor is Gaussian and independent | negligible | mandatory baseline |
| 3-sigma, all features | same rule, full feature set | negligible | secondary — isolates what the features contribute |
| Elliptic Envelope | normal data is roughly elliptical | moderate | exploratory |

All share the same preprocessing, so a difference in results is a difference
between models rather than between pipelines. The baseline is deliberately not
handicapped: beating a crippled baseline would prove nothing.

The 3-sigma baseline sees only the five raw sensor means, because that is the
rule a plant already has without any feature engineering.

`elliptic_envelope` drops the `*_range` columns: `range = max - min` is an exact
linear combination, so the covariance is rank-deficient. scikit-learn does not
refuse it — it warns and proceeds with a condition number at the edge of float64
precision, which is worse than failing.

## Threshold

Raw scores are incomparable across models (an isolation depth, a distance to a
kernel boundary, a maximum z-score). An ECDF fitted on the **training** scores
maps each onto its rank in that reference distribution, so `0.995` means the
same thing — and the same nominal alert budget — for every candidate.

The operating point is then chosen from **what an operator can absorb**, not
from the F1 maximum. Both are reported; the gap between them is part of the
result. An alert event is a run of at least two consecutive windows above the
threshold on one machine, so one degradation counts once rather than once per
window.

## Artefact

```
artifacts/<model>/<version>/
├── model.joblib            the whole inference path, plus calibration and threshold
├── metadata.json           versions, hyperparameters, feature order, threshold,
│                           metrics, dataset provenance, SHA-256
├── calibration.json        the ECDF grid
├── machine_profiles.json   per-machine median/IQR, plus the fleet fallback
└── reference_profile.json  training distribution, for the Phase 4 drift work
```

Loading **verifies rather than trusts**: a mismatch in the digest, the
scikit-learn version, the feature-set version or the column order fails at load.
Each of those has the same failure mode if unchecked — the model loads, scores,
and is quietly wrong.

## Running

```sh
docker build -f ml-training/Dockerfile -t ml-training .

# 1. snapshot Kafka into a Parquet dataset with a digest
docker run --rm --network anomaly-platform_anomaly-net \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -v "$PWD/data":/workspace/data ml-training \
  python -m ml_training.cli export --destination ../data/run-01 --source-seed 424242

# 2. train, evaluate, write the artefact and the report
docker run --rm -v "$PWD":/workspace -w /workspace/ml-training ml-training \
  python -m ml_training.cli train --dataset ../data/run-01
```

`export` needs Kafka; `train` needs only the dataset file and a seed. That split
is what makes "reproduce the training" a statement about one command and one
digest rather than about the state of a broker.

## Checks

```sh
docker run --rm -v "$PWD":/workspace -w /workspace/ml-training ml-training \
  sh -c "ruff check . && ruff format --check . && mypy && pytest -q"
```

`tests/test_feature_conformance.py` is the one to know about: it asserts that
the pandas translation reproduces the shared reference implementation on random
windows, including the awkward cases — heavy missing data, a frozen sensor, a
window with too few samples. It is the template the Spark job will reuse.

Bit-exact equality is neither achievable nor required there: `math.fsum` and
pandas' pairwise summation add in different orders. The tolerance is tight
enough that a real semantic difference — a ddof mismatch, a positional "last" —
fails it by many orders of magnitude.

## Known coupling

Unpickling the artefact requires `ml_training.models` to be importable, because
the per-machine normaliser is a custom transformer. Phase 3 either installs this
package in the Spark image or the inference classes move to a small shared
library. `matplotlib` is deliberately an optional extra so the runtime install
stays light.
