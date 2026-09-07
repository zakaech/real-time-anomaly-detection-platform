"""Rendering the report from measured results only.

The report is generated, never written by hand. Every number it contains is read
out of ``results.json``, which is produced by an actual run. That is the
mechanism behind "no invented metrics": there is no path by which a figure can
reach the document without having been measured first.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

__all__ = ["render_report", "write_precision_recall_plot"]


def _format(value: Any, digits: int = 4) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def write_precision_recall_plot(payload: dict[str, Any], destination: Path) -> None:
    """Plot every model's test precision-recall curve on one pair of axes."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    figure, axes = plt.subplots(figsize=(7.0, 5.0), dpi=140)
    plotted = 0
    for name, model in sorted(payload["models"].items()):
        if model.get("failed"):
            continue
        curve = model["test"]["precision_recall_curve"]
        if not curve["recall"]:
            continue
        axes.plot(curve["recall"], curve["precision"], label=name, linewidth=1.6)
        plotted += 1

    prevalence = payload["dataset"]["splits"]["test"]["prevalence"]
    axes.axhline(
        prevalence,
        color="grey",
        linestyle="--",
        linewidth=1.0,
        label=f"random classifier ({prevalence:.4f})",
    )
    axes.set_xlabel("recall (window level)")
    axes.set_ylabel("precision (window level)")
    axes.set_title("Precision-recall on the held-out test split")
    axes.set_xlim(0.0, 1.0)
    axes.set_ylim(0.0, 1.0)
    axes.grid(alpha=0.25)
    if plotted:
        axes.legend(loc="upper right", fontsize=8)
    figure.tight_layout()
    figure.savefig(destination)
    plt.close(figure)


def _model_comparison_table(payload: dict[str, Any]) -> str:
    header = (
        "| Model | Role | Fit (s) | Test PR-AUC | Test precision | Test recall | Test F1 | "
        "Episode recall | Alerts/machine/h |\n"
        "|---|---|---|---|---|---|---|---|---|\n"
    )
    rows: list[str] = []
    for name, model in sorted(payload["models"].items()):
        if model.get("failed"):
            rows.append(
                f"| `{name}` | {model['role']} | — | **failed**: {model['error']} "
                + "| — " * 5
                + "|\n"
            )
            continue
        metrics = model["test"]["window_metrics"]
        point = model["selected_operating_point"]
        rows.append(
            f"| `{name}` | {model['role']} | {_format(model['fit_seconds'], 2)} | "
            f"{_format(metrics['average_precision'])} | {_format(metrics['precision'])} | "
            f"{_format(metrics['recall'])} | {_format(metrics['f1'])} | "
            f"{_format(model['test']['episode_recall'])} | "
            f"{_format(point['alert_events_per_machine_hour'], 3)} |\n"
        )
    return header + "".join(rows)


def _operating_point_table(model: dict[str, Any]) -> str:
    header = (
        "| Quantile | Threshold | Alert events/h | Per machine/h | Window precision | "
        "Window recall | Episode recall | Median latency (s) |\n"
        "|---|---|---|---|---|---|---|---|\n"
    )
    rows = "".join(
        f"| {point['quantile']} | {_format(point['threshold'])} | "
        f"{_format(point['alert_events_per_hour'], 2)} | "
        f"{_format(point['alert_events_per_machine_hour'], 3)} | "
        f"{_format(point['window_precision'])} | {_format(point['window_recall'])} | "
        f"{_format(point['episode_recall'])} | "
        f"{_format(point['median_detection_latency_seconds'], 1)} |\n"
        for point in model["operating_points"]
    )
    return header + rows


def _split_table(payload: dict[str, Any]) -> str:
    header = (
        "| Split | Windows | Machines | Hours | Anomalous windows | Prevalence | Episodes |\n"
        "|---|---|---|---|---|---|---|\n"
    )
    rows = "".join(
        f"| {name} | {split['windows']} | {split['machines']} | {_format(split['hours'], 2)} | "
        f"{split['anomalous_windows']} | {_format(split['prevalence'])} | {split['episodes']} |\n"
        for name, split in payload["dataset"]["splits"].items()
    )
    return header + rows


def _missed_table(model: dict[str, Any]) -> str:
    by_type = model["test"]["missed_episodes"].get("by_type", {})
    if not by_type:
        return "_No episode was missed on the test split._\n"
    header = "| Anomaly type | Episodes | Detected | Missed | Recall |\n|---|---|---|---|---|\n"
    rows = ""
    for anomaly_type, counts in sorted(by_type.items()):
        total = counts["total"]
        recall = counts["detected"] / total if total else 0.0
        rows += (
            f"| `{anomaly_type}` | {total} | {counts['detected']} | {counts['missed']} | "
            f"{_format(recall)} |\n"
        )
    return header + rows


def _exclusion_notes(payload: dict[str, Any]) -> str:
    """List the candidates that were disqualified, and why."""
    lines = [
        f"- `{name}`: {model['excluded_from_selection']}"
        for name, model in sorted(payload["models"].items())
        if model.get("excluded_from_selection")
    ]
    if not lines:
        return "_Every candidate was eligible._"
    return chr(10).join(lines)


def _covariance_note(payload: dict[str, Any]) -> str:
    """State the measured health of the covariance rather than assert it."""
    model = payload["models"].get("elliptic_envelope", {})
    diagnostics = model.get("covariance_diagnostics")
    if not diagnostics:
        return "_Not measured._"
    return (
        f"Fitted covariance: {diagnostics['covariance_rank']} of "
        f"{diagnostics['covariance_columns']} columns full rank, condition number "
        f"{diagnostics['covariance_condition']:.3e}, "
        f"{diagnostics['dropped_dependent_columns']} dependent column(s) dropped before "
        "fitting."
    )


def _ocsvm_table(payload: dict[str, Any]) -> str:
    scaling = payload["secondary"]["ocsvm_scaling"]
    if not scaling:
        return "_Not measured._\n"
    header = "| Training rows | Fit seconds | Support vectors |\n|---|---|---|\n"
    rows = "".join(
        f"| {entry['training_rows']} | {_format(entry['fit_seconds'], 2)} | "
        f"{entry['support_vectors']} |\n"
        for entry in scaling
    )
    return header + rows


def render_report(payload: dict[str, Any], *, plot_name: str) -> str:
    """Build the Markdown report. Every figure comes from ``payload``."""
    run = payload["run"]
    dataset = payload["dataset"]
    manifest = dataset["manifest"]
    selected_name = payload["selected_model"]
    selected = payload["models"][selected_name]
    point = selected["selected_operating_point"]
    best_f1 = selected["best_f1_operating_point"]
    test_metrics = selected["test"]["window_metrics"]
    clean = payload["secondary"]["clean_training_variant"]
    covariance_note = _covariance_note(payload)
    exclusions = _exclusion_notes(payload)
    settings = run["settings"]

    return f"""# Phase 2 — Anomaly detector: dataset, models, results

Generated on {run["generated_at"]} by `ml-training {run["ml_training_version"]}`.
Every figure below is read from `reports/results.json`, which is written by an
actual training run. Nothing here is hand-entered.

## 1. Dataset

Generated by `event-simulator` in replay mode, exported from Kafka as a bounded
snapshot, and identified by its digest.

| | |
|---|---|
| Telemetry topic | `{manifest["telemetry_topic"]}` |
| Label topic | `{manifest["labels_topic"]}` |
| Telemetry rows | {manifest["telemetry_rows"]} |
| Duplicates removed on `event_id` | {manifest["telemetry_duplicates_removed"]} |
| Decode failures | {manifest["telemetry_decode_failures"]} telemetry, {manifest["labels_decode_failures"]} labels |
| Label rows | {manifest["labels_rows"]} |
| Machines | {manifest["machines"]} |
| Event time span | {manifest["event_time_min"]} → {manifest["event_time_max"]} |
| Simulator seed | {manifest["source_seed"]} |
| `samples.parquet` SHA-256 | `{manifest["samples_sha256"]}` |
| `labels.parquet` SHA-256 | `{manifest["labels_sha256"]}` |

`telemetry.raw` provides the features and nothing else. `telemetry.labels` is
used only to evaluate and to calibrate the threshold; it never enters the
matrix a model is fitted on.

### Splits — temporal, never random

Two windows either side of a random cut are near-identical, which is a leak. The
cut is applied to the samples **before** windowing, so no window straddles a
boundary.

{_split_table(payload)}

Training uses tumbling {settings["window_seconds"]} s windows; validation and
test use the {settings["window_seconds"]} s / {settings["slide_seconds"]} s
sliding cadence of the streaming job, so the measured alert rate is the rate an
operator would actually see.

A window counts as anomalous when at least
{settings["min_anomaly_fraction"]:.0%} of its samples are.

**On prevalence.** Precision depends directly on how common anomalies are. The
test split's prevalence is {_format(dataset["splits"]["test"]["prevalence"])};
at a lower prevalence the same detector would show lower precision at the same
threshold. Any precision figure below should be read together with that number.

## 2. Features

{run["feature_count"]} features, specification version `{run["feature_set_version"]}`,
defined once in `telemetry_core.features` and translated to pandas here. The
translation is checked against the reference implementation by
`tests/test_feature_conformance.py`, which is the mechanism that will let the
Spark job in Phase 3 claim the same semantics.

Covered families: rolling means, rolling standard deviations, deviation from the
moving average (raw and normalised), rate of change, and cross-sensor
correlations — plus per-sample derived ratios and two data-quality features.

## 3. Models

All candidates share the same pipeline — per-machine robust normalisation,
median imputation with missingness indicators, standardisation — so a difference
in results is a difference between models rather than between preprocessing. The
baseline is deliberately not handicapped.

{_model_comparison_table(payload)}

### Candidates excluded from selection

{exclusions}

`three_sigma` sees only the five raw sensor means, which is the rule a plant
already has without any feature engineering. `three_sigma_all_features` is the
same rule given the full feature set; the gap between the two isolates what the
feature engineering contributes, separately from the choice of estimator.

`elliptic_envelope` needs two exclusions, and only the first was anticipated.
`range = max - min` is an exact linear combination, so the covariance becomes
rank-deficient. Measurement then showed a second cause: `sample_count` is
constant for every complete window once the admission gate has run, and a
zero-variance column makes the determinant zero and the condition number
infinite. The detector therefore drops zero-variance columns at fit time as a
general guard rather than a hard-coded list.

scikit-learn does not refuse a singular covariance -- it warns and proceeds --
which is worse than failing, because the model then loads and scores as if
nothing were wrong. So the result is measured rather than assumed:

{covariance_note}

### One-Class SVM: measured cost, not asserted cost

{_ocsvm_table(payload)}

Inference cost is proportional to the number of support vectors, which is what
matters for a job that scores every window of every machine continuously.

![precision-recall curves]({plot_name})

## 4. Threshold

Chosen on **validation**, never on test, and chosen from an alert budget rather
than from the F1 maximum.

| | Quantile | Threshold | Alerts/machine/h | Window F1 | Episode recall |
|---|---|---|---|---|---|
| Budget-selected | {point["quantile"]} | {_format(point["threshold"])} | {_format(point["alert_events_per_machine_hour"], 3)} | {_format(point["window_f1"])} | {_format(point["episode_recall"])} |
| F1-maximising | {best_f1["quantile"]} | {_format(best_f1["threshold"])} | {_format(best_f1["alert_events_per_machine_hour"], 3)} | {_format(best_f1["window_f1"])} | {_format(best_f1["episode_recall"])} |

The budget is {settings["alert_budget_per_machine_hour"]} alert events per
machine per hour. An alert event is a run of at least two consecutive windows
above the threshold on one machine, so one degradation counts once rather than
once per window — which is what an operator receives.

The four detectors produce scores on incomparable scales (an isolation depth, a
distance to a kernel boundary, a maximum z-score). An empirical CDF fitted on
the **training** scores maps each onto its rank in that reference distribution,
so the same quantile means the same nominal alert budget for every model.

### Operating points on validation — selected model

{_operating_point_table(selected)}

## 5. Results for the selected model: `{selected_name}`

Measured once on the held-out test split, with the threshold already frozen.

| | |
|---|---|
| Threshold (calibrated) | {_format(point["threshold"])} |
| Window precision | {_format(test_metrics["precision"])} |
| Window recall | {_format(test_metrics["recall"])} |
| Window F1 | {_format(test_metrics["f1"])} |
| PR-AUC (average precision) | {_format(test_metrics["average_precision"])} |
| Confusion matrix | TP {test_metrics["true_positives"]}, FP {test_metrics["false_positives"]}, TN {test_metrics["true_negatives"]}, FN {test_metrics["false_negatives"]} |
| Episodes on test | {selected["test"]["episodes_total"]} total, {selected["test"]["episodes_scorable"]} scorable |
| Episodes detected | {selected["test"]["episodes_detected"]} |
| Episode recall | {_format(selected["test"]["episode_recall"])} |
| Median detection latency | {_format(selected["test"]["median_detection_latency_seconds"], 1)} s |

PR-AUC rather than ROC-AUC: with a small minority of anomalous windows the
false-positive rate barely moves, and ROC-AUC flatters even a weak model.

Episodes that overlap no scorable window are excluded from the recall
denominator. A fault occurring entirely while a machine is stopped is
undetectable by design, and charging it to the model would flatter whoever chose
the admission rule.

### False positives

{_format(selected["test"]["false_positives"].get("count"))} false positives,
{_format(selected["test"]["false_positives"].get("share_of_alerts"))} of all
alerts at this threshold.

By machine: `{selected["test"]["false_positives"].get("by_machine")}`

By machine state: `{selected["test"]["false_positives"].get("by_machine_state")}`

### Missed episodes

{_missed_table(selected)}

Median duration of a missed episode:
{_format(selected["test"]["missed_episodes"].get("median_duration_seconds"), 1)} s,
against {_format(selected["test"]["missed_episodes"].get("median_duration_detected_seconds"), 1)} s
for detected ones.

## 6. Secondary experiment: training on label-filtered data

Not the main protocol. Filtering the training set by the labels uses information
production does not have, so any gain it shows is unavailable in practice
(decision D-27). Measured to put a number on the gap rather than leave it as an
assertion.

Both columns use the **same estimator** (`{clean["detector"]}`), so the
difference is the effect of the filter and not of a change of model.

| | Contaminated (main) | Label-filtered (secondary) |
|---|---|---|
| Training rows | {selected["training_rows"]} | {clean["training_rows"]} |
| Anomalous rows removed | 0 | {clean["removed_by_label_filter"]} |
| Test precision | {_format(test_metrics["precision"])} | {_format(clean["test_window_metrics"]["precision"])} |
| Test recall | {_format(test_metrics["recall"])} | {_format(clean["test_window_metrics"]["recall"])} |
| Test F1 | {_format(test_metrics["f1"])} | {_format(clean["test_window_metrics"]["f1"])} |
| Test episode recall | {_format(selected["test"]["episode_recall"])} | {_format(clean["test_episode_recall"])} |

## 7. Reproducibility

| | |
|---|---|
| Training seed | {run["seed"]} |
| Python | {run["python_version"]} |
| scikit-learn | {run["sklearn_version"]} |
| NumPy | {run["numpy_version"]} |
| pandas | {run["pandas_version"]} |
| Feature set | `{run["feature_set_version"]}` |
| Dataset | `{dataset["directory"]}`, samples SHA-256 `{manifest["samples_sha256"][:16]}…` |
| Artefact | `{payload["artifact"]["directory"]}`, SHA-256 `{str(payload["artifact"]["sha256"])[:16]}…` |

```sh
# 1. generate the history (same seed => same data)
docker run --rm --network anomaly-platform_anomaly-net \\
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 event-simulator \\
  python -m event_simulator.main --mode replay \\
  --duration-seconds {int(dataset["splits"]["test"]["hours"] * 3600 * 3)} --seed {manifest["source_seed"]}

# 2. snapshot it
docker run --rm --network anomaly-platform_anomaly-net \\
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 ml-training \\
  python -m ml_training.cli export --destination {dataset["directory"]} \\
  --source-seed {manifest["source_seed"]}

# 3. train, evaluate, write the artefact and this report
docker run --rm ml-training python -m ml_training.cli train \\
  --dataset {dataset["directory"]} --seed {run["seed"]}
```

## 8. Limits

* The ground truth comes from a simulator we wrote. The guards against a
  complacent generator are in `docs/07-ml-methodology.md`, and the mandatory
  trivial baseline in the table above is the main one: if the baseline is not
  clearly beaten, the machine learning earns nothing here.
* Precision is reported at the measured prevalence and does not transfer to a
  plant with a different one.
* The threshold is calibrated globally. Per-machine thresholds would handle
  fleet heterogeneity better but need a history per machine and turn operations
  into managing as many thresholds as there are machines (decision D-07).
* Short spikes are diluted in a 60 s window by construction. The missed-episode
  table shows which anomaly types this costs.
* Unpickling the artefact requires `ml_training.models` to be importable,
  because the per-machine normaliser is a custom transformer. Phase 3 either
  installs this package or the inference classes move to a small shared library.

## 9. Next steps for the Spark integration

1. Translate the feature specification to Spark SQL, using `min_by`/`max_by`
   for every "first"/"last" — `first()`/`last()` are non-deterministic after a
   shuffle and would make the conformance test fail intermittently.
2. Reuse `tests/test_feature_conformance.py` as the template: same reference,
   same tolerance, same random windows.
3. Load the artefact once per executor behind a lazy singleton, checking the
   SHA-256, the scikit-learn version and the feature-set version at startup — a
   mismatch must fail the job, not change the scores quietly.
4. Apply the admission gate before scoring, and publish a `skip_reason` for
   windows that are not scored, so a scoring outage never looks like a healthy
   plant.
"""
