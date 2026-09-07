"""Command line: export a dataset, train and evaluate, render the report.

Three commands rather than one, because they have different reproducibility
properties. ``export`` depends on Kafka and produces a file with a digest;
``train`` depends only on that file and a seed; ``report`` depends only on the
measured results. Splitting them is what makes "reproduce the training" a
statement about one command and one file rather than about the state of a
broker.
"""

from __future__ import annotations

import argparse
import json
import platform
import sys
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import sklearn
from telemetry_core.config import KafkaSettings, KafkaTopics, LoggingSettings, load_settings
from telemetry_core.errors import ConfigurationError
from telemetry_core.features import FEATURE_NAMES, FEATURE_SET_VERSION
from telemetry_core.logging import configure_logging, get_logger
from telemetry_core.timeutil import format_instant

from ml_training import __version__
from ml_training.artifact import CalibratedAnomalyModel, ModelMetadata, save_artifact
from ml_training.config.settings import TrainingSettings
from ml_training.dataset.export import (
    LABELS_FILE,
    SAMPLES_FILE,
    export_dataset,
    load_manifest,
)
from ml_training.report import render_report, write_precision_recall_plot
from ml_training.training import (
    artifact_directory,
    measure_clean_training_variant,
    measure_ocsvm_scaling,
    reference_profile,
    run_training,
    selection_exclusion,
)

__all__ = ["main"]

_LOGGER = get_logger("ml_training.cli")
_SERVICE = "ml-training"

RESULTS_FILE = "results.json"
REPORT_FILE = "model-report.md"
PR_CURVE_FILE = "precision-recall.png"


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="ml-training", description="Offline detector training.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    export = subparsers.add_parser("export", help="Snapshot Kafka into Parquet")
    export.add_argument("--destination", type=Path, required=True)
    export.add_argument("--source-seed", type=int, help="Simulator seed that produced the history")
    export.add_argument("--source-note", type=str)

    train = subparsers.add_parser("train", help="Train, evaluate and write the artefact")
    train.add_argument("--dataset", type=Path, required=True)
    train.add_argument("--model-version", default="2.0.0")
    train.add_argument("--seed", type=int)

    report = subparsers.add_parser("report", help="Render the report from measured results")
    report.add_argument("--results", type=Path)

    return parser.parse_args(argv)


def _configure() -> TrainingSettings:
    logging_settings = load_settings(LoggingSettings)
    configure_logging(service=_SERVICE, version=__version__, level=logging_settings.log_level)
    return TrainingSettings()


def _command_export(args: argparse.Namespace) -> int:
    try:
        kafka_settings = load_settings(KafkaSettings)
        topics = load_settings(KafkaTopics)
    except ConfigurationError as exc:
        _LOGGER.error("configuration_invalid", error=str(exc))
        return 2

    manifest = export_dataset(
        kafka_settings=kafka_settings,
        topics=topics,
        destination=args.destination,
        source_seed=args.source_seed,
        source_note=args.source_note,
    )
    print(manifest.to_json())
    return 0


def _load_dataset(directory: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    samples = pd.read_parquet(directory / SAMPLES_FILE)
    labels = pd.read_parquet(directory / LABELS_FILE)
    for frame, column in ((samples, "event_time"), (labels, "event_time")):
        if not frame.empty:
            frame[column] = pd.to_datetime(frame[column], utc=True)
    return samples, labels


def _select_model(results: dict[str, Any]) -> str:
    """Pick on validation, never on test.

    Ranked by episode recall at the budgeted operating point -- the question an
    operator asks -- with window F1 as the tie-break. Failed candidates are
    excluded here but stay in the report.
    """
    for payload in results.values():
        payload["excluded_from_selection"] = selection_exclusion(payload)

    usable = {
        name: payload
        for name, payload in results.items()
        if payload["excluded_from_selection"] is None
    }
    if not usable:
        raise RuntimeError("every candidate was excluded from selection")
    return max(
        usable,
        key=lambda name: (
            usable[name]["selected_operating_point"]["episode_recall"],
            usable[name]["selected_operating_point"]["window_f1"],
        ),
    )


def _command_train(args: argparse.Namespace, settings: TrainingSettings) -> int:
    if args.seed is not None:
        settings = settings.model_copy(update={"seed": args.seed})

    dataset_dir = args.dataset
    manifest = load_manifest(dataset_dir)
    samples, labels = _load_dataset(dataset_dir)

    _LOGGER.info(
        "training_started",
        seed=settings.seed,
        dataset=str(dataset_dir),
        samples=len(samples),
        labels=len(labels),
        feature_set_version=FEATURE_SET_VERSION,
    )

    splits, results = run_training(samples, labels, settings)
    rng = np.random.default_rng(settings.seed + 1)

    selected_name = _select_model(results)
    selected = results[selected_name]
    _LOGGER.info("model_selected", model=selected_name)

    # Secondary experiments run after selection, so the label-filtered variant
    # uses the same estimator as the selected model. Comparing two different
    # estimators would measure the wrong thing.
    secondary = {
        "ocsvm_scaling": measure_ocsvm_scaling(splits, settings, rng),
        "clean_training_variant": measure_clean_training_variant(
            splits, settings, detector_name=selected_name
        ),
    }

    artifacts_dir = settings.artifacts_dir
    reports_dir = settings.reports_dir
    reports_dir.mkdir(parents=True, exist_ok=True)

    model = CalibratedAnomalyModel(
        pipeline=selected["_pipeline"],
        calibrator=selected["_calibrator"],
        threshold=selected["_threshold"],
        model_name=selected_name,
        model_version=args.model_version,
    )
    metadata = ModelMetadata(
        model_name=selected_name,
        model_version=args.model_version,
        trained_at=format_instant(datetime.now(tz=UTC)),
        feature_set_version=FEATURE_SET_VERSION,
        feature_names=list(FEATURE_NAMES),
        hyperparameters=selected["hyperparameters"],
        threshold=selected["_threshold"],
        threshold_quantile=selected["selected_operating_point"]["quantile"],
        threshold_rule=(
            "most permissive validation quantile whose alert-event rate stays within "
            f"{settings.alert_budget_per_machine_hour} per machine per hour"
        ),
        training_rows=selected["training_rows"],
        training_seed=settings.seed,
        python_version=platform.python_version(),
        sklearn_version=sklearn.__version__,
        numpy_version=np.__version__,
        pandas_version=pd.__version__,
        dataset={
            "directory": str(dataset_dir),
            "manifest": asdict(manifest),
            "splits": splits.summary(),
            "window_seconds": settings.window_seconds,
            "slide_seconds": settings.slide_seconds,
            "min_anomaly_fraction": settings.min_anomaly_fraction,
            "training_contaminated": True,
        },
        metrics={
            "validation_selected": selected["selected_operating_point"],
            "test": selected["test"]["window_metrics"],
            "test_episode_recall": selected["test"]["episode_recall"],
        },
        notes={
            "training_policy": (
                "Trained on unfiltered windows: production cannot know which are clean, "
                "so filtering by labels would make every metric optimistic (decision D-27)."
            ),
            "prevalence_caveat": (
                "Precision depends directly on the prevalence of anomalies in the "
                "evaluation set; the measured prevalence is recorded in dataset.splits."
            ),
        },
    )

    target = artifact_directory(artifacts_dir, selected_name, args.model_version)
    metadata = save_artifact(
        target,
        model=model,
        metadata=metadata,
        machine_profiles=selected["_pipeline"].named_steps["per_machine"].to_profile_dict(),
        reference_profile=reference_profile(splits.train.features),
    )

    payload: dict[str, Any] = {
        "run": {
            "generated_at": format_instant(datetime.now(tz=UTC)),
            "seed": settings.seed,
            "ml_training_version": __version__,
            "feature_set_version": FEATURE_SET_VERSION,
            "feature_count": len(FEATURE_NAMES),
            "python_version": platform.python_version(),
            "sklearn_version": sklearn.__version__,
            "numpy_version": np.__version__,
            "pandas_version": pd.__version__,
            "settings": settings.model_dump(mode="json"),
        },
        "dataset": {
            "directory": str(dataset_dir),
            "manifest": asdict(manifest),
            "splits": splits.summary(),
        },
        "models": {
            name: {key: value for key, value in payload.items() if not key.startswith("_")}
            for name, payload in results.items()
        },
        "secondary": secondary,
        "selected_model": selected_name,
        "artifact": {
            "directory": str(target),
            "sha256": metadata.artifact_sha256,
            "threshold": metadata.threshold,
            "threshold_quantile": metadata.threshold_quantile,
        },
    }

    results_path = reports_dir / RESULTS_FILE
    results_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    plot_path = reports_dir / PR_CURVE_FILE
    write_precision_recall_plot(payload, plot_path)

    report_path = reports_dir / REPORT_FILE
    report_path.write_text(render_report(payload, plot_name=PR_CURVE_FILE), encoding="utf-8")

    _LOGGER.info(
        "training_finished",
        selected_model=selected_name,
        threshold=metadata.threshold,
        artifact=str(target),
        results=str(results_path),
        report=str(report_path),
    )
    print(f"selected model : {selected_name}")
    print(f"threshold      : {metadata.threshold:.6f}")
    print(f"artifact       : {target}")
    print(f"report         : {report_path}")
    return 0


def _command_report(args: argparse.Namespace, settings: TrainingSettings) -> int:
    results_path = args.results or (settings.reports_dir / RESULTS_FILE)
    payload = json.loads(results_path.read_text(encoding="utf-8"))
    report_path = settings.reports_dir / REPORT_FILE
    report_path.write_text(render_report(payload, plot_name=PR_CURVE_FILE), encoding="utf-8")
    print(f"report written to {report_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    settings = _configure()

    if args.command == "export":
        return _command_export(args)
    if args.command == "train":
        return _command_train(args, settings)
    if args.command == "report":
        return _command_report(args, settings)
    return 2  # pragma: no cover - argparse enforces the choices


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
