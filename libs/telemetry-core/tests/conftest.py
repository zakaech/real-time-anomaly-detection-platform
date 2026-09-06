"""Shared fixtures.

The contract files live at the repository root, outside this package, because
they are consumed by four languages. Tests therefore walk up from their own
location rather than assuming a working directory.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

# tests -> telemetry-core -> libs -> repository root
_REPO_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return _REPO_ROOT


@pytest.fixture(scope="session")
def schema_dir(repo_root: Path) -> Path:
    directory = repo_root / "contracts" / "json-schema"
    if not directory.is_dir():
        pytest.fail(f"contract schemas not found at {directory}")
    return directory


@pytest.fixture(scope="session")
def example_dir(repo_root: Path) -> Path:
    directory = repo_root / "contracts" / "examples"
    if not directory.is_dir():
        pytest.fail(f"contract examples not found at {directory}")
    return directory


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        payload: dict[str, Any] = json.load(handle)
    return payload
