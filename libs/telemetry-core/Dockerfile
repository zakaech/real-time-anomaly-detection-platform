# Development and CI toolchain for telemetry-core.
#
# The build context is the REPOSITORY ROOT, not this directory: the conformance
# tests validate against contracts/ , which is shared by four languages and
# therefore lives above this package (docs/08 section 1).
#
#   docker build -f libs/telemetry-core/Dockerfile -t telemetry-core-dev .
#   docker run --rm -v "$PWD":/workspace -w /workspace/libs/telemetry-core telemetry-core-dev
#
# Python 3.11 rather than the interpreter that happens to be installed locally:
# it is the version PySpark 3.5 supports, and the one CI runs, so a check that
# passes here passes there.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/workspace/libs/telemetry-core/src

WORKDIR /workspace

# Dependency layer first: it is invalidated only by a pyproject change, so
# editing source or tests does not reinstall anything.
COPY libs/telemetry-core/pyproject.toml libs/telemetry-core/README.md libs/telemetry-core/
COPY libs/telemetry-core/src libs/telemetry-core/src
RUN pip install --no-cache-dir -e "libs/telemetry-core[dev]"

COPY contracts contracts
COPY libs/telemetry-core/tests libs/telemetry-core/tests

WORKDIR /workspace/libs/telemetry-core

# The same three gates as CI, in the order that fails fastest.
CMD ["sh", "-c", "ruff check . && ruff format --check . && mypy && pytest -q"]
