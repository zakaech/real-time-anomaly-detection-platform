# Training and evaluation toolchain.
#
# Build context is the REPOSITORY ROOT: this component depends on
# libs/telemetry-core and validates against contracts/.
#
#   docker build -f ml-training/Dockerfile -t ml-training .
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/workspace/libs/telemetry-core/src:/workspace/ml-training/src \
    MPLBACKEND=Agg

WORKDIR /workspace

COPY libs/telemetry-core/pyproject.toml libs/telemetry-core/README.md libs/telemetry-core/
COPY libs/telemetry-core/src libs/telemetry-core/src
RUN pip install --no-cache-dir -e libs/telemetry-core

COPY ml-training/pyproject.toml ml-training/README.md ml-training/
COPY ml-training/src ml-training/src
RUN pip install --no-cache-dir -e "ml-training[dev]"

COPY contracts contracts
COPY ml-training/tests ml-training/tests

WORKDIR /workspace/ml-training

CMD ["sh", "-c", "ruff check . && ruff format --check . && mypy && pytest -q"]
