# Runtime and toolchain image for the event simulator.
#
# Build context is the REPOSITORY ROOT: the simulator depends on
# libs/telemetry-core, and the conformance tests validate against contracts/.
#
#   docker build -f event-simulator/Dockerfile -t event-simulator .
#
# Python 3.11 rather than whatever is installed locally: it is the version
# PySpark 3.5 supports and the one CI runs, so a check that passes here passes
# there.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/workspace/libs/telemetry-core/src:/workspace/event-simulator/src

WORKDIR /workspace

# telemetry-core first, in editable mode. pip then sees the `telemetry-core`
# requirement of event-simulator as already satisfied and does not reach out to
# an index for a package that is not published anywhere.
COPY libs/telemetry-core/pyproject.toml libs/telemetry-core/README.md libs/telemetry-core/
COPY libs/telemetry-core/src libs/telemetry-core/src
RUN pip install --no-cache-dir -e libs/telemetry-core

COPY event-simulator/pyproject.toml event-simulator/README.md event-simulator/
COPY event-simulator/src event-simulator/src
RUN pip install --no-cache-dir -e "event-simulator[dev]"

COPY contracts contracts
COPY event-simulator/config event-simulator/config
COPY event-simulator/tests event-simulator/tests
COPY libs/telemetry-core/tests libs/telemetry-core/tests

WORKDIR /workspace/event-simulator

# Realtime generation by default. Compose overrides the command for a replay.
CMD ["python", "-m", "event_simulator.main"]
