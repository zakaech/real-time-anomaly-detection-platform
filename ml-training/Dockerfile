# Training and evaluation toolchain.
#
# Build context is the REPOSITORY ROOT: this component depends on
# libs/telemetry-core and validates against contracts/.
#
#   docker build -f ml-training/Dockerfile -t ml-training .
#
# This one stays root, unlike event-simulator and stream-processor. It is a
# toolchain image, run with the repository bind-mounted from the host
# (`-v "$PWD":/workspace`), and it writes back into that mount: training
# artefacts, reports, and the ruff/mypy/pytest caches. A container user whose
# uid does not match the host owner cannot write there, so dropping root would
# break the image at its actual job. Nothing here is exposed to a network.
#
# Not multi-stage either, and for a reason that can be checked rather than
# asserted: there is no build stage to discard. No compiler or -dev package is
# installed, so a runtime stage would copy everything the first stage held.
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
