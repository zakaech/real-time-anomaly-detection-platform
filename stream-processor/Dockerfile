# Spark Structured Streaming job.
#
# Build context is the REPOSITORY ROOT: this component depends on
# libs/telemetry-core and on ml-training, whose classes the model pickle
# references (decision D-35).
#
#   docker build -f stream-processor/Dockerfile -t stream-processor .
#
# Base image and JDK are a verified constraint, not a preference. Spark 3.5 is
# certified for Java 8/11/17. The current python:3.11-slim tag moved to Debian
# 13, which ships Java 21 only, and Arrow inside Spark then fails with
#   sun.misc.Unsafe or java.nio.DirectByteBuffer.<init>(long,int) not available
# Pinning bookworm keeps openjdk-17 available.
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

RUN apt-get update -qq \
 && apt-get install -y -qq --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY libs/telemetry-core/pyproject.toml libs/telemetry-core/README.md libs/telemetry-core/
COPY libs/telemetry-core/src libs/telemetry-core/src
RUN pip install --no-cache-dir -e libs/telemetry-core

COPY ml-training/pyproject.toml ml-training/README.md ml-training/
COPY ml-training/src ml-training/src
RUN pip install --no-cache-dir -e ml-training

COPY stream-processor/pyproject.toml stream-processor/README.md stream-processor/
COPY stream-processor/src stream-processor/src
RUN pip install --no-cache-dir -e "stream-processor[dev]"

# Resolve the Kafka connector at build time so a run never depends on Maven
# being reachable. The jars land in the Ivy cache that Spark reads at startup.
ENV SPARK_KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
RUN python -c "\
from pyspark.sql import SparkSession; \
import os; \
s = (SparkSession.builder.appName('warm-ivy').master('local[1]') \
     .config('spark.jars.packages', os.environ['SPARK_KAFKA_PACKAGE']) \
     .config('spark.ui.enabled','false').getOrCreate()); \
print('ivy cache warmed'); s.stop()"

COPY contracts contracts
COPY stream-processor/tests stream-processor/tests

WORKDIR /workspace/stream-processor

CMD ["sh", "-c", "ruff check . && ruff format --check . && mypy && pytest -q"]
