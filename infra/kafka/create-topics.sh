#!/bin/sh
# Creates and reconciles the Kafka topology described in docs/03-kafka-topology.md.
#
# The script is idempotent and convergent: it creates missing topics, then always
# reapplies the declared configuration to existing ones. Running it twice is a
# no-op; running it after changing a retention below is a configuration update.
#
# What it deliberately does NOT do: reduce a partition count. Kafka cannot, and
# increasing it later changes hash(key) % partitions, which breaks per-key
# ordering at the boundary. Partition counts are chosen with headroom instead.

set -eu

KAFKA_BIN="/opt/kafka/bin"
BOOTSTRAP="${KAFKA_BOOTSTRAP:?KAFKA_BOOTSTRAP is required}"

# Retention windows, in milliseconds, with the reasoning from docs/03 section 8.
MS_3_DAYS=259200000     # display and short-term drift analysis, no archive value
MS_7_DAYS=604800000     # replay window: long enough to find, fix and rerun
MS_30_DAYS=2592000000   # PostgreSQL is the system of record; this is catch-up only
MS_90_DAYS=7776000000   # labels are small, rare and durably useful for training

# name <TAB> partitions <TAB> retention.ms
# Partition rationale, per topic:
#   6 -> hot path, divisible by 1/2/3/6 so any consumer group size up to 6 balances
#   3 -> alert volume is orders of magnitude lower; matches Spring concurrency=3
#   1 -> dead letter and late data: read end to end by a human, never parallelised
TOPICS=$(cat <<EOF
${TOPIC_TELEMETRY_RAW}	6	${MS_7_DAYS}
${TOPIC_TELEMETRY_SCORED}	6	${MS_3_DAYS}
${TOPIC_ALERTS}	3	${MS_30_DAYS}
${TOPIC_TELEMETRY_LABELS}	3	${MS_90_DAYS}
${TOPIC_TELEMETRY_DLQ}	1	${MS_30_DAYS}
${TOPIC_TELEMETRY_LATE}	1	${MS_7_DAYS}
EOF
)

# Single-node development broker. Production uses 3 / 2, see docs/03 section 4.
REPLICATION_FACTOR=1

echo "[kafka-init] bootstrap=${BOOTSTRAP}"

printf '%s\n' "${TOPICS}" | while IFS='	' read -r name partitions retention; do
  [ -z "${name}" ] && continue

  echo "[kafka-init] ensuring topic ${name} (partitions=${partitions}, retention.ms=${retention})"

  "${KAFKA_BIN}/kafka-topics.sh" \
    --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${name}" \
    --partitions "${partitions}" \
    --replication-factor "${REPLICATION_FACTOR}"

  # Always reapply: --if-not-exists skips creation but leaves an existing topic
  # with its old configuration, so creation alone is not convergent.
  #
  # cleanup.policy=delete on every topic: these are event streams where each
  # message is a distinct fact. Compaction keeps only the last value per key,
  # which models state, not events (docs/03 section 8).
  "${KAFKA_BIN}/kafka-configs.sh" \
    --bootstrap-server "${BOOTSTRAP}" \
    --alter --entity-type topics --entity-name "${name}" \
    --add-config "retention.ms=${retention},cleanup.policy=delete,compression.type=producer" \
    >/dev/null

done

echo "[kafka-init] topology after reconciliation:"
"${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --describe \
  | grep -E '^Topic:' \
  | sed 's/^/[kafka-init]   /'

echo "[kafka-init] done"
