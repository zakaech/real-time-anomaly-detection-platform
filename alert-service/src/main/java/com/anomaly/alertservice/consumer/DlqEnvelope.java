package com.anomaly.alertservice.consumer;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.time.Instant;
import java.util.Base64;

/**
 * A rejected message, in the shape defined by
 * {@code contracts/json-schema/telemetry-dlq.v1.json}.
 *
 * <p>The same envelope the Python components emit (ADR-002), reused rather than
 * reinvented: the contract already carries {@code source_topic}, so one dead
 * letter queue can serve both the Spark job and this service, and there is one
 * place to watch instead of two. A conformance test replays this against the
 * schema of record rather than trusting the two implementations to agree.
 *
 * <p>{@code raw_payload} holds the <strong>original bytes</strong>, base64 and
 * otherwise untouched. Replay after a fix is only possible on that condition.
 *
 * <p>What belongs here is only <em>data</em> errors. A broker outage or a
 * database failure is infrastructure, handled by retry; routing those here would
 * discard valid alerts during an incident, which is exactly when losing them
 * costs most.
 */
@JsonPropertyOrder({
    "schema_version",
    "dlq_reason",
    "dlq_detail",
    "source_topic",
    "source_partition",
    "source_offset",
    "failed_at",
    "processor_version",
    "raw_payload",
    "raw_key"
})
public record DlqEnvelope(
        @JsonProperty("schema_version") String schemaVersion,
        @JsonProperty("dlq_reason") String dlqReason,
        @JsonProperty("dlq_detail") String dlqDetail,
        @JsonProperty("source_topic") String sourceTopic,
        @JsonProperty("source_partition") int sourcePartition,
        @JsonProperty("source_offset") long sourceOffset,
        @JsonProperty("failed_at") Instant failedAt,
        @JsonProperty("processor_version") String processorVersion,
        @JsonProperty("raw_payload") String rawPayload,
        @JsonProperty("raw_key") String rawKey) {

    private static final String SCHEMA_VERSION = "1.0";

    public static DlqEnvelope wrap(
            byte[] payload,
            byte[] key,
            String reason,
            String detail,
            String topic,
            int partition,
            long offset,
            String processorVersion) {
        Base64.Encoder encoder = Base64.getEncoder();
        return new DlqEnvelope(
                SCHEMA_VERSION,
                reason,
                // The contract refuses an empty detail: a rejection nobody can
                // act on is a log line, not a dead letter.
                detail == null || detail.isBlank() ? "no detail provided" : detail,
                topic,
                partition,
                offset,
                Instant.now(),
                processorVersion,
                encoder.encodeToString(payload == null ? new byte[0] : payload),
                key == null ? null : encoder.encodeToString(key));
    }
}
