package com.anomaly.alertservice.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * An alert as it arrives on the {@code alerts} topic.
 *
 * <p>The constraints below are transcribed from
 * {@code contracts/json-schema/alert.v1.json}, which is the contract of record.
 * They are declared here rather than trusted because a message that violates
 * them is a <em>data</em> error: it must go to the dead letter queue
 * immediately, not be retried, since replaying an invalid message ten times will
 * not make it valid.
 *
 * <p>{@code @JsonIgnoreProperties(ignoreUnknown = true)} is deliberate and is
 * the consumer half of the schema evolution rule: a producer adding an optional
 * field within the same major version must not break this consumer.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record AlertMessage(
        @JsonProperty("schema_version")
                @NotBlank
                @Pattern(
                        regexp = "^1\\.[0-9]+$",
                        message = "unsupported major schema version; only 1.x is understood")
                String schemaVersion,
        @JsonProperty("alert_id")
                @NotNull
                @Pattern(
                        regexp =
                                "^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
                        message = "alert_id must be a UUIDv5, which is what makes replay idempotent")
                String alertId,
        @JsonProperty("machine_id") @NotNull @Pattern(regexp = "^M-[0-9]{3}$") String machineId,
        @JsonProperty("line_id") @NotNull @Pattern(regexp = "^LINE-[A-Z]$") String lineId,
        @NotNull @Pattern(regexp = "^(MEDIUM|HIGH|CRITICAL)$") String severity,
        @JsonProperty("anomaly_score") @NotNull @DecimalMin("0.0") @DecimalMax("1.0") Double anomalyScore,
        @JsonProperty("score_threshold") @NotNull @DecimalMin("0.0") @DecimalMax("1.0")
                Double scoreThreshold,
        @JsonProperty("detected_at") @NotNull Instant detectedAt,
        @JsonProperty("window_start") @NotNull Instant windowStart,
        @JsonProperty("window_end") @NotNull Instant windowEnd,
        @JsonProperty("published_at") @NotNull Instant publishedAt,
        @JsonProperty("consecutive_windows") @Min(1) Integer consecutiveWindows,
        @JsonProperty("top_contributors") @Valid List<ContributorMessage> topContributors,
        Map<String, Double> features,
        @NotNull @Valid ModelRefMessage model) {

    /** The contract defaults this to 1 when absent. */
    public int consecutiveWindowsOrDefault() {
        return consecutiveWindows == null ? 1 : consecutiveWindows;
    }

    /**
     * The contract requires detected_at to equal window_end, and the producer
     * validates it. Restating it here means a producer bug is caught at the
     * boundary rather than showing an operator the wrong instant.
     */
    public boolean hasConsistentDetectionInstant() {
        return detectedAt != null && detectedAt.equals(windowEnd);
    }

    public boolean hasOrderedWindow() {
        return windowStart != null && windowEnd != null && windowEnd.isAfter(windowStart);
    }
}
