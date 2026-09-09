package com.anomaly.alertservice.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.time.Instant;
import java.util.Map;

/**
 * A window as it arrives on {@code telemetry.scored}.
 *
 * <p>Only the fields the dashboard needs are declared. The message also carries
 * {@code raw_score}, {@code processing_delay_ms}, {@code top_contributors},
 * {@code model} and the full 52-feature vector; none of them is read here, and
 * {@code ignoreUnknown} means their presence costs nothing and their future
 * addition breaks nothing.
 *
 * <p>The five sensor means live inside {@code features}, alongside 47 others.
 * They are pulled out by name rather than persisted wholesale: the dashboard
 * draws five lines, and storing a training matrix to do that would be keeping
 * data nobody reads.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ScoredWindowMessage(
        @JsonProperty("schema_version")
                @Pattern(regexp = "^1\\.[0-9]+$", message = "unsupported major schema version")
                String schemaVersion,
        @JsonProperty("machine_id") @NotNull @Pattern(regexp = "^M-[0-9]{3}$") String machineId,
        @JsonProperty("line_id") @NotNull @Pattern(regexp = "^LINE-[A-Z]$") String lineId,
        @JsonProperty("window_start") @NotNull Instant windowStart,
        @JsonProperty("window_end") @NotNull Instant windowEnd,
        @JsonProperty("sample_count") @NotNull Integer sampleCount,
        @JsonProperty("machine_state") @NotNull String machineState,
        @JsonProperty("is_scored") @NotNull Boolean scored,
        @JsonProperty("anomaly_score") Double anomalyScore,
        @JsonProperty("score_threshold") Double scoreThreshold,
        @JsonProperty("is_anomaly") Boolean anomaly,
        Map<String, Double> features) {

    /** The five physical signals, by their names in the feature specification. */
    public static final String TEMPERATURE = "temperature_c_mean";
    public static final String VIBRATION = "vibration_mm_s_mean";
    public static final String PRESSURE = "pressure_bar_mean";
    public static final String POWER = "power_kw_mean";
    public static final String ROTATION = "rotation_rpm_mean";
    public static final String NULL_RATIO = "null_ratio";

    /**
     * A feature by name, or null when it is absent or not finite.
     *
     * <p>A failed sensor yields a null aggregate. Returning 0.0 instead would
     * put a plausible reading on the chart where there was no measurement at
     * all -- which is exactly the kind of invented data this project refuses.
     */
    public Double feature(String name) {
        if (features == null) {
            return null;
        }
        Double value = features.get(name);
        if (value == null || value.isNaN() || value.isInfinite()) {
            return null;
        }
        return value;
    }

    public boolean hasOrderedWindow() {
        return windowStart != null && windowEnd != null && windowEnd.isAfter(windowStart);
    }
}
