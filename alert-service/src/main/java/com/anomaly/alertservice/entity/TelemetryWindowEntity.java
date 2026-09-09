package com.anomaly.alertservice.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * One scored window of one machine, kept so the dashboard can draw a real
 * sensor curve (decision D-41).
 *
 * <p>Read-only from JPA. Writes go through the native upsert, for the same
 * reason alerts do: telemetry.scored is published in update mode, so the same
 * window arrives repeatedly and only the database can collapse that atomically.
 *
 * <p>The five sensor means are stored and the other 47 features are not. The
 * dashboard draws five lines; persisting a training matrix to do that would be
 * storing data nobody reads.
 */
@Entity
@Table(name = "telemetry_window")
public class TelemetryWindowEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "machine_id", nullable = false)
    private Long machineId;

    @Column(name = "window_start", nullable = false)
    private Instant windowStart;

    @Column(name = "window_end", nullable = false)
    private Instant windowEnd;

    /** Rises across successive emissions of the same window as it fills. */
    @Column(name = "sample_count", nullable = false)
    private int sampleCount;

    @Column(name = "machine_state", nullable = false, length = 16)
    private String machineState;

    @Column(name = "is_scored", nullable = false)
    private boolean scored;

    @Column(name = "anomaly_score")
    private Double anomalyScore;

    @Column(name = "score_threshold")
    private Double scoreThreshold;

    @Column(name = "is_anomaly")
    private Boolean anomaly;

    @Column(name = "temperature_c_mean")
    private Double temperatureCMean;

    @Column(name = "vibration_mm_s_mean")
    private Double vibrationMmSMean;

    @Column(name = "pressure_bar_mean")
    private Double pressureBarMean;

    @Column(name = "power_kw_mean")
    private Double powerKwMean;

    @Column(name = "rotation_rpm_mean")
    private Double rotationRpmMean;

    /** Explains a gap in a curve instead of leaving it unexplained. */
    @Column(name = "null_ratio")
    private Double nullRatio;

    @Column(name = "updated_at", nullable = false, insertable = false, updatable = false)
    private Instant updatedAt;

    protected TelemetryWindowEntity() {
        // JPA
    }

    public Long getId() {
        return id;
    }

    public Long getMachineId() {
        return machineId;
    }

    public Instant getWindowStart() {
        return windowStart;
    }

    public Instant getWindowEnd() {
        return windowEnd;
    }

    public int getSampleCount() {
        return sampleCount;
    }

    public String getMachineState() {
        return machineState;
    }

    public boolean isScored() {
        return scored;
    }

    public Double getAnomalyScore() {
        return anomalyScore;
    }

    public Double getScoreThreshold() {
        return scoreThreshold;
    }

    public Boolean getAnomaly() {
        return anomaly;
    }

    public Double getTemperatureCMean() {
        return temperatureCMean;
    }

    public Double getVibrationMmSMean() {
        return vibrationMmSMean;
    }

    public Double getPressureBarMean() {
        return pressureBarMean;
    }

    public Double getPowerKwMean() {
        return powerKwMean;
    }

    public Double getRotationRpmMean() {
        return rotationRpmMean;
    }

    public Double getNullRatio() {
        return nullRatio;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
