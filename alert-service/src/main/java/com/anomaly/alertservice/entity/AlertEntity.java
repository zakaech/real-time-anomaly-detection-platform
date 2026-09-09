package com.anomaly.alertservice.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.time.Instant;
import java.util.UUID;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * One persisted detection.
 *
 * <p>The primary key <strong>is</strong> the alert_id from the contract: a
 * deterministic UUIDv5 of (machine_id, window_start, model_name, model_version).
 * It is already the natural key in scalar form and already the identifier the
 * REST API exposes, so a surrogate key would only add a second identity that no
 * client ever sees.
 *
 * <p>This entity is <strong>never used to insert</strong>. Ingestion goes
 * through the native upsert in AlertRepository, because save() issues a SELECT
 * then an INSERT or UPDATE, and another thread can insert the same row between
 * the two -- turning an operation that is meant to be idempotent into a
 * constraint violation. JPA is used for reads and for the acknowledgement
 * transition, where entity tracking and optimistic locking are worth having.
 */
@Entity
@Table(name = "alert")
public class AlertEntity {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "machine_id", nullable = false)
    private MachineEntity machine;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 16)
    private AlertStatus status;

    @Enumerated(EnumType.STRING)
    @Column(name = "severity", nullable = false, length = 16)
    private AlertSeverity severity;

    @Column(name = "anomaly_score", nullable = false)
    private double anomalyScore;

    @Column(name = "score_threshold", nullable = false)
    private double scoreThreshold;

    /** Event time: when the machine deviated, not when the computation ended. */
    @Column(name = "detected_at", nullable = false)
    private Instant detectedAt;

    @Column(name = "window_start", nullable = false)
    private Instant windowStart;

    @Column(name = "window_end", nullable = false)
    private Instant windowEnd;

    @Column(name = "published_at", nullable = false)
    private Instant publishedAt;

    @Column(name = "consecutive_windows", nullable = false)
    private int consecutiveWindows;

    @Column(name = "model_name", nullable = false, length = 64)
    private String modelName;

    @Column(name = "model_version", nullable = false, length = 32)
    private String modelVersion;

    @Column(name = "model_trained_at")
    private Instant modelTrainedAt;

    /** Ties this alert to the exact artefact that produced it. */
    @Column(name = "model_artifact_sha256", length = 64)
    private String modelArtifactSha256;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "top_contributors", nullable = false)
    private String topContributors;

    /**
     * Declared by the contract but currently always empty: the alerting query
     * never parses the 52-feature vector, so it is not lost in transit -- it
     * never enters that path. Kept so that carrying it later needs no migration.
     */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "features", nullable = false)
    private String features;

    /**
     * Monotonic publication order, assigned by the database. SSE reconnection
     * needs an ordered cursor and the primary key is a random UUID, which orders
     * nothing.
     */
    @Column(name = "event_seq", nullable = false, insertable = false, updatable = false)
    private Long eventSeq;

    @Column(name = "ingested_at", nullable = false, insertable = false, updatable = false)
    private Instant ingestedAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Version
    @Column(name = "optlock_version", nullable = false)
    private long optlockVersion;

    protected AlertEntity() {
        // JPA
    }

    public UUID getId() {
        return id;
    }

    public MachineEntity getMachine() {
        return machine;
    }

    public AlertStatus getStatus() {
        return status;
    }

    public AlertSeverity getSeverity() {
        return severity;
    }

    public double getAnomalyScore() {
        return anomalyScore;
    }

    public double getScoreThreshold() {
        return scoreThreshold;
    }

    public Instant getDetectedAt() {
        return detectedAt;
    }

    public Instant getWindowStart() {
        return windowStart;
    }

    public Instant getWindowEnd() {
        return windowEnd;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }

    public int getConsecutiveWindows() {
        return consecutiveWindows;
    }

    public String getModelName() {
        return modelName;
    }

    public String getModelVersion() {
        return modelVersion;
    }

    public Instant getModelTrainedAt() {
        return modelTrainedAt;
    }

    public String getModelArtifactSha256() {
        return modelArtifactSha256;
    }

    public String getTopContributors() {
        return topContributors;
    }

    public String getFeatures() {
        return features;
    }

    public Long getEventSeq() {
        return eventSeq;
    }

    public Instant getIngestedAt() {
        return ingestedAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public long getOptlockVersion() {
        return optlockVersion;
    }

    /**
     * Move this alert to a new status. The caller is responsible for having
     * checked that the transition is legal; this only records it.
     */
    public void transitionTo(AlertStatus next, Instant at) {
        this.status = next;
        this.updatedAt = at;
    }
}
