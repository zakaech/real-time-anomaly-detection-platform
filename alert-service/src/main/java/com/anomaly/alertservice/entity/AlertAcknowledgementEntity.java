package com.anomaly.alertservice.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/**
 * One status transition, recorded as a fact.
 *
 * <p><strong>Append-only.</strong> There is no update and no delete: this is the
 * audit trail of who did what to an alert and when. Overwriting a previous
 * acknowledgement would destroy the only record that it happened.
 *
 * <p>The table already accepts RESOLVED and DISMISSED, so extending the
 * lifecycle needs no migration.
 */
@Entity
@Table(name = "alert_acknowledgement")
public class AlertAcknowledgementEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "alert_id", nullable = false)
    private UUID alertId;

    @Enumerated(EnumType.STRING)
    @Column(name = "previous_status", nullable = false, length = 16)
    private AlertStatus previousStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "new_status", nullable = false, length = 16)
    private AlertStatus newStatus;

    /**
     * Who acted. There is no authentication in v1, so this comes from a request
     * header and defaults to "unknown". It is an audit field and is documented
     * as such rather than presented as an authenticated identity.
     */
    @Column(name = "actor", nullable = false, length = 128)
    private String actor;

    @Column(name = "comment")
    private String comment;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    protected AlertAcknowledgementEntity() {
        // JPA
    }

    public AlertAcknowledgementEntity(
            UUID alertId,
            AlertStatus previousStatus,
            AlertStatus newStatus,
            String actor,
            String comment,
            Instant occurredAt) {
        this.alertId = alertId;
        this.previousStatus = previousStatus;
        this.newStatus = newStatus;
        this.actor = actor;
        this.comment = comment;
        this.occurredAt = occurredAt;
    }

    public Long getId() {
        return id;
    }

    public UUID getAlertId() {
        return alertId;
    }

    public AlertStatus getPreviousStatus() {
        return previousStatus;
    }

    public AlertStatus getNewStatus() {
        return newStatus;
    }

    public String getActor() {
        return actor;
    }

    public String getComment() {
        return comment;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }
}
