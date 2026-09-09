package com.anomaly.alertservice.exception;

import com.anomaly.alertservice.entity.AlertStatus;
import java.util.UUID;

/** Raised when an alert is asked to make a transition its current status forbids. */
public class InvalidStateTransitionException extends RuntimeException {

    private final UUID alertId;
    private final AlertStatus currentStatus;
    private final long currentVersion;

    public InvalidStateTransitionException(
            UUID alertId, AlertStatus currentStatus, long currentVersion, String detail) {
        super(detail);
        this.alertId = alertId;
        this.currentStatus = currentStatus;
        this.currentVersion = currentVersion;
    }

    public UUID getAlertId() {
        return alertId;
    }

    public AlertStatus getCurrentStatus() {
        return currentStatus;
    }

    public long getCurrentVersion() {
        return currentVersion;
    }
}
