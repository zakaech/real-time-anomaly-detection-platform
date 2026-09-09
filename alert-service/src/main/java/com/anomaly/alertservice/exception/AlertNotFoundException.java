package com.anomaly.alertservice.exception;

import java.util.UUID;

public class AlertNotFoundException extends RuntimeException {

    private final UUID alertId;

    public AlertNotFoundException(UUID alertId) {
        super("Alert " + alertId + " does not exist");
        this.alertId = alertId;
    }

    public UUID getAlertId() {
        return alertId;
    }
}
