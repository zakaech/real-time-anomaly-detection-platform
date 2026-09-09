package com.anomaly.alertservice.dto;

import com.anomaly.alertservice.entity.AlertSeverity;
import com.anomaly.alertservice.entity.AlertStatus;
import java.time.Instant;
import java.util.UUID;

/** One row of the alert list. Deliberately narrow: a list view is scrolled, not read. */
public record AlertSummary(
        UUID alertId,
        String machineCode,
        String lineCode,
        AlertSeverity severity,
        AlertStatus status,
        double anomalyScore,
        double scoreThreshold,
        Instant detectedAt,
        int consecutiveWindows) {}
