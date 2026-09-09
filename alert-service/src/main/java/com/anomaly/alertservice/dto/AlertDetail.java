package com.anomaly.alertservice.dto;

import com.anomaly.alertservice.entity.AlertSeverity;
import com.anomaly.alertservice.entity.AlertStatus;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Everything the platform holds about one alert.
 *
 * <p>What it does <strong>not</strong> hold is worth stating. The raw sensor
 * readings of the window live only in telemetry.raw, which has seven days of
 * retention and is persisted nowhere; and the 52-feature vector is never parsed
 * by the alerting query, so it never reaches an alert. This response therefore
 * carries the window bounds, the score, and the ranked contributors -- which is
 * what an operator triages on -- and no sensor time series. Promising a chart
 * the data cannot support would be worse than not offering one.
 */
public record AlertDetail(
        UUID alertId,
        String machineCode,
        String lineCode,
        String machineType,
        AlertSeverity severity,
        AlertStatus status,
        double anomalyScore,
        double scoreThreshold,
        Instant detectedAt,
        Instant windowStart,
        Instant windowEnd,
        Instant publishedAt,
        Instant ingestedAt,
        int consecutiveWindows,
        ModelInfo model,
        List<ContributorMessage> topContributors,
        List<AcknowledgementEntry> history,
        long version) {

    public record ModelInfo(String name, String version, Instant trainedAt, String artifactSha256) {}

    public record AcknowledgementEntry(
            AlertStatus previousStatus,
            AlertStatus newStatus,
            String actor,
            String comment,
            Instant occurredAt) {}
}
