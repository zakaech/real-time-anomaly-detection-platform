package com.anomaly.alertservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Everything tunable, in one typed place.
 *
 * <p>Broker addresses and datasource credentials are deliberately NOT here: they
 * come from the standard Spring properties with no default at all, so a missing
 * variable fails startup instead of silently connecting somewhere unintended.
 */
@ConfigurationProperties(prefix = "alert-service")
public record AlertServiceProperties(
        KafkaProperties kafka,
        SseProperties sse,
        ApiProperties api,
        TelemetryProperties telemetry) {

    public record KafkaProperties(
            String topicAlerts,
            String topicDlq,
            // telemetry.scored, consumed by its own group so a flood of chart
            // data can never delay the operator queue (D-41).
            String topicScored,
            String telemetryGroup,
            int telemetryConcurrency,
            int telemetryMaxPollRecords,
            long retryInitialIntervalMs,
            double retryMultiplier,
            long retryMaxIntervalMs) {}

    public record SseProperties(
            long heartbeatIntervalMs, int clientQueueCapacity, int maxReplayEvents) {}

    public record ApiProperties(int defaultPageSize, int maxPageSize) {}

    /**
     * Telemetry curve limits (D-41). maxPoints is a hard ceiling for the same
     * reason page size is: a 10-second window over a month is a quarter of a
     * million rows, and one request must not be able to ask for the table.
     */
    public record TelemetryProperties(
            int defaultPoints,
            int maxPoints,
            int retentionHours,
            long retentionInitialDelayMs,
            long retentionIntervalMs) {}
}
