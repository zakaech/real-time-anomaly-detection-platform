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
public record AlertServiceProperties(KafkaProperties kafka, SseProperties sse, ApiProperties api) {

    public record KafkaProperties(
            String topicAlerts,
            String topicDlq,
            long retryInitialIntervalMs,
            double retryMultiplier,
            long retryMaxIntervalMs) {}

    public record SseProperties(
            long heartbeatIntervalMs, int clientQueueCapacity, int maxReplayEvents) {}

    public record ApiProperties(int defaultPageSize, int maxPageSize) {}
}
