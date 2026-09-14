package com.anomaly.alertservice.service;

import com.anomaly.alertservice.config.AlertServiceProperties;
import com.anomaly.alertservice.repository.TelemetryWindowRepository;
import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Keeps the telemetry table bounded.
 *
 * <p>Measured volume: the scoring query emitted 11 342 window updates for two
 * hours of simulated telemetry across 15 machines, so this table grows at
 * roughly 5 700 rows an hour once the update-mode duplicates collapse. Its only
 * reader is a chart showing recent history, so old rows are dead weight.
 *
 * <p>Not a partitioned table with DROP PARTITION: that is the right answer at
 * millions of rows and premature here.
 */
@Component
public class TelemetryRetentionJob {

    private static final Logger log = LoggerFactory.getLogger(TelemetryRetentionJob.class);

    private final TelemetryWindowRepository windows;
    private final AlertServiceProperties properties;

    public TelemetryRetentionJob(
            TelemetryWindowRepository windows, AlertServiceProperties properties) {
        this.windows = windows;
        this.properties = properties;
    }

    @Scheduled(
            initialDelayString = "${alert-service.telemetry.retention-initial-delay-ms}",
            fixedDelayString = "${alert-service.telemetry.retention-interval-ms}")
    @Transactional
    public void purge() {
        Duration horizon = Duration.ofHours(properties.telemetry().retentionHours());
        Instant before = Instant.now().minus(horizon);
        int deleted = windows.deleteOlderThan(before);
        if (deleted > 0) {
            log.info(
                    "telemetry_retention_purged rows={} older_than={} horizon_hours={}",
                    deleted,
                    before,
                    properties.telemetry().retentionHours());
        }
    }
}
