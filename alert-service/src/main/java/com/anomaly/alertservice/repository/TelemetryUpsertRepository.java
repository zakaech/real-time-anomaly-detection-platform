package com.anomaly.alertservice.repository;

import java.time.Instant;

/**
 * The idempotent write for telemetry windows.
 *
 * <p>Separate from the JPA repository for the same reason the alert upsert is:
 * this is hand-written SQL whose behaviour on conflict is what matters, and
 * Spring Data has no way to express it.
 */
public interface TelemetryUpsertRepository {

    /**
     * Store a window, or refresh the one already stored.
     *
     * <p>telemetry.scored is published in <strong>update mode</strong>, so the
     * same window arrives several times as it fills (5.67 emissions per window
     * measured on a live run, docs/10). The last write wins, which is the one
     * built on the most samples.
     */
    void upsert(
            Long machineId,
            Instant windowStart,
            Instant windowEnd,
            int sampleCount,
            String machineState,
            boolean scored,
            Double anomalyScore,
            Double scoreThreshold,
            Boolean anomaly,
            Double temperatureCMean,
            Double vibrationMmSMean,
            Double pressureBarMean,
            Double powerKwMean,
            Double rotationRpmMean,
            Double nullRatio);
}
