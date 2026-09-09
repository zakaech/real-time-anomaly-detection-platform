package com.anomaly.alertservice.dto;

import java.time.Instant;
import java.util.List;

/**
 * The curve one machine draws over a time range.
 *
 * <p>Every value here was measured and published by the pipeline. Nothing is
 * interpolated, smoothed or back-filled: a gap in the series is a gap in the
 * data, and the dashboard is expected to show it as one.
 *
 * <p>{@code truncated} tells the caller the range held more windows than the
 * limit allowed, so a chart can say so rather than silently drawing a partial
 * picture as if it were complete.
 */
public record TelemetrySeries(
        String machineCode,
        String lineCode,
        Instant from,
        Instant to,
        int windowSeconds,
        boolean truncated,
        List<TelemetryPoint> points) {

    /**
     * One window.
     *
     * <p>The sensor means are nullable because a failed sensor produces a null
     * aggregate; the score fields are nullable because an unscored window has no
     * score. Both are left null rather than defaulted, so the chart can break
     * the line instead of inventing a reading.
     */
    public record TelemetryPoint(
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
            Double nullRatio) {}
}
