package com.anomaly.alertservice.consumer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * Two counters for the telemetry path, kept apart from the alert counters.
 *
 * <p>Mixing them would make the alert numbers unreadable: this topic carries
 * roughly two orders of magnitude more traffic, so a shared counter would be
 * dominated by chart data and would say nothing about alerts.
 */
@Component
public class TelemetryMetrics {

    private final Counter stored;
    private final Counter rejected;

    public TelemetryMetrics(MeterRegistry registry) {
        this.stored =
                Counter.builder("telemetry.windows.stored")
                        .description("Scored windows written to the database")
                        .register(registry);
        this.rejected =
                Counter.builder("telemetry.windows.rejected")
                        .description("Windows dropped because they could not be parsed or validated")
                        .register(registry);
    }

    public void recordWindows(int storedCount, int rejectedCount) {
        if (storedCount > 0) {
            stored.increment(storedCount);
        }
        if (rejectedCount > 0) {
            rejected.increment(rejectedCount);
        }
    }
}
