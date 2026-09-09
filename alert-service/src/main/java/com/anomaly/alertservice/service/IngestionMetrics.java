package com.anomaly.alertservice.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

/**
 * The counters that say whether ingestion is healthy.
 *
 * <p>Every one of these is measured, none is derived from another. Deliberately
 * absent: a "success rate" (computable from inserted and dlq), and an
 * "alerts per second" (computable from consumed) -- a metric that restates
 * another one only adds a second thing to keep consistent.
 *
 * <p>Consumer lag is not reimplemented either: Kafka already exposes it, and a
 * second measurement of the same quantity would eventually disagree with the
 * first.
 */
@Component
public class IngestionMetrics {

    private final Counter consumed;
    private final Counter inserted;
    private final Counter duplicates;
    private final Counter deadLettered;
    private final Counter retries;
    private final Counter persistenceErrors;
    private final Timer ingestionLatency;
    private final AtomicInteger sseClients = new AtomicInteger();

    public IngestionMetrics(MeterRegistry registry) {
        this.consumed =
                Counter.builder("alerts.consumed")
                        .description("Alert messages read from the topic")
                        .register(registry);
        this.inserted =
                Counter.builder("alerts.inserted")
                        .description("Alerts that resulted in a new row")
                        .register(registry);
        this.duplicates =
                Counter.builder("alerts.duplicate")
                        .description(
                                "Replays absorbed by the database. This is the number that shows"
                                        + " idempotence working rather than being claimed.")
                        .register(registry);
        this.deadLettered =
                Counter.builder("alerts.dlq")
                        .description("Messages rejected as data errors")
                        .register(registry);
        this.retries =
                Counter.builder("alerts.retry")
                        .description("Retry attempts after a transient failure")
                        .register(registry);
        this.persistenceErrors =
                Counter.builder("alerts.persistence.errors")
                        .description("Database failures while writing an alert")
                        .register(registry);
        this.ingestionLatency =
                Timer.builder("alerts.ingestion.latency")
                        .description(
                                "From the producer publishing the alert to it being committed here")
                        .register(registry);
        registry.gauge("sse.clients", sseClients);
    }

    public void recordConsumed() {
        consumed.increment();
    }

    public void recordInserted(Instant publishedAt) {
        inserted.increment();
        if (publishedAt != null) {
            Duration elapsed = Duration.between(publishedAt, Instant.now());
            // A negative duration would mean the producer clock is ahead of
            // ours; recording it would corrupt the distribution rather than
            // reveal the clock skew, which belongs in logs.
            if (!elapsed.isNegative()) {
                ingestionLatency.record(elapsed);
            }
        }
    }

    public void recordDuplicate() {
        duplicates.increment();
    }

    public void recordDeadLettered() {
        deadLettered.increment();
    }

    public void recordRetry() {
        retries.increment();
    }

    public void recordPersistenceError() {
        persistenceErrors.increment();
    }

    public void sseClientOpened() {
        sseClients.incrementAndGet();
    }

    public void sseClientClosed() {
        sseClients.decrementAndGet();
    }

    public int currentSseClients() {
        return sseClients.get();
    }
}
