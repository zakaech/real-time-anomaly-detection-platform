package com.anomaly.alertservice;

import com.anomaly.alertservice.config.AlertServiceProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Consumes the {@code alerts} topic, persists each alert idempotently, and
 * serves the operator API.
 *
 * <p>The delivery semantics of the whole chain are <strong>at-least-once from
 * Kafka plus idempotence in PostgreSQL</strong>, which yields effectively-once
 * persistence. This is deliberately <em>not</em> exactly-once: the Kafka sink
 * upstream is not transactional, and a restart replays the uncommitted batch.
 * A crash test of the stream processor measured 8 duplicate alert deliveries on
 * one run and 0 on another (docs/10): duplicates are possible, not guaranteed,
 * and the database is what makes them harmless.
 */
@SpringBootApplication
@EnableConfigurationProperties(AlertServiceProperties.class)
// Only for the telemetry retention purge (D-41); nothing else is scheduled.
@EnableScheduling
public class AlertServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AlertServiceApplication.class, args);
    }
}
