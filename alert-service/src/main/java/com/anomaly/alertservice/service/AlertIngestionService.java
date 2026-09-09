package com.anomaly.alertservice.service;

import com.anomaly.alertservice.dto.AlertMessage;
import com.anomaly.alertservice.entity.MachineEntity;
import com.anomaly.alertservice.exception.NonRetryableMessageException;
import com.anomaly.alertservice.repository.AlertRepository;
import com.anomaly.alertservice.repository.MachineRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Writes an alert exactly once, however many times it is delivered.
 *
 * <p>The transaction boundary here is one message. Kafka offsets are committed
 * by the listener container <em>after</em> this returns, which means the order
 * is always: database commit, then offset commit. That ordering is chosen
 * deliberately, because the two are not one atomic unit and cannot be -- Kafka
 * does not take part in a distributed transaction, and pretending otherwise
 * would be false.
 *
 * <p>Of the two possible orderings, only one is acceptable:
 *
 * <ul>
 *   <li>commit the offset first, then write: a crash between the two loses the
 *       alert permanently;
 *   <li>write first, then commit the offset: a crash between the two replays the
 *       alert, which the upsert absorbs.
 * </ul>
 *
 * <p>Idempotence is therefore not a nicety here, it is the price of the ordering
 * that does not lose data. Phase 3's crash test produced 8 duplicate alert
 * deliveries on one run and 0 on another, so replays are real and intermittent.
 */
@Service
public class AlertIngestionService {

    private static final Logger log = LoggerFactory.getLogger(AlertIngestionService.class);

    private final AlertRepository alerts;
    private final MachineRepository machines;
    private final ObjectMapper objectMapper;
    private final ApplicationEventPublisher events;
    private final IngestionMetrics metrics;

    public AlertIngestionService(
            AlertRepository alerts,
            MachineRepository machines,
            ObjectMapper objectMapper,
            ApplicationEventPublisher events,
            IngestionMetrics metrics) {
        this.alerts = alerts;
        this.machines = machines;
        this.objectMapper = objectMapper;
        this.events = events;
        this.metrics = metrics;
    }

    /**
     * Persist one alert.
     *
     * @return {@code true} if a row was created, {@code false} if this delivery
     *     was a replay of an alert already stored. A replay is a
     *     <strong>logical success</strong>, never an error: retrying it would
     *     loop forever on a perfectly valid message.
     */
    @Transactional
    public boolean ingest(AlertMessage message) {
        UUID alertId = parseAlertId(message);
        MachineEntity machine = resolveMachine(message);

        boolean created =
                alerts.upsert(
                        alertId,
                        machine.getId(),
                        message.severity(),
                        message.anomalyScore(),
                        message.scoreThreshold(),
                        message.detectedAt(),
                        message.windowStart(),
                        message.windowEnd(),
                        message.publishedAt(),
                        message.consecutiveWindowsOrDefault(),
                        message.model().name(),
                        message.model().version(),
                        message.model().trainedAt(),
                        message.model().artifactSha256(),
                        writeJson(message.topContributors(), "[]"),
                        writeJson(message.features(), "{}"));

        // false covers both a replay and an alert an operator has already
        // handled, which the statement deliberately refuses to touch. Neither is
        // an insertion, and neither is an error.
        if (created) {
            metrics.recordInserted(message.publishedAt());
            // Only a genuine creation reaches the stream, and only once this
            // transaction commits: the listener is bound to AFTER_COMMIT, so a
            // rolled-back write can never surface on a dashboard. Broadcasting
            // on every delivery would make it blink once per duplicate.
            events.publishEvent(new AlertBroadcaster.AlertCreatedEvent(alertId));
        } else {
            metrics.recordDuplicate();
            log.info(
                    "alert_duplicate_absorbed alert_id={} machine_id={} status=already_present",
                    alertId,
                    message.machineId());
        }
        return created;
    }

    private UUID parseAlertId(AlertMessage message) {
        try {
            return UUID.fromString(message.alertId());
        } catch (IllegalArgumentException e) {
            // The pattern constraint should have caught this already; if it did
            // not, the message is still data-wrong and must not be retried.
            throw new NonRetryableMessageException(
                    "SCHEMA_VALIDATION_FAILED", "alert_id is not a UUID: " + message.alertId(), e);
        }
    }

    /**
     * Find the machine, registering it if this is the first time it is seen.
     *
     * <p>Rejecting an alert because its machine is missing from the reference
     * table would discard a real detection over a stale seed (decision D-39).
     * The alert carries everything the row needs.
     */
    private MachineEntity resolveMachine(AlertMessage message) {
        return machines.findByCode(message.machineId())
                .orElseGet(
                        () -> {
                            machines.insertIfAbsent(message.machineId(), message.lineId());
                            log.info(
                                    "machine_auto_provisioned machine_id={} line_id={}",
                                    message.machineId(),
                                    message.lineId());
                            return machines.findByCode(message.machineId())
                                    .orElseThrow(
                                            () ->
                                                    new IllegalStateException(
                                                            "machine "
                                                                    + message.machineId()
                                                                    + " absent immediately after"
                                                                    + " insert"));
                        });
    }

    private String writeJson(Object value, String fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Map<?, ?> map && map.isEmpty()) {
            return fallback;
        }
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            // Serialising a value we just deserialised cannot normally fail, and
            // if it does the alert is still worth storing without it.
            log.warn("alert_json_fallback reason={}", e.getOriginalMessage());
            return fallback;
        }
    }
}
