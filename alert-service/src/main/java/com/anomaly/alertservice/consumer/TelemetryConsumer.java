package com.anomaly.alertservice.consumer;

import com.anomaly.alertservice.dto.ScoredWindowMessage;
import com.anomaly.alertservice.exception.NonRetryableMessageException;
import com.anomaly.alertservice.service.TelemetryIngestionService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Reads {@code telemetry.scored} to keep the dashboard curve fed (D-41).
 *
 * <p>Separate listener, separate consumer group, and that separation is
 * deliberate. This topic carries roughly two orders of magnitude more traffic
 * than {@code alerts} -- 11 342 window updates against 25 alerts on the same
 * two hours of measured data -- and the operator queue must never be delayed
 * behind a chart. A failure on one path leaves the other running.
 *
 * <p>Batched on purpose. Alerts are handled one message per transaction because
 * each is an event an operator will act on; windows are points on a line, and
 * committing them individually would mean one transaction per 10 seconds per
 * machine for data whose only reader is a chart.
 *
 * <p>The error policy is the one from ADR-006, with one difference stated
 * plainly: a window that cannot be parsed is <strong>dropped with a log line,
 * not dead-lettered</strong>. A dead letter exists to be replayed by a human,
 * and nobody will replay one point of a curve; sending them to
 * {@code telemetry.dlq} would bury the alert rejections that do need attention
 * under a flood of chart data. Transient failures are still retried, so a
 * database outage loses nothing.
 */
@Component
public class TelemetryConsumer {

    private static final Logger log = LoggerFactory.getLogger(TelemetryConsumer.class);

    private final ObjectMapper objectMapper;
    private final Validator validator;
    private final TelemetryIngestionService ingestion;
    private final TelemetryMetrics metrics;

    public TelemetryConsumer(
            ObjectMapper objectMapper,
            Validator validator,
            TelemetryIngestionService ingestion,
            TelemetryMetrics metrics) {
        this.objectMapper = objectMapper;
        this.validator = validator;
        this.ingestion = ingestion;
        this.metrics = metrics;
    }

    @KafkaListener(
            topics = "${alert-service.kafka.topic-scored}",
            groupId = "${alert-service.kafka.telemetry-group}",
            concurrency = "${alert-service.kafka.telemetry-concurrency}",
            batch = "true",
            containerFactory = "telemetryListenerContainerFactory")
    public void onBatch(List<ConsumerRecord<String, byte[]>> records, Acknowledgment acknowledgment) {
        int stored = 0;
        int rejected = 0;

        for (ConsumerRecord<String, byte[]> record : records) {
            ScoredWindowMessage message;
            try {
                message = parse(record);
            } catch (NonRetryableMessageException e) {
                // Dropped, not dead-lettered: see the class comment. Logged with
                // its coordinates so it can still be found in the topic.
                rejected++;
                log.warn(
                        "telemetry_window_rejected partition={} offset={} reason={} detail={}",
                        record.partition(),
                        record.offset(),
                        e.getReason(),
                        e.getMessage());
                continue;
            }
            // A transient failure here propagates and the whole batch is
            // retried. Re-storing windows that already succeeded is harmless --
            // the upsert is idempotent.
            ingestion.ingest(message);
            stored++;
        }

        metrics.recordWindows(stored, rejected);
        acknowledgment.acknowledge();

        if (stored > 0) {
            log.debug("telemetry_batch_stored windows={} rejected={}", stored, rejected);
        }
    }

    private ScoredWindowMessage parse(ConsumerRecord<String, byte[]> record) {
        if (record.value() == null) {
            throw new NonRetryableMessageException("MALFORMED_JSON", "tombstone on telemetry.scored");
        }
        ScoredWindowMessage message;
        try {
            message = objectMapper.readValue(record.value(), ScoredWindowMessage.class);
        } catch (JsonProcessingException e) {
            throw new NonRetryableMessageException(
                    "MALFORMED_JSON", "payload is not valid JSON: " + e.getOriginalMessage(), e);
        } catch (Exception e) {
            throw new NonRetryableMessageException(
                    "MALFORMED_JSON", "payload could not be read: " + e.getMessage(), e);
        }

        Set<ConstraintViolation<ScoredWindowMessage>> violations = validator.validate(message);
        if (!violations.isEmpty()) {
            throw new NonRetryableMessageException(
                    "SCHEMA_VALIDATION_FAILED",
                    violations.stream()
                            .map(v -> v.getPropertyPath() + " " + v.getMessage())
                            .sorted()
                            .collect(Collectors.joining("; ")));
        }
        if (!message.hasOrderedWindow()) {
            throw new NonRetryableMessageException(
                    "SCHEMA_VALIDATION_FAILED", "window_end must be after window_start");
        }
        return message;
    }
}
