package com.anomaly.alertservice.consumer;

import com.anomaly.alertservice.dto.AlertMessage;
import com.anomaly.alertservice.exception.NonRetryableMessageException;
import com.anomaly.alertservice.service.AlertIngestionService;
import com.anomaly.alertservice.service.IngestionMetrics;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Reads the {@code alerts} topic.
 *
 * <p>The topic has three partitions keyed by {@code machine_id}, so all alerts
 * for one machine land in one partition and are read in publication order by a
 * single thread. There is <strong>no global ordering</strong> across machines
 * and none is assumed: three partitions are three independent ordered streams.
 * Listener concurrency is capped at three for the same reason -- a fourth thread
 * would simply idle.
 *
 * <p>The payload arrives as bytes rather than through a JSON deserializer. A
 * deserializer that throws does so inside {@code poll()}, out of reach of the
 * error handler, and the original bytes are gone -- which would make the dead
 * letter unreplayable, defeating the point of having one.
 */
@Component
public class AlertConsumer {

    private static final Logger log = LoggerFactory.getLogger(AlertConsumer.class);

    private final ObjectMapper objectMapper;
    private final Validator validator;
    private final AlertIngestionService ingestion;
    private final IngestionMetrics metrics;

    public AlertConsumer(
            ObjectMapper objectMapper,
            Validator validator,
            AlertIngestionService ingestion,
            IngestionMetrics metrics) {
        this.objectMapper = objectMapper;
        this.validator = validator;
        this.ingestion = ingestion;
        this.metrics = metrics;
    }

    /**
     * Handle one alert.
     *
     * <p>The offset is acknowledged only after {@link AlertIngestionService}
     * has committed, so a crash in between replays the message rather than
     * losing it. That replay is precisely what the idempotent upsert absorbs.
     *
     * <p>Exceptions leaving this method are classified by the error handler:
     * {@link NonRetryableMessageException} goes straight to the dead letter
     * topic, anything else is retried with backoff and the offset stays put.
     */
    @KafkaListener(
            topics = "${alert-service.kafka.topic-alerts}",
            groupId = "${spring.kafka.consumer.group-id}",
            concurrency = "${spring.kafka.listener.concurrency}")
    public void onMessage(ConsumerRecord<String, byte[]> record, Acknowledgment acknowledgment) {
        metrics.recordConsumed();
        MDC.put("kafka_partition", Integer.toString(record.partition()));
        MDC.put("kafka_offset", Long.toString(record.offset()));
        MDC.put("machine_id", String.valueOf(record.key()));
        try {
            AlertMessage message = parse(record);
            MDC.put("alert_id", message.alertId());

            boolean created = ingestion.ingest(message);

            // A duplicate is a logical success. Retrying it would loop forever
            // on a message that is perfectly valid and already stored.
            log.info(
                    "alert_ingested alert_id={} machine_id={} created={} severity={}",
                    message.alertId(),
                    message.machineId(),
                    created,
                    message.severity());
            acknowledgment.acknowledge();
        } finally {
            MDC.clear();
        }
    }

    /**
     * Decode and validate, turning every data problem into a non-retryable
     * failure.
     *
     * <p>Malformed JSON, a missing required field and an unsupported major
     * version are all the same kind of problem: the message will never become
     * valid, so retrying it is pure noise. They are distinguished only by the
     * reason recorded on the dead letter, which is what makes the queue
     * diagnosable.
     */
    private AlertMessage parse(ConsumerRecord<String, byte[]> record) {
        if (record.value() == null) {
            throw new NonRetryableMessageException(
                    "MALFORMED_JSON", "tombstone on a topic that has no compaction");
        }

        AlertMessage message;
        try {
            message = objectMapper.readValue(record.value(), AlertMessage.class);
        } catch (JsonProcessingException e) {
            throw new NonRetryableMessageException(
                    "MALFORMED_JSON", "payload is not valid JSON: " + e.getOriginalMessage(), e);
        } catch (Exception e) {
            throw new NonRetryableMessageException(
                    "MALFORMED_JSON", "payload could not be read: " + e.getMessage(), e);
        }

        Set<ConstraintViolation<AlertMessage>> violations = validator.validate(message);
        if (!violations.isEmpty()) {
            String detail =
                    violations.stream()
                            .map(v -> v.getPropertyPath() + " " + v.getMessage())
                            .sorted()
                            .collect(Collectors.joining("; "));
            // The schema_version pattern is one of these constraints, so an
            // unsupported major version is reported as its own reason rather
            // than lumped in with a missing field.
            String reason =
                    violations.stream().anyMatch(v -> "schemaVersion".equals(v.getPropertyPath().toString()))
                            ? "UNSUPPORTED_SCHEMA_VERSION"
                            : "SCHEMA_VALIDATION_FAILED";
            throw new NonRetryableMessageException(reason, detail);
        }

        if (!message.hasOrderedWindow()) {
            throw new NonRetryableMessageException(
                    "SCHEMA_VALIDATION_FAILED", "window_end must be after window_start");
        }
        if (!message.hasConsistentDetectionInstant()) {
            // The contract requires detected_at == window_end. A producer bug
            // here would show an operator the wrong instant, so it is refused at
            // the boundary rather than stored.
            throw new NonRetryableMessageException(
                    "SCHEMA_VALIDATION_FAILED",
                    "detected_at ("
                            + message.detectedAt()
                            + ") must equal window_end ("
                            + message.windowEnd()
                            + ")");
        }
        return message;
    }

    static String describe(ConsumerRecord<String, byte[]> record) {
        return record.value() == null ? "" : new String(record.value(), StandardCharsets.UTF_8);
    }
}
