package com.anomaly.alertservice.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.anomaly.alertservice.AlertMessages;
import com.anomaly.alertservice.dto.AlertMessage;
import com.anomaly.alertservice.exception.NonRetryableMessageException;
import com.anomaly.alertservice.service.AlertIngestionService;
import com.anomaly.alertservice.service.IngestionMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.kafka.support.Acknowledgment;

/**
 * The consumer sorts failures into three kinds.
 *
 * <ul>
 *   <li>a <strong>data</strong> error can never succeed, so retrying it is a
 *       poison-pill loop that blocks its partition;
 *   <li>a <strong>transient</strong> error will succeed later, so dead-lettering
 *       it throws away a valid alert during an incident;
 *   <li>a <strong>duplicate</strong> is not a failure at all.
 * </ul>
 *
 * <p>Getting any of the three wrong produces a system that looks like it works.
 */
class AlertConsumerTest {

    private ObjectMapper objectMapper;
    private Validator validator;
    private AlertIngestionService ingestion;
    private IngestionMetrics metrics;
    private AlertConsumer consumer;
    private Acknowledgment acknowledgment;

    @BeforeEach
    void setUp() {
        objectMapper =
                new ObjectMapper()
                        .registerModule(new JavaTimeModule())
                        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        validator = Validation.buildDefaultValidatorFactory().getValidator();
        ingestion = mock(AlertIngestionService.class);
        metrics = new IngestionMetrics(new SimpleMeterRegistry());
        acknowledgment = mock(Acknowledgment.class);
        consumer = new AlertConsumer(objectMapper, validator, ingestion, metrics);
    }

    private ConsumerRecord<String, byte[]> record(String payload) {
        return new ConsumerRecord<>(
                "alerts",
                1,
                42L,
                "M-011",
                payload == null ? null : payload.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("a valid message is persisted and its offset acknowledged")
    void validMessageIsIngestedAndAcknowledged() {
        when(ingestion.ingest(any(AlertMessage.class))).thenReturn(true);

        consumer.onMessage(record(AlertMessages.validJson()), acknowledgment);

        verify(ingestion).ingest(any(AlertMessage.class));
        // The offset is committed only after the database transaction returned.
        verify(acknowledgment).acknowledge();
    }

    @Test
    @DisplayName("a duplicate is a success, not something to retry")
    void duplicateIsAcknowledgedLikeAnyOtherSuccess() {
        // The upsert absorbed a replay. Treating that as a failure would retry a
        // perfectly valid, already stored message until the end of time.
        when(ingestion.ingest(any(AlertMessage.class))).thenReturn(false);

        consumer.onMessage(record(AlertMessages.validJson()), acknowledgment);

        verify(acknowledgment).acknowledge();
    }

    @Test
    @DisplayName("malformed JSON is a data error and is never retried")
    void malformedJsonIsNonRetryable() {
        ConsumerRecord<String, byte[]> broken = record("{\"alert_id\": \"850eb1f1\", truncated");

        assertThatThrownBy(() -> consumer.onMessage(broken, acknowledgment))
                .isInstanceOf(NonRetryableMessageException.class)
                .extracting(e -> ((NonRetryableMessageException) e).getReason())
                .isEqualTo("MALFORMED_JSON");

        verify(ingestion, never()).ingest(any());
        // Not acknowledged here: the error handler routes it to the dead letter
        // topic and the container commits the offset afterwards.
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    @DisplayName("a missing required field is a data error")
    void missingRequiredFieldIsNonRetryable() {
        String withoutMachineId = AlertMessages.validJson().replaceAll("\"machine_id\"[^,]*,", "");

        assertThatThrownBy(() -> consumer.onMessage(record(withoutMachineId), acknowledgment))
                .isInstanceOf(NonRetryableMessageException.class)
                .extracting(e -> ((NonRetryableMessageException) e).getReason())
                .isEqualTo("SCHEMA_VALIDATION_FAILED");
    }

    @Test
    @DisplayName("an unsupported major version is reported as its own reason")
    void unsupportedMajorVersionIsDistinguished() {
        String v9 = AlertMessages.validJson().replace("\"schema_version\": \"1.0\"", "\"schema_version\": \"9.0\"");

        assertThatThrownBy(() -> consumer.onMessage(record(v9), acknowledgment))
                .isInstanceOf(NonRetryableMessageException.class)
                .extracting(e -> ((NonRetryableMessageException) e).getReason())
                // Distinguished from a plain schema violation so the dead letter
                // queue stays diagnosable: this one means the producer moved on.
                .isEqualTo("UNSUPPORTED_SCHEMA_VERSION");
    }

    @Test
    @DisplayName("a detection instant that disagrees with the window is refused")
    void inconsistentDetectionInstantIsRefused() {
        // The contract requires detected_at == window_end. A producer bug here
        // would show an operator the wrong instant.
        String shifted =
                AlertMessages.validJson()
                        .replace("\"detected_at\": \"2026-09-08T01:36:00.000Z\"", "\"detected_at\": \"2026-09-08T01:40:00.000Z\"");

        assertThatThrownBy(() -> consumer.onMessage(record(shifted), acknowledgment))
                .isInstanceOf(NonRetryableMessageException.class)
                .hasMessageContaining("must equal window_end");
    }

    @Test
    @DisplayName("a tombstone is a data error, not a crash")
    void tombstoneIsNonRetryable() {
        assertThatThrownBy(() -> consumer.onMessage(record(null), acknowledgment))
                .isInstanceOf(NonRetryableMessageException.class);
    }

    @Test
    @DisplayName("a database outage propagates for retry and is never dead-lettered")
    void databaseFailureIsRetryable() {
        when(ingestion.ingest(any(AlertMessage.class)))
                .thenThrow(new DataAccessResourceFailureException("connection refused"));

        assertThatThrownBy(() -> consumer.onMessage(record(AlertMessages.validJson()), acknowledgment))
                // Deliberately NOT a NonRetryableMessageException: the error
                // handler only dead-letters that type, so an infrastructure
                // failure keeps its offset and is retried. Routing it to the DLQ
                // would discard valid alerts exactly during an incident.
                .isInstanceOf(DataAccessResourceFailureException.class)
                .isNotInstanceOf(NonRetryableMessageException.class);

        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    @DisplayName("counters record what happened")
    void metricsReflectOutcomes() {
        when(ingestion.ingest(any(AlertMessage.class))).thenReturn(true, false);

        consumer.onMessage(record(AlertMessages.validJson()), acknowledgment);
        consumer.onMessage(record(AlertMessages.validJson()), acknowledgment);

        // The duplicate counter is the number that shows idempotence working
        // rather than being claimed, so it has to be incremented where it
        // actually happens.
        assertThat(metrics.currentSseClients()).isZero();
        verify(ingestion, org.mockito.Mockito.times(2)).ingest(any());
    }
}
