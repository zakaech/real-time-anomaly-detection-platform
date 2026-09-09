package com.anomaly.alertservice.config;

import com.anomaly.alertservice.consumer.DlqEnvelope;
import com.anomaly.alertservice.exception.NonRetryableMessageException;
import com.anomaly.alertservice.service.IngestionMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * The error policy, which is really three policies for three different problems.
 *
 * <ul>
 *   <li><strong>Transient</strong> -- PostgreSQL unreachable, a timeout, a
 *       deadlock. Retried with exponential backoff and <em>never exhausted</em>.
 *       The offset is not committed, so nothing is lost. Sending these to a dead
 *       letter queue would throw away perfectly valid alerts during an incident,
 *       which is when losing them costs most (ADR-002).
 *   <li><strong>Data</strong> -- malformed JSON, a schema violation, an
 *       unsupported major version. Dead-lettered immediately with zero retries,
 *       because replaying an invalid message ten times will not make it valid.
 *   <li><strong>Duplicate</strong> -- not an error at all. It never reaches this
 *       class: the upsert absorbs it and the listener acknowledges normally.
 * </ul>
 *
 * <p>The asymmetry is the point. Retrying forever on a database outage is
 * correct; retrying forever on a corrupt payload is a poison-pill loop that
 * blocks the partition behind it.
 */
@Configuration
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    @Bean
    CommonErrorHandler alertErrorHandler(
            KafkaTemplate<String, byte[]> kafkaTemplate,
            ObjectMapper objectMapper,
            AlertServiceProperties properties,
            IngestionMetrics metrics,
            org.springframework.core.env.Environment environment) {

        String dlqTopic = properties.kafka().topicDlq();
        String processorVersion =
                "alert-service/" + environment.getProperty("application.version", "0.1.0");

        DefaultErrorHandler handler =
                new DefaultErrorHandler(
                        (record, exception) ->
                                publishDeadLetter(
                                        kafkaTemplate,
                                        objectMapper,
                                        metrics,
                                        dlqTopic,
                                        processorVersion,
                                        record,
                                        exception),
                        backOff(properties));

        // Data errors skip the backoff entirely and land on the dead letter
        // topic on the first attempt.
        handler.addNotRetryableExceptions(NonRetryableMessageException.class);
        handler.setRetryListeners(
                (record, ex, deliveryAttempt) -> {
                    metrics.recordRetry();
                    log.warn(
                            "alert_retry topic={} partition={} offset={} attempt={} error={}",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            deliveryAttempt,
                            ex.getMessage());
                });
        return handler;
    }

    /**
     * Exponential backoff with <strong>no maximum number of attempts</strong>.
     *
     * <p>The cap is on the interval, not on the count: a database that is down
     * for an hour must not cause an hour of alerts to be discarded. The
     * partition stalls, consumer lag grows, and both are visible -- which is the
     * correct, loud failure.
     */
    private ExponentialBackOff backOff(AlertServiceProperties properties) {
        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setInitialInterval(properties.kafka().retryInitialIntervalMs());
        backOff.setMultiplier(properties.kafka().retryMultiplier());
        backOff.setMaxInterval(properties.kafka().retryMaxIntervalMs());
        backOff.setMaxElapsedTime(Long.MAX_VALUE);
        return backOff;
    }

    private void publishDeadLetter(
            KafkaTemplate<String, byte[]> kafkaTemplate,
            ObjectMapper objectMapper,
            IngestionMetrics metrics,
            String dlqTopic,
            String processorVersion,
            ConsumerRecord<?, ?> record,
            Exception exception) {

        Throwable cause = exception.getCause() == null ? exception : exception.getCause();
        String reason =
                cause instanceof NonRetryableMessageException nonRetryable
                        ? nonRetryable.getReason()
                        : "SCHEMA_VALIDATION_FAILED";

        byte[] payload = record.value() instanceof byte[] bytes ? bytes : null;
        byte[] key =
                record.key() instanceof String text ? text.getBytes(StandardCharsets.UTF_8) : null;

        DlqEnvelope envelope =
                DlqEnvelope.wrap(
                        payload,
                        key,
                        reason,
                        cause.getMessage(),
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        processorVersion);

        try {
            kafkaTemplate.send(dlqTopic, objectMapper.writeValueAsBytes(envelope));
            metrics.recordDeadLettered();
            log.error(
                    "alert_dead_lettered topic={} partition={} offset={} reason={} detail={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    reason,
                    cause.getMessage());
        } catch (Exception publishFailure) {
            // If the dead letter cannot be published, the original offset must
            // not be committed either: swallowing both would lose the message
            // silently, which is the one outcome worth failing loudly over.
            log.error(
                    "alert_dead_letter_publish_failed topic={} partition={} offset={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    publishFailure);
            throw new IllegalStateException("could not publish to the dead letter topic", publishFailure);
        }
    }
}
