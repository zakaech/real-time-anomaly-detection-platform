package com.anomaly.alertservice.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;
import java.util.HashMap;
import java.util.Map;

/**
 * A separate listener container for {@code telemetry.scored} (D-41).
 *
 * <p>It is separate rather than shared because the two topics have nothing in
 * common operationally. Alerts are consumed one message per transaction, since
 * each is an event an operator acts on. Windows are points on a chart and
 * arrive roughly two orders of magnitude more often -- 11 342 against 25 on the
 * same measured two hours -- so they are consumed in batches. Sharing a factory
 * would force one policy on both and let a flood of chart data delay the
 * operator queue.
 *
 * <p>The error handler retries transient failures with the same non-exhausting
 * backoff as the alert path: a database outage must not cost data here either.
 * Unparseable windows never reach it -- the listener drops them itself, because
 * a dead letter is meant to be replayed by a human and nobody replays one point
 * of a curve.
 */
@Configuration
public class TelemetryKafkaConfig {

    @Bean
    ConsumerFactory<String, byte[]> telemetryConsumerFactory(
            KafkaProperties properties,
            @Value("${alert-service.kafka.telemetry-group}") String groupId,
            @Value("${alert-service.kafka.telemetry-max-poll-records}") int maxPollRecords) {

        Map<String, Object> config = new HashMap<>(properties.buildConsumerProperties(null));
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // A larger batch than the alert path: these are written in bulk and the
        // per-transaction overhead is what would otherwise dominate.
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, byte[]> telemetryListenerContainerFactory(
            ConsumerFactory<String, byte[]> telemetryConsumerFactory,
            @Value("${alert-service.kafka.retry-initial-interval-ms}") long initialInterval,
            @Value("${alert-service.kafka.retry-max-interval-ms}") long maxInterval,
            @Value("${alert-service.kafka.retry-multiplier}") double multiplier) {

        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(telemetryConsumerFactory);
        factory.setBatchListener(true);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setInitialInterval(initialInterval);
        backOff.setMultiplier(multiplier);
        backOff.setMaxInterval(maxInterval);
        // No maximum number of attempts, for the same reason as the alert path:
        // a database down for an hour must not become an hour of lost data.
        backOff.setMaxElapsedTime(Long.MAX_VALUE);
        factory.setCommonErrorHandler(new DefaultErrorHandler(backOff));
        return factory;
    }
}
