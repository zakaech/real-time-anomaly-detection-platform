package com.anomaly.alertservice.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;

/**
 * The shipped application.yml really defines what the code reads from it.
 *
 * <p>No Spring context and no database: this loads the file with the same loader
 * Spring Boot uses and asserts the property names exist. A typo in a key is
 * otherwise invisible until startup, where it surfaces as an unresolved
 * placeholder several layers away from its cause.
 */
class ConfigurationYamlTest {

    @Test
    void theShippedConfigurationDefinesEveryKeyTheCodeResolves() throws IOException {
        List<PropertySource<?>> sources =
                new YamlPropertySourceLoader()
                        .load("application", new ClassPathResource("application.yml"));

        assertThat(sources).hasSize(1);
        PropertySource<?> source = sources.get(0);

        // Referenced from @KafkaListener and from AlertServiceProperties.
        assertThat(source.getProperty("alert-service.kafka.topic-alerts")).isNotNull();
        assertThat(source.getProperty("alert-service.kafka.topic-dlq")).isNotNull();
        assertThat(source.getProperty("spring.kafka.consumer.group-id")).isNotNull();
        assertThat(source.getProperty("spring.kafka.listener.concurrency")).isNotNull();
        assertThat(source.getProperty("alert-service.api.max-page-size")).isNotNull();
        assertThat(source.getProperty("alert-service.sse.heartbeat-interval-ms")).isNotNull();

        // Flyway owns the schema and Hibernate must only ever validate it.
        assertThat(source.getProperty("spring.jpa.hibernate.ddl-auto")).isEqualTo("validate");
    }
}
