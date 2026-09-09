package com.anomaly.alertservice.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JacksonConfig {

    /**
     * Instants are written as ISO-8601 strings, not epoch numbers.
     *
     * <p>The platform contracts define every timestamp as an ISO date-time; a
     * numeric timestamp would still be valid JSON and would silently break every
     * consumer that reads the contract literally.
     */
    @Bean
    Jackson2ObjectMapperBuilderCustomizer platformJsonConventions() {
        return builder ->
                builder.featuresToDisable(
                                SerializationFeature.WRITE_DATES_AS_TIMESTAMPS,
                                DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                        .featuresToEnable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
    }
}
