package com.anomaly.alertservice.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.anomaly.alertservice.AlertMessages;
import com.anomaly.alertservice.dto.AlertMessage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import java.nio.file.Path;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The Java side agrees with the contracts of record, not just with itself.
 *
 * <p>Two implementations now read and write these messages -- Python in the
 * simulator and the Spark job, Java here -- and the only thing keeping them
 * aligned is that both are checked against the same JSON Schema. Testing the
 * Java DTO against the Java DTO would prove nothing at all.
 *
 * <p>This matters most for the dead letter envelope, which this service
 * <em>produces</em>: a divergence there would be discovered by whoever tries to
 * replay a rejected message, long after the fact.
 */
class ContractConformanceTest {

    private static final Path CONTRACTS = Path.of("..", "contracts", "json-schema");

    private final ObjectMapper objectMapper =
            new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private JsonSchema schema(String file) {
        return JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012)
                .getSchema(CONTRACTS.resolve(file).toUri());
    }

    @Test
    @DisplayName("a real alert from the topic validates and deserialises")
    void realAlertMatchesTheContract() throws Exception {
        JsonNode payload = objectMapper.readTree(AlertMessages.validJson());

        Set<ValidationMessage> problems = schema("alert.v1.json").validate(payload);
        assertThat(problems).as("fixture must satisfy the contract of record").isEmpty();

        // And the DTO must be able to read exactly that.
        AlertMessage message = objectMapper.readValue(AlertMessages.validJson(), AlertMessage.class);
        assertThat(message.alertId()).isEqualTo(AlertMessages.REAL_ALERT_ID);
        assertThat(message.model().artifactSha256()).hasSize(64);
        assertThat(message.consecutiveWindowsOrDefault()).isEqualTo(2);
        assertThat(message.hasConsistentDetectionInstant()).isTrue();
    }

    @Test
    @DisplayName("the dead letter envelope this service emits validates against its schema")
    void deadLetterEnvelopeMatchesTheContract() throws Exception {
        DlqEnvelope envelope =
                DlqEnvelope.wrap(
                        "{\"broken\": ".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        "M-011".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        "MALFORMED_JSON",
                        "payload is not valid JSON",
                        "alerts",
                        1,
                        42L,
                        "alert-service/0.1.0");

        JsonNode payload = objectMapper.valueToTree(envelope);
        Set<ValidationMessage> problems = schema("telemetry-dlq.v1.json").validate(payload);

        assertThat(problems).as("the Java envelope must match the Python one").isEmpty();
        // source_topic is what lets one dead letter queue serve both producers.
        assertThat(payload.get("source_topic").asText()).isEqualTo("alerts");
    }

    @Test
    @DisplayName("the original bytes survive base64 so a dead letter stays replayable")
    void deadLetterKeepsTheOriginalBytes() {
        byte[] original = "{\"alert_id\": \"truncated".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        DlqEnvelope envelope =
                DlqEnvelope.wrap(
                        original, null, "MALFORMED_JSON", "detail", "alerts", 0, 1L, "alert-service/0.1.0");

        // Replay after a fix is only possible because nothing was normalised on
        // the way in.
        assertThat(java.util.Base64.getDecoder().decode(envelope.rawPayload())).isEqualTo(original);
        assertThat(envelope.rawKey()).isNull();
    }

    @Test
    @DisplayName("an unactionable rejection is still given a detail")
    void deadLetterAlwaysCarriesADetail() {
        // The contract refuses an empty detail: a rejection nobody can act on is
        // a log line, not a dead letter.
        DlqEnvelope envelope =
                DlqEnvelope.wrap(new byte[0], null, "MALFORMED_JSON", null, "alerts", 0, 0L, "alert-service/0.1.0");

        assertThat(envelope.dlqDetail()).isNotBlank();
    }
}
