package com.anomaly.alertservice.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.anomaly.alertservice.PostgresTestBase;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.servlet.mvc.method.RequestMappingInfo;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

/**
 * The OpenAPI document, checked against the application itself.
 *
 * <p>The central test is {@link #documentsExactlyTheEndpointsThatExist()}, and
 * it is the reason springdoc was chosen over a hand-written {@code openapi.yaml}
 * (decision D-50). It does not compare the document against a list someone
 * typed here -- a list is just a second document to forget. It compares it
 * against Spring's own routing table, so the assertion is:
 *
 * <blockquote>every documented path is routable, and every routable path is
 * documented.</blockquote>
 *
 * <p>Both halves matter. Documenting an endpoint that does not exist sends a
 * client after a 404; leaving a real endpoint out means the document is quietly
 * incomplete. Neither can survive this test, and neither can be reintroduced by
 * adding a controller and forgetting the docs.
 */
@AutoConfigureMockMvc
class OpenApiDocumentationTest extends PostgresTestBase {

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper objectMapper;

    /**
     * Qualified by name, because the actuator contributes a second bean of this
     * exact type ({@code controllerEndpointHandlerMapping}). Injecting by type
     * alone fails to start the context, and picking the wrong one would compare
     * the document against the actuator's routes instead of the API's.
     */
    @Autowired
    @Qualifier("requestMappingHandlerMapping")
    RequestMappingHandlerMapping handlerMapping;

    private JsonNode apiDocs() throws Exception {
        String body =
                mvc.perform(get("/v3/api-docs"))
                        .andExpect(status().isOk())
                        .andReturn()
                        .getResponse()
                        .getContentAsString();
        return objectMapper.readTree(body);
    }

    @Test
    @DisplayName("the document is served and describes this service")
    void documentIsServed() throws Exception {
        mvc.perform(get("/v3/api-docs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.openapi").exists())
                .andExpect(jsonPath("$.info.title").value("Alert Service API"))
                .andExpect(jsonPath("$.info.version").value("0.1.0"));
    }

    @Test
    @DisplayName("it documents exactly the endpoints that exist, no more and no fewer")
    void documentsExactlyTheEndpointsThatExist() throws Exception {
        Set<String> documented = new TreeSet<>();
        apiDocs()
                .path("paths")
                .fieldNames()
                .forEachRemaining(
                        path -> {
                            if (path.startsWith("/api/")) {
                                documented.add(path);
                            }
                        });

        Set<String> routable = new TreeSet<>();
        for (RequestMappingInfo info : handlerMapping.getHandlerMethods().keySet()) {
            if (info.getPathPatternsCondition() == null) {
                continue;
            }
            info.getPathPatternsCondition().getPatternValues().stream()
                    .filter(pattern -> pattern.startsWith("/api/"))
                    .forEach(routable::add);
        }

        assertThat(routable)
                .as("the application must actually expose some /api routes for this test to mean anything")
                .isNotEmpty();

        assertThat(documented)
                .as(
                        "OpenAPI must describe every routable /api endpoint and nothing else."
                            + " A path documented but not routable sends clients to a 404;"
                            + " a path routable but undocumented is a silent gap.")
                .isEqualTo(routable);
    }

    @Test
    @DisplayName("the six operator endpoints are present under their real verbs")
    void theSixEndpointsArePresent() throws Exception {
        JsonNode paths = apiDocs().path("paths");

        assertThat(paths.path("/api/v1/alerts").has("get")).isTrue();
        assertThat(paths.path("/api/v1/alerts/{alertId}").has("get")).isTrue();
        assertThat(paths.path("/api/v1/alerts/{alertId}/acknowledge").has("post")).isTrue();
        assertThat(paths.path("/api/v1/alerts/stats").has("get")).isTrue();
        assertThat(paths.path("/api/v1/alerts/stream").has("get")).isTrue();
        assertThat(paths.path("/api/v1/machines/{machineCode}/telemetry").has("get")).isTrue();
    }

    @Test
    @DisplayName("the stream is declared as text/event-stream and its protocol is described in prose")
    void sseIsDocumentedHonestly() throws Exception {
        JsonNode stream = apiDocs().path("paths").path("/api/v1/alerts/stream").path("get");

        assertThat(stream.path("responses").path("200").path("content").has("text/event-stream"))
                .as("the media type is the one thing OpenAPI can state about SSE; it must state it")
                .isTrue();

        // OpenAPI has no vocabulary for an event protocol, so the description is
        // where it lives. If someone strips it, clients lose the only account of
        // how the stream actually behaves -- hence an assertion rather than trust.
        String description = stream.path("description").asText("");
        assertThat(description)
                .as("the SSE event protocol must be described in prose")
                .contains("alert.created")
                .contains("alert.updated")
                .contains("heartbeat")
                .contains("Last-Event-ID")
                .contains("event_seq");
    }

    @Test
    @DisplayName("the acknowledgement documents 409, which is its whole concurrency contract")
    void acknowledgeDocumentsConflict() throws Exception {
        JsonNode responses =
                apiDocs()
                        .path("paths")
                        .path("/api/v1/alerts/{alertId}/acknowledge")
                        .path("post")
                        .path("responses");

        assertThat(responses.has("200")).isTrue();
        assertThat(responses.has("404")).isTrue();
        assertThat(responses.has("409")).isTrue();
        assertThat(responses.path("409").path("content").has("application/problem+json"))
                .as("errors are RFC 9457 and the document must say so")
                .isTrue();
    }

    @Test
    @DisplayName("adding springdoc did not displace the ProblemDetail error format")
    void errorsAreStillProblemDetail() throws Exception {
        // The regression this guards is real: springdoc registers its own
        // handlers, and a mistake there would have errors leaving as Spring's
        // default body instead of through GlobalExceptionHandler. The document
        // would still claim problem+json while the service returned something
        // else -- documentation that validates and lies.
        mvc.perform(get("/api/v1/alerts").param("sort", "notAColumn,desc"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.type").exists())
                .andExpect(jsonPath("$.title").exists())
                .andExpect(jsonPath("$.traceId").exists());

        mvc.perform(get("/api/v1/alerts/{id}", "00000000-0000-5000-8000-000000000000"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.traceId").exists());
    }

    @Test
    @DisplayName("the contributor score keeps its snake_case name, as the alert contract has it")
    void contributorFieldNameMatchesTheContract() throws Exception {
        JsonNode contributor =
                apiDocs().path("components").path("schemas").path("ContributorMessage");

        assertThat(contributor.isMissingNode())
                .as("the contributor schema must be published")
                .isFalse();
        // Renaming it to camelCase at the documentation boundary would describe a
        // response the service does not send (decision D-45).
        assertThat(contributor.path("properties").has("z_score")).isTrue();
    }
}
