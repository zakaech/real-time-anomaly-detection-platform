package com.anomaly.alertservice.controller;

import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.anomaly.alertservice.AlertMessages;
import com.anomaly.alertservice.PostgresTestBase;
import com.anomaly.alertservice.dto.AcknowledgeRequest;
import com.anomaly.alertservice.service.AlertIngestionService;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

/**
 * The operator API, end to end against a real database.
 *
 * <p>Mocking the service here would test the controller against a fiction. The
 * things most worth checking -- that a filter is executed by PostgreSQL, that a
 * page is bounded, that a second acknowledgement conflicts -- only mean
 * something with a real query behind them.
 */
@AutoConfigureMockMvc
class AlertControllerTest extends PostgresTestBase {

    @Autowired MockMvc mvc;
    @Autowired AlertIngestionService ingestion;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper objectMapper;

    private static final String ALERT_ID = AlertMessages.REAL_ALERT_ID;

    @BeforeEach
    void seed() {
        jdbc.update("DELETE FROM alert_acknowledgement");
        jdbc.update("DELETE FROM alert");
        ingestion.ingest(AlertMessages.valid());
        ingestion.ingest(
                AlertMessages.valid(
                        "44444444-4444-5444-8444-444444444444",
                        "M-003",
                        java.time.Instant.parse("2026-09-08T01:40:00Z"),
                        java.time.Instant.parse("2026-09-08T01:41:00Z")));
    }

    @Test
    @DisplayName("the list is paginated and filtered by the database")
    void listIsPaginated() throws Exception {
        mvc.perform(get("/api/v1/alerts").param("page", "0").param("size", "1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content.length()").value(1))
                .andExpect(jsonPath("$.totalElements").value(2))
                .andExpect(jsonPath("$.totalPages").value(2))
                .andExpect(jsonPath("$.last").value(false));
    }

    @Test
    @DisplayName("filters narrow the result set")
    void filtersApply() throws Exception {
        mvc.perform(get("/api/v1/alerts").param("machineCode", "M-011"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalElements").value(1))
                .andExpect(jsonPath("$.content[0].machineCode").value("M-011"));

        mvc.perform(get("/api/v1/alerts").param("severity", "MEDIUM"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalElements").value(0));

        mvc.perform(get("/api/v1/alerts").param("status", "NEW"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalElements").value(2));
    }

    @Test
    @DisplayName("a page larger than the ceiling is capped, not honoured")
    void pageSizeIsCapped() throws Exception {
        // 100 is the documented maximum; 5000 must not become a way to ask for
        // the whole table and turn pagination into a formality.
        mvc.perform(get("/api/v1/alerts").param("size", "5000"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("an unsortable property is refused instead of reaching the database")
    void sortIsWhitelisted() throws Exception {
        mvc.perform(get("/api/v1/alerts").param("sort", "machine.secret,desc"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail", containsString("sort property")));
    }

    @Test
    @DisplayName("an inverted date range is refused")
    void invertedRangeIsRefused() throws Exception {
        mvc.perform(
                        get("/api/v1/alerts")
                                .param("from", "2026-09-09T00:00:00Z")
                                .param("to", "2026-09-08T00:00:00Z"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("the detail carries what the alert actually holds")
    void detailReturnsStoredFacts() throws Exception {
        mvc.perform(get("/api/v1/alerts/{id}", ALERT_ID))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.alertId").value(ALERT_ID))
                .andExpect(jsonPath("$.machineCode").value("M-011"))
                .andExpect(jsonPath("$.status").value("NEW"))
                .andExpect(jsonPath("$.topContributors.length()").value(2))
                // Restored by the Phase 3 fix: without it an alert cannot be
                // tied back to the artefact that produced it.
                .andExpect(
                        jsonPath("$.model.artifactSha256")
                                .value("0f608323f944e63d30b80cc91418c10b3cfb1d3fc46bac184d3591c03a8d0aa5"))
                .andExpect(jsonPath("$.model.trainedAt").exists());
    }

    @Test
    @DisplayName("an unknown alert is a 404 problem detail")
    void unknownAlertIsNotFound() throws Exception {
        mvc.perform(get("/api/v1/alerts/{id}", UUID.randomUUID()))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.title").value("Alert not found"))
                .andExpect(jsonPath("$.traceId").exists());
    }

    @Test
    @DisplayName("acknowledging moves the alert and records who did it")
    void acknowledgeTransitionsAndAudits() throws Exception {
        mvc.perform(
                        post("/api/v1/alerts/{id}/acknowledge", ALERT_ID)
                                .contentType(MediaType.APPLICATION_JSON)
                                .header("X-Operator", "op.martin")
                                .content(
                                        objectMapper.writeValueAsString(
                                                new AcknowledgeRequest("checked the spindle", null))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("ACKNOWLEDGED"))
                .andExpect(jsonPath("$.history.length()").value(1))
                .andExpect(jsonPath("$.history[0].actor").value("op.martin"))
                .andExpect(jsonPath("$.history[0].comment").value("checked the spindle"));
    }

    @Test
    @DisplayName("acknowledging twice conflicts and says what the state is")
    void secondAcknowledgeConflicts() throws Exception {
        mvc.perform(
                        post("/api/v1/alerts/{id}/acknowledge", ALERT_ID)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content("{}"))
                .andExpect(status().isOk());

        mvc.perform(
                        post("/api/v1/alerts/{id}/acknowledge", ALERT_ID)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content("{}"))
                .andExpect(status().isConflict())
                // A 409 returns the current state so the client can resynchronise
                // without a second request.
                .andExpect(jsonPath("$.currentStatus").value("ACKNOWLEDGED"))
                .andExpect(jsonPath("$.currentVersion").exists());
    }

    @Test
    @DisplayName("a stale expectedVersion is refused")
    void staleExpectedVersionConflicts() throws Exception {
        mvc.perform(
                        post("/api/v1/alerts/{id}/acknowledge", ALERT_ID)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(new AcknowledgeRequest(null, 99L))))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.detail", containsString("modified concurrently")));
    }

    @Test
    @DisplayName("an over-long comment is rejected before it reaches the database")
    void commentLengthIsValidated() throws Exception {
        String tooLong = "x".repeat(1001);

        mvc.perform(
                        post("/api/v1/alerts/{id}/acknowledge", ALERT_ID)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(new AcknowledgeRequest(tooLong, null))))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("Validation failed"));
    }

    @Test
    @DisplayName("statistics are aggregated by the database")
    void statisticsAggregate() throws Exception {
        mvc.perform(
                        get("/api/v1/alerts/stats")
                                .param("from", "2026-09-08T00:00:00Z")
                                .param("to", "2026-09-09T00:00:00Z"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(2))
                .andExpect(jsonPath("$.bySeverity.CRITICAL").value(2))
                .andExpect(jsonPath("$.byStatus.NEW").value(2))
                .andExpect(jsonPath("$.byMachine.length()").value(2))
                .andExpect(jsonPath("$.overTime").isArray())
                .andExpect(jsonPath("$.acknowledgementRate").value(0.0));
    }

    @Test
    @DisplayName("an unknown granularity is refused rather than interpolated into SQL")
    void granularityIsWhitelisted() throws Exception {
        // date_trunc takes a literal unit and cannot be a bind parameter, so the
        // whitelist is what keeps this endpoint from being an injection point.
        mvc.perform(get("/api/v1/alerts/stats").param("granularity", "century; DROP TABLE alert"))
                .andExpect(status().isBadRequest());
    }
}
