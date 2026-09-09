package com.anomaly.alertservice.controller;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.anomaly.alertservice.PostgresTestBase;
import com.anomaly.alertservice.dto.ScoredWindowMessage;
import com.anomaly.alertservice.service.TelemetryIngestionService;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

/** The endpoint the dashboard chart reads (D-41). */
@AutoConfigureMockMvc
class TelemetryControllerTest extends PostgresTestBase {

    @Autowired MockMvc mvc;
    @Autowired TelemetryIngestionService ingestion;
    @Autowired JdbcTemplate jdbc;

    private static final Instant W1 = Instant.parse("2026-09-09T10:00:00Z");

    @BeforeEach
    void seed() {
        jdbc.update("DELETE FROM telemetry_window");
        for (int i = 0; i < 5; i++) {
            ingestion.ingest(
                    new ScoredWindowMessage(
                            "1.0",
                            "M-001",
                            "LINE-A",
                            W1.plusSeconds(i * 10L),
                            W1.plusSeconds(i * 10L + 60),
                            60,
                            "RUNNING",
                            true,
                            i == 3 ? 0.995 : 0.176,
                            0.9927653712984937,
                            i == 3,
                            Map.of(
                                    ScoredWindowMessage.TEMPERATURE, 45.0 + i,
                                    ScoredWindowMessage.VIBRATION, 1.13,
                                    ScoredWindowMessage.PRESSURE, 12.19,
                                    ScoredWindowMessage.POWER, 6.10,
                                    ScoredWindowMessage.ROTATION, 1033.56,
                                    ScoredWindowMessage.NULL_RATIO, 0.0)));
        }
    }

    @Test
    @DisplayName("the series carries real sensor values and marks the anomaly")
    void seriesCarriesSensorsAndAnomalies() throws Exception {
        mvc.perform(
                        get("/api/v1/machines/{code}/telemetry", "M-001")
                                .param("from", "2026-09-09T09:59:00Z")
                                .param("to", "2026-09-09T10:10:00Z"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.machineCode").value("M-001"))
                .andExpect(jsonPath("$.lineCode").value("LINE-A"))
                .andExpect(jsonPath("$.windowSeconds").value(60))
                .andExpect(jsonPath("$.truncated").value(false))
                .andExpect(jsonPath("$.points.length()").value(5))
                .andExpect(jsonPath("$.points[0].temperatureCMean").value(45.0))
                .andExpect(jsonPath("$.points[0].anomaly").value(false))
                // The point the chart marks: a real anomaly, not a decoration.
                .andExpect(jsonPath("$.points[3].anomaly").value(true))
                .andExpect(jsonPath("$.points[3].anomalyScore").value(0.995))
                .andExpect(jsonPath("$.points[3].scoreThreshold").value(0.9927653712984937));
    }

    @Test
    @DisplayName("an unknown machine is a 404 problem detail, not an empty chart")
    void unknownMachineIsNotFound() throws Exception {
        mvc.perform(get("/api/v1/machines/{code}/telemetry", "M-999"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.title").value("Machine not found"))
                .andExpect(jsonPath("$.machineCode").value("M-999"))
                .andExpect(jsonPath("$.traceId").exists());
    }

    @Test
    @DisplayName("a malformed machine code is refused before it reaches the database")
    void malformedMachineCodeIsRejected() throws Exception {
        mvc.perform(get("/api/v1/machines/{code}/telemetry", "not-a-machine"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("an inverted range is refused")
    void invertedRangeIsRejected() throws Exception {
        mvc.perform(
                        get("/api/v1/machines/{code}/telemetry", "M-001")
                                .param("from", "2026-09-10T00:00:00Z")
                                .param("to", "2026-09-09T00:00:00Z"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("maxPoints is capped, and the response admits the truncation")
    void maxPointsIsCapped() throws Exception {
        mvc.perform(
                        get("/api/v1/machines/{code}/telemetry", "M-001")
                                .param("from", "2026-09-09T09:59:00Z")
                                .param("to", "2026-09-09T10:10:00Z")
                                .param("maxPoints", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.points.length()").value(2))
                .andExpect(jsonPath("$.truncated").value(true));
    }

    @Test
    @DisplayName("an empty range returns an empty series rather than an error")
    void emptyRangeIsAnEmptySeries() throws Exception {
        // A machine with no data in the window is a normal state for a chart,
        // not a failure.
        mvc.perform(
                        get("/api/v1/machines/{code}/telemetry", "M-002")
                                .param("from", "2026-09-09T09:59:00Z")
                                .param("to", "2026-09-09T10:10:00Z"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.points.length()").value(0))
                .andExpect(jsonPath("$.windowSeconds").value(0));
    }
}
