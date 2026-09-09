package com.anomaly.alertservice.repository;

import static org.assertj.core.api.Assertions.assertThat;

import com.anomaly.alertservice.PostgresTestBase;
import com.anomaly.alertservice.dto.ScoredWindowMessage;
import com.anomaly.alertservice.dto.TelemetrySeries;
import com.anomaly.alertservice.exception.MachineNotFoundException;
import com.anomaly.alertservice.service.TelemetryIngestionService;
import com.anomaly.alertservice.service.TelemetryQueryService;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Telemetry persistence (D-41), against a real PostgreSQL.
 *
 * <p>The property that matters most here is the same one as for alerts, for the
 * same reason: {@code telemetry.scored} is published in <strong>update
 * mode</strong>, so one window arrives repeatedly as it fills -- 5.67 emissions
 * per window on a live run, measured in Phase 3. Without an upsert keyed on the
 * window, the chart would hold five copies of every point.
 */
class TelemetryWindowTest extends PostgresTestBase {

    @Autowired TelemetryIngestionService ingestion;
    @Autowired TelemetryQueryService queries;
    @Autowired TelemetryWindowRepository windows;
    @Autowired JdbcTemplate jdbc;

    private static final Instant W1 = Instant.parse("2026-09-09T10:00:00Z");

    @BeforeEach
    void clean() {
        jdbc.update("DELETE FROM telemetry_window");
    }

    private static ScoredWindowMessage window(
            String machine, Instant start, int samples, boolean scored, Double temperature) {
        Map<String, Double> features = new HashMap<>();
        if (temperature != null) {
            features.put(ScoredWindowMessage.TEMPERATURE, temperature);
        }
        features.put(ScoredWindowMessage.VIBRATION, 1.13);
        features.put(ScoredWindowMessage.PRESSURE, 12.19);
        features.put(ScoredWindowMessage.POWER, 6.10);
        features.put(ScoredWindowMessage.ROTATION, 1033.56);
        features.put(ScoredWindowMessage.NULL_RATIO, 0.0);
        return new ScoredWindowMessage(
                "1.0",
                machine,
                "LINE-A",
                start,
                start.plusSeconds(60),
                samples,
                "RUNNING",
                scored,
                scored ? 0.176 : null,
                scored ? 0.9927653712984937 : null,
                scored ? Boolean.FALSE : null,
                features);
    }

    @Test
    @DisplayName("the same window delivered repeatedly stays one row")
    void updateModeEmissionsCollapseToOneRow() {
        // What update mode really does: the window is published while it fills,
        // so it arrives with 20, then 40, then 60 samples.
        ingestion.ingest(window("M-001", W1, 20, false, 45.0));
        ingestion.ingest(window("M-001", W1, 40, false, 45.5));
        ingestion.ingest(window("M-001", W1, 60, true, 45.97));

        assertThat(windows.count()).as("one window, one point on the chart").isEqualTo(1);

        // The last write wins, and it is the one built on the most samples.
        assertThat(jdbc.queryForObject("SELECT sample_count FROM telemetry_window", Integer.class))
                .isEqualTo(60);
        assertThat(jdbc.queryForObject("SELECT is_scored FROM telemetry_window", Boolean.class)).isTrue();
        assertThat(jdbc.queryForObject("SELECT temperature_c_mean FROM telemetry_window", Double.class))
                .isEqualTo(45.97);
    }

    @Test
    @DisplayName("an unscored window is still stored, with its sensor values")
    void unscoredWindowsAreKept() {
        // A window the model did not score still carries real measurements.
        // Dropping it would leave an unexplained hole in the curve.
        ingestion.ingest(window("M-001", W1, 12, false, 44.2));

        TelemetrySeries series =
                queries.series("M-001", W1.minusSeconds(60), W1.plusSeconds(600), 100);

        assertThat(series.points()).hasSize(1);
        TelemetrySeries.TelemetryPoint point = series.points().get(0);
        assertThat(point.scored()).isFalse();
        assertThat(point.anomalyScore()).as("no score, and not a fabricated zero").isNull();
        assertThat(point.temperatureCMean()).isEqualTo(44.2);
        assertThat(point.sampleCount()).isEqualTo(12);
    }

    @Test
    @DisplayName("a missing sensor is stored as null, never as zero")
    void missingSensorStaysNull() {
        // A failed sensor produces a null aggregate. Writing 0.0 would put a
        // plausible reading on the chart where there was no measurement.
        ingestion.ingest(window("M-001", W1, 60, true, null));

        assertThat(jdbc.queryForObject("SELECT temperature_c_mean FROM telemetry_window", Double.class))
                .isNull();
        assertThat(jdbc.queryForObject("SELECT vibration_mm_s_mean FROM telemetry_window", Double.class))
                .isEqualTo(1.13);
    }

    @Test
    @DisplayName("the series comes back in chronological order, filtered by range")
    void seriesIsOrderedAndRanged() {
        for (int i = 0; i < 6; i++) {
            ingestion.ingest(window("M-001", W1.plusSeconds(i * 10L), 60, true, 45.0 + i));
        }

        TelemetrySeries series =
                queries.series("M-001", W1.plusSeconds(20), W1.plusSeconds(40), 100);

        // The range is applied by PostgreSQL, not by trimming in Java.
        assertThat(series.points()).hasSize(3);
        assertThat(series.points())
                .extracting(TelemetrySeries.TelemetryPoint::windowStart)
                .isSorted();
        assertThat(series.points().get(0).windowStart()).isEqualTo(W1.plusSeconds(20));
    }

    @Test
    @DisplayName("a range larger than the limit is truncated and says so")
    void oversizedRangeIsTruncatedAndFlagged() {
        for (int i = 0; i < 10; i++) {
            ingestion.ingest(window("M-001", W1.plusSeconds(i * 10L), 60, true, 45.0));
        }

        TelemetrySeries series = queries.series("M-001", W1, W1.plusSeconds(600), 4);

        assertThat(series.points()).hasSize(4);
        // Saying so matters: a chart that silently drew a partial picture would
        // look complete.
        assertThat(series.truncated()).isTrue();
    }

    @Test
    @DisplayName("the window length is read from the data, not configured twice")
    void windowLengthComesFromTheData() {
        ingestion.ingest(window("M-001", W1, 60, true, 45.0));

        assertThat(queries.series("M-001", W1, W1.plusSeconds(120), 10).windowSeconds())
                .isEqualTo(60);
    }

    @Test
    @DisplayName("telemetry for an unknown machine is a 404, not an empty chart")
    void unknownMachineIsRejected() {
        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> queries.series("M-999", W1, W1.plusSeconds(600), 10))
                .isInstanceOf(MachineNotFoundException.class);
    }

    @Test
    @DisplayName("a machine seen only in telemetry is provisioned like one seen in an alert")
    void unknownMachineIsAutoProvisionedOnIngest() {
        ingestion.ingest(window("M-077", W1, 60, true, 45.0));

        assertThat(jdbc.queryForObject("SELECT count(*) FROM machine WHERE code = 'M-077'", Integer.class))
                .isEqualTo(1);
    }

    @Test
    @DisplayName("the retention purge drops old windows and keeps recent ones")
    void retentionPurgeIsBoundedByAge() {
        Instant old = Instant.now().minusSeconds(100 * 3600);
        Instant recent = Instant.now().minusSeconds(3600);
        ingestion.ingest(window("M-001", old, 60, true, 45.0));
        ingestion.ingest(window("M-001", recent, 60, true, 46.0));

        int deleted = windows.deleteOlderThan(Instant.now().minusSeconds(72 * 3600));

        assertThat(deleted).isEqualTo(1);
        assertThat(windows.count()).isEqualTo(1);
    }
}
