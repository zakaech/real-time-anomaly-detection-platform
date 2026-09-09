package com.anomaly.alertservice;

import com.anomaly.alertservice.dto.AlertMessage;
import com.anomaly.alertservice.dto.ContributorMessage;
import com.anomaly.alertservice.dto.ModelRefMessage;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Alert fixtures.
 *
 * <p>The values are taken from a message actually read back from the topic
 * during the Phase 3 runs, rather than invented: the same UUIDv5, the same score
 * above the same threshold, the same three contributors. A fixture that does not
 * resemble production traffic tests the test.
 */
public final class AlertMessages {

    /** A real alert_id observed on the alerts topic. */
    public static final String REAL_ALERT_ID = "850eb1f1-029f-5b3d-9f62-123bfd5e8efc";

    public static final Instant WINDOW_START = Instant.parse("2026-09-08T01:35:00Z");
    public static final Instant WINDOW_END = Instant.parse("2026-09-08T01:36:00Z");

    private AlertMessages() {}

    public static AlertMessage valid() {
        return valid(REAL_ALERT_ID, "M-011", WINDOW_START, WINDOW_END);
    }

    public static AlertMessage valid(
            String alertId, String machineId, Instant windowStart, Instant windowEnd) {
        return new AlertMessage(
                "1.0",
                alertId,
                machineId,
                "LINE-C",
                "CRITICAL",
                0.9999995735846934,
                0.9927653712984937,
                // The contract requires detected_at to equal window_end.
                windowEnd,
                windowStart,
                windowEnd,
                Instant.parse("2026-09-08T01:36:22.991Z"),
                2,
                List.of(
                        new ContributorMessage("power_per_rpm_stddev", 15.880134925166342),
                        new ContributorMessage("power_kw_range", 9.11)),
                Map.of(),
                new ModelRefMessage(
                        "one_class_svm",
                        "2.0.0",
                        Instant.parse("2026-09-07T00:36:44.723Z"),
                        "0f608323f944e63d30b80cc91418c10b3cfb1d3fc46bac184d3591c03a8d0aa5"));
    }

    /** A distinct alert, for tests that need more than one row. */
    public static AlertMessage another(String machineId, Instant windowStart) {
        return valid(
                UUID.randomUUID().toString().replaceFirst("(?<=^.{14}).", "5"),
                machineId,
                windowStart,
                windowStart.plusSeconds(60));
    }

    public static byte[] json(String raw) {
        return raw.getBytes(StandardCharsets.UTF_8);
    }

    /** The wire form of a valid alert, as the topic carries it. */
    public static String validJson() {
        return """
               {
                 "schema_version": "1.0",
                 "alert_id": "850eb1f1-029f-5b3d-9f62-123bfd5e8efc",
                 "machine_id": "M-011",
                 "line_id": "LINE-C",
                 "severity": "CRITICAL",
                 "status": "NEW",
                 "anomaly_score": 0.9999995735846934,
                 "score_threshold": 0.9927653712984937,
                 "detected_at": "2026-09-08T01:36:00.000Z",
                 "window_start": "2026-09-08T01:35:00.000Z",
                 "window_end": "2026-09-08T01:36:00.000Z",
                 "published_at": "2026-09-08T01:36:22.991Z",
                 "consecutive_windows": 2,
                 "top_contributors": [
                   {"feature": "power_per_rpm_stddev", "z_score": 15.880134925166342}
                 ],
                 "features": {},
                 "model": {
                   "name": "one_class_svm",
                   "version": "2.0.0",
                   "trained_at": "2026-09-07T00:36:44.723Z",
                   "artifact_sha256":
                     "0f608323f944e63d30b80cc91418c10b3cfb1d3fc46bac184d3591c03a8d0aa5"
                 }
               }
               """;
    }
}
