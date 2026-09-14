package com.anomaly.alertservice.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.anomaly.alertservice.AlertMessages;
import com.anomaly.alertservice.PostgresTestBase;
import com.anomaly.alertservice.dto.AlertMessage;
import com.anomaly.alertservice.entity.AlertStatus;
import com.anomaly.alertservice.service.AlertIngestionService;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * The guarantee the service rests on, proved against a real PostgreSQL.
 *
 * <p>The scenario is not hypothetical: a crash test of the stream processor
 * measured <strong>8 duplicate alert deliveries</strong> on one run and 0 on
 * another (docs/10). At-least-once makes replays possible, not certain, so the
 * database has to absorb them whenever they happen.
 *
 * <p>What is asserted here is that <em>PostgreSQL</em> enforces this, not Java.
 * None of these tests would pass if the uniqueness came from an {@code if}.
 */
class AlertIdempotencyTest extends PostgresTestBase {

    @Autowired AlertIngestionService ingestion;
    @Autowired AlertRepository alerts;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void clean() {
        jdbc.update("DELETE FROM alert_acknowledgement");
        jdbc.update("DELETE FROM alert");
    }

    @Test
    @DisplayName("the same alert delivered twice creates one row")
    void replayCreatesNoSecondRow() {
        AlertMessage message = AlertMessages.valid();

        boolean firstDelivery = ingestion.ingest(message);
        boolean secondDelivery = ingestion.ingest(message);

        assertThat(firstDelivery).as("first delivery inserts").isTrue();
        assertThat(secondDelivery).as("replay is absorbed, not inserted").isFalse();
        assertThat(alerts.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("a replay does not resurrect an alert an operator already handled")
    void replayDoesNotReopenAnAcknowledgedAlert() {
        AlertMessage message = AlertMessages.valid();
        ingestion.ingest(message);
        UUID id = UUID.fromString(message.alertId());
        jdbc.update("UPDATE alert SET status = 'ACKNOWLEDGED' WHERE id = ?", id);

        boolean inserted = ingestion.ingest(message);

        assertThat(inserted).isFalse();
        // The WHERE status = 'NEW' clause is what stops a Spark incident from
        // pushing already handled alerts back into the operator queue.
        String status = jdbc.queryForObject("SELECT status FROM alert WHERE id = ?", String.class, id);
        assertThat(status).isEqualTo(AlertStatus.ACKNOWLEDGED.name());
    }

    @Test
    @DisplayName("a replay carrying an updated score refreshes the stored fact")
    void replayUpdatesAnAlertStillNew() {
        AlertMessage first = AlertMessages.valid();
        ingestion.ingest(first);

        AlertMessage updated =
                new AlertMessage(
                        first.schemaVersion(),
                        first.alertId(),
                        first.machineId(),
                        first.lineId(),
                        "HIGH",
                        0.995,
                        first.scoreThreshold(),
                        first.detectedAt(),
                        first.windowStart(),
                        first.windowEnd(),
                        first.publishedAt(),
                        4,
                        first.topContributors(),
                        first.features(),
                        first.model());

        boolean inserted = ingestion.ingest(updated);

        assertThat(inserted).as("still not an insertion").isFalse();
        assertThat(alerts.count()).isEqualTo(1);
        // DO UPDATE rather than DO NOTHING: a replay can legitimately carry a
        // revised count, and the latest version of the fact is the useful one.
        assertThat(jdbc.queryForObject("SELECT consecutive_windows FROM alert", Integer.class))
                .isEqualTo(4);
        assertThat(jdbc.queryForObject("SELECT severity FROM alert", String.class)).isEqualTo("HIGH");
    }

    @Test
    @DisplayName("concurrent deliveries of one alert still produce one row")
    void concurrentDeliveriesCollapseToOneRow() throws Exception {
        AlertMessage message = AlertMessages.valid();
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Callable<Boolean>> tasks =
                    java.util.Collections.nCopies(threads, () -> ingestion.ingest(message));
            List<Future<Boolean>> results = pool.invokeAll(tasks);

            long insertions = 0;
            for (Future<Boolean> result : results) {
                if (Boolean.TRUE.equals(result.get())) {
                    insertions++;
                }
            }
            // This is the test a check-then-act implementation fails: several
            // threads would pass an existsById at once, and all but one would
            // then blow up on the constraint.
            assertThat(insertions).as("exactly one thread inserted").isEqualTo(1);
            assertThat(alerts.count()).isEqualTo(1);
        } finally {
            pool.shutdown();
            pool.awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("the natural key constraint catches a duplicate the primary key would miss")
    void naturalKeyIsDefenceInDepth() {
        AlertMessage message = AlertMessages.valid();
        ingestion.ingest(message);
        UUID sameWindowDifferentId = UUID.fromString("11111111-1111-5111-8111-111111111111");

        // Simulates the upstream derivation of alert_id changing: the same
        // window arrives under a new UUID, so the primary key sees nothing wrong.
        assertThatThrownBy(
                        () ->
                                jdbc.update(
                                        """
                                        INSERT INTO alert (id, machine_id, severity, anomaly_score,
                                            score_threshold, detected_at, window_start, window_end,
                                            published_at, model_name, model_version)
                                        SELECT ?, machine_id, severity, anomaly_score, score_threshold,
                                               detected_at, window_start, window_end, published_at,
                                               model_name, model_version
                                          FROM alert WHERE id = ?
                                        """,
                                        sameWindowDifferentId,
                                        UUID.fromString(message.alertId())))
                .as("the natural key turns silent duplication into a loud failure")
                .hasMessageContaining("uq_alert_natural");

        assertThat(alerts.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("an unknown machine is provisioned instead of costing us the alert")
    void unknownMachineIsAutoProvisioned() {
        // M-042 is in no seed: it is a machine the fleet gained after the seed
        // was written. Rejecting its alerts would discard real detections.
        AlertMessage message =
                AlertMessages.valid(
                        "22222222-2222-5222-8222-222222222222",
                        "M-042",
                        Instant.parse("2026-09-08T02:00:00Z"),
                        Instant.parse("2026-09-08T02:01:00Z"));

        boolean inserted = ingestion.ingest(message);

        assertThat(inserted).isTrue();
        assertThat(jdbc.queryForObject("SELECT line_code FROM machine WHERE code = 'M-042'", String.class))
                .isEqualTo("LINE-C");
    }

    @Test
    @DisplayName("the database rejects a detection instant that disagrees with the window")
    void checkConstraintRefusesAnInconsistentDetectionInstant() {
        assertThatThrownBy(
                        () ->
                                jdbc.update(
                                        """
                                        INSERT INTO alert (id, machine_id, severity, anomaly_score,
                                            score_threshold, detected_at, window_start, window_end,
                                            published_at, model_name, model_version)
                                        VALUES (?, (SELECT id FROM machine WHERE code = 'M-011'),
                                                'HIGH', 0.99, 0.98,
                                                TIMESTAMPTZ '2026-09-08T03:00:00Z',
                                                TIMESTAMPTZ '2026-09-08T02:00:00Z',
                                                TIMESTAMPTZ '2026-09-08T02:01:00Z',
                                                now(), 'one_class_svm', '2.0.0')
                                        """,
                                        UUID.fromString("33333333-3333-5333-8333-333333333333")))
                .hasMessageContaining("ck_alert_detected_at");
    }
}
