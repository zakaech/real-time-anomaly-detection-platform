package com.anomaly.alertservice.repository;

import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * The single statement that makes at-least-once delivery survivable.
 *
 * <p>Three details of it are load-bearing, and each replaces an approach that
 * looks equivalent and is not:
 *
 * <ol>
 *   <li><strong>{@code ON CONFLICT (id) DO UPDATE}</strong> rather than a
 *       check-then-insert. {@code existsById} followed by {@code save} is a
 *       time-of-check race: two consumer threads can both pass the check, and
 *       the loser then fails on the constraint -- turning an operation that is
 *       supposed to be idempotent into an error. Only the database can arbitrate
 *       this atomically.
 *   <li><strong>{@code WHERE alert.status = 'NEW'}</strong>. An alert an
 *       operator has already handled is never rewritten by a Kafka replay.
 *       Without it, a Spark incident would push acknowledged alerts back into
 *       the operator queue.
 *   <li><strong>{@code RETURNING (xmax = 0)}</strong>. {@code xmax} is zero on a
 *       row that was really inserted and non-zero on one that was updated. That
 *       single boolean is what lets the SSE stream fire only for a genuine
 *       creation; without it every duplicate would make the dashboard blink.
 * </ol>
 *
 * <p>{@code DO UPDATE} rather than {@code DO NOTHING} because a replay can carry
 * a revised score or consecutive-window count, and the latest version of the
 * fact is the useful one.
 */
@Repository
class AlertUpsertRepositoryImpl implements AlertUpsertRepository {

    private static final String SQL =
            """
            INSERT INTO alert (id, machine_id, status, severity, anomaly_score, score_threshold,
                               detected_at, window_start, window_end, published_at,
                               consecutive_windows, model_name, model_version,
                               model_trained_at, model_artifact_sha256,
                               top_contributors, features)
            VALUES (?, ?, 'NEW', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    CAST(? AS jsonb), CAST(? AS jsonb))
            ON CONFLICT (id) DO UPDATE
               SET severity              = EXCLUDED.severity,
                   anomaly_score         = EXCLUDED.anomaly_score,
                   score_threshold       = EXCLUDED.score_threshold,
                   consecutive_windows   = EXCLUDED.consecutive_windows,
                   top_contributors      = EXCLUDED.top_contributors,
                   features              = EXCLUDED.features,
                   model_trained_at      = EXCLUDED.model_trained_at,
                   model_artifact_sha256 = EXCLUDED.model_artifact_sha256,
                   updated_at            = now()
             WHERE alert.status = 'NEW'
            RETURNING (xmax = 0)
            """;

    private final JdbcTemplate jdbc;

    AlertUpsertRepositoryImpl(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public boolean upsert(
            UUID id,
            Long machineId,
            String severity,
            double anomalyScore,
            double scoreThreshold,
            Instant detectedAt,
            Instant windowStart,
            Instant windowEnd,
            Instant publishedAt,
            int consecutiveWindows,
            String modelName,
            String modelVersion,
            Instant modelTrainedAt,
            String modelArtifactSha256,
            String topContributors,
            String features) {

        List<Boolean> result =
                jdbc.query(
                        SQL,
                        preparedStatement -> {
                            int index = 1;
                            preparedStatement.setObject(index++, id);
                            preparedStatement.setLong(index++, machineId);
                            preparedStatement.setString(index++, severity);
                            preparedStatement.setDouble(index++, anomalyScore);
                            preparedStatement.setDouble(index++, scoreThreshold);
                            preparedStatement.setObject(index++, atUtc(detectedAt));
                            preparedStatement.setObject(index++, atUtc(windowStart));
                            preparedStatement.setObject(index++, atUtc(windowEnd));
                            preparedStatement.setObject(index++, atUtc(publishedAt));
                            preparedStatement.setInt(index++, consecutiveWindows);
                            preparedStatement.setString(index++, modelName);
                            preparedStatement.setString(index++, modelVersion);
                            if (modelTrainedAt == null) {
                                preparedStatement.setNull(index++, Types.TIMESTAMP_WITH_TIMEZONE);
                            } else {
                                preparedStatement.setObject(index++, atUtc(modelTrainedAt));
                            }
                            preparedStatement.setString(index++, modelArtifactSha256);
                            preparedStatement.setString(index++, topContributors);
                            preparedStatement.setString(index, features);
                        },
                        (rs, rowNum) -> rs.getBoolean(1));

        // No row comes back when the WHERE clause blocked the update, which
        // happens for an alert that is no longer NEW. That is not an insertion
        // either, so both cases collapse to false.
        return !result.isEmpty() && Boolean.TRUE.equals(result.get(0));
    }

    /**
     * pgjdbc binds an OffsetDateTime to timestamptz directly; an Instant is not
     * a JDBC type and would be rejected.
     */
    private static OffsetDateTime atUtc(Instant instant) {
        return instant == null ? null : instant.atOffset(ZoneOffset.UTC);
    }
}
