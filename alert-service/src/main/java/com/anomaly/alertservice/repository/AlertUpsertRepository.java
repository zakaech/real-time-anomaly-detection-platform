package com.anomaly.alertservice.repository;

import java.time.Instant;
import java.util.UUID;

/**
 * The idempotent write path, kept apart from the JPA repository.
 *
 * <p>Spring Data cannot express this one: a {@code @Modifying} query is only
 * allowed to return void or a row count, and what this statement needs to return
 * is <em>whether the row was inserted or updated</em>. Writing it against
 * JdbcTemplate is also more honest -- it is hand-written SQL, and pretending
 * otherwise by dressing it as a derived query would hide the part that matters.
 */
public interface AlertUpsertRepository {

    /**
     * Insert an alert, or absorb the replay of one already stored.
     *
     * @return {@code true} only if a row was really created. A replay returns
     *     {@code false}, and so does an alert an operator has already
     *     acknowledged, which the statement deliberately refuses to touch.
     */
    boolean upsert(
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
            String features);
}
