package com.anomaly.alertservice.repository;

import com.anomaly.alertservice.entity.TelemetryWindowEntity;
import java.time.Instant;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

/**
 * Reads for the dashboard curve; the write is in {@link TelemetryUpsertRepository}.
 */
@Repository
public interface TelemetryWindowRepository
        extends JpaRepository<TelemetryWindowEntity, Long>, TelemetryUpsertRepository {

    /**
     * One machine, one time range, chronological.
     *
     * <p>Bounded by the caller rather than open-ended: a curve is drawn from a
     * few hundred points, and letting a client ask for a month of 10-second
     * windows would be a way to pull the whole table through the API.
     */
    @Query(
            """
            SELECT w FROM TelemetryWindowEntity w
             WHERE w.machineId = :machineId
               AND w.windowStart >= :from
               AND w.windowStart <= :to
             ORDER BY w.windowStart ASC
            """)
    List<TelemetryWindowEntity> findRange(
            @Param("machineId") Long machineId,
            @Param("from") Instant from,
            @Param("to") Instant to,
            org.springframework.data.domain.Pageable limit);

    /**
     * Drop windows older than the retention horizon.
     *
     * <p>Without this the table grows without bound at roughly 5 700 rows an
     * hour (measured), for data whose only consumer is a chart showing recent
     * history.
     */
    @org.springframework.data.jpa.repository.Modifying
    // Carries its own transaction rather than trusting every caller to open one:
    // a bulk delete that silently depends on ambient context is a trap for the
    // next caller, not a saving.
    @org.springframework.transaction.annotation.Transactional
    @Query("DELETE FROM TelemetryWindowEntity w WHERE w.windowStart < :before")
    int deleteOlderThan(@Param("before") Instant before);
}
