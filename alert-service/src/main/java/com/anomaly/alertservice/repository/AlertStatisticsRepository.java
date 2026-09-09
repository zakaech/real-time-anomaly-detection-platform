package com.anomaly.alertservice.repository;

import com.anomaly.alertservice.entity.AlertEntity;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

/**
 * Aggregations, all of them executed by PostgreSQL.
 *
 * <p>Separate from {@link AlertRepository} because these are read-only reporting
 * queries with a different shape: they return tuples, not entities, and none of
 * them should ever be mistaken for a way to load alerts.
 */
@Repository
public interface AlertStatisticsRepository extends JpaRepository<AlertEntity, UUID> {

    @Query(
            value = "SELECT count(*) FROM alert WHERE detected_at >= :from AND detected_at <= :to",
            nativeQuery = true)
    long countBetween(@Param("from") Instant from, @Param("to") Instant to);

    @Query(
            value =
                    """
                    SELECT severity, count(*)
                      FROM alert
                     WHERE detected_at >= :from AND detected_at <= :to
                     GROUP BY severity
                    """,
            nativeQuery = true)
    List<Object[]> countBySeverity(@Param("from") Instant from, @Param("to") Instant to);

    @Query(
            value =
                    """
                    SELECT status, count(*)
                      FROM alert
                     WHERE detected_at >= :from AND detected_at <= :to
                     GROUP BY status
                    """,
            nativeQuery = true)
    List<Object[]> countByStatus(@Param("from") Instant from, @Param("to") Instant to);

    /** Top machines by alert count. Bounded: a fleet listing is not a statistic. */
    @Query(
            value =
                    """
                    SELECT m.code, m.line_code, count(*) AS total
                      FROM alert a
                      JOIN machine m ON m.id = a.machine_id
                     WHERE a.detected_at >= :from AND a.detected_at <= :to
                     GROUP BY m.code, m.line_code
                     ORDER BY total DESC, m.code ASC
                     LIMIT :maxRows
                    """,
            nativeQuery = true)
    List<Object[]> countByMachine(
            @Param("from") Instant from, @Param("to") Instant to, @Param("maxRows") int maxRows);

    /**
     * Alerts per time bucket.
     *
     * <p>The granularity is interpolated into {@code date_trunc}, so the caller
     * must only ever pass a value it has validated against a fixed list -- the
     * controller does. It cannot be a bind parameter: {@code date_trunc} takes a
     * literal unit, not an expression.
     */
    @Query(
            value =
                    """
                    SELECT date_trunc(:granularity, detected_at) AS bucket, count(*)
                      FROM alert
                     WHERE detected_at >= :from AND detected_at <= :to
                     GROUP BY bucket
                     ORDER BY bucket ASC
                    """,
            nativeQuery = true)
    List<Object[]> countOverTime(
            @Param("from") Instant from,
            @Param("to") Instant to,
            @Param("granularity") String granularity);
}
