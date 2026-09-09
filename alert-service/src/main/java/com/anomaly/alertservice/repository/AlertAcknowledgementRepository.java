package com.anomaly.alertservice.repository;

import com.anomaly.alertservice.entity.AlertAcknowledgementEntity;
import java.time.Instant;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface AlertAcknowledgementRepository
        extends JpaRepository<AlertAcknowledgementEntity, Long> {

    List<AlertAcknowledgementEntity> findByAlertIdOrderByOccurredAtDesc(java.util.UUID alertId);

    /**
     * Median seconds between an alert being detected and first acknowledged.
     *
     * <p>A median rather than a mean: one alert acknowledged after a weekend
     * would drag an average somewhere no operator would recognise.
     * {@code percentile_cont} is computed by PostgreSQL over the join; pulling
     * the rows back to sort them in Java would be the same query plus a network
     * round trip.
     */
    @Query(
            value =
                    """
                    SELECT percentile_cont(0.5) WITHIN GROUP (
                               ORDER BY EXTRACT(EPOCH FROM (ack.occurred_at - a.detected_at)))
                      FROM alert a
                      JOIN (SELECT alert_id, MIN(occurred_at) AS occurred_at
                              FROM alert_acknowledgement
                             WHERE new_status = 'ACKNOWLEDGED'
                             GROUP BY alert_id) ack
                        ON ack.alert_id = a.id
                     WHERE a.detected_at >= :from AND a.detected_at <= :to
                    """,
            nativeQuery = true)
    Double medianSecondsToAcknowledge(@Param("from") Instant from, @Param("to") Instant to);
}
