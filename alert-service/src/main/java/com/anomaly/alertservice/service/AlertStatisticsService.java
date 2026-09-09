package com.anomaly.alertservice.service;

import com.anomaly.alertservice.dto.AlertStatistics;
import com.anomaly.alertservice.entity.AlertSeverity;
import com.anomaly.alertservice.entity.AlertStatus;
import com.anomaly.alertservice.repository.AlertAcknowledgementRepository;
import com.anomaly.alertservice.repository.AlertStatisticsRepository;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Dashboard aggregates.
 *
 * <p>Every figure here is produced by a GROUP BY in PostgreSQL. Counting rows in
 * Java would mean fetching them all first, which is the one thing a statistics
 * endpoint must never do.
 */
@Service
public class AlertStatisticsService {

    private final AlertStatisticsRepository statistics;
    private final AlertAcknowledgementRepository acknowledgements;

    public AlertStatisticsService(
            AlertStatisticsRepository statistics,
            AlertAcknowledgementRepository acknowledgements) {
        this.statistics = statistics;
        this.acknowledgements = acknowledgements;
    }

    @Transactional(readOnly = true)
    public AlertStatistics summary(Instant from, Instant to, String granularity, int machineLimit) {
        long total = statistics.countBetween(from, to);

        Map<AlertSeverity, Long> bySeverity = new EnumMap<>(AlertSeverity.class);
        for (Object[] row : statistics.countBySeverity(from, to)) {
            bySeverity.put(AlertSeverity.valueOf((String) row[0]), ((Number) row[1]).longValue());
        }

        Map<AlertStatus, Long> byStatus = new EnumMap<>(AlertStatus.class);
        long acknowledged = 0;
        for (Object[] row : statistics.countByStatus(from, to)) {
            AlertStatus status = AlertStatus.valueOf((String) row[0]);
            long count = ((Number) row[1]).longValue();
            byStatus.put(status, count);
            if (status != AlertStatus.NEW) {
                // Anything that left NEW was acted upon by an operator.
                acknowledged += count;
            }
        }

        List<AlertStatistics.MachineCount> byMachine =
                statistics.countByMachine(from, to, machineLimit).stream()
                        .map(
                                row ->
                                        new AlertStatistics.MachineCount(
                                                (String) row[0],
                                                (String) row[1],
                                                ((Number) row[2]).longValue()))
                        .toList();

        List<AlertStatistics.TimeBucket> overTime =
                statistics.countOverTime(from, to, granularity).stream()
                        .map(
                                row ->
                                        new AlertStatistics.TimeBucket(
                                                ((java.sql.Timestamp) row[0]).toInstant(),
                                                ((Number) row[1]).longValue()))
                        .toList();

        double acknowledgementRate = total == 0 ? 0.0 : (double) acknowledged / total;
        Double median = acknowledgements.medianSecondsToAcknowledge(from, to);

        return new AlertStatistics(
                from,
                to,
                total,
                bySeverity,
                byStatus,
                byMachine,
                overTime,
                acknowledgementRate,
                median == null ? null : median.longValue());
    }
}
