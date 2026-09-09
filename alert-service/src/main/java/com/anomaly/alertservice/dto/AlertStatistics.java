package com.anomaly.alertservice.dto;

import com.anomaly.alertservice.entity.AlertSeverity;
import com.anomaly.alertservice.entity.AlertStatus;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Dashboard aggregates, every one of them computed by PostgreSQL.
 *
 * <p>Deliberately omitted: mean anomaly score (no operational meaning -- scores
 * are calibrated per model, so their average says nothing), and counts per model
 * (there is one active model). Both would be figures nobody could act on.
 */
public record AlertStatistics(
        Instant from,
        Instant to,
        long total,
        Map<AlertSeverity, Long> bySeverity,
        Map<AlertStatus, Long> byStatus,
        List<MachineCount> byMachine,
        List<TimeBucket> overTime,
        double acknowledgementRate,
        Long medianSecondsToAcknowledge) {

    public record MachineCount(String machineCode, String lineCode, long count) {}

    public record TimeBucket(Instant bucketStart, long count) {}
}
