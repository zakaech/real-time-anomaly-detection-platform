package com.anomaly.alertservice.dto;

import com.anomaly.alertservice.entity.AlertSeverity;
import com.anomaly.alertservice.entity.AlertStatus;
import java.time.Instant;
import java.util.UUID;

/**
 * Payload of an SSE event.
 *
 * <p>Small on purpose: the stream tells a dashboard that something happened and
 * gives it enough to render a row. Anything more is fetched over REST. The
 * stream is an accelerator for the display, never a source of truth -- a
 * disconnected client loses nothing, because the alert is in PostgreSQL.
 */
public record AlertStreamEvent(
        UUID alertId,
        long eventSeq,
        String machineCode,
        String lineCode,
        AlertSeverity severity,
        AlertStatus status,
        double anomalyScore,
        Instant detectedAt) {}
