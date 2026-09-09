package com.anomaly.alertservice.controller;

import com.anomaly.alertservice.entity.AlertSeverity;
import com.anomaly.alertservice.service.AlertBroadcaster;
import jakarta.validation.constraints.Pattern;
import java.util.List;
import java.util.Set;
import org.springframework.http.MediaType;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * Live alert stream, over Server-Sent Events.
 *
 * <p>SSE rather than WebSocket (decision D-08), and the reason is not fashion.
 * The need is strictly one-way: the server pushes, and the client acts through
 * REST. A WebSocket would add an upstream channel nobody uses, and its
 * reconnection logic would have to be written by hand -- whereas EventSource
 * reconnects on its own and replays Last-Event-ID, which matters for a shop
 * floor dashboard left open for hours on a flaky network.
 *
 * <p>Filtering happens on the server. Filtering in the browser would mean
 * shipping the whole stream to every client and discarding most of it there.
 */
@RestController
@RequestMapping("/api/v1/alerts")
@Validated
public class AlertStreamController {

    private final AlertBroadcaster broadcaster;

    public AlertStreamController(AlertBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(
            @RequestParam(required = false) List<AlertSeverity> severity,
            @RequestParam(required = false) @Pattern(regexp = "^LINE-[A-Z]$") String lineCode,
            @RequestHeader(value = "Last-Event-ID", required = false) String lastEventId) {

        Set<AlertSeverity> severities = severity == null ? Set.of() : Set.copyOf(severity);
        return broadcaster.subscribe(severities, lineCode, parseLastEventId(lastEventId));
    }

    /**
     * A browser sends back whatever id we last emitted. A value we cannot parse
     * means "start from now" rather than an error: refusing the connection would
     * leave the dashboard permanently blank over a malformed header.
     */
    private Long parseLastEventId(String lastEventId) {
        if (lastEventId == null || lastEventId.isBlank()) {
            return null;
        }
        try {
            return Long.parseLong(lastEventId.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
