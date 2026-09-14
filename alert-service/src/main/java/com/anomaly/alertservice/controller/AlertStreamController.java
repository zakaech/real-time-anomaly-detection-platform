package com.anomaly.alertservice.controller;

import com.anomaly.alertservice.entity.AlertSeverity;
import com.anomaly.alertservice.service.AlertBroadcaster;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
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
 * <p>SSE rather than WebSocket (decision D-08). The need is strictly one-way:
 * the server pushes, and the client acts through REST. A WebSocket would add
 * an upstream channel nobody uses, and its reconnection logic would have to
 * be written by hand, whereas EventSource reconnects on its own and replays
 * Last-Event-ID, which matters for a shop floor dashboard left open for hours
 * on a flaky network.
 *
 * <p>Filtering happens on the server. Filtering in the browser would mean
 * shipping the whole stream to every client and discarding most of it there.
 */
@RestController
@RequestMapping("/api/v1/alerts")
@Validated
@Tag(name = "Alerts")
public class AlertStreamController {

    private final AlertBroadcaster broadcaster;

    public AlertStreamController(AlertBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    @Operation(
            summary = "Live alert stream (Server-Sent Events)",
            description =
                    """
                    A long-lived `text/event-stream` connection. **OpenAPI can state the media \
                    type and nothing more** -- it has no vocabulary for an event protocol, so the \
                    wire format is described here in prose rather than modelled by a schema that \
                    would validate and mislead.

                    ### Frames on the wire

                    Three event names are emitted:

                    | `event:` | When | `data:` |
                    |---|---|---|
                    | `alert.created` | a new alert was committed to PostgreSQL | an alert object |
                    | `alert.updated` | an operator changed one (an acknowledgement) | the same shape |
                    | `heartbeat` | every 15 s, no matter what | `{}` |

                    The `data:` payload of the two `alert.*` events is deliberately small -- \
                    `alertId`, `eventSeq`, `machineCode`, `lineCode`, `severity`, `status`, \
                    `anomalyScore`, `detectedAt`. Enough to render a row; anything more is fetched \
                    over REST.

                    The heartbeat is not decoration. Without traffic, a proxy closes an idle \
                    connection after 30 to 60 seconds and the dashboard stops updating **while \
                    still showing itself as connected** -- the most deceptive failure a live \
                    display has.

                    ### `id:` and `Last-Event-ID`

                    Every `alert.*` frame carries `id: <event_seq>`, a monotonic per-alert \
                    sequence from a database sequence -- not the alert UUID, because a random \
                    identifier cannot order anything and therefore cannot serve as a cursor.

                    A browser stores the last `id:` it saw and sends it back as the \
                    `Last-Event-ID` **request header** when it reconnects automatically. The \
                    server then replays alerts with a higher sequence, capped at 500 events: a \
                    client that has been away for hours is expected to reload the list rather than \
                    receive that history down a pipe.

                    Two consequences worth knowing before building a client:

                    - **`EventSource` cannot set request headers.** The browser sends \
                      `Last-Event-ID` by itself, only on an automatic reconnection. An application \
                      cannot supply it on a first connection, so the correct sequence is: load \
                      over REST, then open the stream.
                    - **An unparseable value means "start from now", not an error.** Refusing the \
                      connection would leave a dashboard permanently blank over a malformed header.

                    ### What this stream is not

                    It is **not a source of truth and not a persistence mechanism**. A \
                    disconnected client loses nothing: the alert is in PostgreSQL and \
                    `GET /api/v1/alerts` returns it. The stream only spares the dashboard from \
                    polling. Combined with at-least-once delivery from Kafka, a client must \
                    **deduplicate on `alertId`** -- the same alert can legitimately arrive twice.

                    Filtering is applied server-side; a filtered client never receives what it did \
                    not ask for.

                    ### Through the nginx proxy

                    The dashboard reaches this at `/api/v1/alerts/stream` on its own origin. That \
                    location needs `proxy_buffering off`, `proxy_cache off`, a long \
                    `proxy_read_timeout` and `Connection ''`. Omitting any of them produces the \
                    same silent failure: the connection establishes, and no event ever arrives.\
                    """)
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description =
                        "The stream is open. It stays open until the client disconnects or the"
                                + " server shuts down",
                content =
                        @Content(
                                mediaType = MediaType.TEXT_EVENT_STREAM_VALUE,
                                schema = @Schema(type = "string", format = "text/event-stream"))),
        @ApiResponse(
                responseCode = "400",
                description = "`lineCode` does not match its pattern",
                content = @Content(mediaType = "application/problem+json"))
    })
    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(
            @Parameter(description = "Repeatable; only these severities are pushed")
                    @RequestParam(required = false)
                    List<AlertSeverity> severity,
            @Parameter(description = "Only this production line is pushed", example = "LINE-A")
                    @RequestParam(required = false)
                    @Pattern(regexp = "^LINE-[A-Z]$")
                    String lineCode,
            @Parameter(
                            description =
                                    "Sent by the browser on an automatic reconnection, never by"
                                        + " application code. Replay is capped at 500 events",
                            in = io.swagger.v3.oas.annotations.enums.ParameterIn.HEADER)
                    @RequestHeader(value = "Last-Event-ID", required = false)
                    String lastEventId) {

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
