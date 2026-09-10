package com.anomaly.alertservice.controller;

import com.anomaly.alertservice.config.AlertServiceProperties;
import com.anomaly.alertservice.dto.TelemetrySeries;
import com.anomaly.alertservice.service.TelemetryQueryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.Pattern;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * The telemetry curve for one machine (D-41).
 *
 * <p>Nested under the machine because the series belongs to it: there is no
 * useful cross-machine view of a sensor reading, and a flat endpoint would
 * invite one.
 */
@RestController
@RequestMapping("/api/v1/machines")
@Validated
@Tag(name = "Telemetry")
public class TelemetryController {

    private final TelemetryQueryService telemetry;
    private final AlertServiceProperties properties;

    public TelemetryController(
            TelemetryQueryService telemetry, AlertServiceProperties properties) {
        this.telemetry = telemetry;
        this.properties = properties;
    }

    @Operation(
            summary = "Sensor curve for one machine",
            description =
                    """
                    The five sensor signals (temperature, vibration, pressure, power, rotation) \
                    over a time range, with the windows the model flagged marked as anomalous.

                    **These are per-window means, not a raw sensor trace**, and the distinction \
                    matters. Raw readings go to the `telemetry.raw` topic, which has seven days of \
                    retention and is persisted nowhere; what is stored is what the pipeline \
                    computed and published on `telemetry.scored` (decision D-41). The window \
                    length the points describe is returned in `windowSeconds` rather than assumed \
                    by the caller.

                    Three fields exist so a client can render honestly instead of plausibly:

                    - a **null sensor mean** is a sensor that produced nothing. It is left null, \
                      never defaulted to zero, so a chart can break the line rather than draw a \
                      measurement that was not taken.
                    - `scored: false` marks a window that was measured but not scored -- too few \
                      samples to be mature. Its sensor values are real; its `anomalyScore` is null.
                    - `truncated: true` means the range held more windows than `maxPoints` \
                      allowed, so a chart can say so instead of silently drawing a partial picture \
                      as a complete one.

                    Retention is 72 hours by default. Older windows are purged, so a distant \
                    `from` returns an empty series rather than an error.\
                    """)
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "The series, possibly empty"),
        @ApiResponse(
                responseCode = "400",
                description = "`from` later than `to`, `maxPoints` below 1, or a malformed code",
                content = @Content(mediaType = "application/problem+json")),
        @ApiResponse(
                responseCode = "404",
                description = "No machine with that code",
                content = @Content(mediaType = "application/problem+json"))
    })
    @GetMapping("/{machineCode}/telemetry")
    public TelemetrySeries series(
            @Parameter(description = "Machine code", example = "M-003")
                    @PathVariable
                    @Pattern(regexp = "^M-[0-9]{3}$")
                    String machineCode,
            @Parameter(description = "Range start. Defaults to one hour before `to`")
                    @RequestParam(required = false)
                    Instant from,
            @Parameter(description = "Range end. Defaults to now")
                    @RequestParam(required = false)
                    Instant to,
            @Parameter(
                            description =
                                    "Maximum points returned. Defaults to 500, hard ceiling of"
                                            + " 5000. A larger value is reduced to the ceiling")
                    @RequestParam(required = false)
                    Integer maxPoints) {

        Instant effectiveTo = to == null ? Instant.now() : to;
        Instant effectiveFrom = from == null ? effectiveTo.minus(1, ChronoUnit.HOURS) : from;
        if (effectiveFrom.isAfter(effectiveTo)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "from must not be after to");
        }

        int ceiling = properties.telemetry().maxPoints();
        int limit = maxPoints == null ? properties.telemetry().defaultPoints() : maxPoints;
        if (limit < 1) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "maxPoints must be at least 1");
        }
        // A hard ceiling rather than advice, exactly like the alert page size:
        // without it one request can ask for the whole table.
        limit = Math.min(limit, ceiling);

        return telemetry.series(machineCode, effectiveFrom, effectiveTo, limit);
    }
}
