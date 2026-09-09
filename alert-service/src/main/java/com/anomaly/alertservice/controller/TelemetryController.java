package com.anomaly.alertservice.controller;

import com.anomaly.alertservice.config.AlertServiceProperties;
import com.anomaly.alertservice.dto.TelemetrySeries;
import com.anomaly.alertservice.service.TelemetryQueryService;
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
public class TelemetryController {

    private final TelemetryQueryService telemetry;
    private final AlertServiceProperties properties;

    public TelemetryController(
            TelemetryQueryService telemetry, AlertServiceProperties properties) {
        this.telemetry = telemetry;
        this.properties = properties;
    }

    @GetMapping("/{machineCode}/telemetry")
    public TelemetrySeries series(
            @PathVariable @Pattern(regexp = "^M-[0-9]{3}$") String machineCode,
            @RequestParam(required = false) Instant from,
            @RequestParam(required = false) Instant to,
            @RequestParam(required = false) Integer maxPoints) {

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
