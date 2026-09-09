package com.anomaly.alertservice.controller;

import com.anomaly.alertservice.config.AlertServiceProperties;
import com.anomaly.alertservice.dto.AcknowledgeRequest;
import com.anomaly.alertservice.dto.AlertDetail;
import com.anomaly.alertservice.dto.AlertStatistics;
import com.anomaly.alertservice.dto.AlertSummary;
import com.anomaly.alertservice.dto.PageResponse;
import com.anomaly.alertservice.entity.AlertSeverity;
import com.anomaly.alertservice.entity.AlertStatus;
import com.anomaly.alertservice.service.AlertLifecycleService;
import com.anomaly.alertservice.service.AlertQueryService;
import com.anomaly.alertservice.service.AlertStatisticsService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** The operator API. */
@RestController
@RequestMapping("/api/v1/alerts")
@Validated
public class AlertController {

    /**
     * Sort is a whitelist, not free text. An arbitrary property name would reach
     * the database as an unvalidated identifier and, at best, sort on an
     * unindexed column; at worst it produces a 500 from a property that does not
     * exist.
     */
    private static final Set<String> SORTABLE =
            Set.of("detectedAt", "severity", "anomalyScore", "status");

    private static final Set<String> GRANULARITIES = Set.of("hour", "day");

    private final AlertQueryService queries;
    private final AlertLifecycleService lifecycle;
    private final AlertStatisticsService statistics;
    private final AlertServiceProperties properties;

    public AlertController(
            AlertQueryService queries,
            AlertLifecycleService lifecycle,
            AlertStatisticsService statistics,
            AlertServiceProperties properties) {
        this.queries = queries;
        this.lifecycle = lifecycle;
        this.statistics = statistics;
        this.properties = properties;
    }

    @GetMapping
    public PageResponse<AlertSummary> list(
            @RequestParam(defaultValue = "0") @Min(0) int page,
            @RequestParam(required = false) @Min(1) @Max(100) Integer size,
            @RequestParam(defaultValue = "detectedAt,desc") String sort,
            @RequestParam(required = false) @Pattern(regexp = "^M-[0-9]{3}$") String machineCode,
            @RequestParam(required = false) @Pattern(regexp = "^LINE-[A-Z]$") String lineCode,
            @RequestParam(required = false) List<AlertSeverity> severity,
            @RequestParam(required = false) List<AlertStatus> status,
            @RequestParam(required = false) Instant from,
            @RequestParam(required = false) Instant to) {

        int effectiveSize = size == null ? properties.api().defaultPageSize() : size;
        if (effectiveSize > properties.api().maxPageSize()) {
            // A hard ceiling, not advice: without it a single request can ask
            // for the whole table and turn pagination into a formality.
            effectiveSize = properties.api().maxPageSize();
        }
        if (from != null && to != null && from.isAfter(to)) {
            throw new ResponseStatusException(
                    org.springframework.http.HttpStatus.BAD_REQUEST, "from must not be after to");
        }

        var pageable = PageRequest.of(page, effectiveSize, parseSort(sort));
        var filter =
                new AlertQueryService.AlertFilter(
                        machineCode, lineCode, severity, status, from, to);
        return PageResponse.of(queries.search(filter, pageable));
    }

    @GetMapping("/{alertId}")
    public AlertDetail detail(@PathVariable UUID alertId) {
        return queries.detail(alertId);
    }

    /**
     * Acknowledge an alert.
     *
     * <p>There is no authentication in v1, so the actor comes from a header and
     * falls back to "unknown". That is an audit field, and it is documented as
     * such rather than presented as an authenticated identity.
     */
    @PostMapping("/{alertId}/acknowledge")
    public ResponseEntity<AlertDetail> acknowledge(
            @PathVariable UUID alertId,
            @RequestBody(required = false) @Valid AcknowledgeRequest request,
            @RequestHeader(value = "X-Operator", defaultValue = "unknown") String operator) {
        AcknowledgeRequest body = request == null ? new AcknowledgeRequest(null, null) : request;
        return ResponseEntity.ok(lifecycle.acknowledge(alertId, body, operator));
    }

    @GetMapping("/stats")
    public AlertStatistics stats(
            @RequestParam(required = false) Instant from,
            @RequestParam(required = false) Instant to,
            @RequestParam(defaultValue = "hour") String granularity,
            @RequestParam(defaultValue = "10") @Min(1) @Max(50) int machineLimit) {

        if (!GRANULARITIES.contains(granularity)) {
            // date_trunc takes a literal unit and cannot be bound as a
            // parameter, so this value is interpolated into SQL. The whitelist
            // is what makes that safe.
            throw new ResponseStatusException(
                    org.springframework.http.HttpStatus.BAD_REQUEST,
                    "granularity must be one of " + GRANULARITIES);
        }
        Instant effectiveTo = to == null ? Instant.now() : to;
        Instant effectiveFrom = from == null ? effectiveTo.minus(24, ChronoUnit.HOURS) : from;
        if (effectiveFrom.isAfter(effectiveTo)) {
            throw new ResponseStatusException(
                    org.springframework.http.HttpStatus.BAD_REQUEST, "from must not be after to");
        }
        return statistics.summary(effectiveFrom, effectiveTo, granularity, machineLimit);
    }

    private Sort parseSort(String sort) {
        String[] parts = sort.split(",", 2);
        String property = parts[0].trim();
        if (!SORTABLE.contains(property)) {
            throw new ResponseStatusException(
                    org.springframework.http.HttpStatus.BAD_REQUEST,
                    "sort property must be one of " + SORTABLE);
        }
        Sort.Direction direction =
                parts.length > 1 && "asc".equalsIgnoreCase(parts[1].trim())
                        ? Sort.Direction.ASC
                        : Sort.Direction.DESC;
        return Sort.by(direction, property);
    }
}
