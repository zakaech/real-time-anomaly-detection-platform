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
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
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
@Tag(name = "Alerts")
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

    @Operation(
            summary = "List alerts",
            description =
                    """
                    Filtered, sorted and paginated by PostgreSQL: the response carries one page, \
                    never the table.

                    `size` is capped at 100 server-side. A larger value is silently reduced to the \
                    ceiling rather than rejected, so a client cannot turn pagination into a \
                    formality by asking for everything.

                    `sort` takes `property,direction`. The property is a whitelist -- `detectedAt`, \
                    `severity`, `anomalyScore`, `status` -- because an arbitrary name would reach \
                    the database as an unvalidated identifier. Anything else is a 400. The \
                    direction defaults to `desc` unless it is exactly `asc`.

                    `severity` and `status` may be repeated to mean OR: \
                    `?severity=HIGH&severity=CRITICAL`.\
                    """)
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "One page of alerts"),
        @ApiResponse(
                responseCode = "400",
                description =
                        "Unknown sort property, `from` later than `to`, or a code that does not"
                                + " match its pattern",
                content = @Content(mediaType = "application/problem+json"))
    })
    @GetMapping
    public PageResponse<AlertSummary> list(
            @Parameter(description = "Zero-based page index") @RequestParam(defaultValue = "0") @Min(0) int page,
            @Parameter(description = "Page size. Defaults to 20, hard ceiling of 100")
                    @RequestParam(required = false)
                    @Min(1)
                    @Max(100)
                    Integer size,
            @Parameter(
                            description = "property,direction",
                            schema =
                                    @Schema(
                                            allowableValues = {
                                                "detectedAt,desc",
                                                "detectedAt,asc",
                                                "severity,desc",
                                                "severity,asc",
                                                "anomalyScore,desc",
                                                "anomalyScore,asc",
                                                "status,desc",
                                                "status,asc"
                                            },
                                            defaultValue = "detectedAt,desc"))
                    @RequestParam(defaultValue = "detectedAt,desc")
                    String sort,
            @Parameter(description = "Exact machine code", example = "M-003")
                    @RequestParam(required = false)
                    @Pattern(regexp = "^M-[0-9]{3}$")
                    String machineCode,
            @Parameter(description = "Exact production line code", example = "LINE-A")
                    @RequestParam(required = false)
                    @Pattern(regexp = "^LINE-[A-Z]$")
                    String lineCode,
            @Parameter(description = "Repeatable; several values mean OR")
                    @RequestParam(required = false)
                    List<AlertSeverity> severity,
            @Parameter(description = "Repeatable; several values mean OR")
                    @RequestParam(required = false)
                    List<AlertStatus> status,
            @Parameter(
                            description = "Lower bound on detectedAt, inclusive, ISO-8601 UTC",
                            example = "2026-09-07T00:00:00Z")
                    @RequestParam(required = false)
                    Instant from,
            @Parameter(description = "Upper bound on detectedAt, inclusive, ISO-8601 UTC")
                    @RequestParam(required = false)
                    Instant to) {

        int effectiveSize = size == null ? properties.api().defaultPageSize() : size;
        if (effectiveSize > properties.api().maxPageSize()) {
            // Hard ceiling: without it a single request can ask for the whole
            // table and turn pagination into a formality.
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

    @Operation(
            summary = "Get one alert in full",
            description =
                    """
                    Adds to the summary: the window bounds, the model that scored it (name, \
                    version, training date, artefact SHA-256), the ranked feature contributors, \
                    and the acknowledgement history.

                    It carries **no sensor time series**, and that is a property of the pipeline \
                    rather than an omission. Raw readings live only in the `telemetry.raw` topic, \
                    which is persisted nowhere, and the alert message itself never carries the \
                    feature vector. Use `GET /api/v1/machines/{machineCode}/telemetry` for the \
                    curve.

                    Note that `topContributors[].z_score` is snake_case while the rest of the \
                    document is camelCase. That is the field name in the alert contract the Spark \
                    job publishes, kept as-is rather than silently renamed at the boundary.\
                    """)
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "The alert"),
        @ApiResponse(
                responseCode = "404",
                description = "No alert with that identifier",
                content = @Content(mediaType = "application/problem+json"))
    })
    @GetMapping("/{alertId}")
    public AlertDetail detail(
            @Parameter(description = "Alert identifier (UUIDv5, deterministic)") @PathVariable
                    UUID alertId) {
        return queries.detail(alertId);
    }

    /**
     * Acknowledge an alert.
     *
     * <p>There is no authentication in v1, so the actor comes from a header and
     * falls back to "unknown". That is an audit field, and it is documented as
     * such rather than presented as an authenticated identity.
     */
    @Operation(
            summary = "Acknowledge an alert",
            description =
                    """
                    Moves the alert from `NEW` to `ACKNOWLEDGED` and records who did it, when, and \
                    why. The body is optional.

                    `expectedVersion` is optional too (decision D-09). Supply it and the update is \
                    refused with 409 if someone else changed the alert first; omit it and no \
                    concurrency check is made. Making it mandatory would burden a dashboard acting \
                    on freshly loaded rows; removing it would leave the silent failure where two \
                    operators act, one of them is overwritten, and neither is told.

                    `X-Operator` is an **audit field, not an identity**. There is no \
                    authentication in v1, so nothing verifies the value. It defaults to `unknown`.\
                    """)
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "The alert after the transition"),
        @ApiResponse(
                responseCode = "400",
                description = "Comment longer than 1000 characters, or a malformed body",
                content = @Content(mediaType = "application/problem+json")),
        @ApiResponse(
                responseCode = "404",
                description = "No alert with that identifier",
                content = @Content(mediaType = "application/problem+json")),
        @ApiResponse(
                responseCode = "409",
                description =
                        "Already acknowledged, or `expectedVersion` no longer matches. The body"
                                + " carries `currentStatus` and `currentVersion` so the client can"
                                + " resynchronise without a second request",
                content = @Content(mediaType = "application/problem+json"))
    })
    @PostMapping("/{alertId}/acknowledge")
    public ResponseEntity<AlertDetail> acknowledge(
            @PathVariable UUID alertId,
            @RequestBody(required = false) @Valid AcknowledgeRequest request,
            @Parameter(description = "Who is acknowledging. Audit only, never verified")
                    @RequestHeader(value = "X-Operator", defaultValue = "unknown")
                    String operator) {
        AcknowledgeRequest body = request == null ? new AcknowledgeRequest(null, null) : request;
        return ResponseEntity.ok(lifecycle.acknowledge(alertId, body, operator));
    }

    @Operation(
            summary = "Aggregate alert statistics",
            description =
                    """
                    Counts by severity and status, the top machines, a time histogram, the \
                    acknowledgement rate and the median time to acknowledge. Every aggregate is \
                    computed by PostgreSQL, never in Java over a fetched page.

                    The range defaults to the last 24 hours.

                    Two figures are deliberately absent: the mean anomaly score, which has no \
                    operational meaning because scores are calibrated per model, and counts per \
                    model, because there is one active model. Both would be numbers nobody could \
                    act on.\
                    """)
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "The aggregates"),
        @ApiResponse(
                responseCode = "400",
                description = "Unknown granularity, or `from` later than `to`",
                content = @Content(mediaType = "application/problem+json"))
    })
    @GetMapping("/stats")
    public AlertStatistics stats(
            @Parameter(description = "Range start. Defaults to 24 hours before `to`")
                    @RequestParam(required = false)
                    Instant from,
            @Parameter(description = "Range end. Defaults to now")
                    @RequestParam(required = false)
                    Instant to,
            @Parameter(
                            description = "Bucket width of the time histogram",
                            schema =
                                    @Schema(
                                            allowableValues = {"hour", "day"},
                                            defaultValue = "hour"))
                    @RequestParam(defaultValue = "hour")
                    String granularity,
            @Parameter(description = "How many machines to rank, 1 to 50")
                    @RequestParam(defaultValue = "10")
                    @Min(1)
                    @Max(50)
                    int machineLimit) {

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
