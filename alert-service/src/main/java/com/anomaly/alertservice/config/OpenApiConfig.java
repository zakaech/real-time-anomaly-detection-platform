package com.anomaly.alertservice.config;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.servers.Server;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.context.annotation.Configuration;

/**
 * The OpenAPI document, generated from the controllers (decision D-50).
 *
 * <p>Generated rather than written. A hand-maintained {@code openapi.yaml}
 * drifts from the code on the first change and nothing fails when it does --
 * the one failure mode this project spends most of its effort avoiding. What is
 * published here cannot describe an endpoint that does not exist, because it is
 * read from the mappings themselves.
 *
 * <p>Reachable at {@code /v3/api-docs} (JSON) and {@code /swagger-ui.html}
 * directly on the service port. Not through the dashboard: nginx proxies only
 * {@code /api}, and exposing an API console on the operator origin would be a
 * deliberate decision rather than a side effect of a proxy rule.
 *
 * <p><strong>Known limit, stated rather than papered over.</strong> OpenAPI can
 * declare that {@code GET /api/v1/alerts/stream} produces
 * {@code text/event-stream}, and that is all it can say. The event protocol --
 * event names, the {@code id:} cursor, {@code Last-Event-ID}, the heartbeat --
 * has no schema in OpenAPI 3.1. It is therefore described in prose on that
 * operation. Modelling it as an ordinary JSON response would produce a document
 * that validates and lies.
 */
@Configuration
@OpenAPIDefinition(
        info =
                @Info(
                        title = "Alert Service API",
                        version = "0.1.0",
                        description =
                                """
                                Operator API of the real-time anomaly detection platform.

                                Alerts are produced by a Spark Structured Streaming job that scores \
                                sixty-second windows of industrial sensor telemetry, and are persisted \
                                idempotently into PostgreSQL.

                                Two properties are worth knowing before integrating.

                                **Delivery is at-least-once, never exactly-once.** The same alert can be \
                                consumed from Kafka more than once; the database collapses the repeats \
                                through a natural unique key, so a replay creates no duplicate row. \
                                Clients that also read the SSE stream must deduplicate on `alertId`.

                                **The data is entirely simulated.** No reading here came from a physical \
                                machine.

                                Errors are RFC 9457 `application/problem+json`, always carrying a \
                                `traceId` that matches the server logs. There is no authentication in \
                                v1: the `X-Operator` header is an audit field, not an identity.\
                                """),
        servers = {
            @Server(url = "/", description = "The service itself, or nginx proxying it at /api"),
        },
        tags = {
            @Tag(name = "Alerts", description = "Query, stream and acknowledge alerts"),
            @Tag(
                    name = "Telemetry",
                    description =
                            "Scored sensor windows, per machine. Per-window means, not a raw trace")
        })
public class OpenApiConfig {}
