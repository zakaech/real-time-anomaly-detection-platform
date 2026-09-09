# alert-service

Spring Boot 3 service on Java 17. Consumes the `alerts` topic, persists each
alert idempotently into PostgreSQL, and serves the operator API — a filtered and
paginated list, a detail view, acknowledgement, statistics, and a live SSE
stream.

## Delivery semantics

**At-least-once from Kafka, plus idempotence in PostgreSQL** — which gives
*effectively-once persistence*. It is **not** exactly-once and is never
described as such: Kafka does not take part in a distributed transaction, and
the Spark sink upstream is not transactional.

Phase 3 crash-tested the stream processor and measured **8 duplicate alert
deliveries** on one run and **0** on another. Replays are real and intermittent,
so the database has to absorb them whenever they happen.

## The one statement that matters

```sql
INSERT INTO alert (...) VALUES (...)
ON CONFLICT (id) DO UPDATE
   SET ..., updated_at = now()
 WHERE alert.status = 'NEW'
RETURNING (xmax = 0)
```

The guarantee is **PostgreSQL's, not Java's**. Three alternatives were rejected:

| Approach | Why not |
|---|---|
| `existsById()` then `save()` | Check-then-act race. Two consumer threads both pass the check; the loser then fails the constraint, turning an idempotent operation into an error. |
| `save()` | Issues its own SELECT then INSERT/UPDATE — same race, plus an UPDATE nobody asked for. |
| catch `DataIntegrityViolationException` | Works, but a constraint violation marks the JPA transaction rollback-only, so anything after it in the same transaction fails too. |

Three details of the statement are load-bearing:

- **`DO UPDATE`, not `DO NOTHING`** — a replay can carry a revised score or
  consecutive-window count, and the latest version of the fact is the useful one.
- **`WHERE alert.status = 'NEW'`** — an alert an operator already acknowledged is
  never rewritten by a replay. Without it, a Spark incident would push handled
  alerts back into the operator queue.
- **`RETURNING (xmax = 0)`** — `xmax` is zero on a row that was really inserted.
  That boolean is what lets the SSE stream fire once per *alert* rather than once
  per *delivery*; otherwise every duplicate would make the dashboard blink.

A test with eight concurrent threads asserts exactly one of them inserts.

## Transaction boundary

There is no distributed transaction and there cannot be one. The order is always
**database commit, then Kafka offset commit**:

```
write first, then commit the offset   → a crash replays the alert  → absorbed
commit the offset first, then write   → a crash loses it permanently
```

Idempotence is the price of the ordering that does not lose data.

## Error policy

| Class | Example | Treatment |
|---|---|---|
| **Transient** | PostgreSQL unreachable, timeout | Retry with exponential backoff, **never exhausted**. Offset not committed. **Never dead-lettered** — that would discard valid alerts during an incident (ADR-002). |
| **Data** | malformed JSON, schema violation, unsupported major version | Dead letter **immediately, zero retries**. Replaying an invalid message ten times will not make it valid. |
| **Duplicate** | same `alert_id` | **Logical success.** Offset committed, counter incremented, no retry. |

The asymmetry is the point: retrying forever on a database outage is correct;
retrying forever on a corrupt payload is a poison pill that blocks its partition.

Dead letters go to `telemetry.dlq` in the **same `DlqEnvelope` shape** the Python
components use (ADR-002) — the contract already carries `source_topic`, so one
queue serves both producers. A conformance test validates the Java envelope
against the same JSON Schema.

## Data model

Three tables (decision D-38): `machine`, `alert`, `alert_acknowledgement`.

`alert_id` **is** the primary key. It is a deterministic UUIDv5 of
`(machine_id, window_start, model_name, model_version)`, so it already is the
natural key in scalar form and already the identifier the API exposes; a
surrogate key would add a second identity no client ever sees. The natural key
also carries its own unique constraint as defence in depth — if the upstream
derivation ever changed, the same window would arrive under a new UUID and the
primary key would insert it happily.

An **unknown machine is provisioned from the alert** rather than rejected
(D-39). Discarding a real detection because a seed is stale is the wrong failure
mode.

Columns invented in the Phase 0 sketch — `criticality`, `commissioned_on`,
`nominal_ranges` — are deliberately absent: nothing produces them, and
`criticality` is described in the contract as feeding severity while the
stream-processor computes severity from score and threshold alone.

## What the API cannot show

The alert detail carries the window bounds, the score, and the ranked
contributors. It carries **no sensor time series**, and that is a property of the
data, not an omission:

- the raw readings live only in `telemetry.raw`, which has **7 days** of
  retention and is persisted nowhere;
- the 52-feature vector is **never parsed by the alerting query**, so it never
  reaches an alert — the contract declares `features` and it is always `{}`.

The `features` column exists so that carrying it later needs no migration. It is
documented as empty rather than presented as available.

## Endpoints

```
GET  /api/v1/alerts                     page, size (max 100), sort, machineCode,
                                        lineCode, severity, status, from, to
GET  /api/v1/alerts/{alertId}
POST /api/v1/alerts/{alertId}/acknowledge
GET  /api/v1/alerts/stats               from, to, granularity (hour|day)
GET  /api/v1/alerts/stream              SSE; severity, lineCode
```

Every filter, sort and page boundary is executed by PostgreSQL. `sort` is a
whitelist: a free-text property would reach the database unvalidated and, at
best, sort on an unindexed column.

Errors are RFC 9457 `ProblemDetail`, produced only by the exception handler, and
every one carries a `traceId` correlated with the logs.

## SSE, not WebSocket (D-08)

The need is strictly one-way: the server pushes, the client acts over REST. A
WebSocket would add an upstream channel nobody uses and its reconnection logic
would have to be written by hand, whereas `EventSource` reconnects on its own and
replays `Last-Event-ID`.

The event id is the **`event_seq`** column, not the alert id: a random UUID
cannot serve as a resumption cursor.

**The stream is not persistence.** A disconnected client loses nothing — the
alert is in PostgreSQL and reachable over REST. Replay on reconnect is bounded;
a client that has been away for a long time reloads the list instead.

Known limit, stated rather than discovered later: with more than one instance, a
client connected to A does not receive alerts consumed by B. **v1 is
single-instance.**

## Running it

```sh
docker compose --env-file .env -f infra/docker-compose.yml up -d alert-service
curl localhost:8081/api/v1/alerts | jq
curl -N localhost:8081/api/v1/alerts/stream
```

## Tests

```sh
cd alert-service && mvn verify
```

Testcontainers starts a real PostgreSQL, and only where it is indispensable:
`ON CONFLICT ... RETURNING (xmax = 0)`, the partial index, the JSONB columns and
the CHECK constraints are all PostgreSQL. Proving idempotence on H2 would prove
something about a database we do not ship.

> **On Windows with Docker Desktop**, Testcontainers needs the endpoint:
> `DOCKER_HOST='npipe:////./pipe/dockerDesktopLinuxEngine' mvn verify`.
> The Docker API version is pinned to 1.43 in the pom — Docker 29 refuses
> anything below 1.40 and the bundled client would otherwise negotiate 1.32.
