# dashboard

Angular operator dashboard. Three pages: a live alert feed, a filterable
history, and an alert detail with the machine's sensor curve and an
acknowledgement form.

Consumes only what `alert-service` exposes. Nothing on screen is generated,
interpolated or estimated by the browser.

## Angular 21, not 22

The Angular 22 CLI requires Node `^22.22.3`; this project was built on Node
**v22.14.0**, which it refuses. Angular **21.2.23** requires `^22.12.0` and
builds cleanly, so it is the newest version the toolchain actually supports
(decision D-47).

A consequence worth knowing: Angular 21 scaffolds **Vitest + jsdom**, not
Karma/Jasmine. The provided toolchain is used as-is.

## What it draws, and what it cannot

The sensor curve exists because the backend was extended to persist scored
windows (D-41). Before that, raw telemetry was never stored and the alert's
`features` were always empty — any curve would have been invented.

What the chart shows is the **per-window mean** the pipeline computed, at the
window resolution the data itself declares. It is not a raw sensor trace, and
the caption says so.

Two rules the chart obeys:

- **a null reading breaks the line** (`spanGaps: false`). A failed sensor leaves
  a visible hole rather than a straight segment between its neighbours. Drawing
  through the gap would invent a measurement.
- **anomalies are marked, not inferred.** The red markers are windows the model
  flagged, read from `anomaly` on the row — the chart never re-derives them from
  the score.

## Deduplication

Alerts are keyed by `alertId` in a `Map` and always upserted, never appended.
That one choice covers three different causes of a repeat:

| Cause | Why it happens |
|---|---|
| The same SSE event twice | at-least-once delivery; Phase 3 measured 8 duplicate deliveries on a real crash |
| A reconnection replay | the backend replays up to 500 events through `Last-Event-ID` |
| REST and SSE overlapping | the initial load and the live channel describe the same alerts |

Counters are `computed` from the map, never incremented on arrival — a
hand-maintained counter drifts on the first duplicate and never recovers.

**The store is a projection of PostgreSQL, not a second source of truth.** Every
reconnection triggers a REST reload, and a reload *replaces* rather than merges,
so the dashboard cannot keep showing an alert the database no longer returns.

## Last-Event-ID: the real constraint

`EventSource` **cannot set request headers**. `Last-Event-ID` is sent by the
browser, automatically, and **only on an automatic reconnection**, from the last
`id:` line received. An application cannot supply it on a first connection.

That is not a problem here, and the reason is architectural: the stream is not
the source of truth. The sequence is *load over REST → open the stream → reload
over REST on every reconnection*. The browser closes the short gap; the REST
resynchronisation closes the long one, including an absence longer than the
backend's replay bound (D-46).

Native reconnection is **disabled** in favour of an exponential backoff
(1 s → 30 s): the browser retries on a fixed interval and would hammer a backend
that is down.

## No CORS, a reverse proxy

`OPTIONS /api/v1/alerts` from a browser origin returns **403** — the backend has
no CORS configuration, deliberately. nginx serves the SPA and proxies `/api`, so
the browser sees a single origin: the problem is removed rather than worked
around, and the backend is unchanged (D-42).

> The SSE location needs `proxy_buffering off`, `proxy_cache off`, a long
> `proxy_read_timeout` and `Connection ''`. Without them nginx holds the stream
> and the dashboard shows "connected" while receiving nothing — a silent
> failure.

The dev server does the same job through `proxy.conf.json`, so development and
production have the same shape and the code has no environment branch.

## Configuration

No URL is compiled in. `assets/config.json` is read **before** bootstrap and is
rewritten at container start from environment variables, so one image runs
anywhere:

| Variable | Default |
|---|---|
| `DASHBOARD_API_BASE_URL` | `/api/v1` (relative — same origin) |
| `DASHBOARD_LIVE_FEED_LIMIT` | `100` |
| `DASHBOARD_SSE_INITIAL_BACKOFF_MS` | `1000` |
| `DASHBOARD_SSE_MAX_BACKOFF_MS` | `30000` |

## Running it

```sh
# whole stack
docker compose --env-file .env -f infra/docker-compose.yml up -d dashboard
# then open http://localhost:4200

# development against a running backend
cd dashboard && npm start        # proxies /api to localhost:8081
```

## Tests

```sh
npm run test:ci        # Vitest + jsdom
npm run build:prod
```

`--legacy-peer-deps` is required on install: npm 10.9 crashes resolving the
vitest peer graph with `Cannot read properties of null (reading 'edgesOut')`.
The lock file was produced the same way, so `npm ci` stays reproducible.
