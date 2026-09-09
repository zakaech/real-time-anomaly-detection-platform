#!/bin/sh
# Rewrites the runtime configuration before nginx starts.
#
# Installed into /docker-entrypoint.d/, which the nginx image runs on startup
# before exec-ing nginx itself -- so this script must NOT exec anything.
#
# The bundle is built once and never carries a URL: a value baked in at build
# time would mean one image per environment. Everything below is a shape or a
# path, never a secret -- the API base stays relative because the dashboard is
# served behind the same origin as the backend.
set -eu

CONFIG_FILE=/usr/share/nginx/html/assets/config.json

cat > "${CONFIG_FILE}" <<JSON
{
  "apiBaseUrl": "${DASHBOARD_API_BASE_URL:-/api/v1}",
  "liveFeedLimit": ${DASHBOARD_LIVE_FEED_LIMIT:-100},
  "sseInitialBackoffMs": ${DASHBOARD_SSE_INITIAL_BACKOFF_MS:-1000},
  "sseMaxBackoffMs": ${DASHBOARD_SSE_MAX_BACKOFF_MS:-30000}
}
JSON

echo "[entrypoint] runtime config written to ${CONFIG_FILE}"
cat "${CONFIG_FILE}"
