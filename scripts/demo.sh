#!/bin/sh
# One command, from a clean clone to a dashboard with real data in it.
#
#   make demo
#
# Why this exists rather than `docker compose up` doing everything: the
# simulator and the Spark job sit behind compose profiles, decided in Phase 1
# and kept in Phase 6 (D-49). `up` leaves the stack passive, because GENERATING
# DATA IS A DELIBERATE ACT -- a plain `up` that silently starts fabricating
# telemetry and burning CPU on a Spark job is a surprise, not a convenience.
#
# So the profiles stay, and this script is the deliberate act. It is one
# command for the reviewer and an explicit one for everyone else.
#
# What it does, in order:
#
#   1. verifies the model artefact           (fails before starting anything)
#   2. builds and starts the passive stack   (Kafka, PostgreSQL, API, dashboard)
#   3. verifies topics, migrations, readiness
#   4. replays two hours of history          (bounded, so it ends)
#   5. starts the scoring job                (reads that history from earliest)
#   6. starts the live simulator             (so the stream keeps moving)
#   7. waits until alerts actually exist, then prints where to look
#
# Re-runnable. Step 4 adds two more hours of history; steps 2, 5 and 6 converge
# on an already-running container instead of duplicating it.
#
# One caveat worth knowing on a SECOND run: Spark resumes from its checkpoint,
# so it scores only what arrived after the offsets it already committed. That is
# the recovery guarantee working as designed, not a fault -- but it means a
# re-run does not rescore history it has already seen. For a genuinely clean
# demonstration, `make clean` first: it drops the volumes, checkpoint included.

set -eu

REPO_ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$REPO_ROOT"

ENV_FILE="${ENV_FILE:-.env}"
COMPOSE_FILE="infra/docker-compose.yml"

# Two simulated hours. Long enough for anomaly episodes to occur at the
# configured rate (0.4 per machine per hour over 15 machines) and for the
# telemetry charts to have a curve rather than a handful of points.
DEMO_REPLAY_SECONDS="${DEMO_REPLAY_SECONDS:-7200}"
ALERT_WAIT_TIMEOUT="${DEMO_ALERT_TIMEOUT:-420}"

say()   { printf '\n=== %s ===\n' "$1"; }
note()  { printf '    %s\n' "$1"; }

compose() {
  docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" "$@"
}

env_value() {
  _key="$1"; _default="${2:-}"
  _value=$(sed -n "s/^[[:space:]]*${_key}=//p" "$ENV_FILE" 2>/dev/null | head -n 1 | tr -d '\r')
  [ -z "$_value" ] && printf '%s' "$_default" || printf '%s' "$_value"
}

started_at=$(date +%s)

# ---------------------------------------------------------------------------
say "0/7  Configuration"

if [ ! -f "$ENV_FILE" ]; then
  cp .env.example "$ENV_FILE"
  note "$ENV_FILE cree depuis .env.example"
else
  note "$ENV_FILE present, laisse tel quel"
fi

if ! docker info >/dev/null 2>&1; then
  note "ERREUR : le demon Docker ne repond pas. Demarrez Docker Desktop."
  exit 1
fi
note "Docker $(docker version --format '{{.Server.Version}}' 2>/dev/null || echo '?')"

# ---------------------------------------------------------------------------
# The artefact first. It is the one failure no amount of waiting fixes, and
# discovering it after a ten-minute image build would waste the reviewer time.
say "1/7  Verification du modele ML"
./scripts/bootstrap.sh --model-only

# ---------------------------------------------------------------------------
say "2/7  Construction et demarrage de la pile passive"
note "Kafka, PostgreSQL, alert-service, dashboard."
note "La premiere execution construit quatre images et peut prendre 10 a 15 min."
compose up -d --build

# ---------------------------------------------------------------------------
say "3/7  Verification de la plateforme"
./scripts/bootstrap.sh

# ---------------------------------------------------------------------------
# A bounded replay: event time starts in the past and ends at the present
# instant, so the live simulator started in step 6 continues from where this
# stops without leaving a gap in the middle of the chart.
say "4/7  Rejeu de ${DEMO_REPLAY_SECONDS}s d'historique simule"
note "Mode replay borne : il se termine tout seul, contrairement au mode realtime."
replay_started=$(date +%s)
compose --profile sim run --rm --name anomaly-demo-replay \
  -e SIMULATOR_MODE=replay \
  -e SIMULATOR_DURATION_SECONDS="$DEMO_REPLAY_SECONDS" \
  -e SIMULATOR_SEED=424242 \
  event-simulator
replay_elapsed=$(( $(date +%s) - replay_started ))
note "Rejeu termine en ${replay_elapsed}s reels."

# ---------------------------------------------------------------------------
# Started AFTER the replay so its first micro-batches read a topic that already
# holds history. STREAM_STARTING_OFFSETS=earliest means it scores all of it.
say "5/7  Demarrage du job de scoring Spark"
note "Detection des retards DESACTIVEE pour cette execution, deliberement."
note "Le retard vaut 'ingest_time - event_time' : le temps qu'un producteur a"
note "garde un echantillon avant de le publier. En rejeu, le producteur publie"
note "maintenant des evenements vieux de deux heures, donc presque tout le"
note "backfill est declare en retard -- et le scoring ne lit QUE les messages a"
note "l'heure. Mesure d'un essai avec la detection active : 107 055 messages"
note "sur 108 359 routes vers telemetry.late, 0 fenetre scoree, 0 alerte."
note "Contrepartie assumee : pendant la demo, un evenement reellement en retard"
note "n'est pas signale. Le canal telemetry.late est verifie separement en"
note "Phase 3 (docs/10 section 5.5)."

# Exported for compose interpolation: the shell environment takes precedence
# over --env-file, so this overrides the .env value without editing the file.
STREAM_LATE_DETECTION_ENABLED=false \
  compose --profile stream up -d stream-processor

note ""
note "Spark verifie le SHA-256 du modele au demarrage et refuse de scorer si"
note "l'artefact ne correspond pas (ADR-001)."

# ---------------------------------------------------------------------------
say "6/7  Demarrage du simulateur temps reel"
compose --profile sim up -d event-simulator
note "Le flux continue : le dashboard recoit des alertes en direct via SSE."

# ---------------------------------------------------------------------------
say "7/7  Attente des premieres alertes"
note "Le pipeline complet doit se derouler : generation -> fenetrage Spark"
note "(watermark 90s) -> scoring -> Kafka -> persistance idempotente."

db=$(env_value POSTGRES_DB anomaly)
user=$(env_value POSTGRES_USER anomaly)
elapsed=0
alerts=0
while [ "$elapsed" -lt "$ALERT_WAIT_TIMEOUT" ]; do
  alerts=$(compose exec -T postgres psql -U "$user" -d "$db" -tAc \
    "SELECT count(*) FROM alert" 2>/dev/null | tr -d '\r ' || echo 0)
  case "$alerts" in
    ''|*[!0-9]*) alerts=0 ;;
  esac
  [ "$alerts" -gt 0 ] && break
  sleep 10
  elapsed=$((elapsed + 10))
  [ $((elapsed % 60)) -eq 0 ] && note "toujours en attente... ${elapsed}s"
done

windows=$(compose exec -T postgres psql -U "$user" -d "$db" -tAc \
  "SELECT count(*) FROM telemetry_window" 2>/dev/null | tr -d '\r ' || echo 0)

total_elapsed=$(( $(date +%s) - started_at ))

dashboard_port=$(env_value DASHBOARD_EXTERNAL_PORT 4200)
api_port=$(env_value ALERT_SERVICE_EXTERNAL_PORT 8081)

printf '\n'
if [ "${alerts:-0}" -gt 0 ]; then
  printf 'Plateforme prete en %ss.\n' "$total_elapsed"
  printf '  alertes persistees          : %s\n' "$alerts"
  printf '  fenetres de telemetrie      : %s\n' "$windows"
else
  printf 'Pile demarree en %ss, mais AUCUNE alerte apres %ss.\n' "$total_elapsed" "$ALERT_WAIT_TIMEOUT"
  printf '  fenetres de telemetrie      : %s\n' "$windows"
  printf '\n'
  printf 'Ce n est pas forcement une panne : le modele retenu signale environ 1 %%\n'
  printf 'des fenetres completes, donc une periode calme peut n en produire aucune.\n'
  printf 'Le dashboard fonctionne, il sera simplement vide. Pour diagnostiquer :\n'
  printf '  docker compose --env-file .env -f infra/docker-compose.yml logs stream-processor\n'
fi

printf '\n'
printf 'Dashboard        http://localhost:%s\n' "$dashboard_port"
printf 'API              http://localhost:%s/api/v1/alerts\n' "$api_port"
printf 'OpenAPI          http://localhost:%s/swagger-ui.html\n' "$api_port"
printf '\n'
printf 'Arreter          make down          (conserve les donnees)\n'
printf 'Tout effacer     make clean         (supprime les volumes)\n'
