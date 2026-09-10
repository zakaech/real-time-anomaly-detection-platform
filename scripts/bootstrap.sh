#!/bin/sh
# Verifies that the platform has everything it needs to run. It CREATES NOTHING.
#
# That restraint is the whole design. Two systems already own provisioning and
# both are convergent:
#
#   * Kafka topics  -> the kafka-init service (infra/kafka/create-topics.sh)
#   * the schema    -> Flyway, inside alert-service
#
# A bootstrap that also created topics or ran SQL would be a second source of
# truth for each, and a second source of truth diverges. So this script only
# asks questions, and the one thing it adds is the check neither of those two
# can make: that the model artefact on disk is the one the pipeline expects.
#
# Idempotent by construction: it has no side effects at all. Run it as often as
# you like, before or after `make demo`.
#
#   ./scripts/bootstrap.sh              # verify everything
#   ./scripts/bootstrap.sh --model-only # only the artefact, no Docker needed
#
# Exit codes: 0 every check passed, 1 at least one failed.

set -eu

# Git Bash rewrites any argument that looks like a Unix absolute path into a
# Windows one before handing it to a native .exe. So
#   docker compose exec kafka /opt/kafka/bin/kafka-topics.sh
# reaches docker.exe as C:/Program Files/Git/opt/kafka/bin/kafka-topics.sh and
# the command silently finds nothing -- which this script would then report as
# "broker unreachable", blaming the platform for a quoting problem on the host.
# Both variables are inert on Linux and in CI.
MSYS_NO_PATHCONV=1
MSYS2_ARG_CONV_EXCL='*'
export MSYS_NO_PATHCONV MSYS2_ARG_CONV_EXCL

REPO_ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$REPO_ROOT"

ENV_FILE="${ENV_FILE:-.env}"
COMPOSE_FILE="infra/docker-compose.yml"
WAIT_TIMEOUT="${BOOTSTRAP_WAIT_TIMEOUT:-180}"
MODEL_ONLY=0

[ "${1:-}" = "--model-only" ] && MODEL_ONLY=1

FAILURES=0

# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------
step() { printf '\n== %s ==\n' "$1"; }
ok()   { printf '  OK    %s\n' "$1"; }
fail() { printf '  FAIL  %s\n' "$1"; FAILURES=$((FAILURES + 1)); }
info() { printf '        %s\n' "$1"; }

# ---------------------------------------------------------------------------
# Reading .env
#
# The file is parsed, never sourced. `. .env` would EXECUTE it, and one line in
# the template is enough to show why that is not pedantry:
#
#   KAFKA_HEAP_OPTS=-Xmx768m -Xms768m
#
# A shell reads that as: assign -Xmx768m, then run the command -Xms768m.
# ---------------------------------------------------------------------------
env_value() {
  _key="$1"
  _default="${2:-}"
  _value=""
  if [ -f "$ENV_FILE" ]; then
    _value=$(sed -n "s/^[[:space:]]*${_key}=//p" "$ENV_FILE" | head -n 1 | tr -d '\r')
  fi
  if [ -z "$_value" ]; then
    printf '%s' "$_default"
  else
    printf '%s' "$_value"
  fi
}

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | cut -d' ' -f1
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | cut -d' ' -f1
  elif command -v python3 >/dev/null 2>&1; then
    python3 -c "import hashlib,sys;print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())" "$1"
  elif command -v python >/dev/null 2>&1; then
    python -c "import hashlib,sys;print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())" "$1"
  else
    echo "NO_SHA256_TOOL"
  fi
}

compose() {
  docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" "$@"
}

# ===========================================================================
# 1. The model artefact
#
# Checked first, and checked without Docker: it is the one failure that no
# amount of waiting fixes, and finding it after a four-minute stack start would
# waste the reviewer time.
# ===========================================================================
check_model() {
  step "Modele ML"

  artifact_dir=$(env_value STREAM_ARTIFACT_DIR /models/one_class_svm/2.0.0)
  # The container path maps to a host path through the read-only bind mount
  # declared in compose: ../ml-training/artifacts -> /models
  host_dir="ml-training/artifacts${artifact_dir#/models}"

  if [ ! -d "$host_dir" ]; then
    fail "repertoire artefact absent : $host_dir"
    info "STREAM_ARTIFACT_DIR=$artifact_dir designe un modele inexistant."
    info "Modeles disponibles :"
    find ml-training/artifacts -mindepth 1 -maxdepth 1 -type d 2>/dev/null \
      | sed 's|.*/|          |' || info "          aucun"
    return
  fi

  # The five files the streaming job opens. Listed explicitly rather than
  # globbed: a missing calibration file is a different failure from a missing
  # model, and the message should say which.
  missing=0
  for file in model.joblib metadata.json calibration.json machine_profiles.json reference_profile.json; do
    if [ -f "$host_dir/$file" ]; then
      ok "$file present"
    else
      fail "$file ABSENT dans $host_dir"
      missing=$((missing + 1))
    fi
  done

  if [ "$missing" -gt 0 ]; then
    info ""
    info "Le job Spark refusera de demarrer sans ces fichiers, et c est voulu"
    info "(ADR-001) : scorer avec un artefact incomplet produit des resultats"
    info "plausibles et faux."
    info ""
    info "model.joblib est versionne dans le depot (D-48, ADR-009). S il manque"
    info "apres un clone, restaurez-le :"
    info "    git checkout -- ml-training/artifacts"
    return
  fi

  # The digest. metadata.json is the record of what was trained; the file beside
  # it must be that exact byte sequence. The streaming job repeats this check at
  # startup (ADR-001) -- doing it here only means the reviewer learns about a
  # corrupt artefact now rather than in a Spark stack trace.
  expected=$(sed -n 's/.*"artifact_sha256"[[:space:]]*:[[:space:]]*"\([0-9a-f]*\)".*/\1/p' "$host_dir/metadata.json" | head -n 1)
  actual=$(sha256_of "$host_dir/model.joblib")

  if [ "$actual" = "NO_SHA256_TOOL" ]; then
    fail "aucun outil SHA-256 disponible (sha256sum, shasum ou python)"
    return
  fi
  if [ -z "$expected" ]; then
    fail "artifact_sha256 introuvable dans metadata.json"
    return
  fi

  if [ "$expected" = "$actual" ]; then
    ok "SHA-256 conforme a metadata.json"
    info "$actual"
  else
    fail "SHA-256 NON CONFORME"
    info "attendu (metadata.json) : $expected"
    info "calcule (model.joblib)  : $actual"
    info ""
    info "Le fichier a change depuis l entrainement. Sur Windows la cause la"
    info "plus frequente est une conversion CRLF : .gitattributes declare"
    info "*.joblib binary pour l empecher. Restaurez-le :"
    info "    git checkout -- ml-training/artifacts/one_class_svm/2.0.0/model.joblib"
    return
  fi

  name=$(sed -n 's/.*"model_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$host_dir/metadata.json" | head -n 1)
  version=$(sed -n 's/.*"model_version"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$host_dir/metadata.json" | head -n 1)
  sklearn=$(sed -n 's/.*"sklearn_version"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$host_dir/metadata.json" | head -n 1)
  trained=$(sed -n 's/.*"trained_at"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$host_dir/metadata.json" | head -n 1)
  info "modele   : ${name} ${version}"
  info "entraine : ${trained}   scikit-learn ${sklearn}"
}

# ===========================================================================
# 2. Kafka
# ===========================================================================
wait_for_healthy() {
  _service="$1"
  _elapsed=0
  while [ "$_elapsed" -lt "$WAIT_TIMEOUT" ]; do
    _id=$(compose ps -q "$_service" 2>/dev/null | head -n 1)
    if [ -n "$_id" ]; then
      _state=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$_id" 2>/dev/null || echo unknown)
      case "$_state" in
        healthy) return 0 ;;
        running) return 0 ;;
        exited) return 1 ;;
        dead) return 1 ;;
      esac
    fi
    sleep 3
    _elapsed=$((_elapsed + 3))
    if [ $((_elapsed % 30)) -eq 0 ]; then
      info "attente de ${_service}... ${_elapsed}s"
    fi
  done
  return 1
}

check_kafka() {
  step "Kafka"

  if ! wait_for_healthy kafka; then
    fail "broker Kafka non healthy apres ${WAIT_TIMEOUT}s"
    info "Demarrez la pile :  make up      (ou  make demo)"
    return
  fi
  ok "broker healthy"

  bootstrap=$(env_value KAFKA_INTERNAL_BOOTSTRAP kafka:9092)
  if compose exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
       --bootstrap-server "$bootstrap" >/dev/null 2>&1; then
    ok "API broker joignable sur $bootstrap"
  else
    fail "broker injoignable sur $bootstrap"
  fi
}

# ---------------------------------------------------------------------------
# Topics. The partition counts are not decoration: 6 for the hot path so any
# consumer group up to 6 balances, 3 for alerts to match the Spring listener
# concurrency, 1 for the two topics a human reads end to end. Kafka can never
# REDUCE a partition count, so a wrong value here is permanent -- which is
# exactly why it is asserted rather than assumed.
# ---------------------------------------------------------------------------
check_topics() {
  step "Topics Kafka"

  bootstrap=$(env_value KAFKA_INTERNAL_BOOTSTRAP kafka:9092)

  # A plain `for` over "variable:partitions:default" entries, deliberately not a
  # `while read` over a pipe. Two reasons, both learned by running this:
  #
  #   * the loop body calls `docker compose exec`, which READS STDIN. Inside a
  #     `while read` fed by a pipe, the first call swallows the remaining lines
  #     and the loop checks one topic instead of six -- while reporting success.
  #   * a piped loop runs in a subshell, so every FAILURES increment inside it
  #     is discarded when that subshell exits.
  #
  # A `for` has neither problem, and `</dev/null` on the exec makes the first
  # one impossible to reintroduce.
  for entry in \
    "KAFKA_TOPIC_TELEMETRY_RAW:6:telemetry.raw" \
    "KAFKA_TOPIC_TELEMETRY_SCORED:6:telemetry.scored" \
    "KAFKA_TOPIC_ALERTS:3:alerts" \
    "KAFKA_TOPIC_TELEMETRY_LABELS:3:telemetry.labels" \
    "KAFKA_TOPIC_TELEMETRY_DLQ:1:telemetry.dlq" \
    "KAFKA_TOPIC_TELEMETRY_LATE:1:telemetry.late"
  do
    var=${entry%%:*}
    rest=${entry#*:}
    expected=${rest%%:*}
    default=${rest#*:}
    topic=$(env_value "$var" "$default")

    described=$(compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server "$bootstrap" --describe --topic "$topic" \
      </dev/null 2>/dev/null | tr -d '\r' || true)

    if [ -z "$described" ]; then
      fail "topic $topic ABSENT"
      info "kafka-init aurait du le creer. Relancez :  make topics"
      continue
    fi

    partitions=$(printf '%s' "$described" \
      | sed -n 's/.*PartitionCount:[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -n 1)
    if [ "$partitions" = "$expected" ]; then
      ok "$topic  partitions=$partitions"
    else
      fail "$topic  partitions=${partitions:-inconnu} (attendu $expected)"
      info "Kafka ne peut pas REDUIRE un nombre de partitions."
      info "Si la valeur est trop haute :  make clean  puis  make up"
    fi
  done
}

# ===========================================================================
# 3. PostgreSQL, through alert-service readiness
#
# The readiness probe is the check, not pg_isready. A reachable database proves
# nothing about the schema: readiness stays DOWN while Flyway migrates, so UP
# here means the migrations ran AND Hibernate validated every entity against
# them (ddl-auto: validate). That is a stronger statement than any query this
# script could make on its own.
# ===========================================================================
check_alert_service() {
  step "alert-service et PostgreSQL"

  if ! wait_for_healthy postgres; then
    fail "PostgreSQL non healthy apres ${WAIT_TIMEOUT}s"
    return
  fi
  ok "PostgreSQL healthy"

  elapsed=0
  ready=0
  while [ "$elapsed" -lt "$WAIT_TIMEOUT" ]; do
    if compose exec -T alert-service wget -q -O - \
         http://127.0.0.1:8080/actuator/health/readiness 2>/dev/null | grep -q UP; then
      ready=1
      break
    fi
    sleep 5
    elapsed=$((elapsed + 5))
    if [ $((elapsed % 30)) -eq 0 ]; then
      info "attente readiness alert-service... ${elapsed}s"
    fi
  done

  if [ "$ready" -eq 1 ]; then
    ok "readiness UP (Flyway a migre, JPA a valide le schema)"
  else
    fail "alert-service non ready apres ${WAIT_TIMEOUT}s"
    info "Journal :  docker compose --env-file .env -f infra/docker-compose.yml logs alert-service"
  fi
}

# ---------------------------------------------------------------------------
# Flyway. Read only: a SELECT on its own history table.
#
# This script never runs SQL that changes anything. Flyway is the single source
# of truth for the schema (docs/05 section 3) and a second migration path would
# guarantee divergence between what is migrated and what is created.
# ---------------------------------------------------------------------------
check_flyway() {
  step "Migrations Flyway"

  db=$(env_value POSTGRES_DB anomaly)
  user=$(env_value POSTGRES_USER anomaly)

  # `success::text`, not bare `success`. The `t` / `f` shown by psql is its
  # DISPLAY form for a boolean column; concatenated into a string, PostgreSQL
  # renders the same value as `true` / `false`. Comparing against `t` therefore
  # reports every applied migration as failed -- which is exactly what this
  # script did on its first real run, against a database that was perfectly fine.
  history=$(compose exec -T postgres psql -U "$user" -d "$db" -tAc \
    "SELECT version || '|' || success::text FROM flyway_schema_history WHERE version IS NOT NULL ORDER BY installed_rank" \
    </dev/null 2>/dev/null | tr -d '\r' || true)

  if [ -z "$history" ]; then
    fail "flyway_schema_history vide ou absente"
    info "alert-service n a pas migre. Consultez ses journaux."
    return
  fi

  for expected_version in 1 2 3; do
    line=$(printf '%s\n' "$history" | grep "^${expected_version}|" || true)
    status=${line#*|}
    if [ -z "$line" ]; then
      fail "migration V${expected_version} non appliquee"
    elif [ "$status" = "true" ] || [ "$status" = "t" ]; then
      ok "V${expected_version} appliquee"
    else
      fail "V${expected_version} appliquee mais EN ECHEC (success=false)"
      info "Une migration en echec bloque les suivantes. Base a recreer :"
      info "    make clean && make up"
    fi
  done

  # A schema that migrated but holds no machine would make every page empty for
  # a reason nobody could guess. V2 seeds the fleet; this proves it landed.
  machines=$(compose exec -T postgres psql -U "$user" -d "$db" -tAc \
    "SELECT count(*) FROM machine" 2>/dev/null | tr -d '\r ' || echo 0)
  if [ "${machines:-0}" -gt 0 ] 2>/dev/null; then
    ok "referentiel machines : ${machines} lignes"
  else
    fail "table machine vide (V2 devait inserer la flotte)"
  fi
}

# ===========================================================================
main() {
  printf 'Verification de la plateforme\n'
  printf 'racine : %s\n' "$REPO_ROOT"

  if [ ! -f "$ENV_FILE" ]; then
    printf '\n  FAIL  %s introuvable\n' "$ENV_FILE"
    printf '        Creez-le :  make env      (ou  cp .env.example .env)\n'
    exit 1
  fi

  check_model

  if [ "$MODEL_ONLY" -eq 1 ]; then
    printf '\n'
    if [ "$FAILURES" -eq 0 ]; then
      printf 'Artefact ML verifie.\n'
      exit 0
    fi
    printf '%s verification(s) en echec.\n' "$FAILURES"
    exit 1
  fi

  if ! docker info >/dev/null 2>&1; then
    step "Docker"
    fail "le demon Docker ne repond pas"
    info "Demarrez Docker Desktop, puis relancez."
    exit 1
  fi

  check_kafka
  check_topics
  check_alert_service
  check_flyway

  printf '\n'
  if [ "$FAILURES" -eq 0 ]; then
    printf 'Toutes les verifications sont passees.\n'
    exit 0
  fi
  printf '%s verification(s) en echec.\n' "$FAILURES"
  exit 1
}

main "$@"
