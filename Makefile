# Task entry points for the platform.
#
# `make` is not installed on every Windows setup. Every recipe below is a plain
# one-liner that can be copied and run directly in a shell; the Makefile is a
# convenience, never the only way to run something. The README lists the raw
# equivalents.

SHELL := /bin/sh
COMPOSE := docker compose --env-file .env -f infra/docker-compose.yml
CORE_IMAGE := telemetry-core-dev
CORE_RUN := docker run --rm -v "$(CURDIR)":/workspace -w /workspace/libs/telemetry-core $(CORE_IMAGE)

.DEFAULT_GOAL := help
.PHONY: help env demo bootstrap check-env up up-tools down clean topics describe \
        logs ps psql build-core check lint format typecheck test test-contracts

help: ## Show the available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------

env: ## Create .env from the template if it does not exist
	@if [ -f .env ]; then echo ".env already exists, leaving it alone"; \
	else cp .env.example .env && echo ".env created from .env.example"; fi

# ---------------------------------------------------------------------------
# Demonstration
#
# `demo` is the single command the README gives a reviewer. It exists because
# the simulator and the Spark job stay behind compose profiles (D-49): plain
# `up` leaves the stack passive, since producing data is a deliberate act. This
# target IS that deliberate act, wrapped once instead of explained three times.
# ---------------------------------------------------------------------------

demo: ## Build, start everything and produce real data (the one command)
	./scripts/demo.sh

bootstrap: ## Verify the platform: artefact, topics, migrations, readiness
	./scripts/bootstrap.sh

check-env: ## Fail if .env.example and the compose file disagree
	python scripts/check_env_example.py

up: env ## Start Kafka and PostgreSQL, then create the topics
	$(COMPOSE) up -d

up-tools: env ## Start the stack plus the Kafka console (profile: tools)
	$(COMPOSE) --profile tools up -d

down: ## Stop the stack, keeping volumes
	$(COMPOSE) down

clean: ## Stop the stack AND DELETE its volumes (Kafka log, database)
	$(COMPOSE) down -v

topics: ## Re-run topic reconciliation (idempotent)
	$(COMPOSE) run --rm kafka-init

describe: ## Show the current topic topology
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server $${KAFKA_INTERNAL_BOOTSTRAP:-kafka:9092} --describe

ps: ## Show container status
	$(COMPOSE) ps -a

logs: ## Follow the logs of every service
	$(COMPOSE) logs -f

psql: ## Open a psql shell on the platform database
	$(COMPOSE) exec postgres psql -U $${POSTGRES_USER:-anomaly} -d $${POSTGRES_DB:-anomaly}

# ---------------------------------------------------------------------------
# telemetry-core
#
# The toolchain runs in a container: Python 3.11 is what PySpark 3.5 supports
# and what CI uses, so a check that passes locally passes there too, whatever
# interpreter happens to be installed on the developer machine.
# ---------------------------------------------------------------------------

build-core: ## Build the telemetry-core toolchain image
	docker build -f libs/telemetry-core/Dockerfile -t $(CORE_IMAGE) .

check: build-core ## Run every gate: lint, format, types, tests
	$(CORE_RUN)

lint: ## ruff check
	$(CORE_RUN) sh -c "ruff check ."

format: ## ruff format (writes)
	$(CORE_RUN) sh -c "ruff format ."

typecheck: ## mypy, strict
	$(CORE_RUN) sh -c "mypy"

test: ## pytest
	$(CORE_RUN) sh -c "pytest -q"

test-contracts: ## Only the two tests guarding silent-failure invariants
	$(CORE_RUN) sh -c "pytest -q tests/test_schema_conformance.py tests/test_ids.py"
