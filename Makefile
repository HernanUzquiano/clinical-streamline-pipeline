.PHONY: help install up connector jobs upgrade pipeline simulate smoke bench test lint down clean

help:  ## Show targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

install:  ## Install Python package + dev tools into the current environment
	pip install -e '.[dev]'

up:  ## Start the local stack (Postgres, Kafka, Debezium, Flink, ClickHouse, API)
	docker compose up -d --build --wait

connector:  ## Register the Debezium CDC connector
	./scripts/register_connector.sh

jobs:  ## Submit Flink SQL jobs (skips jobs already running)
	./scripts/submit_jobs.sh

upgrade:  ## Stateful redeploy of one job, e.g. make upgrade JOB=vitals-1m SQL=vitals_1m.sql
	./scripts/upgrade_job.sh $(JOB) $(SQL)

pipeline: up connector jobs  ## Start everything end-to-end

simulate:  ## Run the hospital simulator for 5 minutes
	python -m generator.simulate --hospitals 3 --duration 300

smoke:  ## End-to-end smoke gate against the running stack
	pytest -m smoke -v tests/smoke

bench:  ## CDC freshness benchmark (source commit -> ClickHouse visible)
	python scripts/bench_freshness.py --samples 50

test:  ## Unit + contract tests (no stack needed)
	pytest -v

lint:  ## Lint + format check
	ruff check . && ruff format --check .

down:  ## Stop the stack, keep volumes
	docker compose down

clean:  ## Stop the stack and delete all data
	docker compose down -v
