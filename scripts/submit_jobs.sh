#!/usr/bin/env bash
# Submits each Flink SQL job unless a job with the same pipeline.name is already running.
set -euo pipefail

cd "$(dirname "$0")/.."

running="$(docker compose exec -T jobmanager ./bin/flink list -r 2>/dev/null || true)"

submit() {
  local name="$1" file="$2"
  if grep -q ": $name (RUNNING)" <<< "$running"; then
    echo "job $name already running, skipping"
    return
  fi
  echo "submitting $name"
  docker compose exec -T jobmanager ./bin/sql-client.sh -f "/opt/flink/jobs/$file" \
    | grep -E "Job ID|ERROR|Exception" || true
}

submit encounters-curated encounters_curated.sql
submit vitals-1m vitals_1m.sql

sleep 5
docker compose exec -T jobmanager ./bin/flink list -r
