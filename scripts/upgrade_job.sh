#!/usr/bin/env bash
# Stateful redeploy of one Flink SQL job: stop-with-savepoint, then resubmit the (possibly changed)
# SQL from that savepoint. Usage: scripts/upgrade_job.sh <pipeline.name> <file in flink/jobs/>
#   e.g. scripts/upgrade_job.sh vitals-1m vitals_1m.sql
# The job image must be rebuilt first if the SQL changed: docker compose up -d --build jobmanager taskmanager
set -euo pipefail

cd "$(dirname "$0")/.."
name="$1" file="$2"

flink() { docker compose exec -T jobmanager ./bin/flink "$@" 2>/dev/null; }

job_id="$(flink list -r | awk -v n="$name" '$0 ~ ": "n" \\(RUNNING\\)" {print $4}')"
if [[ -z "$job_id" ]]; then
  echo "no RUNNING job named $name" >&2
  exit 1
fi

echo "stopping $name ($job_id) with savepoint"
savepoint="$(flink stop --savepointPath file:///flink-data/savepoints "$job_id" \
  | sed -n 's/.*Savepoint completed\. Path: \(.*\)$/\1/p')"
if [[ -z "$savepoint" ]]; then
  echo "savepoint failed; job $job_id left as is" >&2
  exit 1
fi
echo "savepoint: $savepoint"

echo "resubmitting $name from savepoint"
docker compose exec -T jobmanager bash -c "
  { echo \"SET 'execution.savepoint.path' = '$savepoint';\"; cat /opt/flink/jobs/$file; } > /tmp/$file
  ./bin/sql-client.sh -f /tmp/$file" 2>/dev/null | grep -E "Job ID|ERROR|Exception"

sleep 3
flink list -r
