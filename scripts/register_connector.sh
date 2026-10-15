#!/usr/bin/env bash
# Creates or updates the Debezium connector (PUT is idempotent) and waits until its task is RUNNING.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
NAME=ehr-postgres
CONFIG="$(dirname "$0")/../connect/ehr-postgres.json"

curl -sf -X PUT -H "Content-Type: application/json" --data @"$CONFIG" \
  "$CONNECT_URL/connectors/$NAME/config" > /dev/null

for _ in $(seq 1 30); do
  status="$(curl -sf "$CONNECT_URL/connectors/$NAME/status" || true)"
  if grep -q '"tasks":\[{[^]]*"state":"RUNNING"' <<< "$status"; then
    echo "connector $NAME: RUNNING"
    exit 0
  fi
  sleep 2
done

echo "connector $NAME did not reach RUNNING:" >&2
echo "$status" >&2
exit 1
