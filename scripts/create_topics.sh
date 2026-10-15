#!/usr/bin/env bash
# Creates every topic the pipeline uses. Idempotent (--if-not-exists).
# Topic design is documented in docs/topics.md.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-localhost:29092}"
KT="${KAFKA_TOPICS:-/opt/kafka/bin/kafka-topics.sh}"

create() {
  local name="$1" partitions="$2"; shift 2
  local args=()
  for cfg in "$@"; do args+=(--config "$cfg"); done
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$name" --partitions "$partitions" --replication-factor 1 "${args[@]}"
}

# Raw CDC changelogs (keyed by primary key). Dimension-like tables are compacted so a
# replay from offset 0 always rebuilds the full current state.
create ehr.public.patients            3 cleanup.policy=compact
create ehr.public.encounters          3 cleanup.policy=compact
# Append-only fact stream: time-based retention, more partitions for throughput.
create ehr.public.vital_observations  6 cleanup.policy=delete retention.ms=604800000
create __debezium-heartbeat.ehr       1 cleanup.policy=delete retention.ms=86400000

# Curated, contract-governed topics (upsert semantics -> compacted).
create curated.encounters.v1          3 cleanup.policy=compact
create curated.vitals_1m.v1           6 cleanup.policy=compact

"$KT" --bootstrap-server "$BOOTSTRAP" --list
