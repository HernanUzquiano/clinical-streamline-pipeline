# Runbook: Flink jobs (`encounters-curated`, `vitals-1m`)

| Job | Source | Sink | State |
|---|---|---|---|
| `encounters-curated` | `ehr.public.encounters` + `ehr.public.patients` (debezium-json changelogs) | `curated.encounters.v1` (upsert) | Both sides of the join. TTL 30 d. |
| `vitals-1m` | `ehr.public.vital_observations` (Debezium envelope, read append-only) | `curated.vitals_1m.v1` (upsert) | Open windows only. State is released when each window fires. |

Checkpointing: every 10 s, exactly-once, RocksDB incremental, retained on cancel, stored under `file:///flink-data/checkpoints`. Restart strategy: exponential delay.

## Health checks

```bash
docker compose exec -T jobmanager ./bin/flink list -r              # both jobs RUNNING
open http://localhost:8081                                           # UI: checkpoints, backpressure, watermarks
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group flink-vitals-1m   # source lag
```

The Kafka offsets for the `flink-*` groups are committed on checkpoint. They are for monitoring only: on recovery, Flink restores offsets from its own checkpoint.

## TaskManager lost or job restarting

The restart strategy recovers automatically from the latest completed checkpoint. To confirm a restore:

```bash
curl -s localhost:8081/jobs/<job-id>/checkpoints | jq '.latest.restored'
```

**Verified behaviour:** the smoke gate was run while the TaskManager was killed and restarted mid-stream. Both jobs restored from their checkpoints, and every window still matched the source exactly. Records written between the last checkpoint and the failure are re-emitted, and upsert keys make that idempotent downstream.

When restarts loop: check the exceptions tab (`/jobs/<id>/exceptions`). Typical causes are a deserialization failure, which `json.ignore-parse-errors` skips for the vitals source, or a missing topic, which shows as `UnknownTopicOrPartition`.

## Stateful upgrade (changed SQL, same state)

```bash
docker compose up -d --build jobmanager taskmanager   # only if flink/jobs/*.sql changed
./scripts/upgrade_job.sh vitals-1m vitals_1m.sql
```

The script stops the job with a savepoint and resubmits the SQL from that savepoint. Flink SQL maps state to operators by plan, so check whether the change is **state-compatible**:

- Compatible: changing projections or expressions after the stateful operator, such as score thresholds or the risk-level mapping.
- **Not** compatible: changing the GROUP BY or join keys, the window size, or the set of aggregates. Deploy these as a new job that writes to a new topic version (`.v2`) from `earliest-offset`, then cut consumers over.

If restore fails with a state-mapping error, resubmit without a savepoint (`./scripts/submit_jobs.sh`). The job rebuilds state from the compacted changelog topics. For `vitals-1m`, windows older than the topic retention (7 d) cannot be rebuilt, but they already exist downstream.

## Watermark not advancing (vitals windows not emitted)

Symptom: `curated.vitals_1m.v1` stops receiving records while the source topic is still growing.

- The watermark is the minimum across all source partitions. A partition with no data holds it back until `table.exec.source.idle-timeout` (30 s) marks it idle.
- Readings with far-future `observed_at` (clock skew at a source device) push one partition's watermark ahead. That is harmless, but readings more than 5 s late against it are dropped. Check the UI's *Watermarks* tab per subtask.
- When the source stops entirely, the last window stays open until new data arrives. This is expected behaviour, not data loss.

## Join state growth (`encounters-curated`)

State is roughly (number of patients + number of encounters) × row size. The 30-day TTL bounds it. Trade-off: if an encounter idle for more than 30 days is updated, the job re-emits it with null `sex` and `age_band`. The contract documents both fields as nullable. If that becomes a real case, enrich from a lookup table instead of a regular join.
