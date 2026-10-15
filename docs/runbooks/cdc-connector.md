# Runbook: Debezium CDC connector (`ehr-postgres`)

**Path:** Postgres WAL → logical replication slot `ehr_cdc_slot` (publication `ehr_cdc`) → Kafka Connect → `ehr.public.*` topics.

## Health checks

```bash
# Connector and task state (both must be RUNNING)
curl -s localhost:8083/connectors/ehr-postgres/status

# Replication slot: active, and how much WAL Postgres is holding for it
docker compose exec -T postgres psql -U ehr -c \
  "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
   FROM pg_replication_slots;"
```

A healthy slot is `active = t` with lag in kB. The heartbeat (every 10 s) keeps the lag flat even when the captured tables are quiet.

## Task FAILED

1. Read the trace: `curl -s localhost:8083/connectors/ehr-postgres/status | jq -r '.tasks[0].trace' | head -40`.
2. For a transient cause (database restart, network), restart the task. Offsets are stored in Kafka, so it resumes where it left off:
   `curl -X POST 'localhost:8083/connectors/ehr-postgres/restart?includeTasks=true&onlyFailed=true'`
3. For a configuration cause, fix `connect/ehr-postgres.json`, then run `make connector`. The PUT is idempotent.

## Replication slot lag growing

The slot keeps WAL on the source disk until Debezium confirms it. Unbounded growth will eventually fill the database disk, so this is the most important CDC alert.

- **Slot inactive** (`active = f`): the connector is down. Fix the connector first (see above).
- **Slot active but lag still growing**: Debezium cannot keep up, or Kafka is rejecting writes. Check the Connect logs (`docker compose logs connect --since 10m`) and broker health.
- **Last resort, when the source disk is at risk:** drop the slot (`SELECT pg_drop_replication_slot('ehr_cdc_slot');`), which **loses CDC continuity**. Then follow *Re-snapshot* below. Record it as an incident.

## Re-snapshot (rebuild topics from the source)

Use this after a slot was dropped, or after a table was added to the publication with historical rows.

1. Delete the connector: `curl -X DELETE localhost:8083/connectors/ehr-postgres`.
2. Reset its offsets: stop Connect, delete the connector's key from `_connect_offsets`, or create the connector under a new name with a new `slot.name`.
3. Run `make connector`. `snapshot.mode=initial` re-reads every table as `op = r` events.
4. Downstream impact: the changelog topics are compacted and consumers upsert, so re-emitted rows are idempotent. `vitals-1m` will recompute any windows whose readings are re-snapshotted. This is also idempotent, because the curated topic is keyed by window.

## Adding a table to capture

Add the table to the publication (`ALTER PUBLICATION ehr_cdc ADD TABLE …`), add its topic to `scripts/create_topics.sh`, and add it to `table.include.list`. Then run `make connector`. To backfill existing rows, use an incremental snapshot signal. See the [onboarding runbook](onboarding-a-domain.md).
