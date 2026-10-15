# Runbook: ClickHouse serving layer

**Path:** curated topic → `clinical.kafka_*` (Kafka engine) → `clinical.mv_*` (materialized view) → `clinical.encounters` and `clinical.vitals_1m` (`ReplacingMergeTree`).

## Health checks

```bash
docker compose exec -T clickhouse clickhouse-client --user clinical --password clinical -q "
  SELECT table, num_messages_read, last_poll_time, length(exceptions.text) AS errors
  FROM system.kafka_consumers WHERE database = 'clinical' FORMAT PrettyCompact"

docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group clickhouse-encounters
```

## Duplicates or stale rows in query results

`ReplacingMergeTree` collapses versions of a key during background merges, which happen eventually. Until then, a key can have several rows.

- Readers must use `FINAL`, or `argMax(col, version)`. The API does this.
- `encounters` keeps the row with the highest `updated_at`. `vitals_1m` keeps the latest `ingested_at`, so a window re-emitted after Flink recovery replaces the earlier copy.
- Force a merge (maintenance only, it is expensive): `OPTIMIZE TABLE clinical.vitals_1m FINAL`.

## Broken messages

The Kafka tables skip up to 1000 unparseable messages per block (`kafka_skip_broken_messages`). Tombstones from the upsert topics are empty values and are skipped by design. A rising error count in `system.kafka_consumers.exceptions` means a contract drift. Run `make test` and the smoke gate's contract check against the topic.

## Rebuilding a table from the topic

The curated topics are compacted, so they contain the latest value for every key.

```sql
DETACH TABLE clinical.mv_vitals_1m;                 -- pause ingestion
TRUNCATE TABLE clinical.vitals_1m;
```

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group clickhouse-vitals-1m --reset-offsets --to-earliest --topic curated.vitals_1m.v1 --execute
```

```sql
ATTACH TABLE clinical.mv_vitals_1m;                 -- resume, replays from offset 0
```

## Schema change on a curated topic

Follow [contracts/README.md](../../contracts/README.md). For an additive field: `ALTER TABLE` the target table, recreate the Kafka engine table and the MV with the new column, and update the contract. Keep all of it in one PR, because `tests/unit/test_contracts.py` enforces that they agree.
