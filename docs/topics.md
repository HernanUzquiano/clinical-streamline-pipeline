# Topic design

All topics are created by [scripts/create_topics.sh](../scripts/create_topics.sh). The broker runs with `auto.create.topics.enable=false`, so a typo in a job or connector fails loudly instead of quietly creating an empty topic.

| Topic | Partitions | Cleanup | Key | Producer | Consumers |
|---|---|---|---|---|---|
| `ehr.public.patients` | 3 | compact | `{patient_id}` | Debezium | encounters-curated |
| `ehr.public.encounters` | 3 | compact | `{encounter_id}` | Debezium | encounters-curated |
| `ehr.public.vital_observations` | 6 | delete, 7 d | `{observation_id}` | Debezium | vitals-1m |
| `__debezium-heartbeat.ehr` | 1 | delete, 1 d | n/a | Debezium | none |
| `curated.encounters.v1` | 3 | compact | `{tenant_id, encounter_id}` | encounters-curated | ClickHouse |
| `curated.vitals_1m.v1` | 6 | compact | `{tenant_id, encounter_id, window_start}` | vitals-1m | ClickHouse |

## Rationale

**Compaction on changelog topics.** For `patients` and `encounters`, only the latest image of each row matters. Compaction keeps the topic bounded while still letting a new consumer, or a Flink job started without state, rebuild the full current state by reading from offset 0. Debezium tombstones are disabled (`tombstones.on.delete=false`) because rows are never hard-deleted in the source.

**Retention on the fact stream.** `vital_observations` is append-only and high-volume. Keeping the full history in Kafka would duplicate the source of record. Seven days of retention covers any realistic replay window for the downstream job.

**Partition counts.** The fact stream has six partitions, twice the number for the dimension changelogs, because it carries roughly two orders of magnitude more records. Partition counts are an upper bound on consumer parallelism (Flink source subtasks and ClickHouse consumers), so they are sized for the expected peak load rather than for this demo's volume.

**Keys and multi-tenant skew.** Curated keys begin with `tenant_id` but include the entity id, so one large hospital is still spread across every partition and does not create a hot partition. A key of `tenant_id` alone would send all of that tenant's records to one partition.

**Versioned curated topics.** The `.v1` suffix is part of the contract. A breaking change ships as `.v2`, and both versions run in parallel until every consumer has migrated (see [contracts/README.md](../contracts/README.md)).
