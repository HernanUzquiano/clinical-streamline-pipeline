# Onboarding a new data domain

This checklist takes a new source entity from a legacy table to a contract-governed curated topic that is served from ClickHouse. The worked example is **lab results** (`lab_results`: one row per resulted test).

Each step lists the artifact it produces. A domain is done when every gate at the end passes locally and in CI.

## 1. Map the legacy schema

- [ ] List the source columns, types, nullability, primary key, and update/delete behaviour. Decide: **append-only fact** or **mutable entity**?
- [ ] Map types through Debezium. In this stack: `timestamp` → epoch micros (`BIGINT` in Flink), `numeric` → `double` (`decimal.handling.mode=double`), `date` → epoch days (`INT`).
- [ ] Identify the tenant column. Every curated record must carry `tenant_id`.
- [ ] Mutable entity: `ALTER TABLE … REPLICA IDENTITY FULL`.

*Artifact:* a mapping table in the PR description.

## 2. Capture: publication, topic, connector

- [ ] `ALTER PUBLICATION ehr_cdc ADD TABLE lab_results;`
- [ ] Add `ehr.public.lab_results` to [scripts/create_topics.sh](../../scripts/create_topics.sh). A mutable entity gets `cleanup.policy=compact`. A fact gets `delete` with a retention period. Size partitions for peak volume (see [docs/topics.md](../topics.md)).
- [ ] Add the table to `table.include.list` in [connect/ehr-postgres.json](../../connect/ehr-postgres.json), then run `make connector`.
- [ ] Backfill existing rows with an incremental snapshot (a signal table insert). See [cdc-connector.md](cdc-connector.md).

## 3. Contract first

- [ ] Write `contracts/curated.lab_results.v1.json` before writing the job. Define the fields in order, `x-key` (tenant first), `x-semantics`, and `x-freshness-slo`.
- [ ] Review the contract with the consuming team. Agree on access patterns (which table and query shapes) and the SLO baseline.

## 4. Transform: Flink job

- [ ] Add `flink/jobs/lab_results_curated.sql` with a unique `pipeline.name` and its own consumer `group.id`.
- [ ] Mutable entity: use `format = 'debezium-json'` with a primary key. Append-only fact: read the envelope with `format = 'json'` and filter `op IN ('c','r')`, which keeps the job eligible for windowing.
- [ ] Sink: `upsert-kafka` with `PRIMARY KEY` equal to the contract's `x-key`.
- [ ] Bound all state: a TTL for joins and regular aggregations, or windowed operators.
- [ ] Add the job to [scripts/submit_jobs.sh](../../scripts/submit_jobs.sh).

## 5. Serve: ClickHouse

- [ ] Add to [clickhouse/init.sql](../../clickhouse/init.sql): a target `ReplacingMergeTree` with `ORDER BY (tenant_id, …key)`, a Kafka engine table whose columns match the contract exactly, and an MV.
- [ ] For an existing environment, apply the same DDL as a migration.

## 6. Gates

- [ ] `make test`. The contract tests pick up the new contract automatically: producer DDL and consumer table must both match it.
- [ ] Extend `tests/smoke` with a completeness check (source vs served) and a correctness check (an independent recomputation of any aggregate).
- [ ] `make smoke` and `make bench` pass locally, then in CI.
- [ ] Add a section to the Flink and ClickHouse runbooks if the domain has any special behaviour.

## Time-savers

- Copy the closest existing job: `encounters_curated.sql` for a changelog with a join, or `vitals_1m.sql` for an append-only fact with windows.
- Most failures on a first run are type mismatches at a boundary (micros vs millis, numeric encoding). The contract tests catch the schema mismatches. The smoke gate's recomputation catches the semantic ones.
