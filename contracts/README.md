# Data contracts

Each curated topic has exactly one contract file, named after the topic. The contract is the boundary between this pipeline and the teams that consume it: they depend on the contract, not on the Flink job.

| Field | Meaning |
|---|---|
| `properties`, `required`, `additionalProperties: false` | Record shape. Field order matches the producer DDL. |
| `x-topic` | The Kafka topic the contract governs. |
| `x-key` | Kafka message key fields, in order. |
| `x-semantics` | `upsert`: the latest value per key wins, and a null value (tombstone) retracts the key. |
| `x-freshness-slo` | The agreed freshness baseline. It is measured by `make bench` for encounters and checked by the smoke gate for vitals. |

## Enforcement

| Where | Check |
|---|---|
| `tests/unit/test_contracts.py` (every commit) | The producer's Flink sink DDL and the consumer's ClickHouse Kafka table match the contract: same fields, same order, same key. |
| `tests/smoke` (against the running stack) | Real records on each topic validate against the JSON Schema. |

## Change policy

- **Non-breaking**, applied in place: adding an optional (nullable) field, or adding a new value to an enum after notifying consumers. Update the contract, the producer DDL and the ClickHouse table in the same PR. The contract tests make that mandatory.
- **Breaking**, which needs a new version: removing or renaming a field, changing a type, changing key fields or semantics. Publish `<topic>.v2` with a new contract file, run v1 and v2 side by side, and retire v1 once every consumer has confirmed migration.
- Data semantics, such as the definition of `news2_score`, count as part of the contract. A change to scoring is announced to consumers even when the schema does not change.

## Reading guide for consumers

- `curated.encounters.v1`: one record per encounter, where the latest record wins. `sex` and `age_band` can be null for a short time if an encounter arrives before its patient record. A corrected record follows with the same key.
- `curated.vitals_1m.v1`: one record per (encounter, minute), emitted when the event-time watermark passes `window_end`. A reading that arrives more than 5 seconds after its window closes is dropped. Upsert semantics make re-emission after recovery idempotent.
