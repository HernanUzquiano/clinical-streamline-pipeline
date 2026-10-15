-- Job: encounters-curated
-- CDC changelog of encounters + patients  ->  curated.encounters.v1 (upsert, contract: contracts/curated.encounters.v1.json)
--
-- Both sources are Debezium changelogs, so the join is a regular (non-windowed) join that keeps
-- both sides in state. State TTL bounds that state; encounters older than the TTL that receive
-- an update will be re-emitted without patient attributes (see docs/runbooks/encounters-curated.md).

SET 'pipeline.name' = 'encounters-curated';
SET 'table.local-time-zone' = 'UTC';
SET 'table.exec.state.ttl' = '30 d';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';

CREATE TABLE src_patients (
    patient_id  BIGINT,
    tenant_id   STRING,
    mrn         STRING,
    birth_year  INT,
    sex         STRING,
    created_at  BIGINT,   -- Debezium MicroTimestamp (epoch micros)
    updated_at  BIGINT,
    PRIMARY KEY (patient_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'ehr.public.patients',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-encounters-curated',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

CREATE TABLE src_encounters (
    encounter_id   BIGINT,
    tenant_id      STRING,
    patient_id     BIGINT,
    encounter_type STRING,
    status         STRING,
    ward           STRING,
    admitted_at    BIGINT,
    discharged_at  BIGINT,
    updated_at     BIGINT,
    PRIMARY KEY (encounter_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'ehr.public.encounters',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-encounters-curated',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

CREATE TABLE curated_encounters (
    tenant_id            STRING NOT NULL,
    encounter_id         BIGINT NOT NULL,
    patient_id           BIGINT,
    encounter_type       STRING,
    status               STRING,
    ward                 STRING,
    admitted_at          TIMESTAMP(3),
    discharged_at        TIMESTAMP(3),
    updated_at           TIMESTAMP(3),
    sex                  STRING,
    age_band             STRING,
    length_of_stay_hours DOUBLE,
    PRIMARY KEY (tenant_id, encounter_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'curated.encounters.v1',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json',
    'value.fields-include' = 'ALL'
);

CREATE TEMPORARY VIEW encounters_typed AS
SELECT
    encounter_id,
    tenant_id,
    patient_id,
    encounter_type,
    status,
    ward,
    CAST(TO_TIMESTAMP_LTZ(admitted_at / 1000, 3) AS TIMESTAMP(3)) AS admitted_at,
    CAST(TO_TIMESTAMP_LTZ(discharged_at / 1000, 3) AS TIMESTAMP(3)) AS discharged_at,
    CAST(TO_TIMESTAMP_LTZ(updated_at / 1000, 3) AS TIMESTAMP(3)) AS updated_at,
    discharged_at - admitted_at AS stay_micros
FROM src_encounters;

INSERT INTO curated_encounters
SELECT
    e.tenant_id,
    e.encounter_id,
    e.patient_id,
    e.encounter_type,
    e.status,
    e.ward,
    e.admitted_at,
    e.discharged_at,
    e.updated_at,
    p.sex,
    CASE
        WHEN p.birth_year IS NULL THEN CAST(NULL AS STRING)
        WHEN EXTRACT(YEAR FROM e.admitted_at) - p.birth_year < 18 THEN '0-17'
        WHEN EXTRACT(YEAR FROM e.admitted_at) - p.birth_year < 40 THEN '18-39'
        WHEN EXTRACT(YEAR FROM e.admitted_at) - p.birth_year < 65 THEN '40-64'
        ELSE '65+'
    END AS age_band,
    ROUND(CAST(e.stay_micros AS DOUBLE) / 3600000000.0, 2) AS length_of_stay_hours
FROM encounters_typed AS e
LEFT JOIN src_patients AS p
    ON e.patient_id = p.patient_id;
