-- Analytical serving layer. Each curated topic is consumed by a Kafka engine table and
-- projected into a ReplacingMergeTree by a materialized view. Readers use FINAL (or argMax)
-- to see the latest version of each key.

CREATE DATABASE IF NOT EXISTS clinical;

-- curated.encounters.v1 -------------------------------------------------------------------

CREATE TABLE clinical.encounters
(
    tenant_id            LowCardinality(String),
    encounter_id         Int64,
    patient_id           Int64,
    encounter_type       LowCardinality(String),
    status               LowCardinality(String),
    ward                 LowCardinality(String),
    admitted_at          DateTime64(3, 'UTC'),
    discharged_at        Nullable(DateTime64(3, 'UTC')),
    updated_at           DateTime64(3, 'UTC'),
    sex                  LowCardinality(Nullable(String)),
    age_band             LowCardinality(Nullable(String)),
    length_of_stay_hours Nullable(Float64),
    ingested_at          DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (tenant_id, encounter_id);

CREATE TABLE clinical.kafka_encounters
(
    tenant_id            String,
    encounter_id         Int64,
    patient_id           Int64,
    encounter_type       String,
    status               String,
    ward                 String,
    admitted_at          DateTime64(3, 'UTC'),
    discharged_at        Nullable(DateTime64(3, 'UTC')),
    updated_at           DateTime64(3, 'UTC'),
    sex                  Nullable(String),
    age_band             Nullable(String),
    length_of_stay_hours Nullable(Float64)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'curated.encounters.v1',
         kafka_group_name = 'clickhouse-encounters',
         kafka_format = 'JSONEachRow',
         kafka_skip_broken_messages = 1000,
         kafka_flush_interval_ms = 1000;

CREATE MATERIALIZED VIEW clinical.mv_encounters TO clinical.encounters AS
SELECT * FROM clinical.kafka_encounters;

-- curated.vitals_1m.v1 --------------------------------------------------------------------

CREATE TABLE clinical.vitals_1m
(
    tenant_id         LowCardinality(String),
    encounter_id      Int64,
    patient_id        Int64,
    window_start      DateTime64(3, 'UTC'),
    window_end        DateTime64(3, 'UTC'),
    sample_count      UInt32,
    avg_heart_rate    Float64,
    avg_resp_rate     Float64,
    avg_spo2          Float64,
    avg_systolic_bp   Float64,
    avg_temperature_c Float64,
    min_spo2          Int32,
    max_heart_rate    Int32,
    news2_score       UInt8,
    risk_level        LowCardinality(String),
    ingested_at       DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(window_start)
ORDER BY (tenant_id, encounter_id, window_start);

CREATE TABLE clinical.kafka_vitals_1m
(
    tenant_id         String,
    encounter_id      Int64,
    patient_id        Int64,
    window_start      DateTime64(3, 'UTC'),
    window_end        DateTime64(3, 'UTC'),
    sample_count      UInt32,
    avg_heart_rate    Float64,
    avg_resp_rate     Float64,
    avg_spo2          Float64,
    avg_systolic_bp   Float64,
    avg_temperature_c Float64,
    min_spo2          Int32,
    max_heart_rate    Int32,
    news2_score       UInt8,
    risk_level        String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'curated.vitals_1m.v1',
         kafka_group_name = 'clickhouse-vitals-1m',
         kafka_format = 'JSONEachRow',
         kafka_skip_broken_messages = 1000,
         kafka_flush_interval_ms = 1000;

CREATE MATERIALIZED VIEW clinical.mv_vitals_1m TO clinical.vitals_1m AS
SELECT * FROM clinical.kafka_vitals_1m;
