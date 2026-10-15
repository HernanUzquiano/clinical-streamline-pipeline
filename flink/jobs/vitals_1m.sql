-- Job: vitals-1m
-- CDC inserts of vital_observations  ->  1-minute tumbling windows per encounter, scored with a
-- simplified NEWS2-style early-warning score  ->  curated.vitals_1m.v1 (contract: contracts/curated.vitals_1m.v1.json)
--
-- vital_observations is append-only by contract, so this job reads the raw Debezium envelope as an
-- append stream (op = 'c' | 'r') instead of a changelog. That keeps the job eligible for
-- event-time window aggregation, whose state is dropped as soon as each window fires.
--
-- The scoring thresholds must stay in sync with pipeline_tools/news2.py (enforced by tests/unit/test_news2_parity.py).
-- NOT FOR CLINICAL USE.

SET 'pipeline.name' = 'vitals-1m';
SET 'table.local-time-zone' = 'UTC';
SET 'table.exec.source.idle-timeout' = '30 s';

CREATE TABLE src_vital_observations (
    op    STRING,
    `after` ROW<
        observation_id BIGINT,
        tenant_id      STRING,
        encounter_id   BIGINT,
        patient_id     BIGINT,
        observed_at    BIGINT,
        heart_rate     INT,
        resp_rate      INT,
        spo2           INT,
        systolic_bp    INT,
        temperature_c  DOUBLE
    >,
    event_time AS TO_TIMESTAMP_LTZ(`after`.observed_at / 1000, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'ehr.public.vital_observations',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-vitals-1m',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE curated_vitals_1m (
    tenant_id       STRING NOT NULL,
    encounter_id    BIGINT NOT NULL,
    patient_id      BIGINT,
    window_start    TIMESTAMP(3) NOT NULL,
    window_end      TIMESTAMP(3),
    sample_count    BIGINT,
    avg_heart_rate  DOUBLE,
    avg_resp_rate   DOUBLE,
    avg_spo2        DOUBLE,
    avg_systolic_bp DOUBLE,
    avg_temperature_c DOUBLE,
    min_spo2        INT,
    max_heart_rate  INT,
    news2_score     INT,
    risk_level      STRING,
    PRIMARY KEY (tenant_id, encounter_id, window_start) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'curated.vitals_1m.v1',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json',
    'value.fields-include' = 'ALL'
);

CREATE TEMPORARY VIEW vitals AS
SELECT
    `after`.tenant_id     AS tenant_id,
    `after`.encounter_id  AS encounter_id,
    `after`.patient_id    AS patient_id,
    `after`.heart_rate    AS heart_rate,
    `after`.resp_rate     AS resp_rate,
    `after`.spo2          AS spo2,
    `after`.systolic_bp   AS systolic_bp,
    `after`.temperature_c AS temperature_c,
    event_time
FROM src_vital_observations
WHERE op IN ('c', 'r') AND `after` IS NOT NULL;

CREATE TEMPORARY VIEW vitals_windowed AS
SELECT
    tenant_id,
    encounter_id,
    MAX(patient_id) AS patient_id,
    window_start,
    window_end,
    COUNT(*) AS sample_count,
    ROUND(AVG(CAST(heart_rate AS DOUBLE)), 2)  AS avg_heart_rate,
    ROUND(AVG(CAST(resp_rate AS DOUBLE)), 2)   AS avg_resp_rate,
    ROUND(AVG(CAST(spo2 AS DOUBLE)), 2)        AS avg_spo2,
    ROUND(AVG(CAST(systolic_bp AS DOUBLE)), 2) AS avg_systolic_bp,
    ROUND(AVG(temperature_c), 2)               AS avg_temperature_c,
    MIN(spo2)       AS min_spo2,
    MAX(heart_rate) AS max_heart_rate
FROM TABLE(TUMBLE(TABLE vitals, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY tenant_id, encounter_id, window_start, window_end;

CREATE TEMPORARY VIEW vitals_scored AS
SELECT
    *,
    -- respiration rate
    (CASE WHEN avg_resp_rate <= 8 THEN 3
          WHEN avg_resp_rate <= 11 THEN 1
          WHEN avg_resp_rate <= 20 THEN 0
          WHEN avg_resp_rate <= 24 THEN 2
          ELSE 3 END)
    -- oxygen saturation (scale 1)
  + (CASE WHEN avg_spo2 <= 91 THEN 3
          WHEN avg_spo2 <= 93 THEN 2
          WHEN avg_spo2 <= 95 THEN 1
          ELSE 0 END)
    -- systolic blood pressure
  + (CASE WHEN avg_systolic_bp <= 90 THEN 3
          WHEN avg_systolic_bp <= 100 THEN 2
          WHEN avg_systolic_bp <= 110 THEN 1
          WHEN avg_systolic_bp <= 219 THEN 0
          ELSE 3 END)
    -- pulse
  + (CASE WHEN avg_heart_rate <= 40 THEN 3
          WHEN avg_heart_rate <= 50 THEN 1
          WHEN avg_heart_rate <= 90 THEN 0
          WHEN avg_heart_rate <= 110 THEN 1
          WHEN avg_heart_rate <= 130 THEN 2
          ELSE 3 END)
    -- temperature
  + (CASE WHEN avg_temperature_c <= 35.0 THEN 3
          WHEN avg_temperature_c <= 36.0 THEN 1
          WHEN avg_temperature_c <= 38.0 THEN 0
          WHEN avg_temperature_c <= 39.0 THEN 1
          ELSE 2 END) AS news2_score
FROM vitals_windowed;

INSERT INTO curated_vitals_1m
SELECT
    tenant_id,
    encounter_id,
    patient_id,
    CAST(window_start AS TIMESTAMP(3)),
    CAST(window_end AS TIMESTAMP(3)),
    sample_count,
    avg_heart_rate,
    avg_resp_rate,
    avg_spo2,
    avg_systolic_bp,
    avg_temperature_c,
    min_spo2,
    max_heart_rate,
    news2_score,
    CASE WHEN news2_score >= 7 THEN 'high'
         WHEN news2_score >= 5 THEN 'medium'
         ELSE 'low' END AS risk_level
FROM vitals_scored;
