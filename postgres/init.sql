-- Source-of-record EHR schema (synthetic data only, no PHI).
-- Every row carries tenant_id: one tenant = one hospital.

CREATE TABLE hospitals (
    tenant_id   TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
);

CREATE TABLE patients (
    patient_id  BIGSERIAL PRIMARY KEY,
    tenant_id   TEXT NOT NULL REFERENCES hospitals (tenant_id),
    mrn         TEXT NOT NULL,
    birth_year  INT  NOT NULL,
    sex         TEXT NOT NULL CHECK (sex IN ('F', 'M', 'U')),
    created_at  TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    updated_at  TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    UNIQUE (tenant_id, mrn)
);

CREATE TABLE encounters (
    encounter_id   BIGSERIAL PRIMARY KEY,
    tenant_id      TEXT NOT NULL REFERENCES hospitals (tenant_id),
    patient_id     BIGINT NOT NULL REFERENCES patients (patient_id),
    encounter_type TEXT NOT NULL CHECK (encounter_type IN ('inpatient', 'emergency', 'observation')),
    status         TEXT NOT NULL CHECK (status IN ('admitted', 'discharged')),
    ward           TEXT NOT NULL,
    admitted_at    TIMESTAMP NOT NULL,
    discharged_at  TIMESTAMP,
    updated_at     TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
);

-- Append-only fact table: rows are never updated or deleted (see contracts/README.md).
CREATE TABLE vital_observations (
    observation_id BIGSERIAL PRIMARY KEY,
    tenant_id      TEXT NOT NULL,
    encounter_id   BIGINT NOT NULL REFERENCES encounters (encounter_id),
    patient_id     BIGINT NOT NULL REFERENCES patients (patient_id),
    observed_at    TIMESTAMP NOT NULL,
    heart_rate     INT NOT NULL,
    resp_rate      INT NOT NULL,
    spo2           INT NOT NULL,
    systolic_bp    INT NOT NULL,
    temperature_c  DOUBLE PRECISION NOT NULL
);

CREATE INDEX ON encounters (tenant_id, status);
CREATE INDEX ON vital_observations (encounter_id, observed_at);

-- Full before-images so downstream consumers can see what changed on updates.
ALTER TABLE patients   REPLICA IDENTITY FULL;
ALTER TABLE encounters REPLICA IDENTITY FULL;

CREATE PUBLICATION ehr_cdc FOR TABLE patients, encounters, vital_observations;
