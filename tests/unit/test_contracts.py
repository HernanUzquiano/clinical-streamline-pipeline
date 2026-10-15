"""Contract drift tests: the producer (Flink sink DDL) and the consumer (ClickHouse Kafka table) must both
match the published contract exactly — same fields, same order, same key."""

import re
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from pipeline_tools import contracts

ROOT = Path(__file__).resolve().parents[2]
TOPICS = contracts.all_topics()


def flink_sink(topic: str) -> tuple[list[str], list[str]]:
    """(columns, primary key) of the Flink table that writes `topic`."""
    for sql_file in (ROOT / "flink/jobs").glob("*.sql"):
        sql = sql_file.read_text()
        for ddl in re.findall(r"CREATE TABLE \w+ \((.*?)\) WITH \((.*?)\);", sql, re.DOTALL):
            columns_sql, options = ddl
            if f"'topic' = '{topic}'" in options and "'upsert-kafka'" in options:
                columns = re.findall(r"^\s{4}(\w+)\s+[A-Z]", columns_sql, re.MULTILINE)
                (pk,) = re.findall(r"PRIMARY KEY \(([^)]*)\)", columns_sql)
                return [c for c in columns if c != "PRIMARY"], [k.strip() for k in pk.split(",")]
    raise AssertionError(f"no Flink upsert-kafka sink writes {topic}")


def clickhouse_consumer(topic: str) -> list[str]:
    sql = (ROOT / "clickhouse/init.sql").read_text()
    kafka_tables = re.findall(r"CREATE TABLE clinical\.kafka_\w+\s*\((.*?)\)\s*ENGINE = Kafka(.*?);", sql, re.DOTALL)
    for columns_sql, settings in kafka_tables:
        if f"kafka_topic_list = '{topic}'" in settings:
            return re.findall(r"^\s+(\w+)\s+\w", columns_sql, re.MULTILINE)
    raise AssertionError(f"no ClickHouse Kafka table consumes {topic}")


def test_contracts_exist():
    assert TOPICS == ["curated.encounters.v1", "curated.vitals_1m.v1"]


@pytest.mark.parametrize("topic", TOPICS)
def test_contract_is_valid_json_schema(topic):
    Draft202012Validator.check_schema(contracts.load(topic))
    assert contracts.load(topic)["x-topic"] == topic
    assert set(contracts.key_fields(topic)) <= set(contracts.load(topic)["required"])


@pytest.mark.parametrize("topic", TOPICS)
def test_flink_producer_matches_contract(topic):
    columns, pk = flink_sink(topic)
    assert columns == contracts.fields(topic)
    assert pk == contracts.key_fields(topic)


@pytest.mark.parametrize("topic", TOPICS)
def test_clickhouse_consumer_matches_contract(topic):
    assert clickhouse_consumer(topic) == contracts.fields(topic)


def test_valid_encounter_record_passes():
    record = {
        "tenant_id": "H001",
        "encounter_id": 1,
        "patient_id": 7,
        "encounter_type": "inpatient",
        "status": "discharged",
        "ward": "ICU",
        "admitted_at": "2026-10-15 08:00:00",
        "discharged_at": "2026-10-15 20:30:00.250",
        "updated_at": "2026-10-15 20:30:00.250",
        "sex": "F",
        "age_band": "65+",
        "length_of_stay_hours": 12.5,
    }
    assert contracts.validate("curated.encounters.v1", record) == []


def test_contract_violations_are_reported():
    record = {
        "tenant_id": "H001",
        "encounter_id": 1,
        "patient_id": 7,
        "encounter_type": "outpatient",
        "status": "admitted",
        "ward": "ICU",
        "admitted_at": "2026-10-15T08:00:00Z",
        "updated_at": "2026-10-15 08:00:00",
        "unexpected": True,
    }
    errors = contracts.validate("curated.encounters.v1", record)
    assert any(e.startswith("encounter_type") for e in errors)
    assert any(e.startswith("admitted_at") for e in errors)
    assert any("unexpected" in e for e in errors)
